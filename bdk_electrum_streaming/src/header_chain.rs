use std::collections::BTreeMap;

use anyhow::{ensure, Context};
use bdk_core::{
    bitcoin::{
        block::Header, constants::genesis_block, params::Params, BlockHash, CompactTarget, Work,
    },
    CheckPoint,
};

/// A chain of block headers, verified against a set of user-provided trusted headers.
///
/// Every applied header must:
/// 1. match the trusted header at its height (if the user provided one),
/// 2. link to the block below it via `prev_blockhash`,
/// 3. claim the difficulty that consensus requires at its height, and
/// 4. hash below the target of that difficulty.
///
/// Syncing starts one block above the highest trusted block, so (2) pins the very first header we
/// download to a block the user vouched for. Everything from there up is verified, contiguous, and
/// kept in memory. A reorg on top of that is only accepted if it brings more work than the blocks
/// it replaces.
///
/// Genesis is trusted implicitly (it is derived from `params`, not downloaded) and every trusted
/// block is included in the [`CheckPoint`] handed out by [`tip`](Self::tip), so it can always be
/// connected to a `LocalChain`. Between the trusted blocks and the sync start there are gaps.
///
/// # Difficulty
///
/// (3) is what makes (4) mean anything: without it a server could claim a trivial difficulty and
/// mine a fake chain cheaply. Recomputing a retarget needs the header that opened the previous
/// difficulty period, so on networks where difficulty actually moves, the highest trusted block is
/// required to sit on a difficulty-adjustment boundary (`height % 2016 == 0`). That way every
/// retarget above it is recomputed from a header we already have, with no gap taken on faith.
///
/// Trusted blocks *below* the sync start are exempt: a backfilled run is pinned by a trusted block
/// at the bottom and the verified chain at the top, so its difficulty needs no checking.
#[derive(Debug, Clone)]
pub struct HeaderChain {
    params: Params,
    /// Headers the user vouches for, plus genesis. Never downloaded, never replaced.
    trusted: BTreeMap<u32, Header>,
    /// Lowest height of the contiguous verified segment.
    base: u32,
    cp: Option<CheckPoint<Header>>,
}

impl HeaderChain {
    /// Construct a [`HeaderChain`] that trusts `trusted` (height to header).
    ///
    /// The highest entry decides where syncing starts, and must sit on a difficulty-adjustment
    /// boundary on networks where difficulty moves. Lower entries are what allow history *below*
    /// the sync start to be verified later, when a transaction turns out to be confirmed down
    /// there; they can be at any height.
    ///
    /// Genesis is added automatically; an entry at height `0` must agree with `params`.
    pub fn new(
        params: impl Into<Params>,
        trusted: impl IntoIterator<Item = (u32, Header)>,
    ) -> anyhow::Result<Self> {
        let params = params.into();
        let genesis = genesis_block(&params).header;
        let mut trusted = trusted.into_iter().collect::<BTreeMap<u32, Header>>();
        if let Some(header) = trusted.insert(0, genesis) {
            ensure!(
                header.block_hash() == genesis.block_hash(),
                "trusted block at height 0 is {}, but {} has genesis {}",
                header.block_hash(),
                params.network,
                genesis.block_hash(),
            );
        }
        let anchor = *trusted
            .keys()
            .next_back()
            .expect("genesis was just inserted");
        let interval = params.difficulty_adjustment_interval() as u32;
        if retargets(&params) {
            ensure!(
                anchor % interval == 0,
                "the highest trusted block must sit on a difficulty-adjustment boundary (a \
                 multiple of {interval}) so that every retarget above it can be recomputed; \
                 height {anchor} is not one",
            );
        }
        Ok(Self {
            params,
            trusted,
            base: anchor + 1,
            cp: None,
        })
    }

    fn interval(&self) -> u32 {
        self.params.difficulty_adjustment_interval() as u32
    }

    /// The lowest height we need to download a header for.
    ///
    /// This starts one block above the highest trusted block and moves down as history is
    /// backfilled.
    pub fn base_height(&self) -> u32 {
        self.base
    }

    /// The highest trusted height at or below `height`.
    pub fn trusted_at_or_below(&self, height: u32) -> Option<u32> {
        self.trusted.range(..=height).next_back().map(|(&h, _)| h)
    }

    /// The verified tip, if we have one.
    ///
    /// Every trusted block is in it, so it can be applied to a `LocalChain`.
    pub fn tip(&self) -> Option<&CheckPoint<Header>> {
        self.cp.as_ref()
    }

    /// Height of the verified tip, if we have one.
    pub fn tip_height(&self) -> Option<u32> {
        self.cp.as_ref().map(CheckPoint::height)
    }

    /// The trusted or verified header at `height`, if we have it.
    pub fn header(&self, height: u32) -> Option<Header> {
        if let Some(&header) = self.trusted.get(&height) {
            return Some(header);
        }
        self.cp.as_ref()?.get(height).map(|cp| cp.data())
    }

    /// The trusted or verified blockhash at `height`, if we have it.
    pub fn block_hash(&self, height: u32) -> Option<BlockHash> {
        self.header(height).map(|h| h.block_hash())
    }

    /// Apply a contiguous, ascending run of `headers` beginning at `start`.
    ///
    /// The run may extend the tip, replace it (reorg), or sit below the current
    /// [`base_height`](Self::base_height) to backfill history — in which case it must stop
    /// exactly where the chain already begins, filling the gap and no more.
    ///
    /// A backfill is rebuilt rather than compared for work, because the blocks it adds sit
    /// below everything already verified and displace nothing. A run that started below the
    /// base and carried on past the tip would take that path while replacing verified blocks,
    /// so it is refused: reorging the tip is the extend path's business, where the replacement
    /// has to out-work what it replaces.
    ///
    /// The chain is left untouched if anything fails to verify.
    pub fn apply(&mut self, start: u32, headers: Vec<Header>) -> anyhow::Result<()> {
        if headers.is_empty() {
            return Ok(());
        }
        ensure!(start > 0, "genesis is never applied");
        self.verify(start, &headers)?;

        let end = start + headers.len() as u32 - 1;
        let old_tip_height = self.tip_height();
        let run = (start..).zip(headers);
        // Trusted blocks below the run keep the checkpoint connectable to a `LocalChain`.
        let below = self
            .trusted
            .range(..start)
            .map(|(&height, &header)| (height, header))
            .collect::<Vec<_>>();

        let cp = match self.cp.clone() {
            // Backfill: rebuild as the trusted blocks, the run, then whatever sat above the run.
            Some(cp) if start < self.base => {
                // Exactly, not merely far enough: a run that carried on past the base would
                // replace verified blocks here, where nothing compares work.
                ensure!(
                    end + 1 == self.base,
                    "a backfill must stop where the chain begins, at {}, but these headers \
                     stop at {end}",
                    self.base - 1,
                );
                let above = cp
                    .iter()
                    .take_while(|cp| cp.height() > end)
                    .collect::<Vec<_>>();
                build(
                    below
                        .into_iter()
                        .chain(run)
                        .chain(above.into_iter().rev().map(|cp| (cp.height(), cp.data()))),
                )?
            }
            // Extend the tip, evicting any block the run disagrees with.
            Some(old) => {
                ensure!(
                    start <= old.height() + 1,
                    "headers starting at {start} would leave a gap above tip {}",
                    old.height()
                );
                let cp = run.fold(old.clone(), |cp, (height, header)| {
                    cp.insert(height, header)
                });
                // Blocks below `start` are untouched, so comparing the chains from there is the
                // same as comparing their totals.
                let evicts_old_tip = cp
                    .get(old.height())
                    .is_none_or(|cp| cp.hash() != old.hash());
                if evicts_old_tip {
                    ensure!(
                        work_from(&cp, start) > work_from(&old, start),
                        "the run reorgs our chain from height {start} without more work than the \
                         blocks it replaces",
                    );
                }
                cp
            }
            // Nothing yet: the trusted blocks plus the run become the chain.
            None => build(below.into_iter().chain(run))?,
        };

        // An eviction must never take out a block we already trust.
        if let Some(old_tip_height) = old_tip_height {
            for (&height, header) in self.trusted.range(..=old_tip_height) {
                let hash = header.block_hash();
                ensure!(
                    cp.get(height).is_some_and(|cp| cp.hash() == hash),
                    "the applied headers displace the trusted block at height {height} ({hash})"
                );
            }
        }

        self.base = self.base.min(start);
        self.cp = Some(cp);
        Ok(())
    }

    fn verify(&self, start: u32, headers: &[Header]) -> anyhow::Result<()> {
        // Look up a header by height, preferring the run being verified over what we hold.
        let at = |height: u32| -> Option<Header> {
            height
                .checked_sub(start)
                .and_then(|i| headers.get(i as usize).copied())
                .or_else(|| self.header(height))
        };

        for (i, header) in headers.iter().enumerate() {
            let height = start + i as u32;
            let hash = header.block_hash();

            if let Some(trusted) = self.trusted.get(&height) {
                ensure!(
                    hash == trusted.block_hash(),
                    "block {hash} at height {height} conflicts with trusted block {}",
                    trusted.block_hash(),
                );
            }
            if let Some(prev) = height.checked_sub(1).and_then(at) {
                let prev_hash = prev.block_hash();
                ensure!(
                    header.prev_blockhash == prev_hash,
                    "block {hash} at height {height} does not link to {prev_hash} below it"
                );
            }
            if let Some(bits) = self.required_bits(height, at) {
                ensure!(
                    header.bits == bits,
                    "block {hash} at height {height} claims difficulty {:#x}, consensus requires {:#x}",
                    header.bits.to_consensus(),
                    bits.to_consensus(),
                );
            }
            let target = header.target();
            ensure!(
                target <= self.params.max_attainable_target,
                "block {hash} at height {height} claims a target above the proof-of-work limit"
            );
            header
                .validate_pow(target)
                .with_context(|| format!("block {hash} at height {height}"))?;
        }
        Ok(())
    }

    /// The difficulty consensus requires at `height`.
    ///
    /// Difficulty is fixed for a whole retarget period and recomputed at each boundary. Returns
    /// `None` when the rule cannot be enforced: on networks that allow min-difficulty blocks, or
    /// when we are missing a header the calculation needs — which, above the trusted anchor,
    /// cannot happen, since the anchor sits on a boundary.
    fn required_bits(
        &self,
        height: u32,
        at: impl Fn(u32) -> Option<Header>,
    ) -> Option<CompactTarget> {
        if self.params.allow_min_difficulty_blocks {
            return None;
        }
        let prev = at(height.checked_sub(1)?)?;
        let interval = self.interval();
        if height % interval != 0 {
            return Some(prev.bits);
        }
        let boundary = at(height.checked_sub(interval)?)?;
        Some(CompactTarget::from_header_difficulty_adjustment(
            boundary,
            prev,
            &self.params,
        ))
    }
}

/// Whether difficulty actually moves on this network.
fn retargets(params: &Params) -> bool {
    !params.allow_min_difficulty_blocks && !params.no_pow_retargeting
}

/// Total work of `cp` from `from` up to its tip.
fn work_from(cp: &CheckPoint<Header>, from: u32) -> Work {
    cp.range(from..)
        .fold(Work::from_be_bytes([0; 32]), |sum, cp| {
            sum + cp.data().work()
        })
}

fn build(blocks: impl IntoIterator<Item = (u32, Header)>) -> anyhow::Result<CheckPoint<Header>> {
    CheckPoint::from_blocks(blocks).map_err(|_| anyhow::anyhow!("headers do not form a chain"))
}

#[cfg(test)]
mod test {
    use super::*;
    use bdk_core::bitcoin::{block::Version, hashes::Hash, CompactTarget, TxMerkleNode};

    /// Regtest, but with the difficulty rules switched on so they actually get exercised.
    /// Retargeting stays off, so trusted blocks may sit anywhere.
    fn params() -> Params {
        let mut params = Params::REGTEST;
        params.allow_min_difficulty_blocks = false;
        params
    }

    /// Regtest with real retargeting over a 10-block period, so retargets are cheap to mine.
    fn retarget_params() -> Params {
        let mut params = params();
        params.no_pow_retargeting = false;
        params.pow_target_timespan = 10 * params.pow_target_spacing;
        params
    }

    /// Append `n` mined headers to `chain` (indexed by height), retargeting per `params`.
    ///
    /// `tag` distinguishes otherwise-identical forks. `bits` overrides the difficulty, which only
    /// makes sense on a network that allows min-difficulty blocks.
    fn extend(
        params: &Params,
        chain: &mut Vec<Header>,
        n: usize,
        tag: u8,
        bits: Option<CompactTarget>,
    ) {
        let interval = params.difficulty_adjustment_interval() as u32;
        for _ in 0..n {
            let height = chain.len() as u32;
            let prev = chain[height as usize - 1];
            let bits = bits.unwrap_or(if height % interval == 0 {
                CompactTarget::from_header_difficulty_adjustment(
                    chain[(height - interval) as usize],
                    prev,
                    params,
                )
            } else {
                prev.bits
            });
            let mut merkle_root = [0u8; 32];
            merkle_root[0] = tag;
            merkle_root[1] = height as u8;
            let mut header = Header {
                version: Version::ONE,
                prev_blockhash: prev.block_hash(),
                merkle_root: TxMerkleNode::from_byte_array(merkle_root),
                time: prev.time + 600,
                bits,
                nonce: 0,
            };
            // Grind for real proof-of-work.
            while header.validate_pow(header.target()).is_err() {
                header.nonce += 1;
            }
            chain.push(header);
        }
    }

    /// Genesis plus `n` mined headers. `chain[h]` is the header at height `h`.
    fn mine(params: &Params, n: usize) -> Vec<Header> {
        let mut chain = vec![genesis_block(params).header];
        extend(params, &mut chain, n, 0, None);
        chain
    }

    /// A fork of `chain` that branches above `from`, `n` blocks long.
    fn fork(
        params: &Params,
        chain: &[Header],
        from: u32,
        n: usize,
        bits: Option<CompactTarget>,
    ) -> Vec<Header> {
        let mut forked = chain[..=from as usize].to_vec();
        extend(params, &mut forked, n, 1, bits);
        forked[from as usize + 1..].to_vec()
    }

    fn chain(headers: &[Header], trusted_heights: &[u32]) -> HeaderChain {
        HeaderChain::new(
            params(),
            trusted_heights.iter().map(|&h| (h, headers[h as usize])),
        )
        .unwrap()
    }

    #[test]
    fn accepts_a_valid_chain() {
        let headers = mine(&params(), 10);
        let mut c = chain(&headers, &[5]);
        assert_eq!(c.base_height(), 6, "sync starts above the trusted block");
        c.apply(6, headers[6..].to_vec()).unwrap();
        assert_eq!(c.tip_height(), Some(10));
        assert_eq!(c.header(7), Some(headers[7]));
        assert_eq!(
            c.tip().unwrap().iter().last().unwrap().height(),
            0,
            "genesis is always the base"
        );
        assert_eq!(
            c.tip().unwrap().get(5).map(|cp| cp.hash()),
            Some(headers[5].block_hash()),
            "trusted blocks are in the checkpoint"
        );
    }

    #[test]
    fn rejects_a_run_that_does_not_link_to_the_trusted_block() {
        let headers = mine(&params(), 10);
        let forked = fork(&params(), &headers, 4, 6, None);
        let mut c = chain(&headers, &[5]);
        let err = c.apply(6, forked[1..].to_vec()).unwrap_err().to_string();
        assert!(err.contains("does not link to"), "{err}");
    }

    #[test]
    fn rejects_bad_pow() {
        let mut headers = mine(&params(), 3);
        let mut c = chain(&headers, &[]);
        while headers[2].validate_pow(headers[2].target()).is_ok() {
            headers[2].nonce = headers[2].nonce.wrapping_add(1);
        }
        let err = c.apply(1, headers[1..3].to_vec()).unwrap_err().to_string();
        assert!(err.contains("height 2"), "{err}");
        assert!(c.tip().is_none());
    }

    #[test]
    fn rejects_broken_link() {
        let mut headers = mine(&params(), 3);
        let mut c = chain(&headers, &[]);
        headers[3].prev_blockhash = BlockHash::all_zeros();
        assert!(c.apply(1, headers[1..].to_vec()).is_err());
    }

    #[test]
    fn rejects_difficulty_change_within_a_period() {
        let mut headers = mine(&params(), 2);
        let mut c = chain(&headers, &[]);
        headers[2].bits = CompactTarget::from_consensus(0x207ffffe);
        let err = c.apply(1, headers[1..].to_vec()).unwrap_err().to_string();
        assert!(err.contains("consensus requires"), "{err}");
    }

    #[test]
    fn rejects_conflict_with_trusted_blockhash() {
        let headers = mine(&params(), 4);
        let forked = fork(&params(), &headers, 1, 3, None);
        let mut c = chain(&headers, &[3]);
        let err = c.apply(2, forked).unwrap_err().to_string();
        assert!(err.contains("conflicts with trusted"), "{err}");
    }

    #[test]
    fn rejects_reorg_that_displaces_a_trusted_block() {
        // Plain regtest: min-difficulty blocks are allowed, so a fork may be harder than the
        // chain it replaces without tripping the difficulty rule.
        let params = Params::REGTEST;
        let headers = mine(&params, 10);
        let mut c = HeaderChain::new(params.clone(), [(3, headers[3]), (8, headers[8])]).unwrap();
        c.apply(9, headers[9..].to_vec()).unwrap();
        c.apply(4, headers[4..9].to_vec()).unwrap();
        assert_eq!(c.base_height(), 4);

        // Two blocks at 4x the difficulty out-work the seven they replace, and say nothing about
        // height 8 — but they would drop it.
        let harder = CompactTarget::from_consensus(0x201f_ffff);
        let forked = fork(&params, &headers, 3, 2, Some(harder));
        let err = c.apply(4, forked).unwrap_err().to_string();
        assert!(err.contains("displace the trusted block"), "{err}");
        assert_eq!(c.tip_height(), Some(10), "chain is left untouched");
        assert_eq!(c.header(5), Some(headers[5]));
    }

    #[test]
    fn accepts_a_reorg_with_more_work() {
        let headers = mine(&params(), 6);
        let mut c = chain(&headers, &[4]);
        c.apply(5, headers[5..].to_vec()).unwrap();
        let forked = fork(&params(), &headers, 4, 3, None);
        c.apply(5, forked.clone()).unwrap();
        assert_eq!(c.tip_height(), Some(7));
        assert_eq!(c.header(5), Some(forked[0]));
    }

    #[test]
    fn rejects_a_reorg_with_less_work() {
        let headers = mine(&params(), 8);
        let mut c = chain(&headers, &[4]);
        c.apply(5, headers[5..].to_vec()).unwrap();
        // Every block here carries the same work, so a shorter fork is a weaker chain.
        let forked = fork(&params(), &headers, 4, 2, None);
        let err = c.apply(5, forked).unwrap_err().to_string();
        assert!(err.contains("without more work"), "{err}");
        assert_eq!(c.tip_height(), Some(8), "chain is left untouched");
        assert_eq!(c.header(6), Some(headers[6]));
    }

    #[test]
    fn re_applying_the_same_headers_is_not_a_reorg() {
        let headers = mine(&params(), 8);
        let mut c = chain(&headers, &[4]);
        c.apply(5, headers[5..].to_vec()).unwrap();
        // The reorg window re-downloads blocks we already have; equal work must still be fine.
        c.apply(5, headers[5..].to_vec()).unwrap();
        assert_eq!(c.tip_height(), Some(8));
    }

    #[test]
    fn backfills_below_the_base() {
        let headers = mine(&params(), 12);
        let mut c = chain(&headers, &[3, 8]);
        c.apply(9, headers[9..].to_vec()).unwrap();
        assert_eq!(c.base_height(), 9);
        assert_eq!(c.header(5), None);
        assert_eq!(c.trusted_at_or_below(5), Some(3));

        c.apply(4, headers[4..9].to_vec()).unwrap();
        assert_eq!(c.base_height(), 4);
        assert_eq!(c.tip_height(), Some(12));
        assert_eq!(c.header(5), Some(headers[5]));
        assert_eq!(c.tip().unwrap().iter().last().unwrap().height(), 0);
    }

    #[test]
    fn rejects_backfill_that_does_not_reach_the_base() {
        let headers = mine(&params(), 12);
        let mut c = chain(&headers, &[3, 8]);
        c.apply(9, headers[9..].to_vec()).unwrap();
        assert!(c.apply(4, headers[4..7].to_vec()).is_err());
    }

    /// A run starting below the base takes the backfill path, which rebuilds the chain rather
    /// than comparing work — so a run that also reaches past the tip could swap out verified
    /// blocks for an equal-work fork, the very thing
    /// [`rejects_a_reorg_with_less_work`](Self::rejects_a_reorg_with_less_work) forbids on the
    /// extend path. A backfill has to fill the gap and stop.
    #[test]
    fn rejects_backfill_that_overshoots_the_base() {
        let headers = mine(&params(), 10);
        let mut c = chain(&headers, &[3]);
        c.apply(4, headers[4..].to_vec()).unwrap();
        assert_eq!(c.tip_height(), Some(10));

        // Real history up to the trusted block, then a fork of equal length — and so equal
        // work — over everything above it.
        let forked = fork(&params(), &headers, 3, 7, None);
        let run = headers[1..=3].iter().copied().chain(forked).collect();

        let err = c.apply(1, run).unwrap_err().to_string();
        assert!(err.contains("backfill"), "{err}");
        assert_eq!(c.header(5), Some(headers[5]), "chain is left untouched");
        assert_eq!(c.tip_height(), Some(10));
    }

    #[test]
    fn rejects_a_trusted_anchor_off_the_retarget_boundary() {
        let params = retarget_params();
        let headers = mine(&params, 12);
        let err = HeaderChain::new(params, [(11, headers[11])])
            .unwrap_err()
            .to_string();
        assert!(err.contains("difficulty-adjustment boundary"), "{err}");
    }

    #[test]
    fn verifies_every_retarget_above_a_boundary_anchor() {
        let params = retarget_params();
        let headers = mine(&params, 25);
        assert_ne!(
            headers[20].bits, headers[19].bits,
            "difficulty must actually move for this test to mean anything"
        );

        let mut c = HeaderChain::new(params.clone(), [(10, headers[10])]).unwrap();
        assert_eq!(c.base_height(), 11);
        c.apply(11, headers[11..].to_vec()).unwrap();
        assert_eq!(c.tip_height(), Some(25));

        // The retarget at 20 is recomputed from the trusted header at 10 — no gap on faith.
        let mut faked = headers.clone();
        faked[20].bits = headers[19].bits;
        let mut c = HeaderChain::new(params, [(10, headers[10])]).unwrap();
        let err = c.apply(11, faked[11..].to_vec()).unwrap_err().to_string();
        assert!(err.contains("height 20"), "{err}");
        assert!(err.contains("consensus requires"), "{err}");
    }
}
