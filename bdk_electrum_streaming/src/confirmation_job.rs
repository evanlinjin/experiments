use std::collections::{BTreeMap, BTreeSet};

use bdk_core::{
    bitcoin::{block::Header, BlockHash, Txid},
    BlockId, CheckPoint,
};
use electrum_streaming_client::{request, ElectrumScriptStatus};

use crate::{AnchorUpdate, Cache, ReqQueuer};

/// How far along [`ConfirmationJob`] is.
#[derive(Debug, Default, Clone)]
pub enum ConfirmationStage {
    #[default]
    Init,
    FetchBlocks {
        to_fetch: BTreeSet<u32>,
    },
    FetchAnchors {
        to_fetch: BTreeSet<(u32, Txid)>,
    },
    /// Everything the job set out to get has arrived, and the update has not been taken yet.
    ///
    /// Kept until [`ConfirmationJob::set_idle`], so a caller not ready to publish can come back
    /// for the update on a later poll.
    Done,
    /// Nothing left to do until the target tip or the statuses move.
    ///
    /// The update was taken, or the job was abandoned on inconsistent headers. Distinct from
    /// [`Done`], which still owes one — a single stage for both would hand the same update over
    /// twice, and hand one over for an abandoned job.
    ///
    /// [`Done`]: Self::Done
    Idle,
}

impl ConfirmationStage {
    pub fn fetch_anchors(
        cache: &Cache,
        spk_statuses: impl IntoIterator<Item = ElectrumScriptStatus>,
    ) -> Self {
        let to_fetch = cache
            .subscriptions
            .spk_histories(spk_statuses)
            .filter_map(|tx| {
                let conf_height = tx.confirmation_height()?.to_consensus_u32();
                Some((conf_height, tx.txid()))
            })
            .collect();
        Self::FetchAnchors { to_fetch }
    }
}

/// What one [`ConfirmationJob::poll`] achieved.
///
/// The two `Update` variants are the parts of an [`Update`] this job owns; the rest come from
/// the [`SpkJob`]s, and the caller assembles them.
///
/// [`Update`]: crate::Update
/// [`SpkJob`]: crate::SpkJob
pub enum ConfirmationProgress {
    /// The local chain moved.
    CheckPointUpdate {
        cp: CheckPoint,
        /// If there are any evictions, we need to check which spks need reanchoring
        evicted: Vec<u32>,
    },
    /// Every anchor the job set out to prove, resolved against one chain.
    AnchorUpdate(AnchorUpdate),
    /// Something changed; poll again.
    Continue,
    /// Waiting on the server.
    Blocked,
    /// Finished, and the update is there to be taken. Reported on every poll until it is.
    Done,
}

/// The single job that moves the local chain and anchors what the scripts found.
///
/// Runs once every [`SpkJob`] has its history — the heights those histories name are all it
/// reads, so a script still downloading its own transactions has already told it every block it
/// needs. Owning the chain and the anchors together is what lets a whole set of anchors be
/// resolved against one chain: resolved per-script, each job raced a tip only this one can
/// move.
///
/// Responses from an abandoned chain must not reach it. [`Self::set_tip`] reports when the
/// target moved off the chain it was heading for so the caller can forget those requests.
///
/// [`SpkJob`]: crate::SpkJob
#[derive(Debug, Clone)]
pub struct ConfirmationJob {
    target_height: u32,
    target_header: Header,
    target_statuses: BTreeSet<ElectrumScriptStatus>,

    /// Always contains the target header; the notification carries it, so it is never fetched.
    fetched_headers: BTreeMap<u32, Header>,
    stage: ConfirmationStage,
}

impl ConfirmationJob {
    /// An assumption of the max reorg depth.
    const MAX_REORG_DEPTH: u32 = 21;

    /// Number of blocks before difficulty adjustment.
    const MAX_BATCH_HEADERS_REQUEST: u32 = 2016;

    pub fn new(target_height: u32, target_header: Header) -> Self {
        let mut job = ConfirmationJob {
            target_height,
            target_header,
            target_statuses: BTreeSet::default(),
            fetched_headers: BTreeMap::default(),
            stage: ConfirmationStage::default(),
        };
        job.reset_headers();
        job
    }

    /// Drop every header but the target's.
    ///
    /// A tip notification carries the target's header, so it is the one header never worth
    /// asking for and the one that must survive any reset — a run that does not link up to it
    /// is a chain we were never told about. Reads the target, so set that first.
    fn reset_headers(&mut self) {
        self.fetched_headers = core::iter::once((self.target_height, self.target_header)).collect();
    }

    pub fn target_tip(&self) -> BlockId {
        BlockId {
            height: self.target_height,
            hash: self.target_header.block_hash(),
        }
    }

    /// Set the target tip.
    ///
    /// Returns whether the new tip abandons the chain we were heading for. When it does, every
    /// header already in flight answers for that abandoned chain, so the caller must forget
    /// those requests before the replacements are queued.
    pub fn set_tip(&mut self, height: u32, header: Header) -> bool {
        let tip = BlockId {
            height,
            hash: header.block_hash(),
        };
        if self.target_tip() == tip {
            return false;
        }
        let prev = height.checked_sub(1).map(|height| BlockId {
            height,
            hash: header.prev_blockhash,
        });
        // A tip whose parent is the one we were already heading for extends the same chain, so
        // the headers gathered for it still describe it. Anything else may not.
        let reorged = match prev {
            Some(prev) => self.target_tip() != prev,
            None => true,
        };
        self.target_height = height;
        self.target_header = header;
        self.stage = ConfirmationStage::Init;
        if reorged {
            self.reset_headers();
        } else {
            self.fetched_headers.insert(height, header);
        }
        reorged
    }

    pub fn set_statuses(&mut self, statuses: impl IntoIterator<Item = ElectrumScriptStatus>) {
        let statuses = statuses.into_iter().collect::<BTreeSet<_>>();
        if self.target_statuses != statuses {
            self.target_statuses = statuses;
            self.stage = ConfirmationStage::Init;
        }
    }

    /// Whether the job has finished and its update has not been taken yet.
    ///
    /// False again after [`Self::set_idle`], so the same update is never handed over twice, and
    /// false for a job abandoned mid-fetch.
    pub fn is_done(&self) -> bool {
        matches!(self.stage, ConfirmationStage::Done)
    }

    /// Park the job until the target tip or the statuses move.
    ///
    /// Call this once the update it was offering has been taken; until then it keeps reporting
    /// [`ConfirmationProgress::Done`].
    pub fn set_idle(&mut self) {
        self.stage = ConfirmationStage::Idle;
    }

    /// Answer the heights the job asked for.
    pub fn resolve_blocks(&mut self, blocks: impl IntoIterator<Item = (u32, Header)>) {
        self.fetched_headers.extend(blocks);
    }

    /// Polls the job as far as it will go.
    pub fn poll(
        &mut self,
        queuer: &mut ReqQueuer,
        cache: &Cache,
        cp: &CheckPoint,
    ) -> anyhow::Result<ConfirmationProgress> {
        match core::mem::take(&mut self.stage) {
            ConfirmationStage::Init => {
                let to_fetch = self.missing_heights(cache, cp);

                // NOTE: This logic is not perfect and we may duplicate requests due to spk history
                // changes between calls to `ConfirmationJob::poll`. Let's not fix it here as we will
                // change this crate to download all headers and verify PoW later so there will be
                // no need for this logic.
                let mut start_height_opt = Option::<u32>::None;
                let mut iter = to_fetch
                    .iter()
                    .copied()
                    .filter(|h| !self.fetched_headers.contains_key(h))
                    .peekable();
                while let Some(h) = iter.next() {
                    if start_height_opt.is_none() {
                        start_height_opt = Some(h);
                    }
                    let start_height = start_height_opt.expect("must exist");
                    if iter.peek().is_some_and(|&next_h| {
                        next_h <= h.saturating_add(1)
                            && next_h.saturating_sub(start_height) < Self::MAX_BATCH_HEADERS_REQUEST
                    }) {
                        continue;
                    }
                    queuer.enqueue(request::Headers {
                        start_height,
                        count: (h + 1).saturating_sub(start_height) as usize,
                    });
                    start_height_opt = None;
                }

                self.stage = ConfirmationStage::FetchBlocks { to_fetch };
                Ok(ConfirmationProgress::Continue)
            }
            ConfirmationStage::FetchBlocks { to_fetch } => {
                if !to_fetch
                    .iter()
                    .all(|h| self.fetched_headers.contains_key(h))
                {
                    self.stage = ConfirmationStage::FetchBlocks { to_fetch };
                    return Ok(ConfirmationProgress::Blocked);
                }

                // Headers that disagree mean a reorg landed between fetches; wait to be told.
                let mut iter = self
                    .fetched_headers
                    .iter()
                    .rev()
                    .take((Self::MAX_REORG_DEPTH + 1) as usize)
                    .peekable();
                while let Some((&height, header)) = iter.next() {
                    if let Some(&(&prev_height, prev_header)) = iter.peek() {
                        if prev_height + 1 == height
                            && prev_header.block_hash() != header.prev_blockhash
                        {
                            tracing::info!(
                                height,
                                prev_blockhash = header.prev_blockhash.to_string(),
                                actual_prev_blockhash = prev_header.block_hash().to_string(),
                                "Fetched headers are inconsistent. Reorg? Abandoning."
                            );
                            self.reset_headers();
                            self.stage = ConfirmationStage::Idle;
                            return Ok(ConfirmationProgress::Blocked);
                        }
                    }
                }

                // Everything we hold is spliced in from the lowest header up. The target
                // header is always one of them, so there is always a run to splice.
                let start = self
                    .fetched_headers
                    .keys()
                    .next()
                    .copied()
                    .unwrap_or(self.target_height);
                let mut extension = BTreeMap::<u32, BlockHash>::new();
                let mut base_opt = Option::<CheckPoint>::None;
                for cp in cp.iter() {
                    if cp.height() < start {
                        base_opt = Some(cp);
                        break;
                    }
                    extension.insert(cp.height(), cp.hash());
                }
                let new_blocks = self
                    .fetched_headers
                    .iter()
                    .map(|(&height, header)| (height, header.block_hash()));
                extension.extend(new_blocks);
                if extension.get(&0).is_some_and(|&genesis_hash| {
                    genesis_hash != cp.get(0).expect("genesis must exist").hash()
                }) {
                    return Err(anyhow::anyhow!("server attempted to replace genesis"));
                }
                let extension = extension
                    .into_iter()
                    .map(|(height, hash)| BlockId { height, hash });
                let cp_update = match base_opt {
                    Some(base) => base.extend(extension).expect("must not error"),
                    None => CheckPoint::from_block_ids(extension).expect("must not error"),
                };

                let mut evicted_heights = Vec::<u32>::new();
                for cp in cp.iter() {
                    if cp_update
                        .get(cp.height())
                        .is_some_and(|cp_update| cp_update == cp)
                    {
                        break;
                    }
                    evicted_heights.push(cp.height());
                }

                self.stage =
                    ConfirmationStage::fetch_anchors(cache, self.target_statuses.iter().copied());
                Ok(ConfirmationProgress::CheckPointUpdate {
                    cp: cp_update,
                    evicted: evicted_heights,
                })
            }
            ConfirmationStage::FetchAnchors { to_fetch } => {
                let mut resolved = AnchorUpdate::new();
                let mut all_resolved = true;
                for &(height, txid) in &to_fetch {
                    let header = match self.fetched_headers.get(&height) {
                        Some(header) => header,
                        // Not expected to fire: a changed history moves the status set, which
                        // sends the job back to `Init` to plan this height. Release goes back and
                        // fetches rather than assume which block this height holds.
                        None => {
                            debug_assert!(
                                false,
                                "history named height {height}, which the header pass did not cover"
                            );
                            self.stage = ConfirmationStage::Init;
                            return Ok(ConfirmationProgress::Continue);
                        }
                    };
                    match cache.tx_cache.anchors.get(&(txid, header.block_hash())) {
                        Some(&anchor) => {
                            resolved.insert((anchor, txid));
                        }
                        None => {
                            all_resolved = false;
                            queuer.enqueue(request::GetTxMerkle { txid, height });
                        }
                    }
                }
                if !all_resolved {
                    // The whole set is kept, not just what is left: each pass resolves all of it
                    // afresh against the chain as it stands right then, so a reorg landing
                    // midway cannot leave anchors from two chains in one update.
                    self.stage = ConfirmationStage::FetchAnchors { to_fetch };
                    return Ok(ConfirmationProgress::Blocked);
                }
                self.stage = ConfirmationStage::Done;
                Ok(ConfirmationProgress::AnchorUpdate(resolved))
            }
            // `poll` took the stage, so both terminal stages have to put themselves back.
            // `Done` still owes an update and keeps offering it until it is taken.
            ConfirmationStage::Done => {
                self.stage = ConfirmationStage::Done;
                Ok(ConfirmationProgress::Done)
            }
            ConfirmationStage::Idle => {
                self.stage = ConfirmationStage::Idle;
                Ok(ConfirmationProgress::Blocked)
            }
        }
    }

    /// The heights we still need from the server.
    ///
    /// Heights whose header is already reachable from `cp` and `cache` are absorbed into
    /// `fetched_headers` on the way through, so what comes back is only the gap.
    fn missing_heights(&mut self, cache: &Cache, cp: &CheckPoint) -> BTreeSet<u32> {
        let mut to_fetch = BTreeSet::<u32>::new();

        // Heights the chain itself has to be checked at. Settled first, because a height in
        // here is one whose block may be about to be replaced — absorbing it from `cp` below
        // would answer the question with the very block under suspicion.
        if self.target_tip() != cp.block_id() {
            let only_extends_tip = self
                .target_height
                .checked_sub(1)
                .map(|height| {
                    let hash = self.target_header.prev_blockhash;
                    BlockId { height, hash }
                })
                .is_some_and(|prev| cp.block_id() == prev);
            if only_extends_tip {
                to_fetch.extend(cp.height() + 1..=self.target_height);
            } else {
                // Assumes no reorg is deeper than `MAX_REORG_DEPTH`.
                let old_tip = cp.height();
                let new_tip = self.target_height;
                to_fetch.extend(old_tip.saturating_sub(Self::MAX_REORG_DEPTH)..=old_tip);
                to_fetch.extend(new_tip.saturating_sub(Self::MAX_REORG_DEPTH)..=new_tip);
            }
        }

        // Heights that carry a transaction to anchor. One the chain already places, and whose
        // header we have, needs no request.
        let anchor_heights = self
            .target_statuses
            .iter()
            .filter_map(|&spk_status| {
                let heights = cache
                    .subscriptions
                    .spk_history(spk_status)?
                    .iter()
                    .filter_map(|tx| Some(tx.confirmation_height()?.to_consensus_u32()));
                Some(heights)
            })
            .flatten()
            .collect::<BTreeSet<u32>>();
        for height in anchor_heights {
            if !to_fetch.insert(height) {
                continue;
            }
            if let Some(&header) = cp.get(height).and_then(|cp| cache.headers.get(&cp.hash())) {
                self.fetched_headers.insert(height, header);
            }
        }

        to_fetch
    }
}
