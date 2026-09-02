use std::collections::{BTreeMap, BTreeSet};

use bdk_core::{
    bitcoin::{block::Header, Txid},
    BlockId, CheckPoint,
};
use electrum_streaming_client::{request, ElectrumScriptStatus};

use crate::{AnchorUpdate, Cache, HeaderChain, ReqQueuer};

/// How far along [`ConfirmationJob`] is.
#[derive(Debug, Default, Clone)]
pub enum ConfirmationStage {
    #[default]
    Init,
    /// Waiting on contiguous runs of headers, each `start -> end` inclusive.
    ///
    /// Runs, not scattered heights: [`HeaderChain`] verifies each header against the one below
    /// it, so a header is only worth having as part of an unbroken run reaching the chain.
    FetchHeaders {
        runs: BTreeMap<u32, u32>,
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
    /// The update was taken, or the job was abandoned on a chain the server has left. Distinct
    /// from [`Done`], which still owes one — a single stage for both would hand the same update
    /// over twice, and hand one over for an abandoned job.
    ///
    /// [`Done`]: Self::Done
    Idle,
}

/// What one [`ConfirmationJob::poll`] achieved.
///
/// The two `Update` variants are the parts of an [`Update`] this job owns; the rest come from
/// the [`SpkJob`]s, and the caller assembles them.
///
/// [`Update`]: crate::Update
/// [`SpkJob`]: crate::SpkJob
#[derive(Debug)]
pub enum ConfirmationProgress {
    /// The verified chain moved.
    ChainUpdate {
        cp: CheckPoint<Header>,
        /// Whether the run displaced blocks we already had.
        reorged: bool,
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

/// The single job that moves the verified chain and anchors what the scripts found.
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

    /// Headers as the server gave them, before [`HeaderChain`] has verified any of them.
    ///
    /// Kept apart from the chain deliberately: nothing here has been checked, and a header only
    /// becomes part of the chain once its whole run passes. Always contains the target header;
    /// the notification carries it, so it is never fetched.
    fetched_headers: BTreeMap<u32, Header>,
    stage: ConfirmationStage,
}

impl ConfirmationJob {
    /// How far below the tip to re-download so a reorg is noticed.
    ///
    /// A reorg deeper than this is not walked back to; the run will not link to what we hold and
    /// the connection errors out rather than quietly keeping a chain the server has left. The
    /// verified chain is what stops a deep fork being adopted wrongly — it still has to out-work
    /// what it replaces — but noticing one at all stops here.
    ///
    /// ponytail: fixed 21-block window, downloading every header and verifying work from genesis
    /// removes the need for a window at all.
    const REORG_WINDOW: u32 = 21;

    /// Most headers a server will hand over in one `blockchain.block.headers`.
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

    /// Take one step towards a verified chain that covers everything worth anchoring.
    pub fn poll(
        &mut self,
        queuer: &mut ReqQueuer,
        cache: &Cache,
        chain: &mut HeaderChain,
    ) -> anyhow::Result<ConfirmationProgress> {
        match core::mem::take(&mut self.stage) {
            ConfirmationStage::Init => {
                let runs = self.required_runs(cache, chain);
                for (&start, &end) in &runs {
                    self.queue_gaps(queuer, start, end);
                }
                self.stage = ConfirmationStage::FetchHeaders { runs };
                Ok(ConfirmationProgress::Continue)
            }
            ConfirmationStage::FetchHeaders { runs } => {
                let complete = runs.iter().all(|(&start, &end)| {
                    (start..=end).all(|h| self.fetched_headers.contains_key(&h))
                });
                if !complete {
                    self.stage = ConfirmationStage::FetchHeaders { runs };
                    return Ok(ConfirmationProgress::Blocked);
                }

                // A run reaching the target must put the announced block at the announced
                // height. Anything else is a chain we were never told about — the server has
                // moved on, and moving is what makes it announce again, so abandon rather than
                // retry.
                if let Some(header) = self.fetched_headers.get(&self.target_height) {
                    if header.block_hash() != self.target_header.block_hash() {
                        tracing::info!(
                            height = self.target_height,
                            announced = self.target_header.block_hash().to_string(),
                            received = header.block_hash().to_string(),
                            "Headers describe a chain other than the one announced. Abandoning.",
                        );
                        self.reset_headers();
                        self.stage = ConfirmationStage::Idle;
                        return Ok(ConfirmationProgress::Blocked);
                    }
                }

                let was = chain.tip().map(|cp| (cp.height(), cp.hash()));
                // Ascending, so a backfill run lands before the run that extends the tip — which
                // is the order `HeaderChain::apply` needs, since backfill has to reach the base
                // the other run may then move.
                for (&start, &end) in &runs {
                    let headers = (start..=end)
                        .map(|h| self.fetched_headers[&h])
                        .collect::<Vec<_>>();
                    chain.apply(start, headers)?;
                }
                let cp = match chain.tip() {
                    Some(cp) => cp.clone(),
                    None => {
                        self.stage = ConfirmationStage::Done;
                        return Ok(ConfirmationProgress::Done);
                    }
                };
                let reorged =
                    was.is_some_and(|(height, hash)| chain.block_hash(height) != Some(hash));

                self.stage = self.anchor_stage(cache);
                Ok(ConfirmationProgress::ChainUpdate { cp, reorged })
            }
            ConfirmationStage::FetchAnchors { to_fetch } => {
                let mut resolved = AnchorUpdate::new();
                let mut all_resolved = true;
                let mut needs_backfill = false;
                for &(height, txid) in &to_fetch {
                    let header = match chain.header(height) {
                        Some(header) => header,
                        // Below where the chain starts: it has to grow downwards before this
                        // height can be checked at all, which `Init` plans a run for.
                        None if height < chain.base_height() => {
                            needs_backfill = true;
                            continue;
                        }
                        // Above the verified tip. There is nothing to plan: a run only reaches
                        // the tip the server has announced, and this height is beyond it. The
                        // announcement that carries it is what re-polls this — going back to
                        // `Init` here would replan the same unreachable run forever.
                        None => {
                            all_resolved = false;
                            continue;
                        }
                    };
                    match cache.anchors.get(&(txid, header.block_hash())) {
                        Some(anchor) => {
                            resolved.insert((anchor.clone(), txid));
                        }
                        None => {
                            all_resolved = false;
                            queuer.enqueue(request::GetTxMerkle { txid, height });
                        }
                    }
                }
                if needs_backfill {
                    self.stage = ConfirmationStage::Init;
                    return Ok(ConfirmationProgress::Continue);
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

    /// The heights carrying a transaction we have to anchor.
    fn anchor_heights(&self, cache: &Cache) -> BTreeSet<(u32, Txid)> {
        cache
            .subscriptions
            .spk_histories(self.target_statuses.iter().copied())
            .filter_map(|tx| Some((tx.confirmation_height()?.to_consensus_u32(), tx.txid())))
            .collect()
    }

    fn anchor_stage(&self, cache: &Cache) -> ConfirmationStage {
        ConfirmationStage::FetchAnchors {
            to_fetch: self.anchor_heights(cache),
        }
    }

    /// The contiguous runs of headers the chain needs before every anchor can be checked.
    ///
    /// At most two: one up to the announced tip, and one backfilling history below where the
    /// chain currently starts. Both are runs rather than the individual heights that want them,
    /// because a header is only verifiable as part of a chain reaching a block we trust.
    fn required_runs(&self, cache: &Cache, chain: &HeaderChain) -> BTreeMap<u32, u32> {
        let mut runs = BTreeMap::new();

        let base = chain.base_height();
        let start = match chain.tip_height() {
            // Re-download a window below the tip, so a reorg within it is seen at all. Never
            // below the base: the run has to stay contiguous with what is already verified.
            Some(tip) => base.max(tip.saturating_sub(Self::REORG_WINDOW)),
            None => base,
        };
        if start <= self.target_height {
            runs.insert(start, self.target_height);
        }

        // A transaction confirmed below where the chain starts cannot be checked against it, so
        // the chain has to grow downwards to a block we already trust.
        if let Some(&(lowest, _)) = self.anchor_heights(cache).iter().next() {
            if lowest < base {
                let from = chain.trusted_at_or_below(lowest).unwrap_or(0) + 1;
                if from < base {
                    runs.insert(from, base - 1);
                }
            }
        }

        runs
    }

    /// Queue whatever part of `start..=end` we do not already hold, in server-sized batches.
    fn queue_gaps(&self, queuer: &mut ReqQueuer, start: u32, end: u32) {
        let mut height = start;
        while height <= end {
            if self.fetched_headers.contains_key(&height) {
                height += 1;
                continue;
            }
            let mut count = 0;
            while height + count <= end
                && count < Self::MAX_BATCH_HEADERS_REQUEST
                && !self.fetched_headers.contains_key(&(height + count))
            {
                count += 1;
            }
            queuer.enqueue(request::Headers {
                start_height: height,
                count: count as usize,
            });
            height += count;
        }
    }
}
