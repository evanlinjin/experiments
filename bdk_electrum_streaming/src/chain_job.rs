use crate::req::ReqQueuer;
use bdk_core::{
    bitcoin::{block::Header, BlockHash},
    BlockId, CheckPoint,
};
use electrum_streaming_client::request;
use std::collections::{BTreeMap, BTreeSet};

/// A job that tries to update the [`State`]'s internal [`CheckPoint`] to the latest tip.
///
/// The job can be completed with [`try_finish()`] given that we have all the blocks required to
/// complete the job. Otherwise, blocks can be introduced to the job with [`process_blocks()`].
///
/// [`State`]: crate::State
/// [`try_finish()`]: ChainJob::try_finish
/// [`process_blocks()`]: ChainJob::process_blocks
#[derive(Debug, Clone)]
pub struct ChainJob {
    /// The block this job was created to reach.
    ///
    /// A `blockchain.block.headers` batch is answered from whichever chain the server holds when
    /// it *replies*, which need not be the one it announced. Keeping the target lets
    /// [`try_finish()`] tell the two apart instead of writing an unannounced chain into the
    /// checkpoint chain.
    ///
    /// [`try_finish()`]: ChainJob::try_finish
    target: BlockId,
    missing_headers: BTreeSet<u32>,
    cp_update: BTreeMap<u32, BlockHash>,
}

/// The outcome of [`ChainJob::try_finish`].
#[derive(Debug)]
pub enum ChainJobOutcome {
    /// Every header arrived, and they agree with the block the job was created for.
    Finished(CheckPoint),
    /// Still waiting on headers.
    Pending(ChainJob),
    /// The headers arrived but disagree with the block the job was created for, so they describe
    /// a chain other than the one announced.
    ///
    /// The job is abandoned rather than retried: the server only answers from a chain it has
    /// moved to, and moving is what makes it announce a new tip, so the notification that
    /// rebuilds the job is already on its way.
    Superseded,
}

impl ChainJob {
    const CHAIN_SUFFIX_LENGTH: u32 = 21;

    /// Construct [`ChainJob`].
    ///
    /// Returns `None` if no job is required. I.e. `local_tip` is already at `height` and `header`.
    pub fn new(
        mut queuer: ReqQueuer,
        local_tip: &CheckPoint,
        header: Header,
        height: u32,
    ) -> Option<Self> {
        let target = BlockId {
            height,
            hash: header.block_hash(),
        };
        let cp = local_tip
            .iter()
            .find(|cp| cp.height() <= height)
            .expect("Local checkpoint must at least have genesis");

        // Try to short-circuit if possible.
        if cp.height() == height {
            if cp.hash() == header.block_hash() {
                return None;
            }
            if let Some(prev_cp) = cp.prev() {
                if let Some(prev_height) = height.checked_sub(1) {
                    if prev_height == prev_cp.height() && header.prev_blockhash == prev_cp.hash() {
                        return Some(Self {
                            target,
                            missing_headers: BTreeSet::new(),
                            cp_update: core::iter::once((height, header.block_hash())).collect(),
                        });
                    }
                }
            }
        }

        let local_start_height = cp.height().saturating_sub(Self::CHAIN_SUFFIX_LENGTH - 1);
        let local_height = cp.height();
        let remote_start_height = height.saturating_sub(Self::CHAIN_SUFFIX_LENGTH - 1);
        let remote_height = height;

        // Overlap?
        if remote_start_height <= local_height {
            let start_height = Ord::min(local_start_height, remote_start_height);
            let count = (remote_height + 1 - start_height) as usize;
            queuer.enqueue(request::Headers {
                start_height,
                count,
            });
            Some(Self {
                target,
                missing_headers: (start_height..=remote_height).collect(),
                cp_update: BTreeMap::new(),
            })
        } else {
            // Otherwise we have to do two separate requests.
            queuer.enqueue(request::Headers {
                start_height: local_start_height,
                count: (local_height + 1 - local_start_height) as usize,
            });
            queuer.enqueue(request::Headers {
                start_height: remote_start_height,
                count: (remote_height + 1 - remote_start_height) as usize,
            });
            Some(Self {
                target,
                missing_headers: (local_start_height..=local_height)
                    .chain(remote_start_height..=remote_height)
                    .collect(),
                cp_update: BTreeMap::new(),
            })
        }
    }

    pub fn process_blocks(mut self, headers: impl IntoIterator<Item = (u32, BlockHash)>) -> Self {
        let headers = headers.into_iter().collect::<Vec<_>>();
        for (height, header) in headers.iter().cloned() {
            if self.missing_headers.remove(&height) {
                self.cp_update.insert(height, header);
            }
        }
        tracing::trace!(
            processed = headers.len(),
            remaining = self.missing_headers.len(),
            "Processed blocks for chain job",
        );
        self
    }

    pub fn try_finish(self, local_tip: &mut CheckPoint) -> ChainJobOutcome {
        if !self.missing_headers.is_empty() {
            tracing::trace!(
                missing = self.missing_headers.len(),
                "Chain job not finished"
            );
            return ChainJobOutcome::Pending(self);
        }

        // The batch is only evidence about the chain it was answered from. If it does not put
        // the announced block at the announced height, that is a chain we were never told about
        // and must not adopt — the same reasoning the merkle and header handlers apply to a
        // proof built against a block other than ours.
        if self.cp_update.get(&self.target.height) != Some(&self.target.hash) {
            tracing::info!(
                height = self.target.height,
                announced = self.target.hash.to_string(),
                received = self
                    .cp_update
                    .get(&self.target.height)
                    .map(|h| h.to_string()),
                "Headers describe a chain other than the one announced. Abandoning chain job.",
            );
            return ChainJobOutcome::Superseded;
        }

        let mut cp = local_tip.clone();
        for (height, hash) in self.cp_update {
            cp = cp.insert(BlockId { height, hash });
        }
        *local_tip = cp.clone();
        tracing::info!(
            tip_height = cp.height(),
            tip_hash = cp.hash().to_string(),
            "Chain job finished"
        );
        ChainJobOutcome::Finished(cp)
    }
}
