use crate::req::ReqQueuer;
use bdk_core::{
    bitcoin::{block::Header, BlockHash},
    BlockId, CheckPoint,
};
use electrum_streaming_client::request;
use std::collections::{BTreeMap, BTreeSet};

/// A job that tries to update the [`State`]'s internal [`CheckPoint`] to the latest tip.
///
/// The job can be completed with [`try_finish()`] given that we have all the headers required to
/// complete the job. Otherwise, headers can be introduced to the job with [`process_headers()`].
///
/// [`State`]: crate::State
/// [`try_finish()`]: ChainJob::try_finish
/// [`process_headers()`]: ChainJob::process_headers
#[derive(Debug, Clone)]
pub struct ChainJob {
    missing_headers: BTreeSet<u32>,
    cp_update: BTreeMap<u32, BlockHash>,
}

impl ChainJob {
    const CHAIN_SUFFIX_LENGTH: u32 = 21;

    pub fn new(
        mut queuer: ReqQueuer,
        local_tip: &CheckPoint,
        header: Header,
        height: u32,
    ) -> Option<Self> {
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
        tracing::info!(
            processed = headers.len(),
            remaining = self.missing_headers.len(),
            "Chain Job: Processed blocks.",
        );
        self
    }

    pub fn try_finish(self, local_tip: &mut CheckPoint) -> Result<CheckPoint, Self> {
        if !self.missing_headers.is_empty() {
            tracing::trace!(
                missing = self.missing_headers.len(),
                "Chain Job: Not finished."
            );
            return Err(self);
        }

        let mut cp = local_tip.clone();
        for (height, hash) in self.cp_update {
            cp = cp.insert(BlockId { height, hash });
        }
        *local_tip = cp.clone();
        tracing::info!("Chain Job: Finished.");
        Ok(cp)
    }
}
