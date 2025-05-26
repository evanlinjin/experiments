use crate::{req::ReqQueuer, Cache};
use bdk_core::{bitcoin::block::Header, BlockId, CheckPoint};
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
    headers: BTreeMap<u32, Header>,
}

impl ChainJob {
    const CHAIN_SUFFIX_LENGTH: u32 = 21;

    pub fn new(
        mut queuer: ReqQueuer,
        local_tip: CheckPoint,
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
                            headers: core::iter::once((height, header)).collect(),
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
            let count = (remote_height - start_height) as usize;
            queuer.enqueue(request::Headers {
                start_height,
                count,
            });
            Some(Self {
                missing_headers: (start_height..=remote_height).collect(),
                headers: BTreeMap::new(),
            })
        } else {
            // Otherwise we have to do two separate requests.
            queuer.enqueue(request::Headers {
                start_height: local_start_height,
                count: (local_height - local_start_height) as usize,
            });
            queuer.enqueue(request::Headers {
                start_height: remote_start_height,
                count: (remote_height - remote_start_height) as usize,
            });
            Some(Self {
                missing_headers: (local_start_height..=local_height)
                    .chain(remote_start_height..=remote_height)
                    .collect(),
                headers: BTreeMap::new(),
            })
        }
    }

    pub fn process_headers(mut self, headers: impl IntoIterator<Item = (u32, Header)>) -> Self {
        for (height, header) in headers {
            if self.missing_headers.remove(&height) {
                self.headers.insert(height, header);
            }
        }
        self
    }

    pub fn try_finish(
        self,
        cache: &mut Cache,
        local_tip: &mut CheckPoint,
    ) -> Result<CheckPoint, Self> {
        if !self.missing_headers.is_empty() {
            return Err(self);
        }

        let mut cp = local_tip.clone();
        for (height, header) in self.headers {
            let hash = header.block_hash();
            cp = cp.insert(BlockId { height, hash });
            cache.headers.insert(hash, header);
        }
        *local_tip = cp.clone();
        Ok(cp)
    }
}
