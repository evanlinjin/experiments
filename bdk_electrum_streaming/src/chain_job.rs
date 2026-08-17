use crate::{req::ReqQueuer, HeaderChain};
use anyhow::ensure;
use bdk_core::bitcoin::block::Header;
use electrum_streaming_client::request;

/// A job that fetches a contiguous range of headers for the [`HeaderChain`].
///
/// Electrum servers cap how many headers they return per request, so the job keeps asking for the
/// remainder until the whole range has arrived. The range is applied to the chain in one go — a
/// partial run cannot be verified against what we already have.
#[derive(Debug, Clone)]
pub struct HeaderJob {
    start: u32,
    end: u32,
    // ponytail: the whole range is buffered before it is verified; ~80 bytes per header, so a
    // 100k-block initial sync peaks at ~8MB. Chunk it downwards from the tip if that ever bites.
    headers: Vec<Header>,
}

impl HeaderJob {
    /// How far below the tip we re-download to notice a reorg.
    // ponytail: a reorg deeper than this errors the connection instead of walking further back.
    const REORG_WINDOW: u32 = 20;

    fn new(queuer: &mut ReqQueuer, start: u32, end: u32) -> Self {
        let job = Self {
            start,
            end,
            headers: Vec::new(),
        };
        job.request(queuer);
        job
    }

    /// A job that brings `chain` up to the tip the server just announced.
    ///
    /// Returns `None` if no requests are needed: we are already at that tip, or the announcement
    /// simply extends it and was applied on the spot.
    pub fn to_tip(
        queuer: &mut ReqQueuer,
        chain: &mut HeaderChain,
        height: u32,
        header: Header,
    ) -> anyhow::Result<Option<Self>> {
        ensure!(
            height >= chain.base_height(),
            "server tip {height} is below our trusted starting height {}",
            chain.base_height(),
        );
        let hash = header.block_hash();
        if chain.tip_height() == Some(height) && chain.block_hash(height) == Some(hash) {
            return Ok(None);
        }
        if chain.tip_height() == Some(height - 1)
            && chain.block_hash(height - 1) == Some(header.prev_blockhash)
        {
            chain.apply(height, vec![header])?;
            return Ok(None);
        }
        let start = match chain.tip_height() {
            Some(tip) => tip
                .min(height)
                .saturating_sub(Self::REORG_WINDOW)
                .max(chain.base_height()),
            None => chain.base_height(),
        };
        Ok(Some(Self::new(queuer, start, height)))
    }

    /// A job that extends `chain` downwards until it covers `height`.
    ///
    /// Fetching starts just above the highest trusted block at or below `height` — that block is
    /// what makes the run verifiable — and stops where the chain already begins.
    ///
    /// Returns `None` if `height` is already covered.
    pub fn backfill(queuer: &mut ReqQueuer, chain: &HeaderChain, height: u32) -> Option<Self> {
        if chain.header(height).is_some() {
            return None;
        }
        let start = chain
            .trusted_at_or_below(height)
            .expect("genesis is always trusted")
            + 1;
        Some(Self::new(queuer, start, chain.base_height() - 1))
    }

    fn next_height(&self) -> u32 {
        self.start + self.headers.len() as u32
    }

    fn request(&self, queuer: &mut ReqQueuer) {
        let start_height = self.next_height();
        queuer.enqueue(request::Headers {
            start_height,
            count: (self.end + 1 - start_height) as usize,
        });
    }

    /// Absorb a batch of `headers` that starts at `start`.
    ///
    /// Returns the whole run once the last of it has arrived.
    pub fn process(
        &mut self,
        queuer: &mut ReqQueuer,
        start: u32,
        headers: Vec<Header>,
    ) -> anyhow::Result<Option<(u32, Vec<Header>)>> {
        if start != self.next_height() {
            // A batch for a superseded job.
            return Ok(None);
        }
        ensure!(
            !headers.is_empty(),
            "server returned no headers from height {start}",
        );
        self.headers.extend(headers);
        self.headers.truncate((self.end + 1 - self.start) as usize);
        tracing::trace!(
            start = self.start,
            end = self.end,
            have = self.headers.len(),
            "Header job progress",
        );
        if self.next_height() <= self.end {
            self.request(queuer);
            return Ok(None);
        }
        Ok(Some((self.start, core::mem::take(&mut self.headers))))
    }
}
