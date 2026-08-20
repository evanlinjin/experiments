//! BDK Electrum goodness.
//!
//! # Anchors and the chain
//!
//! An anchor is a fact — "this txid is in this block" — established by checking a merkle proof
//! against the header fetched for the height it claims. It is keyed by block hash, so it never
//! becomes false; a reorg only makes it inert, because bdk scans every anchor a tx has and takes
//! whichever one is in the chain.
//!
//! Anchoring necessarily puts a checkpoint at every confirmation height it establishes.
//! [`LocalChain::is_block_in_chain`] answers *unknown* — not false — for a height the chain does
//! not hold, and a tx whose anchor cannot be checked reads as unconfirmed, so those checkpoints
//! are what keep confirmed transactions confirmed.
//!
//! **Those checkpoints are single unverified server answers, and nothing revisits them.** The
//! chain pass fetches a fixed window near the tip, so a checkpoint filled in below that window is
//! never checked again, and a reorg deeper than the window is never noticed at all — leaving an
//! anchor pointing at a block that is no longer in the best chain while the tx still reads
//! confirmed. Anchoring cannot compensate for that; how deep a reorg can be and still be seen is
//! decided by the chain pass alone.
//!
//! [`LocalChain::is_block_in_chain`]: https://docs.rs/bdk_chain/latest/bdk_chain/local_chain/struct.LocalChain.html

use bdk_core::spk_client::FullScanResponse;
/// Re-export.
pub use electrum_streaming_client;

use bdk_core::ConfirmationBlockTime;
mod state;
use electrum_streaming_client::{
    AsyncPendingRequest, BlockingPendingRequest, MaybeBatch, PendingRequest,
};
use miniscript::{Descriptor, DescriptorPublicKey};
pub use state::*;
mod chain_job;
pub use chain_job::*;
mod req;
pub use req::*;
mod spk_job;
pub use spk_job::*;
mod async_client;
pub use async_client::*;
mod derived_spk_tracker;
pub use derived_spk_tracker::*;
mod blocking_client;
pub use blocking_client::*;

pub type Update<K> = FullScanResponse<K, ConfirmationBlockTime>;

pub type BlockingClientAction<K> = ClientAction<K, Box<BlockingPendingRequest>>;
pub type AsyncClientAction<K> = ClientAction<K, AsyncPendingRequest>;

pub enum ClientAction<K, PReq: PendingRequest> {
    Request(MaybeBatch<PReq>),
    AddDescriptor {
        keychain: K,
        descriptor: Box<Descriptor<DescriptorPublicKey>>,
        next_index: u32,
    },
    Stop,
}
