//! BDK Electrum goodness.

use bdk_core::spk_client::FullScanResponse;
/// Re-export.
pub use electrum_streaming_client;

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
mod header_chain;
pub use header_chain::*;
mod anchor;
pub use anchor::*;
mod blocking_client;
pub use blocking_client::*;

/// What a sync produces.
///
/// Anchors are [`ProvenAnchor`]s (merkle-proved against a verified header) and the chain update is
/// a checkpoint of full [`Header`](bdk_core::bitcoin::block::Header)s.
pub type Update<K> = FullScanResponse<K, ProvenAnchor, bdk_core::bitcoin::block::Header>;

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
