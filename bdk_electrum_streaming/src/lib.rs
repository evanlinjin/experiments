//! BDK Electrum goodness.

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
