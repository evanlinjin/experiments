//! BDK Electrum goodness.

use std::collections::BTreeSet;

use bdk_core::{
    bitcoin::{block::Header, Txid},
    spk_client::FullScanResponse,
};
/// Re-export.
pub use electrum_streaming_client;

mod cache;
pub use cache::*;
mod state;
use electrum_streaming_client::{
    AsyncPendingRequest, BlockingPendingRequest, MaybeBatch, PendingRequest,
};
use miniscript::{Descriptor, DescriptorPublicKey};
pub use state::*;
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
mod confirmation_job;
pub use confirmation_job::*;
mod header_chain;
pub use header_chain::*;
mod anchor;
pub use anchor::*;

/// What a sync produces.
///
/// Anchors are [`ProvenAnchor`]s — merkle-proved against a header in the verified
/// [`HeaderChain`] — and the chain update carries full [`Header`]s, so a block's time is read
/// from there rather than copied onto every anchor.
pub type Update<K> = FullScanResponse<K, ProvenAnchor, Header>;
pub type AnchorUpdate = BTreeSet<(ProvenAnchor, Txid)>;

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
