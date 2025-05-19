//! BDK Electrum goodness.

use bdk_core::spk_client::FullScanResponse;
/// Re-export.
pub use electrum_streaming_client;

use bdk_core::ConfirmationBlockTime;
mod state;
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

pub type Update<K> = FullScanResponse<K, ConfirmationBlockTime>;
