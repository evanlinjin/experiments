use bdk_chain::Anchor;
use bdk_core::BlockId;
use electrum_streaming_client::DoubleSHA;

/// Anchors a transaction to a block, recording the merkle proof that put it there.
///
/// A [`ProvenAnchor`] only exists if [`merkle`](Self::merkle) was checked against the `merkle_root`
/// of the block's header, and that header is part of the verified
/// [`HeaderChain`](crate::HeaderChain).
///
/// There is no block time here: the header the proof was checked against travels with every
/// [`Update`](crate::Update) as part of `chain_update`, so times can be read from there.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ProvenAnchor {
    /// The block the transaction is proven to be in.
    pub block_id: BlockId,
    /// Position of the transaction within the block's merkle tree.
    pub pos: usize,
    /// Merkle branch connecting the transaction to the block's merkle root.
    pub merkle: Vec<DoubleSHA>,
}

impl Anchor for ProvenAnchor {
    fn anchor_block(&self) -> BlockId {
        self.block_id
    }

    fn confirmation_height_upper_bound(&self) -> u32 {
        self.block_id.height
    }
}
