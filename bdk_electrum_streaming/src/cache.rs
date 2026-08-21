use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    sync::Arc,
};

use bdk_core::{
    bitcoin::{self, BlockHash, Transaction, Txid},
    ConfirmationBlockTime,
};
use electrum_streaming_client::{response, ElectrumScriptHash, ElectrumScriptStatus};

/// Everything learned from the server, kept so a reconnect need not ask again.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct Cache {
    /// The server's per-script histories.
    pub spk_histories: SpkHistories,
    /// Every txid ever seen for each script hash.
    ///
    /// Stays here rather than in [`SpkHistories`] because it is the one spk-keyed map that must
    /// survive [`SpkHistories::remove`]. Two things read it after the server has stopped
    /// reporting a history: evictions are the difference between this set and the history now
    /// in hand, so replacing it with the latest history would make that difference empty and
    /// no transaction would ever be reported as evicted; and it is the record that a script
    /// was *once* active, which keeps its derivation index revealed and the lookahead
    /// extended past it.
    pub spk_txids: HashMap<ElectrumScriptHash, BTreeSet<Txid>>,
    pub txs: HashMap<Txid, Arc<Transaction>>,
    /// Written as a sequence: a `(Txid, BlockHash)` key is not a string, so a map would be
    /// unserializable in JSON and every other format that requires string keys.
    #[serde(with = "persist::anchors_as_seq")]
    pub anchors: HashMap<(Txid, BlockHash), ConfirmationBlockTime>,
    pub headers: HashMap<BlockHash, bitcoin::block::Header>,
}

/// The last history the server reported for each script hash.
///
/// The only part of [`Cache`] a caller cannot rebuild from wallet data, since the server reports
/// a script's history as it stands now and will never again mention a transaction it has dropped.
///
/// Fields are private: the height index is derived from the histories, and letting it be set
/// independently would reintroduce the desync the type exists to prevent.
#[derive(Debug, Clone, Default)]
pub struct SpkHistories {
    /// The last history reported for each script hash, with the status it stands for.
    ///
    /// The status is stored with the history rather than beside it so the two cannot desync: a
    /// replay needs the last status, and [`Self::get`] needs to know which status the
    /// history it hands back is an answer to.
    spk_hash_to_history: HashMap<ElectrumScriptHash, (ElectrumScriptStatus, Vec<response::Tx>)>,
    /// Script hashes whose history reported a transaction at each height.
    ///
    /// This is what makes a reorg actionable: when the local chain drops a block, the scripts
    /// recorded at that height are the ones whose anchors need refetching. Derived from
    /// `spk_hash_to_history`, so it is rebuilt on deserialization rather than stored.
    height_to_spk_hashes: BTreeMap<u32, BTreeSet<ElectrumScriptHash>>,
}

impl SpkHistories {
    /// How far below the tip the height index is retained by [`Self::prune`].
    ///
    /// Comfortably above [`ChainJob`]'s 21-block suffix, which bounds how deep an eviction — the
    /// only thing that reads the index — can ever reach.
    ///
    /// [`ChainJob`]: crate::chain_job::ChainJob
    pub const HEIGHT_INDEX_HORIZON: u32 = 100;

    /// Drop the history for `spk_hash`, for when the server stops reporting one.
    ///
    /// Does not touch [`Cache::spk_txids`], which has to outlive this to report the evictions.
    pub fn remove(&mut self, spk_hash: ElectrumScriptHash) {
        self.spk_hash_to_history.remove(&spk_hash);
        for spk_hashes in self.height_to_spk_hashes.values_mut() {
            spk_hashes.remove(&spk_hash);
        }
    }

    /// Record `history` as the answer to `spk_status`, replacing whatever `spk_hash` had before.
    pub fn insert(
        &mut self,
        spk_hash: ElectrumScriptHash,
        spk_status: ElectrumScriptStatus,
        history: Vec<response::Tx>,
    ) {
        for tx in &history {
            if let Some(height) = tx.confirmation_height() {
                self.height_to_spk_hashes
                    .entry(height.to_consensus_u32())
                    .or_default()
                    .insert(spk_hash);
            }
        }
        self.spk_hash_to_history
            .insert(spk_hash, (spk_status, history));
    }

    /// The history for `spk_hash`, but only if it is the one `spk_status` stands for.
    ///
    /// A job fetches for the status its notification carried, so handing it a history that
    /// answers an older status would have it finish on stale data. `None` sends it to the
    /// server instead, which is why the status is effectively part of the key.
    pub fn get(
        &self,
        spk_hash: ElectrumScriptHash,
        spk_status: ElectrumScriptStatus,
    ) -> Option<&[response::Tx]> {
        match self.spk_hash_to_history.get(&spk_hash)? {
            (status, history) if *status == spk_status => Some(history),
            _ => None,
        }
    }

    /// Every script hash whose history reported a transaction at one of `heights`, deduplicated.
    ///
    /// Given the heights a reorg evicted, these are the scripts whose anchors need refetching.
    pub fn spk_hashes_at_heights<'a>(
        &'a self,
        heights: impl IntoIterator<Item = u32> + 'a,
    ) -> impl Iterator<Item = ElectrumScriptHash> + 'a {
        heights
            .into_iter()
            .filter_map(|height| self.height_to_spk_hashes.get(&height))
            .flatten()
            .copied()
            .filter({
                let mut dedup = HashSet::new();
                move |&spk_hash| dedup.insert(spk_hash)
            })
    }

    /// The last status the server reported for `spk_hash`, if it still has a history.
    pub fn status(&self, spk_hash: ElectrumScriptHash) -> Option<ElectrumScriptStatus> {
        self.spk_hash_to_history
            .get(&spk_hash)
            .map(|&(status, _)| status)
    }

    /// Drop height index entries too far below `tip_height` for any reorg to reach.
    ///
    /// The histories themselves are untouched; only the index is bounded.
    pub fn prune(&mut self, tip_height: u32) {
        if let Some(height) = tip_height.checked_sub(Self::HEIGHT_INDEX_HORIZON) {
            self.height_to_spk_hashes = self.height_to_spk_hashes.split_off(&height);
        }
    }
}

/// Types and impls that exist only so [`Cache`] can be stored and loaded.
///
/// Kept apart from the cache itself because [`HistoryTx`] mirrors [`response::Tx`] and the two
/// are easy to mistake for each other at a glance.
mod persist {
    use super::*;

    /// A history entry in the shape we can write back out.
    ///
    /// [`response::Tx`] derives `Deserialize` only, so histories round-trip through this instead.
    #[derive(serde::Serialize, serde::Deserialize)]
    enum HistoryTx {
        Mempool {
            txid: Txid,
            fee_sats: u64,
            confirmed_inputs: bool,
        },
        Confirmed {
            txid: Txid,
            height: u32,
        },
    }

    impl From<&response::Tx> for HistoryTx {
        fn from(tx: &response::Tx) -> Self {
            match tx {
                response::Tx::Mempool(tx) => Self::Mempool {
                    txid: tx.txid,
                    fee_sats: tx.fee.to_sat(),
                    confirmed_inputs: tx.confirmed_inputs,
                },
                response::Tx::Confirmed(tx) => Self::Confirmed {
                    txid: tx.txid,
                    height: tx.height.to_consensus_u32(),
                },
            }
        }
    }

    impl TryFrom<HistoryTx> for response::Tx {
        type Error = bitcoin::absolute::ConversionError;

        fn try_from(tx: HistoryTx) -> Result<Self, Self::Error> {
            Ok(match tx {
                HistoryTx::Mempool {
                    txid,
                    fee_sats,
                    confirmed_inputs,
                } => Self::Mempool(response::MempoolTx {
                    txid,
                    fee: bitcoin::Amount::from_sat(fee_sats),
                    confirmed_inputs,
                }),
                HistoryTx::Confirmed { txid, height } => Self::Confirmed(response::ConfirmedTx {
                    txid,
                    height: bitcoin::absolute::Height::from_consensus(height)?,
                }),
            })
        }
    }

    /// Only the histories are written; the height index is rebuilt from them on the way back in.
    impl serde::Serialize for SpkHistories {
        fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
            serializer.collect_map(self.spk_hash_to_history.iter().map(
                |(&spk_hash, (status, history))| {
                    (
                        spk_hash,
                        (
                            status,
                            history.iter().map(HistoryTx::from).collect::<Vec<_>>(),
                        ),
                    )
                },
            ))
        }
    }

    impl<'de> serde::Deserialize<'de> for SpkHistories {
        fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
            use serde::de::Error;
            let stored =
                HashMap::<ElectrumScriptHash, (ElectrumScriptStatus, Vec<HistoryTx>)>::deserialize(
                    deserializer,
                )?;
            let mut spk_histories = Self::default();
            for (spk_hash, (status, history)) in stored {
                let history = history
                    .into_iter()
                    .map(response::Tx::try_from)
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(D::Error::custom)?;
                spk_histories.insert(spk_hash, status, history);
            }
            Ok(spk_histories)
        }
    }

    pub(super) mod anchors_as_seq {
        use super::*;
        use serde::{Deserialize, Deserializer, Serializer};

        type Anchors = HashMap<(Txid, BlockHash), ConfirmationBlockTime>;

        pub fn serialize<S: Serializer>(
            anchors: &Anchors,
            serializer: S,
        ) -> Result<S::Ok, S::Error> {
            serializer.collect_seq(
                anchors
                    .iter()
                    .map(|(&(txid, block_hash), anchor)| (txid, block_hash, anchor)),
            )
        }

        pub fn deserialize<'de, D: Deserializer<'de>>(
            deserializer: D,
        ) -> Result<Anchors, D::Error> {
            Ok(
                Vec::<(Txid, BlockHash, ConfirmationBlockTime)>::deserialize(deserializer)?
                    .into_iter()
                    .map(|(txid, block_hash, anchor)| ((txid, block_hash), anchor))
                    .collect(),
            )
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use bitcoin::hashes::Hash;

    fn txid(byte: u8) -> Txid {
        Txid::from_byte_array([byte; 32])
    }

    fn spk_hash(byte: u8) -> ElectrumScriptHash {
        ElectrumScriptHash::from_byte_array([byte; 32])
    }

    /// The height index is not stored, so it has to come back from the histories themselves.
    #[test]
    fn spk_histories_round_trip_rebuilds_the_height_index() {
        let history = vec![
            response::Tx::Confirmed(response::ConfirmedTx {
                txid: txid(1),
                height: bitcoin::absolute::Height::from_consensus(700_000).unwrap(),
            }),
            response::Tx::Mempool(response::MempoolTx {
                txid: txid(2),
                fee: bitcoin::Amount::from_sat(1234),
                confirmed_inputs: false,
            }),
        ];
        let status = ElectrumScriptStatus::from_history(&history).expect("history is not empty");

        let mut before = SpkHistories::default();
        before.insert(spk_hash(9), status, history);

        let json = serde_json::to_string(&before).expect("must serialize");
        assert!(
            !json.contains("height_to_spk_hashes"),
            "the derived index must not be stored"
        );
        let after: SpkHistories = serde_json::from_str(&json).expect("must deserialize");

        assert_eq!(
            after.get(spk_hash(9), status).map(|h| h.len()),
            Some(2),
            "the history must survive, and still answer to its status"
        );
        assert_eq!(
            after.spk_hashes_at_heights([700_000]).collect::<Vec<_>>(),
            vec![spk_hash(9)],
            "the confirmed entry must be indexed by height again"
        );
        assert_eq!(
            after.spk_hashes_at_heights([700_001]).count(),
            0,
            "only heights the history actually reported"
        );
        assert_eq!(after.status(spk_hash(9)), Some(status));
    }

    /// `anchors` is keyed by a tuple, which JSON cannot use as a map key.
    #[test]
    fn cache_round_trips_through_json() {
        let anchor = (txid(1), bitcoin::BlockHash::from_byte_array([2; 32]));
        let mut before = Cache::default();
        before
            .anchors
            .insert(anchor, ConfirmationBlockTime::default());

        let json = serde_json::to_string(&before).expect("must serialize");
        let after: Cache = serde_json::from_str(&json).expect("must deserialize");

        assert_eq!(after.anchors.get(&anchor), before.anchors.get(&anchor));
    }
}
