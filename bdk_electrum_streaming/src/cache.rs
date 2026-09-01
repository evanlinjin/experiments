use std::{
    collections::{BTreeSet, HashMap},
    sync::Arc,
};

use bdk_core::{
    bitcoin::{self, block::Header, BlockHash, Transaction, Txid},
    ConfirmationBlockTime,
};
use electrum_streaming_client::{request, response, ElectrumScriptHash, ElectrumScriptStatus};

/// Everything learned from the server, kept so a reconnect need not ask again.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct Cache {
    /// The server's per-script histories.
    pub subscriptions: Subscriptions,

    /// What we already hold, so a job knows what it need not ask for.
    ///
    /// Not persisted: every part of it is in the caller's wallet already, and a second copy
    /// would only give the two something to disagree about. Seed it from wallet data instead.
    #[serde(skip)]
    pub tx_cache: TxCache,

    /// This can be removed once we can place `Header`s in `CheckPoint`s.
    pub headers: HashMap<BlockHash, bitcoin::block::Header>,
}

/// The transaction data a job consults before asking the server for anything.
///
/// Separate from the rest of [`Cache`] because a caller can rebuild all of it from their own
/// wallet: the transactions are in their graph, the anchors with them, and which transactions
/// paid a script is what their spk index is for. So none of it is persisted alongside
/// [`Subscriptions`], which nothing can reconstruct.
///
/// Starting empty is always correct, only expensive: a job asks the server for whatever it
/// cannot find here, so an empty one re-downloads every transaction and reproves every anchor.
/// It is not a mirror of the wallet, though — whatever a job fetches lands here too, so it
/// answers "do we already have this" whoever supplied it.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct TxCache {
    /// Every txid ever seen for each script hash.
    ///
    /// This is monotonically growing so that we can detect evictions.
    pub spk_txids: HashMap<ElectrumScriptHash, BTreeSet<Txid>>,

    pub txs: HashMap<Txid, Arc<Transaction>>,

    /// Written as a sequence: a `(Txid, BlockHash)` key is not a string, so a map would be
    /// unserializable in JSON and every other format that requires string keys.
    #[serde(with = "persist::anchors_as_seq")]
    pub anchors: HashMap<(Txid, BlockHash), ConfirmationBlockTime>,
}

impl Cache {
    pub fn resolve_headers_query(
        &mut self,
        req: request::Headers,
        resp: response::HeadersResp,
    ) -> impl Iterator<Item = (u32, Header)> {
        self.headers
            .extend(resp.headers.iter().map(|&h| (h.block_hash(), h)));
        (req.start_height..).zip(resp.headers)
    }

    pub fn resolve_history_query(
        &mut self,
        req: request::GetHistory,
        resp: Vec<response::Tx>,
    ) -> Option<ElectrumScriptStatus> {
        let status_opt = ElectrumScriptStatus::from_history(&resp);
        if let Some(status) = status_opt {
            self.tx_cache
                .spk_txids
                .entry(req.script_hash)
                .or_default()
                .extend(resp.iter().map(|tx| tx.txid()));
            self.subscriptions.insert_spk(req.script_hash, status, resp);
        } else {
            self.subscriptions.remove_spk(req.script_hash);
        }
        status_opt
    }
}

/// The last history the server reported for each script hash.
///
/// Unlike [`TxCache`], a caller cannot rebuild this from wallet data: a status is a hash Electrum
/// computes over the history it stands for and no wallet stores, and the server reports a history
/// as it stands now, never again mentioning a transaction it has dropped.
///
/// Fields are private: a status is a hash of the history it stands for, and letting the two be
/// set independently would reintroduce the desync the type exists to prevent.
#[derive(Debug, Clone, Default)]
pub struct Subscriptions {
    /// The last reported status for a given script.
    spk_hash_to_status: HashMap<ElectrumScriptHash, ElectrumScriptStatus>,
    /// Script history by status.
    ///
    /// An entry is dropped once no script answers to its status, which costs a scan of
    /// `spk_hash_to_status` on every insert. Fine for a wallet's worth of scripts; a refcount
    /// would be the fix if that ever stops being true.
    spk_status_to_history: HashMap<ElectrumScriptStatus, Vec<response::Tx>>,
}

impl Subscriptions {
    fn clear_history_if_no_longer_needed(&mut self, old_status: ElectrumScriptStatus) {
        // A status is a hash of the history it stands for, so two scripts paid by the same
        // transactions and nothing else share one. The history is only dead once no script
        // answers to it any more — dropping it while another still does would leave that
        // script with no history and no notification coming to rebuild it.
        let still_wanted = self
            .spk_hash_to_status
            .values()
            .any(|&status| status == old_status);
        if !still_wanted {
            self.spk_status_to_history.remove(&old_status);
        }
    }

    /// Drop the history for `spk_hash`, for when the server stops reporting one.
    pub fn remove_spk(&mut self, spk_hash: ElectrumScriptHash) {
        if let Some(old_status) = self.spk_hash_to_status.remove(&spk_hash) {
            self.clear_history_if_no_longer_needed(old_status);
        }
    }

    /// Record `history` as the answer to `spk_status`, replacing whatever `spk_hash` had before.
    pub fn insert_spk(
        &mut self,
        spk_hash: ElectrumScriptHash,
        spk_status: ElectrumScriptStatus,
        history: Vec<response::Tx>,
    ) {
        if let Some(old_status) = self.spk_hash_to_status.insert(spk_hash, spk_status) {
            self.clear_history_if_no_longer_needed(old_status);
        }
        self.spk_status_to_history.insert(spk_status, history);
    }

    /// The history for `spk_hash`, but only if it is the one `spk_status` stands for.
    ///
    /// A job fetches for the status its notification carried, so handing it a history that
    /// answers an older status would have it finish on stale data. `None` sends it to the
    /// server instead, which is why the status is effectively part of the key.
    pub fn spk_history(&self, spk_status: ElectrumScriptStatus) -> Option<&[response::Tx]> {
        self.spk_status_to_history
            .get(&spk_status)
            .map(Vec::as_slice)
    }

    pub fn spk_histories<'a>(
        &'a self,
        spk_status: impl IntoIterator<Item = ElectrumScriptStatus> + 'a,
    ) -> impl Iterator<Item = response::Tx> + 'a {
        spk_status
            .into_iter()
            .filter_map(|spk_status| self.spk_history(spk_status))
            .flatten()
            .filter({
                // Two scripts in the same transaction would otherwise yield it twice.
                let mut dedup = BTreeSet::new();
                move |tx: &&response::Tx| dedup.insert(tx.txid())
            })
            .cloned()
    }

    /// The last status the server reported for `spk_hash`, if it still has a history.
    pub fn spk_status(&self, spk_hash: ElectrumScriptHash) -> Option<ElectrumScriptStatus> {
        self.spk_hash_to_status.get(&spk_hash).copied()
    }

    /// The last status reported for every script that still has a history.
    ///
    /// This, rather than whichever jobs are currently in flight, is the set of scripts an
    /// update has to anchor: a reorg moves transactions the server will never mention again,
    /// because one that keeps its height keeps its status.
    pub fn spk_statuses(&self) -> impl Iterator<Item = ElectrumScriptStatus> + '_ {
        self.spk_hash_to_status.values().copied()
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

    /// Written as `spk_hash -> (status, history)`, which is what rebuilds both maps on the way
    /// back in.
    impl serde::Serialize for Subscriptions {
        fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
            serializer.collect_map(self.spk_hash_to_status.iter().map(|(&spk_hash, &status)| {
                let history = self
                    .spk_status_to_history
                    .get(&status)
                    .map(|history| history.iter().map(HistoryTx::from).collect::<Vec<_>>())
                    .unwrap_or_default();
                (spk_hash, (status, history))
            }))
        }
    }

    impl<'de> serde::Deserialize<'de> for Subscriptions {
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
                spk_histories.insert_spk(spk_hash, status, history);
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

    /// `response::Tx` derives `Deserialize` only, so histories round-trip through `HistoryTx`.
    /// Both of its variants have to survive the trip intact.
    #[test]
    fn spk_histories_round_trip() {
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

        let mut before = Subscriptions::default();
        before.insert_spk(spk_hash(9), status, history);

        let json = serde_json::to_string(&before).expect("must serialize");
        let after: Subscriptions = serde_json::from_str(&json).expect("must deserialize");

        assert_eq!(
            after.spk_history(status).map(<[_]>::len),
            Some(2),
            "the history must survive, and still answer to its status"
        );
        assert_eq!(after.spk_status(spk_hash(9)), Some(status));
    }

    /// Two scripts paid by the same transaction, and nothing else, have identical histories —
    /// so they share a status. One of them moving on must not take the other's history with it.
    #[test]
    fn a_shared_history_survives_one_of_its_scripts_moving_on() {
        let shared = vec![response::Tx::Confirmed(response::ConfirmedTx {
            txid: txid(1),
            height: bitcoin::absolute::Height::from_consensus(700_000).unwrap(),
        })];
        let shared_status = ElectrumScriptStatus::from_history(&shared).expect("not empty");

        let mut subs = Subscriptions::default();
        subs.insert_spk(spk_hash(1), shared_status, shared.clone());
        subs.insert_spk(spk_hash(2), shared_status, shared);

        // Script 1 sees another transaction, so its status moves on. Script 2's has not changed,
        // and no notification is coming for it.
        let moved_on = vec![
            response::Tx::Confirmed(response::ConfirmedTx {
                txid: txid(1),
                height: bitcoin::absolute::Height::from_consensus(700_000).unwrap(),
            }),
            response::Tx::Confirmed(response::ConfirmedTx {
                txid: txid(2),
                height: bitcoin::absolute::Height::from_consensus(700_001).unwrap(),
            }),
        ];
        let moved_status = ElectrumScriptStatus::from_history(&moved_on).expect("not empty");
        subs.insert_spk(spk_hash(1), moved_status, moved_on);

        assert_eq!(subs.spk_status(spk_hash(2)), Some(shared_status));
        assert!(
            subs.spk_history(shared_status).is_some(),
            "script 2 still answers to the shared status, so its history must still be there"
        );
    }

    /// `anchors` is keyed by a tuple, which JSON cannot use as a map key. A caller who chooses
    /// to persist a [`TxCache`] rather than rebuild it must still be able to.
    #[test]
    fn tx_cache_round_trips_through_json() {
        let anchor = (txid(1), bitcoin::BlockHash::from_byte_array([2; 32]));
        let mut before = TxCache::default();
        before
            .anchors
            .insert(anchor, ConfirmationBlockTime::default());

        let json = serde_json::to_string(&before).expect("must serialize");
        let after: TxCache = serde_json::from_str(&json).expect("must deserialize");

        assert_eq!(after.anchors.get(&anchor), before.anchors.get(&anchor));
    }

    /// A `Cache` carries none of it, so persisting one cannot go stale against the wallet.
    #[test]
    fn cache_does_not_persist_the_tx_cache() {
        let mut before = Cache::default();
        before.tx_cache.txs.insert(
            txid(1),
            Arc::new(bitcoin::Transaction {
                version: bitcoin::transaction::Version::ONE,
                lock_time: bitcoin::absolute::LockTime::ZERO,
                input: Vec::new(),
                output: Vec::new(),
            }),
        );

        let json = serde_json::to_string(&before).expect("must serialize");
        assert!(
            !json.contains(&txid(1).to_string()),
            "the wallet's own data must not be written here: {json}"
        );
        let after: Cache = serde_json::from_str(&json).expect("must deserialize");
        assert!(after.tx_cache.txs.is_empty());
    }
}
