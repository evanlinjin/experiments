use std::{
    collections::{BTreeSet, HashMap},
    sync::Arc,
};

use bdk_core::bitcoin::{BlockHash, ScriptBuf, Transaction, Txid};
use electrum_streaming_client::{response, ElectrumScriptHash, ElectrumScriptStatus};

use crate::ProvenAnchor;

/// Everything learned from the server, so a job knows what it need not ask for again.
///
/// Not persisted: every part of it is already in the caller's wallet — rebuild all of it with
/// [`Cache::from_wallet_txs`] — and a stored copy would only give the two something to disagree
/// about.
#[derive(Debug, Clone, Default)]
pub struct Cache {
    /// The server's per-script histories.
    pub subscriptions: Subscriptions,

    /// Every txid ever seen for each script hash.
    ///
    /// This is monotonically growing so that we can detect evictions.
    pub spk_txids: HashMap<ElectrumScriptHash, BTreeSet<Txid>>,

    pub txs: HashMap<Txid, Arc<Transaction>>,

    pub anchors: HashMap<(Txid, BlockHash), ProvenAnchor>,
}

/// A transaction's chain position, as a wallet already tracks it — enough to describe the
/// history a script had, without asking the server.
#[derive(Debug, Clone)]
pub enum TxConfirmationStatus {
    /// Confirmed, with the proof a job would otherwise have to ask the server for again.
    ///
    /// `anchor.pos` plays no part in the status hash itself — only `txid` and `height` are
    /// hashed — but a server orders a history by height *and then block position* (per the
    /// [protocol]), so two transactions confirmed in the same block hash to a different status
    /// if their order is wrong.
    ///
    /// [protocol]: https://electrum-protocol.readthedocs.io/en/latest/protocol-basics.html#status
    Confirmed(ProvenAnchor),
    Mempool {
        confirmed_inputs: bool,
    },
}

impl Cache {
    /// Rebuild everything from what a wallet already knows: [`subscriptions`](Self::subscriptions),
    /// `spk_txids`, `txs` and `anchors`. A reconnect need not download every script's history,
    /// refetch its transactions, or reprove its anchors — only whatever actually changed.
    ///
    /// No `bdk_chain` dependency needed: `txs` is whatever a wallet's tx graph or index already
    /// hands over as `(tx, status, relevant_scripts)` — one entry per transaction, not one per
    /// script it pays, since that is how a wallet holds them.
    pub fn from_wallet_txs<Tx, Spks>(
        txs: impl IntoIterator<Item = (Tx, TxConfirmationStatus, Spks)>,
    ) -> Self
    where
        Tx: Into<Arc<Transaction>>,
        Spks: IntoIterator<Item = ScriptBuf>,
    {
        let mut cache = Cache::default();

        // Sorted on later, by (height, block position) — carried alongside the response `Tx`
        // since the wire type itself has nowhere to put it.
        let mut by_spk_hash = HashMap::<ElectrumScriptHash, Vec<(response::Tx, usize)>>::new();
        for (tx, status, relevant_scripts) in txs {
            let tx = tx.into();
            let txid = tx.compute_txid();
            let (history_tx, sort_pos) = match status {
                TxConfirmationStatus::Confirmed(anchor) => {
                    let history_tx = response::Tx::Confirmed(response::ConfirmedTx {
                        txid,
                        height: bdk_core::bitcoin::absolute::Height::from_consensus(
                            anchor.block_id.height,
                        )
                        .expect("confirmed tx must have a valid height"),
                    });
                    let pos = anchor.pos;
                    cache.anchors.insert((txid, anchor.block_id.hash), anchor);
                    (history_tx, pos)
                }
                TxConfirmationStatus::Mempool { confirmed_inputs } => (
                    response::Tx::Mempool(response::MempoolTx {
                        txid,
                        fee: bdk_core::bitcoin::Amount::ZERO,
                        confirmed_inputs,
                    }),
                    0,
                ),
            };
            cache.txs.insert(txid, tx);
            for spk in relevant_scripts {
                by_spk_hash
                    .entry(ElectrumScriptHash::new(&spk))
                    .or_default()
                    .push((history_tx.clone(), sort_pos));
            }
        }

        for (spk_hash, mut entries) in by_spk_hash {
            // The order a server reports a history in, and the order its status hash is
            // computed over: confirmed ascending by height then block position, then mempool
            // ordered by confirmed-inputs before not, each tied by txid.
            entries.sort_by_key(|(tx, pos)| match tx {
                response::Tx::Confirmed(tx) => (0u8, tx.height.to_consensus_u32(), *pos, tx.txid),
                response::Tx::Mempool(tx) if tx.confirmed_inputs => (1, 0, 0, tx.txid),
                response::Tx::Mempool(tx) => (2, 0, 0, tx.txid),
            });
            let history = entries.into_iter().map(|(tx, _)| tx).collect::<Vec<_>>();
            cache
                .spk_txids
                .entry(spk_hash)
                .or_default()
                .extend(history.iter().map(response::Tx::txid));
            if let Some(status) = ElectrumScriptStatus::from_history(&history) {
                cache.subscriptions.insert_spk(spk_hash, status, history);
            }
        }
        cache
    }
}

/// The last history the server reported for each script hash.
///
/// A caller cannot rebuild this from wallet data alone: a status is a hash Electrum computes
/// over the history it stands for and no wallet stores, so [`Cache::from_wallet_txs`] computes
/// it the same way Electrum does.
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

#[cfg(test)]
mod test {
    use super::*;
    use bdk_core::bitcoin::{self, hashes::Hash};

    fn txid(byte: u8) -> Txid {
        Txid::from_byte_array([byte; 32])
    }

    fn spk_hash(byte: u8) -> ElectrumScriptHash {
        ElectrumScriptHash::from_byte_array([byte; 32])
    }

    /// A transaction whose txid varies with `unique`, so distinct calls yield distinct txids.
    fn transaction(unique: u32) -> Transaction {
        Transaction {
            version: bitcoin::transaction::Version::ONE,
            lock_time: bitcoin::absolute::LockTime::from_consensus(unique),
            input: Vec::new(),
            output: Vec::new(),
        }
    }

    fn anchor(height: u32, pos: usize) -> ProvenAnchor {
        ProvenAnchor {
            block_id: bdk_core::BlockId {
                height,
                hash: BlockHash::from_byte_array([height as u8; 32]),
            },
            pos,
            merkle: Vec::new(),
        }
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

    /// A caller's wallet knows which spk each of its transactions paid, the transaction itself,
    /// and whether it is confirmed (with its anchor) or still in the mempool — enough to
    /// rebuild the transaction cache and compute the same status an Electrum server would,
    /// without ever asking it.
    #[test]
    fn from_wallet_txs_rebuilds_a_status_the_server_would_recognise() {
        let spk = ScriptBuf::from_hex("0014000000000000000000000000000000000000000a").unwrap();
        let spk_hash = ElectrumScriptHash::new(&spk);
        let confirmed_tx = transaction(1);
        let mempool_tx = transaction(2);
        let confirmed_txid = confirmed_tx.compute_txid();
        let mempool_txid = mempool_tx.compute_txid();
        let confirmed_anchor = anchor(100, 0);

        let cache = Cache::from_wallet_txs([
            (
                confirmed_tx,
                TxConfirmationStatus::Confirmed(confirmed_anchor.clone()),
                vec![spk.clone()],
            ),
            (
                mempool_tx,
                TxConfirmationStatus::Mempool {
                    confirmed_inputs: true,
                },
                vec![spk],
            ),
        ]);

        assert_eq!(
            cache.spk_txids.get(&spk_hash).map(BTreeSet::len),
            Some(2),
            "both txids must be recorded against the spk"
        );
        assert_eq!(
            cache.txs.keys().copied().collect::<BTreeSet<_>>(),
            BTreeSet::from([confirmed_txid, mempool_txid]),
            "the transactions themselves must be cached too"
        );
        assert_eq!(
            cache
                .anchors
                .get(&(confirmed_txid, confirmed_anchor.block_id.hash)),
            Some(&confirmed_anchor),
            "the confirmed transaction's anchor must be cached, needing no reproof"
        );

        let expected = ElectrumScriptStatus::from_history(&[
            response::Tx::Confirmed(response::ConfirmedTx {
                txid: confirmed_txid,
                height: bitcoin::absolute::Height::from_consensus(100).unwrap(),
            }),
            response::Tx::Mempool(response::MempoolTx {
                txid: mempool_txid,
                fee: bitcoin::Amount::ZERO,
                confirmed_inputs: true,
            }),
        ])
        .expect("history is not empty");
        assert_eq!(
            cache.subscriptions.spk_status(spk_hash),
            Some(expected),
            "the rebuilt status must match what a server hashing the same history would report"
        );
    }

    /// The status hash itself only ever mixes in `txid:height:` — never a block position — so
    /// two transactions confirmed in the same block only get the right status if `pos` orders
    /// them the way the block does. Get it backwards and the computed status is simply wrong.
    #[test]
    fn same_block_transactions_are_ordered_by_block_position() {
        let spk = ScriptBuf::from_hex("0014000000000000000000000000000000000000000a").unwrap();
        let spk_hash = ElectrumScriptHash::new(&spk);
        let (first_tx, second_tx) = (transaction(1), transaction(2));
        let (first, second) = (first_tx.compute_txid(), second_tx.compute_txid());
        let height = bitcoin::absolute::Height::from_consensus(100).unwrap();

        let cache = Cache::from_wallet_txs([
            (
                first_tx,
                TxConfirmationStatus::Confirmed(anchor(100, 0)),
                vec![spk.clone()],
            ),
            (
                second_tx,
                TxConfirmationStatus::Confirmed(anchor(100, 1)),
                vec![spk],
            ),
        ]);

        let in_block_order = ElectrumScriptStatus::from_history(&[
            response::Tx::Confirmed(response::ConfirmedTx {
                txid: first,
                height,
            }),
            response::Tx::Confirmed(response::ConfirmedTx {
                txid: second,
                height,
            }),
        ])
        .expect("history is not empty");
        let reversed = ElectrumScriptStatus::from_history(&[
            response::Tx::Confirmed(response::ConfirmedTx {
                txid: second,
                height,
            }),
            response::Tx::Confirmed(response::ConfirmedTx {
                txid: first,
                height,
            }),
        ])
        .expect("history is not empty");
        assert_ne!(
            in_block_order, reversed,
            "the hash must actually be order-sensitive, or this test proves nothing"
        );

        assert_eq!(
            cache.subscriptions.spk_status(spk_hash),
            Some(in_block_order),
            "same-height entries must be ordered by their position in the block, not insertion \
             order, or the rebuilt status will not match what a server reports"
        );
    }

    /// A wallet holds each of its transactions once, not once per script it happens to pay — a
    /// single entry naming every relevant script must still update every one of them.
    #[test]
    fn one_transaction_can_update_more_than_one_script() {
        let spk_a = ScriptBuf::from_hex("0014000000000000000000000000000000000000000a").unwrap();
        let spk_b = ScriptBuf::from_hex("0014000000000000000000000000000000000000000b").unwrap();
        let tx = transaction(1);
        let txid = tx.compute_txid();

        let cache = Cache::from_wallet_txs([(
            tx,
            TxConfirmationStatus::Confirmed(anchor(100, 0)),
            vec![spk_a.clone(), spk_b.clone()],
        )]);

        for spk in [spk_a, spk_b] {
            let spk_hash = ElectrumScriptHash::new(&spk);
            assert_eq!(
                cache.spk_txids.get(&spk_hash),
                Some(&BTreeSet::from([txid])),
                "the tx must be recorded against every script it pays"
            );
            assert!(
                cache.subscriptions.spk_status(spk_hash).is_some(),
                "and every script must get a status to subscribe with"
            );
        }
    }
}
