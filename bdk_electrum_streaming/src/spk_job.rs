use std::{
    collections::BTreeSet,
    time::{Duration, UNIX_EPOCH},
};

use bdk_core::{
    bitcoin::{OutPoint, Txid},
    BlockId, ConfirmationBlockTime, TxUpdate,
};
use electrum_streaming_client::{request, response, ElectrumScriptHash, ElectrumScriptStatus};

use crate::{req::ReqQueuer, Cache, Observed};

#[derive(Debug)]
pub enum SpkJobStage {
    ProcessingHistory {
        /// The status for which we are fetching.
        status: ElectrumScriptStatus,
    },
    ProcessingTxsAndAnchors {
        txs: Option<TxsJobStage>,
        anchors: BTreeSet<(u32, Txid)>,
    },
}

impl SpkJobStage {
    pub fn done() -> Self {
        Self::ProcessingTxsAndAnchors {
            txs: None,
            anchors: BTreeSet::new(),
        }
    }

    /// Whether it's done.
    pub fn is_done(&self) -> bool {
        matches!(self, SpkJobStage::ProcessingTxsAndAnchors { txs, anchors } if txs.is_none() && anchors.is_empty())
    }
}

#[derive(Debug)]
pub enum TxsJobStage {
    Txs(BTreeSet<Txid>),
    Prevouts(BTreeSet<OutPoint>),
}

impl TxsJobStage {
    pub fn from_missing_txs(txids: impl IntoIterator<Item = Txid>) -> Option<Self> {
        let txids = txids.into_iter().collect::<BTreeSet<_>>();
        if txids.is_empty() {
            None
        } else {
            Some(Self::Txs(txids))
        }
    }

    pub fn from_missing_prev_txs(outpoints: impl IntoIterator<Item = OutPoint>) -> Option<Self> {
        let prev_txs = outpoints.into_iter().collect::<BTreeSet<_>>();
        if prev_txs.is_empty() {
            None
        } else {
            Some(Self::Prevouts(prev_txs))
        }
    }
}

/// The job to perform once we receive a script status notification.
#[derive(Debug)]
pub struct SpkJob {
    /// Time that we got this notification.
    pub start: Duration,
    /// Script hash of this notification.
    pub spk_hash: ElectrumScriptHash,

    pub stage: SpkJobStage,

    /// Staged tx update.
    pub tx_update: TxUpdate<ConfirmationBlockTime>,
}

impl SpkJob {
    pub fn new(
        cache: &Cache,
        spk_hash: ElectrumScriptHash,
        spk_status: Option<ElectrumScriptStatus>,
    ) -> Self {
        let start = UNIX_EPOCH.elapsed().expect("must get unix time");
        let mut tx_update = TxUpdate::default();

        let stage = match spk_status {
            Some(status) => SpkJobStage::ProcessingHistory { status },
            None => {
                if let Some(prev_txids) = cache.spk_txids.get(&spk_hash) {
                    tx_update
                        .evicted_ats
                        .extend(prev_txids.iter().map(|&txid| (txid, start.as_secs())));
                }
                SpkJobStage::done()
            }
        };

        Self {
            start,
            spk_hash,
            stage,
            tx_update,
        }
    }

    pub fn elapsed_seconds(&self) -> String {
        let duration = UNIX_EPOCH.elapsed().expect("must get current timestamp") - self.start;
        let seconds = duration.as_secs();
        let subsec = duration.subsec_millis();
        format!("{seconds}s {subsec}ms")
    }

    /// Try fullfill all that is missing.
    pub fn advance(mut self, queuer: &mut ReqQueuer, cache: &mut Cache) -> Self {
        let mut made_progress = true;
        while made_progress {
            (self, made_progress) = self.try_advance_once(queuer, cache);
            let stage_str = match &self.stage {
                SpkJobStage::ProcessingHistory { status } => format!("ProcessingHistory({status})"),
                SpkJobStage::ProcessingTxsAndAnchors { txs, anchors } => {
                    let inner_str = match txs {
                        Some(TxsJobStage::Txs(txids)) => format!("txs = {}", txids.len()),
                        Some(TxsJobStage::Prevouts(ops)) => format!("prevouts = {}", ops.len()),
                        None => "tx_done".to_string(),
                    };
                    format!(
                        "ProcessingTxsAndAnchors({inner_str}, anchors = {})",
                        anchors.len()
                    )
                }
            };
            tracing::trace!(
                elapsed_seconds = self.elapsed_seconds(),
                spk_hash = self.spk_hash.to_string(),
                stage = stage_str,
                "Spk job progress"
            );
        }
        self
    }

    pub fn try_finish(&mut self) -> Option<(ElectrumScriptHash, TxUpdate<ConfirmationBlockTime>)> {
        if self.stage.is_done() {
            tracing::trace!(
                elapsed_seconds = self.elapsed_seconds(),
                spk_hash = self.spk_hash.to_string(),
                "Spk job not finished"
            );
            Some((self.spk_hash, core::mem::take(&mut self.tx_update)))
        } else {
            tracing::info!(
                elapsed_seconds = self.elapsed_seconds(),
                spk_hash = self.spk_hash.to_string(),
                "Spk job finished"
            );
            None
        }
    }

    /// Try fullfill all that is missing.
    ///
    /// Returns self + bool representing whether we did advance.
    fn try_advance_once(mut self, queuer: &mut ReqQueuer, cache: &mut Cache) -> (Self, bool) {
        match self.stage {
            SpkJobStage::ProcessingHistory { status } => match cache.spk_histories.get(&status) {
                Some(history) => {
                    if let Some(prev_txids) = cache.spk_txids.get(&self.spk_hash) {
                        let these_txids =
                            history.iter().map(|tx| tx.txid()).collect::<BTreeSet<_>>();
                        let to_evict = prev_txids
                            .difference(&these_txids)
                            .map(|&txid| (txid, self.start.as_secs()));
                        self.tx_update.evicted_ats.extend(to_evict);
                    }
                    for tx in history {
                        if let response::Tx::Mempool(tx) = tx {
                            self.tx_update
                                .seen_ats
                                .insert((tx.txid, self.start.as_secs()));
                        }
                    }

                    let txs = TxsJobStage::from_missing_txs(history.iter().map(|tx| tx.txid()));
                    let anchors = history
                        .iter()
                        .filter_map(|tx| {
                            let height = tx.confirmation_height()?.to_consensus_u32();
                            Some((height, tx.txid()))
                        })
                        .collect();
                    self.stage = SpkJobStage::ProcessingTxsAndAnchors { txs, anchors };
                    (self, true)
                }
                None => {
                    let script_hash = self.spk_hash;
                    queuer.enqueue(request::GetHistory { script_hash });
                    (self, false)
                }
            },
            SpkJobStage::ProcessingTxsAndAnchors {
                mut txs,
                mut anchors,
            } => {
                let mut made_progress = false;
                txs = match txs {
                    Some(TxsJobStage::Txs(mut missing_txs)) => {
                        missing_txs.retain(|txid| match cache.txs.get(txid) {
                            Some(tx) => {
                                self.tx_update.txs.push(tx.clone());
                                false
                            }
                            None => {
                                let txid = *txid;
                                queuer.enqueue(request::GetTx { txid });
                                true
                            }
                        });
                        if missing_txs.is_empty() {
                            made_progress = true;
                            TxsJobStage::from_missing_prev_txs(
                                self.tx_update
                                    .txs
                                    .iter()
                                    .filter(|tx| !tx.is_coinbase())
                                    .flat_map(|tx| tx.input.iter())
                                    .map(|txin| txin.previous_output),
                            )
                        } else {
                            Some(TxsJobStage::Txs(missing_txs))
                        }
                    }
                    Some(TxsJobStage::Prevouts(mut missing_prevouts)) => {
                        missing_prevouts.retain(|op| match cache.txs.get(&op.txid) {
                            Some(tx) => {
                                let txout = match tx.output.get(op.vout as usize) {
                                    Some(txout) => txout,
                                    None => {
                                        debug_assert!(false, "Output must exist in tx");
                                        unimplemented!("Handle this error");
                                    }
                                };
                                self.tx_update.txouts.insert(*op, txout.clone());
                                false
                            }
                            None => {
                                let txid = op.txid;
                                queuer.enqueue(request::GetTx { txid });
                                true
                            }
                        });
                        if missing_prevouts.is_empty() {
                            made_progress = true;
                            None
                        } else {
                            Some(TxsJobStage::Prevouts(missing_prevouts))
                        }
                    }
                    None => None,
                };

                let anchors_start_count = anchors.len();
                anchors.retain(|&(height, txid)| {
                    match resolve_anchor(queuer, cache, height, txid) {
                        AnchorStep::Pending => true,
                        AnchorStep::Resolved(anchor) => {
                            self.tx_update.anchors.insert((anchor, txid));
                            false
                        }
                        AnchorStep::Abandoned => false,
                    }
                });
                if anchors.len() < anchors_start_count {
                    made_progress = true;
                }

                self.stage = SpkJobStage::ProcessingTxsAndAnchors { txs, anchors };
                (self, made_progress)
            }
        }
    }
}

/// How far one pending anchor got in a single pass.
#[derive(Debug)]
pub enum AnchorStep {
    /// A request went out; this anchor needs another pass.
    Pending,
    /// Verified against the header fetched for its height.
    Resolved(ConfirmationBlockTime),
    /// Nothing usable this round. Whatever next re-asks for this anchor starts it afresh;
    /// retrying in place would spin, because the server is free to keep answering the same way.
    Abandoned,
}

/// Take one step towards "txid is in the block at `height`".
///
/// The chain is not consulted at all. A proof carries no block hash, so it means nothing except
/// against a specific header — that header is fetched first and this returns [`AnchorStep::Pending`],
/// because asking for both at once is a race the proof can win, leaving nothing to check it
/// against.
pub fn resolve_anchor(
    queuer: &mut ReqQueuer,
    cache: &mut Cache,
    height: u32,
    txid: Txid,
) -> AnchorStep {
    let epoch = cache.eviction_epoch;
    let header = match cache.headers_at.get(&height) {
        Some(&Observed::Seen(header)) => header,
        Some(Observed::Absent) => {
            // The server had no header there. Consumed rather than kept: it is a JSON-RPC error,
            // which says nothing durable, and a later re-ask must be free to try again.
            cache.headers_at.remove(&height);
            return AnchorStep::Abandoned;
        }
        // Enqueued even when a request is already outstanding: `ReqCoord` merges the two, and
        // that merge is how this job registers as a second consumer of the one response. The
        // stamp is left as it was, because it records when the request went out — refreshing it
        // would let a reorg that landed since pass for one that has not happened yet.
        Some(Observed::Awaiting(_)) | None => {
            cache
                .headers_at
                .entry(height)
                .or_insert(Observed::Awaiting(epoch));
            queuer.enqueue(request::Header { height });
            return AnchorStep::Pending;
        }
    };
    let hash = header.block_hash();

    if let Some(anchor) = cache.anchors.get(&(txid, hash)).copied() {
        return AnchorStep::Resolved(anchor);
    }

    let expected = match cache.proofs.get(&(txid, height)) {
        Some(Observed::Seen(proof)) => proof.expected_merkle_root(txid),
        Some(Observed::Absent) => {
            cache.proofs.remove(&(txid, height));
            return AnchorStep::Abandoned;
        }
        Some(Observed::Awaiting(_)) | None => {
            cache
                .proofs
                .entry((txid, height))
                .or_insert(Observed::Awaiting(epoch));
            queuer.enqueue(request::GetTxMerkle { txid, height });
            return AnchorStep::Pending;
        }
    };

    if header.merkle_root == expected {
        // "txid is in this block" — true once, true forever, and keyed by the block's hash
        // rather than a height that can come to mean another block.
        let anchor = ConfirmationBlockTime {
            block_id: BlockId { height, hash },
            confirmation_time: header.time as u64,
        };
        cache.anchors.insert((txid, hash), anchor);
        cache.anchored_at.entry(height).or_default().insert(txid);
        // The fact supersedes the evidence: `anchors` is consulted before `proofs` and is keyed by
        // a block hash rather than a height, so nothing reads this proof again.
        cache.proofs.remove(&(txid, height));
        return AnchorStep::Resolved(anchor);
    }

    // A non-match is ambiguous and so is never recorded as a verdict: it cannot tell "the tx is
    // not in that block" from "the header we hold for this height is one the server has since
    // left". Discard both observations so a later attempt fetches them afresh, and give up on
    // this anchor for now rather than holding the whole job open waiting on it.
    tracing::debug!(
        txid = txid.to_string(),
        height,
        block_hash = hash.to_string(),
        "Proof does not match the header held for this height"
    );
    cache.headers_at.remove(&height);
    cache.proofs.remove(&(txid, height));
    AnchorStep::Abandoned
}
