use std::collections::BTreeSet;

use bdk_core::{
    bitcoin::{OutPoint, Txid},
    CheckPoint, ConfirmationBlockTime, TxUpdate,
};
use electrum_streaming_client::{request, response, ElectrumScriptHash, ElectrumScriptStatus};

use crate::{req::ReqQueuer, Cache};

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

#[derive(Debug)]
pub struct SpkJob {
    /// Time that we got this notification.
    pub start_epoch: u64,
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
        let start_epoch = std::time::UNIX_EPOCH
            .elapsed()
            .expect("must get time")
            .as_secs();
        let mut tx_update = TxUpdate::default();

        let stage = match spk_status {
            Some(status) => SpkJobStage::ProcessingHistory { status },
            None => {
                if let Some(prev_txids) = cache.spk_txids.get(&spk_hash) {
                    tx_update
                        .evicted_ats
                        .extend(prev_txids.iter().map(|&txid| (txid, start_epoch)));
                }
                SpkJobStage::done()
            }
        };

        Self {
            start_epoch,
            spk_hash,
            stage,
            tx_update,
        }
    }

    /// Try fullfill all that is missing.
    pub fn advance(mut self, queuer: &mut ReqQueuer, cache: &Cache, cp: &CheckPoint) -> Self {
        let mut made_progress = true;
        while made_progress {
            (self, made_progress) = self.try_advance_once(queuer, cache, cp.clone());
            let stage_str = match &self.stage {
                SpkJobStage::ProcessingHistory { status } => format!("ProcessingHistory({status})"),
                SpkJobStage::ProcessingTxsAndAnchors { txs, anchors } => {
                    let inner_str = match txs {
                        Some(TxsJobStage::Txs(txids)) => format!("txs = {}", txids.len()),
                        Some(TxsJobStage::Prevouts(ops)) => format!("ops = {}", ops.len()),
                        None => "tx_done".to_string(),
                    };
                    format!(
                        "ProcessingTxsAndAnchors({inner_str}, anchors={})",
                        anchors.len()
                    )
                }
            };
            tracing::debug!(stage = stage_str, "Spk Job: Made progress.");
        }
        self
    }

    pub fn try_finish(&mut self) -> Option<(ElectrumScriptHash, TxUpdate<ConfirmationBlockTime>)> {
        if self.stage.is_done() {
            tracing::trace!("Spk Job: Not finished yet.");
            Some((self.spk_hash, core::mem::take(&mut self.tx_update)))
        } else {
            tracing::info!("Spk Job: Finished.");
            None
        }
    }

    /// Try fullfill all that is missing.
    ///
    /// Returns self + bool representing whether we did advance.
    fn try_advance_once(
        mut self,
        queuer: &mut ReqQueuer,
        cache: &Cache,
        tip: CheckPoint,
    ) -> (Self, bool) {
        match self.stage {
            SpkJobStage::ProcessingHistory { status } => match cache.spk_histories.get(&status) {
                Some(history) => {
                    if let Some(prev_txids) = cache.spk_txids.get(&self.spk_hash) {
                        let these_txids =
                            history.iter().map(|tx| tx.txid()).collect::<BTreeSet<_>>();
                        let to_evict = prev_txids
                            .difference(&these_txids)
                            .map(|&txid| (txid, self.start_epoch));
                        self.tx_update.evicted_ats.extend(to_evict);
                    }
                    for tx in history {
                        if let response::Tx::Mempool(tx) = tx {
                            self.tx_update.seen_ats.insert((tx.txid, self.start_epoch));
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
                    if height > tip.height() {
                        return false;
                    }

                    let blockhash = match tip.get(height) {
                        Some(cp) if cp.height() == height => cp.hash(),
                        _ => {
                            queuer.enqueue(request::Header { height });
                            return true;
                        }
                    };

                    if !cache.headers.contains_key(&blockhash) {
                        queuer.enqueue(request::Header { height });
                    }

                    if let Some(anchor) = cache.anchors.get(&(txid, blockhash)) {
                        self.tx_update.anchors.insert((*anchor, txid));
                        return false;
                    };
                    if cache.failed_anchors.contains(&(txid, blockhash)) {
                        return false;
                    }

                    queuer.enqueue(request::GetTxMerkle { txid, height });
                    true
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
