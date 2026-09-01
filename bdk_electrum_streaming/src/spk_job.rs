use std::{
    collections::BTreeSet,
    time::{Duration, UNIX_EPOCH},
};

use bdk_core::{
    bitcoin::{OutPoint, Txid},
    ConfirmationBlockTime, TxUpdate,
};
use electrum_streaming_client::{request, response, ElectrumScriptHash, ElectrumScriptStatus};

use crate::{req::ReqQueuer, Cache};

/// Where a [`SpkJob`] has got to.
///
/// Each stage names what the job is still waiting for, so being finished is a stage of its own
/// rather than the absence of one.
#[derive(Debug)]
pub enum SpkStage {
    /// Waiting on the history the status stands for.
    ProcessingHistory { status: ElectrumScriptStatus },
    /// Waiting on the transactions that history named.
    ProcessingTxs(BTreeSet<Txid>),
    /// Waiting on the outputs those transactions spend.
    ProcessingPrevouts(BTreeSet<OutPoint>),
    /// Everything the job asked for has arrived.
    Done,
}

impl SpkStage {
    /// What follows a history, given every txid it named.
    fn from_txids(txids: impl IntoIterator<Item = Txid>) -> Self {
        let txids = txids.into_iter().collect::<BTreeSet<_>>();
        if txids.is_empty() {
            Self::Done
        } else {
            Self::ProcessingTxs(txids)
        }
    }

    /// What follows the transactions, given every output they spend.
    fn from_prevouts(outpoints: impl IntoIterator<Item = OutPoint>) -> Self {
        let prevouts = outpoints.into_iter().collect::<BTreeSet<_>>();
        if prevouts.is_empty() {
            Self::Done
        } else {
            Self::ProcessingPrevouts(prevouts)
        }
    }

    pub fn is_done(&self) -> bool {
        matches!(self, SpkStage::Done)
    }
}

/// What one [`SpkJob::poll`] achieved.
#[derive(Debug)]
pub enum SpkProgress {
    /// A stage completed; poll again.
    Continue,
    /// Waiting on the server.
    Blocked,
    /// Everything asked for has arrived. Carries what the job gathered, leaving it empty, so a
    /// job polled again after finishing contributes nothing a second time.
    Done(TxUpdate<ConfirmationBlockTime>),
}

/// The job to perform once we receive a script status notification.
///
/// Fetches the script's history, the transactions in it, and the outputs those transactions
/// spend. Anchoring them is [`ConfirmationJob`]'s work: a transaction's anchor depends on the chain,
/// which no single script can move, so resolving anchors per-script had every job racing a
/// tip that only one of them could move.
///
/// [`ConfirmationJob`]: crate::ConfirmationJob
#[derive(Debug)]
pub struct SpkJob {
    /// When the notification that started this job arrived.
    pub start: Duration,
    pub spk_hash: ElectrumScriptHash,

    stage: SpkStage,
    tx_update: TxUpdate<ConfirmationBlockTime>,
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
            Some(status) => SpkStage::ProcessingHistory { status },
            None => {
                if let Some(prev_txids) = cache.tx_cache.spk_txids.get(&spk_hash) {
                    tx_update
                        .evicted_ats
                        .extend(prev_txids.iter().map(|&txid| (txid, start.as_secs())));
                }
                SpkStage::Done
            }
        };

        Self {
            start,
            spk_hash,
            stage,
            tx_update,
        }
    }

    /// The status this job is still waiting on a history for.
    ///
    /// `None` once the history is in hand, or when the script had none to begin with.
    pub fn awaiting_history(&self) -> Option<ElectrumScriptStatus> {
        match self.stage {
            SpkStage::ProcessingHistory { status } => Some(status),
            _ => None,
        }
    }

    /// Whether everything this job asked for has arrived.
    pub fn is_done(&self) -> bool {
        self.stage.is_done()
    }

    pub fn elapsed_seconds(&self) -> String {
        let now = UNIX_EPOCH.elapsed().expect("must get current timestamp");
        // The system clock can step backwards, which must not bring a log line down with it.
        let duration = now.saturating_sub(self.start);
        format!("{}s {}ms", duration.as_secs(), duration.subsec_millis())
    }

    /// Take one step towards having everything the script's history names.
    ///
    /// One step per call, so the caller drives it the same way it drives [`ConfirmationJob`]: poll
    /// until [`SpkProgress::Blocked`] or [`SpkProgress::Done`].
    ///
    /// Errors when the server answers with a transaction that cannot be the one asked for —
    /// its outputs do not reach an outpoint we know is spent. That is the server's picture
    /// disagreeing with itself, so there is nothing to retry against on this connection.
    ///
    /// [`ConfirmationJob`]: crate::ConfirmationJob
    pub fn poll(&mut self, queuer: &mut ReqQueuer, cache: &Cache) -> anyhow::Result<SpkProgress> {
        let progress = match &mut self.stage {
            SpkStage::ProcessingHistory { status } => {
                match cache.subscriptions.spk_history(*status) {
                    Some(history) => {
                        if let Some(prev_txids) = cache.tx_cache.spk_txids.get(&self.spk_hash) {
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
                        self.stage = SpkStage::from_txids(history.iter().map(|tx| tx.txid()));
                        SpkProgress::Continue
                    }
                    None => {
                        queuer.enqueue(request::GetHistory {
                            script_hash: self.spk_hash,
                        });
                        SpkProgress::Blocked
                    }
                }
            }
            SpkStage::ProcessingTxs(missing_txs) => {
                missing_txs.retain(|txid| match cache.tx_cache.txs.get(txid) {
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
                    self.stage = SpkStage::from_prevouts(
                        self.tx_update
                            .txs
                            .iter()
                            .filter(|tx| !tx.is_coinbase())
                            .flat_map(|tx| tx.input.iter())
                            .map(|txin| txin.previous_output),
                    );
                    SpkProgress::Continue
                } else {
                    SpkProgress::Blocked
                }
            }
            SpkStage::ProcessingPrevouts(missing_prevouts) => {
                // `retain` cannot fail, so a bad output is carried out and raised below.
                let mut err = Option::<anyhow::Error>::None;
                missing_prevouts.retain(|op| {
                    let tx = match cache.tx_cache.txs.get(&op.txid) {
                        Some(tx) => tx,
                        None => {
                            let txid = op.txid;
                            queuer.enqueue(request::GetTx { txid });
                            return true;
                        }
                    };
                    match tx.output.get(op.vout as usize) {
                        Some(txout) => {
                            self.tx_update.txouts.insert(*op, txout.clone());
                        }
                        None => {
                            err.get_or_insert_with(|| {
                                anyhow::anyhow!(
                                    "tx {} has {} outputs, but is spent at vout {}",
                                    op.txid,
                                    tx.output.len(),
                                    op.vout,
                                )
                            });
                        }
                    }
                    false
                });
                if let Some(err) = err {
                    return Err(err);
                }
                if missing_prevouts.is_empty() {
                    self.stage = SpkStage::Done;
                    SpkProgress::Continue
                } else {
                    SpkProgress::Blocked
                }
            }
            SpkStage::Done => SpkProgress::Done(core::mem::take(&mut self.tx_update)),
        };

        let stage_str = match &self.stage {
            SpkStage::ProcessingHistory { status } => format!("ProcessingHistory({status})"),
            SpkStage::ProcessingTxs(txids) => format!("ProcessingTxs({})", txids.len()),
            SpkStage::ProcessingPrevouts(ops) => format!("ProcessingPrevouts({})", ops.len()),
            SpkStage::Done => "Done".to_string(),
        };
        tracing::trace!(
            elapsed_seconds = self.elapsed_seconds(),
            spk_hash = self.spk_hash.to_string(),
            stage = stage_str,
            "Spk job progress"
        );
        Ok(progress)
    }
}
