use std::collections::{BTreeMap, BTreeSet};

use bdk_core::{bitcoin::Txid, TxUpdate};
use electrum_streaming_client::{request, response, ElectrumScriptHash, ElectrumScriptStatus};

use crate::{
    req::{AnyRequest, ReqQueuer},
    Cache, DerivedSpkTracker, StateInner, Update,
};

#[derive(Debug)]
pub struct SpkJob<K> {
    /// Time that we got this notification.
    pub start_epoch: u64,
    /// Script hash of this notification.
    pub spk_hash: ElectrumScriptHash,
    /// Script status of this notification.
    pub spk_status: Option<ElectrumScriptStatus>,

    pub missing_history: bool,
    pub missing_txs: BTreeSet<Txid>,
    pub missing_prev_txs: BTreeMap<Txid, BTreeSet<u32>>,
    pub missing_headers: BTreeSet<u32>,
    pub missing_anchors: BTreeSet<Txid>,

    /// Staged tx update.
    pub tx_update: TxUpdate<K>,
    /// Staged last active indices update.
    pub last_active_indices: BTreeMap<K, u32>,
}

impl<K> SpkJob<K> {
    pub fn new(
        mut queuer: ReqQueuer,
        state: &mut StateInner<K>,
        spk_hash: ElectrumScriptHash,
        spk_status: Option<ElectrumScriptStatus>,
    ) -> Option<Self> {
        let mut job = Self {
            start_epoch: std::time::UNIX_EPOCH
                .elapsed()
                .expect("must get time")
                .as_secs(),
            spk_hash,
            spk_status,
            missing_history: false,
            missing_txs: BTreeSet::new(),
            missing_prev_txs: BTreeMap::new(),
            missing_headers: BTreeSet::new(),
            missing_anchors: BTreeSet::new(),
            tx_update: TxUpdate::default(),
            last_active_indices: BTreeMap::new(),
        };

        let spk_status = match spk_status {
            Some(status) => status,
            None => {
                if let Some(to_evict) = state.cache.spk_txids.get(&spk_hash) {
                    job.tx_update
                        .evicted_ats
                        .extend(to_evict.iter().map(|&txid| (txid, start_epoch)));
                }
                return None;
            }
        };

        if let Some(history) = state.cache.spk_histories.get(&spk_status) {
            let cache_spk_txids = state.cache.spk_txids.entry(spk_hash).or_default();
            return self._handle_history(queuer, cache_spk_txids, history);
        }

        None
    }

    pub fn handle_status(
        mut self,
        mut queuer: ReqQueuer,
        spk_tracker: &mut DerivedSpkTracker<K>,
        cache: &mut Cache,
        status: Option<ElectrumScriptStatus>,
    ) -> Option<Self>
    where
        K: Clone + Ord + Send + Sync + 'static,
    {
        let status = match status {
            Some(status) => status,
            None => {
                if let Some(to_evict) = cache.spk_txids.remove(&self.spk_hash) {
                    self.tx_update
                        .evicted_ats
                        .extend(to_evict.into_iter().map(|txid| (txid, self.start_epoch)));
                }
                return None;
            }
        };

        if let Some(history) = cache.spk_histories.get(&status) {
            let cache_spk_txids = cache.spk_txids.entry(self.spk_hash).or_default();
            return self._handle_history(queuer, cache_spk_txids, history);
        }

        let script_hash = self.spk_hash;
        queuer.enqueue(request::GetHistory { script_hash });
        if let Some((k, i, to_sub)) = spk_tracker.handle_script_status(script_hash) {
            self.last_active_indices.insert(k, i);
            for script_hash in to_sub {
                queuer.enqueue(request::ScriptHashSubscribe { script_hash });
            }
        }

        Some(self)
    }

    fn _handle_history(
        mut self,
        mut queuer: ReqQueuer,
        cache_spk_txids: &mut BTreeSet<Txid>,
        history: &[response::Tx],
    ) -> Option<Self> {
        let history_txids = history
            .iter()
            .map(|tx| tx.txid())
            .collect::<BTreeSet<Txid>>();
        self.tx_update.evicted_ats.extend(
            cache_spk_txids
                .difference(&history_txids)
                .map(|&txid| (txid, self.start_epoch)),
        );
        cache_spk_txids.extend(history_txids);

        // Make sure we have all txs.

        for history_tx in history {
            let txid = history_tx.txid();
            let conf_height = history_tx.confirmation_height();
        }
        todo!()
    }
}
