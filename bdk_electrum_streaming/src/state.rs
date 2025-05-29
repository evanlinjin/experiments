use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    sync::Arc,
};

use anyhow::Context;
use bdk_core::{
    bitcoin::{self, BlockHash, Transaction, Txid},
    BlockId, CheckPoint, ConfirmationBlockTime,
};
use electrum_streaming_client::{
    notification::Notification, request, response, ElectrumScriptHash, ElectrumScriptStatus,
    RawNotificationOrResponse, Request,
};
use serde_json::from_value;

use crate::{
    chain_job::ChainJob,
    req::{AnyRequest, ReqCoord, ReqQueue},
    spk_job::SpkJob,
    DerivedSpkTracker, Update,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum JobId {
    Spk(ElectrumScriptHash),
    Chain,
}

impl JobId {
    pub fn spk_hash(self) -> Option<ElectrumScriptHash> {
        match self {
            JobId::Spk(spk_hash) => Some(spk_hash),
            JobId::Chain => None,
        }
    }
}

#[derive(Debug)]
pub struct State<K> {
    spk_tracker: DerivedSpkTracker<K>,
    coord: ReqCoord,
    cp: CheckPoint,
    cache: Cache,

    spk_jobs: BTreeMap<ElectrumScriptHash, SpkJob>,
    chain_job: Option<ChainJob>,
    /// Whether at least one chain job has completed.
    first_chain_job_completed: bool,
}

impl<K: Ord + Clone> State<K> {
    pub fn new(
        req_queue: &mut ReqQueue,
        mut coord: ReqCoord,
        cache: Cache,
        spk_tracker: DerivedSpkTracker<K>,
        cp: CheckPoint,
    ) -> Self {
        coord
            .queuer(req_queue, JobId::Chain)
            .enqueue(request::HeadersSubscribe);
        Self {
            spk_tracker,
            coord,
            cp,
            cache,
            spk_jobs: BTreeMap::new(),
            chain_job: None,
            first_chain_job_completed: false,
        }
    }

    fn first_chain_job_completed_callback(&mut self, req_queue: &mut ReqQueue) {
        if self.first_chain_job_completed {
            return;
        }
        self.first_chain_job_completed = true;

        println!("[FIRST CHAIN JOB DONE CALLBACK]");

        for script_hash in self.spk_tracker.all_spk_hashes() {
            let mut queuer = self.coord.queuer(req_queue, JobId::Spk(script_hash));
            queuer.enqueue(request::ScriptHashSubscribe { script_hash });
        }
    }

    pub fn advance(
        &mut self,
        req_queue: &mut ReqQueue,
        raw: RawNotificationOrResponse,
    ) -> anyhow::Result<Option<Update<K>>> {
        match raw {
            RawNotificationOrResponse::Notification(raw_notification) => {
                let notification = Notification::new(&raw_notification)
                    .context("Failed to deserialize notification from server")?;
                match notification {
                    Notification::Header(header_notification) => {
                        // Always replace prev job since a new notification means a new tip.
                        self.chain_job = ChainJob::new(
                            self.coord.queuer(req_queue, JobId::Chain),
                            &self.cp,
                            *header_notification.header(),
                            header_notification.height(),
                        );
                        if let Some(job) = self.chain_job.take() {
                            match job.try_finish(&mut self.cp) {
                                Ok(cp) => {
                                    self.first_chain_job_completed_callback(req_queue);
                                    Ok(Some(Update {
                                        chain_update: Some(cp),
                                        ..Default::default()
                                    }))
                                }
                                Err(job) => {
                                    self.chain_job = Some(job);
                                    Ok(None)
                                }
                            }
                        } else {
                            Ok(None)
                        }
                    }
                    Notification::ScriptHash(script_hash_notification) => {
                        let spk_hash = script_hash_notification.script_hash();
                        let spk_status = script_hash_notification.script_status();
                        if spk_status.is_some() || self.cache.spk_txids.contains_key(&spk_hash) {
                            if let Some((_, _, new_subs)) =
                                self.spk_tracker.handle_script_status(spk_hash)
                            {
                                for script_hash in new_subs {
                                    self.coord
                                        .queuer(req_queue, JobId::Spk(script_hash))
                                        .enqueue(request::ScriptHashSubscribe { script_hash });
                                }
                            }
                        }

                        let job = SpkJob::new(&self.cache, spk_hash, spk_status)
                            // Too
                            .advance(
                                &mut self.coord.queuer(req_queue, JobId::Spk(spk_hash)),
                                &self.cache,
                                &self.cp,
                            );
                        match job.try_finish() {
                            Ok((spk_hash, tx_update)) => Ok(Some(Update {
                                tx_update,
                                last_active_indices: self
                                    .spk_tracker
                                    .index_of_spk_hash(spk_hash)
                                    .into_iter()
                                    .collect(),
                                chain_update: Some(self.cp.clone()),
                            })),
                            Err(job) => {
                                self.spk_jobs.insert(spk_hash, job);
                                Ok(None)
                            }
                        }
                    }
                    Notification::Unknown(_) => Ok(None),
                }
            }
            RawNotificationOrResponse::Response(raw_response) => {
                let (orig_req, job_ids) = match self.coord.pop(raw_response.id) {
                    Some(req) => req,
                    None => return Ok(None),
                };

                let raw = match raw_response.result {
                    Ok(raw) => raw,
                    Err(err) => {
                        // Cancel jobs that resulted in error.
                        self.cancel_jobs(job_ids);
                        return Err(anyhow::anyhow!(err).context("Server responded with error"));
                    }
                };

                match orig_req {
                    AnyRequest::GetHeaders(req) => {
                        let resp = from_raw(&req, raw)?;
                        self.cache
                            .headers
                            .extend(resp.headers.iter().map(|&h| (h.block_hash(), h)));
                        debug_assert!(job_ids.contains(&JobId::Chain));
                        if let Some(job) = self.chain_job.take() {
                            let new_blocks = (req.start_height..)
                                .zip(resp.headers.into_iter().map(|h| h.block_hash()));
                            match job.process_blocks(new_blocks).try_finish(&mut self.cp) {
                                Ok(cp) => {
                                    self.first_chain_job_completed_callback(req_queue);
                                    Ok(Some(Update {
                                        chain_update: Some(cp),
                                        ..Default::default()
                                    }))
                                }
                                Err(job) => {
                                    self.chain_job = Some(job);
                                    Ok(None)
                                }
                            }
                        } else {
                            Ok(None)
                        }
                    }
                    AnyRequest::GetHeader(req) => {
                        let resp = from_raw(&req, raw)?;
                        self.cache
                            .headers
                            .insert(resp.header.block_hash(), resp.header);

                        // Do not extend checkpoints.
                        if req.height > self.cp.height() {
                            return Ok(None);
                        }
                        // Do not replace blocks.
                        if self.cp.get(req.height).is_some() {
                            return Ok(None);
                        }
                        self.cp = self
                            .cp
                            .clone()
                            .insert(BlockId::from((req.height, resp.header.block_hash())));
                        Ok(self.advance_spk_jobs(req_queue, job_ids))
                    }
                    AnyRequest::GetHistory(req) => {
                        let resp = from_raw(&req, raw)?;
                        if let Some(spk_status) = ElectrumScriptStatus::from_history(&resp) {
                            self.cache
                                .spk_histories
                                .entry(spk_status)
                                .or_default()
                                .extend(resp.clone());
                            self.cache
                                .spk_txids
                                .entry(req.script_hash)
                                .or_default()
                                .extend(resp.iter().map(|tx| tx.txid()));
                        }
                        Ok(self.advance_spk_jobs(req_queue, job_ids))
                    }
                    AnyRequest::GetTx(get_tx) => {
                        let resp = from_raw(&get_tx, raw)?;
                        self.cache.txs.insert(get_tx.txid, resp.tx.into());
                        Ok(self.advance_spk_jobs(req_queue, job_ids))
                    }
                    AnyRequest::GetTxMerkle(req) => {
                        let resp = from_raw(&req, raw)?;
                        let cp = match self.cp.get(req.height) {
                            Some(cp) => cp,
                            None => {
                                println!("Must get header before tx merkle.");
                                self.cancel_jobs(job_ids);
                                return Ok(None);
                            }
                        };
                        let header = match self.cache.headers.get(&cp.hash()) {
                            Some(header) => header,
                            None => {
                                println!("Missing header, reorg?");
                                self.cancel_jobs(job_ids);
                                return Ok(None);
                            }
                        };
                        let exp_root = resp.expected_merkle_root(req.txid);
                        if header.merkle_root == exp_root {
                            self.cache.anchors.insert(
                                (req.txid, header.block_hash()),
                                ConfirmationBlockTime {
                                    block_id: cp.block_id(),
                                    confirmation_time: header.time as u64,
                                },
                            );
                        } else {
                            self.cache
                                .failed_anchors
                                .insert((req.txid, header.block_hash()));
                        }
                        Ok(self.advance_spk_jobs(req_queue, job_ids))
                    }
                    AnyRequest::ScriptHashSubscribe(req) => {
                        let spk_hash = req.script_hash;
                        let spk_status = from_raw(&req, raw)?;
                        println!("[ADVANCE] spk sub response: {spk_status:?}");
                        if spk_status.is_some() || self.cache.spk_txids.contains_key(&spk_hash) {
                            if let Some((_, _, new_subs)) =
                                self.spk_tracker.handle_script_status(spk_hash)
                            {
                                for script_hash in new_subs {
                                    self.coord
                                        .queuer(req_queue, JobId::Spk(script_hash))
                                        .enqueue(request::ScriptHashSubscribe { script_hash });
                                }
                            }
                        }

                        let job = SpkJob::new(&self.cache, spk_hash, spk_status)
                            // Too
                            .advance(
                                &mut self.coord.queuer(req_queue, JobId::Spk(spk_hash)),
                                &self.cache,
                                &self.cp,
                            );
                        match job.try_finish() {
                            Ok((spk_hash, tx_update)) => Ok(Some(Update {
                                tx_update,
                                last_active_indices: self
                                    .spk_tracker
                                    .index_of_spk_hash(spk_hash)
                                    .into_iter()
                                    .collect(),
                                chain_update: Some(self.cp.clone()),
                            })),
                            Err(job) => {
                                self.spk_jobs.insert(spk_hash, job);
                                Ok(None)
                            }
                        }
                    }
                    AnyRequest::HeadersSubscribe(req) => {
                        let resp = from_raw(&req, raw)?;
                        // Always replace prev job since a new notification means a new tip.
                        self.chain_job = ChainJob::new(
                            self.coord.queuer(req_queue, JobId::Chain),
                            &self.cp,
                            resp.header,
                            resp.height,
                        );
                        if let Some(job) = self.chain_job.take() {
                            match job.try_finish(&mut self.cp) {
                                Ok(cp) => {
                                    self.first_chain_job_completed_callback(req_queue);
                                    Ok(Some(Update {
                                        chain_update: Some(cp),
                                        ..Default::default()
                                    }))
                                }
                                Err(job) => {
                                    self.chain_job = Some(job);
                                    Ok(None)
                                }
                            }
                        } else {
                            Ok(None)
                        }
                    }
                }
            }
        }
    }

    fn advance_spk_jobs(
        &mut self,
        req_queue: &mut ReqQueue,
        job_ids: impl IntoIterator<Item = JobId>,
    ) -> Option<Update<K>> {
        let mut update = Option::<Update<K>>::None;
        let spk_hashes = job_ids.into_iter().filter_map(|jid| jid.spk_hash());
        for spk_hash in spk_hashes {
            if let Some(job) = self.spk_jobs.remove(&spk_hash) {
                match job
                    .advance(
                        &mut self.coord.queuer(req_queue, JobId::Spk(spk_hash)),
                        &self.cache,
                        &self.cp,
                    )
                    .try_finish()
                {
                    Ok((spk_hash, tx_update)) => {
                        let update = update.get_or_insert(Update::default());
                        update.tx_update.extend(tx_update);
                        update
                            .last_active_indices
                            .extend(self.spk_tracker.index_of_spk_hash(spk_hash));
                    }
                    Err(job) => {
                        self.spk_jobs.insert(spk_hash, job);
                    }
                }
            }
        }
        update
    }

    fn cancel_jobs(&mut self, job_ids: impl IntoIterator<Item = JobId>) {
        for jid in job_ids {
            match jid {
                JobId::Spk(spk_hash) => {
                    self.spk_jobs.remove(&spk_hash);
                }
                JobId::Chain => {
                    self.chain_job = None;
                }
            }
        }
    }
}

pub fn from_raw<R>(_req: &R, raw: serde_json::Value) -> Result<R::Response, serde_json::Error>
where
    R: Request,
{
    from_value(raw)
}

/// A monotonically growing cache.
#[derive(Debug, Clone, Default)]
pub struct Cache {
    pub spk_histories: HashMap<ElectrumScriptStatus, Vec<response::Tx>>,
    pub spk_txids: HashMap<ElectrumScriptHash, BTreeSet<Txid>>,
    pub txs: HashMap<Txid, Arc<Transaction>>,
    pub anchors: HashMap<(Txid, BlockHash), ConfirmationBlockTime>,
    pub failed_anchors: HashSet<(Txid, BlockHash)>,
    pub headers: HashMap<BlockHash, bitcoin::block::Header>,
}
