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
    notification::Notification, request, response, AsyncPendingRequest, BlockingPendingRequest,
    ElectrumScriptHash, ElectrumScriptStatus, MaybeBatch, PendingRequest,
    RawNotificationOrResponse, Request,
};
use miniscript::{Descriptor, DescriptorPublicKey};
use serde_json::from_value;

use crate::{
    chain_job::ChainJob,
    req::{JobRequest, ReqCoord, ReqQueue},
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
            _ => None,
        }
    }
}

pub type AsyncState<K = &'static str> = State<AsyncPendingRequest, K>;
pub type BlockingState<K = &'static str> = State<BlockingPendingRequest, K>;

#[derive(Debug)]
pub struct State<PReq: PendingRequest, K = &'static str> {
    spk_tracker: DerivedSpkTracker<K>,
    coord: ReqCoord,
    cp: CheckPoint,
    cache: Cache,

    spk_jobs: BTreeMap<ElectrumScriptHash, SpkJob>,
    chain_job: Option<ChainJob>,
    user_state: electrum_streaming_client::State<PReq>,

    /// Whether we have sent initial requests.
    ///
    /// This includes subscribing to headers, and existing pending requests.
    init_reqs_sent: bool,
    /// Whether at least one chain job has completed.
    first_chain_job_completed: bool,
}

impl<PReq: PendingRequest, K: Ord + Clone> State<PReq, K> {
    pub fn new(
        coord: ReqCoord,
        cache: Cache,
        spk_tracker: DerivedSpkTracker<K>,
        cp: CheckPoint,
    ) -> Self {
        Self {
            spk_tracker,
            coord,
            cp,
            cache,
            spk_jobs: BTreeMap::new(),
            chain_job: None,
            user_state: electrum_streaming_client::State::new(),
            init_reqs_sent: false,
            first_chain_job_completed: false,
        }
    }

    /// Get a reference to the internal cache.
    pub fn cache(&self) -> &Cache {
        &self.cache
    }

    /// Reset the state to be not initialized.
    ///
    /// Call this after disconnection otherwise pending requests will not be resent and no
    /// subscriptions to the chain or spks will be made.
    pub fn reset(&mut self) {
        tracing::trace!("Reseting state");
        self.chain_job = None;
        self.init_reqs_sent = false;
        self.first_chain_job_completed = false;
    }

    /// Insert a descriptor and queue outgoing requests (if needed).
    pub fn insert_descriptor(
        &mut self,
        req_queue: &mut ReqQueue,
        keychain: K,
        descriptor: Descriptor<DescriptorPublicKey>,
        next_index: u32,
    ) {
        let new_script_hashes = self
            .spk_tracker
            .insert_descriptor(keychain, descriptor, next_index);
        if self.first_chain_job_completed {
            for script_hash in new_script_hashes {
                let mut queuer = self.coord.queuer(req_queue, JobId::Spk(script_hash));
                queuer.enqueue(request::ScriptHashSubscribe { script_hash });
            }
        }
    }

    /// Only start spk jobs after the first chain job completes.
    ///
    /// This ensures we are at the latest tip while running spk jobs since spk jobs cannot extend
    /// the local chain. If we allow an spk job to complete without the latest tip, we would end
    /// up missing checkpoints until the next chain update.
    fn first_chain_job_completed_callback(&mut self, req_queue: &mut ReqQueue) {
        if self.first_chain_job_completed {
            return;
        }
        self.first_chain_job_completed = true;
        for script_hash in self.spk_tracker.all_spk_hashes() {
            tracing::info!(
                script_hash = script_hash.to_string(),
                "Queue script subscribe"
            );
            let mut queuer = self.coord.queuer(req_queue, JobId::Spk(script_hash));
            queuer.enqueue(request::ScriptHashSubscribe { script_hash });
        }
    }

    pub fn init(&mut self, req_queue: &mut ReqQueue) {
        if !self.init_reqs_sent {
            self.init_reqs_sent = true;
            // Resend pending requests.
            req_queue.extend(self.user_state.pending_requests());
            req_queue.extend(self.coord.pending_requests());

            tracing::info!("Queue headers subscribe");
            self.coord
                .queuer(req_queue, JobId::Chain)
                .enqueue(request::HeadersSubscribe);
        }
    }

    pub fn user_request<R>(&mut self, req_queue: &mut ReqQueue, req: R)
    where
        R: Into<MaybeBatch<PReq>>,
    {
        req_queue.extend(
            self.user_state
                .track_request(self.coord.next_id_mut(), req)
                .into_vec(),
        );
    }

    pub fn advance(
        &mut self,
        req_queue: &mut ReqQueue,
        raw: RawNotificationOrResponse,
    ) -> anyhow::Result<Option<Update<K>>> {
        self.init(req_queue);
        if let Err(e) = self.user_state.process_incoming(raw.clone()) {
            match e {
                electrum_streaming_client::ProcessError::MissingRequest(_) => {}
                other_err => return Err(other_err.into()),
            }
        }
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
                            self.first_chain_job_completed_callback(req_queue);
                            Ok(None)
                        }
                    }
                    Notification::ScriptHash(script_hash_notification) => {
                        let spk_hash = script_hash_notification.script_hash();
                        let spk_status = script_hash_notification.script_status();

                        let (k, i) =
                            self.spk_tracker
                                .index_of_spk_hash(spk_hash)
                                .ok_or(anyhow::anyhow!(
                                    "unexpected script hash notification: {}",
                                    spk_hash
                                ))?;

                        let mut last_active_indices = BTreeMap::new();

                        if spk_status.is_some() || self.cache.spk_txids.contains_key(&spk_hash) {
                            for script_hash in self.spk_tracker.mark_script_hash_used(&k, i) {
                                self.coord
                                    .queuer(req_queue, JobId::Spk(script_hash))
                                    .enqueue(request::ScriptHashSubscribe { script_hash });
                            }
                            last_active_indices.insert(k, i);
                        }

                        let mut job = SpkJob::new(&self.cache, spk_hash, spk_status).advance(
                            &mut self.coord.queuer(req_queue, JobId::Spk(spk_hash)),
                            &self.cache,
                            &self.cp,
                        );
                        match job.try_finish() {
                            Some((_, tx_update)) => Ok(Some(Update {
                                tx_update,
                                last_active_indices,
                                chain_update: Some(self.cp.clone()),
                            })),
                            None => {
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
                tracing::trace!(?raw_response, ?orig_req, ?job_ids, "Got raw response");

                let raw = match raw_response.result {
                    Ok(raw) => raw,
                    Err(err) => {
                        // Cancel jobs that resulted in error.
                        self.cancel_jobs(job_ids);
                        return Err(anyhow::anyhow!(err).context("Server responded with error"));
                    }
                };

                match orig_req {
                    JobRequest::GetHeaders(req) => {
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
                    JobRequest::GetHeader(req) => {
                        let resp = from_raw(&req, raw)?;

                        self.cache
                            .headers
                            .insert(resp.header.block_hash(), resp.header);

                        // Do not extend checkpoints.
                        if req.height > self.cp.height() {
                            return Ok(None);
                        }
                        // Do not replace blocks.
                        if self
                            .cp
                            .get(req.height)
                            .is_some_and(|cp| cp.height() == req.height)
                        {
                            return Ok(None);
                        }
                        self.cp = self
                            .cp
                            .clone()
                            .insert(BlockId::from((req.height, resp.header.block_hash())));
                        Ok(self.advance_spk_jobs(req_queue, job_ids))
                    }
                    JobRequest::GetHistory(req) => {
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
                    JobRequest::GetTx(get_tx) => {
                        let resp = from_raw(&get_tx, raw)?;
                        self.cache.txs.insert(get_tx.txid, resp.tx.into());
                        Ok(self.advance_spk_jobs(req_queue, job_ids))
                    }
                    JobRequest::GetTxMerkle(req) => {
                        let resp = from_raw(&req, raw)?;
                        let cp = match self.cp.get(req.height) {
                            Some(cp) if cp.height() == req.height => cp,
                            _ => {
                                tracing::warn!(
                                    ?req,
                                    ?resp,
                                    "Received a merkle proof before we got the header"
                                );
                                self.cancel_jobs(job_ids);
                                return Ok(None);
                            }
                        };
                        let header = match self.cache.headers.get(&cp.hash()) {
                            Some(header) => header,
                            None => {
                                tracing::warn!(
                                    ?req,
                                    blockhash = cp.hash().to_string(),
                                    "Missing associated header. Reorg?",
                                );
                                self.cancel_jobs(job_ids);
                                return Ok(None);
                            }
                        };
                        let exp_root = resp.expected_merkle_root(req.txid);
                        if header.merkle_root == exp_root {
                            tracing::debug!(
                                txid = req.txid.to_string(),
                                block_height = req.height,
                                block_hash = header.block_hash().to_string(),
                                "Inserting anchor.",
                            );
                            self.cache.anchors.insert(
                                (req.txid, header.block_hash()),
                                ConfirmationBlockTime {
                                    block_id: cp.block_id(),
                                    confirmation_time: header.time as u64,
                                },
                            );
                        } else {
                            tracing::warn!(
                                txid = req.txid.to_string(),
                                block_height = req.height,
                                block_hash = header.block_hash().to_string(),
                                header_root = header.merkle_root.to_string(),
                                expected_root = exp_root.to_string(),
                                "Failed to verify anchor."
                            );
                            self.cache
                                .failed_anchors
                                .insert((req.txid, header.block_hash()));
                        }
                        Ok(self.advance_spk_jobs(req_queue, job_ids))
                    }
                    JobRequest::ScriptHashSubscribe(req) => {
                        let spk_hash = req.script_hash;
                        let spk_status = from_raw(&req, raw)?;

                        let (k, i) =
                            self.spk_tracker
                                .index_of_spk_hash(spk_hash)
                                .ok_or(anyhow::anyhow!(
                            "response's request spk was never registered in the spk tracker: {}",
                            spk_hash
                        ))?;

                        let mut last_active_indices = BTreeMap::new();

                        if spk_status.is_some() || self.cache.spk_txids.contains_key(&spk_hash) {
                            for script_hash in self.spk_tracker.mark_script_hash_used(&k, i) {
                                self.coord
                                    .queuer(req_queue, JobId::Spk(script_hash))
                                    .enqueue(request::ScriptHashSubscribe { script_hash });
                            }
                            last_active_indices.insert(k, i);
                        }

                        let mut job = SpkJob::new(&self.cache, spk_hash, spk_status).advance(
                            &mut self.coord.queuer(req_queue, JobId::Spk(spk_hash)),
                            &self.cache,
                            &self.cp,
                        );

                        match job.try_finish() {
                            Some((_, tx_update)) => Ok(Some(Update {
                                tx_update,
                                last_active_indices,
                                chain_update: Some(self.cp.clone()),
                            })),
                            None => {
                                self.spk_jobs.insert(spk_hash, job);
                                Ok(None)
                            }
                        }
                    }
                    JobRequest::HeadersSubscribe(req) => {
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
                            self.first_chain_job_completed_callback(req_queue);
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
            if let Some(mut job) = self.spk_jobs.remove(&spk_hash) {
                job = job.advance(
                    &mut self.coord.queuer(req_queue, JobId::Spk(spk_hash)),
                    &self.cache,
                    &self.cp,
                );
                match job.try_finish() {
                    Some((spk_hash, tx_update)) => {
                        let update = update.get_or_insert(Update::default());
                        update.tx_update.extend(tx_update);
                        update
                            .last_active_indices
                            .extend(self.spk_tracker.index_of_spk_hash(spk_hash));
                    }
                    None => {
                        self.spk_jobs.insert(spk_hash, job);
                    }
                }
            }
        }
        if let Some(update) = &mut update {
            update.chain_update = Some(self.cp.clone());
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
