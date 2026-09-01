use std::collections::BTreeMap;

use anyhow::Context;
use bdk_core::{CheckPoint, ConfirmationBlockTime};
use electrum_streaming_client::{
    notification::Notification, request, AsyncPendingRequest, BlockingPendingRequest,
    ElectrumScriptHash, ElectrumScriptStatus, MaybeBatch, PendingRequest,
    RawNotificationOrResponse, Request,
};
use miniscript::{Descriptor, DescriptorPublicKey};
use serde_json::from_value;

use crate::{
    cache::{Cache, Subscriptions},
    confirmation_job::{ConfirmationJob, ConfirmationProgress},
    req::{JobRequest, PoppedRequest, ReqCoord, ReqQueue},
    spk_job::{SpkJob, SpkProgress},
    DerivedSpkTracker, Update,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum JobId {
    Spk(ElectrumScriptHash),
    /// The single job that moves the local chain and anchors what the scripts found.
    Confirmation,
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
    confirmation_job: Option<ConfirmationJob>,

    /// The update being built up.
    ///
    /// Both job kinds write here as they progress, and it is handed to the caller whole when all
    /// pending jobs complete.
    staged: Update<K>,

    user_state: electrum_streaming_client::State<PReq>,

    /// Whether the header subscription, spk subscriptions and pending requests have been sent.
    init_reqs_sent: bool,
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
            confirmation_job: None,
            staged: Update::default(),
            user_state: electrum_streaming_client::State::new(),
            init_reqs_sent: false,
        }
    }

    pub fn cache(&self) -> &Cache {
        &self.cache
    }

    pub fn subscriptions(&self) -> &Subscriptions {
        &self.cache.subscriptions
    }

    /// Reset the state to be not initialized.
    ///
    /// Call this after disconnection otherwise pending requests will not be resent and no
    /// subscriptions to the chain or spks will be made.
    pub fn reset(&mut self) {
        tracing::trace!("Reseting state");
        self.confirmation_job = None;
        self.init_reqs_sent = false;
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
        for script_hash in new_script_hashes {
            let mut queuer = self.coord.queuer(req_queue, JobId::Spk(script_hash));
            queuer.enqueue(request::ScriptHashSubscribe { script_hash });
        }
    }

    pub fn init(&mut self, req_queue: &mut ReqQueue) {
        if !self.init_reqs_sent {
            self.init_reqs_sent = true;
            req_queue.extend(self.user_state.pending_requests());
            req_queue.extend(self.coord.pending_requests());

            tracing::info!("Queue headers subscribe");
            self.coord
                .queuer(req_queue, JobId::Confirmation)
                .enqueue(request::HeadersSubscribe);

            for script_hash in self.spk_tracker.all_spk_hashes() {
                tracing::info!(
                    script_hash = script_hash.to_string(),
                    "Queue script subscribe"
                );
                let mut queuer = self.coord.queuer(req_queue, JobId::Spk(script_hash));
                queuer.enqueue(request::ScriptHashSubscribe { script_hash });
            }
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

    pub fn poll(
        &mut self,
        req_queue: &mut ReqQueue,
        raw: RawNotificationOrResponse,
    ) -> anyhow::Result<Option<Update<K>>> {
        self.handle(req_queue, raw)?;

        // Any path through `handle` may have been the one that finished a job, so the update is
        // handed over here rather than in each of them. Both job kinds have to be done.
        let job = match &mut self.confirmation_job {
            Some(job) => job,
            None => return Ok(None),
        };
        if !job.is_done() || !self.spk_jobs.values().all(SpkJob::is_done) {
            return Ok(None);
        }
        job.set_idle();
        // The scripts have been anchored, so their jobs have served their purpose.
        self.spk_jobs.clear();
        let update = core::mem::take(&mut self.staged);
        tracing::info!(
            tip_height = self.cp.height(),
            anchors = update.tx_update.anchors.len(),
            txs = update.tx_update.txs.len(),
            "Confirmation job finished"
        );
        Ok(Some(update))
    }

    /// Apply one message from the server, driving whatever jobs it touches.
    fn handle(
        &mut self,
        req_queue: &mut ReqQueue,
        raw: RawNotificationOrResponse,
    ) -> anyhow::Result<()> {
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
                    Notification::Header(n) => self.on_new_tip(req_queue, n.height(), *n.header()),
                    Notification::ScriptHash(n) => {
                        self.on_spk_status(req_queue, n.script_hash(), n.script_status())
                    }
                    Notification::Unknown(_) => Ok(()),
                }
            }
            RawNotificationOrResponse::Response(raw_response) => {
                let PoppedRequest {
                    request: orig_req,
                    job_ids,
                    reorged_since_sent,
                } = match self.coord.pop(raw_response.id) {
                    Some(req) => req,
                    None => return Ok(()),
                };
                tracing::trace!(?raw_response, ?orig_req, ?job_ids, "Got raw response");

                let raw = match raw_response.result {
                    Ok(raw) => raw,
                    Err(err) => {
                        // Cancel the jobs waiting on this request.
                        for jid in job_ids {
                            match jid {
                                JobId::Spk(spk_hash) => {
                                    self.spk_jobs.remove(&spk_hash);
                                }
                                JobId::Confirmation => self.confirmation_job = None,
                            }
                        }

                        // An anchor fetch is speculative: a reorg may have taken the
                        // transaction out of the block its height was reported at, or out of the
                        // chain. An error there is an answer, not a reason to bring the
                        // connection down. The job went with the loop above, and the next
                        // notification builds one that asks again.
                        if let JobRequest::GetTxMerkle(req) = &orig_req {
                            tracing::warn!(
                                txid = req.txid.to_string(),
                                block_height = req.height,
                                ?err,
                                "Server gave no merkle proof at this height. Reorg?",
                            );
                            return Ok(());
                        }

                        // The connection goes down here.
                        return Err(anyhow::anyhow!(err).context("Server responded with error"));
                    }
                };

                match orig_req {
                    JobRequest::GetHeaders(req) => {
                        let resp = from_raw(&req, raw)?;
                        debug_assert!(job_ids.contains(&JobId::Confirmation));
                        let blocks = self
                            .cache
                            .resolve_headers_query(req, resp)
                            .collect::<Vec<_>>();
                        if let Some(job) = &mut self.confirmation_job {
                            job.resolve_blocks(blocks);
                        }
                        self.poll_confirmation_job(req_queue)
                    }
                    JobRequest::GetHistory(req) => {
                        let resp = from_raw(&req, raw)?;
                        let resp_status = self.cache.resolve_history_query(req, resp);

                        // A history that does not hash to the status a job awaits can never
                        // satisfy it, and the two differing means the status moved — so a
                        // notification is coming, and it will build the job again.
                        let superseded = self.spk_jobs.get(&req.script_hash).is_some_and(|job| {
                            job.awaiting_history()
                                .is_some_and(|awaited| Some(awaited) != resp_status)
                        });
                        if superseded {
                            tracing::debug!(
                                spk_hash = req.script_hash.to_string(),
                                "History answers a status the job is not waiting for. Dropping.",
                            );
                            self.spk_jobs.remove(&req.script_hash);
                        }
                        self.poll_spk_jobs(req_queue, job_ids)?;
                        self.poll_confirmation_job(req_queue)
                    }
                    JobRequest::GetTx(get_tx) => {
                        let resp = from_raw(&get_tx, raw)?;
                        self.cache.tx_cache.txs.insert(get_tx.txid, resp.tx.into());
                        self.poll_spk_jobs(req_queue, job_ids)?;
                        self.poll_confirmation_job(req_queue)
                    }
                    JobRequest::GetTxMerkle(req) => {
                        let resp = from_raw(&req, raw)?;

                        // The proof answers the server's chain as it was when we asked. Checking
                        // it against the block we now hold would read a disagreement between two
                        // chains as a verdict on this one, so discard it and ask again.
                        if reorged_since_sent {
                            return self.poll_confirmation_job(req_queue);
                        }

                        let cp = match self.cp.get(req.height) {
                            Some(cp) => cp,
                            // Not expected to fire: the job places every height before it asks
                            // a proof for it, and a height leaving the chain bumps the generation
                            // the check above catches. Getting here is our own bookkeeping
                            // breaking, not the server misbehaving.
                            None => {
                                debug_assert!(
                                    false,
                                    "proof for height {}, which is not in our chain",
                                    req.height
                                );
                                tracing::error!(
                                    ?req,
                                    ?resp,
                                    "Received a merkle proof before we placed the block"
                                );
                                self.confirmation_job = None;
                                return Ok(());
                            }
                        };
                        let header = match self.cache.headers.get(&cp.hash()) {
                            Some(header) => *header,
                            // Not expected either, and a reorg is not the reason — that is the
                            // check above. Every header a job puts in the chain lands in
                            // `Cache::headers` as it arrives, and nothing prunes them.
                            None => {
                                debug_assert!(
                                    false,
                                    "no header for {}, the block we hold at height {}",
                                    cp.hash(),
                                    req.height
                                );
                                tracing::error!(
                                    ?req,
                                    blockhash = cp.hash().to_string(),
                                    "No header for the block we hold at this height",
                                );
                                self.confirmation_job = None;
                                return Ok(());
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
                            self.cache.tx_cache.anchors.insert(
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
                                "Proof does not match the block we have at this height",
                            );
                            self.confirmation_job = None;
                            return Ok(());
                        }
                        self.poll_confirmation_job(req_queue)
                    }
                    JobRequest::ScriptHashSubscribe(req) => {
                        let spk_status = from_raw(&req, raw)?;
                        self.on_spk_status(req_queue, req.script_hash, spk_status)
                    }
                    JobRequest::HeadersSubscribe(req) => {
                        let resp = from_raw(&req, raw)?;
                        self.on_new_tip(req_queue, resp.height, resp.header)
                    }
                }
            }
        }
    }

    /// React to the server announcing `header` at `height` as its tip.
    fn on_new_tip(
        &mut self,
        req_queue: &mut ReqQueue,
        height: u32,
        header: bdk_core::bitcoin::block::Header,
    ) -> anyhow::Result<()> {
        // A same-height reorg is applied without fetching anything, so this announcement is the
        // only place the replacement header is ever offered to us. Caching it here saves the
        // anchor refetch a round-trip on the very path it exists for.
        self.cache.headers.insert(header.block_hash(), header);

        match &mut self.confirmation_job {
            Some(job) => {
                if job.set_tip(height, header) {
                    // The tip we were heading for is gone, and its requests go with it: the
                    // replacement asks for the same heights, so a survivor would be deduplicated
                    // against and fill the new job with the chain the server just left.
                    self.coord.forget_job(JobId::Confirmation);
                }
            }
            None => self.confirmation_job = Some(ConfirmationJob::new(height, header)),
        }
        self.poll_confirmation_job(req_queue)
    }

    /// React to the server reporting `spk_status` for `spk_hash`.
    fn on_spk_status(
        &mut self,
        req_queue: &mut ReqQueue,
        spk_hash: ElectrumScriptHash,
        spk_status: Option<ElectrumScriptStatus>,
    ) -> anyhow::Result<()> {
        let (k, i) = self
            .spk_tracker
            .index_of_spk_hash(spk_hash)
            .ok_or(anyhow::anyhow!(
                "unexpected script hash notification: {}",
                spk_hash
            ))?;

        if spk_status.is_none() {
            self.cache.subscriptions.remove_spk(spk_hash);
        }

        if spk_status.is_some() || self.cache.tx_cache.spk_txids.contains_key(&spk_hash) {
            for script_hash in self.spk_tracker.mark_script_hash_used(&k, i) {
                self.coord
                    .queuer(req_queue, JobId::Spk(script_hash))
                    .enqueue(request::ScriptHashSubscribe { script_hash });
            }
            self.staged.last_active_indices.insert(k, i);
        }

        self.spk_jobs
            .insert(spk_hash, SpkJob::new(&self.cache, spk_hash, spk_status));
        self.poll_spk_jobs(req_queue, [JobId::Spk(spk_hash)])?;
        // A notification is all that revives a cancelled job, and below the reorg window the
        // tip never moves — so this is where an anchor the server has come back to is picked up.
        if self.confirmation_job.is_none() {
            match self.cache.headers.get(&self.cp.hash()) {
                Some(&header) => {
                    self.confirmation_job = Some(ConfirmationJob::new(self.cp.height(), header));
                }
                // Not expected to fire: a tip is only adopted through a notification, which
                // caches its header. Ask for the tip rather than leave the anchors waiting on a
                // block ten minutes out; a request already in flight absorbs this one.
                None => {
                    tracing::warn!(
                        tip_height = self.cp.height(),
                        tip_hash = self.cp.hash().to_string(),
                        "No header for our tip, so no confirmation job can be built. Resubscribing."
                    );
                    self.coord
                        .queuer(req_queue, JobId::Confirmation)
                        .enqueue(request::HeadersSubscribe);
                }
            }
        }
        self.poll_confirmation_job(req_queue)
    }

    /// Poll the named spk jobs, staging whatever each one has finished gathering.
    ///
    /// Jobs are left in place when they finish: the update they contribute to is published
    /// only once [`ConfirmationJob`] completes, and they are cleared then.
    fn poll_spk_jobs(
        &mut self,
        req_queue: &mut ReqQueue,
        job_ids: impl IntoIterator<Item = JobId>,
    ) -> anyhow::Result<()> {
        // Borrowed field by field so a job is polled where it sits, not lifted out and put back.
        let Self {
            spk_tracker,
            coord,
            cache,
            spk_jobs,
            staged,
            ..
        } = self;

        for spk_hash in job_ids.into_iter().filter_map(JobId::spk_hash) {
            let job = match spk_jobs.get_mut(&spk_hash) {
                Some(job) => job,
                None => continue,
            };
            loop {
                let mut queuer = coord.queuer(req_queue, JobId::Spk(spk_hash));
                match job.poll(&mut queuer, cache)? {
                    SpkProgress::Continue => continue,
                    SpkProgress::Blocked => break,
                    SpkProgress::Done(tx_update) => {
                        tracing::info!(
                            elapsed_seconds = job.elapsed_seconds(),
                            spk_hash = spk_hash.to_string(),
                            "Spk job finished"
                        );
                        staged.tx_update.extend(tx_update);
                        staged
                            .last_active_indices
                            .extend(spk_tracker.index_of_spk_hash(spk_hash));
                        break;
                    }
                }
            }
        }
        Ok(())
    }

    /// Drive the confirmation job as far as it will go.
    ///
    /// Held back only until every script has its history. The job works from the heights those
    /// histories name, so a script still downloading the transactions in its own history has
    /// already told the job everything it needs — and holding for the downloads would serialise
    /// the header and proof fetches behind them for nothing.
    fn poll_confirmation_job(&mut self, req_queue: &mut ReqQueue) -> anyhow::Result<()> {
        if self
            .spk_jobs
            .values()
            .any(|job| job.awaiting_history().is_some())
        {
            return Ok(());
        }
        let mut job = match self.confirmation_job.take() {
            Some(job) => job,
            None => return Ok(()),
        };
        // Scoped by what the server has told us about, not by which jobs happen to be live:
        // those are cleared on every completion, so a single notification arriving between
        // updates would narrow the next reorg's repair to that one script.
        job.set_statuses(self.cache.subscriptions.spk_statuses());

        loop {
            let progress = {
                let mut queuer = self.coord.queuer(req_queue, JobId::Confirmation);
                match job.poll(&mut queuer, &self.cache, &self.cp) {
                    Ok(progress) => progress,
                    Err(err) => {
                        self.confirmation_job = Some(job);
                        return Err(err);
                    }
                }
            };
            match progress {
                ConfirmationProgress::Continue => continue,
                ConfirmationProgress::CheckPointUpdate { cp, evicted } => {
                    if !evicted.is_empty() {
                        tracing::info!(
                            heights = ?evicted,
                            "Blocks evicted from the local chain. Refetching anchors."
                        );
                        // Responses to requests which are still in flight describe the chain we
                        // just left behind.
                        self.coord.bump_chain_generation();
                    }
                    self.cp = cp.clone();
                    self.staged.chain_update = Some(cp);
                    continue;
                }
                ConfirmationProgress::AnchorUpdate(anchors) => {
                    // Assigned, not extended: each pass re-resolves the whole set at once.
                    self.staged.tx_update.anchors = anchors;
                    continue;
                }
                // Nothing more to do this round. Whether the job owes an update is settled by
                // `poll`, once the scripts can be checked alongside it.
                ConfirmationProgress::Blocked | ConfirmationProgress::Done => break,
            }
        }
        self.confirmation_job = Some(job);
        Ok(())
    }
}

pub fn from_raw<R>(_req: &R, raw: serde_json::Value) -> Result<R::Response, serde_json::Error>
where
    R: Request,
{
    from_value(raw)
}
