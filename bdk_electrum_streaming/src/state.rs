use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::Arc,
};

use anyhow::Context;
use bdk_core::{
    bitcoin::{self, BlockHash, Transaction, Txid},
    BlockId, CheckPoint, ConfirmationBlockTime, TxUpdate,
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
    spk_job::{resolve_anchor, AnchorStep, SpkJob},
    DerivedSpkTracker, Update,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum JobId {
    Spk(ElectrumScriptHash),
    Chain,
    /// Anchors being asked for again because their blocks left the chain.
    Anchor,
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
    /// Anchors whose blocks left the chain, waiting to be established against whatever is at
    /// their height now.
    reanchor: BTreeSet<(u32, Txid)>,
    user_state: electrum_streaming_client::State<PReq>,

    /// Whether we have sent initial requests.
    ///
    /// This includes subscribing to headers, and existing pending requests.
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
            chain_job: None,
            reanchor: BTreeSet::new(),
            user_state: electrum_streaming_client::State::new(),
            init_reqs_sent: false,
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
            // Resend pending requests.
            req_queue.extend(self.user_state.pending_requests());
            req_queue.extend(self.coord.pending_requests());

            tracing::info!("Queue headers subscribe");
            self.coord
                .queuer(req_queue, JobId::Chain)
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
                    Notification::Header(header_notification) => Ok(self.on_new_tip(
                        req_queue,
                        *header_notification.header(),
                        header_notification.height(),
                    )),
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

                        let mut job = SpkJob::new(&self.cache, spk_hash, spk_status);
                        job = job.advance(
                            &mut self.coord.queuer(req_queue, JobId::Spk(spk_hash)),
                            &mut self.cache,
                        );
                        match job.try_finish() {
                            Some((_, tx_update)) => {
                                self.spk_jobs.remove(&spk_hash);
                                Ok(Some(Update {
                                    tx_update,
                                    last_active_indices,
                                    chain_update: Some(self.cp.clone()),
                                }))
                            }
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
                        // An anchor asks about things the server is free to not have: a height it
                        // has not indexed, or a tx that is not in a block. Both come back as
                        // ordinary JSON-RPC errors, so treating them as faults would drop the
                        // connection every time a reorg unconfirmed a tracked tx.
                        let epoch = self.cache.eviction_epoch;
                        match &orig_req {
                            JobRequest::GetHeader(header_req) => {
                                tracing::debug!(
                                    height = header_req.height,
                                    error = ?err,
                                    "Server has no header at this height right now"
                                );
                                record(&mut self.cache.headers_at, header_req.height, None, epoch);
                                return Ok(self.advance_anchor_work(req_queue, job_ids));
                            }
                            JobRequest::GetTxMerkle(merkle_req) => {
                                tracing::debug!(
                                    ?merkle_req,
                                    error = ?err,
                                    "Server has no proof for this tx right now"
                                );
                                let key = (merkle_req.txid, merkle_req.height);
                                record(&mut self.cache.proofs, key, None, epoch);
                                return Ok(self.advance_anchor_work(req_queue, job_ids));
                            }
                            _ => {}
                        }
                        self.cancel_jobs(job_ids);
                        return Err(anyhow::anyhow!(err).context("Server responded with error"));
                    }
                };

                match orig_req {
                    JobRequest::GetHeaders(req) => {
                        let resp = from_raw(&req, raw)?;
                        debug_assert!(job_ids.contains(&JobId::Chain));
                        if let Some(job) = self.chain_job.take() {
                            let new_blocks = (req.start_height..)
                                .zip(resp.headers.into_iter().map(|h| h.block_hash()));
                            self.chain_job = Some(job.process_blocks(new_blocks));
                        }
                        Ok(self.drive_chain_job(req_queue))
                    }
                    JobRequest::GetHeader(req) => {
                        let resp = from_raw(&req, raw)?;
                        let epoch = self.cache.eviction_epoch;
                        let filed = record(
                            &mut self.cache.headers_at,
                            req.height,
                            Some(resp.header),
                            epoch,
                        );

                        // Only an answer that was filed may edit the chain. One that was not is
                        // either stale or already bettered, and inserting it would put a block the
                        // server has left into a chain that nothing will come back to correct.
                        let fills_a_gap = filed
                            && req.height <= self.cp.height()
                            && self.cp.get(req.height).is_none();
                        if fills_a_gap {
                            self.cp = self
                                .cp
                                .clone()
                                .insert(BlockId::from((req.height, resp.header.block_hash())));
                        }
                        Ok(self.advance_anchor_work(req_queue, job_ids))
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
                        // Checking it happens where the header it is checked against is known.
                        let resp = from_raw(&req, raw)?;
                        let epoch = self.cache.eviction_epoch;
                        let key = (req.txid, req.height);
                        record(&mut self.cache.proofs, key, Some(resp), epoch);
                        Ok(self.advance_anchor_work(req_queue, job_ids))
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

                        let mut job = SpkJob::new(&self.cache, spk_hash, spk_status);
                        job = job.advance(
                            &mut self.coord.queuer(req_queue, JobId::Spk(spk_hash)),
                            &mut self.cache,
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
                        Ok(self.on_new_tip(req_queue, resp.header, resp.height))
                    }
                }
            }
        }
    }

    /// Begin a pass towards a tip the server just announced, replacing any pass in progress: a
    /// new tip makes the old one moot.
    fn on_new_tip(
        &mut self,
        req_queue: &mut ReqQueue,
        header: bitcoin::block::Header,
        height: u32,
    ) -> Option<Update<K>> {
        // The notification carries the header for the announced height, which is what an anchor
        // there would otherwise have to ask for. It comes from the server's newest view, so it
        // displaces whatever was held there from an older one — but never a request already
        // outstanding, whose slot holds the stamp saying when it went out.
        if !outstanding(&self.cache.headers_at, &height) {
            self.cache.headers_at.insert(height, Observed::Seen(header));
        }
        self.chain_job = ChainJob::new(
            self.coord.queuer(req_queue, JobId::Chain),
            &self.cp,
            header,
            height,
        );
        self.drive_chain_job(req_queue)
    }

    /// Apply the chain pass if it now has everything it asked for.
    fn drive_chain_job(&mut self, req_queue: &mut ReqQueue) -> Option<Update<K>> {
        let job = self.chain_job.take()?;
        let before = self.cp.clone();
        match job.try_finish(&mut self.cp) {
            Ok(cp) => Some(self.on_chain_advanced(req_queue, before, cp)),
            Err(job) => {
                self.chain_job = Some(job);
                None
            }
        }
    }

    /// Spk jobs cannot extend the local chain, so a job whose anchor is above the local tip
    /// waits with no request in flight. Advancing the tip is what makes such an anchor
    /// resolvable, so every completed chain job must re-advance the stashed spk jobs.
    fn on_chain_advanced(
        &mut self,
        req_queue: &mut ReqQueue,
        before: CheckPoint,
        cp: CheckPoint,
    ) -> Update<K> {
        // The chain job applies its update by inserting blocks, and inserting a different block at
        // a height discards everything above it. The old tip surviving is therefore exactly the
        // condition that nothing was replaced.
        let reorged = self.cp.get(before.height()).map(|cp| cp.hash()) != Some(before.hash());
        if reorged {
            // Bumping the counter is what separates an answer still in flight from one asked for
            // after this reorg. Without it a burst of same-height reorgs is answered once, from
            // whichever chain the server held when it read the first request, and the anchor
            // settles there: `ReqCoord` merges each re-ask into the outstanding request rather
            // than putting one on the wire.
            self.cache.eviction_epoch += 1;
            let evicted = self.evicted_anchors(&before);
            self.reanchor.extend(evicted);
        }
        // `anchored_at` is read only by an eviction, and `headers_at` gains an entry for every
        // block from the tip notifications alone, so both would otherwise grow for the life of the
        // process while nothing below the horizon can ever be consulted again. A slot an
        // outstanding request owns is left wherever it is — it is the stamp its answer is checked
        // against — and a `Seen` header pruned out from under an anchor below the horizon costs
        // that anchor one refetch, not its correctness.
        let horizon = self.cp.height().saturating_sub(EVICTION_HORIZON);
        self.cache.anchored_at = self.cache.anchored_at.split_off(&horizon);
        self.cache
            .headers_at
            .retain(|&height, known| height >= horizon || matches!(known, Observed::Awaiting(_)));

        let stashed_jobs = self
            .spk_jobs
            .keys()
            .map(|&spk_hash| JobId::Spk(spk_hash))
            .collect::<Vec<_>>();
        let mut update = self
            .advance_anchor_work(req_queue, stashed_jobs)
            .unwrap_or_default();
        update.chain_update = Some(cp);
        update
    }

    /// The anchors whose blocks the chain held before this update and no longer holds.
    ///
    /// Their observations are discarded — a height that has come to mean a different block makes
    /// the header cached for it, and any proof checked against that header, worthless. The record
    /// itself is not: a re-ask that finds nothing has to be able to happen again, because the tx
    /// may be reorged back in at the same height with nothing but the chain to say so.
    fn evicted_anchors(&mut self, before: &CheckPoint) -> Vec<(u32, Txid)> {
        let now = self.cp.clone();
        let evicted = self
            .cache
            .anchored_at
            .iter()
            .filter(|(height, _)| {
                let height = **height;
                match before.get(height) {
                    Some(was) => now.get(height).map(|cp| cp.hash()) != Some(was.hash()),
                    None => false,
                }
            })
            .flat_map(|(&height, txids)| txids.iter().map(move |&txid| (height, txid)))
            .collect::<Vec<_>>();
        for &(height, txid) in &evicted {
            // A header observation is worthless once the chain disagrees with it — but a tip
            // notification carries the new block for its own height, so the observation is often
            // already the right one, and re-fetching what the chain agrees with buys nothing.
            let contradicted = match self.cache.headers_at.get(&height) {
                Some(Observed::Awaiting(_)) => false,
                Some(Observed::Seen(header)) => {
                    now.get(height).map(|cp| cp.hash()) != Some(header.block_hash())
                }
                _ => true,
            };
            if contradicted {
                self.cache.headers_at.remove(&height);
            }
            // A proof means nothing except against the header it was checked with, and that
            // height has just come to mean a different block.
            if !outstanding(&self.cache.proofs, &(txid, height)) {
                self.cache.proofs.remove(&(txid, height));
            }
        }
        if !evicted.is_empty() {
            tracing::info!(
                anchors = evicted.len(),
                "Blocks left the chain; re-asking for the anchors that were in them"
            );
        }
        evicted
    }

    /// Advance every anchor waiting on a re-ask.
    ///
    /// A same-height reorg leaves the script status untouched, so nothing will arrive to prompt
    /// this — the chain dropping blocks is the only signal there is.
    fn advance_reanchor(&mut self, req_queue: &mut ReqQueue) -> Option<Update<K>> {
        if self.reanchor.is_empty() {
            return None;
        }
        let mut tx_update = TxUpdate::<ConfirmationBlockTime>::default();
        let mut pending = BTreeSet::new();
        for (height, txid) in core::mem::take(&mut self.reanchor) {
            // A reorg onto a shorter chain leaves re-asks for heights our own chain no longer
            // reaches. There is nothing to ask about yet, so this waits — and waiting is what
            // makes it recoverable, since growing back over the height drives this again whereas
            // giving up would need an eviction to revive it, and growth is not an eviction.
            if height > self.cp.height() {
                pending.insert((height, txid));
                continue;
            }
            let mut queuer = self.coord.queuer(req_queue, JobId::Anchor);
            match resolve_anchor(&mut queuer, &mut self.cache, height, txid) {
                AnchorStep::Pending => {
                    pending.insert((height, txid));
                }
                AnchorStep::Resolved(anchor) => {
                    tx_update.anchors.insert((anchor, txid));
                }
                AnchorStep::Abandoned => {}
            }
        }
        self.reanchor = pending;
        if tx_update.anchors.is_empty() {
            return None;
        }
        Some(Update {
            tx_update,
            chain_update: Some(self.cp.clone()),
            ..Default::default()
        })
    }

    /// Advance the spk jobs a response belongs to, and the re-asks alongside them, since both are
    /// waiting on the same headers and proofs.
    fn advance_anchor_work(
        &mut self,
        req_queue: &mut ReqQueue,
        job_ids: impl IntoIterator<Item = JobId>,
    ) -> Option<Update<K>> {
        let mut update = self.advance_spk_jobs(req_queue, job_ids);
        if let Some(re_asked) = self.advance_reanchor(req_queue) {
            let update = update.get_or_insert(Update::default());
            update.tx_update.extend(re_asked.tx_update);
            update.chain_update = Some(self.cp.clone());
        }
        update
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
                    &mut self.cache,
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
                // Unreachable: a re-ask only ever issues a header or a proof request, and both
                // are answered above rather than reaching here. Clearing the set would be the
                // wrong response anyway — only an eviction revives a forgotten re-ask, and one is
                // not coming a second time for the same reorg.
                JobId::Anchor => {}
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

/// How far below the tip the eviction record is kept.
///
/// A height can only be evicted by a block being replaced, and the two writers of the chain both
/// stay above this: the chain pass rewrites a 21-block suffix, and the gap fill never conflicts
/// because it only writes where the chain holds nothing. Anything further down can therefore never
/// be evicted, so nothing below the horizon can ever be read back.
pub const EVICTION_HORIZON: u32 = 100;

/// What is known about something the server was asked for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Observed<T> {
    /// Asked for and still outstanding, carrying the eviction counter the request was issued
    /// against.
    Awaiting(u64),
    Seen(T),
    /// The server declined to produce it. Consumed on read rather than kept, because a JSON-RPC
    /// error says nothing durable — it is the everyday answer for a height the server has not
    /// indexed, or for a tx that is not in a block.
    Absent,
}

/// Whether a request for `key` is still outstanding, in which case the slot is that request's to
/// fill and nothing else may write to it: the stamp it holds records when the request went out,
/// and that is the only thing that can tell an answer still in flight from one asked for after
/// the reorg that invalidated it.
fn outstanding<K, T>(map: &HashMap<K, Observed<T>>, key: &K) -> bool
where
    K: Eq + core::hash::Hash,
{
    matches!(map.get(key), Some(Observed::Awaiting(_)))
}

/// File an answer against the request waiting for it, reporting whether it was filed.
///
/// An answer is only filed into a slot still awaiting one at the current eviction counter.
/// Anything else means the question has moved on: the slot was dropped when its block left the
/// chain, a tip notification has already answered it from a newer view, or — the case with no
/// other signal — a reorg landed after the request went out, so the answer describes a chain the
/// server has left. Dropping the slot rather than filling it is what makes the next pass ask
/// again, and that ask reaches the wire because [`ReqCoord`] has just released the request it
/// would otherwise have been merged into.
fn record<K, T>(map: &mut HashMap<K, Observed<T>>, key: K, answer: Option<T>, epoch: u64) -> bool
where
    K: Eq + core::hash::Hash,
{
    let issued = match map.get(&key) {
        Some(Observed::Awaiting(issued)) => *issued,
        _ => return false,
    };
    if issued < epoch {
        map.remove(&key);
        return false;
    }
    map.insert(key, answer.map_or(Observed::Absent, Observed::Seen));
    true
}

/// A monotonically growing cache.
#[derive(Debug, Clone, Default)]
pub struct Cache {
    pub spk_histories: HashMap<ElectrumScriptStatus, Vec<response::Tx>>,
    pub spk_txids: HashMap<ElectrumScriptHash, BTreeSet<Txid>>,
    pub txs: HashMap<Txid, Arc<Transaction>>,
    /// Verified facts: this txid is in this block. Keyed by block hash, which cannot come to mean
    /// a different block, and true forever once established.
    pub anchors: HashMap<(Txid, BlockHash), ConfirmationBlockTime>,
    /// What the server most recently said sits at a height — an observation, not a fact. A proof
    /// for a height is checked against this and against nothing else.
    pub headers_at: HashMap<u32, Observed<bitcoin::block::Header>>,
    /// Proofs as received. They carry no block hash, so alone they say only that a tx is in *some*
    /// block with a given merkle root.
    pub proofs: HashMap<(Txid, u32), Observed<response::TxMerkle>>,
    /// Which txs were last seen in a block at which height, so that blocks leaving the chain can
    /// say whose anchors need asking for again. Survives a re-ask that comes back with nothing.
    pub anchored_at: BTreeMap<u32, BTreeSet<Txid>>,
    /// Counts the chain updates that took blocks out of the chain. Stamped onto outstanding
    /// requests so an answer that predates a reorg can be told from one that follows it.
    pub eviction_epoch: u64,
}
