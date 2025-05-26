use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::Arc,
};

use anyhow::Context;
use bdk_core::{
    bitcoin::{self, BlockHash, Transaction, Txid},
    CheckPoint, ConfirmationBlockTime,
};
use electrum_streaming_client::{
    notification::Notification, request, response, ElectrumScriptHash, ElectrumScriptStatus,
    RawNotificationOrResponse, Request,
};

use crate::{
    chain_job::ChainJob,
    req::{AnyRequest, ReqCoord, ReqQueue},
    spk_job::SpkJob,
    DerivedSpkTracker, Update,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum JobId {
    Spk(u64),
    Chain,
}

impl Default for JobId {
    fn default() -> Self {
        Self::Spk(0)
    }
}

/// We separate this out so that we can easily pass this into methods that advance jobs.
#[derive(Debug)]
pub(crate) struct StateInner<K> {
    pub(crate) cp: CheckPoint,
    pub(crate) cache: Cache,
    pub(crate) coord: ReqCoord,
    pub(crate) spk_tracker: DerivedSpkTracker<K>,
}

#[derive(Debug)]
pub struct State<K> {
    inner: StateInner<K>,

    next_spk_job_id: u64,
    spk_jobs: BTreeMap<u64, SpkJob<K>>,

    chain_job: Option<ChainJob>,
}

impl<K> State<K> {
    fn _next_spk_job_id(&mut self) -> JobId {
        let id = self.next_spk_job_id;
        self.next_spk_job_id += 1;
        JobId::Spk(id)
    }

    pub fn advance(
        &mut self,
        update: &mut Update<K>,
        req_queue: &mut ReqQueue,
        raw: RawNotificationOrResponse,
    ) -> anyhow::Result<()> {
        match raw {
            RawNotificationOrResponse::Notification(raw_notification) => {
                let notification = Notification::new(&raw_notification)
                    .context("Failed to deserialize notification from server")?;
                match notification {
                    Notification::Header(header_notification) => {
                        // Always replace prev job since a new notification means a new tip.
                        self.chain_job = ChainJob::new(
                            self.inner.coord.queuer(req_queue, JobId::Chain),
                            self.inner.cp.clone(),
                            *header_notification.header(),
                            header_notification.height(),
                        );
                        if let Some(job) = self.chain_job.take() {
                            match job.try_finish(&mut self.inner.cache, &mut self.inner.cp) {
                                Ok(cp) => update.chain_update = Some(cp),
                                Err(job) => self.chain_job = Some(job),
                            }
                        }
                        Ok(())
                    }
                    Notification::ScriptHash(script_hash_notification) => todo!(),
                    Notification::Unknown(_) => Ok(()),
                }
            }
            RawNotificationOrResponse::Response(raw_response) => {
                let orig_req = match self.inner.coord.pop(raw_response.id) {
                    Some(req) => req,
                    None => return Ok(()),
                };
                let raw = raw_response
                    .result
                    .map_err(|err| anyhow::anyhow!(err))
                    .context("Server responded with error")?;
                match orig_req {
                    AnyRequest::GetHeader(header) => todo!(),
                    AnyRequest::GetHeaders(headers) => {
                        let resp =
                            serde_json::from_value::<<request::Headers as Request>::Response>(raw)?;
                        if let Some(job) = self.chain_job.take() {
                            match job
                                .process_headers((headers.start_height..).zip(resp.headers))
                                .try_finish(&mut self.inner.cache, &mut self.inner.cp)
                            {
                                Ok(cp) => update.chain_update = Some(cp),
                                Err(job) => self.chain_job = Some(job),
                            }
                        }
                    }
                    AnyRequest::GetHistory(get_history) => todo!(),
                    AnyRequest::GetTx(get_tx) => todo!(),
                    AnyRequest::GetTxMerkle(get_tx_merkle) => todo!(),
                    AnyRequest::ScriptHashSubscribe(script_hash_subscribe) => todo!(),
                };
                Ok(())
            }
        }
    }
}

/// A monotonically growing cache.
#[derive(Debug, Clone)]
pub struct Cache {
    pub spk_histories: HashMap<ElectrumScriptStatus, Vec<response::Tx>>,
    pub spk_txids: HashMap<ElectrumScriptHash, BTreeSet<Txid>>,
    pub txs: HashMap<Txid, Arc<Transaction>>,
    pub anchors: HashMap<(Txid, BlockHash), ConfirmationBlockTime>,
    pub headers: HashMap<BlockHash, bitcoin::block::Header>,
}
