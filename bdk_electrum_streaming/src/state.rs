use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
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
    Spk(ElectrumScriptHash),
    Chain,
}

/// We separate this out so that we can easily pass this into methods that advance jobs.
#[derive(Debug)]
pub(crate) struct StateInner<K> {
    pub(crate) cp: CheckPoint,
    pub(crate) cache: Cache,
    pub(crate) spk_tracker: DerivedSpkTracker<K>,
}

#[derive(Debug)]
pub struct State<K> {
    inner: StateInner<K>,
    coord: ReqCoord,

    spk_jobs: BTreeMap<ElectrumScriptHash, SpkJob<K>>,
    chain_job: Option<ChainJob>,
}

impl<K: Ord + Clone> State<K> {
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
                            self.inner.cp.clone(),
                            *header_notification.header(),
                            header_notification.height(),
                        );
                        if let Some(job) = self.chain_job.take() {
                            match job.try_finish(&mut self.inner.cache, &mut self.inner.cp) {
                                Ok(cp) => Ok(Some(Update {
                                    chain_update: Some(cp),
                                    ..Default::default()
                                })),
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
                        let mut queuer = self.coord.queuer(req_queue, JobId::Spk(spk_hash));
                        let mut job =
                            SpkJob::new(&mut queuer, &mut self.inner, spk_hash, spk_status);
                        job = job.advance(&mut queuer, &self.inner);
                        match job.try_finish(&self.inner) {
                            Some(resp) => Ok(Some(resp)),
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
                let orig_req = match self.coord.pop(raw_response.id) {
                    Some(req) => req,
                    None => return Ok(None),
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
                                Ok(cp) => {
                                    return Ok(Some(Update {
                                        chain_update: Some(cp),
                                        ..Default::default()
                                    }))
                                }
                                Err(job) => {
                                    self.chain_job = Some(job);
                                    return Ok(None);
                                }
                            }
                        }
                    }
                    AnyRequest::GetHistory(get_history) => todo!(),
                    AnyRequest::GetTx(get_tx) => todo!(),
                    AnyRequest::GetTxMerkle(get_tx_merkle) => todo!(),
                    AnyRequest::ScriptHashSubscribe(script_hash_subscribe) => todo!(),
                };
                Ok(None)
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
    pub failed_anchors: HashSet<(Txid, BlockHash)>,
    pub headers: HashMap<BlockHash, bitcoin::block::Header>,
}
