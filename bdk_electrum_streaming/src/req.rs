use std::collections::{hash_map, BTreeSet, HashMap, VecDeque};

use electrum_streaming_client::{request, RawRequest, Request};

use crate::JobId;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AnyRequest {
    GetHeader(request::Header),
    GetHeaders(request::Headers),
    GetHistory(request::GetHistory),
    GetTx(request::GetTx),
    GetTxMerkle(request::GetTxMerkle),
    ScriptHashSubscribe(request::ScriptHashSubscribe),
    HeadersSubscribe(request::HeadersSubscribe),
}

impl AnyRequest {
    pub fn into_raw(self, req_id: usize) -> RawRequest {
        let (method, params) = match self {
            AnyRequest::GetHeader(header) => header.to_method_and_params(),
            AnyRequest::GetHeaders(headers) => headers.to_method_and_params(),
            AnyRequest::GetHistory(get_history) => get_history.to_method_and_params(),
            AnyRequest::GetTx(get_tx) => get_tx.to_method_and_params(),
            AnyRequest::GetTxMerkle(get_tx_merkle) => get_tx_merkle.to_method_and_params(),
            AnyRequest::ScriptHashSubscribe(script_hash_subscribe) => {
                script_hash_subscribe.to_method_and_params()
            }
            AnyRequest::HeadersSubscribe(headers_subscribe) => {
                headers_subscribe.to_method_and_params()
            }
        };
        RawRequest::new(req_id, method, params)
    }
}

impl From<request::Header> for AnyRequest {
    fn from(value: request::Header) -> Self {
        Self::GetHeader(value)
    }
}

impl From<request::Headers> for AnyRequest {
    fn from(value: request::Headers) -> Self {
        Self::GetHeaders(value)
    }
}

impl From<request::GetHistory> for AnyRequest {
    fn from(value: request::GetHistory) -> Self {
        Self::GetHistory(value)
    }
}

impl From<request::GetTx> for AnyRequest {
    fn from(value: request::GetTx) -> Self {
        Self::GetTx(value)
    }
}

impl From<request::GetTxMerkle> for AnyRequest {
    fn from(value: request::GetTxMerkle) -> Self {
        Self::GetTxMerkle(value)
    }
}

impl From<request::ScriptHashSubscribe> for AnyRequest {
    fn from(value: request::ScriptHashSubscribe) -> Self {
        Self::ScriptHashSubscribe(value)
    }
}

impl From<request::HeadersSubscribe> for AnyRequest {
    fn from(value: request::HeadersSubscribe) -> Self {
        Self::HeadersSubscribe(value)
    }
}

/// Request coordinator.
///
/// Associates responses to their requests and requests to their jobs.
#[derive(Debug, Clone)]
pub struct ReqCoord {
    /// Next request id.
    next_id: usize,
    /// Req id -> Req.
    awaiting_responses: HashMap<usize, AnyRequest>,
    /// So we won't have duplicate requests.
    req_to_job: HashMap<AnyRequest, BTreeSet<JobId>>,
}

impl ReqCoord {
    pub fn new(next_id: usize) -> Self {
        Self {
            next_id,
            awaiting_responses: HashMap::new(),
            req_to_job: HashMap::new(),
        }
    }

    pub fn pop(&mut self, req_id: usize) -> Option<(AnyRequest, BTreeSet<JobId>)> {
        let any_req = self.awaiting_responses.remove(&req_id)?;
        let job_ids = self.req_to_job.remove(&any_req).unwrap_or_default();
        Some((any_req, job_ids))
    }

    pub fn queuer<'q>(&'q mut self, queue: &'q mut ReqQueue, job_id: JobId) -> ReqQueuer<'q> {
        let coord = self;
        ReqQueuer {
            coord,
            queue,
            job_id,
        }
    }
}

/// Queue of raw requests.
pub type ReqQueue = VecDeque<RawRequest>;

/// Queues requests to broadcast so that once the response is received, we can determine it's
/// response type and associated jobs.
#[derive(Debug)]
pub struct ReqQueuer<'q> {
    coord: &'q mut ReqCoord,
    queue: &'q mut ReqQueue,
    job_id: JobId,
}

impl<'q> ReqQueuer<'q> {
    pub fn enqueue<R: Into<AnyRequest>>(&mut self, req: R) {
        let req: AnyRequest = req.into();
        match self.coord.req_to_job.entry(req) {
            hash_map::Entry::Occupied(mut e) => {
                e.get_mut().insert(self.job_id);
            }
            hash_map::Entry::Vacant(e) => {
                e.insert(BTreeSet::new()).insert(self.job_id);
                let req_id = self.coord.next_id;
                self.coord.next_id += 1;
                self.coord.awaiting_responses.insert(req_id, req);
                self.queue.push_back(req.into_raw(req_id));
            }
        }
    }
}
