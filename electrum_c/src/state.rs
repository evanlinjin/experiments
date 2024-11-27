use std::{
    collections::HashMap,
    io::{self, ErrorKind},
};

use bitcoin::block::Header;
use futures::{
    channel::mpsc::{self, UnboundedReceiver},
    select, AsyncRead, AsyncReadExt, AsyncWrite, Future, StreamExt,
};
use serde_json::Value;

use crate::{
    notification::Notification,
    pending_request::{ErroredRequest, PendingRequest, SatisfiedRequest},
    Client, CowStr, MaybeBatch, RawNotificationOrResponse, RawRequest,
};

#[derive(Debug, Clone)]
pub enum Event {
    Response(SatisfiedRequest),
    ResponseError(ErroredRequest),
    Notification(Notification),
}

impl Event {
    pub fn try_to_headers(&self) -> Option<Vec<(u32, Header)>> {
        match self {
            Event::Response(SatisfiedRequest::Header { req, resp }) => {
                Some(vec![(req.height, resp.header)])
            }
            Event::Response(SatisfiedRequest::Headers { req, resp }) => {
                Some((req.start_height..).zip(resp.headers.clone()).collect())
            }
            Event::Notification(Notification::Header(n)) => Some(vec![(n.height(), *n.header())]),
            _ => None,
        }
    }
}

pub struct State {
    next_id: usize,
    pending: HashMap<usize, PendingRequest>,
}

impl State {
    pub fn new(next_id: usize) -> Self {
        Self {
            next_id,
            pending: HashMap::new(),
        }
    }

    pub fn pending_requests(&self) -> impl Iterator<Item = RawRequest> + '_ {
        self.pending.iter().map(|(&id, pending_req)| {
            let (method, params) = pending_req.to_method_and_params();
            RawRequest::new(id, method, params)
        })
    }

    pub fn add_single_request(&mut self, req: PendingRequest) -> RawRequest {
        let id = self.next_id;
        self.next_id = id + 1;
        let (method, params) = req.to_method_and_params();
        self.pending.insert(id, req);
        RawRequest::new(id, method, params)
    }

    pub fn add_request<R>(&mut self, req: R) -> MaybeBatch<RawRequest>
    where
        R: Into<MaybeBatch<PendingRequest>>,
    {
        match req.into() {
            MaybeBatch::Single(req) => self.add_single_request(req).into(),
            MaybeBatch::Batch(v) => v
                .into_iter()
                .map(|req| self.add_single_request(req))
                .collect::<Vec<_>>()
                .into(),
        }
    }

    pub fn consume(
        &mut self,
        notification_or_response: RawNotificationOrResponse,
    ) -> Result<Option<Event>, ProcessError> {
        match notification_or_response {
            RawNotificationOrResponse::Notification(raw) => {
                let notification = Notification::new(&raw).map_err(|error| {
                    ProcessError::CannotDeserializeNotification {
                        method: raw.method,
                        params: raw.params,
                        error,
                    }
                })?;
                Ok(Some(Event::Notification(notification)))
            }
            RawNotificationOrResponse::Response(resp) => {
                let pending_req = self
                    .pending
                    .remove(&resp.id)
                    .ok_or(ProcessError::MissingRequest(resp.id))?;
                Ok(match resp.result {
                    Ok(raw_resp) => pending_req
                        .satisfy(raw_resp)
                        .map_err(|de_err| ProcessError::CannotDeserializeResponse(resp.id, de_err))?
                        .map(Event::Response),
                    Err(raw_err) => pending_req.satisfy_error(raw_err).map(Event::ResponseError),
                })
            }
        }
    }
}

#[derive(Debug)]
pub enum ProcessError {
    MissingRequest(usize),
    CannotDeserializeResponse(usize, serde_json::Error),
    CannotDeserializeNotification {
        method: CowStr,
        params: Value,
        error: serde_json::Error,
    },
}

impl std::fmt::Display for ProcessError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "")
    }
}

impl std::error::Error for ProcessError {}

pub fn run<C>(
    conn: C,
) -> (
    Client,
    UnboundedReceiver<Event>,
    impl Future<Output = io::Result<()>> + Send,
)
where
    C: AsyncRead + AsyncWrite + Send,
{
    let (event_tx, event_recv) = mpsc::unbounded::<Event>();
    let (req_tx, mut req_recv) = mpsc::unbounded::<MaybeBatch<PendingRequest>>();

    let (reader, mut writer) = conn.split();
    let mut incoming_stream =
        crate::io::ReadStreamer::new(futures::io::BufReader::new(reader)).fuse();
    let mut state = State::new(0);

    let fut = async move {
        loop {
            select! {
                req_opt = req_recv.next() => match req_opt {
                    Some(req) => {
                        let raw_req = state.add_request(req);
                        crate::io::write_async(&mut writer, raw_req).await?;
                    },
                    None => break,
                },
                incoming_opt = incoming_stream.next() => match incoming_opt {
                    Some(incoming_res) => {
                        let event_opt = state
                            .consume(incoming_res?)
                            .map_err(|error| std::io::Error::new(ErrorKind::Other, error))?;
                        if let Some(event) = event_opt {
                            if let Err(_err) = event_tx.unbounded_send(event) {
                                break;
                            }
                        }
                    },
                    None => break,
                }
            }
        }
        io::Result::<()>::Ok(())
    };

    (Client::new(req_tx), event_recv, fut)
}
