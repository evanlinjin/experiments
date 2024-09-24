use futures::{
    channel::mpsc::{self},
    pin_mut, select, AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, FutureExt, SinkExt,
    StreamExt,
};
use serde_json::Value;
use std::{
    collections::{HashMap, HashSet},
    io::ErrorKind,
};

use crate::{
    Action, Client, CowStr, IncomingObject, NotificationReceiver, NotificationSender,
    OutgoingObject, RequestMethodAndParams, ResponseSender, JSONRPC_VERSION_2_0, SUBSCRIBE_SUFFIX,
    UNSUBSCRIBE_SUFFIX,
};

#[derive(Debug)]
pub struct Service {
    next_id: usize,
    action_tx: mpsc::UnboundedSender<Action>,
    action_rx: mpsc::UnboundedReceiver<Action>,
    notification_txs: Vec<NotificationSender>,
    notification_requests: HashSet<RequestMethodAndParams>,
    pending_requests: HashMap<usize, (RequestMethodAndParams, ResponseSender)>,
}

impl Default for Service {
    fn default() -> Self {
        Self::new()
    }
}

impl Service {
    pub fn new() -> Self {
        Self::with_next_id(0)
    }

    pub fn with_next_id(next_id: usize) -> Self {
        let (action_tx, action_rx) = mpsc::unbounded();
        Self {
            next_id,
            action_tx,
            action_rx,
            notification_txs: Vec::new(),
            notification_requests: HashSet::new(),
            pending_requests: HashMap::new(),
        }
    }

    pub fn spawn_client(&self) -> Client {
        Client {
            action_tx: self.action_tx.clone(),
        }
    }

    pub fn spawn_notification_receiver(&mut self) -> NotificationReceiver {
        let (notification_tx, notification_rx) = mpsc::unbounded();
        self.notification_txs.push(notification_tx);
        notification_rx
    }

    pub fn insert_notification_sender(&mut self, tx: NotificationSender) {
        self.notification_txs.push(tx);
    }

    pub async fn run<T>(&mut self, transport: T) -> Result<(), std::io::Error>
    where
        T: AsyncRead + AsyncWrite,
    {
        let conn = futures::io::BufReader::new(transport);
        pin_mut!(conn);
        let mut read_buf = Vec::<u8>::new();

        conn.write_all(&self.requests_to_resend()).await?;

        loop {
            select! {
                action_opt = self.action_rx.next() => match action_opt {
                    Some(Action::Stop) | None => return Ok(()),
                    Some(Action::Call { method, params, resp_tx }) => {
                        let id = next_id(&mut self.next_id);
                        self.pending_requests.insert(id, ((method.clone(), params.clone()), resp_tx));
                        let obj = OutgoingObject::new(id, method, params);
                        let mut req_bytes = serde_json::to_vec(&obj).expect("must serialize request");
                        req_bytes.push(b'\n');
                        conn.write_all(&req_bytes).await?;
                    },
                },
                read_res = conn.read_until(b'\n', &mut read_buf).fuse() => {
                    let read_len = read_res?;
                    if read_len == 0 {
                        // EOF
                        return Err(std::io::Error::new(ErrorKind::UnexpectedEof, "server sent unexpected EOF"));
                    }
                    //assert!(read_len > 0, "read_buf = {:?}", read_buf);
                    assert_eq!(read_buf.pop(), Some(b'\n'));
                    let obj = serde_json::from_slice::<IncomingObject>(&read_buf)?;
                    read_buf.clear();
                    self.handle_incoming_obj(obj).await;
                },
            }
        }
    }

    fn requests_to_resend(&mut self) -> Vec<u8> {
        let mut to_resend = Vec::<(usize, CowStr, Vec<Value>)>::with_capacity(
            self.notification_requests.len() + self.pending_requests.len(),
        );
        for (method, params) in &self.notification_requests {
            let id = next_id(&mut self.next_id);
            to_resend.push((id, method.clone(), params.clone()));
        }
        for (id, ((method, params), _)) in &self.pending_requests {
            to_resend.push((*id, method.clone(), params.clone()));
        }
        let mut write_buf = Vec::<u8>::new();
        for (id, method, params) in to_resend {
            let obj = OutgoingObject::new(id, method, params);
            serde_json::to_writer(&mut write_buf, &obj).expect("must serialize and write request");
            std::io::Write::write_all(&mut write_buf, b"\n").expect("must write request delimiter");
        }
        write_buf
    }

    /// Handle incoming object from Electrum server.
    async fn handle_incoming_obj(&mut self, obj: IncomingObject) {
        if obj.version_str() != JSONRPC_VERSION_2_0 {
            // TODO: Report incorrect version str.
        }

        match obj {
            IncomingObject::ResultResponse { id, result, .. } => {
                let (method, params) = match self.pending_requests.remove(&id) {
                    Some((req, resp_tx)) => {
                        let _ = resp_tx.send(Ok(result));
                        // TODO: notify caller that the response channel is closed.
                        req
                    }
                    None => {
                        // TODO: Notify caller that we received response with no corresponding
                        // request id.
                        return;
                    }
                };
                if method.ends_with(SUBSCRIBE_SUFFIX) {
                    self.notification_requests.insert((method, params));
                } else if method.ends_with(UNSUBSCRIBE_SUFFIX) {
                    let subscribe_method =
                        method.trim_end_matches(UNSUBSCRIBE_SUFFIX).to_string() + SUBSCRIBE_SUFFIX;
                    self.notification_requests
                        .remove(&(subscribe_method.into(), params));
                }
            }
            IncomingObject::ErrorResponse { id, error, .. } => {
                match self.pending_requests.remove(&id) {
                    Some((_, resp_tx)) => {
                        let _ = resp_tx.send(Err(error));
                        // TODO: notify caller that the response channel is closed.
                    }
                    None => {
                        // TODO: notify caller that we received response with no corresponding
                        // request it.
                    }
                }
            }
            IncomingObject::Notification { method, params, .. } => {
                let mut failures = Vec::<usize>::new();
                for (i, notify_tx) in &mut self.notification_txs.iter_mut().enumerate() {
                    if notify_tx
                        .send((method.clone(), params.clone()))
                        .await
                        .is_err()
                    {
                        failures.push(i);
                    }
                }
                while let Some(i) = failures.pop() {
                    self.notification_txs.swap_remove(i);
                }
            }
        }

        // TODO: Unexpected incoming msg. What to do?
    }
}

fn next_id(id: &mut usize) -> usize {
    core::mem::replace(id, *id + 1)
}
