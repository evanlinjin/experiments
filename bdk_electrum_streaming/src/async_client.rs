//! Yoo

use anyhow::Context;
use electrum_streaming_client::{
    AsyncBatchRequest, AsyncPendingRequest, AsyncPendingRequestTuple, RawNotificationOrResponse,
    RawRequest,
};
use futures::{channel::mpsc, SinkExt, StreamExt};
use futures::{pin_mut, select, FutureExt, TryFutureExt};
use miniscript::{Descriptor, DescriptorPublicKey};

use crate::{AsyncClientAction, AsyncState, ReqQueue, Update};

#[derive(Debug, Clone)]
pub struct AsyncClient<K> {
    client_tx: mpsc::UnboundedSender<AsyncClientAction<K>>,
}

pub type AsyncReceiver<K> = mpsc::UnboundedReceiver<AsyncClientAction<K>>;

impl<K: Sync + Send + 'static> AsyncClient<K> {
    pub fn new() -> (AsyncClient<K>, AsyncReceiver<K>) {
        let (client_tx, client_rx) = mpsc::unbounded();
        (Self { client_tx }, client_rx)
    }

    pub fn from_sender(client_tx: mpsc::UnboundedSender<AsyncClientAction<K>>) -> AsyncClient<K> {
        Self { client_tx }
    }

    pub fn send_batch_request(&self, batch_req: AsyncBatchRequest) -> anyhow::Result<bool> {
        match batch_req.into_inner() {
            Some(batch) => Ok(self
                .client_tx
                .unbounded_send(AsyncClientAction::Request(batch))
                .map(|_| true)?),
            None => todo!(),
        }
    }

    pub async fn send_request<Req>(&self, req: Req) -> anyhow::Result<Req::Response>
    where
        Req: electrum_streaming_client::Request,
        AsyncPendingRequestTuple<Req, Req::Response>: Into<AsyncPendingRequest>,
    {
        let mut batch = AsyncBatchRequest::new();
        let resp_rx = batch.request(req);
        self.send_batch_request(batch)
            .context("failed to send request to state machine")?;
        Ok(resp_rx.await?)
    }

    pub fn track_descriptor<D>(
        &self,
        keychain: K,
        descriptor: D,
        next_index: u32,
    ) -> anyhow::Result<()>
    where
        D: Into<Box<Descriptor<DescriptorPublicKey>>>,
    {
        Ok(self
            .client_tx
            .unbounded_send(AsyncClientAction::AddDescriptor {
                keychain,
                descriptor: descriptor.into(),
                next_index,
            })?)
    }

    pub async fn stop(&self) -> anyhow::Result<()> {
        Ok(self.client_tx.unbounded_send(AsyncClientAction::Stop)?)
    }
}

/// Run [`State`](crate::State) with the provided transport, update channel and client channel.
///
/// # Parameters
///
/// * The transport is provided with the `read` and `write` halfs separately.
/// * `update_tx` is the sending end of the update channel. Wallet updates will be sent through here.
/// * `client_rx` is the receiving end of the client channel. The sending end can be transformed
///   into a [`AsyncClient`] for requests.
pub async fn run_async<K, R, W>(
    state: &mut AsyncState<K>,
    update_tx: &mut mpsc::UnboundedSender<Update<K>>,
    client_rx: &mut AsyncReceiver<K>,
    read: R,
    write: W,
) -> anyhow::Result<()>
where
    K: Ord + Clone,
    R: futures::io::AsyncRead + Unpin,
    W: futures::io::AsyncWrite + Unpin,
{
    let res = _run_async(state, update_tx, client_rx, read, write).await;
    res
}

async fn _run_async<K, R, W>(
    state: &mut AsyncState<K>,
    update_tx: &mut mpsc::UnboundedSender<Update<K>>,
    client_rx: &mut AsyncReceiver<K>,
    read: R,
    mut write: W,
) -> anyhow::Result<()>
where
    K: Ord + Clone,
    R: futures::io::AsyncRead + Unpin,
    W: futures::io::AsyncWrite + Unpin,
{
    let (mut write_tx, mut write_rx) = mpsc::unbounded::<RawRequest>();

    let mut read_stream =
        electrum_streaming_client::io::ReadStreamer::new(futures::io::BufReader::new(read));
    let mut req_queue = ReqQueue::new();
    state.reset();
    state.init(&mut req_queue);

    let read_fut = async move {
        loop {
            while let Some(req) = req_queue.pop_front() {
                write_tx.feed(req).await?;
            }
            write_tx.flush().await?;

            select! {
                opt = read_stream.next().fuse() => {
                    let raw = match opt {
                        Some(r) => r?,
                        None => break,
                    };
                    match &raw {
                        RawNotificationOrResponse::Notification(raw_notification) => {
                            tracing::trace!(
                                method = &*raw_notification.method,
                                params = raw_notification.params.to_string(),
                                "Read raw notification",
                            );
                        },
                        RawNotificationOrResponse::Response(raw_response) => {
                            tracing::trace!(
                                id = raw_response.id,
                                is_ok = raw_response.result.is_ok(),
                                "Read raw response"
                            );
                        },
                    };
                    if let Some(update) = state.advance(&mut req_queue, raw)? {
                        update_tx.unbounded_send(update).map_err(|err| anyhow::anyhow!(err.to_string()))?;
                    }
                }
                opt = client_rx.next().fuse() => {
                    let action = match opt {
                        Some(action) => action,
                        None => break,
                    };
                    match action {
                        crate::ClientAction::Request(batch) => {
                            state.user_request(&mut req_queue, batch);
                        },
                        crate::ClientAction::AddDescriptor { keychain, descriptor, next_index } => {
                            state.insert_descriptor(&mut req_queue, keychain, *descriptor, next_index);
                        },
                        crate::ClientAction::Stop => {
                            tracing::info!("Client sent stop signal");                           
                            break;
                        },
                    }
                }
            };
        }

        tracing::debug!("Finished read future");
        write_tx.close_channel();
        anyhow::Ok(())
    }
    .inspect_err(|e| tracing::error!(err = e.to_string(), "Finished read future with error"))
    .inspect_ok(|_| tracing::debug!("Finished read future cleanly"));

    let write_fut = async move {
        while let Some(req) = write_rx.next().await {
            tracing::debug!(
                method = &*req.method,
                id = req.id,
                params = ?req.params,
                "Writing"
            );
            electrum_streaming_client::io::async_write(&mut write, req).await?;
        }
        println!("Finished write future");
        anyhow::Ok(())
    }
    .inspect_err(|e| tracing::error!(err = e.to_string(), "Finished write future with error"))
    .inspect_ok(|_| tracing::debug!("Finished write future cleanly"));

    pin_mut!(read_fut);
    pin_mut!(write_fut);

    select! {
        result = read_fut => result?,
        result = write_fut => result?,
    }
    Ok(())
}
