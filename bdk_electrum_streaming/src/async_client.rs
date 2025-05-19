//! Yoo

use electrum_streaming_client::{
    AsyncPendingRequest, MaybeBatch, RawNotificationOrResponse, RawRequest,
};
use futures::stream::FuturesUnordered;
use futures::{channel::mpsc, SinkExt, StreamExt};
use futures::{select, FutureExt, TryFutureExt};

use crate::{AsyncState, ReqQueue, Update};

/// Run [`State`] with the provided transport, update channel and client channel.
///
/// # Parameters
///
/// * The transport is provided with the `read` and `write` halfs separately.
/// * `update_tx` is the sending end of the update channel. Wallet updates will be sent through here.
/// * `client_rx` is the receiving end of the client channel. The sending end can be transformed
///   into a [`Client`] for requests.
pub async fn run_async<K, R, W>(
    state: &mut AsyncState<K>,
    update_tx: &mut mpsc::UnboundedSender<Update<K>>,
    client_rx: &mut mpsc::UnboundedReceiver<MaybeBatch<AsyncPendingRequest>>,
    read: R,
    mut write: W,
) -> anyhow::Result<()>
where
    K: Ord + Clone,
    R: futures::io::AsyncRead + Unpin,
    W: futures::io::AsyncWrite + Unpin,
{
    state.reset();

    let (mut write_tx, mut write_rx) = mpsc::unbounded::<RawRequest>();

    let mut read_stream =
        electrum_streaming_client::io::ReadStreamer::new(futures::io::BufReader::new(read));
    let mut req_queue = ReqQueue::new();
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
                            log::trace!(
                                method = raw_notification.method,
                                params = raw_notification.params.to_string();
                                "read_fut: got notification",
                            );
                        },
                        RawNotificationOrResponse::Response(raw_response) => {
                            log::trace!(
                                id = raw_response.id,
                                is_ok = raw_response.result.is_ok();
                                "read_fut: got response"
                            );
                        },
                    };
                    if let Some(update) = state.advance(&mut req_queue, raw)? {
                        update_tx.unbounded_send(update).map_err(|err| anyhow::anyhow!(err.to_string()))?;
                    }
                }
                opt = client_rx.next().fuse() => {
                    let batch = match opt {
                        Some(batch) => batch,
                        None => break,
                    };
                    state.user_request(&mut req_queue, batch);
                }
            };
        }

        println!("read_fut: Finish");
        write_tx.close_channel();
        anyhow::Ok(())
    }
    .inspect_err(|e| log::error!(err = e.to_string(); "read_fut: finished with error"))
    .inspect_ok(|_| log::debug!("read_fut finished cleanly"));

    let write_fut = async move {
        while let Some(req) = write_rx.next().await {
            log::trace!(method = req.method, id = req.id; "write_fut: writing");
            electrum_streaming_client::io::async_write(&mut write, req).await?;
        }
        println!("write_fut: Finish");
        anyhow::Ok(())
    }
    .inspect_err(|e| log::error!(err = e.to_string(); "write_fut: finished with error"))
    .inspect_ok(|_| log::debug!("write_fut: finished cleanly"));

    let mut futs = FuturesUnordered::new();
    futs.push(read_fut.left_future());
    futs.push(write_fut.right_future());
    while let Some(r) = futs.next().await {
        r?;
    }
    Ok(())
}
