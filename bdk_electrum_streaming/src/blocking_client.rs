use std::{
    io::{BufReader, ErrorKind, Read, Write},
    sync::{
        atomic::{self, AtomicBool},
        mpsc,
    },
};

use anyhow::Context;
use electrum_streaming_client::{
    BlockingBatchRequest, BlockingPendingRequest, BlockingPendingRequestTuple,
    RawNotificationOrResponse, RawRequest,
};
use miniscript::{Descriptor, DescriptorPublicKey};

use crate::{BlockingClientAction, BlockingState, ReqQueue, Update};

#[derive(Debug)]
pub struct BlockingClient<K> {
    client_tx: mpsc::Sender<BlockingClientAction<K>>,
}

pub type BlockingReceiver<K> = mpsc::Receiver<BlockingClientAction<K>>;

impl<K: Sync + Send + 'static> BlockingClient<K> {
    pub fn new() -> (BlockingClient<K>, BlockingReceiver<K>) {
        let (client_tx, client_rx) = mpsc::channel::<BlockingClientAction<K>>();
        (Self { client_tx }, client_rx)
    }

    pub fn send_batch_request(&self, batch_req: BlockingBatchRequest) -> anyhow::Result<bool> {
        match batch_req.into_inner() {
            Some(batch) => Ok(self
                .client_tx
                .send(BlockingClientAction::Request(batch.map_into()))
                .map(|_| true)?),
            None => Ok(false),
        }
    }

    pub fn send_request<Req>(&self, req: Req) -> anyhow::Result<Req::Response>
    where
        Req: electrum_streaming_client::Request,
        BlockingPendingRequestTuple<Req, Req::Response>: Into<BlockingPendingRequest>,
    {
        let mut batch = BlockingBatchRequest::new();
        let resp_rx = batch.request(req);
        self.send_batch_request(batch)
            .context("failed to send request to state machine")?;
        Ok(resp_rx
            .recv()
            .map_err(|_| anyhow::anyhow!("request got cancelled by the state machine"))??)
    }

    pub fn track_descriptor(
        &self,
        keychain: K,
        descriptor: Descriptor<DescriptorPublicKey>,
        next_index: u32,
    ) -> anyhow::Result<()> {
        let descriptor = Box::new(descriptor);
        Ok(self.client_tx.send(BlockingClientAction::AddDescriptor {
            keychain,
            descriptor,
            next_index,
        })?)
    }

    pub fn stop(self) -> anyhow::Result<()> {
        Ok(self.client_tx.send(BlockingClientAction::Stop)?)
    }
}

enum StateAction<K> {
    FromServer(RawNotificationOrResponse),
    FromClient(BlockingClientAction<K>),
}

pub fn run_blocking<'env, K, R, W>(
    state: &'env mut BlockingState<K>,
    shutdown: &'env AtomicBool,
    update_tx: &'env mut mpsc::Sender<Update<K>>,
    client_rx: &'env mut BlockingReceiver<K>,
    read: R,
    mut write: W,
) -> anyhow::Result<()>
where
    K: Ord + Clone + Send + 'env,
    R: Read + Send + 'env,
    W: Write + Send + 'env,
{
    shutdown.store(false, atomic::Ordering::Relaxed);

    fn shutdown_on_return<F, T>(shutdown: &AtomicBool, f: F) -> impl FnOnce() -> T + use<'_, F, T>
    where
        F: FnOnce() -> T,
    {
        move || {
            let r = f();
            shutdown.store(true, atomic::Ordering::Relaxed);
            r
        }
    }

    std::thread::scope::<'env>(|s| -> anyhow::Result<()> {
        let read_stream = electrum_streaming_client::io::ReadStreamer::new(BufReader::new(read));

        let mut req_queue = ReqQueue::new();
        state.reset();
        state.init(&mut req_queue);

        // For sending to the write thread.
        let (write_tx, write_rx) = mpsc::channel::<RawRequest>();
        // For sending to the state thread.
        let (state_tx, state_rx) = mpsc::channel::<StateAction<K>>();

        // Read thread.
        // Only errors on failed read.
        let read_state_tx = state_tx.clone();
        let read_join = s.spawn(shutdown_on_return(shutdown, move || {
            for r in read_stream {
                let raw = match r {
                    Err(e)
                        if e.kind() == ErrorKind::TimedOut || e.kind() == ErrorKind::WouldBlock =>
                    {
                        if shutdown.load(atomic::Ordering::Relaxed) {
                            break;
                        } else {
                            tracing::debug!("Read timeout");
                            continue;
                        }
                    }
                    other_result => other_result?,
                };

                // Purely for logging.
                match &raw {
                    RawNotificationOrResponse::Notification(raw_notification) => {
                        tracing::trace!(
                            method = &*raw_notification.method,
                            params = raw_notification.params.to_string(),
                            "read_thread: got notification",
                        );
                    }
                    RawNotificationOrResponse::Response(raw_response) => {
                        tracing::trace!(
                            id = raw_response.id,
                            is_ok = raw_response.result.is_ok(),
                            "read_thread: got response"
                        );
                    }
                };
                if read_state_tx.send(StateAction::FromServer(raw)).is_err() {
                    break;
                };
            }
            tracing::trace!("read_thread: finished cleanly");
            anyhow::Ok(())
        }));

        // Write thread.
        // Only errors on failed writes.
        let write_join = s.spawn(shutdown_on_return(shutdown, move || {
            while let Ok(req) = write_rx.recv() {
                if shutdown.load(atomic::Ordering::Relaxed) {
                    break;
                }
                tracing::trace!(method = &*req.method, id = req.id, "write_thread: writing");
                electrum_streaming_client::io::blocking_write(&mut write, req)?;
            }
            tracing::trace!("write_thread: finished cleanly");
            anyhow::Ok(())
        }));

        // Client thread.
        let _client_join = s.spawn(shutdown_on_return(shutdown, move || {
            while let Ok(batch) = client_rx.recv() {
                if shutdown.load(atomic::Ordering::Relaxed) {
                    break;
                }
                if state_tx.send(StateAction::FromClient(batch)).is_err() {
                    break;
                }
            }
            tracing::trace!("client_thread: finished cleanly");
        }));

        // State thread.
        let state_join = s.spawn(shutdown_on_return(shutdown, move || {
            loop {
                while let Some(req) = req_queue.pop_front() {
                    write_tx.send(req)?;
                }
                if shutdown.load(atomic::Ordering::Relaxed) {
                    break;
                }
                let action = match state_rx.recv() {
                    Ok(action) => action,
                    Err(_) => break,
                };
                match action {
                    StateAction::FromServer(raw) => {
                        if let Some(update) = state.advance(&mut req_queue, raw)? {
                            update_tx
                                .send(update)
                                .map_err(|err| anyhow::anyhow!(err.to_string()))
                                .context("Failed to send to update channel")?;
                        }
                    }
                    StateAction::FromClient(BlockingClientAction::Request(maybe_batch)) => {
                        state.user_request(&mut req_queue, maybe_batch.map(|br| *br));
                    }
                    StateAction::FromClient(BlockingClientAction::AddDescriptor {
                        keychain,
                        descriptor,
                        next_index,
                    }) => {
                        state.insert_descriptor(&mut req_queue, keychain, *descriptor, next_index);
                    }
                    StateAction::FromClient(BlockingClientAction::Stop) => {
                        drop(write_tx);
                        drop(state_rx);
                        break;
                    }
                }
            }
            tracing::trace!("state_thread: finished cleanly");
            anyhow::Ok(())
        }));

        read_join
            .join()
            .expect("read thread failed")
            .context("read thread stopped")?;
        write_join
            .join()
            .expect("write thread failed")
            .context("write thread stopped")?;
        state_join
            .join()
            .expect("state thread failed")
            .context("state thread stopped")?;
        Ok(())
    })
}
