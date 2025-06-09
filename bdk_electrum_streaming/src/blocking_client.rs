use std::{
    io::{BufReader, Read, Write},
    sync::mpsc,
};

use anyhow::Context;
use electrum_streaming_client::{
    BlockingPendingRequest, MaybeBatch, RawNotificationOrResponse, RawRequest,
};

use crate::{BlockingState, ReqQueue, Update};

enum StateAction {
    FromServer(RawNotificationOrResponse),
    FromClient(MaybeBatch<BlockingPendingRequest>),
}

pub fn run_blocking<'env, K, R, W>(
    state: &'env mut BlockingState<K>,
    update_tx: &'env mut mpsc::Sender<Update<K>>,
    client_rx: &'env mut mpsc::Receiver<MaybeBatch<BlockingPendingRequest>>,
    read: R,
    mut write: W,
) -> anyhow::Result<()>
where
    K: Ord + Clone + Send + 'env,
    R: Read + Send + 'env,
    W: Write + Send + 'env,
{
    std::thread::scope::<'env>(|s| -> anyhow::Result<()> {
        let read_stream = electrum_streaming_client::io::ReadStreamer::new(BufReader::new(read));

        let mut req_queue = ReqQueue::new();
        state.reset();
        state.init(&mut req_queue);

        // For sending to the write thread.
        let (write_tx, write_rx) = mpsc::channel::<RawRequest>();
        // For sending to the state thread.
        let (state_tx, state_rx) = mpsc::channel::<StateAction>();

        // Read thread.
        // Only errors on failed read.
        let read_state_tx = state_tx.clone();
        let read_join = s.spawn(move || {
            for r in read_stream {
                let raw = r?;

                // Purely for logging.
                match &raw {
                    RawNotificationOrResponse::Notification(raw_notification) => {
                        log::trace!(
                            method = raw_notification.method,
                            params = raw_notification.params.to_string();
                            "read_thread: got notification",
                        );
                    }
                    RawNotificationOrResponse::Response(raw_response) => {
                        log::trace!(
                            id = raw_response.id,
                            is_ok = raw_response.result.is_ok();
                            "read_thread: got response"
                        );
                    }
                };
                if read_state_tx.send(StateAction::FromServer(raw)).is_err() {
                    break;
                };
            }
            log::trace!("read_thread: finished cleanly");
            anyhow::Ok(())
        });

        // Write thread.
        // Only errors on failed writes.
        let write_join = s.spawn(move || {
            while let Ok(req) = write_rx.recv() {
                log::trace!(method = req.method, id = req.id; "write_thread: writing");
                electrum_streaming_client::io::blocking_write(&mut write, req)?;
            }
            log::trace!("write_thread: finished cleanly");
            anyhow::Ok(())
        });

        // Client thread.
        let _client_join = s.spawn(move || {
            while let Ok(batch) = client_rx.recv() {
                if state_tx.send(StateAction::FromClient(batch)).is_err() {
                    return;
                }
            }
        });

        // State thread.
        let state_join = s.spawn(move || {
            loop {
                while let Some(req) = req_queue.pop_front() {
                    write_tx.send(req)?;
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
                    StateAction::FromClient(maybe_batch) => {
                        for req in maybe_batch.into_vec() {
                            state.user_request(&mut req_queue, req);
                        }
                    }
                }
            }
            log::trace!("state_thread: finished cleanly");
            anyhow::Ok(())
        });

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
