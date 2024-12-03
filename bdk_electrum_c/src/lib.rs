//! BDK Electrum goodness.

use bdk_core::spk_client::FullScanResult;
use electrum_c::notification::Notification;
use electrum_c::pending_request::{ErroredRequest, SatisfiedRequest};
use electrum_c::{request, Client, ElectrumScriptHash, Event, ResponseError};
use futures::channel::mpsc::{self, UnboundedReceiver};
use futures::channel::oneshot;
use futures::{select, AsyncRead, AsyncWrite, Future, FutureExt, StreamExt};

use std::collections::{btree_map, hash_map, BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;

use bdk_core::bitcoin::block::Header;
use bdk_core::bitcoin::{BlockHash, OutPoint, Transaction, TxOut, Txid};
use bdk_core::{collections::HashMap, CheckPoint};
use bdk_core::{BlockId, ConfirmationBlockTime, TxUpdate};
use miniscript::{Descriptor, DescriptorPublicKey};

pub type Update<K> = FullScanResult<K, ConfirmationBlockTime>;
pub type HeaderCache = HashMap<u32, (BlockHash, Header)>;

/// Keeps track of spks.
///
/// This manages subscriptions to spk histories.
/// * When we reconnect with the Electrum server, we wish to resubscribe to all spks.
/// * When we receive history for a spk, we wish to ensure `lookahead` number of spks above are
///   also tracked.
/// * When we receive history for a spk, we wish to include a `last_active_index` update in case
///   `KeychainTxOutIndex` is not up-to-date.
#[derive(Debug, Clone)]
pub struct DerivedSpkTracker<K: Clone + Ord + Send + Sync + 'static> {
    lookahead: u32,
    descriptors: BTreeMap<K, Descriptor<DescriptorPublicKey>>,
    derived_spks: BTreeMap<(K, u32), ElectrumScriptHash>,
    derived_spks_rev: HashMap<ElectrumScriptHash, (K, u32)>,
}

impl<K: Clone + Ord + Send + Sync + 'static> DerivedSpkTracker<K> {
    pub fn new(lookahead: u32) -> Self {
        Self {
            lookahead,
            descriptors: BTreeMap::new(),
            derived_spks: BTreeMap::new(),
            derived_spks_rev: HashMap::new(),
        }
    }

    pub fn all_spk_hashes(&self) -> impl Iterator<Item = ElectrumScriptHash> + '_ {
        self.derived_spks.values().copied()
    }

    fn _add_derived_spk(&mut self, keychain: K, index: u32) -> Option<ElectrumScriptHash> {
        if let btree_map::Entry::Vacant(spk_hash_entry) =
            self.derived_spks.entry((keychain.clone(), index))
        {
            let descriptor = self
                .descriptors
                .get(&keychain)
                .expect("keychain must have associated descriptor");
            let spk = descriptor
                .at_derivation_index(index)
                .expect("descriptor must derive")
                .script_pubkey();
            let script_hash = ElectrumScriptHash::new(&spk);
            spk_hash_entry.insert(script_hash);
            assert!(self
                .derived_spks_rev
                .insert(script_hash, (keychain, index))
                .is_none());
            return Some(script_hash);
        }
        None
    }

    fn _clear_tracked_spks_of_keychain(&mut self, keychain: K) {
        let split = {
            let mut split = self.derived_spks.split_off(&(keychain.clone(), 0));
            let to_add_back = split.split_off(&(keychain, u32::MAX)); // `u32::MAX` is never derived
            self.derived_spks.extend(to_add_back);
            split
        };
        for script_hash in split.into_values() {
            self.derived_spks_rev.remove(&script_hash);
        }
    }

    pub fn insert_descriptor(
        &mut self,
        keychain: K,
        descriptor: Descriptor<DescriptorPublicKey>,
        next_index: u32,
    ) -> Vec<ElectrumScriptHash> {
        if let Some(old_descriptor) = self
            .descriptors
            .insert(keychain.clone(), descriptor.clone())
        {
            if old_descriptor == descriptor {
                return vec![];
            }
            self._clear_tracked_spks_of_keychain(keychain.clone());
        }
        (0_u32..=next_index + self.lookahead + 1)
            .filter_map(|index| self._add_derived_spk(keychain.clone(), index))
            .collect()
    }

    pub fn handle_script_status(
        &mut self,
        script_hash: ElectrumScriptHash,
    ) -> Option<(K, u32, Vec<ElectrumScriptHash>)> {
        let (k, mut next_index) = self.derived_spks_rev.get(&script_hash).cloned()?;
        next_index += 1;

        let mut spk_hashes = Vec::new();
        for index in (next_index..=next_index + 1 + self.lookahead).rev() {
            match self._add_derived_spk(k.clone(), index) {
                Some(spk_hash) => spk_hashes.push(spk_hash),
                None => break,
            }
        }
        Some((k, next_index, spk_hashes))
    }
}

/// This is a plceholder until we can put headers in checkpoints.
#[derive(Debug)]
pub struct Headers {
    tip: CheckPoint,
    headers: HashMap<BlockHash, Header>,
}

impl Headers {
    pub fn new(tip: CheckPoint) -> Self {
        Self {
            tip,
            headers: HashMap::new(),
        }
    }

    pub fn tip(&self) -> CheckPoint {
        self.tip.clone()
    }

    pub async fn update(
        &mut self,
        client: &Client,
        tip_height: u32,
        tip_hash: BlockHash,
    ) -> anyhow::Result<Option<CheckPoint>> {
        const ASSUME_FINAL_DEPTH: u32 = 8;
        const CONSECUTIVE_THRESHOLD: usize = 3;

        // TODO: Get rid of `ASSUME_FINAL_DEPTH`. Instead, get headers one by one and stop when we
        // connect with a checkpoint.
        let start_height = tip_height.saturating_sub(ASSUME_FINAL_DEPTH);
        let count = (tip_height - start_height) as usize;
        let mut new_headers = (start_height..)
            .zip(
                client
                    .request(request::Headers {
                        start_height,
                        count,
                        cp_height: None,
                    })
                    .await?
                    .headers,
            )
            .collect::<BTreeMap<u32, Header>>();

        // Check that the tip is still the same.
        if new_headers.get(&tip_height).map(|h| h.block_hash()) != Some(tip_hash) {
            // It is safe to ignore this update and wait for the next notification.
            return Ok(None);
        }

        // Ensure local recent headers are still in the best chain.
        let mut consecutive_matches = 0_usize;
        for cp in self.tip.iter() {
            let height = cp.height();
            let orig_hash = cp.hash();
            let header = match new_headers.entry(height) {
                btree_map::Entry::Vacant(e) => {
                    *e.insert(client.request(request::Header { height }).await?.header)
                }
                btree_map::Entry::Occupied(e) => *e.get(),
            };
            let hash = header.block_hash();
            self.headers.insert(hash, header);
            if header.block_hash() == orig_hash {
                consecutive_matches += 1;
                if consecutive_matches > CONSECUTIVE_THRESHOLD {
                    break;
                }
            } else {
                consecutive_matches = 0;
            }
        }
        for (height, header) in new_headers {
            let hash = header.block_hash();
            self.tip = self.tip.clone().insert(BlockId { height, hash });
        }
        Ok(Some(self.tip.clone()))
    }

    pub async fn ensure_heights(
        &mut self,
        client: &Client,
        heights: BTreeSet<u32>,
    ) -> anyhow::Result<HeaderCache> {
        let mut header_cache = HeaderCache::new();

        // Anything above this height is ignored.
        let tip_height = self.tip.height();

        let mut heights_iter = heights.into_iter().filter(|&h| h <= tip_height).peekable();
        let start_height = match heights_iter.peek() {
            Some(&h) => h,
            None => return Ok(header_cache),
        };

        let mut cp_tail = BTreeMap::<u32, BlockHash>::new();
        let start_cp = {
            let mut start_cp = Option::<CheckPoint>::None;
            for cp in self.tip.iter() {
                let BlockId { height, hash } = cp.block_id();
                if height < start_height {
                    start_cp = Some(cp);
                    break;
                }
                cp_tail.insert(height, hash);
            }
            match start_cp {
                Some(cp) => cp,
                // TODO: Is this the correct thing to do when we don't have a start cp?
                None => return Ok(header_cache),
            }
        };

        // Ensure `heights` are all in `cp_tail`.
        for height in heights_iter {
            let header_req = request::Header { height };
            let header_opt = match cp_tail.entry(height) {
                btree_map::Entry::Vacant(tail_e) => {
                    let header = client.request(header_req).await?.header;
                    let hash = header.block_hash();
                    self.headers.insert(hash, header);
                    tail_e.insert(hash);
                    Some((hash, header))
                }
                btree_map::Entry::Occupied(tail_e) => {
                    let hash = *tail_e.get();
                    // Try ensure we also have the header.
                    match self.headers.entry(hash) {
                        hash_map::Entry::Occupied(header_e) => Some((hash, *header_e.get())),
                        hash_map::Entry::Vacant(header_e) => {
                            let header = client.request(header_req).await?.header;
                            if header.block_hash() == hash {
                                header_e.insert(header);
                                Some((hash, header))
                            } else {
                                // TODO: What to do here?
                                None
                            }
                        }
                    }
                }
            };
            if let Some(hash_and_header) = header_opt {
                header_cache.insert(height, hash_and_header);
            }
        }

        // Create new cp.
        self.tip = start_cp
            .extend(cp_tail.into_iter().map(Into::into))
            .expect("must extend");
        Ok(header_cache)
    }
}

#[derive(Debug, Default)]
pub struct Txs {
    txs: HashMap<Txid, Arc<Transaction>>,
}

impl Txs {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert_tx(&mut self, tx: impl Into<Arc<Transaction>>) {
        let tx: Arc<Transaction> = tx.into();
        self.txs.insert(tx.compute_txid(), tx);
    }

    pub async fn fetch_tx(
        &mut self,
        client: &Client,
        txid: Txid,
    ) -> anyhow::Result<Arc<Transaction>> {
        match self.txs.entry(txid) {
            hash_map::Entry::Occupied(entry) => Ok(entry.get().clone()),
            hash_map::Entry::Vacant(entry) => {
                let tx = client.request(request::GetTx(txid)).await?.tx;
                let arc_tx = entry.insert(Arc::new(tx)).clone();
                Ok(arc_tx)
            }
        }
    }

    pub async fn fetch_txout(
        &mut self,
        client: &Client,
        outpoint: OutPoint,
    ) -> anyhow::Result<Option<TxOut>> {
        let tx = self.fetch_tx(client, outpoint.txid).await?;
        Ok(tx.output.get(outpoint.vout as usize).cloned())
    }
}

/// Initiate emitter by subscribing to headers and scripts.
pub async fn init<K>(client: &Client, spk_tracker: &mut DerivedSpkTracker<K>) -> anyhow::Result<()>
where
    K: Clone + Ord + Send + Sync + 'static,
{
    client.request_event(request::HeadersSubscribe)?;

    // Assume `spk_tracker` is initiated.
    for script_hash in spk_tracker.all_spk_hashes() {
        client.request_event(request::ScriptHashSubscribe { script_hash })?;
    }

    Ok(())
}

/// [`init`] should be called beforehand.
pub async fn handle_event<K>(
    client: &Client,
    spk_tracker: &mut DerivedSpkTracker<K>,
    headers: &mut Headers,
    txs: &mut Txs,
    broadcast_queue: &mut BroadcastQueue,
    event: Event,
) -> anyhow::Result<Option<Update<K>>>
where
    K: Clone + Ord + Send + Sync + 'static,
{
    match event {
        Event::Response(SatisfiedRequest::Header { req, resp }) => Ok(headers
            .update(client, req.height, resp.header.block_hash())
            .await?
            .map(|cp| Update {
                chain_update: Some(cp),
                ..Default::default()
            })),
        Event::Notification(Notification::Header(h)) => Ok(headers
            .update(client, h.height(), h.header().block_hash())
            .await?
            .map(|cp| Update {
                chain_update: Some(cp),
                ..Default::default()
            })),
        Event::Response(SatisfiedRequest::ScriptHashSubscribe { req, resp }) => {
            if resp.is_none() {
                return Ok(None);
            }
            let (k, i) = match spk_tracker.handle_script_status(req.script_hash) {
                Some((k, i, new_spk_hashes)) => {
                    for script_hash in new_spk_hashes {
                        client.request_event(request::ScriptHashSubscribe { script_hash })?;
                    }
                    (k, i)
                }
                None => return Ok(None),
            };
            let tx_update = script_hash_update(client, headers, txs, req.script_hash).await?;
            let last_active_indices = core::iter::once((k, i)).collect();
            let chain_update = Some(headers.tip());
            Ok(Some(Update {
                tx_update,
                last_active_indices,
                chain_update,
            }))
        }
        Event::Notification(Notification::ScriptHash(inner)) => {
            let (k, i) = match spk_tracker.handle_script_status(inner.script_hash()) {
                Some((k, i, new_spk_hashes)) => {
                    for script_hash in new_spk_hashes {
                        client.request_event(request::ScriptHashSubscribe { script_hash })?;
                    }
                    (k, i)
                }
                None => return Ok(None),
            };
            let tx_update = script_hash_update(client, headers, txs, inner.script_hash()).await?;
            let last_active_indices = core::iter::once((k, i)).collect();
            let chain_update = Some(headers.tip());
            Ok(Some(Update {
                tx_update,
                last_active_indices,
                chain_update,
            }))
        }
        Event::Response(SatisfiedRequest::BroadcastTx { resp, .. }) => {
            broadcast_queue.handle_resp_ok(resp);
            Ok(None)
        }
        Event::ResponseError(ErroredRequest::BroadcastTx { req, error }) => {
            broadcast_queue.handle_resp_err(req.0.compute_txid(), error);
            Ok(None)
        }
        Event::ResponseError(err) => Err(err.into()),
        _ => Ok(None),
    }
}

async fn script_hash_update(
    client: &Client,
    headers: &mut Headers,
    txs: &mut Txs,
    script_hash: ElectrumScriptHash,
) -> anyhow::Result<TxUpdate<ConfirmationBlockTime>> {
    let electrum_txs = client.request(request::GetHistory { script_hash }).await?;

    let header_cache = headers
        .ensure_heights(
            client,
            electrum_txs
                .iter()
                .filter_map(|tx| tx.confirmation_height().map(|h| h.to_consensus_u32()))
                .collect(),
        )
        .await?;

    let mut tx_update = TxUpdate::<ConfirmationBlockTime>::default();

    for tx in electrum_txs {
        let txid = tx.txid();
        let full_tx = txs.fetch_tx(client, txid).await?;

        for txin in &full_tx.input {
            let op = txin.previous_output;
            if let Some(txout) = txs.fetch_txout(client, op).await? {
                tx_update.txouts.insert(op, txout);
            }
        }
        tx_update.txs.push(full_tx);

        if let Some(height) = tx.confirmation_height() {
            let height = height.to_consensus_u32();
            let merkle_res = client
                .request(request::GetTxMerkle { txid, height })
                .await?;
            let (hash, header) = match header_cache.get(&height) {
                Some(&hash_and_header) => hash_and_header,
                None => continue,
            };
            if header.merkle_root != merkle_res.expected_merkle_root(txid) {
                continue;
            }
            tx_update.anchors.insert((
                ConfirmationBlockTime {
                    block_id: BlockId { height, hash },
                    confirmation_time: header.time as _,
                },
                txid,
            ));
        }
    }

    Ok(tx_update)
}

#[derive(Debug, Default)]
pub struct BroadcastQueue {
    queue: VecDeque<(Transaction, oneshot::Sender<Result<(), ResponseError>>)>,
}

impl BroadcastQueue {
    pub fn txs(&self) -> impl Iterator<Item = Transaction> + '_ {
        self.queue.iter().map(|(tx, _)| tx.clone())
    }

    pub fn add(&mut self, tx: Transaction, resp: oneshot::Sender<Result<(), ResponseError>>) {
        self.queue.push_back((tx, resp));
    }

    pub fn handle_resp_ok(&mut self, txid: Txid) {
        let i_opt = self.queue.iter().enumerate().find_map(|(i, (tx, _))| {
            if tx.compute_txid() == txid {
                Some(i)
            } else {
                None
            }
        });
        if let Some(i) = i_opt {
            let (_, resp_tx) = self.queue.remove(i).expect("must exist");
            let _ = resp_tx.send(Ok(()));
        }
    }

    pub fn handle_resp_err(&mut self, txid: Txid, err: ResponseError) {
        let i_opt = self.queue.iter().enumerate().find_map(|(i, (tx, _))| {
            if tx.compute_txid() == txid {
                Some(i)
            } else {
                None
            }
        });
        if let Some(i) = i_opt {
            let (_, resp_tx) = self.queue.remove(i).expect("must exist");
            let _ = resp_tx.send(Err(err));
        }
    }
}

#[derive(Debug)]
pub struct Emitter<K: Clone + Ord + Send + Sync + 'static> {
    spk_tracker: DerivedSpkTracker<K>,
    header_cache: Headers,
    tx_cache: Txs,

    update_tx: mpsc::UnboundedSender<Update<K>>,
    broadcast_queue: BroadcastQueue,
}

impl<K> Emitter<K>
where
    K: Clone + Ord + Send + Sync + 'static,
{
    pub fn new(wallet_tip: CheckPoint, lookahead: u32) -> (Self, UnboundedReceiver<Update<K>>) {
        let (update_tx, update_rx) = mpsc::unbounded::<Update<K>>();
        (
            Self {
                spk_tracker: DerivedSpkTracker::new(lookahead),
                header_cache: Headers::new(wallet_tip),
                tx_cache: Txs::new(),
                update_tx,
                broadcast_queue: BroadcastQueue::default(),
            },
            update_rx,
        )
    }

    /// Populate tx cache.
    pub fn insert_txs<Tx>(&mut self, txs: impl IntoIterator<Item = Tx>)
    where
        Tx: Into<Arc<Transaction>>,
    {
        for tx in txs {
            self.tx_cache.insert_tx(tx);
        }
    }

    pub fn run<'a, C>(
        &'a mut self,
        conn: C,
    ) -> (
        CmdSender<K>,
        impl Future<Output = anyhow::Result<()>> + Send + 'a,
    )
    where
        C: AsyncRead + AsyncWrite + Send + 'a,
    {
        let (cmd_tx, mut cmd_rx) = mpsc::unbounded::<Cmd<K>>();
        let (client, mut event_rx, run_fut) = electrum_c::run(conn);

        let fut = async move {
            client.request_event(request::HeadersSubscribe)?;
            for script_hash in self.spk_tracker.all_spk_hashes() {
                client.request_event(request::ScriptHashSubscribe { script_hash })?;
            }
            for tx in self.broadcast_queue.txs() {
                client.request_event(request::BroadcastTx(tx))?;
            }

            let spk_tracker = &mut self.spk_tracker;
            let header_cache = &mut self.header_cache;
            let tx_cache = &mut self.tx_cache;
            let update_tx = &mut self.update_tx;
            let broadcast_queue = &mut self.broadcast_queue;

            let process_fut = async move {
                // TODO: We should not await in the select branches. Instead, have a struct that keeps
                // state and mutate this state in the `handle_event` logic (which should be inlined).
                loop {
                    select! {
                        opt = event_rx.next() => match opt {
                            Some(event) => {
                                let update_opt =
                                    handle_event(&client, spk_tracker, header_cache, tx_cache, broadcast_queue, event).await?;
                                if let Some(update) = update_opt {
                                    if let Err(_err) = update_tx.unbounded_send(update) {
                                        break;
                                    }
                                }
                            },
                            None => break,
                        },
                        opt = cmd_rx.next() => match opt {
                            Some(Cmd::InsertDescriptor { keychain, descriptor, next_index }) => {
                                for script_hash in spk_tracker.insert_descriptor(keychain, descriptor, next_index) {
                                    client.request_event(request::ScriptHashSubscribe { script_hash })?;
                                }
                            }
                            Some(Cmd::Broadcast { tx, resp_tx }) => {
                                broadcast_queue.add(tx.clone(), resp_tx);
                                client.request_event(request::BroadcastTx(tx))?;
                            }
                            Some(Cmd::Ping) => {
                                client.request_event(request::Ping)?;
                            }
                            Some(Cmd::Close) | None => break,
                        },
                    }
                }
                anyhow::Ok(())
            };

            select! {
                res = run_fut.fuse() => res?,
                res = process_fut.fuse() => res?,
            }
            Ok(())
        };
        (CmdSender { tx: cmd_tx }, fut)
    }
}

pub type CmdRx<K> = mpsc::UnboundedReceiver<Cmd<K>>;

#[non_exhaustive]
pub enum Cmd<K> {
    InsertDescriptor {
        keychain: K,
        descriptor: Descriptor<DescriptorPublicKey>,
        next_index: u32,
    },
    Broadcast {
        tx: Transaction,
        resp_tx: oneshot::Sender<Result<(), ResponseError>>,
    },
    Ping,
    Close,
}

#[derive(Debug, Clone)]
pub struct CmdSender<K> {
    tx: mpsc::UnboundedSender<Cmd<K>>,
}

impl<K: Send + Sync + 'static> CmdSender<K> {
    pub fn insert_descriptor(
        &self,
        keychain: K,
        descriptor: Descriptor<DescriptorPublicKey>,
        next_index: u32,
    ) -> anyhow::Result<()> {
        self.tx.unbounded_send(Cmd::InsertDescriptor {
            keychain,
            descriptor,
            next_index,
        })?;
        Ok(())
    }

    pub async fn broadcast_tx(&self, tx: Transaction) -> anyhow::Result<()> {
        let (resp_tx, rx) = oneshot::channel();
        self.tx.unbounded_send(Cmd::Broadcast { tx, resp_tx })?;
        rx.await??;
        Ok(())
    }

    pub fn ping(&self) -> anyhow::Result<()> {
        self.tx.unbounded_send(Cmd::Ping)?;
        Ok(())
    }

    pub async fn close(&self) -> anyhow::Result<()> {
        self.tx.unbounded_send(Cmd::Close)?;
        Ok(())
    }
}
