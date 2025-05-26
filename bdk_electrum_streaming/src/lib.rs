//! BDK Electrum goodness.

use bdk_core::spk_client::FullScanResponse;
/// Re-export.
pub use electrum_streaming_client;
use electrum_streaming_client::notification::Notification;
use electrum_streaming_client::{
    request, AsyncClient, AsyncPendingRequest, AsyncPendingRequestTuple, AsyncRequestError,
    ElectrumScriptHash, ElectrumScriptStatus, Event, Request, ResponseError,
};
use electrum_streaming_client::{ErroredRequest, SatisfiedRequest};
use futures::channel::mpsc::{self, UnboundedReceiver};
use futures::channel::oneshot;
use futures::future::Either;
use futures::stream::{FuturesOrdered, FuturesUnordered};
use futures::{select, AsyncRead, AsyncWrite, FutureExt, StreamExt, TryStreamExt};
use futures_timer::Delay;

use std::collections::{btree_map, hash_map, BTreeMap, BTreeSet, HashSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bdk_core::bitcoin::block::Header;
use bdk_core::bitcoin::{BlockHash, ScriptBuf, Transaction, Txid};
use bdk_core::{collections::HashMap, CheckPoint};
use bdk_core::{BlockId, ConfirmationBlockTime};
use miniscript::{Descriptor, DescriptorPublicKey};
mod state;
pub use state::*;
mod chain_job;
mod req;
mod spk_job;

pub type Update<K> = FullScanResponse<K, ConfirmationBlockTime>;
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
pub struct DerivedSpkTracker<K> {
    lookahead: u32,
    descriptors: BTreeMap<K, Descriptor<DescriptorPublicKey>>,
    derived_spks: BTreeMap<(K, u32), ElectrumScriptHash>,
    derived_spks_rev: HashMap<ElectrumScriptHash, (K, u32)>,
}

impl<K: Ord + Clone> DerivedSpkTracker<K> {
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
        let (k, this_index) = self.derived_spks_rev.get(&script_hash).cloned()?;
        let next_index = this_index + 1;

        let mut spk_hashes = Vec::new();
        // We iterate the derivation indices backwards so that we return script hashes that starts
        // with the latest spk, since we want to send request for later spks first.
        for index in (next_index..=next_index + 1 + self.lookahead).rev() {
            match self._add_derived_spk(k.clone(), index) {
                Some(spk_hash) => spk_hashes.push(spk_hash),
                None => break,
            }
        }
        Some((k, this_index, spk_hashes))
    }
}

pub struct ScriptStatusResult<K> {
    pub last_active: (K, u32),

}

/// This is a plceholder until we can put headers in checkpoints.
#[derive(Debug)]
pub struct Headers {
    tip: CheckPoint,
    headers: HashMap<BlockHash, Header>,
}

impl Headers {
    const CHAIN_SUFFIX_LENGTH: u32 = 21;

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
        client: &AsyncClient,
        tip_height: u32,
        tip_hash: BlockHash,
    ) -> anyhow::Result<Option<CheckPoint>> {
        // TODO: Get rid of `ASSUME_FINAL_DEPTH`. Instead, get headers one by one and stop when we
        // connect with a checkpoint.
        let start_height = tip_height.saturating_sub(Self::CHAIN_SUFFIX_LENGTH - 1);
        let headers_resp = client
            .send_request(request::Headers {
                start_height,
                count: Self::CHAIN_SUFFIX_LENGTH as _,
            })
            .await;
        let mut new_headers = (start_height..)
            .zip(headers_resp?.headers)
            .collect::<BTreeMap<u32, Header>>();

        // Check that the tip is still the same.
        if new_headers.get(&tip_height).map(|h| h.block_hash()) != Some(tip_hash) {
            // It is safe to ignore this update and wait for the next notification.
            return Ok(None);
        }

        // Ensure local recent headers are still in the best chain.
        for cp in self.tip.iter() {
            let height = cp.height();
            let orig_hash = cp.hash();
            let header = match new_headers.entry(height) {
                btree_map::Entry::Vacant(e) => *e.insert(
                    client
                        .send_request(request::Header { height })
                        .await?
                        .header,
                ),
                btree_map::Entry::Occupied(e) => *e.get(),
            };
            let hash = header.block_hash();
            self.headers.insert(hash, header);
            if header.block_hash() == orig_hash {
                break;
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
        client: &AsyncClient,
        heights: BTreeSet<u32>,
    ) -> anyhow::Result<(HeaderCache, bool)> {
        // Any height above the tip height is ignored to avoid inconsistencies.
        let local_tip_height = self.tip().height();

        let mut header_cache = HeaderCache::new();
        let mut tail = BTreeMap::<u32, BlockHash>::new();
        let mut to_fetch = Vec::<u32>::new();

        let mut base = self.tip();
        let mut has_reached_past_local_genesis = false;

        let heights = heights
            .iter()
            .rev()
            .copied()
            .skip_while(|&h| h > local_tip_height);
        for height in heights {
            while !has_reached_past_local_genesis && base.height() >= height {
                tail.insert(base.height(), base.hash());
                if let Some(header) = self.headers.get(&base.hash()) {
                    debug_assert_eq!(
                        base.hash(),
                        header.block_hash(),
                        "must not be any discrepancies between headers and checkpoints"
                    );
                    header_cache.insert(base.height(), (base.hash(), *header));
                } else {
                    to_fetch.push(base.height());
                }
                base = match base.prev() {
                    Some(base) => base,
                    None => {
                        has_reached_past_local_genesis = true;
                        break;
                    }
                };
            }
            to_fetch.push(height);
        }

        // Fetch headers in one go.
        // Only include if hash does not conflict with that in tail.
        let mut fetch_headers = FuturesUnordered::new();
        let mut has_changes = false;
        for height in to_fetch {
            fetch_headers.push(
                client
                    .send_request(request::Header { height })
                    .map(move |r| (height, r)),
            );
        }
        while let Some((height, result)) = fetch_headers.next().await {
            let header = result?.header;
            let block_hash = header.block_hash();
            match tail.entry(height) {
                btree_map::Entry::Vacant(tail_entry) => {
                    tail_entry.insert(block_hash);
                }
                btree_map::Entry::Occupied(tail_entry) => {
                    if tail_entry.get() != &block_hash {
                        continue;
                    }
                }
            };
            header_cache.insert(height, (block_hash, header));
            self.headers.insert(block_hash, header);
            has_changes = true;
        }

        if has_reached_past_local_genesis {
            // We basically need to rebuild the chain at this point.
            self.tip = CheckPoint::from_block_ids(tail.into_iter().map(Into::into))
                .expect("must be in order and non-empty");
        } else if has_changes {
            self.tip = base
                .extend(tail.into_iter().map(Into::into))
                .expect("must extend");
        }

        Ok((header_cache, has_changes))
    }
}

#[derive(Debug, Default)]
pub struct Txs {
    txs: HashMap<Txid, Arc<Transaction>>,
    spk_txs: HashMap<ElectrumScriptHash, BTreeSet<Txid>>,
    anchors: HashMap<(Txid, BlockHash), ConfirmationBlockTime>,
}

impl Txs {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert_tx(&mut self, tx: impl Into<Arc<Transaction>>) {
        let tx: Arc<Transaction> = tx.into();
        let txid = tx.compute_txid();
        self.txs.insert(txid, tx);
    }

    pub fn insert_expected_spk_txids<S: Into<ElectrumScriptHash>>(
        &mut self,
        txs: impl IntoIterator<Item = (S, Txid)>,
    ) {
        for (s, txid) in txs {
            self.spk_txs.entry(s.into()).or_default().insert(txid);
        }
    }

    pub fn replace_expected_spk_txids(
        &mut self,
        spk: ElectrumScriptHash,
        spk_txids: BTreeSet<Txid>,
    ) -> Vec<Txid> {
        match self.spk_txs.entry(spk) {
            hash_map::Entry::Occupied(mut e) => {
                println!("[SCRIPT] {}, electrum_txs={:?}", spk, spk_txids);
                let evicted_txids = e.get().difference(&spk_txids).copied().collect();
                e.get_mut().extend(spk_txids);
                evicted_txids
            }
            hash_map::Entry::Vacant(e) => {
                println!("[SCRIPT] {}, electrum_txs={:?}", spk, spk_txids);
                e.insert(spk_txids);
                Vec::new()
            }
        }
    }

    pub async fn fetch_txs(
        &mut self,
        client: &AsyncClient,
        txids: impl IntoIterator<Item = Txid>,
    ) -> anyhow::Result<Vec<Arc<Transaction>>> {
        let mut futs = FuturesOrdered::new();
        let mut needs_insert = Vec::<(usize, Txid)>::new();
        let mut no_dup = HashSet::<Txid>::new();
        for (i, txid) in txids.into_iter().enumerate() {
            // So we don't fetch something twice.
            if !no_dup.insert(txid) {
                continue;
            }
            futs.push_back(match self.txs.get(&txid).cloned() {
                Some(tx) => Either::Left(futures::future::ok(tx)),
                None => {
                    needs_insert.push((i, txid));
                    Either::Right(
                        client.send_request(request::GetTx { txid }).map(move |r| {
                            r.map(|resp| Arc::new(resp.tx)).map_err(anyhow::Error::new)
                        }),
                    )
                }
            });
        }
        let txs = futs.try_collect::<Vec<_>>().await?;
        for (i, txid) in needs_insert {
            self.txs.insert(txid, txs[i].clone());
        }
        Ok(txs)
    }

    pub async fn fetch_anchors(
        &mut self,
        client: &AsyncClient,
        headers_cache: &HeaderCache,
        txs: impl IntoIterator<Item = (Txid, u32)>,
    ) -> anyhow::Result<impl Iterator<Item = (Txid, ConfirmationBlockTime)>> {
        let mut futs = FuturesOrdered::new();
        let mut needs_insert = Vec::<(usize, BlockHash)>::new();
        let mut no_dup = HashSet::<Txid>::new();
        for (i, (txid, conf_height)) in txs.into_iter().enumerate() {
            if !no_dup.insert(txid) {
                continue;
            }
            let (hash, header) = match headers_cache.get(&conf_height) {
                Some((hash, header)) => (*hash, *header),
                None => continue,
            };
            futs.push_back(match self.anchors.get(&(txid, hash)) {
                Some(anchor) => Either::Left(futures::future::ok(Some((txid, *anchor)))),
                None => {
                    needs_insert.push((i, hash));
                    let height = conf_height;
                    Either::Right(
                        client
                            .send_request(request::GetTxMerkle { txid, height })
                            .map(move |r| -> anyhow::Result<_> {
                                let resp = r?;
                                if header.merkle_root != resp.expected_merkle_root(txid) {
                                    Ok(None)
                                } else {
                                    let confirmation_time = header.time as u64;
                                    let anchor = ConfirmationBlockTime {
                                        block_id: BlockId { height, hash },
                                        confirmation_time,
                                    };
                                    Ok(Some((txid, anchor)))
                                }
                            }),
                    )
                }
            });
        }
        let anchors = futs.try_collect::<Vec<_>>().await?;
        for (i, hash) in needs_insert {
            if let Some((txid, a)) = anchors[i] {
                self.anchors.insert((txid, hash), a);
            }
        }
        Ok(anchors.into_iter().flatten())
    }
}

/// Initiate emitter by subscribing to headers and scripts.
pub async fn init<K>(
    client: &AsyncClient,
    spk_tracker: &mut DerivedSpkTracker<K>,
) -> anyhow::Result<()>
where
    K: Clone + Ord + Send + Sync + 'static,
{
    client.send_event_request(request::HeadersSubscribe)?;

    // Assume `spk_tracker` is initiated.
    for script_hash in spk_tracker.all_spk_hashes() {
        client.send_event_request(request::ScriptHashSubscribe { script_hash })?;
    }

    Ok(())
}

/// [`init`] should be called beforehand.
pub async fn handle_event<K>(
    client: &AsyncClient,
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
        Event::Response(SatisfiedRequest::Header { req, resp }) => {
            println!("[RESPONSE] Header");
            Ok(headers
                .update(client, req.height, resp.header.block_hash())
                .await?
                .map(|cp| Update {
                    chain_update: Some(cp),
                    ..Default::default()
                }))
        }
        Event::Response(SatisfiedRequest::HeadersSubscribe { resp, .. }) => {
            println!("[RESPONSE] HeadersSubscribe");
            Ok(headers
                .update(client, resp.height, resp.header.block_hash())
                .await?
                .map(|cp| Update {
                    chain_update: Some(cp),
                    ..Default::default()
                }))
        }
        Event::Notification(Notification::Header(h)) => {
            println!("[NOTIFICATION] Header");
            Ok(headers
                .update(client, h.height(), h.header().block_hash())
                .await?
                .map(|cp| Update {
                    chain_update: Some(cp),
                    ..Default::default()
                }))
        }
        Event::Response(SatisfiedRequest::ScriptHashSubscribe { req, resp }) => {
            println!("[RESPONSE] ScriptHashSubscribe");
            let update =
                script_hash_update(client, headers, spk_tracker, txs, req.script_hash, resp)
                    .await?;
            // TODO: Return `None` if update is empty.
            Ok(Some(update))
        }
        Event::Notification(Notification::ScriptHash(inner)) => {
            println!("[NOTIFICATION] ScriptHash");
            let update = script_hash_update(
                client,
                headers,
                spk_tracker,
                txs,
                inner.script_hash(),
                inner.script_status(),
            )
            .await?;
            // TODO: Return `None` if update is empty.
            Ok(Some(update))
        }
        Event::Response(SatisfiedRequest::BroadcastTx { resp, .. }) => {
            println!("[RESPONSE] BroadcastTx");
            broadcast_queue.handle_resp_ok(resp);
            Ok(None)
        }
        Event::ResponseError(ErroredRequest::BroadcastTx { req, error }) => {
            broadcast_queue.handle_resp_err(req.0.compute_txid(), error);
            Ok(None)
        }
        Event::ResponseError(err) => {
            println!("[ERROR] err={}", err);
            Err(err.into())
        }
        _ => Ok(None),
    }
}

async fn script_hash_update<K>(
    client: &AsyncClient,
    headers: &mut Headers,
    spk_tracker: &mut DerivedSpkTracker<K>,
    txs: &mut Txs,
    script_hash: ElectrumScriptHash,
    script_status: Option<ElectrumScriptStatus>,
) -> anyhow::Result<Update<K>>
where
    K: Clone + Ord + Send + Sync + 'static,
{
    let start_epoch = std::time::UNIX_EPOCH.elapsed()?.as_secs();

    let mut update = Update::<K>::default();
    if script_status.is_some() {
        if let Some((k, i, new_spks)) = spk_tracker.handle_script_status(script_hash) {
            for script_hash in new_spks {
                client.send_event_request(request::ScriptHashSubscribe { script_hash })?;
            }
            update.last_active_indices.insert(k, i);
        }
    }

    let electrum_txs = client
        .send_request(request::GetHistory { script_hash })
        .await?;

    let start = Instant::now();
    let (header_cache, has_cp_changes) = headers
        .ensure_heights(
            client,
            electrum_txs
                .iter()
                .filter_map(|tx| tx.confirmation_height().map(|h| h.to_consensus_u32()))
                .collect(),
        )
        .await?;
    println!(
        "[DONE] method=ensure_height, elapsed={}s",
        start.elapsed().as_secs_f64()
    );
    if has_cp_changes {
        update.chain_update = Some(headers.tip());
    }

    let electrum_txids = electrum_txs
        .iter()
        .map(|tx| tx.txid())
        .collect::<BTreeSet<Txid>>();
    let evicted_txids = txs.replace_expected_spk_txids(script_hash, electrum_txids);
    for txid in evicted_txids {
        println!("Evicted tx: {txid}");
        update.tx_update.evicted_ats.insert((txid, start_epoch));
    }

    let fetched_txs = txs
        .fetch_txs(client, electrum_txs.iter().map(|tx| tx.txid()))
        .await?;
    let prevouts = fetched_txs
        .iter()
        .flat_map(|tx| tx.input.iter().map(|input| input.previous_output));
    txs.fetch_txs(client, prevouts.clone().map(|op| op.txid))
        .await?;
    for op in prevouts {
        if let Some(tx) = txs.txs.get(&op.txid) {
            if let Some(txout) = tx.output.get(op.vout as usize) {
                update.tx_update.txouts.insert(op, txout.clone());
            }
        }
    }
    update.tx_update.txs = fetched_txs;

    let anchors = txs
        .fetch_anchors(
            client,
            &header_cache,
            electrum_txs
                .iter()
                .filter_map(|tx| Some((tx.txid(), tx.confirmation_height()?.to_consensus_u32()))),
        )
        .await?
        .map(|(txid, anchor)| (anchor, txid));
    update.tx_update.anchors.extend(anchors);

    for (i, tx) in (0..electrum_txs.len() as u64).rev().zip(electrum_txs) {
        let txid = tx.txid();

        // We tweak the last_seen so that transctions are sorted in the order of
        // `electrum_txs` for a single spk history.
        let tweaked_last_seen = start_epoch.saturating_sub(i);
        if tx.confirmation_height().is_none() {
            update.tx_update.seen_ats.insert((txid, tweaked_last_seen));
        }
    }

    Ok(update)
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

    client: Arc<futures::lock::Mutex<Option<AsyncClient>>>,
    cmd_rx: UnboundedReceiver<Cmd<K>>,
    update_tx: mpsc::UnboundedSender<Update<K>>,
    broadcast_queue: BroadcastQueue,
}

impl<K> Emitter<K>
where
    K: core::fmt::Debug + Clone + Ord + Send + Sync + 'static,
{
    pub fn new(
        wallet_tip: CheckPoint,
        lookahead: u32,
    ) -> (Self, CmdSender<K>, UnboundedReceiver<Update<K>>) {
        let (cmd_tx, cmd_rx) = mpsc::unbounded::<Cmd<K>>();
        let (update_tx, update_rx) = mpsc::unbounded::<Update<K>>();
        let client = Arc::new(futures::lock::Mutex::new(None));
        (
            Self {
                spk_tracker: DerivedSpkTracker::new(lookahead),
                header_cache: Headers::new(wallet_tip),
                tx_cache: Txs::new(),
                client: client.clone(),
                cmd_rx,
                update_tx,
                broadcast_queue: BroadcastQueue::default(),
            },
            CmdSender { tx: cmd_tx, client },
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

    /// Populate tx cache with expected spk txids.
    pub fn insert_expected_spk_txids(&mut self, txs: impl IntoIterator<Item = (ScriptBuf, Txid)>) {
        self.tx_cache.insert_expected_spk_txids(
            txs.into_iter()
                .map(|(spk, txid)| (ElectrumScriptHash::new(&spk), txid)),
        );
    }

    pub async fn run<C>(&mut self, ping_delay: Duration, conn: C) -> anyhow::Result<()>
    where
        C: AsyncRead + AsyncWrite + Send,
    {
        use futures::AsyncReadExt;
        let (reader, writer) = conn.split();
        let (client, mut event_rx, run_fut) = AsyncClient::new(reader, writer);
        self.client.lock().await.replace(client.clone());

        client.send_event_request(request::HeadersSubscribe)?;
        for script_hash in self.spk_tracker.all_spk_hashes() {
            client.send_event_request(request::ScriptHashSubscribe { script_hash })?;
        }
        for tx in self.broadcast_queue.txs() {
            client.send_event_request(request::BroadcastTx(tx))?;
        }

        let spk_tracker = &mut self.spk_tracker;
        let header_cache = &mut self.header_cache;
        let tx_cache = &mut self.tx_cache;
        let cmd_rx = &mut self.cmd_rx;
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
                                client.send_event_request(request::ScriptHashSubscribe { script_hash })?;
                            }
                        }
                        Some(Cmd::Broadcast { tx, resp_tx }) => {
                            broadcast_queue.add(tx.clone(), resp_tx);
                            client.send_event_request(request::BroadcastTx(tx))?;
                        }
                        Some(Cmd::Close) | None => break,
                    },
                    _ = Delay::new(ping_delay).fuse() => {
                        client.send_event_request(request::Ping)?;
                    }
                }
            }
            anyhow::Ok(())
        };

        select! {
            res = run_fut.fuse() => res?,
            res = process_fut.fuse() => res?,
        }
        Ok(())
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
    Close,
}

#[derive(Debug, Clone)]
pub struct CmdSender<K> {
    tx: mpsc::UnboundedSender<Cmd<K>>,
    client: Arc<futures::lock::Mutex<Option<AsyncClient>>>,
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

    pub async fn request<Req>(&self, request: Req) -> Result<Req::Response, AsyncRequestError>
    where
        Req: Request,
        AsyncPendingRequestTuple<Req, Req::Response>: Into<AsyncPendingRequest>,
    {
        match self.client.lock().await.as_ref().cloned() {
            Some(client) => client.send_request(request).await,
            None => Err(AsyncRequestError::Canceled),
        }
    }

    pub async fn broadcast_tx(&self, tx: Transaction) -> anyhow::Result<()> {
        let (resp_tx, rx) = oneshot::channel();
        self.tx.unbounded_send(Cmd::Broadcast { tx, resp_tx })?;
        rx.await??;
        Ok(())
    }

    pub async fn close(&self) -> anyhow::Result<()> {
        self.tx.unbounded_send(Cmd::Close)?;
        Ok(())
    }
}
