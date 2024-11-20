//! BDK Electrum goodness.

use bdk_core::spk_client::FullScanResult;
use e_electrum_client::client::TrySendRequestError;
use e_electrum_client::notification::Notification;
use e_electrum_client::pending_request::SatisfiedRequest;
use e_electrum_client::{request, Client, ElectrumScriptHash, Event};

use std::collections::{btree_map, hash_map, BTreeMap, BTreeSet};
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

    async fn _track_derived_spk(
        &mut self,
        client: &Client,
        keychain: K,
        index: u32,
    ) -> Result<bool, TrySendRequestError> {
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
            client.request_event(request::ScriptHashSubscribe { script_hash })?;
            return Ok(true);
        }
        Ok(false)
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

    pub async fn add_descriptor(
        &mut self,
        client: &Client,
        keychain: K,
        descriptor: Descriptor<DescriptorPublicKey>,
    ) -> Result<bool, TrySendRequestError> {
        if let Some(old_descriptor) = self
            .descriptors
            .insert(keychain.clone(), descriptor.clone())
        {
            if old_descriptor == descriptor {
                return Ok(false);
            }
            self._clear_tracked_spks_of_keychain(keychain.clone());
        }
        for index in 0_u32..=self.lookahead {
            self._track_derived_spk(client, keychain.clone(), index)
                .await?;
        }
        Ok(true)
    }

    pub async fn handle_script_notification(
        &mut self,
        client: &Client,
        script_hash: ElectrumScriptHash,
    ) -> Result<Option<(K, u32)>, TrySendRequestError> {
        let (k, i) = match self.derived_spks_rev.get(&script_hash) {
            Some(spk_i) => spk_i.clone(),
            None => return Ok(None),
        };
        for index in (i + 1..=i + 1 + self.lookahead).rev() {
            if !self._track_derived_spk(client, k.clone(), index).await? {
                break;
            }
        }
        Ok(None)
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
            let keychain_index_opt = spk_tracker
                .handle_script_notification(client, req.script_hash)
                .await?;
            let (k, i) = match keychain_index_opt {
                Some((k, i)) => (k, i),
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
            let keychain_index_opt = spk_tracker
                .handle_script_notification(client, inner.script_hash())
                .await?;
            let (k, i) = match keychain_index_opt {
                Some((k, i)) => (k, i),
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
