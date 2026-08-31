use std::{str::FromStr, sync::Arc};

use bdk_core::{
    bitcoin::{
        absolute, block,
        consensus::encode::serialize_hex,
        constants,
        hashes::{sha256d, Hash},
        transaction, Amount, CompactTarget, Network, OutPoint, ScriptBuf, Sequence, Transaction,
        TxIn, TxMerkleNode, TxOut, Txid, Witness,
    },
    BlockId,
};
use bdk_electrum_streaming::{
    electrum_streaming_client::{
        response, ElectrumScriptHash, ElectrumScriptStatus, RawNotificationOrResponse, RawRequest,
    },
    BlockingState, Cache, DerivedSpkTracker, HeaderChain, ProvenAnchor, ReqCoord, ReqQueue, Update,
};
use miniscript::{Descriptor, DescriptorPublicKey};
use serde_json::json;

const XPUB: &str = "xpub661MyMwAqRbcFtXgS5sYJABqqG9YLmC4Q1Rdap9gSE8NqtwybGhePY2gZ29ESFjqJoCu1Rupje8YtGqsefD265TMg7usUDFdp6W1EGMcet8";

fn raw_msg(v: serde_json::Value) -> RawNotificationOrResponse {
    serde_json::from_value(v).expect("must deserialize raw message")
}

#[derive(Clone)]
struct Server {
    headers: Vec<block::Header>,
    /// Every transaction the server knows, and the height each is confirmed at.
    ///
    /// A transaction belongs to the history of whichever scripts its outputs pay, so a server
    /// can serve any number of scripts without being told which.
    txs: Vec<(Transaction, u32)>,
    /// The merkle branch and position this server answers every merkle request with.
    merkle_proof: (Vec<sha256d::Hash>, usize),
}

impl Server {
    fn tip_height(&self) -> usize {
        self.headers.len() - 1
    }

    /// The history of `spk_hash`: every tx paying it that the chain is long enough to contain.
    fn history(&self, spk_hash: &serde_json::Value) -> Vec<response::Tx> {
        self.txs
            .iter()
            .filter(|(tx, height)| {
                self.tip_height() >= *height as usize
                    && tx.output.iter().any(|txout| {
                        *spk_hash
                            == json!(ElectrumScriptHash::new(&txout.script_pubkey).to_string())
                    })
            })
            .map(|(tx, height)| {
                response::Tx::Confirmed(response::ConfirmedTx {
                    txid: tx.compute_txid(),
                    height: absolute::Height::from_consensus(*height)
                        .expect("must be a valid height"),
                })
            })
            .collect()
    }

    /// Answer a request, or fail it the way a server would.
    fn answer(&self, req: &RawRequest) -> Result<serde_json::Value, String> {
        Ok(match req.method.as_ref() {
            "blockchain.headers.subscribe" => {
                let tip = self.headers.last().expect("server must have blocks");
                json!({ "hex": serialize_hex(tip), "height": self.tip_height() })
            }
            "blockchain.block.headers" => {
                let start = req.params[0].as_u64().expect("must have start_height") as usize;
                let count = req.params[1].as_u64().expect("must have count") as usize;
                let hex = self.headers[start..start + count]
                    .iter()
                    .map(serialize_hex)
                    .collect::<String>();
                json!({ "count": count, "hex": hex, "max": 2016 })
            }
            "blockchain.block.header" => {
                let height = req.params[0].as_u64().expect("must have height") as usize;
                match self.headers.get(height) {
                    Some(header) => json!(serialize_hex(header)),
                    None => return Err(format!("height {height} is above the chain tip")),
                }
            }
            "blockchain.scripthash.subscribe" => {
                match ElectrumScriptStatus::from_history(&self.history(&req.params[0])) {
                    Some(status) => json!(status.to_string()),
                    None => json!(null),
                }
            }
            "blockchain.scripthash.get_history" => json!(self
                .history(&req.params[0])
                .iter()
                .map(|tx| json!({
                    "tx_hash": tx.txid().to_string(),
                    "height": tx.electrum_height(),
                }))
                .collect::<Vec<_>>()),
            "blockchain.transaction.get" => {
                let (tx, _) = self
                    .txs
                    .iter()
                    .find(|(tx, _)| req.params[0] == json!(tx.compute_txid().to_string()))
                    .expect("must be a tx the server knows");
                json!(serialize_hex(tx))
            }
            "blockchain.transaction.get_merkle" => {
                // A proof can only be given for a tx the server has in that block. Both
                // romanz/electrs and ElectrumX raise an error otherwise.
                let height = req.params[1].as_u64().expect("must have height") as u32;
                if !self
                    .txs
                    .iter()
                    .any(|(tx, h)| *h == height && req.params[0] == json!(tx.compute_txid()))
                {
                    return Err(format!(
                        "tx {} not in block at height {height}",
                        req.params[0]
                    ));
                }
                let (branch, pos) = &self.merkle_proof;
                json!({
                    "block_height": req.params[1],
                    "merkle": branch.iter().map(|h| h.to_string()).collect::<Vec<_>>(),
                    "pos": pos,
                })
            }
            other => panic!("unexpected request: {other}"),
        })
    }
}

fn drain_requests(
    state: &mut BlockingState,
    queue: &mut ReqQueue,
    server: &Server,
) -> Vec<Update<&'static str>> {
    let mut updates = Vec::new();
    while let Some(req) = queue.pop_front() {
        if let Some(update) = state
            .poll(queue, response(&req, server))
            .expect("must poll")
        {
            updates.push(update);
        }
    }
    updates
}

/// Drain like [`drain_requests`], but hand back the first error rather than panicking on it.
fn drain_requests_fallible(
    state: &mut BlockingState,
    queue: &mut ReqQueue,
    server: &Server,
) -> anyhow::Result<Vec<Update<&'static str>>> {
    let mut updates = Vec::new();
    while let Some(req) = queue.pop_front() {
        if let Some(update) = state.poll(queue, response(&req, server))? {
            updates.push(update);
        }
    }
    Ok(updates)
}

/// Drain like [`drain_requests`], but answer every merkle proof in the queue ahead of everything
/// else in each round.
///
/// The Electrum protocol carries a request id precisely because responses need not come back in
/// the order they were asked for: romanz/electrs happens to answer in order, Fulcrum processes
/// requests concurrently. Nothing may depend on the ordering.
fn drain_requests_proofs_first(
    state: &mut BlockingState,
    queue: &mut ReqQueue,
    server: &Server,
) -> Vec<Update<&'static str>> {
    let mut updates = Vec::new();
    while !queue.is_empty() {
        let (proofs, rest): (Vec<_>, Vec<_>) = queue
            .drain(..)
            .partition(|req| req.method.as_ref() == "blockchain.transaction.get_merkle");
        for req in proofs.into_iter().chain(rest) {
            if let Some(update) = state
                .poll(queue, response(&req, server))
                .expect("must poll")
            {
                updates.push(update);
            }
        }
    }
    updates
}

/// The server's answer to `req`, as a raw JSON-RPC result or error message.
fn response(req: &RawRequest, server: &Server) -> RawNotificationOrResponse {
    raw_msg(match server.answer(req) {
        Ok(result) => json!({ "jsonrpc": "2.0", "id": req.id, "result": result }),
        Err(message) => json!({
            "jsonrpc": "2.0",
            "id": req.id,
            "error": { "code": 1, "message": message },
        }),
    })
}

/// A descriptor to track, the script hash of its first spk, and a tx paying to that spk.
fn tracked_descriptor() -> anyhow::Result<(
    Descriptor<DescriptorPublicKey>,
    ElectrumScriptHash,
    ScriptBuf,
)> {
    let descriptor = Descriptor::<DescriptorPublicKey>::from_str(&format!("wpkh({XPUB}/0/*)"))?;
    let spk = descriptor.at_derivation_index(0)?.script_pubkey();
    let spk_hash = ElectrumScriptHash::new(&spk);
    Ok((descriptor, spk_hash, spk))
}

/// A coinbase paying `sats` to `spk`, so that varying `sats` gives a distinct tx.
fn tx_paying(spk: &ScriptBuf, sats: u64) -> Transaction {
    Transaction {
        version: transaction::Version::ONE,
        lock_time: absolute::LockTime::ZERO,
        input: vec![TxIn {
            previous_output: OutPoint::null(),
            script_sig: ScriptBuf::new(),
            sequence: Sequence::MAX,
            witness: Witness::new(),
        }],
        output: vec![TxOut {
            value: Amount::from_sat(sats),
            script_pubkey: spk.clone(),
        }],
    }
}

/// Grind `nonce` upwards until the header actually clears its own target.
///
/// Regtest's target is easy but not free: a little under half of all nonces miss it, and the
/// header chain checks proof-of-work for real.
fn mine(mut header: block::Header) -> block::Header {
    while header.validate_pow(header.target()).is_err() {
        header.nonce = header.nonce.wrapping_add(1);
    }
    header
}

/// The regtest genesis block and an empty block on top of it.
fn base_headers() -> (block::Header, block::Header) {
    let genesis = constants::genesis_block(Network::Regtest).header;
    let header_1 = mine(block::Header {
        version: block::Version::ONE,
        prev_blockhash: genesis.block_hash(),
        merkle_root: TxMerkleNode::all_zeros(),
        time: 100,
        bits: CompactTarget::from_consensus(0x207fffff),
        nonce: 0,
    });
    (genesis, header_1)
}

/// A block whose transactions have the given merkle root.
fn block_with_root(
    prev: &block::Header,
    merkle_root: TxMerkleNode,
    time: u32,
    nonce: u32,
) -> block::Header {
    mine(block::Header {
        version: block::Version::ONE,
        prev_blockhash: prev.block_hash(),
        merkle_root,
        time,
        bits: CompactTarget::from_consensus(0x207fffff),
        nonce,
    })
}

/// A block whose only transaction is `txid`, so that its merkle root is the txid itself.
fn block_with_tx(prev: &block::Header, txid: Txid, time: u32, nonce: u32) -> block::Header {
    block_with_root(prev, Txid::to_raw_hash(txid).into(), time, nonce)
}

/// A state that trusts nothing but genesis, so everything above it has to be verified.
fn new_state(cache: Cache, descriptor: Descriptor<DescriptorPublicKey>) -> BlockingState {
    new_state_trusting(cache, descriptor, [])
}

fn new_state_trusting(
    cache: Cache,
    descriptor: Descriptor<DescriptorPublicKey>,
    trusted: impl IntoIterator<Item = (u32, block::Header)>,
) -> BlockingState {
    let mut spk_tracker = DerivedSpkTracker::new(0);
    spk_tracker.insert_descriptor("external", descriptor, 0);
    let chain = HeaderChain::new(Network::Regtest, trusted).expect("must build header chain");
    BlockingState::new(ReqCoord::default(), cache, spk_tracker, chain)
}

/// The anchor a tx confirmed in `header` at `height` must be given, for a server answering with
/// the empty proof that a single-transaction block has.
fn anchor_of(header: &block::Header, height: u32) -> ProvenAnchor {
    anchor_proved_by(header, height, Vec::new(), 0)
}

/// The same, for a server answering with a real merkle branch.
fn anchor_proved_by(
    header: &block::Header,
    height: u32,
    merkle: Vec<sha256d::Hash>,
    pos: usize,
) -> ProvenAnchor {
    ProvenAnchor {
        block_id: BlockId {
            height,
            hash: header.block_hash(),
        },
        pos,
        merkle,
    }
}

/// A history response can report a confirmation height above the local tip: on a new block,
/// romanz/electrs notifies the script hash before the header and answers requests in order, so
/// the history arrives while the local tip is still one block behind. Such an anchor must be
/// deferred until the tip catches up — not dropped — and must still be delivered without any
/// further notification for that script.
#[test]
fn anchor_above_local_tip_is_deferred_until_tip_catches_up() -> anyhow::Result<()> {
    let (descriptor, spk_hash, spk) = tracked_descriptor()?;
    let tx = tx_paying(&spk, 50_000);
    let txid = tx.compute_txid();
    let (genesis, header_1) = base_headers();
    let header_2 = block_with_tx(&header_1, txid, 200, 0);

    let mut cache = Cache::default();
    cache.tx_cache.txs.insert(txid, Arc::new(tx.clone()));

    let mut state = new_state(cache, descriptor);
    let mut queue = ReqQueue::new();
    let mut server = Server {
        headers: vec![genesis, header_1],
        txs: vec![(tx, 2)],
        merkle_proof: (Vec::new(), 0),
    };

    state.start(&mut queue);
    let updates = drain_requests(&mut state, &mut queue, &server);
    assert!(
        updates
            .iter()
            .any(|u| u.chain_update.as_ref().is_some_and(|cp| cp.height() == 1)),
        "initial sync must reach the server tip"
    );

    // Block 2 confirms the tx. The script hash notification is processed first, so its
    // history response will report height 2 while the local tip is still at height 1.
    let status =
        ElectrumScriptStatus::from_history(&[response::Tx::Confirmed(response::ConfirmedTx {
            txid,
            height: absolute::Height::from_consensus(2)?,
        })])
        .expect("history is not empty");
    state.poll(
        &mut queue,
        raw_msg(json!({
            "jsonrpc": "2.0",
            "method": "blockchain.scripthash.subscribe",
            "params": [spk_hash.to_string(), status.to_string()],
        })),
    )?;
    state.poll(
        &mut queue,
        raw_msg(json!({
            "jsonrpc": "2.0",
            "method": "blockchain.headers.subscribe",
            "params": [{ "hex": serialize_hex(&header_2), "height": 2 }],
        })),
    )?;

    server.headers.push(header_2);
    let updates = drain_requests(&mut state, &mut queue, &server);

    assert!(
        updates.iter().any(|u| u
            .tx_update
            .anchors
            .contains(&(anchor_of(&header_2, 2), txid))),
        "anchor must be delivered once the tip catches up"
    );
    Ok(())
}

/// A descriptor inserted while the connection is live must be subscribed to immediately.
/// Only [`State::start`] subscribes to the tracker's existing spks, so a subscription missed here
/// is missed until the next reconnection.
#[test]
fn descriptor_inserted_mid_connection_is_subscribed() -> anyhow::Result<()> {
    let descriptor = Descriptor::<DescriptorPublicKey>::from_str(&format!("wpkh({XPUB}/0/*)"))?;
    let spk_hash = ElectrumScriptHash::new(descriptor.at_derivation_index(0)?.script_pubkey());

    let mut state = BlockingState::new(
        ReqCoord::default(),
        Cache::default(),
        DerivedSpkTracker::new(0),
        HeaderChain::new(Network::Regtest, [])?,
    );

    let mut queue = ReqQueue::new();
    state.start(&mut queue);
    queue.clear();

    state.insert_descriptor(&mut queue, "external", descriptor, 0);
    assert!(
        queue
            .iter()
            .any(|req| &*req.method == "blockchain.scripthash.subscribe"
                && req.params[0] == json!(spk_hash.to_string())),
        "inserting a descriptor must queue a subscribe for its first spk, got: {queue:?}"
    );
    Ok(())
}

/// `last_active_indices` must name the derivation index of the spk that has history, not the
/// index after it. Emitting `index + 1` reveals one spk too many on every sync, permanently
/// skipping an unused address.
#[test]
fn last_active_index_is_index_of_active_spk() -> anyhow::Result<()> {
    const ACTIVE_INDEX: u32 = 3;
    const LOOKAHEAD: u32 = 5;

    let descriptor = Descriptor::<DescriptorPublicKey>::from_str(&format!("wpkh({XPUB}/0/*)"))?;
    let spk = descriptor
        .at_derivation_index(ACTIVE_INDEX)?
        .script_pubkey();
    let tx = tx_paying(&spk, 50_000);
    let txid = tx.compute_txid();
    let (genesis, header_1) = base_headers();
    let header_2 = block_with_tx(&header_1, txid, 200, 0);

    let mut spk_tracker = DerivedSpkTracker::new(LOOKAHEAD);
    spk_tracker.insert_descriptor("external", descriptor, 0);
    let mut state = BlockingState::new(
        ReqCoord::default(),
        Cache::default(),
        spk_tracker,
        HeaderChain::new(Network::Regtest, []).expect("must build header chain"),
    );
    let mut queue = ReqQueue::new();
    let server = Server {
        headers: vec![genesis, header_1, header_2],
        txs: vec![(tx, 2)],
        merkle_proof: (Vec::new(), 0),
    };

    state.start(&mut queue);
    let updates = drain_requests(&mut state, &mut queue, &server);

    let emitted = updates
        .iter()
        .flat_map(|update| &update.last_active_indices)
        .map(|(&k, &i)| (k, i))
        .collect::<Vec<_>>();
    assert_eq!(
        emitted,
        vec![("external", ACTIVE_INDEX)],
        "only the spk with history is active, at its own index"
    );
    Ok(())
}

/// Two spks of the same keychain can each have history, and the server notifies their statuses
/// independently, in an order that has nothing to do with derivation index — a later-derived spk
/// can be notified first. The keychain's last active index must still end up as the *highest* of
/// the two. Reporting a lower one leaves the higher spk unrevealed, so the wallet does not
/// recognise its txouts as its own.
#[test]
fn last_active_index_is_highest_regardless_of_notification_order() -> anyhow::Result<()> {
    const LOOKAHEAD: u32 = 5;

    let descriptor = Descriptor::<DescriptorPublicKey>::from_str(&format!("wpkh({XPUB}/0/*)"))?;
    let spk_3 = descriptor.at_derivation_index(3)?.script_pubkey();
    let spk_4 = descriptor.at_derivation_index(4)?.script_pubkey();
    let spk_hash_3 = ElectrumScriptHash::new(&spk_3);
    let spk_hash_4 = ElectrumScriptHash::new(&spk_4);
    let tx_3 = tx_paying(&spk_3, 10_000);
    let tx_4 = tx_paying(&spk_4, 20_000);
    let txid_3 = tx_3.compute_txid();
    let txid_4 = tx_4.compute_txid();
    let (genesis, header_1) = base_headers();
    // Each in its own single-tx block, so the block's merkle root is the txid and no real proof
    // construction is needed.
    let header_2 = block_with_tx(&header_1, txid_3, 200, 0);
    let header_3 = block_with_tx(&header_2, txid_4, 300, 0);

    let mut spk_tracker = DerivedSpkTracker::new(LOOKAHEAD);
    spk_tracker.insert_descriptor("external", descriptor, 0);
    let mut state = BlockingState::new(
        ReqCoord::default(),
        Cache::default(),
        spk_tracker,
        HeaderChain::new(Network::Regtest, []).expect("must build header chain"),
    );
    let mut queue = ReqQueue::new();
    let mut server = Server {
        headers: vec![genesis, header_1, header_2, header_3],
        txs: Vec::new(),
        merkle_proof: (Vec::new(), 0),
    };

    // Sync with neither spk active yet, so both are only ever reached through the notifications
    // sent below, not through the initial subscribe responses.
    state.start(&mut queue);
    drain_requests(&mut state, &mut queue, &server);

    server.txs = vec![(tx_3, 2), (tx_4, 3)];
    let status_3 =
        ElectrumScriptStatus::from_history(&server.history(&json!(spk_hash_3.to_string())))
            .expect("history is not empty");
    let status_4 =
        ElectrumScriptStatus::from_history(&server.history(&json!(spk_hash_4.to_string())))
            .expect("history is not empty");

    // The higher derivation index is notified first.
    state.poll(
        &mut queue,
        raw_msg(json!({
            "jsonrpc": "2.0",
            "method": "blockchain.scripthash.subscribe",
            "params": [spk_hash_4.to_string(), status_4.to_string()],
        })),
    )?;
    state.poll(
        &mut queue,
        raw_msg(json!({
            "jsonrpc": "2.0",
            "method": "blockchain.scripthash.subscribe",
            "params": [spk_hash_3.to_string(), status_3.to_string()],
        })),
    )?;

    let updates = drain_requests(&mut state, &mut queue, &server);
    let emitted = updates
        .iter()
        .flat_map(|update| &update.last_active_indices)
        .map(|(&k, &i)| (k, i))
        .collect::<Vec<_>>();
    assert_eq!(
        emitted,
        vec![("external", 4)],
        "the highest active index must survive being notified before the lower one"
    );
    Ok(())
}

/// A reorg can move a transaction into a different block at the *same* height. An Electrum
/// script status is a hash over txid-height pairs, so it does not change and the server has no
/// reason to send a script hash notification. The anchor we already delivered now points at a
/// block that is no longer in the chain, so it must be refetched off the tip update alone.
#[test]
fn anchor_is_refetched_when_tx_moves_to_another_block_of_same_height() -> anyhow::Result<()> {
    let (descriptor, _spk_hash, spk) = tracked_descriptor()?;
    let tx = tx_paying(&spk, 50_000);
    let txid = tx.compute_txid();
    let (genesis, header_1) = base_headers();
    let header_2 = block_with_tx(&header_1, txid, 200, 0);
    // The block that replaces height 2 contains the tx too, hence the identical script status.
    let header_2b = block_with_tx(&header_1, txid, 222, 1);
    let header_3b = block_with_root(&header_2b, TxMerkleNode::all_zeros(), 300, 0);
    assert_ne!(header_2.block_hash(), header_2b.block_hash());

    let mut state = new_state(Cache::default(), descriptor);
    let mut queue = ReqQueue::new();
    let mut server = Server {
        headers: vec![genesis, header_1, header_2],
        txs: vec![(tx, 2)],
        merkle_proof: (Vec::new(), 0),
    };

    state.start(&mut queue);
    let updates = drain_requests(&mut state, &mut queue, &server);
    assert!(
        updates.iter().any(|u| u
            .tx_update
            .anchors
            .contains(&(anchor_of(&header_2, 2), txid))),
        "tx must first be anchored to the original block"
    );

    // Reorg. Only a header notification is sent: the script status is unchanged, so a server
    // has no reason to notify the script hash.
    server.headers = vec![genesis, header_1, header_2b, header_3b];
    state.poll(
        &mut queue,
        raw_msg(json!({
            "jsonrpc": "2.0",
            "method": "blockchain.headers.subscribe",
            "params": [{ "hex": serialize_hex(&header_3b), "height": 3 }],
        })),
    )?;
    let updates = drain_requests(&mut state, &mut queue, &server);

    assert!(
        updates.iter().any(|u| u
            .chain_update
            .as_ref()
            .is_some_and(|cp| cp.block_id() == anchor_of(&header_3b, 3).block_id)),
        "chain update must follow the reorg"
    );
    assert!(
        updates.iter().any(|u| u
            .tx_update
            .anchors
            .contains(&(anchor_of(&header_2b, 2), txid))),
        "anchor must be refetched for the block that replaced the evicted one"
    );
    Ok(())
}

/// A reorg can land while an anchor fetch is in flight. The merkle proof we get back was built
/// against the chain the server had when it received the request, so it may not prove inclusion
/// in the block we now have at that height. Verifying it against that block would record a
/// permanent "not in this block" verdict for an anchor which is in fact valid.
#[test]
fn merkle_proof_predating_a_reorg_is_not_taken_as_a_failed_anchor() -> anyhow::Result<()> {
    let (descriptor, _spk_hash, spk) = tracked_descriptor()?;
    let tx = tx_paying(&spk, 50_000);
    let txid = tx.compute_txid();
    let (genesis, header_1) = base_headers();
    let header_2 = block_with_tx(&header_1, txid, 200, 0);
    // The block which replaces height 2 contains the tx alongside another one, so the tx keeps
    // its height — and with it its script status — but needs a different merkle proof.
    let proof_2b = response::TxMerkle {
        block_height: absolute::Height::from_consensus(2)?,
        merkle: vec![sha256d::Hash::hash(b"the other tx")],
        pos: 1,
    };
    let header_2b = block_with_root(&header_1, proof_2b.expected_merkle_root(txid), 222, 1);
    let header_3b = block_with_root(&header_2b, TxMerkleNode::all_zeros(), 300, 0);

    let mut state = new_state(Cache::default(), descriptor);
    let mut queue = ReqQueue::new();
    let mut server = Server {
        headers: vec![genesis, header_1, header_2],
        txs: vec![(tx, 2)],
        merkle_proof: (Vec::new(), 0),
    };

    // Sync, but hold back the merkle proof so the anchor fetch is still in flight.
    state.start(&mut queue);
    let mut in_flight = Vec::new();
    while let Some(req) = queue.pop_front() {
        if req.method.as_ref() == "blockchain.transaction.get_merkle" {
            in_flight.push(req);
            continue;
        }
        state.poll(&mut queue, response(&req, &server))?;
    }
    let stale_req = match in_flight.as_slice() {
        [req] => req.clone(),
        reqs => panic!(
            "expected exactly one merkle request in flight, got {}",
            reqs.len()
        ),
    };
    let stale_resp = response(&stale_req, &server);

    // The reorg lands before the server answers.
    server.headers = vec![genesis, header_1, header_2b, header_3b];
    server.merkle_proof = (proof_2b.merkle.clone(), proof_2b.pos);
    state.poll(
        &mut queue,
        raw_msg(json!({
            "jsonrpc": "2.0",
            "method": "blockchain.headers.subscribe",
            "params": [{ "hex": serialize_hex(&header_3b), "height": 3 }],
        })),
    )?;
    let mut updates = drain_requests(&mut state, &mut queue, &server);

    // The held answer proves inclusion in the block which was evicted, not in the one which
    // replaced it.
    updates.extend(state.poll(&mut queue, stale_resp)?);
    updates.extend(drain_requests(&mut state, &mut queue, &server));

    let anchors = updates
        .iter()
        .flat_map(|u| u.tx_update.anchors.iter().cloned())
        .collect::<Vec<_>>();
    assert!(
        anchors.contains(&(
            anchor_proved_by(&header_2b, 2, proof_2b.merkle.clone(), proof_2b.pos),
            txid
        )),
        "the anchor must be refetched rather than written off from a proof of the evicted block"
    );
    Ok(())
}

/// A reorg can land while a job is midway through fetching anchors. Anchors are staged as they
/// resolve, so the job must give up the ones it staged against the chain it started on — otherwise
/// it goes on to emit them in a single update alongside the ones it resolved against the chain it
/// ended on.
#[test]
fn anchors_staged_before_a_reorg_are_not_emitted_after_it() -> anyhow::Result<()> {
    let (descriptor, _spk_hash, spk) = tracked_descriptor()?;
    let (tx_a, tx_b) = (tx_paying(&spk, 50_000), tx_paying(&spk, 60_000));
    let (txid_a, txid_b) = (tx_a.compute_txid(), tx_b.compute_txid());
    let (genesis, header_1) = base_headers();
    let header_2 = block_with_tx(&header_1, txid_a, 200, 0);
    let header_3 = block_with_tx(&header_2, txid_b, 300, 0);
    // The reorg keeps both txs at their heights — hence the unchanged script status — but in
    // different blocks, and extends the chain by one.
    let header_2b = block_with_tx(&header_1, txid_a, 222, 1);
    let header_3b = block_with_tx(&header_2b, txid_b, 333, 1);
    let header_4b = block_with_root(&header_3b, TxMerkleNode::all_zeros(), 400, 0);

    let mut state = new_state(Cache::default(), descriptor);
    let mut queue = ReqQueue::new();
    let mut server = Server {
        headers: vec![genesis, header_1, header_2, header_3],
        txs: vec![(tx_a, 2), (tx_b, 3)],
        merkle_proof: (Vec::new(), 0),
    };

    // Sync, but hold back tx_b's proof so that the job has staged tx_a's anchor and is still
    // waiting on tx_b's when the reorg lands.
    state.start(&mut queue);
    let mut in_flight = Vec::new();
    while let Some(req) = queue.pop_front() {
        if req.method.as_ref() == "blockchain.transaction.get_merkle"
            && req.params[0] == json!(txid_b.to_string())
        {
            in_flight.push(req);
            continue;
        }
        state.poll(&mut queue, response(&req, &server))?;
    }
    let held_req = match in_flight.as_slice() {
        [req] => req.clone(),
        reqs => panic!("expected one held merkle request, got {}", reqs.len()),
    };
    let held_resp = response(&held_req, &server);

    server.headers = vec![genesis, header_1, header_2b, header_3b, header_4b];
    state.poll(
        &mut queue,
        raw_msg(json!({
            "jsonrpc": "2.0",
            "method": "blockchain.headers.subscribe",
            "params": [{ "hex": serialize_hex(&header_4b), "height": 4 }],
        })),
    )?;
    let mut updates = drain_requests(&mut state, &mut queue, &server);
    updates.extend(state.poll(&mut queue, held_resp)?);
    updates.extend(drain_requests(&mut state, &mut queue, &server));

    let anchors = updates
        .iter()
        .flat_map(|u| u.tx_update.anchors.iter().cloned())
        .collect::<Vec<_>>();
    for evicted in [
        (anchor_of(&header_2, 2), txid_a),
        (anchor_of(&header_3, 3), txid_b),
    ] {
        assert!(
            !anchors.contains(&evicted),
            "an anchor to an evicted block must not be emitted after the reorg: {evicted:?}"
        );
    }
    for expected in [
        (anchor_of(&header_2b, 2), txid_a),
        (anchor_of(&header_3b, 3), txid_b),
    ] {
        assert!(
            anchors.contains(&expected),
            "both anchors must be refetched against the new chain: {expected:?}"
        );
    }
    Ok(())
}

/// The everyday reorg: one which takes a transaction out of its block and back to the mempool.
/// The refetch is speculative — we ask for a proof of inclusion at a height the transaction was
/// *last seen* at — so the server answering "not in that block" is expected, and must not take
/// the connection down with it.
#[test]
fn a_tx_unconfirmed_by_a_reorg_does_not_error_the_connection() -> anyhow::Result<()> {
    let (descriptor, _spk_hash, spk) = tracked_descriptor()?;
    let tx = tx_paying(&spk, 50_000);
    let txid = tx.compute_txid();
    let (genesis, header_1) = base_headers();
    let header_2 = block_with_tx(&header_1, txid, 200, 0);
    // Height 2 is replaced by a block without the tx, and the chain grows by one.
    let header_2b = block_with_root(&header_1, TxMerkleNode::all_zeros(), 222, 1);
    let header_3b = block_with_root(&header_2b, TxMerkleNode::all_zeros(), 333, 0);

    let mut state = new_state(Cache::default(), descriptor);
    let mut queue = ReqQueue::new();
    let mut server = Server {
        headers: vec![genesis, header_1, header_2],
        txs: vec![(tx, 2)],
        merkle_proof: (Vec::new(), 0),
    };

    state.start(&mut queue);
    let updates = drain_requests(&mut state, &mut queue, &server);
    assert!(
        updates.iter().any(|u| u
            .tx_update
            .anchors
            .contains(&(anchor_of(&header_2, 2), txid))),
        "tx must first be anchored"
    );

    // The reorg leaves the tx in the mempool, so the server no longer has it in any block.
    server.headers = vec![genesis, header_1, header_2b, header_3b];
    server.txs = Vec::new();
    state.poll(
        &mut queue,
        raw_msg(json!({
            "jsonrpc": "2.0",
            "method": "blockchain.headers.subscribe",
            "params": [{ "hex": serialize_hex(&header_3b), "height": 3 }],
        })),
    )?;

    while let Some(req) = queue.pop_front() {
        state
            .poll(&mut queue, response(&req, &server))
            .map_err(|e| anyhow::anyhow!("{e:#}"))?;
    }
    Ok(())
}

/// The other half of `merkle_proof_predating_a_reorg_is_not_taken_as_a_failed_anchor`: a server
/// answers a proof request from the chain it had when it *received* it, so if the tx was out of
/// its block at that moment it answers with an error — an error about the chain we have since
/// left. Blaming that on whichever block the reorg put at the height would write off an anchor
/// which is in fact valid.
#[test]
fn merkle_error_predating_a_reorg_is_not_taken_as_a_failed_anchor() -> anyhow::Result<()> {
    let (descriptor, _spk_hash, spk) = tracked_descriptor()?;
    let tx = tx_paying(&spk, 50_000);
    let txid = tx.compute_txid();
    let (genesis, header_1) = base_headers();
    let header_2 = block_with_tx(&header_1, txid, 200, 0);
    // Height 2 is replaced by a block which contains the tx too, so the anchor is still valid.
    let header_2b = block_with_tx(&header_1, txid, 222, 1);
    let header_3b = block_with_root(&header_2b, TxMerkleNode::all_zeros(), 300, 0);

    let mut state = new_state(Cache::default(), descriptor);
    let mut queue = ReqQueue::new();
    let mut server = Server {
        headers: vec![genesis, header_1, header_2],
        txs: vec![(tx, 2)],
        merkle_proof: (Vec::new(), 0),
    };

    // Sync, but hold back the merkle request so the anchor fetch is still in flight.
    state.start(&mut queue);
    let mut in_flight = Vec::new();
    while let Some(req) = queue.pop_front() {
        if req.method.as_ref() == "blockchain.transaction.get_merkle" {
            in_flight.push(req);
            continue;
        }
        state.poll(&mut queue, response(&req, &server))?;
    }
    let stale_req = match in_flight.as_slice() {
        [req] => req.clone(),
        reqs => panic!(
            "expected exactly one merkle request in flight, got {}",
            reqs.len()
        ),
    };

    // The reorg lands before the server answers.
    server.headers = vec![genesis, header_1, header_2b, header_3b];
    state.poll(
        &mut queue,
        raw_msg(json!({
            "jsonrpc": "2.0",
            "method": "blockchain.headers.subscribe",
            "params": [{ "hex": serialize_hex(&header_3b), "height": 3 }],
        })),
    )?;
    let mut updates = drain_requests(&mut state, &mut queue, &server);

    // The held request was received while the tx was out of its block, so it is answered with
    // an error — the wording is the one romanz/electrs really sends, a bare JSON string which
    // conflates a genuine fault with the everyday reorg.
    updates.extend(state.poll(
        &mut queue,
        raw_msg(json!({
            "jsonrpc": "2.0",
            "id": stale_req.id,
            "error": "tx not found or is unconfirmed",
        })),
    )?);
    updates.extend(drain_requests(&mut state, &mut queue, &server));

    assert!(
        updates.iter().any(|u| u
            .tx_update
            .anchors
            .contains(&(anchor_of(&header_2b, 2), txid))),
        "the anchor must be refetched rather than written off from an error about the evicted block"
    );
    Ok(())
}

/// A server error is not a disproof — it is equally a rate limit, an index still catching up or
/// a daemon hiccup — so it must not be recorded as one. The transaction stays in the record of
/// what was seen at that height, so the next reorg of that height asks again; and the job it
/// blocked must still finish rather than re-ask in a loop.
#[test]
fn a_merkle_error_is_not_recorded_as_a_failed_anchor() -> anyhow::Result<()> {
    let (descriptor, _spk_hash, spk) = tracked_descriptor()?;
    let tx = tx_paying(&spk, 50_000);
    let txid = tx.compute_txid();
    let (genesis, header_1) = base_headers();
    let header_2 = block_with_tx(&header_1, txid, 200, 0);
    // Height 2 is replaced by a block containing the tx, and the chain grows by one.
    let header_2b = block_with_tx(&header_1, txid, 222, 1);
    let header_3b = block_with_root(&header_2b, TxMerkleNode::all_zeros(), 300, 0);

    let mut state = new_state(Cache::default(), descriptor);
    let mut queue = ReqQueue::new();
    let mut server = Server {
        headers: vec![genesis, header_1, header_2],
        txs: vec![(tx.clone(), 2)],
        merkle_proof: (Vec::new(), 0),
    };

    // Sync, but fail every merkle request the way a busy server would.
    state.start(&mut queue);
    let mut merkle_requests = 0;
    while let Some(req) = queue.pop_front() {
        let resp = if req.method.as_ref() == "blockchain.transaction.get_merkle" {
            merkle_requests += 1;
            assert!(
                merkle_requests < 10,
                "a server error must not put the job in a re-ask loop"
            );
            raw_msg(json!({
                "jsonrpc": "2.0",
                "id": req.id,
                "error": { "code": 1, "message": "server busy" },
            }))
        } else {
            response(&req, &server)
        };
        state.poll(&mut queue, resp)?;
    }
    assert_eq!(
        merkle_requests, 1,
        "the job must give up on the pair rather than re-ask"
    );
    assert!(
        state.cache().tx_cache.anchors.is_empty(),
        "an error proves nothing, so no anchor may be recorded from it"
    );

    // The reorg replaces the block, so the anchor is asked for again — which it could not be
    // had the error dropped this script from `spk_hashes_by_height`.
    server.headers = vec![genesis, header_1, header_2b, header_3b];
    state.poll(
        &mut queue,
        raw_msg(json!({
            "jsonrpc": "2.0",
            "method": "blockchain.headers.subscribe",
            "params": [{ "hex": serialize_hex(&header_3b), "height": 3 }],
        })),
    )?;
    let updates = drain_requests(&mut state, &mut queue, &server);

    assert!(
        updates.iter().any(|u| u
            .tx_update
            .anchors
            .contains(&(anchor_of(&header_2b, 2), txid))),
        "the tx must still be asked about at this height after a server error"
    );
    Ok(())
}

/// A fork of the same height, carrying no more work than the chain it would replace, must be
/// refused — and refusing it must leave the anchor we already have intact.
///
/// This is what a full node does: an equal-work fork loses to the chain already in hand. The
/// server having moved to it is not evidence, since a server is exactly what the verified chain
/// exists to stop trusting. When the fork does out-work us, the tip announcement that says so is
/// what triggers the switch, and
/// [`anchor_is_refetched_when_tx_moves_to_another_block_of_same_height`] covers that.
#[test]
fn a_fork_without_more_work_is_refused() -> anyhow::Result<()> {
    let (descriptor, _spk_hash, spk) = tracked_descriptor()?;
    let tx = tx_paying(&spk, 50_000);
    let txid = tx.compute_txid();
    let (genesis, header_1) = base_headers();
    let header_2 = block_with_tx(&header_1, txid, 200, 0);
    let header_2b = block_with_tx(&header_1, txid, 222, 1);
    assert_ne!(header_2.block_hash(), header_2b.block_hash());

    let mut state = new_state(Cache::default(), descriptor);
    let mut queue = ReqQueue::new();
    let mut server = Server {
        headers: vec![genesis, header_1, header_2],
        txs: vec![(tx, 2)],
        merkle_proof: (Vec::new(), 0),
    };

    state.start(&mut queue);
    let updates = drain_requests(&mut state, &mut queue, &server);
    assert!(
        updates.iter().any(|u| u
            .tx_update
            .anchors
            .contains(&(anchor_of(&header_2, 2), txid))),
        "tx must first be anchored to the original block"
    );

    // The tip does not move: same height, different block, unchanged script status.
    server.headers = vec![genesis, header_1, header_2b];
    let notified = state.poll(
        &mut queue,
        raw_msg(json!({
            "jsonrpc": "2.0",
            "method": "blockchain.headers.subscribe",
            "params": [{ "hex": serialize_hex(&header_2b), "height": 2 }],
        })),
    );
    let err = notified
        .and_then(|_| drain_requests_fallible(&mut state, &mut queue, &server))
        .expect_err("an equal-work fork must be refused");
    assert!(
        format!("{err:#}").contains("without more work"),
        "the error must name the reason, got: {err:#}"
    );
    assert_eq!(
        state.chain().block_hash(2),
        Some(header_2.block_hash()),
        "the chain must be left on the block it already had"
    );
    Ok(())
}

/// A proof is verified against the merkle root of the block we have at that height, so a job
/// which asks for the proof and that block's header together only works if the server answers in
/// request order. It need not: the protocol carries request ids for that reason.
#[test]
fn anchor_is_refetched_whatever_order_the_server_answers_in() -> anyhow::Result<()> {
    let (descriptor, _spk_hash, spk) = tracked_descriptor()?;
    let tx = tx_paying(&spk, 50_000);
    let txid = tx.compute_txid();
    let (genesis, header_1) = base_headers();
    let header_2 = block_with_tx(&header_1, txid, 200, 0);
    // Height 2 is replaced and the chain grows, so the notified header is height 3's — the
    // replacement block's header has to be fetched before its proof can be verified.
    let header_2b = block_with_tx(&header_1, txid, 222, 1);
    let header_3b = block_with_root(&header_2b, TxMerkleNode::all_zeros(), 300, 0);

    let mut state = new_state(Cache::default(), descriptor);
    let mut queue = ReqQueue::new();
    let mut server = Server {
        headers: vec![genesis, header_1, header_2],
        txs: vec![(tx, 2)],
        merkle_proof: (Vec::new(), 0),
    };

    state.start(&mut queue);
    let updates = drain_requests(&mut state, &mut queue, &server);
    assert!(
        updates.iter().any(|u| u
            .tx_update
            .anchors
            .contains(&(anchor_of(&header_2, 2), txid))),
        "tx must first be anchored to the original block"
    );

    server.headers = vec![genesis, header_1, header_2b, header_3b];
    state.poll(
        &mut queue,
        raw_msg(json!({
            "jsonrpc": "2.0",
            "method": "blockchain.headers.subscribe",
            "params": [{ "hex": serialize_hex(&header_3b), "height": 3 }],
        })),
    )?;
    let updates = drain_requests_proofs_first(&mut state, &mut queue, &server);

    assert!(
        updates.iter().any(|u| u
            .tx_update
            .anchors
            .contains(&(anchor_of(&header_2b, 2), txid))),
        "the anchor must be refetched even when the proof overtakes the header"
    );
    Ok(())
}

/// A client restored from a trusted block has no verified history below it, so a transaction
/// confirmed down there cannot be anchored until the chain is backfilled to a block it trusts.
///
/// The proof must still not be checked before the header it is verified against arrives, which
/// `drain_requests_proofs_first` forces by answering every merkle request ahead of everything
/// else.
#[test]
fn anchor_below_the_trusted_block_is_backfilled() -> anyhow::Result<()> {
    let (descriptor, _spk_hash, spk) = tracked_descriptor()?;
    let tx = tx_paying(&spk, 50_000);
    let txid = tx.compute_txid();
    let (genesis, header_1) = base_headers();
    let header_2 = block_with_tx(&header_1, txid, 200, 0);
    let header_3 = block_with_root(&header_2, TxMerkleNode::all_zeros(), 300, 0);

    // Trusting the tip puts the chain base above the transaction, so nothing below it is
    // verified and the sync range covers none of it.
    let mut state = new_state_trusting(Cache::default(), descriptor, [(3, header_3)]);
    let mut queue = ReqQueue::new();
    let server = Server {
        headers: vec![genesis, header_1, header_2, header_3],
        txs: vec![(tx, 2)],
        merkle_proof: (Vec::new(), 0),
    };

    state.start(&mut queue);
    let updates = drain_requests_proofs_first(&mut state, &mut queue, &server);

    assert!(
        updates.iter().any(|u| u
            .tx_update
            .anchors
            .contains(&(anchor_of(&header_2, 2), txid))),
        "the anchor must resolve once history below the trusted block is backfilled"
    );
    assert_eq!(
        state.chain().base_height(),
        1,
        "the chain must have grown down to just above genesis"
    );
    Ok(())
}

/// A header batch fetched before a reorg describes the chain we have since left behind, and
/// splicing it in would put a purged block into the verified chain.
///
/// The batch is dropped when the tip that wanted it is abandoned, so the answer never reaches
/// the chain at all — and even if it did, it neither links to the chain we moved to nor
/// out-works it.
#[test]
fn header_fetched_before_a_reorg_is_not_spliced_into_the_chain() -> anyhow::Result<()> {
    let (descriptor, _spk_hash, spk) = tracked_descriptor()?;
    let tx = tx_paying(&spk, 50_000);
    let txid = tx.compute_txid();
    let (genesis, header_1) = base_headers();

    // Two chains which differ at height 2. Both contain the tx there, so the anchor stays valid
    // throughout — only the block it belongs to changes. The second is longer, so it carries
    // more work and is allowed to replace the first.
    let build = |second: block::Header, tip: u32, nonce: u32| {
        let mut chain = vec![genesis, header_1, second];
        for height in 3..=tip {
            let prev = *chain.last().expect("non-empty");
            chain.push(block_with_root(
                &prev,
                TxMerkleNode::all_zeros(),
                1000 + height,
                nonce,
            ));
        }
        chain
    };
    let chain_a = build(block_with_tx(&header_1, txid, 200, 0), 8, 0);
    let chain_b = build(block_with_tx(&header_1, txid, 222, 1), 9, 1);
    let (a2, b2) = (chain_a[2], chain_b[2]);
    assert_ne!(a2.block_hash(), b2.block_hash());

    let mut state = new_state(Cache::default(), descriptor);
    let mut queue = ReqQueue::new();
    let mut server = Server {
        headers: chain_a.clone(),
        txs: vec![(tx, 2)],
        merkle_proof: (Vec::new(), 0),
    };

    // Sync, but hold back the batch covering height 2 so that fetch is still in flight.
    state.start(&mut queue);
    let mut in_flight = Vec::new();
    while let Some(req) = queue.pop_front() {
        if req.method.as_ref() == "blockchain.block.headers" {
            let start = req.params[0].as_u64().expect("must have start_height");
            let count = req.params[1].as_u64().expect("must have count");
            if (start..start + count).contains(&2) {
                in_flight.push(req);
                continue;
            }
        }
        state.poll(&mut queue, response(&req, &server))?;
    }
    let stale_req = match in_flight.as_slice() {
        [req] => req.clone(),
        reqs => panic!("expected one held header batch, got {}", reqs.len()),
    };
    let stale_resp = response(&stale_req, &server);

    server.headers = chain_b.clone();
    state.poll(
        &mut queue,
        raw_msg(json!({
            "jsonrpc": "2.0",
            "method": "blockchain.headers.subscribe",
            "params": [{ "hex": serialize_hex(&chain_b[9]), "height": 9 }],
        })),
    )?;
    let mut updates = drain_requests(&mut state, &mut queue, &server);

    // The held answer describes the chain we have left behind.
    updates.extend(state.poll(&mut queue, stale_resp)?);
    updates.extend(drain_requests(&mut state, &mut queue, &server));

    assert_eq!(
        state.chain().block_hash(2),
        Some(b2.block_hash()),
        "the height must be verified against the chain we are actually on"
    );
    assert_ne!(
        state.chain().block_hash(2),
        Some(a2.block_hash()),
        "a header from the chain we left must not enter the verified chain"
    );
    assert!(
        updates
            .iter()
            .any(|u| u.tx_update.anchors.contains(&(anchor_of(&b2, 2), txid))),
        "and the anchor must resolve against that block"
    );
    Ok(())
}

/// The refetch is a script hash notification we raise ourselves, so it must not displace one the
/// server actually sent. A real notification carries a status at least as new as anything we
/// could replay from cache; replacing its job would resolve the script against a stale history
/// and drop whatever the new status was reporting.
#[test]
fn a_replayed_job_does_not_displace_one_the_server_started() -> anyhow::Result<()> {
    let (descriptor, spk_hash, spk) = tracked_descriptor()?;
    let (tx_a, tx_b) = (tx_paying(&spk, 50_000), tx_paying(&spk, 60_000));
    let (txid_a, txid_b) = (tx_a.compute_txid(), tx_b.compute_txid());
    let (genesis, _) = base_headers();
    let header_1 = block_with_tx(&genesis, txid_b, 100, 0);
    let header_2 = block_with_tx(&header_1, txid_a, 200, 0);
    // The reorg replaces height 2 with another block holding tx_a, and extends the chain.
    let header_2b = block_with_tx(&header_1, txid_a, 222, 1);
    let header_3b = block_with_root(&header_2b, TxMerkleNode::all_zeros(), 300, 0);

    let mut state = new_state(Cache::default(), descriptor);
    let mut queue = ReqQueue::new();
    let mut server = Server {
        headers: vec![genesis, header_1, header_2],
        // The server only reports tx_a to begin with.
        txs: vec![(tx_a, 2)],
        merkle_proof: (Vec::new(), 0),
    };

    state.start(&mut queue);
    let updates = drain_requests(&mut state, &mut queue, &server);
    assert!(
        updates.iter().any(|u| u
            .tx_update
            .anchors
            .contains(&(anchor_of(&header_2, 2), txid_a))),
        "tx_a must first be anchored to the original block"
    );

    // The server now reports tx_b as well, and notifies the new status. The job that starts is
    // the only thing which knows about tx_b.
    server.txs.insert(0, (tx_b, 1));
    let new_status =
        ElectrumScriptStatus::from_history(&server.history(&json!(spk_hash.to_string())))
            .expect("history must be non-empty");
    state.poll(
        &mut queue,
        raw_msg(json!({
            "jsonrpc": "2.0",
            "method": "blockchain.scripthash.subscribe",
            "params": [spk_hash.to_string(), new_status.to_string()],
        })),
    )?;

    // Hold back that job's history, so it is still in flight when the reorg lands.
    let mut in_flight = Vec::new();
    while let Some(req) = queue.pop_front() {
        if req.method.as_ref() == "blockchain.scripthash.get_history" {
            in_flight.push(req);
            continue;
        }
        state.poll(&mut queue, response(&req, &server))?;
    }
    let held_req = match in_flight.as_slice() {
        [req] => req.clone(),
        reqs => panic!("expected one held history request, got {}", reqs.len()),
    };

    // The reorg evicts height 2, where this script is recorded — so the refetch wants to replay
    // its job, and must decline because the server's own job is already there.
    server.headers = vec![genesis, header_1, header_2b, header_3b];
    state.poll(
        &mut queue,
        raw_msg(json!({
            "jsonrpc": "2.0",
            "method": "blockchain.headers.subscribe",
            "params": [{ "hex": serialize_hex(&header_3b), "height": 3 }],
        })),
    )?;
    let mut updates = drain_requests(&mut state, &mut queue, &server);
    updates.extend(state.poll(&mut queue, response(&held_req, &server))?);
    updates.extend(drain_requests(&mut state, &mut queue, &server));

    assert!(
        updates
            .iter()
            .any(|u| u.tx_update.txs.iter().any(|tx| tx.compute_txid() == txid_b)),
        "the server's own job must survive and deliver what its status was reporting"
    );
    assert!(
        updates.iter().any(|u| u
            .tx_update
            .anchors
            .contains(&(anchor_of(&header_2b, 2), txid_a))),
        "and must still refetch the anchor the reorg invalidated"
    );
    Ok(())
}

/// A trusted block the server disagrees with must stop the connection, not spin it.
///
/// Trusting a block is the user's assertion that it is canonical, so a server offering a
/// different one at that height is not something to reconcile — every header above it descends
/// from a block we do not accept. The old client would ask again forever; this one refuses the
/// run and errors out, and the error names the conflict.
#[test]
fn a_server_disagreeing_with_a_trusted_block_stops_the_connection() -> anyhow::Result<()> {
    let (descriptor, _spk_hash, spk) = tracked_descriptor()?;
    let tx = tx_paying(&spk, 50_000);
    let txid = tx.compute_txid();
    let (genesis, header_1) = base_headers();
    // The server's height 2, and the one we have been told to trust.
    let server_2 = block_with_tx(&header_1, txid, 200, 0);
    let trusted_2 = block_with_tx(&header_1, txid, 999, 7);
    assert_ne!(server_2.block_hash(), trusted_2.block_hash());

    let mut chain = vec![genesis, header_1, server_2];
    for height in 3..=8u32 {
        let prev = *chain.last().expect("non-empty");
        chain.push(block_with_root(
            &prev,
            TxMerkleNode::all_zeros(),
            1000 + height,
            0,
        ));
    }

    let mut state = new_state_trusting(Cache::default(), descriptor, [(2, trusted_2)]);
    let mut queue = ReqQueue::new();
    let server = Server {
        headers: chain,
        txs: vec![(tx, 2)],
        merkle_proof: (Vec::new(), 0),
    };

    state.start(&mut queue);
    let mut served = 0;
    let err = loop {
        let req = match queue.pop_front() {
            Some(req) => req,
            None => panic!("the client must not settle while it disagrees with the server"),
        };
        served += 1;
        assert!(
            served < 200,
            "the client must not loop: {served} requests, last was {} {:?}",
            req.method,
            req.params
        );
        if let Err(err) = state.poll(&mut queue, response(&req, &server)) {
            break err;
        }
    };
    let msg = format!("{err:#}");
    assert!(
        msg.contains("does not link") || msg.contains("trusted"),
        "the error must name the conflict, got: {msg}"
    );
    Ok(())
}

/// A history that comes back empty is the server saying the script has nothing, which is as much
/// an answer as a null status in a notification — and the two paths have to agree, or a script
/// whose transactions vanished between the notification and the answer keeps a status no script
/// should still answer to, and the confirmation job goes on anchoring transactions the server no
/// longer lists.
///
/// The history behind that status has to go too. It is keyed by status, not by script, so leaving
/// it once nothing points at it strands a `Vec` in a structure that is persisted.
#[test]
fn a_history_that_comes_back_empty_clears_the_subscription() -> anyhow::Result<()> {
    let (descriptor, spk_hash, spk) = tracked_descriptor()?;
    let tx = tx_paying(&spk, 50_000);
    let txid = tx.compute_txid();
    let (genesis, header_1) = base_headers();
    let header_2 = block_with_tx(&header_1, txid, 200, 0);

    let mut state = new_state(Cache::default(), descriptor);
    let mut queue = ReqQueue::new();
    let mut server = Server {
        headers: vec![genesis, header_1, header_2],
        txs: vec![(tx, 2)],
        merkle_proof: (Vec::new(), 0),
    };

    state.start(&mut queue);
    drain_requests(&mut state, &mut queue, &server);
    let old_status = state
        .subscriptions()
        .spk_status(spk_hash)
        .expect("the script has a history to start with");
    assert!(
        state.subscriptions().spk_history(old_status).is_some(),
        "the history behind that status must be held"
    );

    // The server reports a second payment, so the status moves and the job goes to fetch the
    // history it stands for.
    let tx2 = tx_paying(&spk, 60_000);
    server.txs.push((tx2, 0));
    let new_status =
        ElectrumScriptStatus::from_history(&server.history(&json!(spk_hash.to_string())))
            .expect("history must be non-empty");
    assert_ne!(new_status, old_status);
    state.poll(
        &mut queue,
        raw_msg(json!({
            "jsonrpc": "2.0",
            "method": "blockchain.scripthash.subscribe",
            "params": [spk_hash.to_string(), new_status.to_string()],
        })),
    )?;

    // By the time we ask, everything the script had is gone — both payments replaced. The server
    // answers with an empty history, which no status stands for.
    let mut answered_empty = false;
    while let Some(req) = queue.pop_front() {
        let resp = if req.method.as_ref() == "blockchain.scripthash.get_history" {
            answered_empty = true;
            raw_msg(json!({ "jsonrpc": "2.0", "id": req.id, "result": [] }))
        } else {
            response(&req, &server)
        };
        state.poll(&mut queue, resp)?;
    }
    assert!(answered_empty, "the job must have asked for the history");

    assert!(
        state.subscriptions().spk_status(spk_hash).is_none(),
        "an empty history must clear the script's status"
    );
    assert!(
        state.subscriptions().spk_history(old_status).is_none(),
        "and drop the history nothing answers to any more"
    );
    Ok(())
}

/// A replay is built from the last status and the heights it was seen at, so both have to go
/// when the server stops reporting a history for the script — an RBF'd transaction, say. Left
/// behind, a later eviction at that height would rebuild the job from a history the script no
/// longer has and go asking for proofs of a transaction that is gone.
#[test]
fn a_script_whose_history_goes_away_is_not_replayed() -> anyhow::Result<()> {
    let (descriptor, spk_hash, spk) = tracked_descriptor()?;
    let tx = tx_paying(&spk, 50_000);
    let txid = tx.compute_txid();
    let (genesis, header_1) = base_headers();
    let header_2 = block_with_tx(&header_1, txid, 200, 0);
    let header_2b = block_with_root(&header_1, TxMerkleNode::all_zeros(), 222, 1);
    let header_3b = block_with_root(&header_2b, TxMerkleNode::all_zeros(), 300, 0);

    let mut state = new_state(Cache::default(), descriptor);
    let mut queue = ReqQueue::new();
    let mut server = Server {
        headers: vec![genesis, header_1, header_2],
        txs: vec![(tx, 2)],
        merkle_proof: (Vec::new(), 0),
    };

    state.start(&mut queue);
    let updates = drain_requests(&mut state, &mut queue, &server);
    assert!(
        updates.iter().any(|u| u
            .tx_update
            .anchors
            .contains(&(anchor_of(&header_2, 2), txid))),
        "tx must first be anchored"
    );
    assert!(
        state.subscriptions().spk_status(spk_hash).is_some(),
        "the status must be recorded while the script has a history"
    );

    // The transaction is gone, so the script's status goes to null.
    server.txs = Vec::new();
    state.poll(
        &mut queue,
        raw_msg(json!({
            "jsonrpc": "2.0",
            "method": "blockchain.scripthash.subscribe",
            "params": [spk_hash.to_string(), null],
        })),
    )?;
    drain_requests(&mut state, &mut queue, &server);
    assert!(
        !state.subscriptions().spk_status(spk_hash).is_some(),
        "a null status must drop the recorded status"
    );

    // A reorg evicting the height it used to be seen at must not replay anything.
    server.headers = vec![genesis, header_1, header_2b, header_3b];
    state.poll(
        &mut queue,
        raw_msg(json!({
            "jsonrpc": "2.0",
            "method": "blockchain.headers.subscribe",
            "params": [{ "hex": serialize_hex(&header_3b), "height": 3 }],
        })),
    )?;
    let mut asked = Vec::new();
    while let Some(req) = queue.pop_front() {
        asked.push(req.method.to_string());
        state.poll(&mut queue, response(&req, &server))?;
    }
    assert!(
        !asked
            .iter()
            .any(|m| m == "blockchain.transaction.get_merkle"),
        "no proof may be asked for a transaction the script no longer has, got: {asked:?}"
    );
    Ok(())
}

/// A server proves inclusion in whichever block *it* has at a height, so a proof whose root does
/// not match ours says the two chains disagree there — not that the transaction is absent from
/// our block. Remembering that against our block would be a verdict the proof cannot support, and
/// a chain that came back to that block would consult it and skip the anchor for good.
#[test]
fn a_proof_for_another_block_is_not_a_verdict_on_ours() -> anyhow::Result<()> {
    let (descriptor, spk_hash, spk) = tracked_descriptor()?;
    let tx = tx_paying(&spk, 50_000);
    let txid = tx.compute_txid();
    let (genesis, header_1) = base_headers();
    let ours = block_with_tx(&header_1, txid, 200, 0);
    // Their block holds the tx alongside another, so it needs a different proof — the root the
    // server's proof expands to cannot match the root of our block.
    let proof_theirs = response::TxMerkle {
        block_height: absolute::Height::from_consensus(2)?,
        merkle: vec![sha256d::Hash::hash(b"the other tx")],
        pos: 1,
    };
    let theirs = block_with_root(&header_1, proof_theirs.expected_merkle_root(txid), 222, 1);
    assert_ne!(ours.merkle_root, theirs.merkle_root);

    // The chain the server serves is ours — a block it genuinely disagreed with would be caught
    // by header verification long before any proof. What it gets wrong is the *proof*: the
    // branch it answers with expands to a root that is not the one in our block.
    let mut chain = vec![genesis, header_1, ours];
    for height in 3..=8u32 {
        let prev = *chain.last().expect("non-empty");
        chain.push(block_with_root(
            &prev,
            TxMerkleNode::all_zeros(),
            1000 + height,
            0,
        ));
    }

    let mut state = new_state(Cache::default(), descriptor);
    let mut queue = ReqQueue::new();
    let mut server = Server {
        headers: chain,
        txs: vec![(tx, 2)],
        merkle_proof: (proof_theirs.merkle.clone(), proof_theirs.pos),
    };

    state.start(&mut queue);
    let mut served = 0;
    while let Some(req) = queue.pop_front() {
        served += 1;
        assert!(served < 200, "a mismatch must not become a request loop");
        state.poll(&mut queue, response(&req, &server))?;
    }
    assert!(
        state.cache().tx_cache.anchors.is_empty(),
        "a proof for a block we do not have must not anchor anything"
    );

    // The server starts answering with the right proof. Nothing durable was written against the
    // bad one, so the job a notification rebuilds must be able to anchor there.
    server.merkle_proof = (Vec::new(), 0);
    let status = ElectrumScriptStatus::from_history(&server.history(&json!(spk_hash.to_string())))
        .expect("history must be non-empty");
    state.poll(
        &mut queue,
        raw_msg(json!({
            "jsonrpc": "2.0",
            "method": "blockchain.scripthash.subscribe",
            "params": [spk_hash.to_string(), status.to_string()],
        })),
    )?;
    let updates = drain_requests(&mut state, &mut queue, &server);
    assert!(
        updates
            .iter()
            .any(|u| u.tx_update.anchors.contains(&(anchor_of(&ours, 2), txid))),
        "the anchor must still be reachable once the chains agree again"
    );
    Ok(())
}

/// A server erroring a merkle request says nothing about the block itself — the headers agree
/// throughout — so no reorg is coming to explain it, and below the reorg window nothing about
/// the chain ever changes to trigger a refetch either. The tip stays put.
///
/// The script notification is the only thing that comes back, and it can only revive a job that
/// still exists — [`ConfirmationJob`] is built in `on_new_tip` and nowhere else. Dropping the job
/// on the error would strand the anchor until an unrelated block arrives.
#[test]
fn a_merkle_error_below_the_reorg_window_is_recovered_by_a_script_notification(
) -> anyhow::Result<()> {
    let (descriptor, spk_hash, spk) = tracked_descriptor()?;
    let tx = tx_paying(&spk, 50_000);
    let txid = tx.compute_txid();
    let (genesis, header_1) = base_headers();
    let ours = block_with_tx(&header_1, txid, 200, 0);

    let mut chain = vec![genesis, header_1, ours];
    for height in 3..=30u32 {
        let prev = *chain.last().expect("non-empty");
        chain.push(block_with_root(
            &prev,
            TxMerkleNode::all_zeros(),
            1000 + height,
            0,
        ));
    }

    let mut state = new_state(Cache::default(), descriptor);
    let mut queue = ReqQueue::new();
    let server = Server {
        headers: chain,
        txs: vec![(tx, 2)],
        merkle_proof: (Vec::new(), 0),
    };

    state.start(&mut queue);
    let mut served = 0;
    while let Some(req) = queue.pop_front() {
        served += 1;
        assert!(served < 200, "an error must not become a request loop");
        let resp = if req.method.as_ref() == "blockchain.transaction.get_merkle" {
            raw_msg(json!({
                "jsonrpc": "2.0",
                "id": req.id,
                "error": { "code": 1, "message": "tx not found or is unconfirmed" },
            }))
        } else {
            response(&req, &server)
        };
        state.poll(&mut queue, resp)?;
    }
    assert!(
        state.cache().tx_cache.anchors.is_empty(),
        "an error must not anchor anything"
    );
    assert_eq!(
        state.chain().tip_height(),
        Some(30),
        "the tip must have synced past the reorg window despite the error"
    );

    // The tip is untouched and 28 blocks above height 2 — well past the reorg window a tip
    // movement would rewrite — so this notification is the whole of the recovery.
    let status = ElectrumScriptStatus::from_history(&server.history(&json!(spk_hash.to_string())))
        .expect("history must be non-empty");
    state.poll(
        &mut queue,
        raw_msg(json!({
            "jsonrpc": "2.0",
            "method": "blockchain.scripthash.subscribe",
            "params": [spk_hash.to_string(), status.to_string()],
        })),
    )?;
    let updates = drain_requests(&mut state, &mut queue, &server);
    assert!(
        updates
            .iter()
            .any(|u| u.tx_update.anchors.contains(&(anchor_of(&ours, 2), txid))),
        "the anchor must be reachable once the server proves it again"
    );
    Ok(())
}

/// A job needs both the transactions and their anchors, and the server answers in any order.
///
/// Every other test lets the `GetTx` land first, which puts the anchors on the job's final pass
/// with nothing running after them. This forces the other order: the merkle proof arrives first,
/// so the anchor resolves early and the job runs again when the transaction finally lands.
///
/// Anchors are re-resolved from scratch on every pass, so that later pass must not lose the
/// anchor already in hand — the set of what to anchor is the question, not the answer, and
/// clearing it once answered would have the next pass ask an empty question and stage nothing.
#[test]
fn anchor_survives_a_pass_that_happens_after_it_resolved() -> anyhow::Result<()> {
    let (descriptor, _spk_hash, spk) = tracked_descriptor()?;
    let tx = tx_paying(&spk, 50_000);
    let txid = tx.compute_txid();
    let (genesis, header_1) = base_headers();
    let header_2 = block_with_tx(&header_1, txid, 200, 0);

    let mut state = new_state(Cache::default(), descriptor);
    let mut queue = ReqQueue::new();
    let server = Server {
        headers: vec![genesis, header_1, header_2],
        txs: vec![(tx, 2)],
        merkle_proof: (Vec::new(), 0),
    };

    state.start(&mut queue);
    let updates = drain_requests_proofs_first(&mut state, &mut queue, &server);
    assert!(
        updates.iter().any(|u| u
            .tx_update
            .anchors
            .contains(&(anchor_of(&header_2, 2), txid))),
        "the anchor must still be emitted after a later pass"
    );
    Ok(())
}

/// A tip notification landing while the previous one's headers are still in flight.
///
/// The replacement job asks for the same heights, so its request is byte-identical to the one
/// already out. Deduplicated against that one, nothing new is sent, and the answer already on its
/// way describes the chain the server has just left.
#[test]
fn a_tip_that_moves_while_headers_are_in_flight_is_not_lost() -> anyhow::Result<()> {
    let (descriptor, _spk_hash, _spk) = tracked_descriptor()?;
    let (genesis, header_1) = base_headers();
    let build = |nonce: u32| {
        let mut chain = vec![genesis, header_1];
        for height in 2..=3 {
            let prev = *chain.last().expect("non-empty");
            chain.push(block_with_root(
                &prev,
                TxMerkleNode::all_zeros(),
                1000 + height,
                nonce,
            ));
        }
        chain
    };
    let (chain_a, chain_b) = (build(0), build(1));
    assert_ne!(chain_a[3].block_hash(), chain_b[3].block_hash());

    let mut state = new_state(Cache::default(), descriptor);
    let mut queue = ReqQueue::new();
    let mut server = Server {
        headers: chain_a.clone(),
        txs: Vec::new(),
        merkle_proof: (Vec::new(), 0),
    };

    let notify = |chain: &[block::Header]| {
        raw_msg(json!({
            "jsonrpc": "2.0",
            "method": "blockchain.headers.subscribe",
            "params": [{ "hex": serialize_hex(chain.last().expect("non-empty")), "height": 3 }],
        }))
    };

    // The A-chain tip. Hold its headers request so the job is still waiting.
    state.poll(&mut queue, notify(&chain_a))?;
    let held = queue
        .drain(..)
        .filter(|req| req.method.as_ref() == "blockchain.block.headers")
        .collect::<Vec<_>>();
    assert!(!held.is_empty(), "the job must have asked for headers");
    let held_resp = held
        .iter()
        .map(|req| response(req, &server))
        .collect::<Vec<_>>();

    // The server reorgs to B and notifies the new tip at the same height.
    server.headers = chain_b.clone();
    state.poll(&mut queue, notify(&chain_b))?;
    assert!(
        queue
            .iter()
            .any(|req| req.method.as_ref() == "blockchain.block.headers"),
        "the replacement job must send its own request rather than adopt the one in flight"
    );

    // The held answer describes the chain the server has left.
    let mut updates = Vec::new();
    for resp in held_resp {
        updates.extend(state.poll(&mut queue, resp)?);
    }
    updates.extend(drain_requests(&mut state, &mut queue, &server));

    let tip = updates
        .iter()
        .rev()
        .find_map(|u| u.chain_update.clone())
        .expect("must get a chain update");
    assert_eq!(
        tip.hash(),
        chain_b[3].block_hash(),
        "the local chain must end on the tip the server actually has"
    );
    Ok(())
}

/// A headers batch answered from a chain other than the one announced.
///
/// `blockchain.block.headers` is answered from whichever chain the server holds when it *replies*,
/// so a reorg between receiving the request and answering it returns blocks for a tip we were
/// never told about. Adopting them would put the checkpoint chain on a chain no notification ever
/// announced, and no notification would arrive to correct it.
#[test]
fn headers_for_a_chain_we_were_not_told_about_are_not_adopted() -> anyhow::Result<()> {
    let (descriptor, _spk_hash, _spk) = tracked_descriptor()?;
    let (genesis, h1) = base_headers();
    let h2 = block_with_root(&h1, TxMerkleNode::all_zeros(), 200, 0);
    let a3 = block_with_root(&h2, TxMerkleNode::all_zeros(), 300, 0);
    let b3 = block_with_root(&h2, TxMerkleNode::all_zeros(), 300, 1);
    assert_ne!(a3.block_hash(), b3.block_hash());

    let mut state = new_state(Cache::default(), descriptor);
    let mut queue = ReqQueue::new();
    let mut server = Server {
        headers: vec![genesis, h1, h2],
        txs: Vec::new(),
        merkle_proof: (Vec::new(), 0),
    };
    state.start(&mut queue);
    drain_requests(&mut state, &mut queue, &server);

    // A3 is announced, so that is the block the job is created to reach.
    state.poll(
        &mut queue,
        raw_msg(json!({
            "jsonrpc": "2.0",
            "method": "blockchain.headers.subscribe",
            "params": [{ "hex": serialize_hex(&a3), "height": 3 }],
        })),
    )?;

    // But by the time the server answers, it is on B3 — and it does not announce it, because
    // this response is the reorg's only appearance.
    server.headers = vec![genesis, h1, h2, b3];
    let updates = drain_requests(&mut state, &mut queue, &server);

    let tip = updates.iter().rev().find_map(|u| u.chain_update.clone());
    assert_ne!(
        tip.as_ref().map(|cp| cp.hash()),
        Some(b3.block_hash()),
        "a chain no notification announced must not be adopted"
    );
    assert_ne!(
        tip.as_ref().map(|cp| cp.hash()),
        Some(a3.block_hash()),
        "and the announced block was never actually delivered"
    );
    Ok(())
}

/// A reorg has to re-verify the anchors of *every* script, not just whichever one notified last.
///
/// Anchor scope is the set of script statuses [`ConfirmationJob`] was told to cover. Taking that from
/// the spk jobs which happen to be live makes it collapse: the jobs are cleared each time an
/// update is emitted, so a single notification arriving between updates narrows the scope to
/// that one script, and a reorg landing afterwards leaves every other script anchored to a block
/// which is no longer ours — with no notification coming to say so, since a transaction that
/// keeps its height keeps its script status.
#[test]
fn a_reorg_reanchors_every_script_not_just_the_last_to_notify() -> anyhow::Result<()> {
    let (descriptor, spk_hash_a, spk_a) = tracked_descriptor()?;
    let spk_b = descriptor.at_derivation_index(1)?.script_pubkey();

    let (tx_a, tx_b) = (tx_paying(&spk_a, 50_000), tx_paying(&spk_b, 60_000));
    let (txid_a, txid_b) = (tx_a.compute_txid(), tx_b.compute_txid());
    let (genesis, header_1) = base_headers();
    let header_2 = block_with_tx(&header_1, txid_a, 200, 0);
    let header_3 = block_with_tx(&header_2, txid_b, 300, 0);
    // The reorg keeps both txs at their heights — so neither script status changes — but moves
    // them into different blocks, and extends the chain by one.
    let header_2b = block_with_tx(&header_1, txid_a, 222, 1);
    let header_3b = block_with_tx(&header_2b, txid_b, 333, 1);
    let header_4b = block_with_root(&header_3b, TxMerkleNode::all_zeros(), 400, 0);

    let mut state = new_state(Cache::default(), descriptor);
    let mut queue = ReqQueue::new();
    let mut server = Server {
        headers: vec![genesis, header_1, header_2, header_3],
        txs: vec![(tx_a, 2), (tx_b, 3)],
        merkle_proof: (Vec::new(), 0),
    };

    let anchors_of = |updates: &[Update<&'static str>]| {
        updates
            .iter()
            .flat_map(|u| u.tx_update.anchors.iter().cloned())
            .collect::<Vec<_>>()
    };

    state.start(&mut queue);
    let anchors = anchors_of(&drain_requests(&mut state, &mut queue, &server));
    assert!(
        anchors.contains(&(anchor_of(&header_2, 2), txid_a)),
        "tx_a must first be anchored"
    );
    assert!(
        anchors.contains(&(anchor_of(&header_3, 3), txid_b)),
        "tx_b must first be anchored"
    );

    // Script A is re-notified with the status it already has — something a server does freely,
    // and which says nothing about script B.
    let status_a =
        ElectrumScriptStatus::from_history(&server.history(&json!(spk_hash_a.to_string())))
            .expect("history must be non-empty");
    state.poll(
        &mut queue,
        raw_msg(json!({
            "jsonrpc": "2.0",
            "method": "blockchain.scripthash.subscribe",
            "params": [spk_hash_a.to_string(), status_a.to_string()],
        })),
    )?;
    drain_requests(&mut state, &mut queue, &server);

    // The reorg lands. Only the tip announcement reports it; neither script will be notified.
    server.headers = vec![genesis, header_1, header_2b, header_3b, header_4b];
    state.poll(
        &mut queue,
        raw_msg(json!({
            "jsonrpc": "2.0",
            "method": "blockchain.headers.subscribe",
            "params": [{ "hex": serialize_hex(&header_4b), "height": 4 }],
        })),
    )?;
    let anchors = anchors_of(&drain_requests(&mut state, &mut queue, &server));

    assert!(
        anchors.contains(&(anchor_of(&header_2b, 2), txid_a)),
        "the script that notified must be re-anchored against the new chain"
    );
    assert!(
        anchors.contains(&(anchor_of(&header_3b, 3), txid_b)),
        "and so must every other script, which no notification will ever mention"
    );
    Ok(())
}

/// A history that does not hash to the status the job is waiting for must not be re-asked for.
///
/// The server has moved on since it notified — a reorg, or the transaction dropped out — so the
/// answer will be the same every time. Asking again is an unbounded request loop against the
/// server, and the notification carrying the status it actually holds is already on its way.
///
/// The empty history is the sharpest version: `ElectrumScriptStatus::from_history` yields
/// nothing for it, so there is not even a status to compare against what was stored.
#[test]
fn a_history_that_cannot_match_the_job_is_not_re_asked() -> anyhow::Result<()> {
    let (descriptor, spk_hash, spk) = tracked_descriptor()?;
    let tx = tx_paying(&spk, 50_000);
    let txid = tx.compute_txid();
    let (genesis, header_1) = base_headers();

    let mut state = new_state(Cache::default(), descriptor);
    let mut queue = ReqQueue::new();
    // The server's chain is one block long, so its history for this script is empty — it will
    // never answer with the status the notification below carries.
    let server = Server {
        headers: vec![genesis, header_1],
        txs: vec![(tx, 2)],
        merkle_proof: (Vec::new(), 0),
    };

    state.start(&mut queue);
    drain_requests(&mut state, &mut queue, &server);

    let status =
        ElectrumScriptStatus::from_history(&[response::Tx::Confirmed(response::ConfirmedTx {
            txid,
            height: absolute::Height::from_consensus(2)?,
        })])
        .expect("history is not empty");
    state.poll(
        &mut queue,
        raw_msg(json!({
            "jsonrpc": "2.0",
            "method": "blockchain.scripthash.subscribe",
            "params": [spk_hash.to_string(), status.to_string()],
        })),
    )?;

    let mut served = 0;
    while let Some(req) = queue.pop_front() {
        served += 1;
        assert!(
            served < 50,
            "the client must not re-ask forever: {served} requests, last was {} {:?}",
            req.method,
            req.params
        );
        state.poll(&mut queue, response(&req, &server))?;
    }
    Ok(())
}

/// A history can name a confirmation height above the tip the server has announced: electrs
/// notifies the script hash before the header. That anchor has to *wait* for the tip to catch
/// up. Treating it as history the chain has not backfilled yet sends the job back to plan a run
/// it can never fetch, and the state machine spins.
#[test]
fn an_anchor_above_the_announced_tip_does_not_spin() -> anyhow::Result<()> {
    let (descriptor, spk_hash, spk) = tracked_descriptor()?;
    let tx = tx_paying(&spk, 50_000);
    let txid = tx.compute_txid();
    let (genesis, header_1) = base_headers();
    let header_2 = block_with_tx(&header_1, txid, 200, 0);

    let mut state = new_state(Cache::default(), descriptor);
    let mut queue = ReqQueue::new();
    let mut server = Server {
        headers: vec![genesis, header_1],
        txs: vec![(tx, 2)],
        merkle_proof: (Vec::new(), 0),
    };

    state.start(&mut queue);
    drain_requests(&mut state, &mut queue, &server);

    // The server now has the block, so its history reports the transaction at height 2 — but
    // only the script hash is notified, which is the order electrs really uses.
    server.headers.push(header_2);
    let status = ElectrumScriptStatus::from_history(&server.history(&json!(spk_hash.to_string())))
        .expect("history is not empty");
    state.poll(
        &mut queue,
        raw_msg(json!({
            "jsonrpc": "2.0",
            "method": "blockchain.scripthash.subscribe",
            "params": [spk_hash.to_string(), status.to_string()],
        })),
    )?;
    drain_requests(&mut state, &mut queue, &server);

    assert_eq!(
        state.chain().tip_height(),
        Some(1),
        "no tip was announced, so the chain must not have moved"
    );
    assert!(
        state.cache().tx_cache.anchors.is_empty(),
        "and nothing may be anchored at a height the chain has not reached"
    );

    // The tip catches up, and only now can the anchor resolve.
    state.poll(
        &mut queue,
        raw_msg(json!({
            "jsonrpc": "2.0",
            "method": "blockchain.headers.subscribe",
            "params": [{ "hex": serialize_hex(&header_2), "height": 2 }],
        })),
    )?;
    let updates = drain_requests(&mut state, &mut queue, &server);
    assert!(
        updates.iter().any(|u| u
            .tx_update
            .anchors
            .contains(&(anchor_of(&header_2, 2), txid))),
        "the anchor must be delivered once the tip catches up"
    );
    Ok(())
}

/// The confirmation job must not wait on transactions it never reads.
///
/// It works from the heights a history names, so once every script has its history it already
/// knows every block it needs. Holding it until the scripts finish downloading the transactions
/// in those histories serialises the header and proof fetches behind those downloads for
/// nothing — on a wallet with many scripts that is the whole sync sitting idle.
///
/// The tip's own header arrives with the notification, so the one height here needs no header
/// request; reaching the proof is the proof that the job ran.
///
/// Running ahead is not publishing ahead. The transactions a script is still downloading belong
/// in the same update as their anchors, so the finished job holds it until every script is done
/// — otherwise a caller sees an anchor for a transaction it was never given.
#[test]
fn confirmation_job_runs_ahead_but_the_update_waits_for_the_scripts() -> anyhow::Result<()> {
    let (descriptor, spk_hash, spk) = tracked_descriptor()?;
    let tx = tx_paying(&spk, 50_000);
    let txid = tx.compute_txid();
    let (genesis, header_1) = base_headers();
    let header_2 = block_with_tx(&header_1, txid, 200, 0);

    // Deliberately not seeded with the transaction, so the spk job has to ask for it.
    let mut state = new_state(Cache::default(), descriptor);
    let mut queue = ReqQueue::new();
    let mut server = Server {
        headers: vec![genesis, header_1],
        txs: vec![(tx, 2)],
        merkle_proof: (Vec::new(), 0),
    };

    state.start(&mut queue);
    drain_requests(&mut state, &mut queue, &server);

    let status =
        ElectrumScriptStatus::from_history(&[response::Tx::Confirmed(response::ConfirmedTx {
            txid,
            height: absolute::Height::from_consensus(2)?,
        })])
        .expect("history is not empty");
    state.poll(
        &mut queue,
        raw_msg(json!({
            "jsonrpc": "2.0",
            "method": "blockchain.scripthash.subscribe",
            "params": [spk_hash.to_string(), status.to_string()],
        })),
    )?;
    state.poll(
        &mut queue,
        raw_msg(json!({
            "jsonrpc": "2.0",
            "method": "blockchain.headers.subscribe",
            "params": [{ "hex": serialize_hex(&header_2), "height": 2 }],
        })),
    )?;
    server.headers.push(header_2);

    // Answer the history and nothing else, so the script's job is left in `ProcessingTxs`.
    // Everything queued in response — including whatever the confirmation job asks for — is set
    // aside unanswered.
    let mut deferred = Vec::<RawRequest>::new();
    while let Some(req) = queue.pop_front() {
        if req.method.as_ref() == "blockchain.scripthash.get_history" {
            state.poll(&mut queue, response(&req, &server))?;
        } else {
            deferred.push(req);
        }
    }

    assert!(
        deferred
            .iter()
            .any(|req| req.method.as_ref() == "blockchain.transaction.get"),
        "the script must still be waiting on the transaction its history named",
    );
    assert!(
        deferred
            .iter()
            .any(|req| req.method.as_ref() == "blockchain.transaction.get_merkle"),
        "the confirmation job must reach proof fetching without waiting for that transaction",
    );

    // Let the confirmation job finish: answer everything, still except the transaction.
    let mut updates = Vec::new();
    let mut tx_reqs = Vec::<RawRequest>::new();
    let mut pending = deferred;
    while let Some(req) = pending.pop() {
        if req.method.as_ref() == "blockchain.transaction.get" {
            tx_reqs.push(req);
            continue;
        }
        if let Some(update) = state.poll(&mut queue, response(&req, &server))? {
            updates.push(update);
        }
        pending.extend(queue.drain(..));
    }
    assert!(
        updates.is_empty(),
        "nothing may be published while a script is still downloading its transactions",
    );

    // The transaction finally arrives, and with it the whole update.
    for req in tx_reqs {
        if let Some(update) = state.poll(&mut queue, response(&req, &server))? {
            updates.push(update);
        }
    }
    updates.extend(drain_requests(&mut state, &mut queue, &server));

    let update = match updates.as_slice() {
        [update] => update,
        other => panic!("exactly one update must be published, got {}", other.len()),
    };
    assert!(
        update
            .tx_update
            .txs
            .iter()
            .any(|t| t.compute_txid() == txid),
        "the update must carry the transaction",
    );
    assert!(
        update
            .tx_update
            .anchors
            .contains(&(anchor_of(&header_2, 2), txid)),
        "the update must carry its anchor alongside it",
    );

    // A finished job must hand its update over once, not on every poll that reaches it. The
    // server re-announcing the tip it already announced drives `poll_confirmation_job` without
    // moving the target or the statuses, so nothing may come back out.
    let again = state.poll(
        &mut queue,
        raw_msg(json!({
            "jsonrpc": "2.0",
            "method": "blockchain.headers.subscribe",
            "params": [{ "hex": serialize_hex(&header_2), "height": 2 }],
        })),
    )?;
    assert!(again.is_none(), "an update must not be handed over twice");
    assert!(
        drain_requests(&mut state, &mut queue, &server).is_empty(),
        "a job with nothing left to do must not republish",
    );
    Ok(())
}

/// A transaction is cached under the txid it was asked for, never under the one it claims.
///
/// Nothing downstream can catch a substitution: the prevouts of the wrong transaction resolve
/// into `txouts` as if they were the right one's, and a caller sees inputs that were never
/// spent. `SpkJob::poll` errors only in the narrow case where the substitute is too short to
/// reach a spent vout, which a server picking any longer transaction sails past.
#[test]
fn a_transaction_that_is_not_the_one_asked_for_is_rejected() -> anyhow::Result<()> {
    let (descriptor, _spk_hash, spk) = tracked_descriptor()?;
    let tx = tx_paying(&spk, 50_000);
    // Same shape, different value, so it is a perfectly valid transaction with another txid.
    let impostor = tx_paying(&spk, 60_000);
    assert_ne!(impostor.compute_txid(), tx.compute_txid());
    let (genesis, header_1) = base_headers();

    let mut state = new_state(Cache::default(), descriptor);
    let mut queue = ReqQueue::new();
    let server = Server {
        headers: vec![genesis, header_1],
        txs: vec![(tx, 1)],
        merkle_proof: (Vec::new(), 0),
    };

    // Answer the initial sync honestly, except that every transaction comes back as the
    // impostor. The subscribe response carries the status, so this drives the whole flow.
    state.start(&mut queue);
    let mut substituted = false;
    let mut result = Ok(None);
    while let Some(req) = queue.pop_front() {
        let msg = if req.method.as_ref() == "blockchain.transaction.get" {
            substituted = true;
            raw_msg(json!({
                "jsonrpc": "2.0",
                "id": req.id,
                "result": serialize_hex(&impostor),
            }))
        } else {
            response(&req, &server)
        };
        result = state.poll(&mut queue, msg);
        if result.is_err() {
            break;
        }
    }
    assert!(substituted, "the test must have answered a `GetTx`");
    assert!(
        result.is_err(),
        "a transaction that is not the one asked for must not be accepted",
    );
    Ok(())
}
