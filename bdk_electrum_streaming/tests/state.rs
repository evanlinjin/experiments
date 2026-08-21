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
    BlockId, CheckPoint, ConfirmationBlockTime,
};
use bdk_electrum_streaming::{
    electrum_streaming_client::{
        response, ElectrumScriptHash, ElectrumScriptStatus, RawNotificationOrResponse, RawRequest,
    },
    BlockingState, Cache, DerivedSpkTracker, ReqCoord, ReqQueue, Update,
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
    spk_hash: ElectrumScriptHash,
    /// The transactions in `spk_hash`'s history, and the height each is confirmed at.
    txs: Vec<(Transaction, u32)>,
    /// The merkle branch and position this server answers every merkle request with.
    merkle_proof: (Vec<sha256d::Hash>, usize),
}

impl Server {
    fn tip_height(&self) -> usize {
        self.headers.len() - 1
    }

    /// The history of `spk_hash`: every tx the chain is long enough to contain.
    fn history(&self, spk_hash: &serde_json::Value) -> Vec<response::Tx> {
        if *spk_hash != json!(self.spk_hash.to_string()) {
            return Vec::new();
        }
        self.txs
            .iter()
            .filter(|(_, height)| self.tip_height() >= *height as usize)
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
            .advance(queue, response(&req, server))
            .expect("must advance")
        {
            updates.push(update);
        }
    }
    updates
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
                .advance(queue, response(&req, server))
                .expect("must advance")
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

/// The regtest genesis block and an empty block on top of it.
fn base_headers() -> (block::Header, block::Header) {
    let genesis = constants::genesis_block(Network::Regtest).header;
    let header_1 = block::Header {
        version: block::Version::ONE,
        prev_blockhash: genesis.block_hash(),
        merkle_root: TxMerkleNode::all_zeros(),
        time: 100,
        bits: CompactTarget::from_consensus(0x207fffff),
        nonce: 0,
    };
    (genesis, header_1)
}

/// A block whose transactions have the given merkle root.
fn block_with_root(
    prev: &block::Header,
    merkle_root: TxMerkleNode,
    time: u32,
    nonce: u32,
) -> block::Header {
    block::Header {
        version: block::Version::ONE,
        prev_blockhash: prev.block_hash(),
        merkle_root,
        time,
        bits: CompactTarget::from_consensus(0x207fffff),
        nonce,
    }
}

/// A block whose only transaction is `txid`, so that its merkle root is the txid itself.
fn block_with_tx(prev: &block::Header, txid: Txid, time: u32, nonce: u32) -> block::Header {
    block_with_root(prev, Txid::to_raw_hash(txid).into(), time, nonce)
}

fn new_state(
    cache: Cache,
    descriptor: Descriptor<DescriptorPublicKey>,
    genesis: block::Header,
) -> BlockingState {
    new_state_with_cp(
        cache,
        descriptor,
        CheckPoint::new(BlockId {
            height: 0,
            hash: genesis.block_hash(),
        }),
    )
}

fn new_state_with_cp(
    cache: Cache,
    descriptor: Descriptor<DescriptorPublicKey>,
    cp: CheckPoint,
) -> BlockingState {
    let mut spk_tracker = DerivedSpkTracker::new(0);
    spk_tracker.insert_descriptor("external", descriptor, 0);
    BlockingState::new(ReqCoord::default(), cache, spk_tracker, cp)
}

/// The anchor a tx confirmed in `header` at `height` must be given.
fn anchor_of(header: &block::Header, height: u32) -> ConfirmationBlockTime {
    ConfirmationBlockTime {
        block_id: BlockId {
            height,
            hash: header.block_hash(),
        },
        confirmation_time: header.time as u64,
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
    cache.txs.insert(txid, Arc::new(tx.clone()));

    let mut state = new_state(cache, descriptor, genesis);
    let mut queue = ReqQueue::new();
    let mut server = Server {
        headers: vec![genesis, header_1],
        spk_hash,
        txs: vec![(tx, 2)],
        merkle_proof: (Vec::new(), 0),
    };

    state.init(&mut queue);
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
    state.advance(
        &mut queue,
        raw_msg(json!({
            "jsonrpc": "2.0",
            "method": "blockchain.scripthash.subscribe",
            "params": [spk_hash.to_string(), status.to_string()],
        })),
    )?;
    state.advance(
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
/// Only [`State::init`] subscribes to the tracker's existing spks, so a subscription missed here
/// is missed until the next reconnection.
#[test]
fn descriptor_inserted_mid_connection_is_subscribed() -> anyhow::Result<()> {
    let descriptor = Descriptor::<DescriptorPublicKey>::from_str(&format!("wpkh({XPUB}/0/*)"))?;
    let spk_hash = ElectrumScriptHash::new(&descriptor.at_derivation_index(0)?.script_pubkey());

    let genesis = constants::genesis_block(Network::Regtest).header;
    let mut state = BlockingState::new(
        ReqCoord::default(),
        Cache::default(),
        DerivedSpkTracker::new(0),
        CheckPoint::new(BlockId {
            height: 0,
            hash: genesis.block_hash(),
        }),
    );

    let mut queue = ReqQueue::new();
    state.init(&mut queue);
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

/// A reorg can move a transaction into a different block at the *same* height. An Electrum
/// script status is a hash over txid-height pairs, so it does not change and the server has no
/// reason to send a script hash notification. The anchor we already delivered now points at a
/// block that is no longer in the chain, so it must be refetched off the tip update alone.
#[test]
fn anchor_is_refetched_when_tx_moves_to_another_block_of_same_height() -> anyhow::Result<()> {
    let (descriptor, spk_hash, spk) = tracked_descriptor()?;
    let tx = tx_paying(&spk, 50_000);
    let txid = tx.compute_txid();
    let (genesis, header_1) = base_headers();
    let header_2 = block_with_tx(&header_1, txid, 200, 0);
    // The block that replaces height 2 contains the tx too, hence the identical script status.
    let header_2b = block_with_tx(&header_1, txid, 222, 1);
    let header_3b = block::Header {
        prev_blockhash: header_2b.block_hash(),
        time: 300,
        ..header_1
    };
    assert_ne!(header_2.block_hash(), header_2b.block_hash());

    let mut state = new_state(Cache::default(), descriptor, genesis);
    let mut queue = ReqQueue::new();
    let mut server = Server {
        headers: vec![genesis, header_1, header_2],
        spk_hash,
        txs: vec![(tx, 2)],
        merkle_proof: (Vec::new(), 0),
    };

    state.init(&mut queue);
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
    state.advance(
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
    let (descriptor, spk_hash, spk) = tracked_descriptor()?;
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

    let mut state = new_state(Cache::default(), descriptor, genesis);
    let mut queue = ReqQueue::new();
    let mut server = Server {
        headers: vec![genesis, header_1, header_2],
        spk_hash,
        txs: vec![(tx, 2)],
        merkle_proof: (Vec::new(), 0),
    };

    // Sync, but hold back the merkle proof so the anchor fetch is still in flight.
    state.init(&mut queue);
    let mut in_flight = Vec::new();
    while let Some(req) = queue.pop_front() {
        if req.method.as_ref() == "blockchain.transaction.get_merkle" {
            in_flight.push(req);
            continue;
        }
        state.advance(&mut queue, response(&req, &server))?;
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
    state.advance(
        &mut queue,
        raw_msg(json!({
            "jsonrpc": "2.0",
            "method": "blockchain.headers.subscribe",
            "params": [{ "hex": serialize_hex(&header_3b), "height": 3 }],
        })),
    )?;
    drain_requests(&mut state, &mut queue, &server);

    // The held answer proves inclusion in the block which was evicted, not in the one which
    // replaced it.
    state.advance(&mut queue, stale_resp)?;
    let updates = drain_requests(&mut state, &mut queue, &server);

    assert!(
        updates.iter().any(|u| u
            .tx_update
            .anchors
            .contains(&(anchor_of(&header_2b, 2), txid))),
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
    let (descriptor, spk_hash, spk) = tracked_descriptor()?;
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

    let mut state = new_state(Cache::default(), descriptor, genesis);
    let mut queue = ReqQueue::new();
    let mut server = Server {
        headers: vec![genesis, header_1, header_2, header_3],
        spk_hash,
        txs: vec![(tx_a, 2), (tx_b, 3)],
        merkle_proof: (Vec::new(), 0),
    };

    // Sync, but hold back tx_b's proof so that the job has staged tx_a's anchor and is still
    // waiting on tx_b's when the reorg lands.
    state.init(&mut queue);
    let mut in_flight = Vec::new();
    while let Some(req) = queue.pop_front() {
        if req.method.as_ref() == "blockchain.transaction.get_merkle"
            && req.params[0] == json!(txid_b.to_string())
        {
            in_flight.push(req);
            continue;
        }
        state.advance(&mut queue, response(&req, &server))?;
    }
    let held_req = match in_flight.as_slice() {
        [req] => req.clone(),
        reqs => panic!("expected one held merkle request, got {}", reqs.len()),
    };
    let held_resp = response(&held_req, &server);

    server.headers = vec![genesis, header_1, header_2b, header_3b, header_4b];
    state.advance(
        &mut queue,
        raw_msg(json!({
            "jsonrpc": "2.0",
            "method": "blockchain.headers.subscribe",
            "params": [{ "hex": serialize_hex(&header_4b), "height": 4 }],
        })),
    )?;
    let mut updates = drain_requests(&mut state, &mut queue, &server);
    updates.extend(state.advance(&mut queue, held_resp)?);
    updates.extend(drain_requests(&mut state, &mut queue, &server));

    let anchors = updates
        .iter()
        .flat_map(|u| u.tx_update.anchors.iter().copied())
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
    let (descriptor, spk_hash, spk) = tracked_descriptor()?;
    let tx = tx_paying(&spk, 50_000);
    let txid = tx.compute_txid();
    let (genesis, header_1) = base_headers();
    let header_2 = block_with_tx(&header_1, txid, 200, 0);
    // Height 2 is replaced by a block without the tx, and the chain grows by one.
    let header_2b = block_with_root(&header_1, TxMerkleNode::all_zeros(), 222, 1);
    let header_3b = block_with_root(&header_2b, TxMerkleNode::all_zeros(), 333, 0);

    let mut state = new_state(Cache::default(), descriptor, genesis);
    let mut queue = ReqQueue::new();
    let mut server = Server {
        headers: vec![genesis, header_1, header_2],
        spk_hash,
        txs: vec![(tx, 2)],
        merkle_proof: (Vec::new(), 0),
    };

    state.init(&mut queue);
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
    state.advance(
        &mut queue,
        raw_msg(json!({
            "jsonrpc": "2.0",
            "method": "blockchain.headers.subscribe",
            "params": [{ "hex": serialize_hex(&header_3b), "height": 3 }],
        })),
    )?;

    while let Some(req) = queue.pop_front() {
        state
            .advance(&mut queue, response(&req, &server))
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
    let (descriptor, spk_hash, spk) = tracked_descriptor()?;
    let tx = tx_paying(&spk, 50_000);
    let txid = tx.compute_txid();
    let (genesis, header_1) = base_headers();
    let header_2 = block_with_tx(&header_1, txid, 200, 0);
    // Height 2 is replaced by a block which contains the tx too, so the anchor is still valid.
    let header_2b = block_with_tx(&header_1, txid, 222, 1);
    let header_3b = block_with_root(&header_2b, TxMerkleNode::all_zeros(), 300, 0);

    let mut state = new_state(Cache::default(), descriptor, genesis);
    let mut queue = ReqQueue::new();
    let mut server = Server {
        headers: vec![genesis, header_1, header_2],
        spk_hash,
        txs: vec![(tx, 2)],
        merkle_proof: (Vec::new(), 0),
    };

    // Sync, but hold back the merkle request so the anchor fetch is still in flight.
    state.init(&mut queue);
    let mut in_flight = Vec::new();
    while let Some(req) = queue.pop_front() {
        if req.method.as_ref() == "blockchain.transaction.get_merkle" {
            in_flight.push(req);
            continue;
        }
        state.advance(&mut queue, response(&req, &server))?;
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
    state.advance(
        &mut queue,
        raw_msg(json!({
            "jsonrpc": "2.0",
            "method": "blockchain.headers.subscribe",
            "params": [{ "hex": serialize_hex(&header_3b), "height": 3 }],
        })),
    )?;
    drain_requests(&mut state, &mut queue, &server);

    // The held request was received while the tx was out of its block, so it is answered with
    // an error — the wording is the one romanz/electrs really sends, a bare JSON string which
    // conflates a genuine fault with the everyday reorg.
    state.advance(
        &mut queue,
        raw_msg(json!({
            "jsonrpc": "2.0",
            "id": stale_req.id,
            "error": "tx not found or is unconfirmed",
        })),
    )?;
    let updates = drain_requests(&mut state, &mut queue, &server);

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
    let (descriptor, spk_hash, spk) = tracked_descriptor()?;
    let tx = tx_paying(&spk, 50_000);
    let txid = tx.compute_txid();
    let (genesis, header_1) = base_headers();
    let header_2 = block_with_tx(&header_1, txid, 200, 0);
    // Height 2 is replaced by a block containing the tx, and the chain grows by one.
    let header_2b = block_with_tx(&header_1, txid, 222, 1);
    let header_3b = block_with_root(&header_2b, TxMerkleNode::all_zeros(), 300, 0);

    let mut state = new_state(Cache::default(), descriptor, genesis);
    let mut queue = ReqQueue::new();
    let mut server = Server {
        headers: vec![genesis, header_1, header_2],
        spk_hash,
        txs: vec![(tx.clone(), 2)],
        merkle_proof: (Vec::new(), 0),
    };

    // Sync, but fail every merkle request the way a busy server would.
    state.init(&mut queue);
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
        state.advance(&mut queue, resp)?;
    }
    assert_eq!(
        merkle_requests, 1,
        "the job must give up on the pair rather than re-ask"
    );
    assert!(
        state.cache().anchors.is_empty(),
        "an error proves nothing, so no anchor may be recorded from it"
    );

    // The reorg replaces the block, so the anchor is asked for again — which it could not be
    // had the error dropped this script from `spk_hashes_by_height`.
    server.headers = vec![genesis, header_1, header_2b, header_3b];
    state.advance(
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

/// Issue #12's literal case: a reorg to a block of the *same* height, with no growth at all.
/// `ChainJob` applies this by short-circuit straight from the header notification, so it is the
/// one reorg shape which never fetches anything — and every other reorg test here also grows the
/// chain, which takes a different path.
#[test]
fn anchor_is_refetched_after_a_same_height_reorg() -> anyhow::Result<()> {
    let (descriptor, spk_hash, spk) = tracked_descriptor()?;
    let tx = tx_paying(&spk, 50_000);
    let txid = tx.compute_txid();
    let (genesis, header_1) = base_headers();
    let header_2 = block_with_tx(&header_1, txid, 200, 0);
    let header_2b = block_with_tx(&header_1, txid, 222, 1);
    assert_ne!(header_2.block_hash(), header_2b.block_hash());

    let mut state = new_state(Cache::default(), descriptor, genesis);
    let mut queue = ReqQueue::new();
    let mut server = Server {
        headers: vec![genesis, header_1, header_2],
        spk_hash,
        txs: vec![(tx, 2)],
        merkle_proof: (Vec::new(), 0),
    };

    state.init(&mut queue);
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
    state.advance(
        &mut queue,
        raw_msg(json!({
            "jsonrpc": "2.0",
            "method": "blockchain.headers.subscribe",
            "params": [{ "hex": serialize_hex(&header_2b), "height": 2 }],
        })),
    )?;
    let updates = drain_requests(&mut state, &mut queue, &server);

    assert!(
        updates.iter().any(|u| u
            .tx_update
            .anchors
            .contains(&(anchor_of(&header_2b, 2), txid))),
        "anchor must be refetched for the block that replaced the evicted one"
    );
    Ok(())
}

/// A proof is verified against the merkle root of the block we have at that height, so a job
/// which asks for the proof and that block's header together only works if the server answers in
/// request order. It need not: the protocol carries request ids for that reason.
#[test]
fn anchor_is_refetched_whatever_order_the_server_answers_in() -> anyhow::Result<()> {
    let (descriptor, spk_hash, spk) = tracked_descriptor()?;
    let tx = tx_paying(&spk, 50_000);
    let txid = tx.compute_txid();
    let (genesis, header_1) = base_headers();
    let header_2 = block_with_tx(&header_1, txid, 200, 0);
    // Height 2 is replaced and the chain grows, so the notified header is height 3's — the
    // replacement block's header has to be fetched before its proof can be verified.
    let header_2b = block_with_tx(&header_1, txid, 222, 1);
    let header_3b = block_with_root(&header_2b, TxMerkleNode::all_zeros(), 300, 0);

    let mut state = new_state(Cache::default(), descriptor, genesis);
    let mut queue = ReqQueue::new();
    let mut server = Server {
        headers: vec![genesis, header_1, header_2],
        spk_hash,
        txs: vec![(tx, 2)],
        merkle_proof: (Vec::new(), 0),
    };

    state.init(&mut queue);
    let updates = drain_requests(&mut state, &mut queue, &server);
    assert!(
        updates.iter().any(|u| u
            .tx_update
            .anchors
            .contains(&(anchor_of(&header_2, 2), txid))),
        "tx must first be anchored to the original block"
    );

    server.headers = vec![genesis, header_1, header_2b, header_3b];
    state.advance(
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

/// A client restored from a persisted checkpoint chain starts with blocks in its chain whose
/// headers are not in its cache. Resolving an anchor at such a height needs both the header and
/// the proof, and nothing else will fetch that header — `ChainJob` short-circuits, since the tip
/// is already correct.
///
/// So this is the case where the two halves of the ordering fix are load-bearing: the proof must
/// not be asked for before the header is cached, and the header response must advance the waiting
/// job even though the chain itself has nothing to learn from it.
#[test]
fn anchor_resolves_when_the_chain_is_restored_without_its_headers() -> anyhow::Result<()> {
    let (descriptor, spk_hash, spk) = tracked_descriptor()?;
    let tx = tx_paying(&spk, 50_000);
    let txid = tx.compute_txid();
    let (genesis, header_1) = base_headers();
    let header_2 = block_with_tx(&header_1, txid, 200, 0);
    let header_3 = block_with_root(&header_2, TxMerkleNode::all_zeros(), 300, 0);

    // The restored chain knows the blocks, the fresh cache knows none of their headers. The tx
    // is one block below the tip, so the header it needs is not the one `headers.subscribe`
    // hands back and nothing else fetches it either — `ChainJob` short-circuits, the tip being
    // already correct.
    let cp = CheckPoint::new(BlockId {
        height: 0,
        hash: genesis.block_hash(),
    })
    .insert(BlockId {
        height: 2,
        hash: header_2.block_hash(),
    })
    .insert(BlockId {
        height: 3,
        hash: header_3.block_hash(),
    });
    let mut state = new_state_with_cp(Cache::default(), descriptor, cp);
    let mut queue = ReqQueue::new();
    let server = Server {
        headers: vec![genesis, header_1, header_2, header_3],
        spk_hash,
        txs: vec![(tx, 2)],
        merkle_proof: (Vec::new(), 0),
    };

    state.init(&mut queue);
    let updates = drain_requests_proofs_first(&mut state, &mut queue, &server);

    assert!(
        updates.iter().any(|u| u
            .tx_update
            .anchors
            .contains(&(anchor_of(&header_2, 2), txid))),
        "the anchor must resolve even when the proof overtakes the header it is verified against"
    );
    Ok(())
}

/// A header fetched before a reorg describes the chain we have since left behind, and inserting
/// it would splice a purged block into the checkpoint chain.
///
/// `extends`/`replaces` do not catch this on their own: they only decline a height the chain
/// already has. The gap they leave open is a *sparse* chain — a restored one, or one whose
/// missing heights sit below the 21-block suffix `ChainJob` rewrites — reorged deeper than that
/// suffix, so the refetch never learns the low block changed too.
#[test]
fn header_fetched_before_a_reorg_is_not_spliced_into_the_chain() -> anyhow::Result<()> {
    let (descriptor, spk_hash, spk) = tracked_descriptor()?;
    let tx = tx_paying(&spk, 50_000);
    let txid = tx.compute_txid();
    let (genesis, header_1) = base_headers();

    // Two chains which differ at height 2 as well as near the tip. Both contain the tx at
    // height 2, so the anchor stays valid throughout — only the block it belongs to changes.
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
    let chain_a = build(block_with_tx(&header_1, txid, 200, 0), 30, 0);
    let chain_b = build(block_with_tx(&header_1, txid, 222, 1), 31, 1);
    let (a2, b2) = (chain_a[2], chain_b[2]);
    assert_ne!(a2.block_hash(), b2.block_hash());

    // A restored chain sparse enough that height 2 is a gap — so the anchor has to fetch that
    // header, and `replaces` will not decline it when it comes back.
    let cp = CheckPoint::new(BlockId {
        height: 0,
        hash: genesis.block_hash(),
    })
    .insert(BlockId {
        height: 30,
        hash: chain_a[30].block_hash(),
    });
    let mut state = new_state_with_cp(Cache::default(), descriptor, cp);
    let mut queue = ReqQueue::new();
    let mut server = Server {
        headers: chain_a.clone(),
        spk_hash,
        txs: vec![(tx, 2)],
        merkle_proof: (Vec::new(), 0),
    };

    // Sync, but hold back the height-2 header so the fetch is still in flight.
    state.init(&mut queue);
    let mut in_flight = Vec::new();
    while let Some(req) = queue.pop_front() {
        if req.method.as_ref() == "blockchain.block.header" && req.params[0] == json!(2) {
            in_flight.push(req);
            continue;
        }
        state.advance(&mut queue, response(&req, &server))?;
    }
    let stale_req = match in_flight.as_slice() {
        [req] => req.clone(),
        reqs => panic!("expected one held header request, got {}", reqs.len()),
    };
    let stale_resp = response(&stale_req, &server);

    // The reorg lands. It runs deeper than `ChainJob`'s suffix, so the refetch rewrites the
    // top 21 blocks and never learns that height 2 changed too.
    server.headers = chain_b.clone();
    state.advance(
        &mut queue,
        raw_msg(json!({
            "jsonrpc": "2.0",
            "method": "blockchain.headers.subscribe",
            "params": [{ "hex": serialize_hex(&chain_b[31]), "height": 31 }],
        })),
    )?;
    drain_requests(&mut state, &mut queue, &server);

    // The held answer describes the chain we have left behind.
    let mut updates = Vec::from_iter(state.advance(&mut queue, stale_resp)?);
    updates.extend(drain_requests(&mut state, &mut queue, &server));

    let tip = updates
        .iter()
        .rev()
        .find_map(|u| u.chain_update.clone())
        .expect("must get a chain update");
    let at_2 = tip.iter().find(|cp| cp.height() == 2);
    assert_ne!(
        at_2.as_ref().map(|cp| cp.hash()),
        Some(a2.block_hash()),
        "a header from the chain we left must not be spliced into the checkpoint chain"
    );
    assert_eq!(
        at_2.map(|cp| cp.hash()),
        Some(b2.block_hash()),
        "the height must be refetched against the chain we are actually on"
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

    let mut state = new_state(Cache::default(), descriptor, genesis);
    let mut queue = ReqQueue::new();
    let mut server = Server {
        headers: vec![genesis, header_1, header_2],
        spk_hash,
        // The server only reports tx_a to begin with.
        txs: vec![(tx_a, 2)],
        merkle_proof: (Vec::new(), 0),
    };

    state.init(&mut queue);
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
    state.advance(
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
        state.advance(&mut queue, response(&req, &server))?;
    }
    let held_req = match in_flight.as_slice() {
        [req] => req.clone(),
        reqs => panic!("expected one held history request, got {}", reqs.len()),
    };

    // The reorg evicts height 2, where this script is recorded — so the refetch wants to replay
    // its job, and must decline because the server's own job is already there.
    server.headers = vec![genesis, header_1, header_2b, header_3b];
    state.advance(
        &mut queue,
        raw_msg(json!({
            "jsonrpc": "2.0",
            "method": "blockchain.headers.subscribe",
            "params": [{ "hex": serialize_hex(&header_3b), "height": 3 }],
        })),
    )?;
    let mut updates = drain_requests(&mut state, &mut queue, &server);
    updates.extend(state.advance(&mut queue, response(&held_req, &server))?);
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

/// A persisted checkpoint chain can be stale at a height below the window [`ChainJob`] rewrites
/// — an offline reorg, say. Then the block *we* have at that height is one the server does not
/// have, and `blockchain.block.header` is keyed by height, so no request can ever fetch it.
///
/// Withholding the proof until that header is cached must not turn into an endless request loop.
#[test]
fn a_header_the_server_does_not_have_does_not_loop() -> anyhow::Result<()> {
    let (descriptor, spk_hash, spk) = tracked_descriptor()?;
    let tx = tx_paying(&spk, 50_000);
    let txid = tx.compute_txid();
    let (genesis, header_1) = base_headers();
    // The server's height 2, and the stale one our persisted chain still claims.
    let server_2 = block_with_tx(&header_1, txid, 200, 0);
    let stale_2 = block_with_tx(&header_1, txid, 999, 7);
    assert_ne!(server_2.block_hash(), stale_2.block_hash());

    let mut chain = vec![genesis, header_1, server_2];
    for height in 3..=30u32 {
        let prev = *chain.last().expect("non-empty");
        chain.push(block_with_root(
            &prev,
            TxMerkleNode::all_zeros(),
            1000 + height,
            0,
        ));
    }

    // Tip agrees with the server, so `ChainJob` short-circuits and never rewrites height 2.
    let cp = CheckPoint::new(BlockId {
        height: 0,
        hash: genesis.block_hash(),
    })
    .insert(BlockId {
        height: 2,
        hash: stale_2.block_hash(),
    })
    .insert(BlockId {
        height: 30,
        hash: chain[30].block_hash(),
    });
    let mut state = new_state_with_cp(Cache::default(), descriptor, cp);
    let mut queue = ReqQueue::new();
    let server = Server {
        headers: chain,
        spk_hash,
        txs: vec![(tx, 2)],
        merkle_proof: (Vec::new(), 0),
    };

    state.init(&mut queue);
    let mut served = 0;
    while let Some(req) = queue.pop_front() {
        served += 1;
        assert!(
            served < 200,
            "the client must not loop: {served} requests, last was {} {:?}",
            req.method,
            req.params
        );
        state.advance(&mut queue, response(&req, &server))?;
    }
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

    let mut state = new_state(Cache::default(), descriptor, genesis);
    let mut queue = ReqQueue::new();
    let mut server = Server {
        headers: vec![genesis, header_1, header_2],
        spk_hash,
        txs: vec![(tx, 2)],
        merkle_proof: (Vec::new(), 0),
    };

    state.init(&mut queue);
    let updates = drain_requests(&mut state, &mut queue, &server);
    assert!(
        updates.iter().any(|u| u
            .tx_update
            .anchors
            .contains(&(anchor_of(&header_2, 2), txid))),
        "tx must first be anchored"
    );
    assert!(
        state.spk_histories().status(spk_hash).is_some(),
        "the status must be recorded while the script has a history"
    );

    // The transaction is gone, so the script's status goes to null.
    server.txs = Vec::new();
    state.advance(
        &mut queue,
        raw_msg(json!({
            "jsonrpc": "2.0",
            "method": "blockchain.scripthash.subscribe",
            "params": [spk_hash.to_string(), null],
        })),
    )?;
    drain_requests(&mut state, &mut queue, &server);
    assert!(
        !state.spk_histories().status(spk_hash).is_some(),
        "a null status must drop the recorded status"
    );

    // A reorg evicting the height it used to be seen at must not replay anything.
    server.headers = vec![genesis, header_1, header_2b, header_3b];
    state.advance(
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
        state.advance(&mut queue, response(&req, &server))?;
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

    let mut chain = vec![genesis, header_1, theirs];
    for height in 3..=30u32 {
        let prev = *chain.last().expect("non-empty");
        chain.push(block_with_root(
            &prev,
            TxMerkleNode::all_zeros(),
            1000 + height,
            0,
        ));
    }

    // A persisted chain holding our block at height 2, and its header already cached — otherwise
    // `GetHeader` catches the disagreement before any proof is asked for. The tip agrees, so no
    // chain job runs and nothing rewrites height 2: the disagreement is below the window.
    let mut cache = Cache::default();
    cache.headers.insert(ours.block_hash(), ours);
    let cp = CheckPoint::new(BlockId {
        height: 0,
        hash: genesis.block_hash(),
    })
    .insert(BlockId {
        height: 2,
        hash: ours.block_hash(),
    })
    .insert(BlockId {
        height: 30,
        hash: chain[30].block_hash(),
    });
    let mut state = new_state_with_cp(cache, descriptor, cp);
    let mut queue = ReqQueue::new();
    let mut server = Server {
        headers: chain,
        spk_hash,
        txs: vec![(tx, 2)],
        merkle_proof: (proof_theirs.merkle.clone(), proof_theirs.pos),
    };

    state.init(&mut queue);
    let mut served = 0;
    while let Some(req) = queue.pop_front() {
        served += 1;
        assert!(served < 200, "a mismatch must not become a request loop");
        state.advance(&mut queue, response(&req, &server))?;
    }
    assert!(
        state.cache().anchors.is_empty(),
        "a proof for a block we do not have must not anchor anything"
    );

    // The server comes back to our block at that height. Nothing durable was written against it,
    // so the job a notification rebuilds must be able to anchor there.
    server.headers[2] = ours;
    server.merkle_proof = (Vec::new(), 0);
    let status = ElectrumScriptStatus::from_history(&server.history(&json!(spk_hash.to_string())))
        .expect("history must be non-empty");
    state.advance(
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
    let (descriptor, spk_hash, spk) = tracked_descriptor()?;
    let tx = tx_paying(&spk, 50_000);
    let txid = tx.compute_txid();
    let (genesis, header_1) = base_headers();
    let header_2 = block_with_tx(&header_1, txid, 200, 0);

    let mut state = new_state(Cache::default(), descriptor, genesis);
    let mut queue = ReqQueue::new();
    let server = Server {
        headers: vec![genesis, header_1, header_2],
        spk_hash,
        txs: vec![(tx, 2)],
        merkle_proof: (Vec::new(), 0),
    };

    state.init(&mut queue);
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
    let (descriptor, spk_hash, _spk) = tracked_descriptor()?;
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

    let mut state = new_state(Cache::default(), descriptor, genesis);
    let mut queue = ReqQueue::new();
    let mut server = Server {
        headers: chain_a.clone(),
        spk_hash,
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
    state.advance(&mut queue, notify(&chain_a))?;
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
    state.advance(&mut queue, notify(&chain_b))?;
    assert!(
        queue
            .iter()
            .any(|req| req.method.as_ref() == "blockchain.block.headers"),
        "the replacement job must send its own request rather than adopt the one in flight"
    );

    // The held answer describes the chain the server has left.
    let mut updates = Vec::new();
    for resp in held_resp {
        updates.extend(state.advance(&mut queue, resp)?);
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
    let (descriptor, spk_hash, _spk) = tracked_descriptor()?;
    let (genesis, h1) = base_headers();
    let h2 = block_with_root(&h1, TxMerkleNode::all_zeros(), 200, 0);
    let a3 = block_with_root(&h2, TxMerkleNode::all_zeros(), 300, 0);
    let b3 = block_with_root(&h2, TxMerkleNode::all_zeros(), 300, 1);
    assert_ne!(a3.block_hash(), b3.block_hash());

    let mut state = new_state(Cache::default(), descriptor, genesis);
    let mut queue = ReqQueue::new();
    let mut server = Server {
        headers: vec![genesis, h1, h2],
        spk_hash,
        txs: Vec::new(),
        merkle_proof: (Vec::new(), 0),
    };
    state.init(&mut queue);
    drain_requests(&mut state, &mut queue, &server);

    // A3 is announced, so that is the block the job is created to reach.
    state.advance(
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
