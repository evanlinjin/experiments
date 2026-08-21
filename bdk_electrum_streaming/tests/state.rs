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
    let mut spk_tracker = DerivedSpkTracker::new(0);
    spk_tracker.insert_descriptor("external", descriptor, 0);
    BlockingState::new(
        ReqCoord::default(),
        cache,
        spk_tracker,
        CheckPoint::new(BlockId {
            height: 0,
            hash: genesis.block_hash(),
        }),
    )
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
