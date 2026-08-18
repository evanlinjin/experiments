use std::{str::FromStr, sync::Arc};

use bdk_core::{
    bitcoin::{
        absolute, block, consensus::encode::serialize_hex, constants, hashes::Hash, transaction,
        Amount, CompactTarget, Network, OutPoint, ScriptBuf, Sequence, Transaction, TxIn,
        TxMerkleNode, TxOut, Txid, Witness,
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

struct Server {
    headers: Vec<block::Header>,
    spk_hash: ElectrumScriptHash,
    tx: Transaction,
}

impl Server {
    fn tip_height(&self) -> usize {
        self.headers.len() - 1
    }

    fn answer(&self, req: &RawRequest) -> serde_json::Value {
        match req.method.as_ref() {
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
            "blockchain.scripthash.subscribe" => json!(null),
            "blockchain.scripthash.get_history" => {
                if req.params[0] == json!(self.spk_hash.to_string()) && self.tip_height() >= 2 {
                    json!([{ "tx_hash": self.tx.compute_txid().to_string(), "height": 2 }])
                } else {
                    json!([])
                }
            }
            "blockchain.transaction.get" => json!(serialize_hex(&self.tx)),
            "blockchain.transaction.get_merkle" => {
                json!({ "block_height": req.params[1], "merkle": [], "pos": 0 })
            }
            other => panic!("unexpected request: {other}"),
        }
    }
}

fn drain_requests(
    state: &mut BlockingState,
    queue: &mut ReqQueue,
    server: &Server,
) -> Vec<Update<&'static str>> {
    let mut updates = Vec::new();
    while let Some(req) = queue.pop_front() {
        let resp =
            raw_msg(json!({ "jsonrpc": "2.0", "id": req.id, "result": server.answer(&req) }));
        if let Some(update) = state.advance(queue, resp).expect("must advance") {
            updates.push(update);
        }
    }
    updates
}

/// A history response can report a confirmation height above the local tip: on a new block,
/// romanz/electrs notifies the script hash before the header and answers requests in order, so
/// the history arrives while the local tip is still one block behind. Such an anchor must be
/// deferred until the tip catches up — not dropped — and must still be delivered without any
/// further notification for that script.
#[test]
fn anchor_above_local_tip_is_deferred_until_tip_catches_up() -> anyhow::Result<()> {
    let descriptor = Descriptor::<DescriptorPublicKey>::from_str(&format!("wpkh({XPUB}/0/*)"))?;
    let spk = descriptor.at_derivation_index(0)?.script_pubkey();
    let spk_hash = ElectrumScriptHash::new(&spk);

    let tx = Transaction {
        version: transaction::Version::ONE,
        lock_time: absolute::LockTime::ZERO,
        input: vec![TxIn {
            previous_output: OutPoint::null(),
            script_sig: ScriptBuf::new(),
            sequence: Sequence::MAX,
            witness: Witness::new(),
        }],
        output: vec![TxOut {
            value: Amount::from_sat(50_000),
            script_pubkey: spk,
        }],
    };
    let txid = tx.compute_txid();

    let genesis = constants::genesis_block(Network::Regtest).header;
    let header_1 = block::Header {
        version: block::Version::ONE,
        prev_blockhash: genesis.block_hash(),
        merkle_root: TxMerkleNode::all_zeros(),
        time: 100,
        bits: CompactTarget::from_consensus(0x207fffff),
        nonce: 0,
    };
    // With the tx at position 0 of a single-tx block, the merkle root is its txid.
    let header_2 = block::Header {
        merkle_root: Txid::to_raw_hash(txid).into(),
        prev_blockhash: header_1.block_hash(),
        time: 200,
        ..header_1
    };

    let mut cache = Cache::default();
    cache.txs.insert(txid, Arc::new(tx.clone()));

    let mut spk_tracker = DerivedSpkTracker::new(0);
    spk_tracker.insert_descriptor("external", descriptor, 0);

    let mut state = BlockingState::new(
        ReqCoord::default(),
        cache,
        spk_tracker,
        CheckPoint::new(BlockId {
            height: 0,
            hash: genesis.block_hash(),
        }),
    );
    let mut queue = ReqQueue::new();
    let mut server = Server {
        headers: vec![genesis, header_1],
        spk_hash,
        tx,
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

    let expected_anchor = ConfirmationBlockTime {
        block_id: BlockId {
            height: 2,
            hash: header_2.block_hash(),
        },
        confirmation_time: header_2.time as u64,
    };
    assert!(
        updates
            .iter()
            .any(|u| u.tx_update.anchors.contains(&(expected_anchor, txid))),
        "anchor must be delivered once the tip catches up"
    );
    Ok(())
}
