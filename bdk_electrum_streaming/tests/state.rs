use std::{str::FromStr, sync::Arc};

use bdk_core::{
    bitcoin::{
        absolute, block, consensus::encode::serialize_hex, constants, hashes::Hash, merkle_tree,
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

/// The merkle root of a block whose transactions are exactly `txids`, in order.
fn merkle_root_of(txids: &[Txid]) -> TxMerkleNode {
    merkle_tree::calculate_root(txids.iter().map(|&txid| Txid::to_raw_hash(txid).into()))
        .expect("a block has at least one transaction")
}

struct Server {
    headers: Vec<block::Header>,
    /// The scripts whose histories this server serves, each with the txs it reports for that
    /// script and the heights they are confirmed at.
    wallet: Vec<(ElectrumScriptHash, Vec<(Transaction, usize)>)>,
    /// A height the server refuses to serve a header for even though its chain has one, the way
    /// an index that has not caught up to its own tip does.
    header_blackout: Option<usize>,
}

impl Server {
    fn new(headers: Vec<block::Header>) -> Self {
        Self {
            headers,
            wallet: Vec::new(),
            header_blackout: None,
        }
    }

    fn tip_height(&self) -> usize {
        self.headers.len() - 1
    }

    /// Every tx the server reports as confirmed at `height`, in txid order so that a block's
    /// contents do not depend on which script was asked about.
    fn block_txids(&self, height: usize) -> Vec<Txid> {
        let mut txids = self
            .wallet
            .iter()
            .flat_map(|(_, txs)| txs.iter())
            .filter(|(_, h)| *h == height)
            .map(|(tx, _)| tx.compute_txid())
            .collect::<Vec<_>>();
        txids.sort();
        txids.dedup();
        txids
    }

    /// The server's reply, or the error string it answers with instead.
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
                    Some(_) if self.header_blackout == Some(height) => {
                        return Err("missing header".to_string())
                    }
                    Some(header) => json!(serialize_hex(header)),
                    None => return Err("missing header".to_string()),
                }
            }
            "blockchain.scripthash.subscribe" => json!(null),
            "blockchain.scripthash.get_history" => {
                match self
                    .wallet
                    .iter()
                    .find(|(spk_hash, _)| req.params[0] == json!(spk_hash.to_string()))
                {
                    Some((_, txs)) => json!(txs
                        .iter()
                        .filter(|(_, height)| *height <= self.tip_height())
                        .map(|(tx, height)| json!({
                            "tx_hash": tx.compute_txid().to_string(),
                            "height": height,
                        }))
                        .collect::<Vec<_>>()),
                    None => json!([]),
                }
            }
            "blockchain.transaction.get" => {
                let wanted = req.params[0].as_str().expect("must have txid");
                let tx = self
                    .wallet
                    .iter()
                    .flat_map(|(_, txs)| txs.iter())
                    .map(|(tx, _)| tx)
                    .find(|tx| tx.compute_txid().to_string() == wanted)
                    .expect("server must have the tx");
                json!(serialize_hex(tx))
            }
            // romanz/electrs looks the block up by the height it was asked for and reports a tx
            // that is not in that block as an error, rather than answering about the block the tx
            // is really in.
            "blockchain.transaction.get_merkle" => {
                let wanted = req.params[0].as_str().expect("must have txid");
                let height = req.params[1].as_u64().expect("must have height") as usize;
                let in_block = self.block_txids(height);
                let pos = match in_block.iter().position(|txid| txid.to_string() == wanted) {
                    Some(pos) => pos,
                    None => return Err(format!("missing txid {wanted} in block {height}")),
                };
                let merkle = match in_block.len() {
                    1 => Vec::new(),
                    2 => vec![in_block[1 - pos].to_string()],
                    n => panic!("the fixtures only build blocks of one or two txs, got {n}"),
                };
                json!({ "block_height": height, "merkle": merkle, "pos": pos })
            }
            other => panic!("unexpected request: {other}"),
        })
    }
}

/// Build the response the server would send, which is an error object when it has no answer.
fn respond(server: &Server, req: &RawRequest) -> RawNotificationOrResponse {
    match server.answer(req) {
        Ok(result) => raw_msg(json!({ "jsonrpc": "2.0", "id": req.id, "result": result })),
        Err(message) => raw_msg(json!({
            "jsonrpc": "2.0",
            "id": req.id,
            "error": { "code": 1, "message": message },
        })),
    }
}

fn drain_requests(
    state: &mut BlockingState,
    queue: &mut ReqQueue,
    server: &Server,
) -> Vec<Update<&'static str>> {
    let mut updates = Vec::new();
    while let Some(req) = queue.pop_front() {
        let resp = respond(server, &req);
        if let Some(update) = state.advance(queue, resp).expect("must advance") {
            updates.push(update);
        }
    }
    updates
}

/// Build a transaction paying `spk`, distinguished from any other by `salt`.
fn tx_paying(spk: ScriptBuf, salt: u32) -> Transaction {
    Transaction {
        version: transaction::Version::ONE,
        lock_time: absolute::LockTime::from_consensus(salt),
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
    }
}

/// A wallet tracking one spk, a tx that block 2 confirms, and a server that so far has genesis
/// and block 1.
///
/// Block 2's merkle root is the txid, so it is a single-tx block and an empty merkle branch at
/// position 0 is the real proof of inclusion in it.
struct AnchorFixture {
    txid: Txid,
    header_2: block::Header,
    /// A different block at height 2, also containing the tx — a same-height reorg.
    header_2b: block::Header,
    /// A block at height 2 that does not contain the tx.
    header_2_without_tx: block::Header,
    /// A different block at height 1, for reorging onto a chain that is shorter than ours.
    header_1b: block::Header,
    /// A block at height 2 that builds on `header_1b`, so that chain can be extended.
    header_2_on_1b: block::Header,
    /// A block at height 3, for extending the chain past the anchor.
    header_3: block::Header,
    spk_hash: ElectrumScriptHash,
    state: BlockingState,
    queue: ReqQueue,
    server: Server,
}

impl AnchorFixture {
    fn new(seed_cache: impl FnOnce(&mut Cache, Txid)) -> anyhow::Result<Self> {
        let descriptor = Descriptor::<DescriptorPublicKey>::from_str(&format!("wpkh({XPUB}/0/*)"))?;
        let spk = descriptor.at_derivation_index(0)?.script_pubkey();
        let spk_hash = ElectrumScriptHash::new(&spk);

        let tx = tx_paying(spk, 0);
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
        let header_2 = block::Header {
            merkle_root: merkle_root_of(&[txid]),
            prev_blockhash: header_1.block_hash(),
            time: 200,
            ..header_1
        };
        let header_2b = block::Header {
            time: 250,
            ..header_2
        };
        let header_2_without_tx = block::Header {
            merkle_root: TxMerkleNode::all_zeros(),
            time: 275,
            ..header_2
        };
        let header_1b = block::Header {
            time: 150,
            ..header_1
        };
        let header_2_on_1b = block::Header {
            prev_blockhash: header_1b.block_hash(),
            time: 300,
            ..header_2
        };
        let header_3 = block::Header {
            merkle_root: TxMerkleNode::all_zeros(),
            prev_blockhash: header_2.block_hash(),
            time: 350,
            ..header_2
        };
        assert_ne!(header_2.block_hash(), header_2b.block_hash());

        let mut cache = Cache::default();
        cache.txs.insert(txid, Arc::new(tx.clone()));
        seed_cache(&mut cache, txid);

        let mut spk_tracker = DerivedSpkTracker::new(0);
        spk_tracker.insert_descriptor("external", descriptor, 0);

        Ok(Self {
            txid,
            header_2,
            header_2b,
            header_2_without_tx,
            header_1b,
            header_2_on_1b,
            header_3,
            spk_hash,
            state: BlockingState::new(
                ReqCoord::default(),
                cache,
                spk_tracker,
                CheckPoint::new(BlockId {
                    height: 0,
                    hash: genesis.block_hash(),
                }),
            ),
            queue: ReqQueue::new(),
            server: Server {
                wallet: vec![(spk_hash, vec![(tx, 2)])],
                ..Server::new(vec![genesis, header_1])
            },
        })
    }

    /// Subscribe and settle against the server's current chain.
    fn sync(&mut self) -> Vec<Update<&'static str>> {
        self.state.init(&mut self.queue);
        drain_requests(&mut self.state, &mut self.queue, &self.server)
    }

    /// Tell the client the server's chain now ends at `header`.
    ///
    /// Returns whatever update that produced: a tip needing no requests resolves inside this
    /// call, so this is the only place that update ever appears.
    fn notify_tip(
        &mut self,
        header: block::Header,
        height: u32,
    ) -> anyhow::Result<Option<Update<&'static str>>> {
        self.state.advance(
            &mut self.queue,
            raw_msg(json!({
                "jsonrpc": "2.0",
                "method": "blockchain.headers.subscribe",
                "params": [{ "hex": serialize_hex(&header), "height": height }],
            })),
        )
    }

    /// Tell the client the tracked script now has the tx confirmed at `height`.
    fn notify_confirmed_at(&mut self, height: u32) -> anyhow::Result<()> {
        let status =
            ElectrumScriptStatus::from_history(&[response::Tx::Confirmed(response::ConfirmedTx {
                txid: self.txid,
                height: absolute::Height::from_consensus(height)?,
            })])
            .expect("history is not empty");
        self.state.advance(
            &mut self.queue,
            raw_msg(json!({
                "jsonrpc": "2.0",
                "method": "blockchain.scripthash.subscribe",
                "params": [self.spk_hash.to_string(), status.to_string()],
            })),
        )?;
        Ok(())
    }

    /// Answer one outstanding request, returning the method it was for.
    fn answer_next(&mut self) -> anyhow::Result<Option<String>> {
        let Some(req) = self.queue.pop_front() else {
            return Ok(None);
        };
        let method = req.method.to_string();
        let resp = respond(&self.server, &req);
        self.state.advance(&mut self.queue, resp)?;
        Ok(Some(method))
    }

    /// Take the answers the server would give to everything outstanding *now*, leaving them
    /// undelivered.
    ///
    /// This is the gap between a server reading a request and its answer reaching the client, and
    /// putting a reorg inside that gap is the only way to produce an answer about a chain the
    /// server has already left.
    fn capture_in_flight(&mut self) -> Vec<RawNotificationOrResponse> {
        let mut captured = Vec::new();
        while let Some(req) = self.queue.pop_front() {
            captured.push(respond(&self.server, &req));
        }
        captured
    }

    /// Deliver answers captured earlier.
    fn deliver(
        &mut self,
        answers: Vec<RawNotificationOrResponse>,
    ) -> anyhow::Result<Vec<Update<&'static str>>> {
        let mut updates = Vec::new();
        for answer in answers {
            if let Some(update) = self.state.advance(&mut self.queue, answer)? {
                updates.push(update);
            }
        }
        Ok(updates)
    }

    /// Answer everything outstanding, returning the methods asked for in order alongside the
    /// updates produced. The order is what the header-before-proof requirement is stated in.
    fn drain(&mut self) -> anyhow::Result<(Vec<String>, Vec<Update<&'static str>>)> {
        let mut asked = Vec::new();
        let mut updates = Vec::new();
        while let Some(req) = self.queue.pop_front() {
            asked.push(req.method.to_string());
            let resp = respond(&self.server, &req);
            if let Some(update) = self.state.advance(&mut self.queue, resp)? {
                updates.push(update);
            }
        }
        Ok((asked, updates))
    }

    fn anchor_in(&self, header: block::Header, height: u32) -> ConfirmationBlockTime {
        ConfirmationBlockTime {
            block_id: BlockId {
                height,
                hash: header.block_hash(),
            },
            confirmation_time: header.time as u64,
        }
    }
}

/// An anchor is a fact about a block, not a claim about the chain, so it must resolve from the
/// header fetched for its height and owe nothing to the local tip.
///
/// The tip never moves here: the client is told the script has history at height 2 and is never
/// told block 2 exists. It must still emit the anchor, naming the block the proof proves. bdk
/// holds anchors additively and ignores the ones whose block is not in its chain, so an anchor
/// and a chain update need neither agree nor travel together.
#[test]
fn an_anchor_resolves_without_the_chain_catching_up() -> anyhow::Result<()> {
    let mut f = AnchorFixture::new(|_, _| {})?;
    let synced = f.sync();
    assert!(
        synced
            .iter()
            .any(|u| u.chain_update.as_ref().is_some_and(|cp| cp.height() == 1)),
        "initial sync must reach the server tip",
    );

    // The server knows about block 2; the client is never told, so its tip stays at 1.
    f.server.headers.push(f.header_2);
    f.notify_confirmed_at(2)?;
    let (asked, updates) = f.drain()?;

    // The proof means nothing except against a specific header, so the header has to be in hand
    // before the proof is asked for. Issuing both at once is a race the proof can win.
    let header_at = asked.iter().position(|m| m == "blockchain.block.header");
    let proof_at = asked
        .iter()
        .position(|m| m == "blockchain.transaction.get_merkle");
    assert!(
        header_at.is_some() && proof_at.is_some() && header_at < proof_at,
        "the header must be fetched, and before the proof: {asked:?}",
    );

    let expected = f.anchor_in(f.header_2, 2);
    assert!(
        updates
            .iter()
            .any(|u| u.tx_update.anchors.contains(&(expected, f.txid))),
        "the anchor must name the block the proof proves, whatever the local tip is doing",
    );
    assert!(
        updates
            .iter()
            .all(|u| u.chain_update.as_ref().is_none_or(|cp| cp.height() == 1)),
        "and nothing here should have moved the chain",
    );
    Ok(())
}

/// A proof can reach the cache before the header it is checked against, so caching it rather
/// than verifying it on arrival is what keeps it useful. Two anchors at one height do it in the
/// wild: the first to non-match discards that height's header while the second's proof is still
/// in flight, and the proof lands with nothing to check it against.
///
/// Ordering the requests does not cover this — the proof is already in hand — so the header must
/// simply be fetched and the anchor resolved from what is held, without asking for the proof
/// again.
#[test]
fn a_proof_held_before_its_header_still_anchors() -> anyhow::Result<()> {
    let mut f = AnchorFixture::new(|cache, txid| {
        cache.proofs.insert(
            (txid, 2),
            bdk_electrum_streaming::Observed::Seen(response::TxMerkle {
                block_height: absolute::Height::from_consensus(2).expect("valid height"),
                merkle: Vec::new(),
                pos: 0,
            }),
        );
    })?;
    f.sync();

    f.server.headers.push(f.header_2);
    f.notify_confirmed_at(2)?;
    let (asked, updates) = f.drain()?;

    let expected = f.anchor_in(f.header_2, 2);
    assert!(
        updates
            .iter()
            .any(|u| u.tx_update.anchors.contains(&(expected, f.txid))),
        "the held proof must still produce the anchor, got requests: {asked:?}",
    );
    assert!(
        !asked
            .iter()
            .any(|m| m == "blockchain.transaction.get_merkle"),
        "the proof was already held and must not be fetched again, got: {asked:?}",
    );
    Ok(())
}

/// A header error for a height our chain does hold must not take the connection down.
///
/// electrs answers `blockchain.block.header` with an error when its index has not caught up to
/// its own tip — it is not only heights above the tip that fail — and that error is the same
/// shape it uses for a genuine fault. Treating it as one would drop the connection, and would
/// discard every other pending anchor along with the one that failed.
#[test]
fn a_header_the_server_will_not_serve_keeps_the_connection() -> anyhow::Result<()> {
    let mut f = AnchorFixture::new(|_, _| {})?;
    f.sync();

    // The block exists and the client is not told about it, so the header has to be fetched —
    // and the server refuses to serve that height.
    f.server.headers.push(f.header_2);
    f.server.header_blackout = Some(2);
    f.notify_confirmed_at(2)?;
    let (asked, _) = f.drain()?;
    assert!(
        asked.iter().any(|m| m == "blockchain.block.header"),
        "the header must have been asked for: {asked:?}",
    );

    // Still serving: the next tip is processed rather than the connection having gone.
    f.server.header_blackout = None;
    let settled = f.notify_tip(f.header_2, 2)?;
    let (_, drained) = f.drain()?;
    assert!(
        settled
            .into_iter()
            .chain(drained)
            .any(|u| u.chain_update.is_some_and(|cp| cp.height() == 2)),
        "the client must still be following the chain after the refused header",
    );
    Ok(())
}

/// Issue #12: a reorg that moves a tx to a different block at the same height.
///
/// The Electrum script status is a hash over `txid:height:` pairs, so the status is byte-for-byte
/// unchanged and the server has no reason to notify. No subscription can be arranged that would
/// tell us. The chain dropping the block is the only signal there is, and it has to be enough.
///
/// The old anchor is not wrong — the tx really was in that block — it is simply no longer in the
/// chain, so bdk ignores it. What is missing is the new one, and only a re-ask produces it.
#[test]
fn a_same_height_reorg_re_asks_for_the_anchor() -> anyhow::Result<()> {
    let mut f = AnchorFixture::new(|_, _| {})?;
    f.sync();

    f.server.headers.push(f.header_2);
    f.notify_tip(f.header_2, 2)?;
    f.notify_confirmed_at(2)?;
    let (_, updates) = f.drain()?;
    let first = f.anchor_in(f.header_2, 2);
    assert!(
        updates
            .iter()
            .any(|u| u.tx_update.anchors.contains(&(first, f.txid))),
        "the tx must anchor in the block that first confirmed it",
    );

    // Same height, different block, tx still in it. Nothing is sent about the script.
    f.server.headers[2] = f.header_2b;
    f.notify_tip(f.header_2b, 2)?;
    let (asked, updates) = f.drain()?;

    let second = f.anchor_in(f.header_2b, 2);
    assert!(
        updates
            .iter()
            .any(|u| u.tx_update.anchors.contains(&(second, f.txid))),
        "the reorg must produce an anchor in the replacement block: {asked:?}",
    );
    assert!(
        !asked
            .iter()
            .any(|m| m == "blockchain.scripthash.get_history"),
        "nothing about the script changed, so its history must not be re-fetched: {asked:?}",
    );
    Ok(())
}

/// A tx reorged out of its block and back in at the same height, with the client never seeing
/// the mempool state in between.
///
/// This is the case a durable negative verdict cannot recover from, and it is why the
/// height-to-txids record must survive a re-ask that finds nothing. Either side of the whole
/// episode the script status is the same bytes — a hash over `txid:height:` pairs, and the height
/// never changed — so if the client misses the moment the tx was unconfirmed, the server never
/// sends anything about this script again. Forget the tx when the first re-ask comes back empty
/// and it stays unanchored for good.
#[test]
fn a_tx_reorged_out_and_back_in_at_the_same_height_re_anchors() -> anyhow::Result<()> {
    let mut f = AnchorFixture::new(|_, _| {})?;
    f.sync();

    f.server.headers.push(f.header_2);
    f.notify_tip(f.header_2, 2)?;
    f.notify_confirmed_at(2)?;
    f.drain()?;
    assert_eq!(
        f.state.cache().anchors.len(),
        1,
        "the tx must be anchored before the reorg",
    );

    // Height 2 becomes a block without the tx. The re-ask happens and finds nothing to anchor.
    f.server.headers[2] = f.header_2_without_tx;
    f.notify_tip(f.header_2_without_tx, 2)?;
    let (asked, updates) = f.drain()?;
    assert!(
        asked
            .iter()
            .any(|m| m == "blockchain.transaction.get_merkle"),
        "the eviction must have prompted a re-ask, got: {asked:?}",
    );
    let re_anchored = f.anchor_in(f.header_2_without_tx, 2);
    assert!(
        !updates
            .iter()
            .any(|u| u.tx_update.anchors.contains(&(re_anchored, f.txid))),
        "the tx is not in that block, so nothing may be anchored to it",
    );

    // The tx is mined again, still at height 2, in yet another block. The script status is now
    // exactly what it was before any of this, so nothing is sent about the script.
    f.server.headers[2] = f.header_2b;
    f.notify_tip(f.header_2b, 2)?;
    let (asked, updates) = f.drain()?;

    let expected = f.anchor_in(f.header_2b, 2);
    assert!(
        updates
            .iter()
            .any(|u| u.tx_update.anchors.contains(&(expected, f.txid))),
        "the tx must re-anchor once it is back in a block, got requests: {asked:?}",
    );
    Ok(())
}

/// A reorg that lands while an anchor is being fetched must not produce an anchor for the block
/// that was there when the fetch started.
///
/// The header is fetched first and the proof second, so there is a window between them, and this
/// puts the reorg squarely in it. What closes the window is that an answer is only accepted
/// against the chain it was asked for: the proof was asked for before the reorg, so it is dropped
/// and asked for again, and the anchor names the block the fresh proof actually proves.
#[test]
fn a_reorg_landing_mid_fetch_anchors_the_block_the_proof_proves() -> anyhow::Result<()> {
    let mut f = AnchorFixture::new(|_, _| {})?;
    f.sync();

    // The tx confirms at height 2. The client is told about the script, not the tip, so it has to
    // fetch the header itself.
    f.server.headers.push(f.header_2);
    f.notify_confirmed_at(2)?;
    loop {
        match f.answer_next()? {
            Some(method) if method == "blockchain.block.header" => break,
            Some(_) => continue,
            None => panic!("the header for the confirmation height was never asked for"),
        }
    }
    assert!(
        f.queue
            .iter()
            .any(|req| &*req.method == "blockchain.transaction.get_merkle"),
        "the proof must be in flight for this to be a mid-fetch reorg at all",
    );

    // The reorg: height 2 is now a different block, and the tx is in that one too.
    f.server.headers[2] = f.header_2b;
    f.notify_tip(f.header_2b, 2)?;
    let (asked, updates) = f.drain()?;

    let proven = f.anchor_in(f.header_2b, 2);
    let superseded = f.anchor_in(f.header_2, 2);
    assert!(
        updates
            .iter()
            .any(|u| u.tx_update.anchors.contains(&(proven, f.txid))),
        "the anchor must name the block the proof was checked against, got: {asked:?}",
    );
    assert!(
        !updates
            .iter()
            .any(|u| u.tx_update.anchors.contains(&(superseded, f.txid))),
        "no anchor may name the block that was there when the fetch began",
    );
    Ok(())
}

/// A reorg onto a shorter chain, and then back up over the height the tx was in.
///
/// While the chain is short there is nothing to ask about: the block that will hold the tx has
/// not been mined. The re-ask has to *wait* rather than ask and be told no, because giving up
/// puts recovery out of reach — the record it would fall back on is only read when a height is
/// evicted, and growing back over a height is not an eviction. Nothing else would cover it
/// either: the status went confirmed, mempool, confirmed-at-the-same-height, so a client that
/// misses the middle sees the status it started with.
#[test]
fn a_re_ask_survives_the_chain_shrinking_below_it() -> anyhow::Result<()> {
    let mut f = AnchorFixture::new(|_, _| {})?;
    f.sync();

    f.server.headers.push(f.header_2);
    f.notify_tip(f.header_2, 2)?;
    f.notify_confirmed_at(2)?;
    f.drain()?;
    let first = f.anchor_in(f.header_2, 2);
    assert!(
        f.state
            .cache()
            .anchors
            .contains_key(&(f.txid, first.block_id.hash)),
        "the tx must be anchored before the reorg",
    );

    // The server reorgs onto a chain shorter than ours: a different block at height 1, nothing
    // above it.
    f.server.headers = vec![f.server.headers[0], f.header_1b];
    f.notify_tip(f.header_1b, 1)?;
    let (asked, _) = f.drain()?;
    assert!(
        !asked.iter().any(|m| m == "blockchain.block.header"),
        "there is nothing at that height to ask about yet, so nothing should be asked: {asked:?}",
    );

    // The chain grows back over the height, with the tx in the new block.
    f.server.headers.push(f.header_2_on_1b);
    let settled = f.notify_tip(f.header_2_on_1b, 2)?;
    let (asked, drained) = f.drain()?;

    let expected = f.anchor_in(f.header_2_on_1b, 2);
    assert!(
        settled
            .into_iter()
            .chain(drained)
            .any(|u| u.tx_update.anchors.contains(&(expected, f.txid))),
        "growing back over the height must revive the re-ask, got: {asked:?}",
    );
    Ok(())
}

/// Extending the chain while an anchor is held must evict nothing.
///
/// An extension replaces no block, so no anchor's block has left the chain and there is nothing
/// to ask about again. Getting this wrong is not a missed refresh but a permanent tax: every new
/// block would re-fetch every anchor a wallet holds.
#[test]
fn extending_the_chain_with_an_anchor_held_evicts_nothing() -> anyhow::Result<()> {
    let mut f = AnchorFixture::new(|_, _| {})?;
    f.sync();

    f.server.headers.push(f.header_2);
    f.notify_tip(f.header_2, 2)?;
    f.notify_confirmed_at(2)?;
    f.drain()?;
    assert!(
        !f.state.cache().anchored_at.is_empty(),
        "the tx must be anchored, or this test cannot reach the case",
    );

    f.server.headers.push(f.header_3);
    let settled = f.notify_tip(f.header_3, 3)?;
    let (asked, drained) = f.drain()?;
    assert!(
        settled
            .into_iter()
            .chain(drained)
            .any(|u| u.chain_update.is_some_and(|cp| cp.height() == 3)),
        "the extension must be followed",
    );
    assert!(
        !asked
            .iter()
            .any(|m| m == "blockchain.transaction.get_merkle"),
        "no block left the chain, so no anchor may be asked for again: {asked:?}",
    );
    Ok(())
}

/// Run a burst of same-height reorgs against an anchor whose header fetch is left in flight.
///
/// `announced` is the burst's speed as the client experiences it: each entry is how many reorgs
/// the server gets through before it announces a tip, so `[1, 1, 1]` is a server announcing every
/// one and `[3]` is a server that reorged three times between announcements and only ever names
/// the last. The reorgs folded into one announcement are invisible — no notification, no script
/// status change, nothing — so a fast burst is not a slower one sped up, it is a burst the client
/// is told less about. Both ends have to reach the same block.
fn run_reorg_burst(announced: &[usize]) -> anyhow::Result<()> {
    assert!(
        !announced.is_empty(),
        "a burst the client never hears about is not a burst",
    );
    let mut f = AnchorFixture::new(|_, _| {})?;

    let confirming = f.header_2;
    let tip_on = |header_2: block::Header, salt: u32| block::Header {
        merkle_root: TxMerkleNode::all_zeros(),
        prev_blockhash: header_2.block_hash(),
        time: 1_000 + salt,
        ..header_2
    };
    let variant = |salt: u32| block::Header {
        time: 2_000 + salt,
        ..confirming
    };

    // The tx confirms at height 2 while the tip is at 3, so the anchor's header is one the client
    // must fetch: a tip notification would otherwise hand it over and there would be no request
    // in flight to race.
    f.server.headers.push(confirming);
    f.server.headers.push(tip_on(confirming, 0));
    f.sync();
    f.notify_confirmed_at(2)?;
    f.drain()?;
    assert!(
        f.state
            .cache()
            .anchors
            .contains_key(&(f.txid, confirming.block_hash())),
        "the tx must be anchored before the burst",
    );

    // The first reorg is announced on its own, because its re-ask has to reach the wire and be
    // read by the server before the rest of the burst lands on top of it.
    let mut salt = 1_u32;
    let first = variant(salt);
    f.server.headers[2] = first;
    f.server.headers[3] = tip_on(first, salt);
    f.notify_tip(f.server.headers[3], 3)?;
    // Only what the chain pass needs: the re-ask it triggers has to be left outstanding, which is
    // the whole point of the burst.
    while f
        .queue
        .front()
        .is_some_and(|req| &*req.method != "blockchain.block.header")
    {
        f.answer_next()?;
    }
    assert!(
        f.queue
            .iter()
            .any(|req| &*req.method == "blockchain.block.header"),
        "the eviction must have put a header request on the wire",
    );
    let in_flight = f.capture_in_flight();

    let mut last = first;
    for &folded in announced {
        assert!(folded >= 1, "an announcement names some reorg");
        for _ in 0..folded {
            salt += 1;
            last = variant(salt);
            f.server.headers[2] = last;
            f.server.headers[3] = tip_on(last, salt);
        }
        f.notify_tip(f.server.headers[3], 3)?;
        let (asked, _) = f.drain()?;
        assert!(
            !asked.iter().any(|m| m == "blockchain.block.header"),
            "the re-ask must have merged into the outstanding request, at {announced:?}: {asked:?}",
        );
    }
    assert_ne!(first.block_hash(), last.block_hash());

    let stale = f.deliver(in_flight)?;
    let (asked, fresh) = f.drain()?;
    assert!(
        asked.iter().any(|m| m == "blockchain.block.header"),
        "dropping the stale answer must be what finally puts a fresh request on the wire, \
         at {announced:?}: {asked:?}",
    );

    let obsolete = f.anchor_in(first, 2);
    let current = f.anchor_in(last, 2);
    let anchored = stale
        .into_iter()
        .chain(fresh)
        .flat_map(|u| u.tx_update.anchors)
        .collect::<Vec<_>>();
    assert!(
        !anchored.contains(&(obsolete, f.txid)),
        "an answer about a chain the server has left must not settle the anchor, \
         at {announced:?}: {anchored:?}",
    );
    assert!(
        anchored.contains(&(current, f.txid)),
        "the anchor must reach the block that is in the chain now, at {announced:?}: {anchored:?}",
    );
    Ok(())
}

/// Reorgs arriving faster than a fetch completes must still leave the anchor on the last block.
///
/// `ReqQueuer::enqueue` merges a request into an identical one already in flight, so every re-ask
/// raised during the burst is absorbed and puts nothing on the wire. The one answer that does
/// arrive describes the chain the server held when it read the first request. Taking it settles
/// the anchor on a block eleven reorgs out of date, and — because the record shows the anchor as
/// resolved and no further eviction is coming — nothing ever revisits it.
///
/// Anchoring the first block would not be *wrong*; the tx really was in it. Failing to reach the
/// last one is, because that anchor is the only one still in the chain.
///
/// Run at three speeds. A server announcing every reorg is the slow end; one folding the whole
/// burst into a single announcement is the fast end, and it is the one worth being suspicious of,
/// because a client told about a single chain change might well be right by accident. It is not:
/// the answer in flight still predates that change.
#[test]
fn a_burst_of_reorgs_anchors_the_last_block_not_the_first() -> anyhow::Result<()> {
    run_reorg_burst(&[1; 11])?;
    run_reorg_burst(&[4, 1, 6])?;
    run_reorg_burst(&[11])?;
    Ok(())
}

/// The far end of the same burst: every reorg lands before the server gets to the request, so the
/// answer it sends is current and must simply be taken.
///
/// A guard that cannot tell a stale answer from a timely one costs a round trip on every reorg,
/// and against a chain that keeps moving it is a re-ask that never converges.
#[test]
fn an_answer_that_outlasts_the_burst_is_taken_as_it_stands() -> anyhow::Result<()> {
    let mut f = AnchorFixture::new(|_, _| {})?;

    let confirming = f.header_2;
    let tip_on = |header_2: block::Header, salt: u32| block::Header {
        merkle_root: TxMerkleNode::all_zeros(),
        prev_blockhash: header_2.block_hash(),
        time: 1_000 + salt,
        ..header_2
    };

    f.server.headers.push(confirming);
    f.server.headers.push(tip_on(confirming, 0));
    f.sync();
    f.notify_confirmed_at(2)?;
    f.drain()?;

    // Twelve reorgs, none of them announced: the server settles before the client is told anything
    // at all, so the re-ask that follows goes out against a chain that has stopped moving.
    let mut last = confirming;
    for salt in 1..13 {
        last = block::Header {
            time: 2_000 + salt,
            ..confirming
        };
        f.server.headers[2] = last;
        f.server.headers[3] = tip_on(last, salt);
    }
    f.notify_tip(f.server.headers[3], 3)?;
    let (asked, updates) = f.drain()?;

    let headers_asked = asked
        .iter()
        .filter(|m| *m == "blockchain.block.header")
        .count();
    assert_eq!(
        headers_asked, 1,
        "an answer nothing has invalidated must be taken as it stands: {asked:?}",
    );
    let current = f.anchor_in(last, 2);
    assert!(
        updates
            .iter()
            .any(|u| u.tx_update.anchors.contains(&(current, f.txid))),
        "and it must anchor the block the chain settled on: {asked:?}",
    );
    Ok(())
}

/// An ordinary new block must not invalidate an answer already in flight.
///
/// Extending the chain takes nothing out of it, so nothing an outstanding request could be asking
/// about has changed. This is why the counter moves on evictions rather than on chain updates: a
/// counter bumped by every block would drop the answer to every anchor fetch that a new block
/// happened to overlap, and blocks keep coming. Every other test here passes with it bumped
/// unconditionally, which is the whole reason this one exists.
#[test]
fn a_new_block_does_not_invalidate_an_answer_in_flight() -> anyhow::Result<()> {
    let mut f = AnchorFixture::new(|_, _| {})?;
    f.sync();

    // The tx confirms at height 2, which the client is not told about, so it asks for the header
    // itself — and that answer is held on the wire.
    f.server.headers.push(f.header_2);
    f.notify_confirmed_at(2)?;
    while f
        .queue
        .front()
        .is_some_and(|req| &*req.method != "blockchain.block.header")
    {
        f.answer_next()?;
    }
    let in_flight = f.capture_in_flight();
    assert_eq!(
        in_flight.len(),
        1,
        "exactly the header fetch must be outstanding",
    );

    // A plain extension lands while it is out: block 3, on top of the block the tx is in.
    f.server.headers.push(f.header_3);
    f.notify_tip(f.header_3, 3)?;
    f.drain()?;

    let settled = f.deliver(in_flight)?;
    let (asked, drained) = f.drain()?;
    assert!(
        !asked.iter().any(|m| m == "blockchain.block.header"),
        "the answer was never invalidated, so nothing may be asked for again: {asked:?}",
    );
    let expected = f.anchor_in(f.header_2, 2);
    assert!(
        settled
            .into_iter()
            .chain(drained)
            .any(|u| u.tx_update.anchors.contains(&(expected, f.txid))),
        "and it must anchor from the answer it already had: {asked:?}",
    );
    Ok(())
}

/// Grow `prefix` to `len` blocks, with the block at `tx_at` containing `txid` and every other
/// block empty. Chains grown from one prefix with different `salt`s diverge at every height above
/// it. Nothing checks proof of work, so nothing needs grinding.
fn grow(
    prefix: Vec<block::Header>,
    len: usize,
    salt: u32,
    tx_at: usize,
    txid: Txid,
) -> Vec<block::Header> {
    let mut headers = prefix;
    for i in headers.len()..len {
        headers.push(block::Header {
            version: block::Version::ONE,
            prev_blockhash: headers[i - 1].block_hash(),
            merkle_root: if i == tx_at {
                merkle_root_of(&[txid])
            } else {
                TxMerkleNode::all_zeros()
            },
            time: 1_000_000 + salt * 100_000 + i as u32,
            bits: CompactTarget::from_consensus(0x207fffff),
            nonce: 0,
        });
    }
    headers
}

/// An error that predates a reorg must not be recorded against the chain that replaced it.
///
/// The stale-answer guard is worth more on this branch than on the successful one, because what
/// it prevents is worse. A recorded refusal is consumed on read and abandons the re-ask, so the
/// anchor stays on the block that left the chain — and the eviction that scheduled the re-ask has
/// already been spent, so only a *further* reorg at that height would ever try again. The answer
/// to the request the reorg replaced would have killed its own replacement.
#[test]
fn an_error_that_predates_a_reorg_is_not_taken_as_a_refusal() -> anyhow::Result<()> {
    let mut f = AnchorFixture::new(|_, _| {})?;

    let confirming = f.header_2;
    let tip_on = |header_2: block::Header, salt: u32| block::Header {
        merkle_root: TxMerkleNode::all_zeros(),
        prev_blockhash: header_2.block_hash(),
        time: 1_000 + salt,
        ..header_2
    };
    let variant = |salt: u32| block::Header {
        time: 2_000 + salt,
        ..confirming
    };

    f.server.headers.push(confirming);
    f.server.headers.push(tip_on(confirming, 0));
    f.sync();
    f.notify_confirmed_at(2)?;
    f.drain()?;
    assert!(
        f.state
            .cache()
            .anchors
            .contains_key(&(f.txid, confirming.block_hash())),
        "the tx must be anchored before the reorg",
    );

    // A reorg evicts the block the tx was in, and the server will not serve a header for that
    // height when it reads the re-ask — an index that has not caught up answers exactly this way.
    let first = variant(1);
    f.server.headers[2] = first;
    f.server.headers[3] = tip_on(first, 1);
    f.notify_tip(f.server.headers[3], 3)?;
    while f
        .queue
        .front()
        .is_some_and(|req| &*req.method != "blockchain.block.header")
    {
        f.answer_next()?;
    }
    f.server.header_blackout = Some(2);
    let refusal = f.capture_in_flight();
    assert_eq!(refusal.len(), 1, "exactly the re-ask must be outstanding");
    f.server.header_blackout = None;

    // A second reorg lands before that refusal arrives, so it is about a chain the server has left.
    let last = variant(2);
    f.server.headers[2] = last;
    f.server.headers[3] = tip_on(last, 2);
    f.notify_tip(f.server.headers[3], 3)?;
    f.drain()?;

    let settled = f.deliver(refusal)?;
    let (asked, drained) = f.drain()?;
    assert!(
        asked.iter().any(|m| m == "blockchain.block.header"),
        "the stale refusal must be discarded and the height asked about again: {asked:?}",
    );
    let expected = f.anchor_in(last, 2);
    assert!(
        settled
            .into_iter()
            .chain(drained)
            .any(|u| u.tx_update.anchors.contains(&(expected, f.txid))),
        "and the anchor must reach the block that is in the chain now: {asked:?}",
    );
    Ok(())
}

/// A header fetched before a reorg must not be spliced into the chain after it.
///
/// Filling a gap is the only place an anchor's header edits the chain, and the checks around it
/// are about shape, not provenance: they ask whether we already hold a block at that height, not
/// whether the answer describes the chain we are on now. A sparse chain is what makes the gap
/// reachable, and a fixed-window chain pass leaves one every time it follows a reorg — it rewrites
/// the window and nothing below it.
#[test]
fn a_header_fetched_before_a_reorg_is_not_spliced_into_the_chain() -> anyhow::Result<()> {
    let descriptor = Descriptor::<DescriptorPublicKey>::from_str(&format!("wpkh({XPUB}/0/*)"))?;
    let spk = descriptor.at_derivation_index(0)?.script_pubkey();
    let spk_hash = ElectrumScriptHash::new(&spk);
    let tx = tx_paying(spk, 0);
    let txid = tx.compute_txid();

    // The tx confirms at height 10, far enough below the tip that the chain pass's window never
    // reaches it, so it stays a gap in the local chain.
    const CONFIRMED_AT: usize = 10;
    let genesis = constants::genesis_block(Network::Regtest).header;
    let abandoned = grow(vec![genesis], 40, 1, CONFIRMED_AT, txid);
    let followed = grow(abandoned[..5].to_vec(), 40, 2, CONFIRMED_AT, txid);
    assert_ne!(
        abandoned[CONFIRMED_AT].block_hash(),
        followed[CONFIRMED_AT].block_hash(),
        "the reorg must reach the height the tx is in, or the stale answer is not stale",
    );

    let mut cache = Cache::default();
    cache.txs.insert(txid, Arc::new(tx.clone()));
    let mut spk_tracker = DerivedSpkTracker::new(0);
    spk_tracker.insert_descriptor("external", descriptor, 0);
    // Sparse from the start, the way a chain restored from persistence is.
    let cp = CheckPoint::from_block_ids([
        BlockId {
            height: 0,
            hash: genesis.block_hash(),
        },
        BlockId {
            height: 39,
            hash: abandoned[39].block_hash(),
        },
    ])
    .expect("heights ascend");
    let mut state = BlockingState::new(ReqCoord::default(), cache, spk_tracker, cp);
    let mut queue = ReqQueue::new();
    let mut server = Server {
        wallet: vec![(spk_hash, vec![(tx, CONFIRMED_AT)])],
        ..Server::new(abandoned.clone())
    };

    state.init(&mut queue);
    drain_requests(&mut state, &mut queue, &server);

    let status =
        ElectrumScriptStatus::from_history(&[response::Tx::Confirmed(response::ConfirmedTx {
            txid,
            height: absolute::Height::from_consensus(CONFIRMED_AT as u32)?,
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
    while queue
        .front()
        .is_some_and(|req| &*req.method != "blockchain.block.header")
    {
        let req = queue.pop_front().expect("just checked");
        let resp = respond(&server, &req);
        state.advance(&mut queue, resp)?;
    }
    let in_flight = queue
        .pop_front()
        .map(|req| respond(&server, &req))
        .expect("the header for the confirmation height must have been asked for");
    assert!(queue.is_empty(), "only the header may be outstanding");

    // The reorg forks below the tx's height, so the answer in flight describes a block on a branch
    // the server has abandoned — while the chain pass rewrites only its window near the tip and
    // leaves height 10 a gap.
    server.headers = followed.clone();
    state.advance(
        &mut queue,
        raw_msg(json!({
            "jsonrpc": "2.0",
            "method": "blockchain.headers.subscribe",
            "params": [{ "hex": serialize_hex(&followed[39]), "height": 39 }],
        })),
    )?;
    drain_requests(&mut state, &mut queue, &server);

    state.advance(&mut queue, in_flight)?;
    let updates = drain_requests(&mut state, &mut queue, &server);

    let chain = updates
        .iter()
        .rev()
        .find_map(|u| u.chain_update.clone())
        .expect("the anchor's update carries the chain it was resolved against");
    assert_eq!(
        chain.get(CONFIRMED_AT as u32).map(|cp| cp.hash()),
        Some(followed[CONFIRMED_AT].block_hash()),
        "the gap must be filled from the chain we are on, not the one we left",
    );
    let expected = ConfirmationBlockTime {
        block_id: BlockId {
            height: CONFIRMED_AT as u32,
            hash: followed[CONFIRMED_AT].block_hash(),
        },
        confirmation_time: followed[CONFIRMED_AT].time as u64,
    };
    assert!(
        updates
            .iter()
            .any(|u| u.tx_update.anchors.contains(&(expected, txid))),
        "and the anchor must name that block too",
    );
    Ok(())
}

/// One header can be what two spk jobs are waiting on, because `ReqCoord` merges identical
/// requests. Delivering that response must advance both.
///
/// The loser is stranded silently: the response it was waiting for has been delivered, so nothing
/// is in flight and nothing will arrive to wake it. Its tx simply never anchors.
#[test]
fn a_header_two_jobs_wanted_advances_both() -> anyhow::Result<()> {
    let descriptor = Descriptor::<DescriptorPublicKey>::from_str(&format!("wpkh({XPUB}/0/*)"))?;
    let spk_a = descriptor.at_derivation_index(0)?.script_pubkey();
    let spk_b = descriptor.at_derivation_index(1)?.script_pubkey();
    let (hash_a, hash_b) = (
        ElectrumScriptHash::new(&spk_a),
        ElectrumScriptHash::new(&spk_b),
    );

    // Two txs paying different scripts of ours, confirmed in the same block — so the two spk jobs
    // want the same header, and the same block's proofs.
    let (tx_a, tx_b) = (tx_paying(spk_a, 1), tx_paying(spk_b, 2));
    let (txid_a, txid_b) = (tx_a.compute_txid(), tx_b.compute_txid());
    let mut in_block = [txid_a, txid_b];
    in_block.sort();

    let genesis = constants::genesis_block(Network::Regtest).header;
    let header_1 = block::Header {
        version: block::Version::ONE,
        prev_blockhash: genesis.block_hash(),
        merkle_root: TxMerkleNode::all_zeros(),
        time: 100,
        bits: CompactTarget::from_consensus(0x207fffff),
        nonce: 0,
    };
    let header_2 = block::Header {
        merkle_root: merkle_root_of(&in_block),
        prev_blockhash: header_1.block_hash(),
        time: 200,
        ..header_1
    };

    let mut cache = Cache::default();
    cache.txs.insert(txid_a, Arc::new(tx_a.clone()));
    cache.txs.insert(txid_b, Arc::new(tx_b.clone()));
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
    let server = Server {
        wallet: vec![(hash_a, vec![(tx_a, 2)]), (hash_b, vec![(tx_b, 2)])],
        ..Server::new(vec![genesis, header_1, header_2])
    };

    state.init(&mut queue);
    for (spk_hash, txid) in [(hash_a, txid_a), (hash_b, txid_b)] {
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
    }
    let updates = drain_requests(&mut state, &mut queue, &server);

    let expected = ConfirmationBlockTime {
        block_id: BlockId {
            height: 2,
            hash: header_2.block_hash(),
        },
        confirmation_time: header_2.time as u64,
    };
    let anchored = updates
        .into_iter()
        .flat_map(|u| u.tx_update.anchors)
        .collect::<Vec<_>>();
    for txid in [txid_a, txid_b] {
        assert!(
            anchored.contains(&(expected, txid)),
            "both jobs waiting on the one header must anchor: {anchored:?}",
        );
    }
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
