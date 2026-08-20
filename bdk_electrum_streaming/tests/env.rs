use std::{sync::atomic::AtomicBool, time::Duration};

use bdk_chain::{
    keychain_txout::KeychainTxOutIndex, local_chain::LocalChain, CanonicalizationParams,
    ChainPosition, IndexedTxGraph,
};
use bdk_core::{
    bitcoin::{key::Secp256k1, params::REGTEST, Address, Amount, Txid},
    BlockId, ConfirmationBlockTime,
};
use bdk_electrum_streaming::{
    run_async, run_blocking, AsyncClient, AsyncState, BlockingClient, BlockingState, Cache,
    DerivedSpkTracker, ReqCoord, Update,
};
use bdk_testenv::{bitcoincore_rpc::RpcApi, utils::DESCRIPTORS, TestEnv};
use futures::{channel::mpsc, pin_mut, FutureExt, StreamExt};
use miniscript::Descriptor;
use tokio::net::TcpStream;
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};
use tracing::Level;

const EXTERNAL: &str = "external";
const INTERNAL: &str = "internal";
const LOOKAHEAD: u32 = 6;

fn init() {
    let _ = tracing_subscriber::fmt()
        .with_test_writer()
        .with_max_level(Level::TRACE)
        .try_init();
}

fn apply_update(
    chain: &mut LocalChain,
    graph: &mut IndexedTxGraph<ConfirmationBlockTime, KeychainTxOutIndex<&'static str>>,
    update: Update<&'static str>,
) -> anyhow::Result<()> {
    let _ = graph
        .index
        .reveal_to_target_multi(&update.last_active_indices);
    let _ = graph.apply_update(update.tx_update);
    if let Some(cp) = update.chain_update {
        chain.apply_update(cp)?;
    } else {
        panic!("NO CHAIN UPDATE!");
    }
    Ok(())
}

#[test]
fn blocking_env() -> anyhow::Result<()> {
    init();

    let secp = Secp256k1::new();
    let env = TestEnv::new()?;
    let electrum_url = env.electrsd.electrum_url.clone();

    let (external, _external_keys) = Descriptor::parse_descriptor(&secp, DESCRIPTORS[0])?;
    let (internal, _internal_keys) = Descriptor::parse_descriptor(&secp, DESCRIPTORS[1])?;

    let mut graph = IndexedTxGraph::<ConfirmationBlockTime, _>::new({
        let mut indexer = KeychainTxOutIndex::<&'static str>::new(LOOKAHEAD, false);
        indexer.insert_descriptor(EXTERNAL, external.clone())?;
        indexer.insert_descriptor(INTERNAL, internal.clone())?;
        indexer
    });
    let (mut chain, _cs) = LocalChain::from_genesis_hash(env.genesis_hash()?);

    let mut spk_tracker = DerivedSpkTracker::<&'static str>::new(LOOKAHEAD);
    spk_tracker.insert_descriptor(EXTERNAL, external, 0);
    spk_tracker.insert_descriptor(INTERNAL, internal, 0);

    let mut state = BlockingState::new(
        ReqCoord::default(),
        Cache::default(),
        spk_tracker,
        chain.tip(),
    );

    let (mut update_tx, update_rx) = std::sync::mpsc::channel::<Update<&'static str>>();
    let (client, mut client_rx) = BlockingClient::new();

    let conn = std::net::TcpStream::connect(&electrum_url)?;
    let run_conn = conn.try_clone()?;
    let run_handle = std::thread::spawn(move || {
        let res = run_blocking(
            &mut state,
            &mut AtomicBool::new(false),
            &mut update_tx,
            &mut client_rx,
            &mut &run_conn,
            &mut &run_conn,
        );
        state.reset();
        res
    });

    // First block update (genesis).
    let update = update_rx.recv().expect("Must have next update");
    apply_update(&mut chain, &mut graph, update)?;

    let ((_, spk), _) = graph
        .index
        .next_unused_spk(EXTERNAL)
        .expect("must derive spk");
    env.mine_blocks(101, Some(Address::from_script(&spk, &REGTEST)?))?;
    std::thread::sleep(Duration::from_secs(3));

    while let Ok(update) = update_rx.recv() {
        let has_tx_update = !update.tx_update.txs.is_empty();
        apply_update(&mut chain, &mut graph, update)?;
        if has_tx_update {
            break;
        }
    }

    let balance = graph.graph().balance(
        &chain,
        chain.tip().block_id(),
        CanonicalizationParams::default(),
        graph.index.outpoints().clone(),
        |(k, _), _| *k == INTERNAL,
    );
    for cp in chain.iter_checkpoints() {
        println!("height={}, hash={}", cp.height(), cp.hash());
    }
    println!("BALANCE: {}", balance);

    // TODO: Figure out a way to stop the thread without having to close the connection.
    conn.shutdown(std::net::Shutdown::Both)?;
    client.stop()?;

    run_handle.join().expect("must join")?;
    Ok(())
}

#[tokio::test]
async fn env() -> anyhow::Result<()> {
    init();

    let secp = Secp256k1::new();
    let env = TestEnv::new()?;
    let electrum_url = env.electrsd.electrum_url.clone();

    let (external, _external_keys) = Descriptor::parse_descriptor(&secp, DESCRIPTORS[0])?;
    let (internal, _internal_keys) = Descriptor::parse_descriptor(&secp, DESCRIPTORS[1])?;

    let mut graph = IndexedTxGraph::<ConfirmationBlockTime, _>::new({
        let mut indexer = KeychainTxOutIndex::<&'static str>::new(LOOKAHEAD, false);
        indexer.insert_descriptor(EXTERNAL, external.clone())?;
        indexer.insert_descriptor(INTERNAL, internal.clone())?;
        indexer
    });
    let (mut chain, _cs) = LocalChain::from_genesis_hash(env.genesis_hash()?);

    let mut spk_tracker = DerivedSpkTracker::<&'static str>::new(LOOKAHEAD);
    spk_tracker.insert_descriptor(EXTERNAL, external, 0);
    spk_tracker.insert_descriptor(INTERNAL, internal, 0);

    let mut state = AsyncState::new(
        ReqCoord::default(),
        Cache::default(),
        spk_tracker,
        chain.tip(),
    );

    let (mut update_tx, mut update_rx) = mpsc::unbounded::<Update<&'static str>>();
    let (client, mut client_rx) = AsyncClient::new();

    let run_handle = tokio::spawn(async move {
        let mut conn = TcpStream::connect(&electrum_url).await?;
        let (read, write) = conn.split();
        run_async(
            &mut state,
            &mut update_tx,
            &mut client_rx,
            read.compat(),
            write.compat_write(),
        )
        .await?;
        anyhow::Ok(())
    });

    // First block update (genesis).
    let update = update_rx.next().await.expect("Must have next update");
    apply_update(&mut chain, &mut graph, update)?;

    let ((_, spk), _) = graph
        .index
        .next_unused_spk(EXTERNAL)
        .expect("must derive spk");
    env.mine_blocks(101, Some(Address::from_script(&spk, &REGTEST)?))?;
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Wait until we sync up to block 101
    let timeout = tokio::time::sleep(Duration::from_secs(150)).fuse();
    pin_mut!(timeout);

    loop {
        futures::select! {
            _ = timeout => return Err(anyhow::anyhow!("Timed-out waiting for chain sync.")),
            update = update_rx.next() => {
                let update = update.expect("Must have next update");
                let has_tx_update = !update.tx_update.txs.is_empty();
                apply_update(&mut chain, &mut graph, update)?;
                if has_tx_update {
                    break;
                }
            },
        }
    }

    let balance = graph.graph().balance(
        &chain,
        chain.tip().block_id(),
        CanonicalizationParams::default(),
        graph.index.outpoints().clone(),
        |(k, _), _| *k == INTERNAL,
    );
    for cp in chain.iter_checkpoints() {
        println!("height={}, hash={}", cp.height(), cp.hash());
    }
    println!("BALANCE: {}", balance);

    client.stop().await?;
    run_handle.await??;

    Ok(())
}

/// A new block confirming a tracked tx must anchor it on the live connection — no reconnect,
/// regardless of the order in which the server sends the script hash and header notifications.
/// The order-sensitive case (history reporting a height above the local tip) is pinned
/// deterministically in `tests/state.rs`.
#[tokio::test]
async fn new_block_confirmation_is_anchored_live() -> anyhow::Result<()> {
    init();

    let secp = Secp256k1::new();
    let env = TestEnv::new()?;
    let electrum_url = env.electrsd.electrum_url.clone();

    let (external, _external_keys) = Descriptor::parse_descriptor(&secp, DESCRIPTORS[0])?;
    let (internal, _internal_keys) = Descriptor::parse_descriptor(&secp, DESCRIPTORS[1])?;

    let mut graph = IndexedTxGraph::<ConfirmationBlockTime, _>::new({
        let mut indexer = KeychainTxOutIndex::<&'static str>::new(LOOKAHEAD, false);
        indexer.insert_descriptor(EXTERNAL, external.clone())?;
        indexer.insert_descriptor(INTERNAL, internal.clone())?;
        indexer
    });
    let (mut chain, _cs) = LocalChain::from_genesis_hash(env.genesis_hash()?);

    let mut spk_tracker = DerivedSpkTracker::<&'static str>::new(LOOKAHEAD);
    spk_tracker.insert_descriptor(EXTERNAL, external, 0);
    spk_tracker.insert_descriptor(INTERNAL, internal, 0);

    let mut state = AsyncState::new(
        ReqCoord::default(),
        Cache::default(),
        spk_tracker,
        chain.tip(),
    );

    let (mut update_tx, mut update_rx) = mpsc::unbounded::<Update<&'static str>>();
    let (client, mut client_rx) = AsyncClient::new();

    let run_handle = tokio::spawn(async move {
        let mut conn = TcpStream::connect(&electrum_url).await?;
        let (read, write) = conn.split();
        run_async(
            &mut state,
            &mut update_tx,
            &mut client_rx,
            read.compat(),
            write.compat_write(),
        )
        .await?;
        anyhow::Ok(())
    });

    let update = update_rx.next().await.expect("Must have next update");
    apply_update(&mut chain, &mut graph, update)?;

    // Coinbase maturity, so that `env.send` has funds to spend.
    env.mine_blocks(101, None)?;
    let premine_height = env.rpc_client().get_block_count()? as u32;

    let timeout = tokio::time::sleep(Duration::from_secs(150)).fuse();
    pin_mut!(timeout);
    while chain.tip().height() < premine_height {
        futures::select! {
            _ = timeout => return Err(anyhow::anyhow!("Timed-out waiting for chain sync.")),
            update = update_rx.next() => {
                let update = update.expect("Must have next update");
                apply_update(&mut chain, &mut graph, update)?;
            },
        }
    }

    let ((_, spk), _) = graph
        .index
        .next_unused_spk(EXTERNAL)
        .expect("must derive spk");
    let txid = env.send(&Address::from_script(&spk, &REGTEST)?, Amount::ONE_BTC)?;

    let timeout = tokio::time::sleep(Duration::from_secs(150)).fuse();
    pin_mut!(timeout);
    loop {
        futures::select! {
            _ = timeout => return Err(anyhow::anyhow!("Timed-out waiting for unconfirmed tx.")),
            update = update_rx.next() => {
                let update = update.expect("Must have next update");
                let has_tx = update.tx_update.txs.iter().any(|tx| tx.compute_txid() == txid);
                apply_update(&mut chain, &mut graph, update)?;
                if has_tx {
                    break;
                }
            },
        }
    }

    let confirm_height = env.rpc_client().get_block_count()? as u32 + 1;
    env.mine_blocks(1, None)?;

    let timeout = tokio::time::sleep(Duration::from_secs(150)).fuse();
    pin_mut!(timeout);
    let anchor = loop {
        futures::select! {
            _ = timeout => return Err(anyhow::anyhow!(
                "Timed-out waiting for anchor: tx is still unconfirmed at tip height {}",
                chain.tip().height(),
            )),
            update = update_rx.next() => {
                let update = update.expect("Must have next update");
                apply_update(&mut chain, &mut graph, update)?;
                let confirmed_anchor = graph
                    .graph()
                    .list_canonical_txs(
                        &chain,
                        chain.tip().block_id(),
                        CanonicalizationParams::default(),
                    )
                    .find(|ctx| ctx.tx_node.txid == txid)
                    .and_then(|ctx| match ctx.chain_position {
                        ChainPosition::Confirmed { anchor, .. } => Some(anchor),
                        ChainPosition::Unconfirmed { .. } => None,
                    });
                if let Some(anchor) = confirmed_anchor {
                    break anchor;
                }
            },
        }
    };
    assert_eq!(anchor.block_id.height, confirm_height);

    client.stop().await?;
    run_handle.await??;

    Ok(())
}

const LIVE_TIMEOUT: Duration = Duration::from_secs(60);

type Graph = IndexedTxGraph<ConfirmationBlockTime, KeychainTxOutIndex<&'static str>>;

type LiveClient = (
    LocalChain,
    Graph,
    mpsc::UnboundedReceiver<Update<&'static str>>,
    AsyncClient<&'static str>,
    tokio::task::JoinHandle<anyhow::Result<()>>,
);

/// A live async client tracking [`DESCRIPTORS`], connected to `env`'s electrs.
fn start_live_client(env: &TestEnv) -> anyhow::Result<LiveClient> {
    let secp = Secp256k1::new();
    let (external, _) = Descriptor::parse_descriptor(&secp, DESCRIPTORS[0])?;
    let (internal, _) = Descriptor::parse_descriptor(&secp, DESCRIPTORS[1])?;

    let graph = IndexedTxGraph::<ConfirmationBlockTime, _>::new({
        let mut indexer = KeychainTxOutIndex::<&'static str>::new(LOOKAHEAD, false);
        indexer.insert_descriptor(EXTERNAL, external.clone())?;
        indexer.insert_descriptor(INTERNAL, internal.clone())?;
        indexer
    });
    let (chain, _cs) = LocalChain::from_genesis_hash(env.genesis_hash()?);

    let mut spk_tracker = DerivedSpkTracker::<&'static str>::new(LOOKAHEAD);
    spk_tracker.insert_descriptor(EXTERNAL, external, 0);
    spk_tracker.insert_descriptor(INTERNAL, internal, 0);

    let mut state = AsyncState::new(
        ReqCoord::default(),
        Cache::default(),
        spk_tracker,
        chain.tip(),
    );

    let (mut update_tx, update_rx) = mpsc::unbounded::<Update<&'static str>>();
    let (client, mut client_rx) = AsyncClient::new();
    let electrum_url = env.electrsd.electrum_url.clone();

    let run_handle = tokio::spawn(async move {
        let mut conn = TcpStream::connect(&electrum_url).await?;
        let (read, write) = conn.split();
        run_async(
            &mut state,
            &mut update_tx,
            &mut client_rx,
            read.compat(),
            write.compat_write(),
        )
        .await?;
        anyhow::Ok(())
    });

    Ok((chain, graph, update_rx, client, run_handle))
}

/// Apply updates until `done`, or fail after [`LIVE_TIMEOUT`].
async fn sync_until(
    update_rx: &mut mpsc::UnboundedReceiver<Update<&'static str>>,
    chain: &mut LocalChain,
    graph: &mut Graph,
    what: &str,
    mut done: impl FnMut(&LocalChain, &Graph) -> bool,
) -> anyhow::Result<()> {
    let timeout = tokio::time::sleep(LIVE_TIMEOUT).fuse();
    pin_mut!(timeout);
    loop {
        if done(chain, graph) {
            return Ok(());
        }
        futures::select! {
            _ = timeout => anyhow::bail!("timed out waiting for {what}"),
            update = update_rx.next() => {
                let update = update.ok_or_else(|| anyhow::anyhow!(
                    "the client stopped while waiting for {what}"
                ))?;
                apply_update(chain, graph, update)?;
            },
        }
    }
}

/// The block `txid` is canonically confirmed in, if it is confirmed at all.
fn confirmed_in(chain: &LocalChain, graph: &Graph, txid: Txid) -> Option<BlockId> {
    graph
        .graph()
        .list_canonical_txs(
            chain,
            chain.tip().block_id(),
            CanonicalizationParams::default(),
        )
        .find(|ctx| ctx.tx_node.txid == txid)
        .and_then(|ctx| match ctx.chain_position {
            ChainPosition::Confirmed { anchor, .. } => Some(anchor.block_id),
            ChainPosition::Unconfirmed { .. } => None,
        })
}

/// Mine `count` blocks and wait for the local chain to reach the resulting tip.
async fn mine_and_sync(
    env: &TestEnv,
    update_rx: &mut mpsc::UnboundedReceiver<Update<&'static str>>,
    chain: &mut LocalChain,
    graph: &mut Graph,
    count: usize,
) -> anyhow::Result<()> {
    env.mine_blocks(count, None)?;
    let height = env.rpc_client().get_block_count()? as u32;
    sync_until(update_rx, chain, graph, "the chain tip", |chain, _| {
        chain.tip().height() >= height
    })
    .await
}

/// Fund a tracked spk and get the tx confirmed, returning its txid and confirming block.
async fn confirm_a_tracked_tx(
    env: &TestEnv,
    update_rx: &mut mpsc::UnboundedReceiver<Update<&'static str>>,
    chain: &mut LocalChain,
    graph: &mut Graph,
) -> anyhow::Result<(Txid, BlockId)> {
    // Coinbase maturity, so that `env.send` has funds to spend.
    mine_and_sync(env, update_rx, chain, graph, 101).await?;

    let ((_, spk), _) = graph
        .index
        .next_unused_spk(EXTERNAL)
        .expect("must derive spk");
    let txid = env.send(&Address::from_script(&spk, &REGTEST)?, Amount::ONE_BTC)?;
    sync_until(update_rx, chain, graph, "the unconfirmed tx", |_, graph| {
        graph.graph().get_tx(txid).is_some()
    })
    .await?;

    mine_and_sync(env, update_rx, chain, graph, 1).await?;
    sync_until(
        update_rx,
        chain,
        graph,
        "the first anchor",
        |chain, graph| confirmed_in(chain, graph, txid).is_some(),
    )
    .await?;

    let block = confirmed_in(chain, graph, txid).expect("just waited for it");
    Ok((txid, block))
}

/// Issue #12 against a real electrs: a reorg moves the tx into a *different* block at the same
/// height, so its script status — a hash over `txid:height:` pairs — does not change and the
/// server has no reason to notify the script hash. The anchor has to be asked for again off the
/// tip update alone.
///
/// The old anchor stays in the graph, and that is the point rather than an oversight: it was
/// never false, the tx really was in that block. bdk scans every anchor a tx has and takes the
/// one in the chain, so the stale one is inert. What matters is that the tx comes back
/// *confirmed via the replacement block*, not merely no longer unconfirmed.
#[tokio::test]
async fn reorg_to_same_height_block_refetches_anchor_live() -> anyhow::Result<()> {
    init();
    let env = TestEnv::new()?;
    let (mut chain, mut graph, mut update_rx, client, run_handle) = start_live_client(&env)?;

    let (txid, original) =
        confirm_a_tracked_tx(&env, &mut update_rx, &mut chain, &mut graph).await?;

    // Invalidating puts the tx back in the mempool and the replacement block mines it again, so
    // it keeps its height and therefore its script status.
    env.invalidate_blocks(1)?;
    let replacement = match env.mine_blocks(1, None)?[..] {
        [hash] => hash,
        ref hashes => panic!("expected one block, got {}", hashes.len()),
    };
    assert_ne!(
        replacement, original.hash,
        "the reorg must replace the block"
    );
    assert!(
        env.rpc_client()
            .get_block(&replacement)?
            .txdata
            .iter()
            .any(|tx| tx.compute_txid() == txid),
        "the replacement block must contain the tx at the same height"
    );

    sync_until(
        &mut update_rx,
        &mut chain,
        &mut graph,
        "the re-asked anchor",
        |chain, graph| {
            confirmed_in(chain, graph, txid).is_some_and(|block| block.hash == replacement)
        },
    )
    .await?;

    assert_eq!(
        confirmed_in(&chain, &graph, txid),
        Some(BlockId {
            height: original.height,
            hash: replacement,
        }),
        "the tx must be confirmed via the replacement block",
    );
    assert!(
        graph
            .graph()
            .all_anchors()
            .get(&txid)
            .is_some_and(|anchors| anchors.iter().any(|a| a.block_id.hash == original.hash)),
        "the anchor to the abandoned block must still be held, and simply ignored",
    );

    client.stop().await?;
    run_handle.await??;
    Ok(())
}

/// The everyday reorg: one that takes a tx out of its block and back to the mempool. The re-ask
/// then asks for a proof that does not exist, and the server answers with a JSON-RPC error — the
/// same shape it uses for a genuine fault. Treating that as one would drop the connection every
/// time a reorg unconfirms a tracked tx.
///
/// Observed here against **Blockstream's esplora-electrs**, which is what `bdk_testenv` runs:
/// `blockchain.transaction.get_merkle` for an unconfirmed tx answers
/// `"tx not found or is unconfirmed"`. Whether romanz/electrs phrases it the same way is not
/// established by this test and should not be claimed from it — the two forks differ, and the
/// handling here does not depend on which one it is.
#[tokio::test]
async fn reorg_unconfirming_a_tx_keeps_the_connection_alive() -> anyhow::Result<()> {
    init();
    let env = TestEnv::new()?;
    let (mut chain, mut graph, mut update_rx, client, run_handle) = start_live_client(&env)?;

    let (txid, original) =
        confirm_a_tracked_tx(&env, &mut update_rx, &mut chain, &mut graph).await?;

    // Empty blocks, so the tx cannot be mined again and stays in the mempool.
    env.invalidate_blocks(1)?;
    env.mine_empty_block()?;
    env.mine_empty_block()?;

    sync_until(
        &mut update_rx,
        &mut chain,
        &mut graph,
        "the tx to become unconfirmed",
        |chain, graph| {
            chain.tip().height() > original.height && confirmed_in(chain, graph, txid).is_none()
        },
    )
    .await?;

    // The connection must still be serving us: a further block has to arrive.
    mine_and_sync(&env, &mut update_rx, &mut chain, &mut graph, 1).await?;

    client.stop().await?;
    run_handle.await??;
    Ok(())
}
