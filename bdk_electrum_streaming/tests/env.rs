use std::{sync::atomic::AtomicBool, time::Duration};

use bdk_chain::{
    keychain_txout::KeychainTxOutIndex, local_chain::LocalChain, CanonicalizationParams,
    ChainPosition, IndexedTxGraph,
};
use bdk_core::{
    bitcoin::{key::Secp256k1, params::REGTEST, Address, Amount},
    ConfirmationBlockTime,
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
            &AtomicBool::new(false),
            &mut update_tx,
            &mut client_rx,
            &run_conn,
            &run_conn,
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
