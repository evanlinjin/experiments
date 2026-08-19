use std::{sync::atomic::AtomicBool, time::Duration};

use bdk_chain::{
    keychain_txout::KeychainTxOutIndex, local_chain::LocalChain, CanonicalizationParams,
    ChainPosition, IndexedTxGraph,
};
use bdk_core::{
    bitcoin::{key::Secp256k1, params::REGTEST, Address, Amount, BlockHash, Txid},
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
        run_blocking(
            &mut state,
            &AtomicBool::new(false),
            &mut update_tx,
            &mut client_rx,
            &run_conn,
            &run_conn,
        )
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

type Graph = IndexedTxGraph<ConfirmationBlockTime, KeychainTxOutIndex<&'static str>>;

/// The anchor `txid` is canonically confirmed at, if it is confirmed at all.
fn canonical_anchor(
    chain: &LocalChain,
    graph: &Graph,
    txid: Txid,
) -> Option<ConfirmationBlockTime> {
    graph
        .graph()
        .list_canonical_txs(
            chain,
            chain.tip().block_id(),
            CanonicalizationParams::default(),
        )
        .find(|ctx| ctx.tx_node.txid == txid)
        .and_then(|ctx| match ctx.chain_position {
            ChainPosition::Confirmed { anchor, .. } => Some(anchor),
            ChainPosition::Unconfirmed { .. } => None,
        })
}

/// A live client against a fresh `electrsd`, with the machinery the reorg tests share.
struct LiveWallet {
    env: TestEnv,
    chain: LocalChain,
    graph: Graph,
    update_rx: mpsc::UnboundedReceiver<Update<&'static str>>,
    client: AsyncClient<&'static str>,
    run_handle: tokio::task::JoinHandle<anyhow::Result<()>>,
}

impl LiveWallet {
    /// Connect to a fresh test environment and apply the first (genesis) update.
    async fn new() -> anyhow::Result<Self> {
        init();

        let secp = Secp256k1::new();
        let env = TestEnv::new()?;
        let electrum_url = env.electrsd.electrum_url.clone();

        let (external, _) = Descriptor::parse_descriptor(&secp, DESCRIPTORS[0])?;
        let (internal, _) = Descriptor::parse_descriptor(&secp, DESCRIPTORS[1])?;

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

        Ok(Self {
            env,
            chain,
            graph,
            update_rx,
            client,
            run_handle,
        })
    }

    /// Apply updates until `f` holds.
    ///
    /// Errors if the client stops — which is what a connection torn down by an expected server
    /// error looks like from here — or if `f` has not held within the timeout.
    async fn wait_until(
        &mut self,
        what: &str,
        mut f: impl FnMut(&LocalChain, &Graph) -> bool,
    ) -> anyhow::Result<()> {
        let timeout = tokio::time::sleep(Duration::from_secs(150)).fuse();
        pin_mut!(timeout);
        loop {
            if f(&self.chain, &self.graph) {
                return Ok(());
            }
            futures::select! {
                _ = timeout => return Err(anyhow::anyhow!("timed out waiting for {what}")),
                update = self.update_rx.next() => {
                    let update = update.ok_or_else(|| {
                        anyhow::anyhow!("the client stopped while waiting for {what}")
                    })?;
                    apply_update(&mut self.chain, &mut self.graph, update)?;
                },
            }
        }
    }

    /// Mine past coinbase maturity, then send `Amount::ONE_BTC` to a tracked spk and mine it in.
    ///
    /// Returns the txid and the hash of the block confirming it.
    async fn confirm_tracked_tx(&mut self) -> anyhow::Result<(Txid, BlockHash)> {
        self.env.mine_blocks(101, None)?;
        let premine_height = self.env.rpc_client().get_block_count()? as u32;
        self.wait_until("the premined chain", |chain, _| {
            chain.tip().height() >= premine_height
        })
        .await?;

        let ((_, spk), _) = self
            .graph
            .index
            .next_unused_spk(EXTERNAL)
            .expect("must derive spk");
        let txid = self
            .env
            .send(&Address::from_script(&spk, &REGTEST)?, Amount::ONE_BTC)?;
        self.wait_until("the unconfirmed tx", |_, graph| {
            graph.graph().get_tx(txid).is_some()
        })
        .await?;

        self.env.mine_blocks(1, None)?;
        self.wait_until("the tx to be anchored", |chain, graph| {
            canonical_anchor(chain, graph, txid).is_some()
        })
        .await?;

        let anchor = canonical_anchor(&self.chain, &self.graph, txid).expect("just waited for it");
        Ok((txid, anchor.block_id.hash))
    }

    async fn stop(self) -> anyhow::Result<()> {
        self.client.stop().await?;
        self.run_handle.await??;
        Ok(())
    }
}

/// Issue #12, end to end: a reorg re-mines a confirmed tx into a *different* block at the *same*
/// height. The Electrum script status is a hash over txid-height pairs, so it is unchanged and no
/// script hash notification is sent. The anchor must still be refetched off the tip alone.
#[tokio::test]
async fn reorg_to_same_height_block_refetches_anchor_live() -> anyhow::Result<()> {
    let mut w = LiveWallet::new().await?;
    let (txid, first_block) = w.confirm_tracked_tx().await?;
    let confirm_height = w.env.rpc_client().get_block_count()? as u32;

    // Invalidate the confirming block and re-mine at the same height. The tx is back in the
    // mempool, so it goes into the replacement block too.
    w.env.reorg(1)?;
    let second_block = w.env.rpc_client().get_best_block_hash()?;
    assert_ne!(
        first_block, second_block,
        "the reorg must actually replace the block"
    );
    assert_eq!(
        w.env.rpc_client().get_block_count()? as u32,
        confirm_height,
        "the replacement block must be at the same height"
    );
    assert!(
        w.env
            .rpc_client()
            .get_block(&second_block)?
            .txdata
            .iter()
            .any(|tx| tx.compute_txid() == txid),
        "the replacement block must still contain the tx"
    );

    w.wait_until("the refetched anchor", |chain, graph| {
        canonical_anchor(chain, graph, txid).is_some_and(|a| a.block_id.hash == second_block)
    })
    .await?;

    let anchor = canonical_anchor(&w.chain, &w.graph, txid).expect("just waited for it");
    assert_eq!(anchor.block_id.height, confirm_height);

    w.stop().await
}

/// The everyday reorg: one which takes a tx out of its block and back to the mempool. The anchor
/// refetch then asks for a proof the server cannot give, and that error must not take the
/// connection down with it.
#[tokio::test]
async fn reorg_unconfirming_a_tx_keeps_the_connection_alive() -> anyhow::Result<()> {
    let mut w = LiveWallet::new().await?;
    let (txid, _) = w.confirm_tracked_tx().await?;
    let confirm_height = w.env.rpc_client().get_block_count()? as u32;

    // Invalidate the confirming block and replace it with empty ones, so the tx cannot be
    // re-mined and the server has no proof to give at that height.
    w.env.invalidate_blocks(1)?;
    w.env.mine_empty_block()?;
    w.env.mine_empty_block()?;
    let tip_height = w.env.rpc_client().get_block_count()? as u32;
    assert_eq!(tip_height, confirm_height + 1);
    assert!(
        w.env.rpc_client().get_raw_mempool()?.contains(&txid),
        "the tx must be back in the mempool, so the server really has no proof for it"
    );

    // The connection has to keep serving: `wait_until` fails if the client stops.
    w.wait_until("the chain tip after the reorg", |chain, _| {
        chain.tip().height() >= tip_height
    })
    .await?;

    assert!(
        canonical_anchor(&w.chain, &w.graph, txid).is_none(),
        "the tx must no longer be canonically confirmed"
    );

    w.stop().await
}
