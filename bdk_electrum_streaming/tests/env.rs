use std::{sync::atomic::AtomicBool, time::Duration};

use std::collections::BTreeMap;

use bdk_chain::{
    keychain_txout::KeychainTxOutIndex, local_chain::LocalChain, ChainPosition, IndexedTxGraph,
};
use bdk_core::bitcoin::{
    block::Header, constants::genesis_block, key::Secp256k1, params::REGTEST, Address, Amount,
    BlockHash, Network, Txid,
};
use bdk_electrum_streaming::{
    run_async, run_blocking, AsyncClient, AsyncState, BlockingClient, BlockingState, Cache,
    DerivedSpkTracker, HeaderChain, ProvenAnchor, ReqCoord, Update,
};
use bdk_testenv::{electrsd::electrum_client::ElectrumApi, utils::DESCRIPTORS, TestEnv};
use futures::{channel::mpsc, pin_mut, FutureExt, StreamExt};
use miniscript::Descriptor;
use tokio::net::TcpStream;
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};
use tracing::Level;

const EXTERNAL: &str = "external";
const INTERNAL: &str = "internal";
const LOOKAHEAD: u32 = 6;

type Graph = IndexedTxGraph<ProvenAnchor, KeychainTxOutIndex<&'static str>>;

fn init() {
    let _ = tracing_subscriber::fmt()
        .with_test_writer()
        .with_max_level(Level::TRACE)
        .try_init();
}

fn genesis_header() -> Header {
    genesis_block(&REGTEST).header
}

fn apply_update(
    chain: &mut LocalChain<Header>,
    graph: &mut Graph,
    update: Update<&'static str>,
) -> anyhow::Result<()> {
    let _ = graph
        .index
        .reveal_to_target_multi(&update.last_active_indices);
    let _ = graph.apply_update(update.tx_update);
    let cp = update.chain_update.expect("NO CHAIN UPDATE!");
    chain.apply_update(cp)?;
    Ok(())
}

/// Set up an indexer/graph/chain trio plus the spk tracker fed to the client.
fn setup() -> anyhow::Result<(Graph, LocalChain<Header>, DerivedSpkTracker<&'static str>)> {
    let secp = Secp256k1::new();
    let (external, _) = Descriptor::parse_descriptor(&secp, DESCRIPTORS[0])?;
    let (internal, _) = Descriptor::parse_descriptor(&secp, DESCRIPTORS[1])?;

    let graph = IndexedTxGraph::new({
        let mut indexer = KeychainTxOutIndex::<&'static str>::new(LOOKAHEAD, false);
        indexer.insert_descriptor(EXTERNAL, external.clone())?;
        indexer.insert_descriptor(INTERNAL, internal.clone())?;
        indexer
    });
    let (chain, _) = LocalChain::from_genesis(genesis_header());

    let mut spk_tracker = DerivedSpkTracker::<&'static str>::new(LOOKAHEAD);
    spk_tracker.insert_descriptor(EXTERNAL, external, 0);
    spk_tracker.insert_descriptor(INTERNAL, internal, 0);

    Ok((graph, chain, spk_tracker))
}

fn confirmed_balance(chain: &LocalChain<Header>, graph: &Graph) -> bdk_chain::Balance {
    // `LocalChain` only canonicalizes over blockhashes for now, so drop the headers.
    let chain = LocalChain::from_blocks(
        chain
            .iter_checkpoints()
            .map(|cp| (cp.height(), cp.hash()))
            .collect::<BTreeMap<_, _>>(),
    )
    .expect("must build blockhash chain");
    chain
        .canonical_view(graph.graph(), chain.tip().block_id(), Default::default())
        .balance(
            graph.index.outpoints().clone(),
            |(k, _), _| *k == INTERNAL,
            0,
        )
}

#[test]
fn blocking_env() -> anyhow::Result<()> {
    init();

    let env = TestEnv::new()?;
    let electrum_url = env.electrsd.electrum_url.clone();
    let (mut graph, mut chain, spk_tracker) = setup()?;

    let mut state = BlockingState::new(
        ReqCoord::default(),
        Cache::default(),
        spk_tracker,
        HeaderChain::new(Network::Regtest, [(0, genesis_header())])?,
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

    println!("BALANCE: {}", confirmed_balance(&chain, &graph));

    // TODO: Figure out a way to stop the thread without having to close the connection.
    conn.shutdown(std::net::Shutdown::Both)?;
    client.stop()?;

    run_handle.join().expect("must join")?;
    Ok(())
}

#[tokio::test]
async fn env() -> anyhow::Result<()> {
    init();

    let env = TestEnv::new()?;
    let electrum_url = env.electrsd.electrum_url.clone();
    let (mut graph, mut chain, spk_tracker) = setup()?;

    let mut state = AsyncState::new(
        ReqCoord::default(),
        Cache::default(),
        spk_tracker,
        HeaderChain::new(Network::Regtest, [(0, genesis_header())])?,
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

    println!("BALANCE: {}", confirmed_balance(&chain, &graph));

    client.stop().await?;
    run_handle.await??;

    Ok(())
}

/// Trust only the tip, so the wallet's whole history sits below the sync start and can only be
/// anchored by backfilling headers from a trusted block below it.
#[tokio::test]
async fn backfills_history_below_the_sync_start() -> anyhow::Result<()> {
    init();

    let env = TestEnv::new()?;
    let electrum_url = env.electrsd.electrum_url.clone();
    let (mut graph, mut chain, spk_tracker) = setup()?;

    // Mine everything *before* the client ever connects.
    let ((_, spk), _) = graph
        .index
        .next_unused_spk(EXTERNAL)
        .expect("must derive spk");
    env.mine_blocks(101, Some(Address::from_script(&spk, &REGTEST)?))?;
    env.wait_until_electrum_sees_block(Duration::from_secs(30))?;

    // The user vouches for this block; take the header from the server but hold it to the hash.
    let trusted_height = 101_u32;
    let trusted_header = env
        .electrum_client()
        .block_header(trusted_height as usize)?;
    let trusted_hash: BlockHash = env.get_block_hash(trusted_height as u64)?;
    assert_eq!(trusted_header.block_hash(), trusted_hash);

    let mut state = AsyncState::new(
        ReqCoord::default(),
        Cache::default(),
        spk_tracker,
        HeaderChain::new(Network::Regtest, [(trusted_height, trusted_header)])?,
    );
    assert_eq!(
        state.chain().base_height(),
        trusted_height + 1,
        "sync starts one block above the highest trusted block",
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

    let timeout = tokio::time::sleep(Duration::from_secs(150)).fuse();
    pin_mut!(timeout);

    let anchor = loop {
        futures::select! {
            _ = timeout => return Err(anyhow::anyhow!("Timed-out waiting for the backfill.")),
            update = update_rx.next() => {
                let update = update.expect("Must have next update");
                let anchor = update.tx_update.anchors.iter().next().cloned();
                apply_update(&mut chain, &mut graph, update)?;
                if let Some((anchor, _)) = anchor {
                    break anchor;
                }
            },
        }
    };

    assert!(
        anchor.block_id.height <= trusted_height,
        "the coinbase we found is below the sync start, at {}",
        anchor.block_id.height,
    );
    assert!(
        !anchor.merkle.is_empty() || anchor.pos == 0,
        "the anchor carries its merkle proof",
    );
    assert_eq!(
        chain.get(anchor.block_id.height).map(|cp| cp.hash()),
        Some(anchor.block_id.hash),
        "the backfilled header made it into the chain update",
    );
    assert!(confirmed_balance(&chain, &graph).confirmed.to_sat() > 0);

    client.stop().await?;
    run_handle.await??;
    Ok(())
}

/// The anchor a canonical, confirmed `txid` has — `None` if it is unconfirmed or unknown.
fn canonical_anchor(chain: &LocalChain<Header>, graph: &Graph, txid: Txid) -> Option<ProvenAnchor> {
    // `LocalChain` only canonicalizes over blockhashes for now, so drop the headers.
    let chain = LocalChain::from_blocks(
        chain
            .iter_checkpoints()
            .map(|cp| (cp.height(), cp.hash()))
            .collect::<BTreeMap<_, _>>(),
    )
    .expect("must build blockhash chain");
    let view = chain.canonical_view(graph.graph(), chain.tip().block_id(), Default::default());
    match view.tx(txid)?.pos {
        ChainPosition::Confirmed { anchor, .. } => Some(anchor),
        ChainPosition::Unconfirmed { .. } => None,
    }
}

/// A live client against a fresh `electrsd`, with the machinery the reorg tests share.
struct LiveWallet {
    env: TestEnv,
    chain: LocalChain<Header>,
    graph: Graph,
    update_rx: mpsc::UnboundedReceiver<Update<&'static str>>,
    client: AsyncClient<&'static str>,
    run_handle: tokio::task::JoinHandle<anyhow::Result<()>>,
}

impl LiveWallet {
    /// Connect to a fresh test environment and apply the first (genesis) update.
    async fn new() -> anyhow::Result<Self> {
        init();

        let env = TestEnv::new()?;
        let electrum_url = env.electrsd.electrum_url.clone();
        let (mut graph, mut chain, spk_tracker) = setup()?;

        let mut state = AsyncState::new(
            ReqCoord::default(),
            Cache::default(),
            spk_tracker,
            HeaderChain::new(Network::Regtest, [(0, genesis_header())])?,
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

    fn block_count(&self) -> anyhow::Result<u32> {
        Ok(self.env.rpc_client().get_block_count()?.0 as u32)
    }

    /// Apply updates until `f` holds.
    ///
    /// Errors if the client stops — which is what a connection torn down by an expected server
    /// error looks like from here — or if `f` has not held within the timeout.
    async fn wait_until(
        &mut self,
        what: &str,
        mut f: impl FnMut(&LocalChain<Header>, &Graph) -> bool,
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
        let premine_height = self.block_count()?;
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
///
/// The fork also has to out-work the chain it replaces, or the verified chain will not take it —
/// so the replacement run is one block longer, which is what makes a real node switch too.
#[tokio::test]
async fn reorg_to_same_height_block_refetches_anchor_live() -> anyhow::Result<()> {
    let mut w = LiveWallet::new().await?;
    let (txid, first_block) = w.confirm_tracked_tx().await?;
    let confirm_height = w.block_count()?;

    // Invalidate the confirming block and re-mine at the same height. The tx is back in the
    // mempool, so it goes into the replacement block too. One extra block gives the fork the
    // work it needs to be adopted.
    w.env.reorg(1)?;
    w.env.mine_empty_block()?;
    let second_block = w
        .env
        .rpc_client()
        .get_block_hash(confirm_height as u64)?
        .block_hash()?;
    assert_ne!(
        first_block, second_block,
        "the reorg must actually replace the block"
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
    let confirm_height = w.block_count()?;

    // Invalidate the confirming block and replace it with empty ones, so the tx cannot be
    // re-mined and the server has no proof to give at that height.
    w.env.invalidate_blocks(1)?;
    w.env.mine_empty_block()?;
    w.env.mine_empty_block()?;
    let tip_height = w.block_count()?;
    assert_eq!(tip_height, confirm_height + 1);

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
