use std::{
    collections::VecDeque,
    time::{Duration, Instant},
};

use bdk_chain::{
    keychain_txout::KeychainTxOutIndex, local_chain::LocalChain, CanonicalizationParams,
    IndexedTxGraph,
};
use bdk_core::{
    bitcoin::{key::Secp256k1, params::REGTEST, Address},
    ConfirmationBlockTime,
};
use bdk_electrum_streaming::{Cache, DerivedSpkTracker, ReqCoord, State, Update};
use bdk_testenv::{utils::DESCRIPTORS, TestEnv};
use electrum_streaming_client::RawRequest;
use futures::{pin_mut, FutureExt, SinkExt, StreamExt};
use miniscript::Descriptor;
use tokio::net::TcpStream;

const EXTERNAL: &str = "external";
const INTERNAL: &str = "internal";
const LOOKAHEAD: u32 = 21;

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
    }
    Ok(())
}

#[tokio::test]
async fn env() -> anyhow::Result<()> {
    let secp = Secp256k1::new();
    let env = TestEnv::new()?;
    let electrum_url = env.electrsd.electrum_url.clone();

    let (external, _external_keys) = Descriptor::parse_descriptor(&secp, DESCRIPTORS[0])?;
    let (internal, _internal_keys) = Descriptor::parse_descriptor(&secp, DESCRIPTORS[1])?;

    let mut graph = IndexedTxGraph::<ConfirmationBlockTime, _>::new({
        let mut indexer = KeychainTxOutIndex::<&'static str>::new(LOOKAHEAD);
        indexer.insert_descriptor(EXTERNAL, external.clone())?;
        indexer.insert_descriptor(INTERNAL, internal.clone())?;
        indexer
    });
    let (mut chain, _cs) = LocalChain::from_genesis_hash(env.genesis_hash()?);

    let mut req_queue = VecDeque::<RawRequest>::new();

    let coord = ReqCoord::new(0);
    let cache = Cache::default();
    let mut spk_tracker = DerivedSpkTracker::<&'static str>::new(LOOKAHEAD);
    spk_tracker.insert_descriptor(EXTERNAL, external, 0);
    spk_tracker.insert_descriptor(INTERNAL, internal, 0);

    let mut state = State::new(&mut req_queue, coord, cache, spk_tracker, chain.tip());

    let (mut tx, mut rx) = futures::channel::mpsc::unbounded::<Update<&'static str>>();

    let _src_handle = tokio::spawn(async move {
        let mut conn = TcpStream::connect(&electrum_url).await?;
        let (read, mut write) = conn.split();
        let read = tokio::io::BufReader::new(read);
        let read_stream = electrum_streaming_client::io::ReadStreamer::new_tokio(read);
        pin_mut!(read_stream);

        let start = Instant::now();

        loop {
            println!("[LOOP] Elapsed: {}s", start.elapsed().as_secs_f32());
            for req in core::mem::take(&mut req_queue) {
                println!("WRITE: {req:?}");
                electrum_streaming_client::io::tokio_write(&mut write, req).await?;
            }
            println!("[LOOP] Waiting for incoming...");
            // TODO: Handle read/write separately.
            let incoming_obj = match read_stream.next().await {
                Some(result) => result?,
                None => break,
            };
            println!("[LOOP] GOT: {incoming_obj:?}");
            if let Some(update) = state.advance(&mut req_queue, incoming_obj)? {
                if let Err(err) = tx.send(update).await {
                    if err.is_disconnected() {
                        println!("It's disconnected!");
                        break;
                    }
                    if err.is_full() {
                        println!("It's full!");
                        return Err(anyhow::anyhow!(err));
                    }
                }
            }
        }
        anyhow::Ok(())
    });

    // First block update (genesis).
    let update = rx.next().await.expect("Must have next update");
    apply_update(&mut chain, &mut graph, update)?;

    let ((_, spk), _) = graph
        .index
        .next_unused_spk(EXTERNAL)
        .expect("must derive spk");
    env.mine_blocks(101, Some(Address::from_script(&spk, &REGTEST)?))?;
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Wait until we sync up to block 101
    let timeout = tokio::time::sleep(Duration::from_secs(5)).fuse();
    pin_mut!(timeout);

    loop {
        futures::select! {
            _ = timeout => return Err(anyhow::anyhow!("Timed-out waiting for chain sync.")),
            update = rx.next() => {
                let update = update.expect("Must have next update");
                let has_tx_update = !update.tx_update.txs.is_empty();
                apply_update(&mut chain, &mut graph, update)?;
                if has_tx_update {
                    break;
                }
            },
        }
    }

    // // We should get spk updates now...
    // let update = rx.next().await.expect("Must get update");
    // panic!("update: {:?}", update);
    // apply_update(&mut chain, &mut graph, update)?;

    let balance = graph.graph().balance(
        &chain,
        chain.tip().block_id(),
        CanonicalizationParams::default(),
        graph.index.outpoints().clone(),
        |(k, _), _| *k == EXTERNAL,
    );
    println!("BALANCE: {}", balance);

    rx.close();

    Ok(())
}
