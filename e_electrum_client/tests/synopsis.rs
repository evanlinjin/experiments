use std::time::Duration;

use async_std::net::TcpStream;
use bdk_testenv::{anyhow, bitcoincore_rpc::RpcApi, TestEnv};
use bitcoin::Amount;
use e_electrum_client::{self, request, Notification, Service};
use futures::{executor::block_on, StreamExt};

#[test]
fn synopsis() -> anyhow::Result<()> {
    let env = TestEnv::new()?;
    let electrum_addr = env.electrsd.electrum_url.clone();
    println!("URL: {}", electrum_addr);

    let addr = env
        .rpc_client()
        .get_new_address(None, None)?
        .assume_checked();

    let mut service = Service::new();
    let mut notification_recv = service.spawn_notification_receiver();
    let client = service.spawn_client();

    let service_thread =
        std::thread::spawn(move || service_thread(service, electrum_addr.as_str()));

    let headers_sub_resp = block_on(client.call(request::HeadersSubscribe))?;
    println!("HEADERS SUBSCRIBE RESPONSE: {:?}", headers_sub_resp);

    let script_sub_resp = block_on(client.call(request::ScriptHashSubscribe::from_script(
        addr.script_pubkey(),
    )))?;
    println!("SCRIPTHASH SUBSCRIBE RESPONSE: {:?}", script_sub_resp);

    const TO_MINE: usize = 3;
    let blockhashes = env.mine_blocks(TO_MINE, Some(addr.clone()))?;
    println!("MINED: {:?}", blockhashes);
    loop {
        let notification_method_and_params =
            block_on(notification_recv.next()).expect("service must still be running");
        println!("GOT RAW NOTIFICATION: {:?}", notification_method_and_params);
        let notification: Notification = notification_method_and_params.try_into()?;
        match notification {
            Notification::Header(inner) => {
                println!(
                    "GOT HEADER NOTIFICATION: {} {:?}",
                    inner.height(),
                    inner.header()
                );
                if inner.height() > TO_MINE as u32 {
                    break;
                }
            }
            Notification::ScriptHash(obj) => {
                println!(
                    "WE DON'T KNOW WHAT TO DO WITH SCRIPTHASH NOTIFICATION YET! {:?}",
                    obj
                );
            }
            Notification::Unknown { method, params } => {
                println!("WHAT NOTIFICATION TYPE IS THIS? {} : {}", method, params)
            }
        }
    }

    let header_resp = block_on(client.call(request::Header {
        height: 3,
        cp_height: Some(3),
    }))?;
    println!("GOT HEADER AT HEIGHT 3: {:?}", header_resp);
    let exp_header = {
        let blockhash = env.rpc_client().get_block_hash(3)?;
        env.rpc_client().get_block_header(&blockhash)?
    };
    assert_eq!(header_resp.as_header(), &exp_header);

    let headers_resp = block_on(client.call(request::Headers {
        start_height: 1,
        count: 2,
        cp_height: Some(3),
    }))?;
    println!("GOT HEADERS: {:?}", headers_resp);

    // Make unconfirmed balance.
    env.mine_blocks(101, Some(addr.clone()))?; // create spendable balance
    let txid = env.rpc_client().send_to_address(
        &addr,
        Amount::from_btc(1.0).unwrap(),
        None,
        None,
        None,
        None,
        None,
        None,
    )?;
    env.wait_until_electrum_sees_txid(txid, Duration::from_secs(10))?;
    let tx = block_on(client.call(request::GetTx(txid)))?;
    println!("GOT TX: {:?}", tx);

    let broadcast_resp = block_on(client.call(request::BroadcastTx(tx.tx)))?;
    println!("BROADCAST RESULT: {}", broadcast_resp);

    println!("try get BALANCE RESP!");
    let balance_resp =
        block_on(client.call(request::GetBalance::from_script(addr.script_pubkey())))?;
    println!("GET BALANCE RESP: {:?}", balance_resp);

    let history = block_on(client.call(request::GetHistory::from_script(addr.script_pubkey())))?;
    println!(
        "GET HISTORY RESP: first = {:?} last = {:?}",
        history.first().unwrap(),
        history.last().unwrap()
    );

    let block_hash = env.mine_blocks(1, None)?.first().copied().unwrap();
    let block_height = env.rpc_client().get_block_info(&block_hash)?.height as u32;
    env.wait_until_electrum_sees_block(Duration::from_secs(5))?; // TODO: not good.
    let tx_merkle = block_on(client.call(request::GetTxMerkle {
        txid,
        height: block_height,
    }))?;
    println!("GET MERKLE: {:?}", tx_merkle);

    let from_pos = block_on(client.call(request::GetTxidFromPos {
        height: block_height,
        tx_pos: tx_merkle.pos,
    }))?;
    println!("TXID FROM POS: {}", from_pos.txid);

    // NOTE: This does not work with `electrs`
    // let mempool_history =
    //     block_on(client.call(request::GetMempool::from_script(addr.script_pubkey())));
    // println!("GET MEMPOOL RESP: {:?}", mempool_history);

    let utxos = block_on(client.call(request::ListUnspent::from_script(addr.script_pubkey())))?;
    println!(
        "GET UTXOs: first = {:?} last = {:?}",
        utxos.first().unwrap(),
        utxos.last().unwrap()
    );

    // NOTE: This does not work with our version of `electrs`
    // let unsub_resp = block_on(client.call(request::ScriptHashUnsubscribe::from_script(
    //     addr.script_pubkey(),
    // )));
    // println!("UNSUB RESP: {:?}", unsub_resp);

    let fee_histogram = block_on(client.call(request::GetFeeHistogram))?;
    println!("FEE HISTOGRAM: {:?}", fee_histogram);

    let server_banner = block_on(client.call(request::Banner))?;
    println!("SERVER BANNER: {}", server_banner);

    block_on(client.call(request::Ping))?;
    println!("PING SUCCESS!");

    if !client.stop_service() {
        println!("service already stopped?");
    }
    service_thread
        .join()
        .expect("service thread must not panic")?;

    // Get remaining notifications
    block_on(async {
        while let Some(a) = notification_recv.next().await {
            let notification = Notification::try_from(a).expect("ARR");
            println!("NOTIFICATION: {:?}", notification);
        }
    });

    Ok(())
}

fn service_thread(mut service: Service, addr: &str) -> Result<(), std::io::Error> {
    block_on(async {
        let conn = TcpStream::connect(addr).await?;
        service.run(conn).await.map_err(|err| {
            println!("SERVICE ENDED WITH ERROR: {}", err);
            err
        })?;
        Ok(())
    })
}
