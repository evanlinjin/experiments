use bitcoin::{consensus::Encodable, hex::DisplayHex, Script, Txid};
use serde::Deserialize;

use crate::{
    ElectrumScriptHash, ElectrumScriptStatus, EstimateFeeResp, FeePair, FullTx, GetBalanceResp,
    HeaderResp, HeadersResp, HeadersSubscribeResp, MempoolTx, RelayFeeResp, RequestMethodAndParams,
    Tx, TxMerkle, TxidFromPos, Utxo,
};

/// The caller-facing [`Request`].
///
/// TODO: check_response(&self, resp: &Self::Response) method.
pub trait Request {
    /// The associated response type of this request.
    type Response: for<'a> Deserialize<'a>;
    fn into_method_and_params(self) -> RequestMethodAndParams;
}

impl Request for RequestMethodAndParams {
    type Response = serde_json::Value;

    fn into_method_and_params(self) -> RequestMethodAndParams {
        self
    }
}

#[derive(Debug, Clone)]
pub struct Header {
    pub height: u32,
    pub cp_height: Option<u32>,
}

impl Request for Header {
    type Response = HeaderResp;
    fn into_method_and_params(self) -> RequestMethodAndParams {
        ("blockchain.block.header".into(), {
            let mut params = vec![self.height.into()];
            if let Some(cp_height) = self.cp_height {
                params.push(cp_height.into());
            }
            params
        })
    }
}

#[derive(Debug, Clone)]
pub struct Headers {
    pub start_height: u32,
    pub count: usize,
    pub cp_height: Option<u32>,
}

impl Request for Headers {
    type Response = HeadersResp;

    fn into_method_and_params(self) -> RequestMethodAndParams {
        ("blockchain.block.headers".into(), {
            let mut params = vec![self.start_height.into(), self.count.into()];
            if let Some(cp_height) = self.cp_height {
                params.push(cp_height.into());
            }
            params
        })
    }
}

#[derive(Debug, Clone)]
pub struct EstimateFee {
    pub number: usize,
}

impl Request for EstimateFee {
    type Response = EstimateFeeResp;

    fn into_method_and_params(self) -> RequestMethodAndParams {
        ("blockchain.estimatefee".into(), vec![self.number.into()])
    }
}

#[derive(Debug, Clone)]
pub struct HeadersSubscribe;

impl Request for HeadersSubscribe {
    type Response = HeadersSubscribeResp;

    fn into_method_and_params(self) -> RequestMethodAndParams {
        ("blockchain.headers.subscribe".into(), vec![])
    }
}

#[derive(Debug, Clone)]
pub struct RelayFee;

impl Request for RelayFee {
    type Response = RelayFeeResp;

    fn into_method_and_params(self) -> RequestMethodAndParams {
        ("blockchain.relayfee".into(), vec![])
    }
}

#[derive(Debug, Clone)]
pub struct GetBalance {
    pub script_hash: ElectrumScriptHash,
}

impl GetBalance {
    pub fn from_script<S: AsRef<Script>>(script: S) -> Self {
        let script_hash = ElectrumScriptHash::new(script.as_ref());
        Self { script_hash }
    }
}

impl Request for GetBalance {
    type Response = GetBalanceResp;

    fn into_method_and_params(self) -> RequestMethodAndParams {
        (
            "blockchain.scripthash.get_balance".into(),
            vec![self.script_hash.to_string().into()],
        )
    }
}

#[derive(Debug, Clone)]
pub struct GetHistory {
    pub script_hash: ElectrumScriptHash,
}

impl GetHistory {
    pub fn from_script<S: AsRef<Script>>(script: S) -> Self {
        let script_hash = ElectrumScriptHash::new(script.as_ref());
        Self { script_hash }
    }
}

impl Request for GetHistory {
    type Response = Vec<Tx>;

    fn into_method_and_params(self) -> RequestMethodAndParams {
        (
            "blockchain.scripthash.get_history".into(),
            vec![self.script_hash.to_string().into()],
        )
    }
}

/// Resource request for `blockchain.scripthash.get_mempool`.
///
/// Note that `electrs` does not support this endpoint.
#[derive(Debug, Clone)]
pub struct GetMempool {
    pub script_hash: ElectrumScriptHash,
}

impl GetMempool {
    pub fn from_script<S: AsRef<Script>>(script: S) -> Self {
        let script_hash = ElectrumScriptHash::new(script.as_ref());
        Self { script_hash }
    }
}

impl Request for GetMempool {
    // TODO: Dedicated type.
    type Response = Vec<MempoolTx>;

    fn into_method_and_params(self) -> RequestMethodAndParams {
        (
            "blockchain.scripthash.get_mempool".into(),
            vec![self.script_hash.to_string().into()],
        )
    }
}

#[derive(Debug, Clone)]
pub struct ListUnspent {
    pub script_hash: ElectrumScriptHash,
}

impl ListUnspent {
    pub fn from_script<S: AsRef<Script>>(script: S) -> Self {
        let script_hash = ElectrumScriptHash::new(script.as_ref());
        Self { script_hash }
    }
}

impl Request for ListUnspent {
    type Response = Vec<Utxo>;

    fn into_method_and_params(self) -> RequestMethodAndParams {
        (
            "blockchain.scripthash.listunspent".into(),
            vec![self.script_hash.to_string().into()],
        )
    }
}

#[derive(Debug, Clone)]
pub struct ScriptHashSubscribe {
    pub script_hash: ElectrumScriptHash,
}

impl ScriptHashSubscribe {
    pub fn from_script<S: AsRef<Script>>(script: S) -> Self {
        let script_hash = ElectrumScriptHash::new(script.as_ref());
        Self { script_hash }
    }
}

impl Request for ScriptHashSubscribe {
    type Response = Option<ElectrumScriptStatus>;

    fn into_method_and_params(self) -> RequestMethodAndParams {
        (
            "blockchain.scripthash.subscribe".into(),
            vec![self.script_hash.to_string().into()],
        )
    }
}

#[derive(Debug, Clone)]
pub struct ScriptHashUnsubscribe {
    pub script_hash: ElectrumScriptHash,
}

impl ScriptHashUnsubscribe {
    pub fn from_script<S: AsRef<Script>>(script: S) -> Self {
        let script_hash = ElectrumScriptHash::new(script.as_ref());
        Self { script_hash }
    }
}

impl Request for ScriptHashUnsubscribe {
    type Response = bool;

    fn into_method_and_params(self) -> RequestMethodAndParams {
        (
            "blockchain.scripthash.unsubscribe".into(),
            vec![self.script_hash.to_string().into()],
        )
    }
}

#[derive(Debug, Clone)]
pub struct BroadcastTx(pub bitcoin::Transaction);

impl Request for BroadcastTx {
    type Response = bitcoin::Txid;

    fn into_method_and_params(self) -> RequestMethodAndParams {
        let mut tx_bytes = Vec::<u8>::new();
        self.0.consensus_encode(&mut tx_bytes).expect("must encode");
        (
            "blockchain.transaction.broadcast".into(),
            vec![tx_bytes.to_lower_hex_string().into()],
        )
    }
}

pub struct GetTx(pub bitcoin::Txid);

impl Request for GetTx {
    type Response = FullTx;

    fn into_method_and_params(self) -> RequestMethodAndParams {
        (
            "blockchain.transaction.get".into(),
            vec![self.0.to_string().into()],
        )
    }
}

#[derive(Debug, Clone)]
pub struct GetTxMerkle {
    pub txid: Txid,
    pub height: u32,
}

impl Request for GetTxMerkle {
    type Response = TxMerkle;

    fn into_method_and_params(self) -> RequestMethodAndParams {
        (
            "blockchain.transaction.get_merkle".into(),
            vec![self.txid.to_string().into(), self.height.into()],
        )
    }
}

#[derive(Debug, Clone)]
pub struct GetTxidFromPos {
    pub height: u32,
    pub tx_pos: usize,
}

impl Request for GetTxidFromPos {
    type Response = TxidFromPos;

    fn into_method_and_params(self) -> RequestMethodAndParams {
        (
            "blockchain.transaction.id_from_pos".into(),
            vec![self.height.into(), self.tx_pos.into()],
        )
    }
}

#[derive(Debug, Clone)]
pub struct GetFeeHistogram;

impl Request for GetFeeHistogram {
    type Response = Vec<FeePair>;

    fn into_method_and_params(self) -> RequestMethodAndParams {
        ("mempool.get_fee_histogram".into(), vec![])
    }
}

#[derive(Debug, Clone)]
pub struct Banner;

impl Request for Banner {
    type Response = String;

    fn into_method_and_params(self) -> RequestMethodAndParams {
        ("server.banner".into(), vec![])
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Ping;

impl Request for Ping {
    type Response = ();

    fn into_method_and_params(self) -> RequestMethodAndParams {
        ("server.ping".into(), vec![])
    }
}
