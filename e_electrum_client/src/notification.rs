use serde::Deserialize;
use serde_json::Value;

use crate::{
    CowStr, ElectrumScriptHash, ElectrumScriptStatus, HeadersSubscribeResp,
    NotificationMethodAndParams,
};

#[derive(Debug, Clone)]
pub enum Notification {
    Header(HeaderNotification),
    ScriptHash(ScriptHashNotification),
    Unknown { method: CowStr, params: Value },
}

impl TryFrom<NotificationMethodAndParams> for Notification {
    type Error = serde_json::Error;

    fn try_from((method, params): NotificationMethodAndParams) -> Result<Self, Self::Error> {
        match method.as_ref() {
            "blockchain.headers.subscribe" => {
                let inner = HeaderNotification::deserialize(params)?;
                Ok(Self::Header(inner))
            }
            "blockchain.scripthash.subscribe" => {
                let inner = ScriptHashNotification::deserialize(params)?;
                Ok(Self::ScriptHash(inner))
            }
            _ => Ok(Self::Unknown { method, params }),
        }
    }
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct HeaderNotification {
    param_0: HeadersSubscribeResp,
}

impl HeaderNotification {
    pub fn height(&self) -> u32 {
        self.param_0.height
    }

    pub fn header(&self) -> &bitcoin::block::Header {
        &self.param_0.header
    }
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct ScriptHashNotification {
    param_0: ElectrumScriptHash,
    param_1: ElectrumScriptStatus,
}

impl ScriptHashNotification {
    pub fn script_hash(&self) -> ElectrumScriptHash {
        self.param_0
    }

    pub fn script_status(&self) -> ElectrumScriptStatus {
        self.param_1
    }
}
