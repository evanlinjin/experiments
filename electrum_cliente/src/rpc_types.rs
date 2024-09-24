use crate::{CowStr, JSONRPC_VERSION_2_0};
use serde_json::Value;
use std::fmt::{self};

#[derive(Debug, Clone, serde::Serialize)]
pub struct OutgoingObject {
    pub jsonrpc: CowStr,
    pub id: usize,
    pub method: CowStr,
    pub params: Vec<Value>,
}

impl OutgoingObject {
    pub fn new(id: usize, method: CowStr, params: Vec<Value>) -> Self {
        Self {
            jsonrpc: JSONRPC_VERSION_2_0.into(),
            id,
            method,
            params,
        }
    }
}

#[derive(Debug, Clone, serde::Deserialize)]
#[serde(untagged)]
pub enum IncomingObject {
    ResultResponse {
        jsonrpc: CowStr,
        id: usize,
        result: Value,
    },
    ErrorResponse {
        jsonrpc: CowStr,
        id: usize,
        error: ResponseError,
    },
    Notification {
        jsonrpc: CowStr,
        method: CowStr,
        params: Value,
    },
}

impl IncomingObject {
    pub fn version_str(&self) -> &str {
        match self {
            IncomingObject::ResultResponse { jsonrpc, .. } => jsonrpc,
            IncomingObject::ErrorResponse { jsonrpc, .. } => jsonrpc,
            IncomingObject::Notification { jsonrpc, .. } => jsonrpc,
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ResponseError(Value);

impl fmt::Display for ResponseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Response.error: {}", self.0)
    }
}

impl std::error::Error for ResponseError {}
