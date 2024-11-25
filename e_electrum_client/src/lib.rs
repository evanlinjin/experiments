pub mod client;
mod custom_serde;
mod hash_types;
pub mod io;
pub mod notification;
pub mod pending_request;
pub mod request;
pub mod response;
mod state;

pub use client::Client;
use futures::channel::{mpsc, oneshot};
pub use hash_types::*;
pub use request::Request;
use serde_json::Value;
pub use state::*;
use std::fmt::Display;

pub const JSONRPC_VERSION_2_0: &str = "2.0";

pub type CowStr = std::borrow::Cow<'static, str>;
pub type DoubleSHA = bitcoin::hashes::sha256d::Hash;
pub type MethodAndParams = (CowStr, Vec<Value>);

pub type Response<Resp> = Result<Resp, ResponseError>;
pub type ResponseSender<Resp> = oneshot::Sender<Response<Resp>>;
pub type ResponseReceiver<Resp> = oneshot::Receiver<Response<Resp>>;

pub type EventSender = mpsc::UnboundedSender<Event>;
pub type EventReceiver = mpsc::UnboundedReceiver<Event>;

#[derive(Debug, Clone, Copy)]
pub struct Version;

impl Display for Version {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(JSONRPC_VERSION_2_0)
    }
}

impl AsRef<str> for Version {
    fn as_ref(&self) -> &str {
        JSONRPC_VERSION_2_0
    }
}

#[derive(Debug, Clone, serde::Deserialize)]
#[allow(clippy::manual_non_exhaustive)]
pub struct RawNotification {
    #[serde(
        rename(deserialize = "jsonrpc"),
        deserialize_with = "crate::custom_serde::version"
    )]
    pub version: Version,
    pub method: CowStr,
    pub params: Value,
}

#[derive(Debug, Clone, serde::Deserialize)]
#[allow(clippy::manual_non_exhaustive)]
pub struct RawResponse {
    #[serde(
        rename(deserialize = "jsonrpc"),
        deserialize_with = "crate::custom_serde::version"
    )]
    pub version: Version,
    pub id: usize,
    #[serde(flatten, deserialize_with = "crate::custom_serde::result")]
    pub result: Result<Value, Value>,
}

#[derive(Debug, Clone, serde::Deserialize)]
#[serde(untagged)]
pub enum RawNotificationOrResponse {
    Notification(RawNotification),
    Response(RawResponse),
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct RawRequest {
    pub jsonrpc: CowStr,
    pub id: usize,
    pub method: CowStr,
    pub params: Vec<Value>,
}

impl RawRequest {
    pub fn new(id: usize, method: CowStr, params: Vec<Value>) -> Self {
        Self {
            jsonrpc: JSONRPC_VERSION_2_0.into(),
            id,
            method,
            params,
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(untagged)]
pub enum MaybeBatch<T> {
    Single(T),
    Batch(Vec<T>),
}

impl<T> MaybeBatch<T> {
    pub fn into_vec(self) -> Vec<T> {
        match self {
            MaybeBatch::Single(item) => vec![item],
            MaybeBatch::Batch(batch) => batch,
        }
    }

    pub fn push_opt(opt: &mut Option<Self>, item: T) {
        *opt = match opt.take() {
            None => Some(Self::Single(item)),
            Some(maybe_batch) => {
                let mut items = maybe_batch.into_vec();
                items.push(item);
                Some(MaybeBatch::Batch(items))
            }
        }
    }
}

impl<T> From<T> for MaybeBatch<T> {
    fn from(value: T) -> Self {
        Self::Single(value)
    }
}

impl<T> From<Vec<T>> for MaybeBatch<T> {
    fn from(value: Vec<T>) -> Self {
        Self::Batch(value)
    }
}

/// Electrum server responds with an error.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct ResponseError(pub(crate) Value);

impl std::fmt::Display for ResponseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Response.error: {}", self.0)
    }
}

impl std::error::Error for ResponseError {}
