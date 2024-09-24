mod client;
mod custom_serde;
mod hash_types;
mod notification;
pub mod request;
mod response;
mod rpc_types;
mod service;

pub use client::*;
pub use hash_types::*;
pub use notification::*;
pub use response::*;
pub use rpc_types::*;
pub use service::*;

use futures::channel::{mpsc, oneshot};
use serde_json::Value;

pub const JSONRPC_VERSION_2_0: &str = "2.0";

const SUBSCRIBE_SUFFIX: &str = ".subscribe";
const UNSUBSCRIBE_SUFFIX: &str = ".unsubscribe";

pub type CowStr = std::borrow::Cow<'static, str>;
pub type DoubleSHA = bitcoin::hashes::sha256d::Hash;

pub type RequestMethodAndParams = (CowStr, Vec<Value>);

pub type ResponseResult = Result<Value, ResponseError>;
pub type ResponseSender = oneshot::Sender<ResponseResult>;
pub type ResponseReceiver = oneshot::Receiver<ResponseResult>;

pub type NotificationMethodAndParams = (CowStr, Value);
pub type NotificationSender = mpsc::UnboundedSender<NotificationMethodAndParams>;
pub type NotificationReceiver = mpsc::UnboundedReceiver<NotificationMethodAndParams>;

pub type ActionSender = mpsc::UnboundedSender<Action>;
pub type ActionReceiver = mpsc::UnboundedReceiver<Action>;

/// Action sent to the [`Service`].
#[derive(Debug)]
pub enum Action {
    Call {
        method: CowStr,
        params: Vec<Value>,
        resp_tx: ResponseSender,
    },
    Stop,
}
