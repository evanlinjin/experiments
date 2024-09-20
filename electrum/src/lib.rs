use core::panic;
use std::{
    borrow::Cow,
    sync::{atomic::AtomicUsize, Arc, OnceLock},
};

use futures::channel::{
    mpsc::{self},
    oneshot,
};

pub fn add(left: u64, right: u64) -> u64 {
    left + right
}

const JSONRPC_VERSION_2_0: &str = "2.0";

/// A single parameter of a [`Request`](struct.Request.html)
#[derive(Debug, Clone)]
pub enum Param {
    /// Integer parameter
    U32(u32),
    /// Integer parameter
    Usize(usize),
    /// String parameter
    String(String),
    /// Boolean parameter
    Bool(bool),
    /// Bytes array parameter
    Bytes(Vec<u8>),
}

#[derive(Debug, Clone)]
pub struct Request<'a> {
    jsonrpc: &'a str,
    pub method: &'a str,
    pub params: Vec<Param>,
    pub id: usize,
}

impl<'a> Request<'a> {
    pub fn new(method: &'a str, params: Vec<Param>, id: usize) -> Self {
        Self {
            jsonrpc: JSONRPC_VERSION_2_0,
            method,
            params,
            id,
        }
    }
}

pub struct Notification<'a> {
    jsonrpc: Cow<'a, str>,
    pub method: Cow<'a, str>,
    pub params: Vec<Param>,
}

pub struct Response<'a> {
    jsonrpc: Cow<'a, str>,
    pub result: Option<serde_json::Value>,
    pub error: Option<ResponseError>,
    id: usize,
}

impl<'a> Response<'a> {
    pub fn check(&self) -> bool {
        self.result.is_some() != self.result.is_some()
    }
}

pub struct ResponseError {
    pub code: usize,
    pub message: String,
    pub data: Option<String>,
}

/// Service side of the call.
pub type CallService = (Request<'static>, oneshot::Sender<Response<'static>>);

#[derive(Debug, Clone)]
pub struct Client {
    req_tx: mpsc::UnboundedSender<CallService>,
    done: Arc<OnceLock<ServiceEnded>>,
    id_inc: Arc<AtomicUsize>,
}

impl Client {
    pub async fn call(
        &self,
        method: &'static str,
        params: Vec<Param>,
    ) -> Result<serde_json::Value, CallError> {
        let id = self
            .id_inc
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let req = Request::<'static>::new(method, params, id);
        let (resp_tx, resp_rx) = oneshot::channel::<Response<'static>>();
        if let Err(err) = self.req_tx.unbounded_send((req.clone(), resp_tx)) {
            if err.is_disconnected() {
                let done_reason = self
                    .done
                    .get()
                    .expect("must have done message if disconnected");
                return Err(CallError::ServiceEnded(*done_reason));
            }
            panic!("channel jammed with error: {}", err);
        };
        let resp = match resp_rx.await {
            Ok(resp) => resp,
            Err(_) => {
                let done_reason = self
                    .done
                    .get()
                    .expect("must have done message if disconnected");
                return Err(CallError::ServiceEnded(*done_reason));
            }
        };
        match (resp.result, resp.error) {
            (Some(resp_result), None) => Ok(resp_result),
            (None, Some(resp_err)) => Err(CallError::Response(resp_err)),
            _ => Err(CallError::Invalid),
        }
    }
}

pub struct Service {
    req_rx: mpsc::Receiver<CallService>,
    done: Arc<OnceLock<String>>,
    id_inc: Arc<AtomicUsize>,
}

#[derive(Debug, Clone, Copy)]
pub enum ServiceEnded {
    Disconnected,
    Stopped,
}

pub enum CallError {
    /// Service ended.
    ServiceEnded(ServiceEnded),
    /// Electrum server responds with an error.
    Response(ResponseError),
    /// Response by Electrum server is invalid.
    Invalid,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    }
}
