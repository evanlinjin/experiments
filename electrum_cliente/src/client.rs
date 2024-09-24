use futures::channel::{mpsc, oneshot};
use serde_json::Value;

use crate::{request::Request, Action, ResponseError};
use std::borrow::Cow;

#[derive(Debug, Clone)]
pub struct Client {
    pub(crate) action_tx: mpsc::UnboundedSender<Action>,
}

impl Client {
    pub fn stop_service(&self) -> bool {
        match self.action_tx.unbounded_send(Action::Stop) {
            Ok(_) => true,
            Err(err) if err.is_disconnected() => false,
            Err(err) => panic!("unexpected error: {}", err),
        }
    }

    pub async fn raw_call<M>(&self, method: M, params: Vec<Value>) -> Result<Value, CallError>
    where
        M: Into<Cow<'static, str>>,
    {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.action_tx
            .unbounded_send(Action::Call {
                method: method.into(),
                params,
                resp_tx,
            })
            .map_err(|_| CallError::ServiceStopped)?;
        resp_rx
            .await
            .map_err(|_| CallError::ServiceStopped)?
            .map_err(CallError::Response)
    }

    pub async fn call<R>(&self, request: R) -> Result<R::Response, CallError>
    where
        R: Request,
    {
        let (method, params) = request.into_method_and_params();
        let result_obj = self.raw_call(method, params).await?;
        <R::Response as serde::de::Deserialize>::deserialize(result_obj)
            .map_err(CallError::Deserialize)
    }
}

#[derive(Debug)]
pub enum CallError {
    /// Service stopped.
    ServiceStopped,
    /// Electrum server responds with an error.
    Response(ResponseError),
    /// Failed to deserialize incoming `Response.result`.
    Deserialize(serde_json::Error),
}

impl std::fmt::Display for CallError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CallError::ServiceStopped => write!(f, "Service stopped."),
            CallError::Response(resp_err) => {
                write!(f, "{}", resp_err)
            }
            CallError::Deserialize(de_err) => {
                write!(f, "Failed to deserialize Response.result: {}", de_err)
            }
        }
    }
}

impl std::error::Error for CallError {}
