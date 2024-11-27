use crate::{MethodAndParams, Request, ResponseError, ResponseSender};

pub type PendingRequestTuple<Req, Resp> = (Req, Option<ResponseSender<Resp>>);

pub enum SatisfyRequestError {
    Deserialize(serde_json::Error),
}

macro_rules! gen_any_pending_request {
    ($($name:ident),*) => {
        #[derive(Debug, Clone)]
        pub enum SatisfiedRequest {
            $($name {
                req: crate::request::$name,
                resp: <crate::request::$name as Request>::Response,
            }),*,
        }

        #[derive(Debug, Clone)]
        pub enum ErroredRequest {
            $($name {
                req: crate::request::$name,
                error: ResponseError,
            }),*,
        }

        impl core::fmt::Display for ErroredRequest {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                match self {
                    $(Self::$name { req, error } => write!(f, "Server responsed to {:?} with error: {}", req, error)),*,
                }
            }
        }

        impl std::error::Error for ErroredRequest {}

        #[derive(Debug)]
        pub enum PendingRequest {
            $($name {
                req: crate::request::$name,
                resp_tx: Option<ResponseSender<<crate::request::$name as Request>::Response>>,
            }),*,
        }

        $(
            impl From<PendingRequestTuple<crate::request::$name, <crate::request::$name as Request>::Response>> for PendingRequest {
                fn from((req, resp_tx): PendingRequestTuple<crate::request::$name, <crate::request::$name as Request>::Response>) -> Self {
                    Self::$name{ req, resp_tx }
                }
            }
        )*

        impl PendingRequest {
            pub fn to_method_and_params(&self) -> MethodAndParams {
                match self {
                    $(PendingRequest::$name{ req, .. } => req.to_method_and_params()),*
                }
            }

            pub fn satisfy(self, raw_resp: serde_json::Value) -> Result<Option<SatisfiedRequest>, serde_json::Error> {
                use crate::request;
                match self {
                    $(Self::$name{ req, resp_tx } => {
                        let resp = serde_json::from_value::<<request::$name as Request>::Response>(raw_resp)?;
                        Ok(match resp_tx {
                            Some(tx) => {
                                let _ = tx.send(Ok(resp));
                                None
                            }
                            None => Some(SatisfiedRequest::$name { req, resp }),
                        })
                    }),*
                }
            }

            pub fn satisfy_error(self, raw_error: serde_json::Value) -> Option<ErroredRequest> {
                let error = ResponseError(raw_error);
                match self {
                    $(Self::$name{ req, resp_tx } => {
                        match resp_tx {
                            Some(tx) => { let _ = tx.send(Err(error)); None }
                            None => Some(ErroredRequest::$name{ req, error }),
                        }
                    }),*
                }
            }
        }
    };
}

gen_any_pending_request! {
    Header,
    HeaderWithProof,
    Headers,
    EstimateFee,
    HeadersSubscribe,
    RelayFee,
    GetBalance,
    GetHistory,
    GetMempool,
    ListUnspent,
    ScriptHashSubscribe,
    ScriptHashUnsubscribe,
    BroadcastTx,
    GetTx,
    GetTxMerkle,
    GetTxidFromPos,
    GetFeeHistogram,
    Banner,
    Ping,
    Custom
}
