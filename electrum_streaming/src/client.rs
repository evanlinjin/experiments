use futures::{
    channel::{mpsc, oneshot},
    Future, TryFutureExt,
};

use crate::{
    pending_request::{PendingRequest, PendingRequestTuple},
    MaybeBatch, Request, ResponseError,
};

pub type TrySendRequestError = mpsc::TrySendError<MaybeBatch<PendingRequest>>;
pub type RequestSender = mpsc::UnboundedSender<MaybeBatch<PendingRequest>>;
pub type RequestReceiver = mpsc::UnboundedReceiver<MaybeBatch<PendingRequest>>;

#[derive(Debug, Clone)]
pub struct Client {
    tx: RequestSender,
}

impl Client {
    pub fn new(tx: RequestSender) -> Self {
        Self { tx }
    }

    pub async fn request<Req>(&self, request: Req) -> Result<Req::Response, RequestError>
    where
        Req: Request,
        PendingRequestTuple<Req, Req::Response>: Into<PendingRequest>,
    {
        let mut batch = self.batch();
        let fut = batch.request(request).map_err(|e| match e {
            BatchedRequestError::Canceled => RequestError::Canceled,
            BatchedRequestError::Response(e) => RequestError::Response(e),
        });
        batch.send().map_err(RequestError::SendFailed)?;
        fut.await
    }

    pub fn request_event<Req>(&self, request: Req) -> Result<(), TrySendRequestError>
    where
        Req: Request,
        PendingRequestTuple<Req, Req::Response>: Into<PendingRequest>,
    {
        let mut batch = self.batch();
        batch.request_event(request);
        batch.send()?;
        Ok(())
    }

    pub fn batch(&self) -> BatchRequest<'_> {
        BatchRequest {
            tx: &self.tx,
            request: None,
        }
    }
}

#[must_use]
pub struct BatchRequest<'a> {
    tx: &'a mpsc::UnboundedSender<MaybeBatch<PendingRequest>>,
    request: Option<MaybeBatch<PendingRequest>>,
}

impl BatchRequest<'_> {
    /// Add `request` to the batch.
    ///
    /// Returns a future which becomes ready once this single request is satisfied. No not await on
    /// the returned future before [`BatchRequest::send`] is called.
    pub fn request<Req>(
        &mut self,
        request: Req,
    ) -> impl Future<Output = Result<Req::Response, BatchedRequestError>> + Send + Sync + 'static
    where
        Req: Request,
        PendingRequestTuple<Req, Req::Response>: Into<PendingRequest>,
    {
        let (resp_tx, resp_rx) = oneshot::channel();
        MaybeBatch::push_opt(&mut self.request, (request, Some(resp_tx)).into());
        async move {
            resp_rx
                .await
                .map_err(|_| BatchedRequestError::Canceled)?
                .map_err(BatchedRequestError::Response)
        }
    }

    pub fn request_event<Req>(&mut self, request: Req)
    where
        Req: Request,
        PendingRequestTuple<Req, Req::Response>: Into<PendingRequest>,
    {
        MaybeBatch::push_opt(&mut self.request, (request, None).into());
    }

    /// Send.
    ///
    /// Returns `false` if there is nothing to send.
    pub fn send(self) -> Result<bool, TrySendRequestError> {
        match self.request {
            Some(batch) => self.tx.unbounded_send(batch).map(|_| true),
            None => Ok(false),
        }
    }
}

#[derive(Debug)]
pub enum RequestError {
    SendFailed(TrySendRequestError),
    Canceled,
    Response(ResponseError),
}

impl std::fmt::Display for RequestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SendFailed(e) => write!(f, "Failed to send request: {}", e),
            Self::Canceled => write!(f, "Request was canceled before being satisfied."),
            Self::Response(e) => write!(f, "Request satisfied with error: {}", e),
        }
    }
}

impl std::error::Error for RequestError {}

#[derive(Debug)]
pub enum BatchedRequestError {
    Canceled,
    Response(ResponseError),
}

impl std::fmt::Display for BatchedRequestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Canceled => write!(f, "Request was canceled before being satisfied."),
            Self::Response(e) => write!(f, "Request satisfied with error: {}", e),
        }
    }
}

impl std::error::Error for BatchedRequestError {}
