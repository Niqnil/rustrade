use derive_more::From;
use rustrade_execution::order::request::{OrderRequestCancel, OrderRequestOpen};
use rustrade_instrument::{exchange::ExchangeIndex, instrument::InstrumentIndex};
use serde::{Deserialize, Serialize};
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

/// Represents an `Engine` request to the `ExecutionManager`.
#[derive(Debug, Clone, PartialEq, PartialOrd, Deserialize, Serialize, From)]
pub enum ExecutionRequest<ExchangeKey = ExchangeIndex, InstrumentKey = InstrumentIndex> {
    /// Stop now, abandoning every request still in flight.
    ///
    /// Whatever those requests would have reported never arrives. This is what a live stop wants:
    /// it should not wait on a venue that may be slow or unreachable.
    Shutdown,

    /// Finish what is in flight, forward the account events that produced, then stop.
    ///
    /// The manager stops accepting new requests, awaits every in-flight open and cancel (each
    /// bounded by its `request_timeout`, so this cannot hang), forwards the account events already
    /// made available alongside those responses, and only then closes its channel.
    ///
    /// # Why this is distinct from [`Shutdown`](Self::Shutdown)
    /// Closing the channel is how a manager reports "I am finished", and a caller may be waiting on
    /// exactly that to end a run. A fill is delivered as a balance, a `Trade` and a response, of
    /// which only the response clears the request from flight — so a stop that closes the channel
    /// as soon as the responses are in truncates the other two. Draining is the graceful form and a
    /// backtest needs it; abandoning is the abrupt form and live trading wants that.
    Drain,

    /// Request to cancel an existing `Order`.
    Cancel(OrderRequestCancel<ExchangeKey, InstrumentKey>),

    /// Request to open an new `Order`.
    Open(OrderRequestOpen<ExchangeKey, InstrumentKey>),
}

#[derive(Debug)]
#[pin_project::pin_project]
pub(super) struct RequestFuture<Request, ResponseFut> {
    request: Request,
    #[pin]
    response_future: tokio::time::Timeout<ResponseFut>,
}

impl<Request, ResponseFut> Future for RequestFuture<Request, ResponseFut>
where
    Request: Clone,
    ResponseFut: Future,
{
    type Output = Result<ResponseFut::Output, Request>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        this.response_future
            .poll(cx)
            .map(|result| result.map_err(|_| this.request.clone()))
    }
}

impl<Request, ResponseFut> RequestFuture<Request, ResponseFut>
where
    ResponseFut: Future,
{
    pub fn new(future: ResponseFut, timeout: std::time::Duration, request: Request) -> Self {
        Self {
            request,
            response_future: tokio::time::timeout(timeout, future),
        }
    }
}
