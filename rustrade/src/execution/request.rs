use derive_more::From;
use rustrade_execution::{
    error::{NotPlacedReason, OrderError},
    order::{
        Order,
        id::ClientOrderId,
        request::{CancelThenOpen, OrderRequestCancel, OrderRequestOpen},
        state::OrderState,
    },
};
use rustrade_instrument::{exchange::ExchangeIndex, instrument::InstrumentIndex};
use serde::{Deserialize, Serialize};
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::time::error::Elapsed;

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
    /// bounded by its `request_timeout`) and every lookup of an open whose outcome came back
    /// unknown (bounded by its
    /// [`UnknownOutcomeLookup`](crate::execution::unknown_outcome::UnknownOutcomeLookup)), so this
    /// cannot hang. It forwards the account events already made available alongside those
    /// responses, and only then closes its channel.
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

    /// Cancel orders, then open orders once every cancel has been answered: see
    /// [`CancelThenOpen`].
    ///
    /// The execution side sends the cancels as it would [`Cancel`](Self::Cancel)s and holds the
    /// opens. It answers each cancel as it arrives. Once the last has been answered, it sends the
    /// opens as it would [`Open`](Self::Open)s, or, when a cancel failed and the group says
    /// [`DropOpens`](rustrade_execution::order::request::OnCancelFailure::DropOpens), answers each
    /// open instead with the order as requested, inactive with
    /// [`NotPlacedReason::CancelFailed`].
    CancelThenOpen(CancelThenOpen<ExchangeKey, InstrumentKey>),
}

/// The answer to an open of a [`CancelThenOpen`] group that is dropped because the cancel `cancel`
/// failed: the order as requested, inactive with
/// [`NotPlacedReason::CancelFailed`].
pub(crate) fn dropped_open<ExchangeKey, AssetKey, InstrumentKey>(
    request: OrderRequestOpen<ExchangeKey, InstrumentKey>,
    cancel: ClientOrderId,
) -> Order<ExchangeKey, InstrumentKey, OrderState<AssetKey, InstrumentKey>> {
    let OrderRequestOpen { key, state } = request;
    Order {
        key,
        side: state.side,
        price: state.price,
        quantity: state.quantity,
        kind: state.kind,
        time_in_force: state.time_in_force,
        state: OrderState::inactive(OrderError::NotPlaced(NotPlacedReason::CancelFailed {
            cancel,
        })),
    }
}

/// An in-flight request to an [`ExecutionClient`](rustrade_execution::client::ExecutionClient),
/// bounded by a timeout.
///
/// Resolves to the request together with its outcome, so the `ExecutionManager` can key every
/// answer by the request it answers rather than by whatever the client put in its response.
#[derive(Debug)]
#[pin_project::pin_project]
pub(super) struct RequestFuture<Request, ResponseFut> {
    /// Taken when the future resolves, so the request is handed back without a `Clone`.
    request: Option<Request>,
    #[pin]
    response_future: tokio::time::Timeout<ResponseFut>,
}

impl<Request, ResponseFut> Future for RequestFuture<Request, ResponseFut>
where
    ResponseFut: Future,
{
    /// The request, and the client's response or [`Elapsed`] if the timeout fired first.
    type Output = (Request, Result<ResponseFut::Output, Elapsed>);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        this.response_future.poll(cx).map(|result| {
            // `Future`'s contract lets a future panic when polled after it returned `Ready`, and
            // the manager's `FuturesUnordered` never does so, so `request` is always present here.
            #[allow(clippy::expect_used)]
            let request = this
                .request
                .take()
                .expect("RequestFuture polled after it resolved");
            (request, result)
        })
    }
}

impl<Request, ResponseFut> RequestFuture<Request, ResponseFut>
where
    ResponseFut: Future,
{
    pub fn new(future: ResponseFut, timeout: std::time::Duration, request: Request) -> Self {
        Self {
            request: Some(request),
            response_future: tokio::time::timeout(timeout, future),
        }
    }
}
