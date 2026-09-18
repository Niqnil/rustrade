use crate::execution::error::ExecutionError;
use chrono::{DateTime, Utc};
use rustrade_data::error::DataError;
use rustrade_instrument::{exchange::ExchangeId, index::error::IndexError};
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize, Error)]
pub enum BarterError {
    #[error("IndexError: {0}")]
    IndexError(#[from] IndexError),

    #[error("ExecutionBuilder: {0}")]
    ExecutionBuilder(String),

    #[error("ExchangeManager dropped it's ExecutionRequest receiver")]
    ExecutionRxDropped(#[from] RxDropped),

    #[error("market data: {0}")]
    MarketData(#[from] DataError),

    #[error("execution: {0}")]
    Execution(#[from] ExecutionError),

    #[error("JoinError: {0}")]
    JoinError(String),

    /// A backtest market data source failed, or violated a
    /// [`BacktestMarketData`](crate::backtest::market_data::BacktestMarketData) caller obligation.
    ///
    /// Reaching a caller as the result of a backtest means the run was **aborted** — any statistics
    /// it would have produced would cover only the portion of the dataset that was read.
    ///
    /// # Why a `String` rather than a typed source
    /// `BarterError` derives `Clone`, `Eq`, `Ord`, `Hash` and both serde impls, so every payload
    /// must too. The sources this reports are caller-supplied and provider-specific — the LSE
    /// integration's own error is deliberately neither `Clone` nor `PartialEq`, matching every
    /// REST-backed integration in `rustrade-data` — so no typed variant could hold one. A source
    /// whose failure *is* a [`DataError`] should surface as [`MarketData`](Self::MarketData), which
    /// keeps the cause; this variant is for everything else, plus this crate's own obligation
    /// diagnostics.
    ///
    /// # Appended deliberately
    /// New variants belong at the end. `BarterError` derives `Ord`/`PartialOrd` from declaration
    /// order and `Serialize`/`Deserialize`, so inserting mid-enum reorders every comparison and
    /// shifts the variant index any index-based serializer writes.
    #[error("backtest market data: {0}")]
    BacktestMarketData(String),

    /// Every open request the strategy sent was rejected, so the session filled nothing.
    ///
    /// The statistics such a run produces are a tear sheet of zeros, indistinguishable at a
    /// glance from a strategy that deliberately stayed flat. That is a failed run rather than a
    /// result, so it is reported as an error instead of being returned as one.
    ///
    /// A strategy that sends no requests at all is unaffected.
    #[error(
        "backtest filled nothing: all {rejected} open requests were rejected (first reason: {reason})"
    )]
    BacktestAllOrdersRejected { rejected: usize, reason: String },

    /// A simulated run delivered `limit` account events without once drawing on its source, so the
    /// strategy is trading on its own fills faster than simulated time advances.
    ///
    /// # What this diagnoses
    /// A strategy that opens an order in response to its own fill, against a venue whose simulated
    /// round trip is **zero**, forms a zero-delay feedback cycle: the response is stamped at the
    /// very instant of the request that provoked it, so it outranks every later source event, the
    /// simulated clock never advances, and the market source is never drawn again. No
    /// discrete-event simulator can resolve such a cycle by scheduling alone — there is no instant
    /// at which to place the response that is both "after its cause" and "before the next input".
    ///
    /// The asynchronous execution path does not report this. It masks the cycle by racing: an
    /// unpaced market stream on another task runs ahead of the engine, so the strategy's own fills
    /// arrive interleaved with input rather than ahead of it. That is the non-determinism
    /// [`SimRunner`] exists to remove, which is why removing it makes this configuration visible.
    ///
    /// # Fixing it
    /// Give the venue a non-zero `latency_ms`, or stop the strategy generating orders in response
    /// to its own account events. Raising [`SimRunner::with_feedback_limit`] only delays the
    /// report; a true zero-delay cycle has no limit at which it terminates.
    ///
    /// # Appended deliberately
    /// See [`BacktestMarketData`](Self::BacktestMarketData) — new variants belong at the end.
    ///
    /// [`SimRunner`]: crate::execution::sim::SimRunner
    /// [`SimRunner::with_feedback_limit`]: crate::execution::sim::SimRunner::with_feedback_limit
    #[error(
        "simulated feedback loop on {exchange} at {time}: {limit} account events were delivered \
         with no intervening source event, so simulated time is not advancing. A strategy trading \
         on its own fills at zero simulated latency is a zero-delay cycle; give the venue a \
         non-zero latency_ms, or stop generating orders from its own account events"
    )]
    SimFeedbackLoop {
        exchange: ExchangeId,
        time: DateTime<Utc>,
        limit: usize,
    },
}
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize, Error)]
#[error("RxDropped")]
pub struct RxDropped;

impl<T> From<tokio::sync::mpsc::error::SendError<T>> for RxDropped {
    fn from(_: tokio::sync::mpsc::error::SendError<T>) -> Self {
        Self
    }
}

impl<T> From<tokio::sync::mpsc::error::SendError<T>> for BarterError {
    fn from(_: tokio::sync::mpsc::error::SendError<T>) -> Self {
        Self::ExecutionRxDropped(RxDropped)
    }
}

impl From<tokio::task::JoinError> for BarterError {
    fn from(value: tokio::task::JoinError) -> Self {
        Self::JoinError(format!("{value:?}"))
    }
}
