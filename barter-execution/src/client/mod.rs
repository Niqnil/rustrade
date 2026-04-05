use crate::{
    UnindexedAccountEvent, UnindexedAccountSnapshot,
    balance::AssetBalance,
    error::{UnindexedClientError, UnindexedOrderError},
    order::{
        Order,
        request::{OrderRequestCancel, OrderRequestOpen, UnindexedOrderResponseCancel},
        state::Open,
    },
    trade::Trade,
};
use barter_instrument::{
    asset::{QuoteAsset, name::AssetNameExchange},
    exchange::ExchangeId,
    instrument::name::InstrumentNameExchange,
};
use chrono::{DateTime, Utc};
use futures::Stream;
use std::future::Future;

// BinanceSpot ExecutionClient implementation
#[cfg(feature = "binance")]
pub mod binance;
pub mod mock;

// `+ Send` bounds on async method return types required for multi-threaded
// Tokio runtime. This is a breaking change vs upstream — any `!Send` executor
// implementation would fail to compile.
pub trait ExecutionClient
where
    Self: Clone,
{
    const EXCHANGE: ExchangeId;

    type Config: Clone;
    // `+ Send` required so generic code (e.g. ExecutionManager) can pass
    // the stream to tokio::spawn, which requires Send.
    type AccountStream: Stream<Item = UnindexedAccountEvent> + Send;

    fn new(config: Self::Config) -> Self;

    fn account_snapshot(
        &self,
        assets: &[AssetNameExchange],
        instruments: &[InstrumentNameExchange],
    ) -> impl Future<Output = Result<UnindexedAccountSnapshot, UnindexedClientError>> + Send;

    /// Returns a live stream of account events (fills, order updates, balance changes).
    ///
    /// # Startup race window
    ///
    /// There is an unavoidable gap between the WebSocket subscribe response and the
    /// first event being delivered: fills arriving in this window (typically milliseconds,
    /// no sub-millisecond guarantee) are silently dropped. `account_snapshot` reconciles
    /// open-order state, but TRADE fills in this window are not recoverable from the stream
    /// alone. Callers that require fill completeness at startup **must** call
    /// [`ExecutionClient::fetch_trades`] with at least a 1-second lookback after this method returns.
    fn account_stream(
        &self,
        assets: &[AssetNameExchange],
        instruments: &[InstrumentNameExchange],
    ) -> impl Future<Output = Result<Self::AccountStream, UnindexedClientError>> + Send;

    fn cancel_order(
        &self,
        request: OrderRequestCancel<ExchangeId, &InstrumentNameExchange>,
    ) -> impl Future<Output = Option<UnindexedOrderResponseCancel>> + Send;

    // `+ Send` on default method return types for multi-threaded Tokio runtime
    fn cancel_orders<'a>(
        &self,
        requests: impl IntoIterator<Item = OrderRequestCancel<ExchangeId, &'a InstrumentNameExchange>>,
    ) -> impl Stream<Item = Option<UnindexedOrderResponseCancel>> + Send {
        futures::stream::FuturesUnordered::from_iter(
            requests
                .into_iter()
                .map(|request| self.cancel_order(request)),
        )
    }

    fn open_order(
        &self,
        request: OrderRequestOpen<ExchangeId, &InstrumentNameExchange>,
    ) -> impl Future<
        Output = Option<
            Order<ExchangeId, InstrumentNameExchange, Result<Open, UnindexedOrderError>>,
        >,
    > + Send;

    // `+ Send` on default method return types for multi-threaded Tokio runtime
    fn open_orders<'a>(
        &self,
        requests: impl IntoIterator<Item = OrderRequestOpen<ExchangeId, &'a InstrumentNameExchange>>,
    ) -> impl Stream<
        Item = Option<Order<ExchangeId, InstrumentNameExchange, Result<Open, UnindexedOrderError>>>,
    > + Send {
        futures::stream::FuturesUnordered::from_iter(
            requests.into_iter().map(|request| self.open_order(request)),
        )
    }

    fn fetch_balances(
        &self,
        assets: &[AssetNameExchange],
    ) -> impl Future<Output = Result<Vec<AssetBalance<AssetNameExchange>>, UnindexedClientError>> + Send;

    fn fetch_open_orders(
        &self,
        instruments: &[InstrumentNameExchange],
    ) -> impl Future<
        Output = Result<Vec<Order<ExchangeId, InstrumentNameExchange, Open>>, UnindexedClientError>,
    > + Send;

    // added instruments parameter — Binance (and most exchanges) require
    // per-symbol queries for trade history. Consistent with fetch_open_orders.
    fn fetch_trades(
        &self,
        time_since: DateTime<Utc>,
        instruments: &[InstrumentNameExchange],
    ) -> impl Future<Output = Result<Vec<Trade<QuoteAsset, InstrumentNameExchange>>, UnindexedClientError>> + Send;
}
