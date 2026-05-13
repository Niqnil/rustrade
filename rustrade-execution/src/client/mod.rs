use crate::{
    UnindexedAccountEvent, UnindexedAccountSnapshot,
    balance::AssetBalance,
    error::UnindexedClientError,
    order::{
        Order,
        bracket::{BracketOrderRequest, BracketOrderResult},
        request::{OrderRequestCancel, OrderRequestOpen, UnindexedOrderResponseCancel},
        state::{Open, UnindexedOrderState},
    },
    trade::Trade,
};
use chrono::{DateTime, Utc};
use futures::Stream;
use rustrade_instrument::{
    asset::name::AssetNameExchange, exchange::ExchangeId, instrument::name::InstrumentNameExchange,
};
use std::future::Future;

// Alpaca ExecutionClient implementation (options, equities, crypto — single unified API)
#[cfg(feature = "alpaca")]
pub mod alpaca;

// BinanceSpot ExecutionClient implementation
#[cfg(feature = "binance")]
pub mod binance;

// Hyperliquid perpetual futures and spot ExecutionClient implementations
#[cfg(feature = "hyperliquid")]
pub mod hyperliquid;

// Interactive Brokers ExecutionClient implementation (equities, futures, options, forex)
#[cfg(feature = "ibkr")]
pub mod ibkr;

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
    ///
    /// # Backpressure
    ///
    /// Implementations use unbounded internal channels. If the consumer cannot keep up,
    /// events queue in memory rather than being dropped — per library philosophy, OOM
    /// crashes are preferable to silent data loss. Consumers requiring backpressure
    /// should implement it at their boundary (e.g., bounded channel with overflow policy).
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

    /// Place an order on the exchange.
    ///
    /// # Return value
    ///
    /// Returns `OrderState` directly rather than `Result<Open, OrderError>`:
    /// - `OrderState::Active(Open)` - order is resting on the order book
    /// - `OrderState::Inactive(FullyFilled)` - order was immediately filled (includes `avg_price` when available)
    /// - `OrderState::Inactive(OpenFailed)` - order placement failed (API error, connectivity, etc.)
    ///
    /// This design allows immediate fills to carry metadata (e.g., `avg_price`) that
    /// would be lost if we had to infer terminal state from `Open::filled_quantity`.
    fn open_order(
        &self,
        request: OrderRequestOpen<ExchangeId, &InstrumentNameExchange>,
    ) -> impl Future<Output = Option<Order<ExchangeId, InstrumentNameExchange, UnindexedOrderState>>>
    + Send;

    // `+ Send` on default method return types for multi-threaded Tokio runtime
    fn open_orders<'a>(
        &self,
        requests: impl IntoIterator<Item = OrderRequestOpen<ExchangeId, &'a InstrumentNameExchange>>,
    ) -> impl Stream<Item = Option<Order<ExchangeId, InstrumentNameExchange, UnindexedOrderState>>> + Send
    {
        futures::stream::FuturesUnordered::from_iter(
            requests.into_iter().map(|request| self.open_order(request)),
        )
    }

    /// Fetch current balances for the specified assets.
    ///
    /// An empty `assets` slice is the "return all" sentinel: implementations must return
    /// balances for every asset held. When non-empty, only the listed assets are returned.
    fn fetch_balances(
        &self,
        assets: &[AssetNameExchange],
    ) -> impl Future<Output = Result<Vec<AssetBalance<AssetNameExchange>>, UnindexedClientError>> + Send;

    /// Fetch currently open orders, optionally filtered by instrument.
    ///
    /// An empty `instruments` slice is the "return all" sentinel: implementations must
    /// return open orders across all instruments. When non-empty, only orders for the
    /// listed instruments are returned.
    fn fetch_open_orders(
        &self,
        instruments: &[InstrumentNameExchange],
    ) -> impl Future<
        Output = Result<Vec<Order<ExchangeId, InstrumentNameExchange, Open>>, UnindexedClientError>,
    > + Send;

    /// Fetch trades (fills) since `time_since`, optionally filtered by instrument.
    ///
    /// An empty `instruments` slice is the "return all" sentinel: implementations must
    /// return trades across all instruments. When non-empty, only trades for the listed
    /// instruments are returned.
    ///
    /// The fee asset (`AssetNameExchange`) may be quote, base, or third-party (e.g., BNB).
    /// Use `fees.fees_quote` for quote-equivalent value when available.
    ///
    /// Note: `MockExecution` currently ignores `instruments` and always returns all trades.
    fn fetch_trades(
        &self,
        time_since: DateTime<Utc>,
        instruments: &[InstrumentNameExchange],
    ) -> impl Future<
        Output = Result<
            Vec<Trade<AssetNameExchange, InstrumentNameExchange>>,
            UnindexedClientError,
        >,
    > + Send;
}

/// Extension trait for exchanges that support native bracket orders.
///
/// A bracket order consists of three linked orders:
/// 1. **Entry**: Limit order to enter the position
/// 2. **Take Profit**: Limit order to exit at profit target
/// 3. **Stop Loss**: Stop (or stop-limit) order to exit at loss limit
///
/// When either exit leg fills, the exchange automatically cancels the other.
///
/// # Type-Level Capability
///
/// This is a supertrait of [`ExecutionClient`], enabling compile-time capability checks:
/// - `impl ExecutionClient` — basic order operations
/// - `impl ExecutionClient + BracketOrderClient` — includes bracket orders
///
/// This follows Rust idioms like `Read + Seek` or `Iterator + ExactSizeIterator`.
///
/// # Why Supertrait Over Alternatives
///
/// **vs. associated types on `ExecutionClient`**: Callers can't construct
/// `Self::BracketRequest` without knowing the concrete type — adds trait surface
/// without enabling generic use.
///
/// **vs. default impl returning `Unsupported`**: Puts a "dead method" on every
/// client (MockClient, BinanceClient, HyperliquidClient). Compile-time capability
/// via trait bounds is better than runtime errors.
///
/// # Result Types
///
/// [`BracketOrderResult`] uses `Option<Order>` for child legs to document API divergence:
///
/// | Exchange | `take_profit` | `stop_loss` | Reason |
/// |----------|---------------|-------------|--------|
/// | IBKR     | `Some(...)` | `Some(...)` | Returns all three orders immediately |
/// | Alpaca   | `None` | `None` | Child legs created server-side |
///
/// # Example
///
/// ```ignore
/// use rustrade_execution::client::{ExecutionClient, BracketOrderClient};
/// use rustrade_execution::order::bracket::{BracketOrderRequest, RequestOpenBracket};
///
/// async fn place_bracket<C: ExecutionClient + BracketOrderClient>(
///     client: &C,
///     request: BracketOrderRequest<ExchangeId, &InstrumentNameExchange>,
/// ) -> BracketOrderResult {
///     client.open_bracket_order(request).await
/// }
/// ```
pub trait BracketOrderClient: ExecutionClient {
    /// Place a bracket order (entry + take-profit + stop-loss).
    ///
    /// # Request
    ///
    /// The [`BracketOrderRequest`] contains:
    /// - `key`: Order key (exchange, instrument, strategy, client order ID)
    /// - `state`: [`RequestOpenBracket`](crate::order::bracket::RequestOpenBracket) with
    ///   side, quantity, prices, and optional stop-loss limit price
    ///
    /// # Constraints
    ///
    /// - `time_in_force` must be `Day` or `GoodUntilCancelled` on most exchanges
    /// - Entry order type is always `Limit`
    /// - Price ordering must be valid for the side (see [`RequestOpenBracket`](crate::order::bracket::RequestOpenBracket))
    ///
    /// # Exchange-Specific Field Handling
    ///
    /// `RequestOpenBracket::stop_loss_limit_price` is **not honored uniformly**:
    /// - **Alpaca**: When `Some`, the stop-loss leg becomes a stop-limit order at that price.
    /// - **IBKR**: Silently ignored — the stop-loss leg is always a stop (market) order.
    ///
    /// Generic callers `T: BracketOrderClient` must treat this field as advisory.
    ///
    /// # Return Value
    ///
    /// Returns [`BracketOrderResult`] with:
    /// - `parent`: Always present (entry order)
    /// - `take_profit`: `Some` if exchange returns legs immediately (IBKR), `None` otherwise (Alpaca)
    /// - `stop_loss`: `Some` if exchange returns legs immediately (IBKR), `None` otherwise (Alpaca)
    ///
    /// Either all orders are `Active(Open)` or all are `Inactive` (placement failed).
    fn open_bracket_order(
        &self,
        request: BracketOrderRequest<ExchangeId, &InstrumentNameExchange>,
    ) -> impl Future<Output = BracketOrderResult> + Send;
}
