//! Execution client implementations for various exchanges.
//!
//! # Connector Comparison
//!
//! | Connector | Reconnect | Dedup | Fill Recovery | Heartbeat |
//! |-----------|-----------|-------|---------------|-----------|
//! | [`binance`] | Auto (1s→30s backoff) | 10k LRU | REST after reconnect | 30s |
//! | [`alpaca`] | Auto (1s→30s backoff) | 2k LRU | REST after reconnect | 35s |
//! | [`ibkr`] | Caller responsibility | N/A | Caller responsibility | N/A |
//! | [`hyperliquid`] | SDK-managed | 10k LRU, fills only | Caller responsibility | SDK-managed |
//!
//! # Resilience Philosophy
//!
//! **WebSocket-based connectors** (Binance, Alpaca) implement auto-reconnection with
//! fill recovery and deduplication. After reconnect, they query REST APIs for missed
//! fills and deduplicate against the LRU cache to prevent duplicate processing.
//!
//! **IBKR** uses TCP to local TWS/Gateway. Reconnection requires IB Gateway availability
//! and client ID coordination — decisions that belong in the caller's wrapper. See
//! [`ibkr`] module docs for caller responsibilities.
//!
//! **Hyperliquid** delegates reconnection to the official SDK's `with_reconnect()` mechanism, but
//! deduplicates fills itself: the SDK resubscribes on reconnect and the venue opens a `userFills`
//! subscription with a snapshot, so every reconnect redelivers fills already seen. It does not
//! recover fills missed while disconnected — callers needing that call
//! [`ExecutionClient::fetch_trades`], whose results are not deduplicated against the stream.
//!
//! # Known Limitations
//!
//! All connectors have a gap between reconnection and fill recovery: **order lifecycle
//! events** (NEW, CANCELED, EXPIRED) during disconnect are NOT recovered. Callers must
//! call [`ExecutionClient::fetch_open_orders`] after reconnect to reconcile state.

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
    asset::name::AssetNameExchange,
    exchange::ExchangeId,
    instrument::{kind::InstrumentKindDiscriminant, name::InstrumentNameExchange},
};
use std::future::Future;

// Account-event deduplication over rustrade's own event type. Gated on the clients that use it
// so a build selecting neither does not compile it unused. Alpaca deduplicates too, but against
// a raw `SmolStr` fill key rather than this cache, so it is deliberately not in this list.
#[cfg(any(feature = "binance", feature = "hyperliquid"))]
pub(crate) mod dedup;

// Alpaca ExecutionClient implementation (options, equities, crypto — single unified API)
#[cfg(feature = "alpaca")]
pub mod alpaca;

// Binance ExecutionClient implementations: Spot (live) and Cross Margin (identity/config scaffolding)
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

    /// The [`InstrumentKindDiscriminant`]s this client can trade.
    ///
    /// A client works off [`InstrumentNameExchange`] strings and never inspects an instrument's
    /// kind, so nothing downstream can infer this from the implementation — routing a `Future`
    /// through a spot-only venue produces a rejected or, worse, misinterpreted order rather than a
    /// type error. Declaring the set here lets [`ExecutionBuilder`] reject an unsupported
    /// instrument set at build time, before any order is sent.
    ///
    /// Declare what the *venue and this implementation together* can actually route, not what the
    /// exchange's product catalogue advertises. A kind the exchange offers but this client builds
    /// no request for does not belong in the set.
    ///
    /// There is deliberately no default. A new client must state its capabilities rather than
    /// inherit a permissive set that would silently admit every kind.
    ///
    /// It is a `const` and not a `fn supported_kinds(&self)` because [`ExecutionBuilder`] validates
    /// the instrument set *before* [`Self::new`] is ever called — there is no instance to ask at the
    /// point the answer is needed, and deferring validation until after construction would mean
    /// standing up a client for a venue the engine is about to reject. A capability set that varies
    /// with `Self::Config` therefore cannot be expressed here; it would need a separate
    /// pre-construction hook on the config.
    ///
    /// [`ExecutionBuilder`]: https://docs.rs/rustrade/latest/rustrade/execution/builder/struct.ExecutionBuilder.html
    const SUPPORTED_KINDS: &'static [InstrumentKindDiscriminant];

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

/// The capability table, pinned.
///
/// [`ExecutionClient::SUPPORTED_KINDS`] is a `const`: widening one, or adding a kind a client builds
/// no request for, compiles and passes clippy. The only thing it changes is which instruments
/// [`ExecutionBuilder`](https://docs.rs/rustrade/latest/rustrade/execution/builder/struct.ExecutionBuilder.html)
/// admits — so the failure mode is not a broken build but an instrument reaching a venue that cannot
/// encode it, as a silently wrong order. These assertions exist so that changing a client's declared
/// capabilities is a deliberate edit to a test that says what the venue can route, rather than a
/// one-word diff nothing observes.
///
/// The real-client cases are each gated on their own feature, matching the module gates above; the
/// mock case is ungated, like its module. So with `default = []`, a bare
/// `cargo test -p rustrade-execution` compiles only the mock assertion — pinning a real client's
/// capabilities requires `--features <name>` or `--all-features`.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::mock::MockExecution;

    #[test]
    fn mock_supports_only_the_kinds_it_can_model() {
        // No expiry, funding schedule or contract chain in `MockExchange`, so `Perpetual`, `Future`
        // and `Option` have no faithful projection onto it. Kept in step with the projection in
        // `generate_mock_exchange_instruments`, which panics on a kind it cannot project.
        assert_eq!(
            <MockExecution<fn() -> DateTime<Utc>> as ExecutionClient>::SUPPORTED_KINDS,
            &[
                InstrumentKindDiscriminant::Spot,
                InstrumentKindDiscriminant::Cfd
            ]
        );
    }

    #[cfg(feature = "binance")]
    #[test]
    fn binance_spot_and_margin_are_spot_only() {
        use crate::client::binance::{BinanceMargin, BinanceSpot};

        assert_eq!(
            BinanceSpot::SUPPORTED_KINDS,
            &[InstrumentKindDiscriminant::Spot]
        );
        // Cross margin trades the same spot instruments on borrowed funds -- the leverage is in the
        // account, not the instrument kind.
        assert_eq!(
            BinanceMargin::SUPPORTED_KINDS,
            &[InstrumentKindDiscriminant::Spot]
        );
    }

    #[cfg(feature = "hyperliquid")]
    #[test]
    fn hyperliquid_splits_perpetual_and_spot_across_two_clients() {
        use crate::client::hyperliquid::{HyperliquidClient, spot::HyperliquidSpotClient};

        assert_eq!(
            HyperliquidClient::SUPPORTED_KINDS,
            &[InstrumentKindDiscriminant::Perpetual]
        );
        assert_eq!(
            HyperliquidSpotClient::SUPPORTED_KINDS,
            &[InstrumentKindDiscriminant::Spot]
        );
    }

    #[cfg(feature = "alpaca")]
    #[test]
    fn alpaca_trades_equities_and_options() {
        use crate::client::alpaca::AlpacaClient;

        assert_eq!(
            AlpacaClient::SUPPORTED_KINDS,
            &[
                InstrumentKindDiscriminant::Spot,
                InstrumentKindDiscriminant::Option
            ]
        );
    }

    #[cfg(feature = "ibkr")]
    #[test]
    fn ibkr_omits_cfd_because_it_builds_no_contract_for_one() {
        use crate::client::ibkr::IbkrClient;

        // `ContractConfig::to_contract` builds `STK`, `FUT`, `OPT` and `CASH` and nothing else, so a
        // CFD routed here would be encoded as some other security type -- exactly the silent
        // wrong-symbol order this const exists to reject. Adding `Cfd` to the list requires adding
        // the arm first.
        assert_eq!(
            IbkrClient::SUPPORTED_KINDS,
            &[
                InstrumentKindDiscriminant::Spot,
                InstrumentKindDiscriminant::Future,
                InstrumentKindDiscriminant::Option
            ]
        );
    }
}
