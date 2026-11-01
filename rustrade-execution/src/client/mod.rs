//! Execution client implementations for various exchanges.
//!
//! # Connector Comparison
//!
//! | Connector | Reconnect | Dedup | Fill Recovery | Ended-order Recovery | Heartbeat | Cancel answers once |
//! |-----------|-----------|-------|---------------|----------------------|-----------|---------------------|
//! | [`binance`] | Auto (1s→30s backoff, 10 tries) | 10k LRU | REST after reconnect | REST after reconnect | 30s | cancelled |
//! | [`alpaca`] | Auto (1s→30s backoff, 10 tries) | 2k LRU | REST after reconnect | REST after reconnect | 35s | accepted |
//! | [`ibkr`] | ibapi-managed | 10k LRU, fills only | Executions request after reconnect | None | N/A | submitted |
//! | [`hyperliquid`] | SDK-managed (every 1s, no limit) | 10k LRU, fills only | Venue snapshot on resubscribe | REST after reconnect | SDK-managed | cancelled |
//!
//! "Reconnect" is how the account stream comes back after its connection drops, and when it gives
//! up: Binance and Alpaca end the stream once 10 reconnects in a row have failed (see
//! [`binance`](binance#account-stream-reconnection) and
//! [`AlpacaClient`](alpaca::AlpacaClient)'s `account_stream`).
//!
//! "Ended-order Recovery" is whether a reconnect reports how an order the client held as live
//! ended while the stream was down, and only those clients implement [`OrderStatusClient`]. The
//! last column is when [`ExecutionClient::cancel_order`] answers `Ok`. Only for "cancelled" has the
//! order ended by then; see that method.
//!
//! # Resilience Philosophy
//!
//! **WebSocket-based connectors** (Binance, Alpaca) implement auto-reconnection with
//! fill recovery and deduplication. After reconnect, they query REST APIs for missed
//! fills and deduplicate against the LRU cache to prevent duplicate processing.
//!
//! **IBKR** uses TCP to local TWS/Gateway. `ibapi` reconnects that socket itself; the
//! account stream stays open across it and recovers the missed fills from TWS's executions,
//! deduplicating against the LRU cache. Replacing a client that is gone for good requires IB
//! Gateway availability and client ID coordination — decisions that belong in the caller's
//! wrapper. See [`ibkr`] module docs for caller responsibilities.
//!
//! **Hyperliquid** delegates reconnection to the official SDK's `with_reconnect()` mechanism, but
//! deduplicates fills itself: the SDK resubscribes on reconnect and the venue opens a `userFills`
//! subscription with a snapshot, so every reconnect redelivers fills already seen. That snapshot
//! is the only fill recovery: it holds recent fills, how many Hyperliquid does not say, so callers
//! needing older ones call [`ExecutionClient::fetch_trades`], whose results are not deduplicated
//! against the stream. Once the snapshot has been sent on, a reconnect checks how the orders the
//! client holds as live ended, like Binance's and Alpaca's.
//!
//! # Known Limitations
//!
//! Only Binance (Spot and Margin), Alpaca and Hyperliquid (perpetuals and spot) recover the **order
//! lifecycle events** (cancelled, expired, rejected, a fill that completes an order) they missed
//! while disconnected, and only for the orders the client holds as live; IBKR recovers fills only
//! (#370). [`OrderStatusClient`] is the lookup that recovery is built on: given the orders a caller
//! still holds as live, it says how each that has ended did end. Binance Spot, Binance Margin,
//! Alpaca, both Hyperliquid clients and the mock client implement it.
//!
//! The engine closes part of that gap from the account snapshot each reconnect produces: an order a
//! complete list no longer shows is retired (see [`ExecutionClient::account_snapshot`]). That covers
//! the connectors whose snapshot declares its orders complete — Binance Spot and Margin,
//! Hyperliquid, Alpaca, and the mock venue — but not IBKR (#371), and it cannot tell how an order
//! ended. A caller that needs more reconciles against
//! [`ExecutionClient::fetch_open_orders`] after a reconnect.

use crate::{
    UnindexedAccountEvent, UnindexedAccountSnapshot,
    balance::AssetBalance,
    borrow_capacity::BorrowCapacity,
    error::UnindexedClientError,
    order::{
        Order, UnindexedInactiveOrder, UnindexedOrderKey,
        bracket::{BracketOrderRequest, BracketOrderResult},
        request::{OrderRequestCancel, OrderRequestOpen, UnindexedOrderResponseCancel},
        state::{Open, UnindexedOrderState},
    },
    shortability::Shortability,
    trade::TradesRead,
};
use chrono::{DateTime, Utc};
use futures::Stream;
use rustrade_instrument::{
    Side,
    asset::name::AssetNameExchange,
    exchange::ExchangeId,
    instrument::{kind::InstrumentKindDiscriminant, name::InstrumentNameExchange},
};
use std::future::Future;

// Account-event deduplication over rustrade's own event type. Alpaca deduplicates too, but against
// a raw `SmolStr` fill key rather than this cache. Always compiled: `AccountEventDeduper` is public.
pub mod dedup;

// What a client knows of its live orders, and the reconnect check of how those orders ended while
// its account stream was down. Gated on the clients that use it.
#[cfg(any(
    feature = "alpaca",
    feature = "binance",
    feature = "hyperliquid",
    feature = "ibkr"
))]
pub(crate) mod order_recovery;

// An estimate of a venue's clock from the times it stamps on its events. Always compiled: the
// execution manager's fill recovery uses it whatever the client.
pub mod venue_clock;

// The account streams' reconnect budget. Gated on the clients that use it.
#[cfg(any(feature = "alpaca", feature = "binance"))]
pub(crate) mod reconnect_budget;

// A REST stub on the test's own runtime, for the reconnect tests on a paused clock.
#[cfg(all(test, any(feature = "alpaca", feature = "binance")))]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
pub(crate) mod test_http;

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

/// An instrument an [`ExecutionClient`] is about to be built to trade, as
/// [`ExecutionClient::validate_config`] sees it.
///
/// `#[non_exhaustive]`, so further detail can be added without breaking an implementation that
/// reads these fields; construct one with [`Self::new`].
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
#[non_exhaustive]
pub struct ClientInstrument<'a> {
    /// The name the venue knows the instrument by, which is what a client's requests carry.
    pub name_exchange: &'a InstrumentNameExchange,
    /// The instrument's kind.
    pub kind: InstrumentKindDiscriminant,
}

impl<'a> ClientInstrument<'a> {
    /// The view of an instrument named `name_exchange`, of kind `kind`.
    pub fn new(
        name_exchange: &'a InstrumentNameExchange,
        kind: InstrumentKindDiscriminant,
    ) -> Self {
        Self {
            name_exchange,
            kind,
        }
    }
}

// `+ Send` bounds on async method return types required for multi-threaded
// Tokio runtime. This is a breaking change vs upstream — any `!Send` executor
// implementation would fail to compile.
/// A venue's account and order API, as the engine's `ExecutionManager` drives it.
///
/// # Wrapping a client
///
/// To add behaviour around a venue client without reimplementing it, wrap it in a generic
/// decorator that implements `ExecutionClient` itself. Typical uses are choosing which fills on a
/// shared account reach the engine, logging, and metrics.
///
/// - **Take the inner client's identity.** Set `const EXCHANGE: ExchangeId = C::EXCHANGE` and
///   `SUPPORTED_KINDS = C::SUPPORTED_KINDS`. The `ExecutionManager` refuses a venue client whose
///   `EXCHANGE` differs from its instrument map's exchange (only the `Mock` and `Simulated` ids
///   are exempt), so one generic wrapper then serves every venue client.
/// - **Forward every method, the provided ones included.** That is
///   [`open_orders`](Self::open_orders), [`cancel_orders`](Self::cancel_orders) and
///   [`validate_config`](Self::validate_config): left to the trait's defaults, they would bypass
///   any batching or config check the inner client has. Implement the extension traits the inner
///   client implements as well, with impls bounded on it, forwarding their provided methods too:
///   - [`OrderStatusClient`], which the `ExecutionManager` requires of every client it runs;
///   - [`BracketOrderClient`]
///   - [`ShortabilityClient`]
///   - [`BorrowCapacityClient`]
/// - **Withhold a fill from [`account_stream`](Self::account_stream) and
///   [`fetch_trades`](Self::fetch_trades) alike, by the same rule.** One fill can reach the engine
///   either way: live on the stream, or in a read made after a disconnect. A fill withheld from
///   only one of them still reaches the engine once.
///   - **Choose a key both paths carry.** [`Trade::client_order_id`] marks the orders a consumer
///     placed, but not every venue reports it on `fetch_trades` (its rustdoc lists which do).
///     Binance's is one that does not. A rule keyed on it has to decide what to do with a fill it
///     cannot classify. Keeping it, as the example below does, delivers too much rather than too
///     little: on such a venue every fill a `fetch_trades` read returns is kept, and only the
///     stream is filtered. To withhold those fills as well, match them another way, such as by
///     [`Trade::order_id`] against the orders the wrapper placed.
///   - **Keep fills the venue forced** ([`TradeOrigin::is_forced`]). They moved the position,
///     whoever placed the order.
///   - **Leave `resume` alone.** Filtering a [`TradesRead`] keeps its `resume` as it was: it marks
///     where the venue's read stopped, not which fills the wrapper kept.
/// - **One inner client per engine.** A wrapper that owns one client counts as that one client.
///   Two clients on one account whose account streams both reach one engine deliver every fill
///   twice.
/// - **Withhold here, not later.** The engine applies a fill that matches none of its orders too.
///   In `OmsMode::Netting` it moves the netting position like any other fill, and the engine
///   cannot tell it from a fill of its own. In `OmsMode::Hedging` an ordinary fill
///   ([`TradeOrigin::Order`] or [`TradeOrigin::External`]) opens a position of its own, and a
///   settlement fill is handled as [`InstrumentState::update_from_trade`] describes.
///
/// ```
/// use chrono::{DateTime, Utc};
/// use futures::{Stream, StreamExt, stream::BoxStream};
/// use rustrade_execution::{
///     AccountEventKind, UnindexedAccountEvent, UnindexedAccountSnapshot,
///     balance::AssetBalance,
///     client::{ClientInstrument, ExecutionClient, OrderStatusClient, VenueOrderStatus},
///     error::UnindexedClientError,
///     order::{
///         Order, UnindexedInactiveOrder, UnindexedOrderKey,
///         request::{OrderRequestCancel, OrderRequestOpen, UnindexedOrderResponseCancel},
///         state::{Open, UnindexedOrderState},
///     },
///     trade::{Trade, TradesRead},
/// };
/// use rustrade_instrument::{
///     asset::name::AssetNameExchange,
///     exchange::ExchangeId,
///     instrument::{kind::InstrumentKindDiscriminant, name::InstrumentNameExchange},
/// };
///
/// /// Delivers the fills of orders whose client order id starts with `prefix`, fills whose
/// /// client order id the venue did not report, and every fill the venue forced.
/// ///
/// /// Keeping a fill without a client order id is a choice: on a venue whose `fetch_trades`
/// /// reports none, this filters the account stream only.
/// #[derive(Debug, Clone)]
/// pub struct OwnFills<C> {
///     inner: C,
///     prefix: String,
/// }
///
/// #[derive(Debug, Clone)]
/// pub struct OwnFillsConfig<Config> {
///     pub inner: Config,
///     pub prefix: String,
/// }
///
/// fn keeps<AssetKey, InstrumentKey>(prefix: &str, trade: &Trade<AssetKey, InstrumentKey>) -> bool {
///     trade.origin.is_forced()
///         || trade
///             .client_order_id
///             .as_ref()
///             .is_none_or(|cid| cid.0.starts_with(prefix))
/// }
///
/// // `Sync` makes the wrapper's method futures `Send`, as the trait requires. `'static` lets the
/// // filtered account stream be boxed.
/// impl<C> ExecutionClient for OwnFills<C>
/// where
///     C: ExecutionClient + Sync,
///     C::AccountStream: 'static,
/// {
///     const EXCHANGE: ExchangeId = C::EXCHANGE;
///     const SUPPORTED_KINDS: &'static [InstrumentKindDiscriminant] = C::SUPPORTED_KINDS;
///     type Config = OwnFillsConfig<C::Config>;
///     type AccountStream = BoxStream<'static, UnindexedAccountEvent>;
///
///     fn validate_config(
///         config: &Self::Config,
///         instruments: &[ClientInstrument<'_>],
///     ) -> Result<(), String> {
///         C::validate_config(&config.inner, instruments)
///     }
///
///     fn new(config: Self::Config) -> Self {
///         Self {
///             inner: C::new(config.inner),
///             prefix: config.prefix,
///         }
///     }
///
///     async fn account_stream(
///         &self,
///         assets: &[AssetNameExchange],
///         instruments: &[InstrumentNameExchange],
///     ) -> Result<Self::AccountStream, UnindexedClientError> {
///         let prefix = self.prefix.clone();
///         let stream = self.inner.account_stream(assets, instruments).await?;
///         Ok(stream
///             .filter(move |event| {
///                 let keep = match &event.kind {
///                     AccountEventKind::Trade(trade) => keeps(&prefix, trade),
///                     _ => true,
///                 };
///                 std::future::ready(keep)
///             })
///             .boxed())
///     }
///
///     async fn fetch_trades(
///         &self,
///         start: DateTime<Utc>,
///         end: DateTime<Utc>,
///         instruments: &[InstrumentNameExchange],
///     ) -> Result<TradesRead<AssetNameExchange, InstrumentNameExchange>, UnindexedClientError> {
///         let mut read = self.inner.fetch_trades(start, end, instruments).await?;
///         read.trades.retain(|trade| keeps(&self.prefix, trade));
///         Ok(read)
///     }
///
///     // Everything else is forwarded unchanged.
///
///     async fn account_snapshot(
///         &self,
///         assets: &[AssetNameExchange],
///         instruments: &[InstrumentNameExchange],
///     ) -> Result<UnindexedAccountSnapshot, UnindexedClientError> {
///         self.inner.account_snapshot(assets, instruments).await
///     }
///
///     async fn cancel_order(
///         &self,
///         request: OrderRequestCancel<ExchangeId, &InstrumentNameExchange>,
///     ) -> UnindexedOrderResponseCancel {
///         self.inner.cancel_order(request).await
///     }
///
///     fn cancel_orders<'a>(
///         &self,
///         requests: impl IntoIterator<Item = OrderRequestCancel<ExchangeId, &'a InstrumentNameExchange>>,
///     ) -> impl Stream<Item = UnindexedOrderResponseCancel> + Send {
///         self.inner.cancel_orders(requests)
///     }
///
///     async fn open_order(
///         &self,
///         request: OrderRequestOpen<ExchangeId, &InstrumentNameExchange>,
///     ) -> Order<ExchangeId, InstrumentNameExchange, UnindexedOrderState> {
///         self.inner.open_order(request).await
///     }
///
///     fn open_orders<'a>(
///         &self,
///         requests: impl IntoIterator<Item = OrderRequestOpen<ExchangeId, &'a InstrumentNameExchange>>,
///     ) -> impl Stream<Item = Order<ExchangeId, InstrumentNameExchange, UnindexedOrderState>> + Send
///     {
///         self.inner.open_orders(requests)
///     }
///
///     async fn fetch_balances(
///         &self,
///         assets: &[AssetNameExchange],
///     ) -> Result<Vec<AssetBalance<AssetNameExchange>>, UnindexedClientError> {
///         self.inner.fetch_balances(assets).await
///     }
///
///     async fn fetch_open_orders(
///         &self,
///         instruments: &[InstrumentNameExchange],
///     ) -> Result<Vec<Order<ExchangeId, InstrumentNameExchange, Open>>, UnindexedClientError> {
///         self.inner.fetch_open_orders(instruments).await
///     }
/// }
///
/// impl<C> OrderStatusClient for OwnFills<C>
/// where
///     C: OrderStatusClient + Sync,
///     C::AccountStream: 'static,
/// {
///     async fn fetch_ended_orders(
///         &self,
///         orders: &[UnindexedOrderKey],
///     ) -> Result<Vec<UnindexedInactiveOrder>, UnindexedClientError> {
///         self.inner.fetch_ended_orders(orders).await
///     }
///
///     async fn fetch_order_status(
///         &self,
///         key: &UnindexedOrderKey,
///     ) -> Result<VenueOrderStatus, UnindexedClientError> {
///         self.inner.fetch_order_status(key).await
///     }
/// }
/// ```
///
/// [`Trade::client_order_id`]: crate::trade::Trade::client_order_id
/// [`Trade::order_id`]: crate::trade::Trade::order_id
/// [`TradeOrigin::is_forced`]: crate::trade::TradeOrigin::is_forced
/// [`TradeOrigin::Order`]: crate::trade::TradeOrigin::Order
/// [`TradeOrigin::External`]: crate::trade::TradeOrigin::External
/// [`TradesRead`]: crate::trade::TradesRead
/// [`InstrumentState::update_from_trade`]: https://docs.rs/rustrade/latest/rustrade/engine/state/instrument/struct.InstrumentState.html#method.update_from_trade
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

    /// Checks `config` against the instruments this client is about to be built to trade, before
    /// [`Self::new`] is called. `Err` names what is wrong.
    ///
    /// [`SUPPORTED_KINDS`](Self::SUPPORTED_KINDS) rejects a kind the client cannot route at all.
    /// This catches what only the config knows: a per-instrument entry that is invalid, or that
    /// disagrees with the instrument it is keyed to, such as a contract registered as a stock for
    /// an instrument modelled as an option. Each such entry would otherwise be skipped or acted on
    /// silently once the client runs.
    ///
    /// The default accepts every config. Override it when `Self::Config` carries per-instrument
    /// detail.
    ///
    /// [`ExecutionBuilder`] calls it, after checking `SUPPORTED_KINDS`, with the instruments
    /// registered on this client's exchange. A client constructed by calling `new` directly is not
    /// checked; call this first to get the same check.
    ///
    /// [`ExecutionBuilder`]: https://docs.rs/rustrade/latest/rustrade/execution/builder/struct.ExecutionBuilder.html
    fn validate_config(
        config: &Self::Config,
        instruments: &[ClientInstrument<'_>],
    ) -> Result<(), String> {
        let _ = (config, instruments);
        Ok(())
    }

    // `+ Send` required so generic code (e.g. ExecutionManager) can pass
    // the stream to tokio::spawn, which requires Send.
    type AccountStream: Stream<Item = UnindexedAccountEvent> + Send;

    fn new(config: Self::Config) -> Self;

    /// Reads the account's balances, and its open orders and positions for `instruments`, from the
    /// venue.
    ///
    /// Besides startup, the engine's `ExecutionManager` calls this again on every account-stream
    /// reconnect, and emits the result ahead of the new stream's updates. It is how the engine
    /// catches up on what changed while the stream was down.
    ///
    /// # Assets
    ///
    /// `assets` are the ones the caller registered. Return balances for those only when the venue
    /// lets you ask, which saves bandwidth. A balance for another asset is not an error: the
    /// `ExecutionManager` drops it and logs the asset names once per snapshot.
    ///
    /// Balances are stamped as [`fetch_balances`](Self::fetch_balances) describes.
    ///
    /// # Order completeness
    ///
    /// Each [`InstrumentAccountSnapshot`](crate::InstrumentAccountSnapshot) states through
    /// `orders_complete` whether its `orders` list is every order open at the venue for that
    /// instrument. Set it only when it is true: the engine retires a tracked order that a complete
    /// list omits, so a list that can miss an open order (a capped page, an order dropped on
    /// conversion, an order reported under an id other than the one it was placed with) must say
    /// `false`. `false` is always safe; it only forgoes that reconciliation.
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

    /// Cancel an order at the venue.
    ///
    /// # Return value
    ///
    /// Every request gets exactly one answer, which the engine's `ExecutionManager` forwards as the
    /// order's response. An implementation must not swallow a request: a failure to send it, or to
    /// read the venue's answer, is an `Err`.
    ///
    /// - `Ok(Cancelled)`: the venue took the cancel. Whether the order has *ended* by then depends
    ///   on the venue; see below.
    /// - `Err`: the venue refused the cancel, or it could not be sent or answered. The order may
    ///   still be open, or may have ended some other way, such as by filling. The account stream
    ///   reports which.
    ///
    /// # A taken cancel is not always an ended order
    ///
    /// Some venues answer a cancel before carrying it out. Until they do, the order is live and can
    /// still fill. From those venues `Ok(Cancelled)` means only that the cancel was accepted, and
    /// the account stream reports how the order actually ended, including any fill in between. The
    /// engine stops tracking an order on `Ok(Cancelled)` from any venue.
    ///
    /// Binance, Hyperliquid and the mock venue answer once the order is cancelled. Alpaca answers
    /// once it has accepted the cancel, and IBKR once the cancel is submitted. The
    /// [connector comparison](crate::client#connector-comparison) lists the same for each
    /// connector; the mock venue is not one, so it has no row there.
    ///
    /// # `filled_quantity` and `time_exchange`
    ///
    /// `Cancelled::filled_quantity` is the quantity the venue reported filled where the answer
    /// carries one, which Binance's and the mock venue's do. Alpaca, IBKR and Hyperliquid answer
    /// without it and report zero, which does not mean nothing filled. The fill total comes from
    /// the account stream, or from [`Self::fetch_trades`]. Likewise `time_exchange` is the venue's
    /// time where the answer carries one, and the local time the answer arrived otherwise.
    fn cancel_order(
        &self,
        request: OrderRequestCancel<ExchangeId, &InstrumentNameExchange>,
    ) -> impl Future<Output = UnindexedOrderResponseCancel> + Send;

    // `+ Send` on default method return types for multi-threaded Tokio runtime
    fn cancel_orders<'a>(
        &self,
        requests: impl IntoIterator<Item = OrderRequestCancel<ExchangeId, &'a InstrumentNameExchange>>,
    ) -> impl Stream<Item = UnindexedOrderResponseCancel> + Send {
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
    /// Every request gets exactly one answer, which the engine's `ExecutionManager` forwards as the
    /// order's response. An implementation must not swallow a request: a failure to send it, or to
    /// read the venue's answer, is `OpenFailed`.
    ///
    /// Returns `OrderState` directly rather than `Result<Open, OrderError>`:
    /// - `OrderState::Active(Open)` - order is resting on the order book
    /// - `OrderState::Inactive(FullyFilled)` - order was immediately filled (includes `avg_price` when available)
    /// - `OrderState::Inactive(Expired)` - order ended in the response itself without filling
    ///   completely, carrying what filled: an IOC or FOK order that found too little liquidity,
    ///   or one expired by self-trade prevention
    /// - `OrderState::Inactive(Cancelled)` - order the venue reports cancelled in the response
    ///   itself, carrying what filled
    /// - `OrderState::Inactive(OpenFailed)` - order placement failed (API error, connectivity, a
    ///   rejection the venue reports in the response, etc.)
    ///
    /// An implementation returns `Expired` or `Cancelled` only when its venue reports the order's
    /// status in the placement response (currently Binance Spot and Margin). The others return
    /// `Open` for such an order, and their account stream reports how it ended.
    ///
    /// This design allows immediate fills to carry metadata (e.g., `avg_price`) that
    /// would be lost if we had to infer terminal state from `Open::filled_quantity`.
    fn open_order(
        &self,
        request: OrderRequestOpen<ExchangeId, &InstrumentNameExchange>,
    ) -> impl Future<Output = Order<ExchangeId, InstrumentNameExchange, UnindexedOrderState>> + Send;

    // `+ Send` on default method return types for multi-threaded Tokio runtime
    fn open_orders<'a>(
        &self,
        requests: impl IntoIterator<Item = OrderRequestOpen<ExchangeId, &'a InstrumentNameExchange>>,
    ) -> impl Stream<Item = Order<ExchangeId, InstrumentNameExchange, UnindexedOrderState>> + Send
    {
        futures::stream::FuturesUnordered::from_iter(
            requests.into_iter().map(|request| self.open_order(request)),
        )
    }

    /// Fetch current balances for the specified assets.
    ///
    /// An empty `assets` slice is the "return all" sentinel: implementations must return
    /// balances for every asset held. When non-empty, only the listed assets are returned.
    ///
    /// # Balance time
    ///
    /// Each balance's `time_exchange` must be no later than the state it describes, because the
    /// engine orders balance and debt state by it: a balance stamped later than its state would
    /// block the stream's balance and debt events from between the two (see
    /// [`AssetBalance::time_exchange`]). Stamp a balance with this host's time taken before the
    /// request that returned it was sent, less how far the venue's REST view can trail its
    /// account stream where the stream carries balances, never with a time taken after the
    /// response. A time the venue states for the account's state can be used instead.
    ///
    /// A host clock ahead of the venue's carries into a stamp taken from this host's time. An
    /// implementation that keeps an estimate of the venue's clock that is never ahead of it takes
    /// the earlier of the two times instead, and says so in its docs. Otherwise keeping the two
    /// clocks within bounds is the caller's obligation.
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

    /// Read the trades (fills) from `start` to `end`, both inclusive, optionally filtered by
    /// instrument.
    ///
    /// An empty `instruments` slice is the "return all" sentinel: implementations must
    /// return trades across all instruments. When non-empty, only trades for the listed
    /// instruments are returned.
    ///
    /// # Bounded, resumable reads
    ///
    /// One call is bounded by the venue's limits, so it may stop before `end`.
    /// [`TradesRead::resume`] says whether it did:
    /// - `None`: every fill in the span the venue still holds was read.
    /// - `Some(t)`: call again with `start = t` and the same `end` to read on. That read can
    ///   return again fills the previous one returned, so match them by instrument and
    ///   [`TradeId`](crate::trade::TradeId).
    ///
    /// The caller owns the loop, and with it how many calls, and how long, it spends on a span.
    /// A `start` after `end` is an empty span, read as no trades and `resume: None`.
    ///
    /// `end` can lie past this host's present. A read placed on the venue's clock, as the
    /// execution manager's missed-fill recovery is, ends past it to cover a venue clock ahead of
    /// this host's. Implementations read to `end` or the venue's present, whichever is earlier,
    /// and must not cut `end` to this host's clock.
    ///
    /// # Venue limits
    ///
    /// `resume: None` covers what the venue still holds, which can be less than the span:
    /// - IBKR lists the last 7 days' executions at most.
    /// - Hyperliquid keeps only each wallet's 10,000 most recent fills.
    ///
    /// Binance needs instruments: an empty slice reads nothing. Each client documents its limits.
    ///
    /// # Errors
    ///
    /// [`Truncated`](crate::error::ClientError::Truncated) when a read cannot advance, because
    /// more fills share the span's first moment than one call can read past.
    ///
    /// The fee asset (`AssetNameExchange`) may be quote, base, or third-party (e.g., BNB).
    /// Use `fees.fees_quote` for quote-equivalent value when available.
    fn fetch_trades(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        instruments: &[InstrumentNameExchange],
    ) -> impl Future<
        Output = Result<
            TradesRead<AssetNameExchange, InstrumentNameExchange>,
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
/// client (MockExecution, BinanceSpot, HyperliquidClient). Compile-time capability
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
    /// Either all orders are `Active(Open)` or all are `Inactive` (placement failed), with one
    /// exception: a leg of a failed placement whose fate at the venue is unknown (no status
    /// arrived, or it was accepted and its rollback cancel could not be sent) is returned
    /// `Active(Open)`, so the account stream can resolve it. Only IBKR returns such legs today;
    /// see its `open_bracket_order`.
    fn open_bracket_order(
        &self,
        request: BracketOrderRequest<ExchangeId, &InstrumentNameExchange>,
    ) -> impl Future<Output = BracketOrderResult> + Send;
}

/// Extension trait for clients that can say how an order ended at the venue.
///
/// A client that loses its account stream recovers the fills it missed when it reconnects, but not
/// an order that was cancelled, expired or rejected in the meantime: the event that said so went
/// out while nothing was listening. The engine then keeps such an order as live until a complete
/// account snapshot no longer lists it, and even then cannot tell how it ended. This trait is the
/// lookup that answers that: given the orders a caller still holds as live, it returns how each one
/// that has ended did end.
///
/// # By order, not by time
///
/// The obvious alternative, asking the venue for every order closed since the disconnect, does not
/// work. Binance's `allOrders` and Alpaca's `GET /v2/orders?status=closed` both filter their time
/// window on when an order was **created**, so a window opening at the disconnect cannot see an
/// order placed before it and cancelled during it — which is the order that matters, because the
/// caller is tracking it. Looking each order up by its client order id finds it whenever it was
/// placed.
///
/// # Type-Level Capability
///
/// A supertrait of [`ExecutionClient`], for the reason [`BracketOrderClient`] is one: a client that
/// cannot look orders up does not implement it, rather than carrying a method that fails at run
/// time. A caller that needs the lookup bounds on `ExecutionClient + OrderStatusClient`.
pub trait OrderStatusClient: ExecutionClient {
    /// How each of `orders` ended, for those that have.
    ///
    /// # Contract
    ///
    /// - One entry per order that has ended, in no particular order:
    ///   [`FullyFilled`](crate::order::state::InactiveOrderState::FullyFilled),
    ///   [`Cancelled`](crate::order::state::InactiveOrderState::Cancelled) carrying what filled
    ///   before it, [`Expired`](crate::order::state::InactiveOrderState::Expired), or
    ///   [`OpenFailed`](crate::order::state::InactiveOrderState::OpenFailed) for an order the venue
    ///   accepted for processing and later rejected (Binance `REJECTED`, Alpaca `rejected`). An
    ///   order rejected in the response to its open request was answered there and is not expected
    ///   here.
    /// - An order still live is **omitted**, and so is one the venue does not know. Neither is an
    ///   error. A caller compares the result with what it asked for.
    /// - An empty `orders` returns an empty list. Unlike
    ///   [`ExecutionClient::fetch_open_orders`]'s empty slice, it does not mean "all".
    /// - An order is found by its [`cid`](crate::order::OrderKey::cid), and each returned order
    ///   carries the key it was asked for, so a `strategy` the venue does not record survives.
    /// - A key whose `instrument` is not the one the order traded is treated as unknown. The
    ///   `exchange` is not checked: a client answers for its own venue.
    /// - A `cid` asked about more than once is reported at most once, under the first key that
    ///   finds it.
    ///
    /// # Errors
    ///
    /// `Err` if the venue could not answer for every order. A partial list is never returned,
    /// because an order missing from it would read as still live.
    fn fetch_ended_orders(
        &self,
        orders: &[UnindexedOrderKey],
    ) -> impl Future<Output = Result<Vec<UnindexedInactiveOrder>, UnindexedClientError>> + Send;

    /// What the venue knows of the order under `key`: live, ended, or nothing.
    ///
    /// This is how an open request whose outcome is unknown is settled (see
    /// [`OrderError::is_outcome_unknown`](crate::error::OrderError::is_outcome_unknown)).
    ///
    /// # Contract
    ///
    /// - [`VenueOrderStatus::Unknown`] means the venue lists the order neither as live nor as
    ///   ended, under the key's `instrument`. A venue may take a moment to list an order it has
    ///   just accepted, so one `Unknown` does not prove the order was never placed. A caller asks
    ///   again before it concludes that.
    /// - The order is found by its [`cid`](crate::order::OrderKey::cid), and a found order
    ///   carries `key`.
    ///
    /// The provided method asks [`fetch_ended_orders`](Self::fetch_ended_orders) about `key`
    /// first, then lists the open orders of its instrument with
    /// [`ExecutionClient::fetch_open_orders`]. In that order, an order that is placed between
    /// the two reads is listed as live, so it is not missed. An order that ends between them is
    /// found by neither, and is reported `Unknown` once; the next lookup finds it ended. A client
    /// whose venue answers in one request may override it.
    ///
    /// # Errors
    ///
    /// `Err` if either read fails. A failed read is never reported as `Unknown`.
    fn fetch_order_status(
        &self,
        key: &UnindexedOrderKey,
    ) -> impl Future<Output = Result<VenueOrderStatus, UnindexedClientError>> + Send
    where
        Self: Sync,
    {
        async move {
            let ended = self.fetch_ended_orders(std::slice::from_ref(key)).await?;
            if let Some(order) = ended.into_iter().find(|order| order.key.cid == key.cid) {
                return Ok(VenueOrderStatus::Ended(Box::new(Order {
                    key: key.clone(),
                    ..order
                })));
            }

            let open = self
                .fetch_open_orders(std::slice::from_ref(&key.instrument))
                .await?;
            Ok(open
                .into_iter()
                .find(|order| order.key.cid == key.cid && order.key.instrument == key.instrument)
                .map_or(VenueOrderStatus::Unknown, |order| {
                    VenueOrderStatus::Live(Box::new(Order {
                        key: key.clone(),
                        ..order
                    }))
                }))
        }
    }
}

/// What a venue knows of one order, as
/// [`OrderStatusClient::fetch_order_status`] reads it.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum VenueOrderStatus {
    /// The order is live at the venue, in this state. Boxed, as `Unknown` carries nothing.
    Live(Box<Order<ExchangeId, InstrumentNameExchange, Open>>),
    /// The order has ended, as this says.
    Ended(Box<UnindexedInactiveOrder>),
    /// The venue lists the order neither as live nor as ended.
    Unknown,
}

/// Extension trait for clients that can say what the venue would lend for a short sale.
///
/// Answers, before an order is sent, the question a short sale otherwise learns only from its
/// rejection: whether the instrument may be sold short, whether it is easy to borrow, how much the
/// venue has to lend and at what fee. Each venue reports a different subset, so each field of the
/// [`Shortability`] it returns is optional; the implementations say which fields they fill.
///
/// # Type-Level Capability
///
/// A supertrait of [`ExecutionClient`], for the reason [`BracketOrderClient`] is one: a client
/// whose venue has nothing to lend, or that does not report it, does not implement it. A simulated
/// venue reads the same [`Shortability`] from a
/// [`ShortabilityProvider`](crate::shortability::ShortabilityProvider) instead.
pub trait ShortabilityClient: ExecutionClient {
    /// What the venue says about lending `instrument` for a short sale.
    ///
    /// # Contract
    ///
    /// - **Advisory.** `Ok` describes the venue's lending as it reported it, not a promise to lend:
    ///   a short sale must still handle
    ///   [`ApiError::BorrowRejected`](crate::error::ApiError::BorrowRejected).
    /// - **Not this account's capacity.** [`Shortability::available`] is what the venue has to
    ///   lend, not what is left of it for this account; that is
    ///   [`BorrowCapacityClient::fetch_borrow_capacity`], where a venue reports it.
    /// - **May be expensive.** An implementation may make several requests, some of them heavy
    ///   against the venue's rate limit, or hold a scarce resource such as a market data line;
    ///   each says what it costs. Fetch it when deciding on a short, not on every tick.
    /// - **May be slow.** Where the venue streams its answer rather than replying to a request,
    ///   the call waits for it, up to a timeout the implementation documents: seconds, not
    ///   milliseconds.
    /// - **May outlive its future.** An implementation that reads on a blocking thread finishes
    ///   that read, and releases what it holds, even when the returned future is dropped first;
    ///   each says so.
    ///
    /// # Errors
    ///
    /// - [`ApiError::InstrumentInvalid`](crate::error::ApiError::InstrumentInvalid) for an
    ///   instrument the venue does not know, or one of a kind that is not sold short by borrowing
    ///   it, such as an option.
    /// - An error, rather than a [`Shortability`] with nothing known, when the venue does not
    ///   answer, including when the account is not entitled to the answer, such as one without the
    ///   market data subscription a venue sends it through.
    fn fetch_shortability(
        &self,
        instrument: &InstrumentNameExchange,
    ) -> impl Future<Output = Result<Shortability, UnindexedClientError>> + Send;
}

/// Extension trait for clients that can say how much more this account can borrow.
///
/// Where [`ShortabilityClient`] says what the venue has to lend, this says how much of it this
/// account could take now, given its collateral, its limit and what it has already borrowed. The
/// two are separate types because they are different facts; see [`BorrowCapacity`].
///
/// # Type-Level Capability
///
/// A supertrait of [`ExecutionClient`], for the reason [`BracketOrderClient`] is one. Only a venue
/// that lends against an account's collateral and reports what is left of it implements it.
pub trait BorrowCapacityClient: ExecutionClient {
    /// How much this account can borrow now for an order on `instrument` on `side`.
    ///
    /// A sell borrows the instrument's base asset, which is a short; a buy borrows its quote asset,
    /// which is a leveraged long. The result names that asset, and its amounts are in its units.
    ///
    /// # Contract
    ///
    /// **Advisory**, as [`BorrowCapacity`] says: the amounts are not reserved, so an order sized to
    /// them must still handle [`ApiError::BorrowRejected`](crate::error::ApiError::BorrowRejected).
    ///
    /// # Errors
    ///
    /// [`ApiError::InstrumentInvalid`](crate::error::ApiError::InstrumentInvalid) for an instrument
    /// the client cannot borrow against, such as a pair the venue does not trade on margin.
    fn fetch_borrow_capacity(
        &self,
        instrument: &InstrumentNameExchange,
        side: Side,
    ) -> impl Future<Output = Result<BorrowCapacity, UnindexedClientError>> + Send;
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

    /// Answers `fetch_ended_orders` and `fetch_open_orders` with fixed lists, whatever is asked.
    #[derive(Debug, Clone, Default)]
    struct ListingClient {
        ended: Vec<UnindexedInactiveOrder>,
        open: Vec<Order<ExchangeId, InstrumentNameExchange, Open>>,
    }

    impl ExecutionClient for ListingClient {
        const EXCHANGE: ExchangeId = ExchangeId::Simulated;
        const SUPPORTED_KINDS: &'static [InstrumentKindDiscriminant] = &[];
        type Config = ();
        type AccountStream = futures::stream::Empty<UnindexedAccountEvent>;

        fn new(_: Self::Config) -> Self {
            Self::default()
        }

        async fn account_snapshot(
            &self,
            _: &[AssetNameExchange],
            _: &[InstrumentNameExchange],
        ) -> Result<UnindexedAccountSnapshot, UnindexedClientError> {
            unreachable!("not a lookup")
        }

        async fn account_stream(
            &self,
            _: &[AssetNameExchange],
            _: &[InstrumentNameExchange],
        ) -> Result<Self::AccountStream, UnindexedClientError> {
            unreachable!("not a lookup")
        }

        async fn cancel_order(
            &self,
            _: OrderRequestCancel<ExchangeId, &InstrumentNameExchange>,
        ) -> UnindexedOrderResponseCancel {
            unreachable!("not a lookup")
        }

        async fn open_order(
            &self,
            _: OrderRequestOpen<ExchangeId, &InstrumentNameExchange>,
        ) -> Order<ExchangeId, InstrumentNameExchange, UnindexedOrderState> {
            unreachable!("not a lookup")
        }

        async fn fetch_balances(
            &self,
            _: &[AssetNameExchange],
        ) -> Result<Vec<AssetBalance<AssetNameExchange>>, UnindexedClientError> {
            unreachable!("not a lookup")
        }

        async fn fetch_open_orders(
            &self,
            _: &[InstrumentNameExchange],
        ) -> Result<Vec<Order<ExchangeId, InstrumentNameExchange, Open>>, UnindexedClientError>
        {
            Ok(self.open.clone())
        }

        async fn fetch_trades(
            &self,
            _: DateTime<Utc>,
            _: DateTime<Utc>,
            _: &[InstrumentNameExchange],
        ) -> Result<TradesRead<AssetNameExchange, InstrumentNameExchange>, UnindexedClientError>
        {
            unreachable!("not a lookup")
        }
    }

    impl OrderStatusClient for ListingClient {
        async fn fetch_ended_orders(
            &self,
            _: &[UnindexedOrderKey],
        ) -> Result<Vec<UnindexedInactiveOrder>, UnindexedClientError> {
            Ok(self.ended.clone())
        }
    }

    /// An order under `cid` on `instrument`, as a venue lists it: with no strategy.
    fn listed<State>(
        instrument: &str,
        cid: &str,
        quantity: rust_decimal::Decimal,
        state: State,
    ) -> Order<ExchangeId, InstrumentNameExchange, State> {
        Order {
            key: crate::order::OrderKey {
                exchange: ExchangeId::Simulated,
                instrument: InstrumentNameExchange::new(instrument),
                strategy: crate::order::id::StrategyId::unknown(),
                cid: crate::order::id::ClientOrderId::new(cid),
            },
            side: Side::Buy,
            price: None,
            quantity,
            kind: crate::order::OrderKind::Market,
            time_in_force: crate::order::TimeInForce::ImmediateOrCancel,
            state,
        }
    }

    fn open_state() -> Open {
        Open::new(
            crate::order::id::VenueOrderId::Assigned(crate::order::id::OrderId::new("v")),
            DateTime::<Utc>::MIN_UTC,
            rust_decimal::Decimal::ZERO,
        )
    }

    fn cancelled_state() -> crate::order::state::UnindexedInactiveOrderState {
        crate::order::state::InactiveOrderState::Cancelled(crate::order::state::Cancelled::new(
            crate::order::id::OrderId::new("v"),
            DateTime::<Utc>::MIN_UTC,
            None,
        ))
    }

    /// The provided lookup picks the order under the asked client order id and instrument out of
    /// what each read lists, ended first, and reports it under the asked key.
    #[tokio::test]
    async fn fetch_order_status_finds_the_order_asked_about_under_its_key() {
        use rust_decimal::Decimal;

        let key = UnindexedOrderKey {
            exchange: ExchangeId::Simulated,
            instrument: InstrumentNameExchange::new("btc_usdt"),
            strategy: crate::order::id::StrategyId::new("asked"),
            cid: crate::order::id::ClientOrderId::new("target"),
        };
        // Decoys come first: another order on the instrument, and the id on another instrument.
        let open = vec![
            listed("btc_usdt", "other", Decimal::ONE, open_state()),
            listed("eth_usdt", "target", Decimal::TWO, open_state()),
            listed("btc_usdt", "target", Decimal::TEN, open_state()),
        ];

        let live = ListingClient {
            ended: Vec::new(),
            open: open.clone(),
        };
        match live.fetch_order_status(&key).await {
            Ok(VenueOrderStatus::Live(order)) => {
                assert_eq!(order.key, key, "reported under the asked key");
                assert_eq!(order.quantity, Decimal::TEN, "the asked order: {order:?}");
            }
            other => panic!("expected live, got {other:?}"),
        }

        let ended = ListingClient {
            ended: vec![
                listed("btc_usdt", "other", Decimal::ONE, cancelled_state()),
                listed("btc_usdt", "target", Decimal::TEN, cancelled_state()),
            ],
            open,
        };
        match ended.fetch_order_status(&key).await {
            Ok(VenueOrderStatus::Ended(order)) => {
                assert_eq!(order.key, key, "reported under the asked key");
                assert_eq!(order.quantity, Decimal::TEN, "the asked order: {order:?}");
            }
            other => panic!("expected ended, got {other:?}"),
        }

        let unknown = ListingClient {
            ended: vec![listed("btc_usdt", "other", Decimal::ONE, cancelled_state())],
            open: vec![
                listed("btc_usdt", "other", Decimal::ONE, open_state()),
                listed("eth_usdt", "target", Decimal::TWO, open_state()),
            ],
        };
        assert_eq!(
            unknown.fetch_order_status(&key).await,
            Ok(VenueOrderStatus::Unknown)
        );
    }
}
