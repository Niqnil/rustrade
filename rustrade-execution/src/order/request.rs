use crate::{
    error::OrderError,
    market::MarketSnapshot,
    order::{
        OrderEvent, OrderKind, TimeInForce,
        id::{OrderId, PositionId},
        state::Cancelled,
    },
};
use derive_more::Constructor;
use rust_decimal::Decimal;
use rustrade_instrument::{
    Side,
    asset::{AssetIndex, name::AssetNameExchange},
    exchange::{ExchangeId, ExchangeIndex},
    instrument::{InstrumentIndex, name::InstrumentNameExchange},
};
use serde::{Deserialize, Serialize};

pub type OrderRequestOpen<ExchangeKey = ExchangeIndex, InstrumentKey = InstrumentIndex> =
    OrderEvent<RequestOpen, ExchangeKey, InstrumentKey>;

pub type OrderRequestCancel<ExchangeKey = ExchangeIndex, InstrumentKey = InstrumentIndex> =
    OrderEvent<RequestCancel, ExchangeKey, InstrumentKey>;

pub type OrderResponseCancel<
    ExchangeKey = ExchangeIndex,
    AssetKey = AssetIndex,
    InstrumentKey = InstrumentIndex,
> = OrderEvent<Result<Cancelled, OrderError<AssetKey, InstrumentKey>>, ExchangeKey, InstrumentKey>;

pub type UnindexedOrderResponseCancel =
    OrderResponseCancel<ExchangeId, AssetNameExchange, InstrumentNameExchange>;

/// Parameters for opening a new order.
///
/// # Warning: `reduce_only` Default Behavior
///
/// The `reduce_only` field defaults to `false`, which means:
/// - A `Sell` order defaults to `SellToOpen` (open short / write option)
/// - A `Buy` order defaults to `BuyToOpen` (open long)
///
/// **For closing positions, callers MUST explicitly set `reduce_only: true`.**
/// Failure to do so on non-crypto instruments (equities, options) will:
/// - On non-margin accounts: result in a 422 rejection from the exchange
/// - On margin accounts: silently open a short position instead of closing the long
///
/// The `close_open_positions_with_market_orders`
/// helper sets this correctly. Direct `RequestOpen` construction must handle it manually.
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize)]
pub struct RequestOpen {
    pub side: Side,
    /// Limit price for the order. Required for Limit/StopLimit/TrailingStopLimit orders.
    /// `None` for Market/Stop/TrailingStop orders (which execute at market price when triggered).
    pub price: Option<Decimal>,
    pub quantity: Decimal,
    pub kind: OrderKind,
    pub time_in_force: TimeInForce,
    /// Target `PositionId` for this order in `OmsMode::Hedging`.
    ///
    /// For opening orders: the position this fill should open or add to.
    /// For closing orders: the position this fill should reduce or close.
    /// In `OmsMode::Netting`, leave as `None` (ignored).
    #[serde(default)]
    pub position_id: Option<PositionId>,
    /// Constrain this order to only reduce existing positions, never open new ones.
    ///
    /// Used by exchanges that require explicit open/close intent (Alpaca, Interactive
    /// Brokers, Schwab). Adapters derive venue-specific semantics from this flag + `side`:
    /// - `reduce_only=false, Buy`  → BuyToOpen (open long / add to long)
    /// - `reduce_only=false, Sell` → SellToOpen (open short / write option)
    /// - `reduce_only=true, Buy`   → BuyToClose (close short)
    /// - `reduce_only=true, Sell`  → SellToClose (close long)
    ///
    /// Exchanges that infer intent from positions (Binance, Deribit) may map this to
    /// their `reduceOnly` parameter or ignore it entirely.
    #[serde(default)]
    pub reduce_only: bool,

    /// Market state observed at the instant this request was created, if any was sampled.
    ///
    /// This is **decision-time provenance**, not an instruction to the venue. It records what the
    /// sender was looking at when it chose to trade, which is the one price a fill can be measured
    /// against after the fact — implementation shortfall is the difference between this and the
    /// price actually obtained.
    ///
    /// # Live venues must ignore this field
    ///
    /// No live exchange adapter reads it and none should: a venue prices a fill from its own book,
    /// and an adapter that forwarded these prices would be sending the client's view of the market
    /// back as an instruction. Every adapter in this crate leaves it untouched.
    ///
    /// # Simulated venues fill from it
    ///
    /// A simulated venue has no book of its own, so this snapshot is its only price source.
    /// [`MockExecution`](crate::client::mock::MockExecution) forwards it to
    /// [`MockExchange`](crate::exchange::mock::MockExchange), which prices the fill through its
    /// [`FillModel`](crate::fill::FillModel). Without it a market order carries no price at all —
    /// `price` is `None` by construction for [`OrderKind::Market`] — and the venue can only reject.
    ///
    /// Sampling it at creation rather than at fill time is deliberate. It is the one instant with a
    /// well-defined position on a simulated timeline: everything the sender had seen by then is in
    /// it, and nothing it had not seen can be. A venue that sampled the feed itself would be
    /// reading a stream it shares with the engine but does not pace, and could fill against prices
    /// from arbitrarily far in the future.
    ///
    /// # `None` versus an empty snapshot
    ///
    /// `None` means no snapshot was taken — a live path, or a caller that constructed the request
    /// directly. `Some` holding a [`MarketSnapshot::is_empty`] snapshot means one was taken and the
    /// instrument had no price yet, which is what a cold start looks like. A simulated venue
    /// distinguishes the two in its rejection, because the fixes differ.
    ///
    /// `#[serde(default)]` so requests serialised before this field existed still load.
    #[serde(default)]
    pub market: Option<MarketSnapshot>,
}

#[derive(
    Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Default, Deserialize, Serialize, Constructor,
)]
pub struct RequestCancel {
    pub id: Option<OrderId>,
}
