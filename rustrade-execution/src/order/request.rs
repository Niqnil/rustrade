use crate::{
    error::OrderError,
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
/// The [`close_open_positions_with_market_orders`](crate::strategy::close_positions::close_open_positions_with_market_orders)
/// helper sets this correctly. Direct `RequestOpen` construction must handle it manually.
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize)]
pub struct RequestOpen {
    pub side: Side,
    pub price: Decimal,
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
}

#[derive(
    Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Default, Deserialize, Serialize, Constructor,
)]
pub struct RequestCancel {
    pub id: Option<OrderId>,
}
