use crate::{
    error::OrderError,
    order::{OrderEvent, OrderKind, TimeInForce, id::{OrderId, PositionId}, state::Cancelled},
};
use barter_instrument::{
    Side,
    asset::{AssetIndex, name::AssetNameExchange},
    exchange::{ExchangeId, ExchangeIndex},
    instrument::{InstrumentIndex, name::InstrumentNameExchange},
};
use derive_more::Constructor;
use rust_decimal::Decimal;
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
}

#[derive(
    Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Default, Deserialize, Serialize, Constructor,
)]
pub struct RequestCancel {
    pub id: Option<OrderId>,
}
