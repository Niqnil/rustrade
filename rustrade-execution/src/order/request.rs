use crate::{
    error::OrderError,
    market::MarketSnapshot,
    order::{
        OrderEvent, OrderKind, TimeInForce,
        id::{PositionId, VenueOrderId},
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
/// The `close_open_positions_with_market_orders` helper sets this correctly. A caller building
/// its own closing request must set it with [`with_reduce_only`](Self::with_reduce_only).
///
/// `#[non_exhaustive]`, so a field can be added without breaking a caller. Outside this crate,
/// build one with [`RequestOpen::new`] and the `with_*` methods, and change a copy by assigning
/// its public fields.
#[non_exhaustive]
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize)]
pub struct RequestOpen {
    pub side: Side,
    /// Limit price for the order. Required for Limit/StopLimit orders, and for
    /// TrailingStopFixedLimit orders, whose limit stays fixed while the trigger trails.
    /// `None` for Market/Stop/TrailingStop orders (which execute at market price when triggered),
    /// and for TrailingStopLimit orders, whose limit price follows from their stop and
    /// `limit_offset`. A price given with a Market order anyway is ignored, by the simulated venue
    /// and by the live clients that accept market orders.
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
    ///
    /// The `rustrade` engine enforces it on fills, whatever the venue did with it: a fill of a
    /// reduce-only order never grows or flips the engine's position. A fill larger than the
    /// opposite position is cut so the position lands flat, and the excess is reported rather
    /// than applied. See [`Trade::holding_move`](crate::trade::Trade::holding_move).
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
    /// # A simulated venue fills from it only when it has no book of its own
    ///
    /// [`SimulatedVenue`](crate::exchange::mock::SimulatedVenue) reads this **only** in
    /// [`VenueRegime::RequestPriced`](crate::exchange::mock::venue::VenueRegime::RequestPriced), where it
    /// has no market feed and this snapshot is its only price source. That is the regime
    /// [`MockExecution`](crate::client::mock::MockExecution) and
    /// [`MockExchange`](crate::exchange::mock::MockExchange) use: the request's snapshot is priced
    /// through the venue's [`FillModel`](crate::fill::FillModel), and without it a market order
    /// has no limit price to fall back on — the venue ignores `price` on an
    /// [`OrderKind::Market`] — so the venue can only reject.
    ///
    /// A [`MarketDriven`](crate::exchange::mock::venue::VenueRegime::MarketDriven) venue ignores it and
    /// prices from its own book, like a live one. This field is then decision-time provenance
    /// alone, and the difference between it and the price obtained is implementation shortfall.
    ///
    /// Sampling it at creation rather than at fill time is deliberate. It is the one instant with a
    /// well-defined position on a simulated timeline *for the sender*: everything the sender had
    /// seen by then is in it, and nothing it had not seen can be. What makes it safe for a venue to
    /// hold a book instead is that its driver paces the feed — routing each market event to the
    /// venue before the client that reacts to it sees the same event, and booking each request at
    /// the instant it arrives. A venue that sampled a stream it shares with the engine but does not
    /// pace could fill against prices from arbitrarily far in the future, which is why the regime
    /// is a property of how the venue is driven rather than a configuration flag.
    ///
    /// # `None` versus an empty snapshot
    ///
    /// `None` means no snapshot was taken — a live path, or a caller that constructed the request
    /// directly. `Some` holding a [`MarketSnapshot::is_empty`] snapshot means one was taken and the
    /// instrument had no price yet, which is what a cold start looks like. A `RequestPriced`
    /// simulated venue distinguishes the two in its rejection, because the fixes differ.
    ///
    /// `#[serde(default)]` so requests serialised before this field existed still load.
    #[serde(default)]
    pub market: Option<MarketSnapshot>,
}

impl RequestOpen {
    /// A request with no [`position_id`](Self::position_id), not
    /// [`reduce_only`](Self::reduce_only), and no [`market`](Self::market) snapshot. Set those
    /// with the `with_*` methods.
    pub fn new(
        side: Side,
        price: Option<Decimal>,
        quantity: Decimal,
        kind: OrderKind,
        time_in_force: TimeInForce,
    ) -> Self {
        Self {
            side,
            price,
            quantity,
            kind,
            time_in_force,
            position_id: None,
            reduce_only: false,
            market: None,
        }
    }

    /// This request with the [`position_id`](Self::position_id) it targets in
    /// `OmsMode::Hedging`.
    #[must_use]
    pub fn with_position_id(self, position_id: PositionId) -> Self {
        Self {
            position_id: Some(position_id),
            ..self
        }
    }

    /// This request with its [`reduce_only`](Self::reduce_only) flag set to `reduce_only`.
    #[must_use]
    pub fn with_reduce_only(self, reduce_only: bool) -> Self {
        Self {
            reduce_only,
            ..self
        }
    }

    /// This request with the [`market`](Self::market) its sender observed when it created it.
    #[must_use]
    pub fn with_market(self, market: MarketSnapshot) -> Self {
        Self {
            market: Some(market),
            ..self
        }
    }
}

#[derive(
    Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Default, Deserialize, Serialize, Constructor,
)]
pub struct RequestCancel {
    /// How the venue addresses the order to cancel, when that is known.
    ///
    /// Three cases, and a client must distinguish all three:
    /// - `None` -- the order is still in flight, so the venue has acknowledged nothing and there
    ///   is no identifier to cancel by.
    /// - `Some(VenueOrderId::Assigned(id))` -- cancel by the venue's identifier.
    /// - `Some(VenueOrderId::ClientAssigned)` -- the venue accepted the order without assigning an
    ///   identifier; cancel by the `ClientOrderId` in the request's key.
    ///
    /// Collapsing the last two into `None` would leave a client unable to tell "nothing to cancel"
    /// from "cancel by client id".
    pub id: Option<VenueOrderId>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn limit() -> RequestOpen {
        RequestOpen::new(
            Side::Sell,
            Some(dec!(101)),
            dec!(2),
            OrderKind::Limit,
            TimeInForce::GoodUntilCancelled { post_only: false },
        )
    }

    #[test]
    fn new_sets_the_given_fields_and_defaults_the_rest() {
        let request = limit();

        assert_eq!(request.side, Side::Sell);
        assert_eq!(request.price, Some(dec!(101)));
        assert_eq!(request.quantity, dec!(2));
        assert_eq!(request.kind, OrderKind::Limit);
        assert_eq!(
            request.time_in_force,
            TimeInForce::GoodUntilCancelled { post_only: false }
        );
        assert_eq!(request.position_id, None);
        assert!(!request.reduce_only);
        assert_eq!(request.market, None);
    }

    #[test]
    fn with_methods_set_only_their_own_field() {
        // Every optional field away from its default, so a builder that reset another field
        // would show.
        let set = RequestOpen {
            position_id: Some(PositionId::new("a")),
            reduce_only: true,
            market: Some(MarketSnapshot::from_last_price(Some(dec!(100)))),
            ..limit()
        };

        assert_eq!(
            set.clone().with_position_id(PositionId::new("b")),
            RequestOpen {
                position_id: Some(PositionId::new("b")),
                ..set.clone()
            }
        );
        assert_eq!(
            set.clone().with_reduce_only(false),
            RequestOpen {
                reduce_only: false,
                ..set.clone()
            }
        );
        let market = MarketSnapshot::from_last_price(Some(dec!(200)));
        assert_eq!(
            set.clone().with_market(market),
            RequestOpen {
                market: Some(market),
                ..set
            }
        );

        // And each sets its field from the default.
        assert_eq!(
            limit().with_position_id(PositionId::NETTING).position_id,
            Some(PositionId::NETTING)
        );
        assert!(limit().with_reduce_only(true).reduce_only);
        assert_eq!(limit().with_market(market).market, Some(market));
    }
}
