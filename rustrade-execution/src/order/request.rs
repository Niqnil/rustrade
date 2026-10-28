use crate::{
    error::OrderError,
    margin::MarginEffect,
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
use serde::{Deserialize, Deserializer, Serialize};
use std::fmt;

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
/// The `reduce_only` field defaults to [`ReduceOnly::No`], which means:
/// - A `Sell` order defaults to `SellToOpen` (open short / write option)
/// - A `Buy` order defaults to `BuyToOpen` (open long)
///
/// **For closing positions, callers MUST explicitly set `reduce_only` to
/// [`ReduceOnly::Position`]** (or [`ReduceOnly::PositionAndFreeBase`]).
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
    /// Constrain this order to only reduce existing positions, never open new ones: see
    /// [`ReduceOnly`].
    ///
    /// Used by exchanges that require explicit open/close intent (Alpaca, Interactive
    /// Brokers, Schwab). Adapters derive venue-specific semantics from
    /// [`ReduceOnly::is_reduce_only`] + `side`, so both reduce-only values map alike:
    /// - not reduce-only, Buy  → BuyToOpen (open long / add to long)
    /// - not reduce-only, Sell → SellToOpen (open short / write option)
    /// - reduce-only, Buy      → BuyToClose (close short)
    /// - reduce-only, Sell     → SellToClose (close long)
    ///
    /// Exchanges that infer intent from positions (Binance, Deribit) may map this to
    /// their `reduceOnly` parameter or ignore it entirely.
    ///
    /// The `rustrade` engine enforces it on fills, whatever the venue did with it: a fill of a
    /// reduce-only order never grows or flips the engine's position. A fill larger than the
    /// opposite position is cut so the position lands flat, and the excess is reported rather
    /// than applied. See [`Trade::holding_move`](crate::trade::Trade::holding_move).
    ///
    /// `#[serde(default)]` so requests serialised before this field existed still load, and a
    /// `true` or `false` serialised before it was a [`ReduceOnly`] loads as
    /// [`ReduceOnly::Position`] or [`ReduceOnly::No`].
    #[serde(default)]
    pub reduce_only: ReduceOnly,

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

    /// What this order does to a margin account's loans, or `None` to take the client's default.
    ///
    /// Only a margin client or a simulated venue with spot margin acts on a borrow or a repay; see
    /// [`MarginEffect`] for the three values. A client that cannot honour a `Some` refuses the
    /// order unsent with
    /// [`OrderError::UnsupportedMarginEffect`]:
    /// a cash or spot-only account takes `Some(NoSideEffect)` alone, and a client that cannot
    /// promise it (account-level margin, or implicit borrowing as under Hyperliquid's portfolio
    /// margin) takes no `Some` at all, `NoSideEffect` included. A caller that sets an effect on
    /// every order must leave it `None` for those venues.
    ///
    /// `#[serde(default)]` so requests serialised before this field existed still load.
    #[serde(default)]
    pub margin_effect: Option<MarginEffect>,
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
            reduce_only: ReduceOnly::No,
            market: None,
            margin_effect: None,
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

    /// This request with its [`reduce_only`](Self::reduce_only) set to `reduce_only`.
    #[must_use]
    pub fn with_reduce_only(self, reduce_only: ReduceOnly) -> Self {
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

    /// This request with its [`margin_effect`](Self::margin_effect) set to `margin_effect`,
    /// instead of the client's default.
    #[must_use]
    pub fn with_margin_effect(self, margin_effect: MarginEffect) -> Self {
        Self {
            margin_effect: Some(margin_effect),
            ..self
        }
    }
}

/// Whether an open order may only reduce the position it closes, and whether it may also trade
/// the account's free base beyond it. Set on [`RequestOpen::reduce_only`].
///
/// A venue that takes an open or close intent, or a reduce-only flag, is sent the same for both
/// reduce-only values: see [`Self::is_reduce_only`]. They differ in how the `rustrade` engine
/// books a fill larger than the position. It cuts the fill so the position lands flat either way
/// (see [`Trade::holding_move`](crate::trade::Trade::holding_move)), and then:
/// - for [`Position`](Self::Position), reads the excess as a mistake: logged at WARN, and
///   counted as a reduce-only fill cut;
/// - for [`PositionAndFreeBase`](Self::PositionAndFreeBase) on a spot instrument, reads it as a
///   trade of free base the order was sized to make: logged at INFO, and counted apart.
///
/// # Serialised form
/// `"no"`, `"position"` or `"position_and_free_base"`. A `false` or `true`, the form before this
/// type replaced a flag, loads as [`No`](Self::No) or [`Position`](Self::Position).
#[derive(Debug, Clone, Copy, Default, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReduceOnly {
    /// The order may open or grow a position.
    #[default]
    No,
    /// The order only reduces the position it closes. Excess over the position is a mistake.
    Position,
    /// The order reduces the position it closes, and may trade base beyond it out of, or into,
    /// the account's free balance. Meant for spot instruments, spot margin included.
    ///
    /// A sell above a long, or from flat, sells free base: the base-fee remainder that a spot
    /// position leaves out (see the `rustrade` engine's `EngineState::free_base`), or base the
    /// account holds for another reason. A buy above a short buys base into the balance, as one
    /// sized to cover a loan's interest does. Size it from the base the account holds outside
    /// the position, so the excess never sells more than the account has.
    ///
    /// A venue that enforces reduce-only itself, rather than ignoring it, may refuse or trim
    /// the excess. On a derivative, which has no free base, the engine books excess as for
    /// [`Position`](Self::Position).
    PositionAndFreeBase,
}

impl ReduceOnly {
    /// Whether the order may only reduce a position: anything but [`No`](Self::No). The intent
    /// or flag an adapter sends the venue.
    pub const fn is_reduce_only(self) -> bool {
        !matches!(self, Self::No)
    }
}

impl<'de> Deserialize<'de> for ReduceOnly {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct ReduceOnlyVisitor;

        impl serde::de::Visitor<'_> for ReduceOnlyVisitor {
            type Value = ReduceOnly;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(r#""no", "position", "position_and_free_base", or a bool"#)
            }

            fn visit_bool<E: serde::de::Error>(self, reduce_only: bool) -> Result<Self::Value, E> {
                Ok(if reduce_only {
                    ReduceOnly::Position
                } else {
                    ReduceOnly::No
                })
            }

            fn visit_str<E: serde::de::Error>(self, name: &str) -> Result<Self::Value, E> {
                match name {
                    "no" => Ok(ReduceOnly::No),
                    "position" => Ok(ReduceOnly::Position),
                    "position_and_free_base" => Ok(ReduceOnly::PositionAndFreeBase),
                    _ => Err(E::unknown_variant(
                        name,
                        &["no", "position", "position_and_free_base"],
                    )),
                }
            }
        }

        deserializer.deserialize_any(ReduceOnlyVisitor)
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
        assert_eq!(request.reduce_only, ReduceOnly::No);
        assert_eq!(request.market, None);
        assert_eq!(request.margin_effect, None);
    }

    #[test]
    #[allow(clippy::unwrap_used)] // Test code: panics are the correct failure mode
    fn a_request_serialised_without_a_margin_effect_still_loads() {
        let mut json = serde_json::to_value(limit()).unwrap();
        json.as_object_mut().unwrap().remove("margin_effect");

        let request: RequestOpen = serde_json::from_value(json).unwrap();

        assert_eq!(request.margin_effect, None);
    }

    #[test]
    #[allow(clippy::unwrap_used)] // Test code: panics are the correct failure mode
    fn reduce_only_round_trips_by_name_and_loads_a_flag() {
        for (reduce_only, name) in [
            (ReduceOnly::No, "no"),
            (ReduceOnly::Position, "position"),
            (ReduceOnly::PositionAndFreeBase, "position_and_free_base"),
        ] {
            let json = serde_json::to_value(limit().with_reduce_only(reduce_only)).unwrap();
            assert_eq!(json["reduce_only"], name);
            let loaded: RequestOpen = serde_json::from_value(json).unwrap();
            assert_eq!(loaded.reduce_only, reduce_only);
        }

        for (flag, reduce_only) in [(false, ReduceOnly::No), (true, ReduceOnly::Position)] {
            let mut json = serde_json::to_value(limit()).unwrap();
            json["reduce_only"] = flag.into();
            let loaded: RequestOpen = serde_json::from_value(json).unwrap();
            assert_eq!(loaded.reduce_only, reduce_only);
        }

        let mut json = serde_json::to_value(limit()).unwrap();
        json.as_object_mut().unwrap().remove("reduce_only");
        let loaded: RequestOpen = serde_json::from_value(json).unwrap();
        assert_eq!(loaded.reduce_only, ReduceOnly::No);

        let mut json = serde_json::to_value(limit()).unwrap();
        json["reduce_only"] = "sometimes".into();
        let error = serde_json::from_value::<RequestOpen>(json).unwrap_err();
        assert!(error.to_string().contains("sometimes"), "{error}");
    }

    #[test]
    fn only_no_is_not_reduce_only() {
        assert!(!ReduceOnly::No.is_reduce_only());
        assert!(ReduceOnly::Position.is_reduce_only());
        assert!(ReduceOnly::PositionAndFreeBase.is_reduce_only());
    }

    #[test]
    fn with_methods_set_only_their_own_field() {
        // Every optional field away from its default, so a builder that reset another field
        // would show.
        let set = RequestOpen {
            position_id: Some(PositionId::new("a")),
            reduce_only: ReduceOnly::Position,
            market: Some(MarketSnapshot::from_last_price(Some(dec!(100)))),
            margin_effect: Some(MarginEffect::Borrow),
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
            set.clone()
                .with_reduce_only(ReduceOnly::PositionAndFreeBase),
            RequestOpen {
                reduce_only: ReduceOnly::PositionAndFreeBase,
                ..set.clone()
            }
        );
        let market = MarketSnapshot::from_last_price(Some(dec!(200)));
        assert_eq!(
            set.clone().with_market(market),
            RequestOpen {
                market: Some(market),
                ..set.clone()
            }
        );
        assert_eq!(
            set.clone().with_margin_effect(MarginEffect::Repay),
            RequestOpen {
                margin_effect: Some(MarginEffect::Repay),
                ..set
            }
        );

        // And each sets its field from the default.
        assert_eq!(
            limit().with_position_id(PositionId::NETTING).position_id,
            Some(PositionId::NETTING)
        );
        assert_eq!(
            limit().with_reduce_only(ReduceOnly::Position).reduce_only,
            ReduceOnly::Position
        );
        assert_eq!(limit().with_market(market).market, Some(market));
        assert_eq!(
            limit()
                .with_margin_effect(MarginEffect::Repay)
                .margin_effect,
            Some(MarginEffect::Repay)
        );
    }
}
