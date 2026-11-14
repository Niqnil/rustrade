use crate::order::{Order, OrderSnapshot, TimeInForce, state::OrderState};
use rustrade_instrument::{
    asset::{AssetIndex, name::AssetNameExchange},
    exchange::{ExchangeId, ExchangeIndex},
    instrument::{InstrumentIndex, name::InstrumentNameExchange},
};
use serde::{Deserialize, Serialize};

/// Convenient type alias for an [`OrderReport`] keyed with [`ExchangeId`], [`AssetNameExchange`]
/// and [`InstrumentNameExchange`].
pub type UnindexedOrderReport = OrderReport<ExchangeId, AssetNameExchange, InstrumentNameExchange>;

/// Which of an order's terms a report states as the venue holds them now.
///
/// An [`Order`] always carries a price, quantity, kind and time in force, but not every report
/// reads them from the venue. Some echo what the order was placed with, some read only its state,
/// and a venue read path may carry some terms and not others. A flag set here says that term on
/// the reported [`Order`] is the venue's current value. A flag left clear says nothing about it:
/// the term may be right or stale, and a reader must not take a difference from what it tracks as
/// a change made at the venue.
///
/// `Default` and [`NONE`](Self::NONE) state no term. `#[non_exhaustive]`, so a term can be added
/// without breaking a caller; build one from [`NONE`](Self::NONE) or [`ALL`](Self::ALL) and the
/// `with_` methods.
#[derive(
    Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Default, Deserialize, Serialize,
)]
#[serde(default)]
#[non_exhaustive]
pub struct ReportedTerms {
    /// [`Order::quantity`] is the order's total quantity as the venue holds it now, filled part
    /// included.
    pub quantity: bool,
    /// [`Order::price`] is the venue's limit price, for an order whose kind carries one.
    ///
    /// On a kind with no limit price it may hold whatever the venue reports in that place (a
    /// market order's worst fill price, say), so a reader takes it only for an order it knows has
    /// a limit.
    pub price: bool,
    /// [`Order::kind`] is the venue's order kind, with its trigger or trailing terms.
    pub kind: bool,
    /// [`Order::time_in_force`] is the venue's time in force.
    pub time_in_force: bool,
}

impl ReportedTerms {
    /// No term is stated: the report carries the order's state alone.
    pub const NONE: Self = Self {
        quantity: false,
        price: false,
        kind: false,
        time_in_force: false,
    };

    /// Every term is stated.
    pub const ALL: Self = Self {
        quantity: true,
        price: true,
        kind: true,
        time_in_force: true,
    };

    /// These terms with [`quantity`](Self::quantity) set to `reported`.
    #[must_use]
    pub const fn with_quantity(self, reported: bool) -> Self {
        Self {
            quantity: reported,
            ..self
        }
    }

    /// These terms with [`price`](Self::price) set to `reported`.
    #[must_use]
    pub const fn with_price(self, reported: bool) -> Self {
        Self {
            price: reported,
            ..self
        }
    }

    /// These terms with [`kind`](Self::kind) set to `reported`.
    #[must_use]
    pub const fn with_kind(self, reported: bool) -> Self {
        Self {
            kind: reported,
            ..self
        }
    }

    /// These terms with [`time_in_force`](Self::time_in_force) set to `reported`.
    #[must_use]
    pub const fn with_time_in_force(self, reported: bool) -> Self {
        Self {
            time_in_force: reported,
            ..self
        }
    }
}

/// An order as a venue reported it, and which of its terms the report states.
///
/// The [`Order`] is always whole, so a reader that wants only the order's state reads
/// [`order`](Self::order) as before. [`reported`](Self::reported) says which of its terms are the
/// venue's current values rather than an echo of the placement, a value kept from an earlier
/// report, or a stand-in.
///
/// Serialized as the order's own fields, with `reported` beside them; a missing `reported` reads
/// as [`ReportedTerms::NONE`].
///
/// # Producer obligations
///
/// - Set a flag only for a term read from the venue on this report's path, never from a cache of
///   what the order was placed with.
/// - Set no flag when the report's state timestamp is not the venue's time for these terms: a
///   reader orders reports by that time, so terms stamped with another time could undo a newer
///   change.
/// - Build with [`new`](Self::new) or [`state_only`](Self::state_only), which never state a time
///   in force the order holds as [`TimeInForce::Unreported`], nor a kind it holds as
///   [`ConditionalUnresolved`](crate::order::OrderKind::ConditionalUnresolved): those are
///   stand-ins, not the venue's terms.
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize)]
#[serde(bound(
    serialize = "ExchangeKey: Serialize, AssetKey: Serialize, InstrumentKey: Serialize",
    deserialize = "ExchangeKey: Deserialize<'de>, AssetKey: Deserialize<'de>, \
                   InstrumentKey: Deserialize<'de>"
))]
pub struct OrderReport<
    ExchangeKey = ExchangeIndex,
    AssetKey = AssetIndex,
    InstrumentKey = InstrumentIndex,
> {
    /// The order as reported. Only the terms [`reported`](Self::reported) names are the venue's.
    #[serde(flatten)]
    pub order: OrderSnapshot<ExchangeKey, AssetKey, InstrumentKey>,
    /// Which of [`order`](Self::order)'s terms are the venue's current values.
    #[serde(default)]
    pub reported: ReportedTerms,
}

impl<ExchangeKey, AssetKey, InstrumentKey> OrderReport<ExchangeKey, AssetKey, InstrumentKey> {
    /// A report of `order` stating the terms in `reported`, less any the order holds as a
    /// stand-in: a [`TimeInForce::Unreported`] or an
    /// [`OrderKind::ConditionalUnresolved`](crate::order::OrderKind::ConditionalUnresolved).
    pub fn new(
        order: Order<ExchangeKey, InstrumentKey, OrderState<AssetKey, InstrumentKey>>,
        reported: ReportedTerms,
    ) -> Self {
        let reported = reported
            .with_time_in_force(
                reported.time_in_force && order.time_in_force != TimeInForce::Unreported,
            )
            .with_kind(reported.kind && !order.kind.is_conditional_unresolved());

        Self { order, reported }
    }

    /// A report of `order`'s state alone: it states none of its terms.
    ///
    /// For a report whose terms echo the placement or come from a cache, such as the engine's own
    /// answers to its requests, or a venue message that carries only the order's state.
    pub fn state_only(
        order: Order<ExchangeKey, InstrumentKey, OrderState<AssetKey, InstrumentKey>>,
    ) -> Self {
        Self {
            order,
            reported: ReportedTerms::NONE,
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use crate::order::{
        ConditionalFamily, OrderKey, OrderKind,
        id::{ClientOrderId, OrderId, StrategyId, VenueOrderId},
        state::{ActiveOrderState, Open},
    };
    use chrono::{DateTime, Utc};
    use rust_decimal_macros::dec;
    use rustrade_instrument::Side;

    fn order(kind: OrderKind, time_in_force: TimeInForce) -> UnindexedOrderReport {
        let order = Order {
            key: OrderKey {
                exchange: ExchangeId::Simulated,
                instrument: InstrumentNameExchange::new("BTC-USDT"),
                strategy: StrategyId::new("strategy"),
                cid: ClientOrderId::new("cid"),
            },
            side: Side::Buy,
            price: Some(dec!(100)),
            quantity: dec!(2),
            kind,
            time_in_force,
            state: OrderState::Active(ActiveOrderState::Open(Open::new(
                VenueOrderId::Assigned(OrderId::new("venue-id")),
                DateTime::<Utc>::from_timestamp(1_700_000_000, 0).unwrap(),
                dec!(0.5),
            ))),
        };
        OrderReport::state_only(order)
    }

    const GTC: TimeInForce = TimeInForce::GoodUntilCancelled { post_only: false };

    #[test]
    fn test_with_methods_set_one_term_each() {
        assert_eq!(ReportedTerms::default(), ReportedTerms::NONE);
        let none = ReportedTerms::NONE;
        let one =
            |terms: ReportedTerms| [terms.quantity, terms.price, terms.kind, terms.time_in_force];
        assert_eq!(one(none.with_quantity(true)), [true, false, false, false]);
        assert_eq!(one(none.with_price(true)), [false, true, false, false]);
        assert_eq!(one(none.with_kind(true)), [false, false, true, false]);
        assert_eq!(
            one(none.with_time_in_force(true)),
            [false, false, false, true]
        );
        assert_eq!(
            ReportedTerms::ALL
                .with_quantity(false)
                .with_price(false)
                .with_kind(false)
                .with_time_in_force(false),
            ReportedTerms::NONE
        );
    }

    #[test]
    fn test_new_keeps_terms_the_order_states() {
        let report = OrderReport::new(order(OrderKind::Limit, GTC).order, ReportedTerms::ALL);
        assert_eq!(report.reported, ReportedTerms::ALL);
    }

    #[test]
    fn test_new_never_states_a_stand_in() {
        let unreported = order(OrderKind::Limit, TimeInForce::Unreported).order;
        assert_eq!(
            OrderReport::new(unreported, ReportedTerms::ALL).reported,
            ReportedTerms::ALL.with_time_in_force(false)
        );

        let unresolved = order(
            OrderKind::ConditionalUnresolved {
                family: ConditionalFamily::Stop,
                trigger_price: Some(dec!(90)),
            },
            GTC,
        )
        .order;
        assert_eq!(
            OrderReport::new(unresolved, ReportedTerms::ALL).reported,
            ReportedTerms::ALL.with_kind(false)
        );
    }

    #[test]
    fn test_state_only_states_nothing() {
        assert_eq!(order(OrderKind::Limit, GTC).reported, ReportedTerms::NONE);
    }

    #[test]
    fn test_serde_flattens_the_order_and_defaults_reported() {
        let report = OrderReport::new(order(OrderKind::Limit, GTC).order, ReportedTerms::ALL);

        let json = serde_json::to_value(&report).unwrap();
        assert_eq!(json["side"], serde_json::json!("Buy"));
        assert_eq!(json["reported"]["kind"], serde_json::json!(true));
        let back: UnindexedOrderReport = serde_json::from_value(json.clone()).unwrap();
        assert_eq!(back, report);

        // An order serialized before `reported` existed reads as stating no term.
        let serde_json::Value::Object(mut fields) = json else {
            panic!("an order report serializes as an object");
        };
        fields.remove("reported");
        let old: UnindexedOrderReport =
            serde_json::from_value(serde_json::Value::Object(fields)).unwrap();
        assert_eq!(old, OrderReport::state_only(report.order));
    }
}
