use crate::{
    order::{Order, OrderKind, OrderSnapshot, TimeInForce, state::OrderState},
    trade::TimeSource,
};
use rust_decimal::Decimal;
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
    /// [`Order::price`] is the venue's limit price, `None` when the order has none.
    ///
    /// A producer states it only when its report says what the order is: a venue may put another
    /// price in the same place for an order with no limit, such as a market order's worst fill
    /// price.
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

    /// Whether no term is stated.
    pub const fn is_none(self) -> bool {
        !self.quantity && !self.price && !self.kind && !self.time_in_force
    }

    /// The terms stated in either `self` or `other`.
    #[must_use]
    pub const fn union(self, other: Self) -> Self {
        Self {
            quantity: self.quantity || other.quantity,
            price: self.price || other.price,
            kind: self.kind || other.kind,
            time_in_force: self.time_in_force || other.time_in_force,
        }
    }

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

    /// The terms a report states when it read the order's quantity, price, kind and time in force
    /// from the venue: those of [`from_read`](Self::from_read) when `time_source` says its state
    /// timestamp is the venue's, and none when it is this host's, since a reader orders reports
    /// by that time (see [`OrderReport`]'s producer obligations).
    ///
    /// `TimeSource::Venue` means the timestamp is the venue's time of these terms. A producer
    /// whose timestamp is the venue's but not the time of the terms uses [`NONE`](Self::NONE)
    /// instead. An order's creation time is such a timestamp on a venue that changes an order's
    /// terms in place. It is the time of the terms only on a venue that never does, where a
    /// change makes a new order.
    ///
    /// A producer stamping with this host's time may still state terms read from a single ordered
    /// channel, under the conditions in [`OrderReport`]'s producer obligations; it uses
    /// [`from_read`](Self::from_read) directly.
    #[must_use]
    pub fn from_venue_read(
        time_source: TimeSource,
        kind: &OrderKind,
        price: Option<Decimal>,
        quantity: Decimal,
    ) -> Self {
        match time_source {
            TimeSource::Venue => Self::from_read(kind, price, quantity),
            TimeSource::Host => Self::NONE,
        }
    }

    /// The terms a report states when it read the order's quantity, price, kind and time in force
    /// from the venue, and its timestamp orders them (see [`OrderReport`]'s producer obligations).
    ///
    /// - **Price.** A `kind` that carries no limit price ([`OrderKind::has_limit_price`]) states
    ///   that it has none. One that carries a limit states `price` only when it is non-zero: a
    ///   zero or missing price is not the venue's limit. For a kind without one, the producer
    ///   leaves its order's price `None`, whatever the venue's message holds there, so that the
    ///   price it states agrees with the order.
    /// - **Quantity.** A zero `quantity` is not stated. A venue reports one for an order sized
    ///   another way, such as by quote-asset amount or notional value, whose quantity is not known
    ///   until it fills.
    /// - **Kind and time in force** are stated as read. [`OrderReport::new`] clears an
    ///   [`OrderKind::ConditionalUnresolved`] kind and a [`TimeInForce::Unreported`] time in
    ///   force. A producer that filled a term in from elsewhere, such as the order it placed,
    ///   clears that flag itself.
    #[must_use]
    pub fn from_read(kind: &OrderKind, price: Option<Decimal>, quantity: Decimal) -> Self {
        Self::ALL
            .with_price(!kind.has_limit_price() || price.is_some_and(|price| !price.is_zero()))
            .with_quantity(!quantity.is_zero())
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
/// as [`ReportedTerms::NONE`]. That flattened layout needs a self-describing format, such as JSON,
/// to read back, and it is why [`Order`] has no field named `reported`.
///
/// # Reading the flags
///
/// A reader treats a [`TimeInForce::Unreported`] or an
/// [`OrderKind::ConditionalUnresolved`] as not
/// stated, whatever [`reported`](Self::reported) says. [`new`](Self::new) never states either, but
/// the fields are public, and a report deserialized or built as a literal is taken as it is.
///
/// # Producer obligations
///
/// - Set a flag only for a term read from the venue on this report's path, never from a cache of
///   what the order was placed with.
/// - Stamp a report that states terms so that its state timestamp orders it against every other
///   report of the order stating the same terms: a reader keeps, term by term, the value of the
///   latest stamped, so a report stamped out of order could undo a newer change. Either:
///   - the timestamp is the venue's time for these terms; or
///   - every report of the order that states terms comes off one channel that delivers them in
///     the venue's order, and is stamped on receipt, in delivery order, with a time that never
///     goes back (this host's clock, held at the latest stamp it gave when the clock steps back).
///
///   Otherwise set no flag. A report stamped with this host's time on another path, such as a
///   listing read on another thread, states none.
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
    /// [`OrderKind::ConditionalUnresolved`].
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

    /// A venue read states every term on the venue's time and none on this host's. A limit kind
    /// states its price only when it is non-zero, a kind without a limit states that it has
    /// none, and a zero quantity is not stated.
    #[test]
    fn a_venue_read_states_its_terms_on_the_venues_time() {
        use crate::trade::TimeSource;
        let read = |time_source, kind, price, quantity| {
            ReportedTerms::from_venue_read(time_source, &kind, price, quantity)
        };
        let limit = |price| read(TimeSource::Venue, OrderKind::Limit, price, dec!(1));
        assert_eq!(limit(Some(dec!(95))), ReportedTerms::ALL);
        assert_eq!(limit(Some(dec!(0))), ReportedTerms::ALL.with_price(false));
        assert_eq!(limit(None), ReportedTerms::ALL.with_price(false));
        assert_eq!(
            read(TimeSource::Venue, OrderKind::Market, None, dec!(1)),
            ReportedTerms::ALL
        );
        assert_eq!(
            read(TimeSource::Venue, OrderKind::Limit, Some(dec!(95)), dec!(0)),
            ReportedTerms::ALL.with_quantity(false)
        );
        assert_eq!(
            read(TimeSource::Host, OrderKind::Limit, Some(dec!(95)), dec!(1)),
            ReportedTerms::NONE
        );
    }

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
    fn test_union_and_is_none() {
        assert!(ReportedTerms::NONE.is_none());
        let none = ReportedTerms::NONE;
        for one in [
            none.with_quantity(true),
            none.with_price(true),
            none.with_kind(true),
            none.with_time_in_force(true),
        ] {
            assert!(!one.is_none(), "{one:?}");
            assert_eq!(none.union(one), one);
            assert_eq!(one.union(none), one);
            assert_eq!(one.union(ReportedTerms::ALL), ReportedTerms::ALL);
        }
        assert_eq!(
            none.with_quantity(true).union(none.with_kind(true)),
            none.with_quantity(true).with_kind(true)
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

    /// An account event and an account snapshot carrying reports read back from their JSON text:
    /// an ended order with a trigger and no limit price, keyed by indices, through the flattened
    /// layout.
    #[test]
    fn test_account_event_and_snapshot_round_trip_through_json_text() {
        use crate::{
            AccountEvent, AccountEventKind, AccountSnapshot, InstrumentAccountSnapshot,
            order::state::Cancelled, position::PositionReport,
        };
        use rustrade_integration::collection::snapshot::Snapshot;

        let order: OrderSnapshot = Order {
            key: OrderKey {
                exchange: ExchangeIndex::new(1),
                instrument: InstrumentIndex::new(2),
                strategy: StrategyId::new("strategy"),
                cid: ClientOrderId::new("cid"),
            },
            side: Side::Sell,
            price: None,
            quantity: dec!(3),
            kind: OrderKind::Stop {
                trigger_price: dec!(95.5),
            },
            time_in_force: TimeInForce::ImmediateOrCancel,
            state: OrderState::inactive(Cancelled::new(
                OrderId::new("venue-id"),
                DateTime::<Utc>::from_timestamp(1_700_000_000, 0).unwrap(),
                Some(dec!(1)),
            )),
        };
        let report = OrderReport::new(order, ReportedTerms::ALL.with_time_in_force(false));

        let event = AccountEvent::new(
            ExchangeIndex::new(1),
            AccountEventKind::OrderSnapshot(Snapshot(report.clone())),
        );
        let text = serde_json::to_string(&event).unwrap();
        assert_eq!(serde_json::from_str::<AccountEvent>(&text).unwrap(), event);

        let snapshot = AccountSnapshot::new(
            ExchangeIndex::new(1),
            Vec::new(),
            vec![InstrumentAccountSnapshot::new(
                InstrumentIndex::new(2),
                vec![report],
                true,
                PositionReport::Unreported,
                None,
            )],
        );
        let text = serde_json::to_string(&snapshot).unwrap();
        assert_eq!(
            serde_json::from_str::<AccountSnapshot>(&text).unwrap(),
            snapshot
        );
    }
}
