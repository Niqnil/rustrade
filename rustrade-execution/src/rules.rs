//! An instrument's order rules (tick, lot step, minimums and maximums) and the check of an order
//! against them before it is sent.
//!
//! A venue publishes, per instrument, the grid a price and a quantity must sit on and the range
//! each must fall in. A client that loads those rules refuses an order that breaks them without
//! sending it, so the caller gets a typed error that says which value and which limit
//! ([`OrderError::InvalidPrecision`] for the grid, [`OrderError::OutOfBounds`] for the range)
//! rather than a venue's text after a round trip. It never rounds: rounding would place a
//! different order from the one asked for.
//!
//! A pass is not a guarantee. The check covers the rules a client has loaded, from the last time
//! it loaded them, and only the ones that can be read off the request. The venue has the last say.

use crate::{
    error::{Bound, BoundsViolation, OrderError, OrderField, PrecisionLimit, PrecisionViolation},
    order::OrderKind,
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// Whether a client checks an order against its instrument's [`OrderRules`] before sending it.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, Default, Eq, PartialEq, Hash, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PreSendRules {
    /// Refuse an order that breaks a rule, unsent. The default: a refusal costs no request, and
    /// carries the limit that was broken.
    #[default]
    Enforce,
    /// Send every order and leave the rules to the venue. Choose it when the loaded rules could
    /// be wrong, for example because the venue changed them since they were loaded.
    Off,
}

/// The grid and range of one kind of value: a price, a quantity or a notional.
///
/// Each part is optional. A venue that publishes a zero for one means the rule is off, and
/// [`ValueRule::new`] reads zero that way.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, Default, Eq, PartialEq, Hash, Deserialize, Serialize)]
pub struct ValueRule {
    /// The smallest value allowed.
    pub min: Option<Decimal>,
    /// The largest value allowed.
    pub max: Option<Decimal>,
    /// The value must be a whole multiple of this, counted from zero.
    pub step: Option<Decimal>,
}

impl ValueRule {
    /// A rule from the venue's `min`, `max` and `step`, each off when zero or negative.
    pub fn new(min: Decimal, max: Decimal, step: Decimal) -> Self {
        let on = |value: Decimal| (value > Decimal::ZERO).then_some(value);
        Self {
            min: on(min),
            max: on(max),
            step: on(step),
        }
    }

    /// Whether every part is off.
    pub fn is_off(&self) -> bool {
        self.min.is_none() && self.max.is_none() && self.step.is_none()
    }

    /// `Ok` when `value` is on the grid and in the range, else the first rule it breaks, grid
    /// before range.
    fn check<AssetKey, InstrumentKey>(
        &self,
        field: OrderField,
        value: Decimal,
    ) -> Result<(), OrderError<AssetKey, InstrumentKey>> {
        // A step set to zero by hand, not through `new`, is no grid: `%` would panic on it.
        if let Some(step) = self.step.filter(|step| *step > Decimal::ZERO)
            && !(value % step).is_zero()
        {
            return Err(OrderError::InvalidPrecision(PrecisionViolation::new(
                field,
                value,
                PrecisionLimit::NotOnGrid { step },
            )));
        }
        if let Some(min) = self.min.filter(|min| value < *min) {
            return Err(OrderError::OutOfBounds(BoundsViolation::new(
                field,
                Bound::Min,
                min,
                value,
            )));
        }
        if let Some(max) = self.max.filter(|max| value > *max) {
            return Err(OrderError::OutOfBounds(BoundsViolation::new(
                field,
                Bound::Max,
                max,
                value,
            )));
        }
        Ok(())
    }
}

/// One instrument's order rules.
///
/// Venue-agnostic: a client fills it from its venue's rules and calls [`check`](Self::check).
/// [`Default`] is no rules at all, which passes every order.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, Default, Eq, PartialEq, Hash, Deserialize, Serialize)]
pub struct OrderRules {
    /// The grid and range of a limit, stop and trigger price, and of a trailing order's
    /// activation price.
    pub price: ValueRule,
    /// The grid and range of every order's quantity.
    pub quantity: ValueRule,
    /// The grid and range of a market order's quantity, on top of [`quantity`](Self::quantity).
    pub market_quantity: ValueRule,
    /// The range of the notional, a price times a quantity. Its `step` is not used.
    pub notional: ValueRule,
}

impl OrderRules {
    /// Rules with `price`, `quantity`, `market_quantity` and `notional` as given.
    pub fn new(
        price: ValueRule,
        quantity: ValueRule,
        market_quantity: ValueRule,
        notional: ValueRule,
    ) -> Self {
        Self {
            price,
            quantity,
            market_quantity,
            notional,
        }
    }

    /// Whether these rules check nothing.
    pub fn is_off(&self) -> bool {
        self.price.is_off()
            && self.quantity.is_off()
            && self.market_quantity.is_off()
            && self.notional.is_off()
    }

    /// Check an order of `kind`, with its limit `price` and `quantity`, against the rules.
    ///
    /// Returns the first rule the order breaks, or `Ok` if it breaks none, checking in this order:
    /// - **Prices**, the grid and then the range, each on its own value: the limit `price` of a
    ///   limit, stop-limit, take-profit-limit or fixed-limit trailing order; the trigger price of
    ///   a stop, stop-limit, take-profit or take-profit-limit order; and a trailing order's
    ///   activation price when it has one. A market order has no price to check.
    /// - **Quantity**, against [`quantity`](Self::quantity) and, for a market order, then
    ///   [`market_quantity`](Self::market_quantity).
    /// - **Notional**, `price × quantity`, only where the price is the order's own limit: a limit,
    ///   stop-limit, take-profit-limit or fixed-limit trailing order. Never a market, plain stop
    ///   or trailing-stop order: the venue prices them at a reference price this check cannot
    ///   know. An order with no `price` skips it.
    ///
    /// A closing order is checked like any other: a venue that enforces a minimum notional
    /// enforces it on a close too, so an exemption would move the refusal to the venue.
    ///
    /// # Errors
    /// [`OrderError::InvalidPrecision`] with [`PrecisionLimit::NotOnGrid`] for a value off its
    /// grid, [`OrderError::OutOfBounds`] for one outside its range.
    pub fn check<AssetKey, InstrumentKey>(
        &self,
        kind: OrderKind,
        price: Option<Decimal>,
        quantity: Decimal,
    ) -> Result<(), OrderError<AssetKey, InstrumentKey>> {
        // A match with no wildcard, so that a new `OrderKind` fails to compile until it is
        // classified here.
        let (limit_price, trigger_price, has_own_limit) = match kind {
            OrderKind::Market => (None, None, false),
            OrderKind::Limit => (price, None, true),
            OrderKind::Stop { trigger_price } | OrderKind::TakeProfit { trigger_price } => {
                (None, Some(trigger_price), false)
            }
            OrderKind::StopLimit { trigger_price }
            | OrderKind::TakeProfitLimit { trigger_price } => (price, Some(trigger_price), true),
            OrderKind::TrailingStop {
                activation_price, ..
            } => (None, activation_price, false),
            OrderKind::TrailingStopFixedLimit {
                activation_price, ..
            } => (price, activation_price, true),
            // The venue prices it from its stop and offset: there is no price to read.
            OrderKind::TrailingStopLimit { .. } => (None, None, false),
        };

        if let Some(price) = limit_price {
            self.price.check(OrderField::Price, price)?;
        }
        if let Some(trigger) = trigger_price {
            self.price.check(OrderField::TriggerPrice, trigger)?;
        }
        self.quantity.check(OrderField::Quantity, quantity)?;
        if matches!(kind, OrderKind::Market) {
            self.market_quantity.check(OrderField::Quantity, quantity)?;
        }
        if let Some(price) = limit_price.filter(|_| has_own_limit) {
            // A product beyond `Decimal`'s range is beyond any maximum too.
            let notional = price.saturating_mul(quantity);
            self.notional.check(OrderField::Notional, notional)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::order::TrailingOffsetType;
    use rust_decimal_macros::dec;

    type Error = OrderError<(), ()>;

    /// BTCUSDT-like: tick 0.01, lot 0.001 in 0.001..=100, market lot up to 10, notional 5..=9e6.
    fn rules() -> OrderRules {
        OrderRules::new(
            ValueRule::new(dec!(0.01), dec!(1000000), dec!(0.01)),
            ValueRule::new(dec!(0.001), dec!(100), dec!(0.001)),
            ValueRule::new(dec!(0), dec!(10), dec!(0)),
            ValueRule::new(dec!(5), dec!(9000000), dec!(0)),
        )
    }

    fn check(kind: OrderKind, price: Option<Decimal>, quantity: Decimal) -> Result<(), Error> {
        rules().check(kind, price, quantity)
    }

    fn bounds(field: OrderField, bound: Bound, limit: Decimal, value: Decimal) -> Error {
        OrderError::OutOfBounds(BoundsViolation::new(field, bound, limit, value))
    }

    fn off_grid(field: OrderField, value: Decimal, step: Decimal) -> Error {
        OrderError::InvalidPrecision(PrecisionViolation::new(
            field,
            value,
            PrecisionLimit::NotOnGrid { step },
        ))
    }

    #[test]
    fn zero_or_negative_means_the_part_is_off() {
        let rule = ValueRule::new(dec!(0), dec!(-1), dec!(0));
        assert!(rule.is_off());
        assert!(OrderRules::default().is_off());
        assert!(!rules().is_off());
        // One part on is enough for the rules to be on.
        let on = ValueRule::new(dec!(1), dec!(0), dec!(0));
        let off = ValueRule::default();
        assert!(!OrderRules::new(on, off, off, off).is_off());
        assert!(!OrderRules::new(off, on, off, off).is_off());
        assert!(!OrderRules::new(off, off, on, off).is_off());
        assert!(!OrderRules::new(off, off, off, on).is_off());
        assert_eq!(
            OrderRules::default().check::<(), ()>(OrderKind::Limit, Some(dec!(0.0001)), dec!(7.77)),
            Ok(())
        );
    }

    #[test]
    fn a_step_of_zero_set_by_hand_is_no_grid_not_a_panic() {
        let mut rules = OrderRules::default();
        rules.quantity.step = Some(Decimal::ZERO);
        assert_eq!(
            rules.check::<(), ()>(OrderKind::Market, None, dec!(1.5)),
            Ok(())
        );
    }

    #[test]
    fn a_limit_order_on_the_grid_and_in_range_passes() {
        assert_eq!(
            check(OrderKind::Limit, Some(dec!(50000.12)), dec!(0.5)),
            Ok(())
        );
    }

    #[test]
    fn a_price_off_the_tick_grid_is_refused_with_the_step() {
        assert_eq!(
            check(OrderKind::Limit, Some(dec!(50000.123)), dec!(0.5)),
            Err(off_grid(OrderField::Price, dec!(50000.123), dec!(0.01)))
        );
    }

    #[test]
    fn a_quantity_off_the_lot_grid_is_refused_with_the_step() {
        assert_eq!(
            check(OrderKind::Limit, Some(dec!(50000)), dec!(0.0015)),
            Err(off_grid(OrderField::Quantity, dec!(0.0015), dec!(0.001)))
        );
    }

    #[test]
    fn a_grid_is_counted_from_zero_not_from_the_minimum() {
        // 0.003 is 1.5 steps of 0.002: off a grid counted from zero, though a minimum of 0.002
        // would put an origin-at-minimum grid's next point at 0.004 as well.
        let rules = OrderRules::new(
            ValueRule::default(),
            ValueRule::new(dec!(0.002), dec!(0), dec!(0.002)),
            ValueRule::default(),
            ValueRule::default(),
        );
        assert_eq!(
            rules.check::<(), ()>(OrderKind::Market, None, dec!(0.003)),
            Err(off_grid(OrderField::Quantity, dec!(0.003), dec!(0.002)))
        );
        assert_eq!(
            rules.check::<(), ()>(OrderKind::Market, None, dec!(0.004)),
            Ok(())
        );
    }

    #[test]
    fn a_price_beyond_either_bound_is_refused_with_the_limit() {
        // On the grid, but beyond a bound.
        let rules = OrderRules::new(
            ValueRule::new(dec!(1), dec!(10), dec!(0.5)),
            ValueRule::default(),
            ValueRule::default(),
            ValueRule::default(),
        );
        assert_eq!(
            rules.check::<(), ()>(OrderKind::Limit, Some(dec!(0.5)), dec!(1)),
            Err(bounds(OrderField::Price, Bound::Min, dec!(1), dec!(0.5)))
        );
        assert_eq!(
            rules.check::<(), ()>(OrderKind::Limit, Some(dec!(10.5)), dec!(1)),
            Err(bounds(OrderField::Price, Bound::Max, dec!(10), dec!(10.5)))
        );
        assert_eq!(
            rules.check::<(), ()>(OrderKind::Limit, Some(dec!(1)), dec!(1)),
            Ok(())
        );
        assert_eq!(
            rules.check::<(), ()>(OrderKind::Limit, Some(dec!(10)), dec!(1)),
            Ok(())
        );
    }

    #[test]
    fn a_quantity_beyond_either_bound_is_refused_and_the_bounds_are_inclusive() {
        assert_eq!(
            check(OrderKind::Limit, Some(dec!(50000)), dec!(100.001)),
            Err(bounds(
                OrderField::Quantity,
                Bound::Max,
                dec!(100),
                dec!(100.001)
            ))
        );
        assert_eq!(
            check(OrderKind::Limit, Some(dec!(50000)), dec!(0)),
            Err(bounds(
                OrderField::Quantity,
                Bound::Min,
                dec!(0.001),
                dec!(0)
            ))
        );
        assert_eq!(
            check(OrderKind::Limit, Some(dec!(50000)), dec!(100)),
            Ok(())
        );
        assert_eq!(
            check(OrderKind::Limit, Some(dec!(50000)), dec!(0.001)),
            Ok(())
        );
    }

    #[test]
    fn the_market_quantity_rule_applies_to_a_market_order_only() {
        assert_eq!(
            check(OrderKind::Market, None, dec!(10.5)),
            Err(bounds(
                OrderField::Quantity,
                Bound::Max,
                dec!(10),
                dec!(10.5)
            ))
        );
        assert_eq!(
            check(OrderKind::Limit, Some(dec!(50000)), dec!(10.5)),
            Ok(())
        );
    }

    #[test]
    fn a_market_order_also_obeys_the_ordinary_quantity_rule() {
        assert_eq!(
            check(OrderKind::Market, None, dec!(0.0005)),
            Err(off_grid(OrderField::Quantity, dec!(0.0005), dec!(0.001)))
        );
    }

    #[test]
    fn the_notional_of_a_limit_order_is_its_price_times_its_quantity() {
        assert_eq!(
            check(OrderKind::Limit, Some(dec!(1000)), dec!(0.004)),
            Err(bounds(OrderField::Notional, Bound::Min, dec!(5), dec!(4)))
        );
        assert_eq!(
            check(OrderKind::Limit, Some(dec!(1000)), dec!(0.005)),
            Ok(())
        );
        assert_eq!(
            check(OrderKind::Limit, Some(dec!(1000000)), dec!(10)),
            Err(bounds(
                OrderField::Notional,
                Bound::Max,
                dec!(9000000),
                dec!(10000000)
            ))
        );
    }

    #[test]
    fn a_notional_beyond_the_decimal_range_is_above_the_maximum_not_a_panic() {
        let rules = OrderRules::new(
            ValueRule::default(),
            ValueRule::default(),
            ValueRule::default(),
            ValueRule::new(dec!(0), dec!(1), dec!(0)),
        );
        assert!(matches!(
            rules.check::<(), ()>(OrderKind::Limit, Some(Decimal::MAX), Decimal::MAX),
            Err(OrderError::OutOfBounds(v)) if v.bound == Bound::Max
        ));
    }

    #[test]
    fn a_stop_limit_checks_the_price_the_trigger_and_the_notional() {
        let kind = |trigger_price| OrderKind::StopLimit { trigger_price };
        assert_eq!(
            check(kind(dec!(100.001)), Some(dec!(100)), dec!(1)),
            Err(off_grid(
                OrderField::TriggerPrice,
                dec!(100.001),
                dec!(0.01)
            ))
        );
        assert_eq!(
            check(kind(dec!(100)), Some(dec!(100.001)), dec!(1)),
            Err(off_grid(OrderField::Price, dec!(100.001), dec!(0.01)))
        );
        assert_eq!(
            check(kind(dec!(100)), Some(dec!(1)), dec!(1)),
            Err(bounds(OrderField::Notional, Bound::Min, dec!(5), dec!(1)))
        );
        assert_eq!(check(kind(dec!(100)), Some(dec!(100)), dec!(1)), Ok(()));
    }

    #[test]
    fn a_take_profit_limit_is_checked_like_a_stop_limit() {
        assert_eq!(
            check(
                OrderKind::TakeProfitLimit {
                    trigger_price: dec!(100.001)
                },
                Some(dec!(100)),
                dec!(1)
            ),
            Err(off_grid(
                OrderField::TriggerPrice,
                dec!(100.001),
                dec!(0.01)
            ))
        );
    }

    #[test]
    fn a_plain_stop_checks_its_trigger_and_never_a_notional() {
        // A 1 USDT order would break the minimum notional if the trigger priced it.
        for kind in [
            OrderKind::Stop {
                trigger_price: dec!(1),
            },
            OrderKind::TakeProfit {
                trigger_price: dec!(1),
            },
        ] {
            assert_eq!(check(kind, None, dec!(1)), Ok(()), "{kind:?}");
        }
        assert_eq!(
            check(
                OrderKind::Stop {
                    trigger_price: dec!(1.001)
                },
                None,
                dec!(1)
            ),
            Err(off_grid(OrderField::TriggerPrice, dec!(1.001), dec!(0.01)))
        );
    }

    #[test]
    fn a_market_order_never_checks_a_notional_or_a_price() {
        // A request price on a market order is ignored.
        assert_eq!(
            check(OrderKind::Market, Some(dec!(0.001)), dec!(0.001)),
            Ok(())
        );
    }

    #[test]
    fn a_trailing_stop_checks_its_activation_price_only() {
        let kind = |activation_price| OrderKind::TrailingStop {
            offset: dec!(50),
            offset_type: TrailingOffsetType::BasisPoints,
            activation_price,
        };
        assert_eq!(check(kind(None), None, dec!(1)), Ok(()));
        assert_eq!(check(kind(Some(dec!(1))), None, dec!(1)), Ok(()));
        assert_eq!(
            check(kind(Some(dec!(1.001))), None, dec!(1)),
            Err(off_grid(OrderField::TriggerPrice, dec!(1.001), dec!(0.01)))
        );
    }

    #[test]
    fn a_fixed_limit_trailing_stop_checks_its_limit_activation_and_notional() {
        let kind = |activation_price| OrderKind::TrailingStopFixedLimit {
            offset: dec!(50),
            offset_type: TrailingOffsetType::BasisPoints,
            activation_price,
        };
        assert_eq!(
            check(kind(None), Some(dec!(100.001)), dec!(1)),
            Err(off_grid(OrderField::Price, dec!(100.001), dec!(0.01)))
        );
        assert_eq!(
            check(kind(Some(dec!(1.001))), Some(dec!(100)), dec!(1)),
            Err(off_grid(OrderField::TriggerPrice, dec!(1.001), dec!(0.01)))
        );
        assert_eq!(
            check(kind(None), Some(dec!(1)), dec!(1)),
            Err(bounds(OrderField::Notional, Bound::Min, dec!(5), dec!(1)))
        );
        assert_eq!(check(kind(None), Some(dec!(100)), dec!(1)), Ok(()));
    }

    #[test]
    fn a_limit_order_without_a_price_skips_the_price_and_notional_checks() {
        assert_eq!(check(OrderKind::Limit, None, dec!(1)), Ok(()));
    }

    #[test]
    fn a_grid_break_is_reported_before_a_range_break_of_the_same_value() {
        // 0.0005 is below the minimum quantity and off the grid: the grid is reported.
        assert_eq!(
            check(OrderKind::Limit, Some(dec!(50000)), dec!(0.0005)),
            Err(off_grid(OrderField::Quantity, dec!(0.0005), dec!(0.001)))
        );
    }

    #[test]
    fn a_pre_send_rules_deserializes_from_snake_case_and_defaults_to_enforce() {
        assert_eq!(PreSendRules::default(), PreSendRules::Enforce);
        assert_eq!(
            serde_json::from_str::<PreSendRules>("\"off\"").ok(),
            Some(PreSendRules::Off)
        );
    }

    #[test]
    fn the_refusals_name_the_value_and_the_limit() {
        let text = |result: Result<(), Error>| match result {
            Err(OrderError::OutOfBounds(violation)) => violation.to_string(),
            Err(OrderError::InvalidPrecision(violation)) => violation.to_string(),
            other => panic!("expected a refusal, got {other:?}"),
        };
        assert_eq!(
            text(check(OrderKind::Limit, Some(dec!(1000)), dec!(0.004))),
            "notional 4.000 is below the minimum 5"
        );
        assert_eq!(
            text(check(OrderKind::Limit, Some(dec!(1000)), dec!(101))),
            "quantity 101 is above the maximum 100"
        );
        assert_eq!(
            text(check(OrderKind::Limit, Some(dec!(1000.001)), dec!(1))),
            "price 1000.001 is not a multiple of 0.01"
        );
    }
}
