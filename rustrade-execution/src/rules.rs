//! An instrument's order rules (tick, lot step, minimums and maximums) and the check of an order
//! against them before it is sent.
//!
//! A venue publishes, per instrument, the grid a price and a quantity must sit on and the range
//! each must fall in. A client that loads those rules refuses an order that breaks them without
//! sending it, so the caller gets a typed error that says which value and which limit
//! ([`OrderError::InvalidPrecision`] for the grid, [`OrderError::OutOfBounds`] for the range)
//! rather than a venue's text after a round trip. It never rounds: rounding would place a
//! different order from the one asked for. A caller that sizes or prices an order puts it on the
//! grid first, with [`ValueRule::round_down`] / [`ValueRule::round_up`], or
//! [`OrderRules::round_quantity_down`] / [`OrderRules::round_quantity_up`] for a quantity.
//!
//! A pass is not a guarantee. The check covers the rules a client has loaded, from the last time
//! it loaded them, and only the ones that can be read off the request. The venue has the last say.

use crate::{
    error::{Bound, BoundsViolation, OrderError, OrderField, PrecisionLimit, PrecisionViolation},
    order::{OrderKind, TrailingOffsetType},
};
use rust_decimal::Decimal;
use rustrade_instrument::Side;
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

    /// The largest multiple of [`step`](Self::step) at or below `value`, counted from zero: the
    /// grid the pre-send check uses. A value on the grid comes back unchanged, and so does any
    /// value when there is no step.
    ///
    /// It does not clamp to [`min`](Self::min) or [`max`](Self::max): rounding down can take a
    /// value below the minimum, down to zero, so check the result before sending it.
    ///
    /// A value already on the grid comes back as it is. One that has to move takes the scale of
    /// `value` or `step`, whichever is finer, so it may carry trailing zeros: `0.19000` rounds
    /// down to `0.15000` on a step of `0.05`.
    ///
    /// Returns `None` only when a [`Decimal`] cannot hold the result: within a step of the ends
    /// of its range, or where the result needs more digits than it has.
    #[must_use]
    pub fn round_down(&self, value: Decimal) -> Option<Decimal> {
        round_down_to(value, self.grid())
    }

    /// The smallest multiple of [`step`](Self::step) at or above `value`, counted from zero. As
    /// [`round_down`](Self::round_down) otherwise: rounding up can take a value above the maximum.
    ///
    /// Returns `None` only when a [`Decimal`] cannot hold the result, as for `round_down`.
    #[must_use]
    pub fn round_up(&self, value: Decimal) -> Option<Decimal> {
        round_up_to(value, self.grid())
    }

    /// The step when it makes a grid. A step set to zero or below by hand, not through `new`, is
    /// no grid: `%` would panic on zero.
    fn grid(&self) -> Option<Decimal> {
        self.step.filter(|step| *step > Decimal::ZERO)
    }

    /// `Ok` when `value` is on the grid and in the range, else the first rule it breaks, grid
    /// before range.
    fn check<AssetKey, InstrumentKey>(
        &self,
        field: OrderField,
        value: Decimal,
    ) -> Result<(), OrderError<AssetKey, InstrumentKey>> {
        if let Some(step) = self.grid()
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

/// The largest multiple of `step` at or below `value`, or `value` itself with no step.
fn round_down_to(value: Decimal, step: Option<Decimal>) -> Option<Decimal> {
    let Some(step) = step else {
        return Some(value);
    };
    // `%` takes the sign of `value`, so `value - remainder` is the neighbour toward zero.
    let remainder = value % step;
    if remainder.is_zero() {
        return Some(value);
    }
    let toward_zero = value - remainder;
    let rounded = if remainder.is_sign_negative() {
        toward_zero.checked_sub(step)?
    } else {
        toward_zero
    };
    on_grid(rounded, step)
}

/// The smallest multiple of `step` at or above `value`, or `value` itself with no step.
fn round_up_to(value: Decimal, step: Option<Decimal>) -> Option<Decimal> {
    let Some(step) = step else {
        return Some(value);
    };
    let remainder = value % step;
    if remainder.is_zero() {
        return Some(value);
    }
    let toward_zero = value - remainder;
    let rounded = if remainder.is_sign_negative() {
        toward_zero
    } else {
        toward_zero.checked_add(step)?
    };
    on_grid(rounded, step)
}

/// `value` if it is a multiple of `step`. A difference that needs more digits than a `Decimal`
/// holds is rounded, which can take it off the grid: then there is no answer.
fn on_grid(value: Decimal, step: Decimal) -> Option<Decimal> {
    (value % step).is_zero().then_some(value)
}

/// The smallest step whose grid lies on both `a`'s and `b`'s, their least common multiple, or
/// `None` when a `Decimal` cannot hold it. Both must be positive.
fn common_step(a: Decimal, b: Decimal) -> Option<Decimal> {
    // As whole numbers of the finer unit, `10^-scale`.
    let scale = a.scale().max(b.scale());
    let whole = |step: Decimal| {
        10_i128
            .checked_pow(scale - step.scale())
            .and_then(|factor| step.mantissa().checked_mul(factor))
    };
    let (a, b) = (whole(a)?, whole(b)?);
    let (mut x, mut y) = (a, b);
    while y != 0 {
        (x, y) = (y, x % y);
    }
    let lcm = (a / x).checked_mul(b)?;
    Decimal::try_from_i128_with_scale(lcm, scale)
        .ok()
        .map(|step| step.normalize())
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
    /// The part of [`notional`](Self::notional) the venue also applies to a market order: off
    /// where the venue exempts market orders. Its `step` is not used.
    ///
    /// [`check`](Self::check) never applies it: a market order has no price of its own, and the
    /// venue measures its notional at a price of its own choosing, such as a reference price or
    /// an average of recent trades, that the check cannot know. A caller that holds a price it
    /// trusts can compare that price times the quantity with this rule's `min` and `max`.
    #[serde(default)]
    pub market_notional: ValueRule,
    /// The range of a trailing order's offset, in basis points, when its trigger trails above the
    /// market and fires on a rise: a buy trailing stop. Its `step` is not used.
    #[serde(default)]
    pub trailing_offset_above: ValueRule,
    /// The range of a trailing order's offset, in basis points, when its trigger trails below the
    /// market and fires on a fall: a sell trailing stop. Its `step` is not used.
    #[serde(default)]
    pub trailing_offset_below: ValueRule,
}

impl OrderRules {
    /// Rules with `price`, `quantity`, `market_quantity` and `notional` as given, and the rest
    /// off: add them with [`with_market_notional`](Self::with_market_notional) and
    /// [`with_trailing_offsets`](Self::with_trailing_offsets).
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
            market_notional: ValueRule::default(),
            trailing_offset_above: ValueRule::default(),
            trailing_offset_below: ValueRule::default(),
        }
    }

    /// These rules with [`market_notional`](Self::market_notional) as given.
    #[must_use]
    pub fn with_market_notional(self, market_notional: ValueRule) -> Self {
        Self {
            market_notional,
            ..self
        }
    }

    /// These rules with [`trailing_offset_above`](Self::trailing_offset_above) and
    /// [`trailing_offset_below`](Self::trailing_offset_below) as given.
    #[must_use]
    pub fn with_trailing_offsets(self, above: ValueRule, below: ValueRule) -> Self {
        Self {
            trailing_offset_above: above,
            trailing_offset_below: below,
            ..self
        }
    }

    /// Whether these rules check nothing.
    pub fn is_off(&self) -> bool {
        self.price.is_off()
            && self.quantity.is_off()
            && self.market_quantity.is_off()
            && self.notional.is_off()
            && self.market_notional.is_off()
            && self.trailing_offset_above.is_off()
            && self.trailing_offset_below.is_off()
    }

    /// The largest quantity at or below `quantity` that an order of `kind` may have on the grid:
    /// a multiple of [`quantity`](Self::quantity)'s step and, for a market order, of
    /// [`market_quantity`](Self::market_quantity)'s too, so a multiple of the smallest step both
    /// grids share. As [`ValueRule::round_down`] otherwise: it does not clamp to either rule's
    /// range, so check the result before sending it.
    ///
    /// Returns `None` when a [`Decimal`] cannot hold the result, as for
    /// [`ValueRule::round_down`], or cannot hold the step of the grid the two steps share.
    #[must_use]
    pub fn round_quantity_down(&self, kind: OrderKind, quantity: Decimal) -> Option<Decimal> {
        round_down_to(quantity, self.quantity_grid(kind)?)
    }

    /// The smallest quantity at or above `quantity` that an order of `kind` may have on the grid.
    /// As [`round_quantity_down`](Self::round_quantity_down) otherwise.
    #[must_use]
    pub fn round_quantity_up(&self, kind: OrderKind, quantity: Decimal) -> Option<Decimal> {
        round_up_to(quantity, self.quantity_grid(kind)?)
    }

    /// The step of the grid an order of `kind`'s quantity must sit on: `Some(None)` when there is
    /// none, `None` when a `Decimal` cannot hold the step of the grid the two steps share.
    fn quantity_grid(&self, kind: OrderKind) -> Option<Option<Decimal>> {
        let quantity = self.quantity.grid();
        if !matches!(kind, OrderKind::Market) {
            return Some(quantity);
        }
        match (quantity, self.market_quantity.grid()) {
            (Some(a), Some(b)) => common_step(a, b).map(Some),
            (step, None) | (None, step) => Some(step),
        }
    }

    /// Check an order on `side` of `kind`, with its limit `price` and `quantity`, against the
    /// rules.
    ///
    /// Returns the first rule the order breaks, or `Ok` if it breaks none, checking in this order:
    /// - **Prices**, the grid and then the range, each on its own value: the limit `price` of a
    ///   limit, stop-limit, take-profit-limit or fixed-limit trailing order; the trigger price of
    ///   a stop, stop-limit, take-profit or take-profit-limit order; and a trailing order's
    ///   activation price when it has one. A market order has no price to check.
    /// - **Trailing offset**, of a trailing stop of any kind: a buy's against
    ///   [`trailing_offset_above`](Self::trailing_offset_above), a sell's against
    ///   [`trailing_offset_below`](Self::trailing_offset_below). An offset in
    ///   [`Percentage`](TrailingOffsetType::Percentage) is compared in percent, the limits divided
    ///   by 100, so a refusal names both in the order's own unit. An
    ///   [`Absolute`](TrailingOffsetType::Absolute) offset is not checked: its size in basis
    ///   points depends on a price the check does not have.
    /// - **Quantity**, against [`quantity`](Self::quantity) and, for a market order, then
    ///   [`market_quantity`](Self::market_quantity).
    /// - **Notional**, `price × quantity`, only where the price is the order's own limit: a limit,
    ///   stop-limit, take-profit-limit or fixed-limit trailing order. Never a market, plain stop
    ///   or trailing-stop order: the venue prices them at a reference price this check cannot
    ///   know, which is also why [`market_notional`](Self::market_notional) is never checked. An
    ///   order with no `price` skips it.
    ///
    /// A closing order is checked like any other: a venue that enforces a minimum notional
    /// enforces it on a close too, so an exemption would move the refusal to the venue.
    ///
    /// # Errors
    /// [`OrderError::InvalidPrecision`] with [`PrecisionLimit::NotOnGrid`] for a value off its
    /// grid, [`OrderError::OutOfBounds`] for one outside its range.
    pub fn check<AssetKey, InstrumentKey>(
        &self,
        side: Side,
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
            // Never placed, but its prices are the venue's and so on its grids.
            OrderKind::ConditionalUnresolved {
                family,
                trigger_price,
            } => (
                price.filter(|_| family.has_limit_price()),
                trigger_price,
                family.has_limit_price(),
            ),
        };

        if let Some(price) = limit_price {
            self.price.check(OrderField::Price, price)?;
        }
        if let Some(trigger) = trigger_price {
            self.price.check(OrderField::TriggerPrice, trigger)?;
        }
        self.check_trailing_offset(side, kind)?;
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

    /// Check a trailing order's offset against the range for its direction: see
    /// [`check`](Self::check).
    fn check_trailing_offset<AssetKey, InstrumentKey>(
        &self,
        side: Side,
        kind: OrderKind,
    ) -> Result<(), OrderError<AssetKey, InstrumentKey>> {
        let (offset, offset_type) = match kind {
            OrderKind::TrailingStop {
                offset,
                offset_type,
                ..
            }
            | OrderKind::TrailingStopLimit {
                offset,
                offset_type,
                ..
            }
            | OrderKind::TrailingStopFixedLimit {
                offset,
                offset_type,
                ..
            } => (offset, offset_type),
            // No offset to check, and never placed: an unresolved kind's offset is not reported.
            OrderKind::Market
            | OrderKind::Limit
            | OrderKind::Stop { .. }
            | OrderKind::StopLimit { .. }
            | OrderKind::TakeProfit { .. }
            | OrderKind::TakeProfitLimit { .. }
            | OrderKind::ConditionalUnresolved { .. } => return Ok(()),
        };
        // Every trailing kind is a stop: a buy's trigger trails above the market, a sell's below.
        let basis_points = match side {
            Side::Buy => &self.trailing_offset_above,
            Side::Sell => &self.trailing_offset_below,
        };
        // A percent is 100 basis points. Dividing by 100 cannot overflow.
        let percent = |bound: Option<Decimal>| bound.map(|bound| bound / Decimal::ONE_HUNDRED);
        // The range alone: the step is not used.
        let range = match offset_type {
            TrailingOffsetType::BasisPoints => ValueRule {
                step: None,
                ..*basis_points
            },
            TrailingOffsetType::Percentage => ValueRule {
                min: percent(basis_points.min),
                max: percent(basis_points.max),
                step: None,
            },
            TrailingOffsetType::Absolute => return Ok(()),
        };
        range.check(OrderField::TrailingOffset, offset)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::order::{ConditionalFamily, TrailingOffsetType};
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
        rules().check(Side::Buy, kind, price, quantity)
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
            OrderRules::default().check::<(), ()>(
                Side::Buy,
                OrderKind::Limit,
                Some(dec!(0.0001)),
                dec!(7.77)
            ),
            Ok(())
        );
    }

    #[test]
    fn a_step_of_zero_set_by_hand_is_no_grid_not_a_panic() {
        let mut rules = OrderRules::default();
        rules.quantity.step = Some(Decimal::ZERO);
        assert_eq!(
            rules.check::<(), ()>(Side::Buy, OrderKind::Market, None, dec!(1.5)),
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
            rules.check::<(), ()>(Side::Buy, OrderKind::Market, None, dec!(0.003)),
            Err(off_grid(OrderField::Quantity, dec!(0.003), dec!(0.002)))
        );
        assert_eq!(
            rules.check::<(), ()>(Side::Buy, OrderKind::Market, None, dec!(0.004)),
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
            rules.check::<(), ()>(Side::Buy, OrderKind::Limit, Some(dec!(0.5)), dec!(1)),
            Err(bounds(OrderField::Price, Bound::Min, dec!(1), dec!(0.5)))
        );
        assert_eq!(
            rules.check::<(), ()>(Side::Buy, OrderKind::Limit, Some(dec!(10.5)), dec!(1)),
            Err(bounds(OrderField::Price, Bound::Max, dec!(10), dec!(10.5)))
        );
        assert_eq!(
            rules.check::<(), ()>(Side::Buy, OrderKind::Limit, Some(dec!(1)), dec!(1)),
            Ok(())
        );
        assert_eq!(
            rules.check::<(), ()>(Side::Buy, OrderKind::Limit, Some(dec!(10)), dec!(1)),
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
            rules.check::<(), ()>(Side::Buy, OrderKind::Limit, Some(Decimal::MAX), Decimal::MAX),
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

    /// An unresolved kind is checked as its family: a limit family's price and notional, and
    /// any trigger. A stop family ignores a request price.
    #[test]
    fn an_unresolved_kind_is_checked_as_its_family() {
        let kind = |family, trigger_price| OrderKind::ConditionalUnresolved {
            family,
            trigger_price,
        };
        let stop_limit = |trigger| kind(ConditionalFamily::StopLimit, trigger);
        assert_eq!(
            check(stop_limit(Some(dec!(100.001))), Some(dec!(100)), dec!(1)),
            Err(off_grid(
                OrderField::TriggerPrice,
                dec!(100.001),
                dec!(0.01)
            ))
        );
        assert_eq!(
            check(stop_limit(None), Some(dec!(100.001)), dec!(1)),
            Err(off_grid(OrderField::Price, dec!(100.001), dec!(0.01)))
        );
        assert_eq!(
            check(stop_limit(None), Some(dec!(1)), dec!(1)),
            Err(bounds(OrderField::Notional, Bound::Min, dec!(5), dec!(1)))
        );
        assert_eq!(check(stop_limit(None), Some(dec!(100)), dec!(1)), Ok(()));
        // A stop family carries no limit: an off-grid request price is not read, and no notional.
        assert_eq!(
            check(
                kind(ConditionalFamily::Stop, Some(dec!(1))),
                Some(dec!(1.001)),
                dec!(1)
            ),
            Ok(())
        );
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

    fn stepped(step: Decimal) -> ValueRule {
        ValueRule::new(dec!(0), dec!(0), step)
    }

    #[test]
    fn a_value_on_the_grid_rounds_to_itself_either_way() {
        for (step, value) in [
            (dec!(0.01), dec!(50000.12)),
            (dec!(0.05), dec!(1.15)),
            (dec!(0.05), dec!(-1.15)),
            (dec!(0.001), dec!(0)),
        ] {
            assert_eq!(
                stepped(step).round_down(value),
                Some(value),
                "{step} {value}"
            );
            assert_eq!(stepped(step).round_up(value), Some(value), "{step} {value}");
        }
    }

    #[test]
    fn a_value_off_the_grid_rounds_to_the_neighbour_below_or_above() {
        let rule = stepped(dec!(0.01));
        assert_eq!(rule.round_down(dec!(50000.129)), Some(dec!(50000.12)));
        assert_eq!(rule.round_up(dec!(50000.121)), Some(dec!(50000.13)));
    }

    #[test]
    fn a_step_that_is_not_a_power_of_ten_rounds_to_its_own_multiples() {
        let rule = stepped(dec!(0.05));
        assert_eq!(rule.round_down(dec!(1.07)), Some(dec!(1.05)));
        assert_eq!(rule.round_up(dec!(1.07)), Some(dec!(1.10)));
        assert_eq!(rule.round_down(dec!(0.04)), Some(dec!(0)));
        assert_eq!(rule.round_up(dec!(0.01)), Some(dec!(0.05)));
        // A value that moves takes the finer scale; one on the grid keeps its own.
        assert_eq!(
            rule.round_down(dec!(0.19000))
                .map(|value| value.to_string()),
            Some("0.15000".to_owned())
        );
        assert_eq!(
            stepped(dec!(0.005))
                .round_down(dec!(0.10))
                .map(|value| value.to_string()),
            Some("0.10".to_owned())
        );
    }

    #[test]
    fn a_negative_value_rounds_down_away_from_zero_and_up_toward_it() {
        let rule = stepped(dec!(0.05));
        assert_eq!(rule.round_down(dec!(-1.07)), Some(dec!(-1.10)));
        assert_eq!(rule.round_up(dec!(-1.07)), Some(dec!(-1.05)));
        assert_eq!(rule.round_up(dec!(-0.01)), Some(dec!(0)));
    }

    #[test]
    fn with_no_step_a_value_comes_back_unchanged() {
        let mut rule = ValueRule::new(dec!(1), dec!(10), dec!(0));
        assert_eq!(rule.round_down(dec!(1.2345)), Some(dec!(1.2345)));
        assert_eq!(rule.round_up(dec!(1.2345)), Some(dec!(1.2345)));
        // A zero step set by hand is no grid either, not a division by zero.
        rule.step = Some(Decimal::ZERO);
        assert_eq!(rule.round_down(dec!(1.2345)), Some(dec!(1.2345)));
    }

    #[test]
    fn rounding_does_not_clamp_to_the_range() {
        let rule = ValueRule::new(dec!(0.002), dec!(1), dec!(0.002));
        assert_eq!(rule.round_down(dec!(0.001)), Some(dec!(0)));
        assert_eq!(rule.round_up(dec!(1.001)), Some(dec!(1.002)));
    }

    #[test]
    fn a_result_beyond_the_decimal_range_is_none_not_a_panic() {
        // Decimal::MAX is odd, so the multiple of 2 above it is out of range; the one below is not.
        let rule = stepped(dec!(2));
        assert_eq!(rule.round_up(Decimal::MAX), None);
        assert_eq!(rule.round_down(Decimal::MAX), Some(Decimal::MAX - dec!(1)));
        assert_eq!(rule.round_down(Decimal::MIN), None);
        assert_eq!(rule.round_up(Decimal::MIN), Some(Decimal::MIN + dec!(1)));
    }

    #[test]
    fn a_result_that_needs_more_digits_than_a_decimal_holds_is_none() {
        // Decimal::MAX is a multiple of 3, so of 0.3; one less is not, and its neighbours on the
        // grid of 0.3 need a decimal place a Decimal that large has no room for.
        let rule = stepped(dec!(0.3));
        assert_eq!(rule.round_down(Decimal::MAX), Some(Decimal::MAX));
        assert_eq!(rule.round_down(Decimal::MAX - dec!(1)), None);
        assert_eq!(rule.round_up(Decimal::MAX - dec!(1)), None);
    }

    /// Quantity step 0.05, market quantity step 0.02: a market order's grid is 0.1.
    fn two_quantity_steps() -> OrderRules {
        OrderRules::new(
            ValueRule::default(),
            stepped(dec!(0.05)),
            stepped(dec!(0.02)),
            ValueRule::default(),
        )
    }

    #[test]
    fn a_market_quantity_rounds_to_the_grid_both_quantity_steps_share() {
        let rules = two_quantity_steps();
        assert_eq!(
            rules.round_quantity_down(OrderKind::Market, dec!(0.19)),
            Some(dec!(0.1))
        );
        assert_eq!(
            rules.round_quantity_up(OrderKind::Market, dec!(0.11)),
            Some(dec!(0.2))
        );
        // The result passes both grids in the check.
        assert_eq!(
            rules.check::<(), ()>(Side::Buy, OrderKind::Market, None, dec!(0.1)),
            Ok(())
        );
    }

    #[test]
    fn any_other_quantity_rounds_to_the_quantity_step_alone() {
        let rules = two_quantity_steps();
        assert_eq!(
            rules.round_quantity_down(OrderKind::Limit, dec!(0.19)),
            Some(dec!(0.15))
        );
        assert_eq!(
            rules.round_quantity_up(
                OrderKind::Stop {
                    trigger_price: dec!(1)
                },
                dec!(0.11)
            ),
            Some(dec!(0.15))
        );
    }

    #[test]
    fn a_market_quantity_with_one_step_off_rounds_to_the_other() {
        let quantity_only = OrderRules::new(
            ValueRule::default(),
            stepped(dec!(0.05)),
            ValueRule::default(),
            ValueRule::default(),
        );
        assert_eq!(
            quantity_only.round_quantity_down(OrderKind::Market, dec!(0.19)),
            Some(dec!(0.15))
        );
        let market_only = OrderRules::new(
            ValueRule::default(),
            ValueRule::default(),
            stepped(dec!(0.02)),
            ValueRule::default(),
        );
        assert_eq!(
            market_only.round_quantity_down(OrderKind::Market, dec!(0.19)),
            Some(dec!(0.18))
        );
        assert_eq!(
            OrderRules::default().round_quantity_up(OrderKind::Market, dec!(0.19)),
            Some(dec!(0.19))
        );
    }

    #[test]
    fn the_shared_grid_is_the_least_common_multiple_of_the_steps() {
        assert_eq!(common_step(dec!(0.05), dec!(0.02)), Some(dec!(0.1)));
        assert_eq!(common_step(dec!(0.001), dec!(0.01)), Some(dec!(0.01)));
        assert_eq!(common_step(dec!(0.01), dec!(0.001)), Some(dec!(0.01)));
        assert_eq!(common_step(dec!(0.4), dec!(0.6)), Some(dec!(1.2)));
        assert_eq!(common_step(dec!(0.4), dec!(6)), Some(dec!(6)));
        assert_eq!(common_step(dec!(3), dec!(0.5)), Some(dec!(3)));
        assert_eq!(common_step(dec!(0.25), dec!(0.25)), Some(dec!(0.25)));
    }

    #[test]
    fn steps_whose_shared_grid_a_decimal_cannot_hold_round_to_none() {
        // Two near-coprime 28-digit steps: their least common multiple has some 56 digits.
        let a = Decimal::from_i128_with_scale(7_922_816_251_426_433_759_354_395_033, 27);
        let b = Decimal::from_i128_with_scale(7_922_816_251_426_433_759_354_395_031, 27);
        assert_eq!(common_step(a, b), None);
        let rules = OrderRules::new(
            ValueRule::default(),
            stepped(a),
            stepped(b),
            ValueRule::default(),
        );
        assert_eq!(rules.round_quantity_down(OrderKind::Market, dec!(1)), None);
        assert_eq!(rules.round_quantity_up(OrderKind::Market, dec!(1)), None);
        // A limit order needs only one of them.
        assert_eq!(rules.round_quantity_down(OrderKind::Limit, a), Some(a));
    }

    /// Trailing offsets of 10..=2000 basis points above the market and 20..=1000 below.
    fn trailing_rules() -> OrderRules {
        OrderRules::default().with_trailing_offsets(
            ValueRule::new(dec!(10), dec!(2000), dec!(0)),
            ValueRule::new(dec!(20), dec!(1000), dec!(0)),
        )
    }

    fn trailing_stop(offset: Decimal, offset_type: TrailingOffsetType) -> OrderKind {
        OrderKind::TrailingStop {
            offset,
            offset_type,
            activation_price: None,
        }
    }

    #[test]
    fn a_buy_trailing_offset_is_checked_against_the_range_above_and_a_sell_against_below() {
        let check = |side, offset| {
            trailing_rules().check::<(), ()>(
                side,
                trailing_stop(offset, TrailingOffsetType::BasisPoints),
                None,
                dec!(1),
            )
        };
        // 15 is inside the range above (from 10) but under the range below (from 20).
        assert_eq!(check(Side::Buy, dec!(15)), Ok(()));
        assert_eq!(
            check(Side::Sell, dec!(15)),
            Err(bounds(
                OrderField::TrailingOffset,
                Bound::Min,
                dec!(20),
                dec!(15)
            ))
        );
        // 1500 is inside the range above (to 2000) but over the range below (to 1000).
        assert_eq!(check(Side::Buy, dec!(1500)), Ok(()));
        assert_eq!(
            check(Side::Sell, dec!(1500)),
            Err(bounds(
                OrderField::TrailingOffset,
                Bound::Max,
                dec!(1000),
                dec!(1500)
            ))
        );
        assert_eq!(
            check(Side::Buy, dec!(9)),
            Err(bounds(
                OrderField::TrailingOffset,
                Bound::Min,
                dec!(10),
                dec!(9)
            ))
        );
        assert_eq!(
            check(Side::Buy, dec!(2001)),
            Err(bounds(
                OrderField::TrailingOffset,
                Bound::Max,
                dec!(2000),
                dec!(2001)
            ))
        );
        // The bounds are inclusive.
        for (side, offset) in [
            (Side::Buy, dec!(10)),
            (Side::Buy, dec!(2000)),
            (Side::Sell, dec!(20)),
            (Side::Sell, dec!(1000)),
        ] {
            assert_eq!(check(side, offset), Ok(()), "{side:?} {offset}");
        }
    }

    #[test]
    fn a_percentage_offset_is_checked_and_reported_in_percent() {
        let check = |offset| {
            trailing_rules().check::<(), ()>(
                Side::Buy,
                trailing_stop(offset, TrailingOffsetType::Percentage),
                None,
                dec!(1),
            )
        };
        // 10..=2000 basis points is 0.1..=20 percent.
        assert_eq!(check(dec!(0.1)), Ok(()));
        assert_eq!(check(dec!(20)), Ok(()));
        assert_eq!(
            check(dec!(0.09)),
            Err(bounds(
                OrderField::TrailingOffset,
                Bound::Min,
                dec!(0.1),
                dec!(0.09)
            ))
        );
        assert_eq!(
            check(dec!(20.5)),
            Err(bounds(
                OrderField::TrailingOffset,
                Bound::Max,
                dec!(20),
                dec!(20.5)
            ))
        );
    }

    #[test]
    fn an_absolute_offset_is_not_checked() {
        assert_eq!(
            trailing_rules().check::<(), ()>(
                Side::Buy,
                trailing_stop(dec!(0.0001), TrailingOffsetType::Absolute),
                None,
                dec!(1),
            ),
            Ok(())
        );
    }

    #[test]
    fn every_trailing_kind_checks_its_offset() {
        let offset = dec!(5);
        for kind in [
            OrderKind::TrailingStopLimit {
                offset,
                offset_type: TrailingOffsetType::BasisPoints,
                stop_price: dec!(100),
                limit_offset: dec!(1),
            },
            OrderKind::TrailingStopFixedLimit {
                offset,
                offset_type: TrailingOffsetType::BasisPoints,
                activation_price: None,
            },
        ] {
            assert_eq!(
                trailing_rules().check::<(), ()>(Side::Sell, kind, Some(dec!(100)), dec!(1)),
                Err(bounds(
                    OrderField::TrailingOffset,
                    Bound::Min,
                    dec!(20),
                    offset
                )),
                "{kind:?}"
            );
        }
    }

    #[test]
    fn a_trailing_offset_is_checked_after_the_prices_and_before_the_quantity() {
        let rules = rules().with_trailing_offsets(
            ValueRule::new(dec!(10), dec!(0), dec!(0)),
            ValueRule::default(),
        );
        let kind = |activation_price| OrderKind::TrailingStop {
            offset: dec!(1),
            offset_type: TrailingOffsetType::BasisPoints,
            activation_price,
        };
        assert_eq!(
            rules.check::<(), ()>(Side::Buy, kind(Some(dec!(1.001))), None, dec!(0.0005)),
            Err(off_grid(OrderField::TriggerPrice, dec!(1.001), dec!(0.01)))
        );
        assert_eq!(
            rules.check::<(), ()>(Side::Buy, kind(None), None, dec!(0.0005)),
            Err(bounds(
                OrderField::TrailingOffset,
                Bound::Min,
                dec!(10),
                dec!(1)
            ))
        );
    }

    #[test]
    fn a_trailing_rules_step_is_not_used() {
        let rules = OrderRules::default().with_trailing_offsets(
            ValueRule::new(dec!(0), dec!(0), dec!(10)),
            ValueRule::new(dec!(0), dec!(0), dec!(10)),
        );
        for side in [Side::Buy, Side::Sell] {
            assert_eq!(
                rules.check::<(), ()>(
                    side,
                    trailing_stop(dec!(15), TrailingOffsetType::BasisPoints),
                    None,
                    dec!(1)
                ),
                Ok(()),
                "{side:?}"
            );
        }
    }

    #[test]
    fn the_trailing_ranges_leave_other_kinds_alone() {
        let rules = OrderRules::default().with_trailing_offsets(
            ValueRule::new(dec!(10), dec!(20), dec!(0)),
            ValueRule::new(dec!(10), dec!(20), dec!(0)),
        );
        for kind in [
            OrderKind::Market,
            OrderKind::Limit,
            OrderKind::Stop {
                trigger_price: dec!(1),
            },
            OrderKind::ConditionalUnresolved {
                family: ConditionalFamily::Stop,
                trigger_price: None,
            },
        ] {
            assert_eq!(
                rules.check::<(), ()>(Side::Sell, kind, Some(dec!(1)), dec!(1)),
                Ok(()),
                "{kind:?}"
            );
        }
    }

    #[test]
    fn the_market_notional_is_never_checked() {
        let rules =
            OrderRules::default().with_market_notional(ValueRule::new(dec!(5), dec!(10), dec!(0)));
        for (kind, price) in [
            (OrderKind::Market, Some(dec!(1))),
            (OrderKind::Limit, Some(dec!(1))),
        ] {
            assert_eq!(
                rules.check::<(), ()>(Side::Buy, kind, price, dec!(1)),
                Ok(()),
                "{kind:?}"
            );
        }
    }

    #[test]
    fn the_new_rules_count_towards_being_on_and_new_leaves_them_off() {
        let on = ValueRule::new(dec!(1), dec!(0), dec!(0));
        let off = ValueRule::default();
        assert!(!OrderRules::default().with_market_notional(on).is_off());
        assert!(
            !OrderRules::default()
                .with_trailing_offsets(on, off)
                .is_off()
        );
        assert!(
            !OrderRules::default()
                .with_trailing_offsets(off, on)
                .is_off()
        );
        let built = rules();
        assert!(built.market_notional.is_off());
        assert!(built.trailing_offset_above.is_off());
        assert!(built.trailing_offset_below.is_off());
        // The builders leave the other parts as they were.
        let with = built
            .with_market_notional(on)
            .with_trailing_offsets(on, off);
        assert_eq!(with.price, built.price);
        assert_eq!(with.notional, built.notional);
        assert_eq!(with.market_notional, on);
        assert_eq!(with.trailing_offset_above, on);
        assert_eq!(with.trailing_offset_below, off);
    }

    /// Rules serialised before the market notional and trailing offsets were added still load,
    /// with those parts off.
    #[test]
    fn rules_serialised_without_the_newer_parts_load_with_them_off() {
        let rule = r#"{"min":"1","max":null,"step":null}"#;
        let json = format!(
            r#"{{"price":{rule},"quantity":{rule},"market_quantity":{rule},"notional":{rule}}}"#
        );
        let loaded: OrderRules = match serde_json::from_str(&json) {
            Ok(loaded) => loaded,
            Err(error) => panic!("older rules should load: {error}"),
        };
        let on = ValueRule::new(dec!(1), dec!(0), dec!(0));
        assert_eq!(loaded, OrderRules::new(on, on, on, on));
    }
}
