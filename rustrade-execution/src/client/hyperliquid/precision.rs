//! Hyperliquid's precision rules for order sizes and prices, and the check an order passes before
//! it is sent.
//!
//! From Hyperliquid's [tick and lot size rules]:
//! - A **size** has at most the asset's `szDecimals` decimal places.
//! - A **price** has at most 5 significant figures, and at most `MAX_DECIMALS - szDecimals`
//!   decimal places, where `MAX_DECIMALS` is 6 for perpetuals and 8 for spot. An integer price is
//!   always valid, whatever its number of significant figures.
//!
//! A spot pair's `szDecimals` is its base token's.
//!
//! [tick and lot size rules]: https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/tick-and-lot-size

use crate::{
    error::{OrderField, PrecisionLimit, PrecisionViolation},
    order::OrderKind,
};
use hyperliquid_rust_sdk::{ClientLimit, ClientOrder, ClientTrigger};
use rust_decimal::{Decimal, RoundingStrategy, prelude::ToPrimitive};
use std::str::FromStr;

/// The most decimal places a perpetual's price may have, before its `szDecimals` are taken off.
const PERP_MAX_DECIMALS: u32 = 6;

/// The most decimal places a spot pair's price may have, before its `szDecimals` are taken off.
const SPOT_MAX_DECIMALS: u32 = 8;

/// The decimal places Hyperliquid's SDK writes a number to before trimming trailing zeros. It
/// takes every size and price as an `f64`, so a value is sent exactly only if formatting that
/// `f64` to this many places gives the value back.
const SDK_WIRE_DECIMALS: usize = 8;

/// Hyperliquid's precision rules for one market's orders. See the [module docs](self).
///
/// Read it from [`HyperliquidClient::order_precision`](super::HyperliquidClient::order_precision)
/// or [`HyperliquidSpotClient::order_precision`](super::spot::HyperliquidSpotClient::order_precision),
/// or build it from metadata with [`perp`](Self::perp) or [`spot`](Self::spot).
///
/// The clients refuse an order whose quantity, price or trigger price breaks these rules with
/// [`OrderError::InvalidPrecision`](crate::error::OrderError::InvalidPrecision), and never round
/// it themselves. Round with [`round_quantity`](Self::round_quantity) and
/// [`round_price`](Self::round_price) first, in the direction your strategy needs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct OrderPrecision {
    sz_decimals: u32,
    max_decimals: u32,
}

impl OrderPrecision {
    /// The most significant figures a price that is not an integer may have.
    pub const MAX_SIGNIFICANT_FIGURES: u32 = 5;

    /// The rules for a perpetual whose `szDecimals` is `sz_decimals`.
    pub const fn perp(sz_decimals: u32) -> Self {
        Self {
            sz_decimals,
            max_decimals: PERP_MAX_DECIMALS,
        }
    }

    /// The rules for a spot pair whose base token's `szDecimals` is `base_sz_decimals`.
    pub const fn spot(base_sz_decimals: u32) -> Self {
        Self {
            sz_decimals: base_sz_decimals,
            max_decimals: SPOT_MAX_DECIMALS,
        }
    }

    /// The most decimal places a quantity may have: the asset's `szDecimals`.
    pub const fn quantity_decimals(&self) -> u32 {
        self.sz_decimals
    }

    /// The most decimal places a price may have: `MAX_DECIMALS - szDecimals`, and never fewer
    /// than zero.
    pub const fn price_decimals(&self) -> u32 {
        self.max_decimals.saturating_sub(self.sz_decimals)
    }

    /// Check that `quantity` is positive, meets the rules, and can be sent exactly.
    ///
    /// # Errors
    ///
    /// The rule `quantity` breaks, as a [`PrecisionViolation`] naming
    /// [`OrderField::Quantity`].
    pub fn check_quantity(&self, quantity: Decimal) -> Result<(), PrecisionViolation> {
        self.wire_quantity(quantity).map(drop)
    }

    /// Check that the limit price `price` is positive, meets the rules, and can be sent exactly.
    ///
    /// # Errors
    ///
    /// The rule `price` breaks, as a [`PrecisionViolation`] naming [`OrderField::Price`].
    pub fn check_price(&self, price: Decimal) -> Result<(), PrecisionViolation> {
        self.wire_price(OrderField::Price, price).map(drop)
    }

    /// Check a stop or take-profit order's `trigger_price` as [`check_price`](Self::check_price)
    /// checks a limit price: it follows the same rules.
    ///
    /// # Errors
    ///
    /// The rule `trigger_price` breaks, as a [`PrecisionViolation`] naming
    /// [`OrderField::TriggerPrice`].
    pub fn check_trigger_price(&self, trigger_price: Decimal) -> Result<(), PrecisionViolation> {
        self.wire_price(OrderField::TriggerPrice, trigger_price)
            .map(drop)
    }

    /// `quantity` rounded to the decimal places the rules allow, by `strategy`.
    ///
    /// A quantity smaller than the smallest step can round to zero, which
    /// [`check_quantity`](Self::check_quantity) refuses.
    pub fn round_quantity(&self, quantity: Decimal, strategy: RoundingStrategy) -> Decimal {
        quantity
            .round_dp_with_strategy(self.quantity_decimals(), strategy)
            .normalize()
    }

    /// `price` rounded by `strategy` to the decimal places and significant figures the rules
    /// allow. An integer price is returned unchanged.
    ///
    /// A price smaller than the smallest step the decimal places allow can round to zero, which
    /// [`check_price`](Self::check_price) refuses.
    pub fn round_price(&self, price: Decimal, strategy: RoundingStrategy) -> Decimal {
        let price = price.normalize();
        if price.scale() == 0 {
            return price;
        }
        // The decimal places that keep `MAX_SIGNIFICANT_FIGURES`: a value with `digits` digits
        // and `scale` decimal places has `digits - scale` digits before the point.
        let significant = (price.scale() + Self::MAX_SIGNIFICANT_FIGURES)
            .saturating_sub(significant_digits(price));
        price
            .round_dp_with_strategy(significant.min(self.price_decimals()), strategy)
            .normalize()
    }

    /// `quantity` as the `f64` the SDK takes, once checked.
    fn wire_quantity(&self, quantity: Decimal) -> Result<f64, PrecisionViolation> {
        let violation = |limit| PrecisionViolation::new(OrderField::Quantity, quantity, limit);
        if quantity <= Decimal::ZERO {
            return Err(violation(PrecisionLimit::NotPositive));
        }
        if quantity.normalize().scale() > self.quantity_decimals() {
            return Err(violation(PrecisionLimit::DecimalPlaces {
                max: self.quantity_decimals(),
            }));
        }
        to_wire(quantity).ok_or_else(|| violation(PrecisionLimit::NotRepresentable))
    }

    /// `price`, the request's `field`, as the `f64` the SDK takes, once checked.
    fn wire_price(&self, field: OrderField, price: Decimal) -> Result<f64, PrecisionViolation> {
        let violation = |limit| PrecisionViolation::new(field, price, limit);
        if price <= Decimal::ZERO {
            return Err(violation(PrecisionLimit::NotPositive));
        }
        let normalized = price.normalize();
        if normalized.scale() > 0 {
            if normalized.scale() > self.price_decimals() {
                return Err(violation(PrecisionLimit::DecimalPlaces {
                    max: self.price_decimals(),
                }));
            }
            if significant_digits(normalized) > Self::MAX_SIGNIFICANT_FIGURES {
                return Err(violation(PrecisionLimit::SignificantFigures {
                    max: Self::MAX_SIGNIFICANT_FIGURES,
                }));
            }
        }
        to_wire(price).ok_or_else(|| violation(PrecisionLimit::NotRepresentable))
    }
}

/// The digits of `value`'s mantissa: its significant figures once normalized, for a value that
/// is not an integer.
fn significant_digits(value: Decimal) -> u32 {
    value
        .mantissa()
        .unsigned_abs()
        .checked_ilog10()
        .map_or(1, |log| log + 1)
}

/// `value` as an `f64`, if the SDK would send it unchanged.
fn to_wire(value: Decimal) -> Option<f64> {
    let float = value.to_f64()?;
    let sent = Decimal::from_str(&format!("{float:.SDK_WIRE_DECIMALS$}")).ok()?;
    (sent == value).then_some(float)
}

/// An order's values in the form the SDK places it in.
#[derive(Debug)]
pub(super) struct WireOrder {
    pub(super) limit_px: f64,
    pub(super) sz: f64,
    pub(super) order_type: ClientOrder,
}

/// Why an order cannot be put in the form the SDK places it in.
#[derive(Debug, PartialEq)]
pub(super) enum WireOrderError {
    /// A limit order, or a limit order a trigger places, with no limit price.
    MissingPrice,
    /// A kind Hyperliquid does not support. The clients refuse these before building the order.
    UnsupportedKind,
    /// A value breaks the market's precision rules.
    Precision(PrecisionViolation),
}

impl From<PrecisionViolation> for WireOrderError {
    fn from(violation: PrecisionViolation) -> Self {
        Self::Precision(violation)
    }
}

/// Check an order's values against `precision`, and put them in the form the SDK places them
/// in, unchanged.
///
/// A trigger order that places a market order (`Stop`, `TakeProfit`) sends its trigger price as
/// its limit price, as the SDK requires.
pub(super) fn wire_order(
    precision: &OrderPrecision,
    kind: OrderKind,
    tif: String,
    price: Option<Decimal>,
    quantity: Decimal,
) -> Result<WireOrder, WireOrderError> {
    let sz = precision.wire_quantity(quantity)?;
    let limit_price = || -> Result<f64, WireOrderError> {
        let price = price.ok_or(WireOrderError::MissingPrice)?;
        Ok(precision.wire_price(OrderField::Price, price)?)
    };
    let trigger = |trigger_price, is_market, tpsl: &str| -> Result<_, WireOrderError> {
        let trigger_px = precision.wire_price(OrderField::TriggerPrice, trigger_price)?;
        let order_type = ClientOrder::Trigger(ClientTrigger {
            is_market,
            trigger_px,
            tpsl: tpsl.to_owned(),
        });
        let limit_px = if is_market {
            trigger_px
        } else {
            limit_price()?
        };
        Ok(WireOrder {
            limit_px,
            sz,
            order_type,
        })
    };

    match kind {
        OrderKind::Limit => Ok(WireOrder {
            limit_px: limit_price()?,
            sz,
            order_type: ClientOrder::Limit(ClientLimit { tif }),
        }),
        OrderKind::Stop { trigger_price } => trigger(trigger_price, true, "sl"),
        OrderKind::StopLimit { trigger_price } => trigger(trigger_price, false, "sl"),
        OrderKind::TakeProfit { trigger_price } => trigger(trigger_price, true, "tp"),
        OrderKind::TakeProfitLimit { trigger_price } => trigger(trigger_price, false, "tp"),
        OrderKind::Market
        | OrderKind::TrailingStop { .. }
        | OrderKind::TrailingStopLimit { .. } => Err(WireOrderError::UnsupportedKind),
    }
}

#[cfg(test)]
// Test code: panics on bad input are acceptable
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn violation(field: OrderField, value: Decimal, limit: PrecisionLimit) -> PrecisionViolation {
        PrecisionViolation {
            field,
            value,
            limit,
        }
    }

    #[test]
    fn a_quantity_may_have_at_most_sz_decimals_places() {
        let three = OrderPrecision::perp(3);
        assert_eq!(three.check_quantity(dec!(1.001)), Ok(()));
        assert_eq!(
            three.check_quantity(dec!(1.00100)),
            Ok(()),
            "trailing zeros do not count"
        );
        assert_eq!(
            three.check_quantity(dec!(1.0001)),
            Err(violation(
                OrderField::Quantity,
                dec!(1.0001),
                PrecisionLimit::DecimalPlaces { max: 3 }
            ))
        );

        let zero = OrderPrecision::perp(0);
        assert_eq!(
            zero.check_quantity(dec!(123456)),
            Ok(()),
            "an integer quantity is not rounded to significant figures"
        );
        assert!(zero.check_quantity(dec!(1.5)).is_err());
    }

    #[test]
    fn an_integer_price_is_valid_whatever_its_significant_figures() {
        let perp = OrderPrecision::perp(5);
        assert_eq!(perp.check_price(dec!(123456)), Ok(()));
        assert_eq!(perp.check_price(dec!(123456.0)), Ok(()));
        assert_eq!(
            perp.check_price(dec!(12345.6)),
            Err(violation(
                OrderField::Price,
                dec!(12345.6),
                PrecisionLimit::SignificantFigures { max: 5 }
            ))
        );
    }

    /// The examples in Hyperliquid's tick and lot size documentation.
    #[test]
    fn a_price_follows_the_documented_examples() {
        let perp = OrderPrecision::perp(0);
        assert_eq!(perp.check_price(dec!(1234.5)), Ok(()));
        assert_eq!(
            perp.check_price(dec!(1234.56)),
            Err(violation(
                OrderField::Price,
                dec!(1234.56),
                PrecisionLimit::SignificantFigures { max: 5 }
            ))
        );
        assert_eq!(perp.check_price(dec!(0.001234)), Ok(()));
        assert_eq!(
            perp.check_price(dec!(0.0012345)),
            Err(violation(
                OrderField::Price,
                dec!(0.0012345),
                PrecisionLimit::DecimalPlaces { max: 6 }
            ))
        );

        let perp_one = OrderPrecision::perp(1);
        assert_eq!(perp_one.check_price(dec!(0.01234)), Ok(()));
        assert_eq!(
            perp_one.check_price(dec!(0.012345)),
            Err(violation(
                OrderField::Price,
                dec!(0.012345),
                PrecisionLimit::DecimalPlaces { max: 5 }
            ))
        );

        // 0.0001234 is valid on spot with szDecimals 0 or 1, and not above 2.
        assert_eq!(OrderPrecision::spot(0).check_price(dec!(0.0001234)), Ok(()));
        assert_eq!(OrderPrecision::spot(1).check_price(dec!(0.0001234)), Ok(()));
        assert!(
            OrderPrecision::spot(3)
                .check_price(dec!(0.0001234))
                .is_err()
        );
    }

    #[test]
    fn a_value_that_is_not_positive_is_refused() {
        let perp = OrderPrecision::perp(2);
        for value in [dec!(0), dec!(0.00), dec!(-1), dec!(-0.5)] {
            assert_eq!(
                perp.check_quantity(value),
                Err(violation(
                    OrderField::Quantity,
                    value,
                    PrecisionLimit::NotPositive
                ))
            );
            assert_eq!(
                perp.check_price(value),
                Err(violation(
                    OrderField::Price,
                    value,
                    PrecisionLimit::NotPositive
                ))
            );
            assert_eq!(
                perp.check_trigger_price(value),
                Err(violation(
                    OrderField::TriggerPrice,
                    value,
                    PrecisionLimit::NotPositive
                ))
            );
        }
    }

    #[test]
    fn a_value_rounded_to_zero_is_refused() {
        let quantity =
            OrderPrecision::perp(2).round_quantity(dec!(0.004), RoundingStrategy::ToZero);
        assert_eq!(quantity, Decimal::ZERO);
        assert!(OrderPrecision::perp(2).check_quantity(quantity).is_err());

        // A perpetual with szDecimals 6 allows no decimal places in a price.
        let precision = OrderPrecision::perp(6);
        let price = precision.round_price(dec!(0.4), RoundingStrategy::ToZero);
        assert_eq!(price, Decimal::ZERO);
        assert_eq!(
            precision.check_price(price),
            Err(violation(
                OrderField::Price,
                price,
                PrecisionLimit::NotPositive
            ))
        );
    }

    #[test]
    fn a_trigger_price_follows_the_price_rules_under_its_own_name() {
        let perp = OrderPrecision::perp(0);
        assert_eq!(perp.check_trigger_price(dec!(1234.5)), Ok(()));
        assert_eq!(
            perp.check_trigger_price(dec!(1234.56)),
            Err(violation(
                OrderField::TriggerPrice,
                dec!(1234.56),
                PrecisionLimit::SignificantFigures { max: 5 }
            ))
        );
    }

    #[test]
    fn the_price_cap_never_goes_below_zero_decimal_places() {
        let precision = OrderPrecision::perp(8);
        assert_eq!(precision.price_decimals(), 0);
        assert_eq!(precision.check_price(dec!(42)), Ok(()));
        assert!(precision.check_price(dec!(42.5)).is_err());
    }

    #[test]
    fn rounding_meets_the_rules_in_the_direction_asked() {
        let perp = OrderPrecision::perp(2);
        assert_eq!(
            perp.round_quantity(dec!(1.239), RoundingStrategy::ToZero),
            dec!(1.23)
        );
        assert_eq!(
            perp.round_quantity(dec!(1.231), RoundingStrategy::AwayFromZero),
            dec!(1.24)
        );

        // Significant figures bind: 5 of them leave 1 decimal place.
        assert_eq!(
            perp.round_price(dec!(1234.56), RoundingStrategy::ToZero),
            dec!(1234.5)
        );
        // Decimal places bind: perp with szDecimals 2 allows 4.
        assert_eq!(
            perp.round_price(dec!(0.0012345), RoundingStrategy::MidpointNearestEven),
            dec!(0.0012)
        );
        assert_eq!(
            perp.round_price(dec!(123456.7), RoundingStrategy::ToZero),
            dec!(123456)
        );
        assert_eq!(
            perp.round_price(dec!(123456), RoundingStrategy::ToZero),
            dec!(123456),
            "an integer price is returned unchanged"
        );
        assert_eq!(
            perp.round_price(dec!(9.99999), RoundingStrategy::AwayFromZero),
            dec!(10)
        );

        for value in [
            dec!(1234.56),
            dec!(0.0012345),
            dec!(9.99999),
            dec!(0.123456789),
        ] {
            for strategy in [RoundingStrategy::ToZero, RoundingStrategy::AwayFromZero] {
                let rounded = perp.round_price(value, strategy);
                assert_eq!(perp.check_price(rounded), Ok(()), "{value} -> {rounded}");
            }
        }
    }

    #[test]
    fn a_value_the_sdk_would_change_is_not_representable() {
        // Valid by the rules (an integer), but past what an f64 holds exactly.
        let huge = dec!(12345678901234567890123);
        assert_eq!(
            OrderPrecision::perp(0).check_price(huge),
            Err(violation(
                OrderField::Price,
                huge,
                PrecisionLimit::NotRepresentable
            ))
        );
        // Within the decimal places allowed, but with more digits than an f64 holds.
        let long = dec!(12345678901234.12345678);
        assert_eq!(
            OrderPrecision::spot(8).check_quantity(long),
            Err(violation(
                OrderField::Quantity,
                long,
                PrecisionLimit::NotRepresentable
            ))
        );
        assert_eq!(to_wire(dec!(0.00000001)), Some(0.00000001));
        assert_eq!(to_wire(dec!(123456.12345678)), Some(123456.12345678));
    }

    #[test]
    fn a_wire_order_carries_the_values_unchanged() {
        let precision = OrderPrecision::perp(2);
        let order = wire_order(
            &precision,
            OrderKind::StopLimit {
                trigger_price: dec!(95),
            },
            "Gtc".to_owned(),
            Some(dec!(94.5)),
            dec!(1.25),
        )
        .unwrap();
        assert_eq!((order.limit_px, order.sz), (94.5, 1.25));
        let ClientOrder::Trigger(trigger) = order.order_type else {
            panic!("expected a trigger order, got {:?}", order.order_type);
        };
        assert_eq!(
            (trigger.trigger_px, trigger.is_market, trigger.tpsl.as_str()),
            (95.0, false, "sl")
        );

        let market_trigger = wire_order(
            &precision,
            OrderKind::TakeProfit {
                trigger_price: dec!(120),
            },
            "Gtc".to_owned(),
            None,
            dec!(1),
        )
        .unwrap();
        assert_eq!(
            market_trigger.limit_px, 120.0,
            "a market trigger's limit price is its trigger price"
        );
    }

    #[test]
    fn a_wire_order_refuses_each_value_that_breaks_the_rules() {
        let precision = OrderPrecision::perp(2);
        let refused = |kind, price, quantity| {
            wire_order(&precision, kind, "Gtc".to_owned(), price, quantity).unwrap_err()
        };

        assert_eq!(
            refused(OrderKind::Limit, Some(dec!(100)), dec!(1.234)),
            WireOrderError::Precision(violation(
                OrderField::Quantity,
                dec!(1.234),
                PrecisionLimit::DecimalPlaces { max: 2 }
            ))
        );
        assert_eq!(
            refused(OrderKind::Limit, Some(dec!(100.123456)), dec!(1)),
            WireOrderError::Precision(violation(
                OrderField::Price,
                dec!(100.123456),
                PrecisionLimit::DecimalPlaces { max: 4 }
            ))
        );
        assert_eq!(
            refused(
                OrderKind::Stop {
                    trigger_price: dec!(1234.56)
                },
                None,
                dec!(1)
            ),
            WireOrderError::Precision(violation(
                OrderField::TriggerPrice,
                dec!(1234.56),
                PrecisionLimit::SignificantFigures { max: 5 }
            ))
        );
        assert_eq!(
            refused(OrderKind::Limit, None, dec!(1)),
            WireOrderError::MissingPrice
        );
        assert_eq!(
            refused(OrderKind::Market, None, dec!(1)),
            WireOrderError::UnsupportedKind
        );
    }
}
