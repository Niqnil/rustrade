use super::SubscriptionKind;
use rust_decimal::Decimal;
use rustrade_macro::{DeSubKind, SerSubKind};
use serde::{Deserialize, Serialize};

/// 10,000 as a Decimal constant for basis point calculations.
const BPS_FACTOR: Decimal = Decimal::from_parts(10_000, 0, 0, false, 0);

/// Barter [`Subscription`](super::Subscription) [`SubscriptionKind`] that yields [`Quote`]
/// [`MarketEvent<T>`](crate::event::MarketEvent) events.
///
/// Represents real-time best bid/ask quotes (NBBO for equities, top-of-book for crypto).
#[derive(
    Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default, DeSubKind, SerSubKind,
)]
pub struct Quotes;

impl SubscriptionKind for Quotes {
    type Event = Quote;

    fn as_str(&self) -> &'static str {
        "quotes"
    }
}

impl std::fmt::Display for Quotes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Normalised Barter [`Quote`] model representing best bid/ask.
#[derive(Copy, Clone, PartialEq, PartialOrd, Debug, Deserialize, Serialize)]
pub struct Quote {
    pub bid_price: Decimal,
    pub bid_amount: Decimal,
    pub ask_price: Decimal,
    pub ask_amount: Decimal,
}

impl Quote {
    /// Calculate the mid-price as the average of bid and ask prices.
    pub fn mid_price(&self) -> Decimal {
        (self.bid_price + self.ask_price) / Decimal::TWO
    }

    /// Calculate the spread (ask - bid).
    pub fn spread(&self) -> Decimal {
        self.ask_price - self.bid_price
    }

    /// Calculate the spread in basis points (1 bps = 0.01%) relative to the mid-price.
    /// Returns zero if mid-price is zero.
    pub fn spread_bps(&self) -> Decimal {
        let mid = self.mid_price();
        if mid.is_zero() {
            Decimal::ZERO
        } else {
            (self.spread() / mid) * BPS_FACTOR
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn test_quote_mid_price() {
        let quote = Quote {
            bid_price: dec!(100),
            bid_amount: dec!(10),
            ask_price: dec!(102),
            ask_amount: dec!(5),
        };
        assert_eq!(quote.mid_price(), dec!(101));
    }

    #[test]
    fn test_quote_spread() {
        let quote = Quote {
            bid_price: dec!(100),
            bid_amount: dec!(10),
            ask_price: dec!(102),
            ask_amount: dec!(5),
        };
        assert_eq!(quote.spread(), dec!(2));
    }

    #[test]
    fn test_quote_spread_bps() {
        let quote = Quote {
            bid_price: dec!(100),
            bid_amount: dec!(10),
            ask_price: dec!(101),
            ask_amount: dec!(5),
        };
        // Spread = 1, mid = 100.5, spread_bps = 1/100.5 * 10000 ≈ 99.50248...
        let bps = quote.spread_bps();
        assert!(bps > dec!(99) && bps < dec!(100), "spread_bps={bps}");
    }
}
