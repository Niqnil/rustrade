use super::SubscriptionKind;
use rustrade_macro::{DeSubKind, SerSubKind};
use serde::{Deserialize, Serialize};

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
#[derive(Clone, PartialEq, PartialOrd, Debug, Deserialize, Serialize)]
pub struct Quote {
    pub bid_price: f64,
    pub bid_amount: f64,
    pub ask_price: f64,
    pub ask_amount: f64,
}

impl Quote {
    /// Calculate the mid-price as the average of bid and ask prices.
    pub fn mid_price(&self) -> f64 {
        (self.bid_price + self.ask_price) / 2.0
    }

    /// Calculate the spread (ask - bid).
    pub fn spread(&self) -> f64 {
        self.ask_price - self.bid_price
    }

    /// Calculate the spread in basis points (1 bps = 0.01%) relative to the mid-price.
    /// Returns 0.0 if mid-price is zero.
    pub fn spread_bps(&self) -> f64 {
        let mid = self.mid_price();
        if mid == 0.0 {
            0.0
        } else {
            (self.spread() / mid) * 10_000.0
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_quote_mid_price() {
        let quote = Quote {
            bid_price: 100.0,
            bid_amount: 10.0,
            ask_price: 102.0,
            ask_amount: 5.0,
        };
        assert_eq!(quote.mid_price(), 101.0);
    }

    #[test]
    fn test_quote_spread() {
        let quote = Quote {
            bid_price: 100.0,
            bid_amount: 10.0,
            ask_price: 102.0,
            ask_amount: 5.0,
        };
        assert_eq!(quote.spread(), 2.0);
    }

    #[test]
    fn test_quote_spread_bps() {
        let quote = Quote {
            bid_price: 100.0,
            bid_amount: 10.0,
            ask_price: 101.0,
            ask_amount: 5.0,
        };
        // Spread = 1, mid = 100.5, spread_bps = 1/100.5 * 10000 ≈ 99.5
        assert!((quote.spread_bps() - 99.502).abs() < 0.01);
    }
}
