use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// Market state for a single instrument at a single instant.
///
/// Two things consume this type, and they meet at the simulated venue:
///
/// - [`RequestOpen::market`](crate::order::request::RequestOpen::market) carries it as the market
///   its sender observed at the moment it decided to open the order.
/// - [`FillModel::fill_price`](crate::fill::FillModel::fill_price) prices a fill from it.
///
/// # Every field is optional, and absence is ordinary
///
/// A feed supplies what it has. An instrument subscribed to trades alone has a `last_price` and no
/// book; one subscribed to an L1 book has both sides and no trade price until the first print; one
/// that has received nothing yet has neither, which is what the first ticks of any backtest look
/// like. Each [`FillModel`](crate::fill::FillModel) implementation documents which fields it reads
/// and what it falls back to, and returns `None` when it cannot price the order at all — see
/// [`MockExchange`](crate::exchange::mock::MockExchange), which turns that `None` into a rejection
/// naming the instrument rather than a panic.
///
/// # This is a snapshot, not a book
///
/// It holds best bid and best ask prices only — no sizes, no depth. A fill model can therefore
/// model the spread but not market impact or partial fills against resting size. Modelling those
/// needs the venue to own a real book, which is a larger change than this type.
#[derive(
    Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Default, Deserialize, Serialize,
)]
pub struct MarketSnapshot {
    /// Best bid price, if the instrument's feed supplies a book.
    pub best_bid: Option<Decimal>,

    /// Best ask price, if the instrument's feed supplies a book.
    pub best_ask: Option<Decimal>,

    /// Most recent trade price, if one has been seen.
    pub last_price: Option<Decimal>,
}

impl MarketSnapshot {
    /// Construct a `MarketSnapshot` from its three optional prices.
    pub fn new(
        best_bid: Option<Decimal>,
        best_ask: Option<Decimal>,
        last_price: Option<Decimal>,
    ) -> Self {
        Self {
            best_bid,
            best_ask,
            last_price,
        }
    }

    /// Construct a `MarketSnapshot` carrying only a last traded price.
    ///
    /// This is what a price-only instrument state can honestly report: a price, and no claim about
    /// either side of a book.
    pub fn from_last_price(last_price: Option<Decimal>) -> Self {
        Self {
            best_bid: None,
            best_ask: None,
            last_price,
        }
    }

    /// Whether this snapshot carries no price at all.
    ///
    /// True for an instrument that has received no market data yet — distinct from no snapshot
    /// having been taken, which [`RequestOpen::market`](crate::order::request::RequestOpen::market)
    /// represents as `None`.
    pub fn is_empty(&self) -> bool {
        self.best_bid.is_none() && self.best_ask.is_none() && self.last_price.is_none()
    }

    /// Mid-price, when both sides of the book are present.
    pub fn mid_price(&self) -> Option<Decimal> {
        match (self.best_bid, self.best_ask) {
            (Some(bid), Some(ask)) => Some((bid + ask) / Decimal::TWO),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn default_snapshot_is_empty() {
        assert!(MarketSnapshot::default().is_empty());
    }

    #[test]
    fn a_snapshot_carrying_any_price_is_not_empty() {
        assert!(!MarketSnapshot::from_last_price(Some(dec!(100))).is_empty());
        assert!(!MarketSnapshot::new(Some(dec!(99)), None, None).is_empty());
        assert!(!MarketSnapshot::new(None, Some(dec!(101)), None).is_empty());
    }

    #[test]
    fn mid_price_requires_both_sides() {
        assert_eq!(
            MarketSnapshot::new(Some(dec!(99)), Some(dec!(101)), None).mid_price(),
            Some(dec!(100))
        );
        assert_eq!(
            MarketSnapshot::new(Some(dec!(99)), None, None).mid_price(),
            None
        );
        assert_eq!(
            MarketSnapshot::new(None, Some(dec!(101)), None).mid_price(),
            None
        );
    }
}
