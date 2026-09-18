use rust_decimal::Decimal;
use rustrade_instrument::Side;
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

/// How much is on offer at each side's best price, where the feed says.
///
/// Carried **alongside** a [`MarketSnapshot`] rather than inside it. A snapshot is the view a
/// [`FillModel`](crate::fill::FillModel) prices from and the view
/// [`RequestOpen::market`](crate::order::request::RequestOpen::market) persists, and a size is
/// neither: a fill model answers *where* a taker prints, while *how much* that price can absorb is
/// the venue's own arithmetic — and only the venue tracks what is left of it as its orders eat
/// into it.
///
/// # Absent size means unlimited, not unfillable
///
/// `None` says this feed supplies no size information. That is the ordinary case, not a
/// degenerate one: a trades-only feed, a candle feed, a bulk price export and a venue with no
/// market feed at all every one of them report it on every observation. A venue reading `None`
/// therefore caps nothing. The opposite reading — no size information means no size — would
/// silently stop every price-only backtest from filling anything.
///
/// `Some(amount)` is a size that was actually reported, and `Some(Decimal::ZERO)` is a reported
/// *absence* of size: there is nothing to take, so an aggressor takes nothing. Whoever derives
/// this from a feed owns that distinction — a feed publishing prices with a zero amount on every
/// row is stating the former, not the latter, and must say `None`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct MarketDepth {
    /// Size resting at [`MarketSnapshot::best_bid`], if the feed supplies one.
    pub best_bid: Option<Decimal>,

    /// Size resting at [`MarketSnapshot::best_ask`], if the feed supplies one.
    pub best_ask: Option<Decimal>,
}

impl MarketDepth {
    /// Neither side's size is known, so nothing consulting this is capped.
    ///
    /// What a feed carrying no book reports, and what every [`MarketDepth::default()`] is.
    pub const UNKNOWN: Self = Self {
        best_bid: None,
        best_ask: None,
    };

    /// Construct a `MarketDepth` from the size on each side.
    pub fn new(best_bid: Option<Decimal>, best_ask: Option<Decimal>) -> Self {
        Self { best_bid, best_ask }
    }

    /// What an aggressor on `side` can take: the ask for a buy, the bid for a sell.
    ///
    /// The far side of the book, because that is the side an order on this one trades against —
    /// the same rule that decides whether a limit order crosses at all.
    pub fn available_to(&self, side: Side) -> Option<Decimal> {
        match side {
            Side::Buy => self.best_ask,
            Side::Sell => self.best_bid,
        }
    }

    /// Reduces what an aggressor on `side` can still take by `quantity`, never below zero.
    ///
    /// A side whose size is unknown stays unknown: there is nothing to draw down, and an
    /// unbounded quantity less a finite one is still unbounded.
    pub fn consume(&mut self, side: Side, quantity: Decimal) {
        debug_assert!(
            quantity >= Decimal::ZERO,
            "consuming a negative quantity {quantity} of depth would create size that was never \
             on offer"
        );

        let available = match side {
            Side::Buy => &mut self.best_ask,
            Side::Sell => &mut self.best_bid,
        };

        if let Some(available) = available {
            *available = (*available - quantity).max(Decimal::ZERO);
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
    fn depth_is_taken_from_the_far_side_of_the_book() {
        let depth = MarketDepth::new(Some(dec!(5)), Some(dec!(3)));
        assert_eq!(depth.available_to(Side::Buy), Some(dec!(3)));
        assert_eq!(depth.available_to(Side::Sell), Some(dec!(5)));
    }

    #[test]
    fn an_unknown_size_stays_unknown_however_much_is_taken() {
        let mut depth = MarketDepth::UNKNOWN;
        depth.consume(Side::Buy, dec!(1000));
        assert_eq!(depth.available_to(Side::Buy), None);
        assert_eq!(MarketDepth::default(), MarketDepth::UNKNOWN);
    }

    #[test]
    fn consuming_draws_down_only_the_side_taken_from() {
        let mut depth = MarketDepth::new(Some(dec!(5)), Some(dec!(3)));
        depth.consume(Side::Buy, dec!(2));
        assert_eq!(depth, MarketDepth::new(Some(dec!(5)), Some(dec!(1))));
    }

    #[test]
    fn consuming_more_than_is_on_offer_leaves_nothing_rather_than_a_debt() {
        let mut depth = MarketDepth::new(None, Some(dec!(3)));
        depth.consume(Side::Buy, dec!(10));
        assert_eq!(depth.available_to(Side::Buy), Some(Decimal::ZERO));
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
