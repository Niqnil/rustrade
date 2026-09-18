use super::SubscriptionKind;
use crate::books::{Level, OrderBook, mid_price, volume_weighted_mid_price};
use chrono::{DateTime, Utc};
use derive_more::Constructor;
use rust_decimal::Decimal;
use rustrade_macro::{DeSubKind, SerSubKind};
use serde::{Deserialize, Serialize};

/// Barter [`Subscription`](super::Subscription) [`SubscriptionKind`] that yields [`OrderBookL1`]
/// market events.
///
/// Level 1 refers to the best non-aggregated bid and ask [`Level`] on each side of the
/// [`OrderBook`].
#[derive(
    Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default, DeSubKind, SerSubKind,
)]
pub struct OrderBooksL1;

impl SubscriptionKind for OrderBooksL1 {
    type Event = OrderBookL1;
    fn as_str(&self) -> &'static str {
        "l1"
    }
}

impl std::fmt::Display for OrderBooksL1 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Normalised Barter [`OrderBookL1`] snapshot containing the latest best bid and ask.
#[derive(
    Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Default, Deserialize, Serialize, Constructor,
)]
pub struct OrderBookL1 {
    /// The instant this book state describes — the venue's own, where the venue publishes one.
    ///
    /// # Producer obligation
    ///
    /// A producer **must** set this to the same instant it puts in the wrapping
    /// [`MarketEvent::time_exchange`](crate::event::MarketEvent::time_exchange), and it **should**
    /// come from the venue rather than from a local or aggregator clock.
    ///
    /// Downstream state orders L1 updates on this field, not on the event's, so the two must not
    /// disagree. A producer that lets them diverge gets no error and no log: a staleness guard keyed
    /// on this field can reject legitimately newer updates and freeze the held book.
    ///
    /// # Not every in-tree producer has a venue instant to use
    ///
    /// The "should" above is not a "must", because some venues publish no book timestamp:
    ///
    /// - **Binance Spot** `bookTicker` carries `{u, s, b, B, a, A}` and no time field, so
    ///   [`BinanceOrderBookL1`](crate::exchange::binance::book::l1::BinanceOrderBookL1) falls back
    ///   to the host clock at deserialisation. (Binance USD-M futures *does* send `T`, and that path
    ///   uses it.)
    /// - **IBKR** quotes are assembled from separate bid/ask/size ticks, which carry no per-tick
    ///   venue instant, so `exchange::ibkr` (behind the `ibkr` feature, hence no link) stamps the
    ///   assembly time.
    ///
    /// Both fill the field from a local clock, and both keep it consistent with `time_exchange` as
    /// required above. Consumers must therefore **not** rank this field against a *tick* timestamp
    /// from another source — a trade's `time_exchange`, say — since the gap between them is
    /// milliseconds and the comparison would be decided by skew rather than by the market. Ranking
    /// it against a coarser instant, such as a candle's `close_time`, is sound while the gap stays
    /// well above the skew. `DefaultInstrumentMarketData::price` in the `rustrade` crate does
    /// exactly that, and documents where the margin runs out.
    pub last_update_time: DateTime<Utc>,
    pub best_bid: Option<Level>,
    pub best_ask: Option<Level>,
}

impl OrderBookL1 {
    /// Calculate the mid-price by taking the average of the best bid and ask prices.
    ///
    /// See Docs: <https://www.quantstart.com/articles/high-frequency-trading-ii-limit-order-book>
    pub fn mid_price(&self) -> Option<Decimal> {
        match (self.best_ask, self.best_bid) {
            (Some(best_ask), Some(best_bid)) => Some(mid_price(best_bid.price, best_ask.price)),
            _ => None,
        }
    }

    /// Calculate the volume weighted mid-price (micro-price), weighing the best bid and ask prices
    /// with their associated amount.
    ///
    /// `None` when either side is absent, and when both sides carry a **zero amount** — a feed that
    /// publishes prices without sizes produces exactly that book, and the weighting is undefined for
    /// it. Fall back to [`mid_price`](Self::mid_price) when a price is needed regardless of size.
    ///
    /// See Docs: <https://www.quantstart.com/articles/high-frequency-trading-ii-limit-order-book>
    pub fn volume_weighed_mid_price(&self) -> Option<Decimal> {
        match (self.best_ask, self.best_bid) {
            (Some(best_ask), Some(best_bid)) => volume_weighted_mid_price(best_bid, best_ask),
            _ => None,
        }
    }
}

/// Barter [`Subscription`](super::Subscription) [`SubscriptionKind`] that yields L2
/// [`OrderBookEvent`] market events
///
/// Level 2 refers to an [`OrderBook`] with orders at each price level aggregated.
#[derive(
    Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default, DeSubKind, SerSubKind,
)]
pub struct OrderBooksL2;

impl SubscriptionKind for OrderBooksL2 {
    type Event = OrderBookEvent;
    fn as_str(&self) -> &'static str {
        "l2"
    }
}

impl std::fmt::Display for OrderBooksL2 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Barter [`Subscription`](super::Subscription) [`SubscriptionKind`] that yields
/// L3 [`OrderBookEvent`] market events.
///
/// Level 3 refers to the non-aggregated [`OrderBook`]. This is a direct replication of the exchange
/// [`OrderBook`].
#[derive(
    Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default, DeSubKind, SerSubKind,
)]
pub struct OrderBooksL3;

impl SubscriptionKind for OrderBooksL3 {
    type Event = OrderBookEvent;

    fn as_str(&self) -> &'static str {
        "l3"
    }
}

impl std::fmt::Display for OrderBooksL3 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

#[derive(Clone, PartialEq, Eq, Debug, Deserialize, Serialize)]
pub enum OrderBookEvent {
    Snapshot(OrderBook),
    Update(OrderBook),
}
