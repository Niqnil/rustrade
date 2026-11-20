//! Quote aggregation for IB market data ticks.
//!
//! IB sends individual tick updates (bid price, bid size, ask price, ask size)
//! as separate events. This aggregator accumulates them and emits complete
//! [`OrderBookL1`] snapshots.

use crate::{books::Level, subscription::book::OrderBookL1};
use chrono::{DateTime, Utc};
use ibapi::contracts::{SecurityType, tick_types::TickType};
use ibapi::market_data::realtime::{TickPrice, TickSize, TickTypes};
use rust_decimal::Decimal;

use super::decimal_from_f64;

/// The price IB sends for a side it has no quote on, for example while the market is closed.
const NO_QUOTE_PRICE: f64 = -1.0;

/// Aggregates IB tick updates into OrderBookL1 snapshots.
///
/// Emits a new OrderBookL1 whenever any bid or ask price/size is available, and when a side it
/// held is withdrawn. Does not require both sides to emit.
///
/// # No quote
///
/// IB reports a side with no quote as a price of -1, usually with a size of 0. That side is then
/// cleared to `None`, with its size, rather than quoted at -1, and the book is emitted so a
/// consumer sees the side go. For an instrument whose price cannot be negative (stocks, options,
/// futures options, warrants, bonds, funds, currencies, crypto) any negative price clears the side.
/// For one whose price can be (futures, spreads, indices, commodities, CFDs, and types this does
/// not know), only -1 with a size of 0 or no size does; any other price, negative or not, is
/// quoted.
///
/// A no-quote price on a side never quoted is emitted too, so on a closed market the first book
/// can have neither side: it says IB has no quote, not that the book is malformed.
///
/// # Event Rate
///
/// Emits on every relevant tick, even if values are unchanged from the previous
/// emission. Consumers should deduplicate if needed.
#[derive(Debug)]
pub struct QuoteAggregator {
    negative_prices: bool,
    bid_price: Option<Decimal>,
    bid_size: Option<Decimal>,
    ask_price: Option<Decimal>,
    ask_size: Option<Decimal>,
    last_update: Option<DateTime<Utc>>,
}

/// The side of the book a tick type quotes, if any.
#[derive(Debug, Clone, Copy)]
enum Side {
    Bid,
    Ask,
}

impl Side {
    fn of_price(tick_type: &TickType) -> Option<Self> {
        match tick_type {
            TickType::Bid | TickType::DelayedBid => Some(Self::Bid),
            TickType::Ask | TickType::DelayedAsk => Some(Self::Ask),
            _ => None,
        }
    }

    fn of_size(tick_type: &TickType) -> Option<Self> {
        match tick_type {
            TickType::BidSize | TickType::DelayedBidSize => Some(Self::Bid),
            TickType::AskSize | TickType::DelayedAskSize => Some(Self::Ask),
            _ => None,
        }
    }
}

impl QuoteAggregator {
    /// Create an aggregator for an instrument of `security_type`, which decides which prices mean
    /// "no quote" (see [`QuoteAggregator`]).
    pub fn new(security_type: &SecurityType) -> Self {
        let negative_prices = !matches!(
            security_type,
            SecurityType::Stock
                | SecurityType::Option
                | SecurityType::FuturesOption
                | SecurityType::Warrant
                | SecurityType::Bond
                | SecurityType::MutualFund
                | SecurityType::ForexPair
                | SecurityType::Crypto
        );
        Self {
            negative_prices,
            bid_price: None,
            bid_size: None,
            ask_price: None,
            ask_size: None,
            last_update: None,
        }
    }

    /// Process a tick and potentially emit an OrderBookL1.
    ///
    /// Returns `Some(OrderBookL1)` when any quote data is available after processing this tick,
    /// or when this tick withdrew a side.
    ///
    /// # Arguments
    ///
    /// * `tick` - The tick update from IB
    /// * `now` - Current timestamp (caller provides to avoid redundant syscalls)
    pub fn update(&mut self, tick: &TickTypes, now: DateTime<Utc>) -> Option<OrderBookL1> {
        match tick {
            TickTypes::Price(price) => self.update_price(price, now),
            TickTypes::Size(size) => self.update_size(size, now),
            TickTypes::PriceSize(ps) => {
                let withdrawn =
                    self.process_price_tick_type(&ps.price_tick_type, ps.price, Some(ps.size), now);
                // A withdrawn side's size goes with its price: IB's size here is the 0 that
                // accompanies the "no quote" price.
                if !withdrawn {
                    self.process_size_tick_type(&ps.size_tick_type, ps.size, now);
                }
                self.try_emit(withdrawn)
            }
            _ => None,
        }
    }

    fn update_price(&mut self, tick: &TickPrice, now: DateTime<Utc>) -> Option<OrderBookL1> {
        let withdrawn = self.process_price_tick_type(&tick.tick_type, tick.price, None, now);
        self.try_emit(withdrawn)
    }

    /// Apply a price tick; returns whether it withdrew its side.
    fn process_price_tick_type(
        &mut self,
        tick_type: &TickType,
        price: f64,
        size: Option<f64>,
        now: DateTime<Utc>,
    ) -> bool {
        let Some(side) = Side::of_price(tick_type) else {
            return false;
        };
        if self.is_no_quote(price, size) {
            let (price, size) = self.side_mut(side);
            *price = None;
            *size = None;
            self.last_update = Some(now);
            return true;
        }
        // Convert and validate: NaN/Inf/out-of-range values must not overwrite valid prices
        let Some(price) = decimal_from_f64(price) else {
            return false;
        };
        *self.side_mut(side).0 = Some(price);
        self.last_update = Some(now);
        false
    }

    /// Whether `price` (with its `size`, when the tick carries one) means IB has no quote on that
    /// side.
    fn is_no_quote(&self, price: f64, size: Option<f64>) -> bool {
        // NaN is not a price either way: it is dropped as invalid, not taken as "no quote".
        if price.is_nan() || price >= 0.0 {
            return false;
        }
        // Exact: IB sends the sentinel itself, not a computed value.
        #[allow(clippy::float_cmp)]
        let sentinel = price == NO_QUOTE_PRICE && size.is_none_or(|size| size == 0.0);
        !self.negative_prices || sentinel
    }

    fn update_size(&mut self, tick: &TickSize, now: DateTime<Utc>) -> Option<OrderBookL1> {
        self.process_size_tick_type(&tick.tick_type, tick.size, now);
        self.try_emit(false)
    }

    fn process_size_tick_type(&mut self, tick_type: &TickType, size: f64, now: DateTime<Utc>) {
        let Some(side) = Side::of_size(tick_type) else {
            return;
        };
        // Convert and validate: NaN/Inf/out-of-range values must not overwrite valid sizes
        let Some(size) = decimal_from_f64(size) else {
            return;
        };
        *self.side_mut(side).1 = Some(size);
        self.last_update = Some(now);
    }

    fn side_mut(&mut self, side: Side) -> (&mut Option<Decimal>, &mut Option<Decimal>) {
        match side {
            Side::Bid => (&mut self.bid_price, &mut self.bid_size),
            Side::Ask => (&mut self.ask_price, &mut self.ask_size),
        }
    }

    /// The book as held, when a side is quoted or this tick `withdrawn` one: a book with neither
    /// side quoted then says both are withdrawn.
    fn try_emit(&self, withdrawn: bool) -> Option<OrderBookL1> {
        if self.bid_price.is_none() && self.ask_price.is_none() && !withdrawn {
            return None;
        }
        #[allow(clippy::expect_used)] // Invariant: a quoted or withdrawn side set last_update
        let last_update_time = self
            .last_update
            .expect("last_update set when a side is quoted or withdrawn");

        // Values already converted to Decimal at storage time
        let bid = self.bid_price.map(|price| Level {
            price,
            amount: self.bid_size.unwrap_or(Decimal::ZERO),
        });

        let ask = self.ask_price.map(|price| Level {
            price,
            amount: self.ask_size.unwrap_or(Decimal::ZERO),
        });

        Some(OrderBookL1 {
            last_update_time,
            best_bid: bid,
            best_ask: ask,
        })
    }
}

#[cfg(test)]
// Test code may unwrap freely since panics indicate test failure
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use ibapi::market_data::realtime::{TickAttribute, TickPriceSize};
    use rust_decimal_macros::dec;

    fn tick_price(tick_type: TickType, price: f64) -> TickTypes {
        TickTypes::Price(TickPrice {
            tick_type,
            price,
            attributes: TickAttribute::default(),
        })
    }

    fn tick_size(tick_type: TickType, size: f64) -> TickTypes {
        TickTypes::Size(TickSize { tick_type, size })
    }

    fn tick_price_size(price_tick_type: TickType, price: f64, size: f64) -> TickTypes {
        let size_tick_type = match price_tick_type {
            TickType::Bid => TickType::BidSize,
            TickType::Ask => TickType::AskSize,
            TickType::DelayedBid => TickType::DelayedBidSize,
            TickType::DelayedAsk => TickType::DelayedAskSize,
            other => panic!("no size tick for {other:?}"),
        };
        TickTypes::PriceSize(TickPriceSize {
            price_tick_type,
            price,
            attributes: TickAttribute::default(),
            size_tick_type,
            size,
        })
    }

    /// An option quoted 1.10 x 1.20, sizes 10 and 20.
    fn quoted_option() -> QuoteAggregator {
        let mut agg = QuoteAggregator::new(&SecurityType::Option);
        let now = Utc::now();
        agg.update(&tick_price_size(TickType::Bid, 1.10, 10.0), now);
        agg.update(&tick_price_size(TickType::Ask, 1.20, 20.0), now);
        agg
    }

    #[test]
    fn a_no_quote_bid_withdraws_the_bid_and_keeps_the_ask() {
        let mut agg = quoted_option();

        let l1 = agg
            .update(&tick_price_size(TickType::Bid, -1.0, 0.0), Utc::now())
            .expect("a withdrawn side is emitted");

        assert_eq!(l1.best_bid, None);
        let ask = l1.best_ask.unwrap();
        assert_eq!((ask.price, ask.amount), (dec!(1.20), dec!(20)));
    }

    #[test]
    fn a_withdrawn_side_loses_its_size_too() {
        let mut agg = quoted_option();
        let now = Utc::now();
        agg.update(&tick_price_size(TickType::Bid, -1.0, 0.0), now);

        // A price with no size of its own must not pick up the size quoted before the withdrawal.
        let l1 = agg.update(&tick_price(TickType::Bid, 1.15), now).unwrap();

        let bid = l1.best_bid.unwrap();
        assert_eq!((bid.price, bid.amount), (dec!(1.15), Decimal::ZERO));
    }

    #[test]
    fn a_no_quote_price_without_a_size_withdraws_the_side() {
        let mut agg = quoted_option();

        let l1 = agg
            .update(&tick_price(TickType::Ask, -1.0), Utc::now())
            .unwrap();

        assert!(l1.best_bid.is_some());
        assert_eq!(l1.best_ask, None);
    }

    #[test]
    fn a_delayed_no_quote_price_withdraws_the_side() {
        let mut agg = QuoteAggregator::new(&SecurityType::Option);
        let now = Utc::now();
        agg.update(&tick_price_size(TickType::DelayedBid, 0.95, 3.0), now);

        let l1 = agg
            .update(&tick_price_size(TickType::DelayedBid, -1.0, 0.0), now)
            .unwrap();

        assert_eq!(l1.best_bid, None);
    }

    #[test]
    fn both_sides_withdrawn_emit_an_empty_book() {
        let mut agg = quoted_option();
        let now = Utc::now();
        agg.update(&tick_price_size(TickType::Bid, -1.0, 0.0), now);

        let l1 = agg
            .update(&tick_price_size(TickType::Ask, -1.0, 0.0), now)
            .expect("the last side's withdrawal is emitted");

        assert_eq!((l1.best_bid, l1.best_ask), (None, None));
    }

    #[test]
    fn a_no_quote_on_a_fresh_book_is_emitted_as_empty() {
        let mut agg = QuoteAggregator::new(&SecurityType::Option);

        let l1 = agg
            .update(&tick_price_size(TickType::Bid, -1.0, 0.0), Utc::now())
            .unwrap();

        assert_eq!((l1.best_bid, l1.best_ask), (None, None));
    }

    #[test]
    fn an_empty_book_is_not_emitted_again_for_a_tick_that_withdraws_nothing() {
        let mut agg = QuoteAggregator::new(&SecurityType::Option);
        let now = Utc::now();
        agg.update(&tick_price_size(TickType::Bid, -1.0, 0.0), now);

        assert!(agg.update(&tick_price(TickType::Last, 1.0), now).is_none());
        assert!(
            agg.update(&tick_size(TickType::BidSize, 5.0), now)
                .is_none()
        );
    }

    #[test]
    fn a_size_before_any_price_emits_nothing() {
        let mut agg = QuoteAggregator::new(&SecurityType::Option);

        assert!(
            agg.update(&tick_size(TickType::BidSize, 5.0), Utc::now())
                .is_none()
        );
    }

    #[test]
    fn a_zero_bid_is_a_quote() {
        let mut agg = QuoteAggregator::new(&SecurityType::Option);

        let l1 = agg
            .update(&tick_price_size(TickType::Bid, 0.0, 50.0), Utc::now())
            .unwrap();

        let bid = l1.best_bid.unwrap();
        assert_eq!((bid.price, bid.amount), (Decimal::ZERO, dec!(50)));
    }

    #[test]
    fn any_negative_price_withdraws_a_side_whose_price_cannot_be_negative() {
        for security_type in [SecurityType::Stock, SecurityType::Option] {
            let mut agg = QuoteAggregator::new(&security_type);

            let l1 = agg
                .update(&tick_price_size(TickType::Bid, -0.5, 7.0), Utc::now())
                .unwrap();

            assert_eq!(l1.best_bid, None, "{security_type:?}");
        }
    }

    #[test]
    fn a_future_takes_negative_prices_other_than_the_no_quote_sentinel() {
        let now = Utc::now();
        let mut agg = QuoteAggregator::new(&SecurityType::Future);

        let l1 = agg
            .update(&tick_price_size(TickType::Bid, -37.63, 4.0), now)
            .unwrap();
        assert_eq!(l1.best_bid.unwrap().price, dec!(-37.63));

        // -1 with a size is a price, not the sentinel.
        let l1 = agg
            .update(&tick_price_size(TickType::Bid, -1.0, 2.0), now)
            .unwrap();
        let bid = l1.best_bid.unwrap();
        assert_eq!((bid.price, bid.amount), (dec!(-1), dec!(2)));

        // -1 with no size, or a size of 0, is the sentinel.
        let l1 = agg.update(&tick_price(TickType::Bid, -1.0), now).unwrap();
        assert_eq!(l1.best_bid, None);
        agg.update(&tick_price_size(TickType::Bid, -2.0, 1.0), now);
        let l1 = agg
            .update(&tick_price_size(TickType::Bid, -1.0, 0.0), now)
            .unwrap();
        assert_eq!(l1.best_bid, None);
    }

    #[test]
    fn partial_update_emits_partial() {
        let mut agg = QuoteAggregator::new(&SecurityType::Stock);
        let now = Utc::now();

        let result = agg.update(&tick_price(TickType::Bid, 100.0), now);
        assert!(result.is_some());
        let l1 = result.unwrap();
        assert!(l1.best_bid.is_some());
        assert!(l1.best_ask.is_none());
    }

    #[test]
    fn complete_quote_emits() {
        let mut agg = QuoteAggregator::new(&SecurityType::Stock);
        let now = Utc::now();

        agg.update(&tick_price(TickType::Bid, 100.0), now);
        agg.update(&tick_size(TickType::BidSize, 10.0), now);
        agg.update(&tick_price(TickType::Ask, 101.0), now);
        let result = agg.update(&tick_size(TickType::AskSize, 5.0), now);

        assert!(result.is_some());
        let l1 = result.unwrap();

        let bid = l1.best_bid.unwrap();
        assert_eq!(bid.price, dec!(100));
        assert_eq!(bid.amount, dec!(10));

        let ask = l1.best_ask.unwrap();
        assert_eq!(ask.price, dec!(101));
        assert_eq!(ask.amount, dec!(5));
    }

    #[test]
    fn delayed_ticks_handled() {
        let mut agg = QuoteAggregator::new(&SecurityType::Stock);
        let now = Utc::now();

        agg.update(&tick_price(TickType::DelayedBid, 99.0), now);
        let result = agg.update(&tick_price(TickType::DelayedAsk, 100.0), now);

        assert!(result.is_some());
        let l1 = result.unwrap();
        assert!(l1.best_bid.is_some());
        assert!(l1.best_ask.is_some());
    }

    #[test]
    fn irrelevant_tick_ignored() {
        let mut agg = QuoteAggregator::new(&SecurityType::Stock);
        let now = Utc::now();

        let result = agg.update(&tick_price(TickType::Last, 100.0), now);
        assert!(result.is_none());
    }

    #[test]
    fn invalid_price_skipped() {
        let mut agg = QuoteAggregator::new(&SecurityType::Stock);
        let now = Utc::now();

        // NaN price should result in None
        agg.update(&tick_price(TickType::Bid, f64::NAN), now);
        let result = agg.update(&tick_price(TickType::Ask, 100.0), now);

        // Ask should be present, bid should be None (NaN skipped)
        assert!(result.is_some());
        let l1 = result.unwrap();
        assert!(l1.best_bid.is_none());
        assert!(l1.best_ask.is_some());
    }

    #[test]
    fn nan_does_not_overwrite_valid_price() {
        let mut agg = QuoteAggregator::new(&SecurityType::Stock);
        let now = Utc::now();

        // Set valid bid price
        agg.update(&tick_price(TickType::Bid, 100.0), now);
        let l1 = agg.update(&tick_price(TickType::Ask, 101.0), now).unwrap();
        assert_eq!(l1.best_bid.unwrap().price, dec!(100));

        // NaN bid should NOT overwrite valid price
        let l1 = agg
            .update(&tick_price(TickType::Bid, f64::NAN), now)
            .unwrap();
        assert_eq!(
            l1.best_bid.unwrap().price,
            dec!(100),
            "NaN should not overwrite valid bid"
        );
    }
}
