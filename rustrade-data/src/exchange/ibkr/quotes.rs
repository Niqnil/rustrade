//! Quote aggregation for IB market data ticks.
//!
//! IB sends individual tick updates (bid price, bid size, ask price, ask size)
//! as separate events. This aggregator accumulates them and emits complete
//! [`OrderBookL1`] snapshots.

use crate::{books::Level, subscription::book::OrderBookL1};
use chrono::{DateTime, Utc};
use ibapi::contracts::tick_types::TickType;
use ibapi::market_data::realtime::{TickPrice, TickSize, TickTypes};
use rust_decimal::Decimal;

use super::decimal_from_f64;

/// Aggregates IB tick updates into OrderBookL1 snapshots.
///
/// Emits a new OrderBookL1 whenever any bid or ask price/size is available.
/// Does not require both sides to emit.
///
/// # Event Rate
///
/// Emits on every relevant tick, even if values are unchanged from the previous
/// emission. Consumers should deduplicate if needed.
#[derive(Debug, Default)]
pub struct QuoteAggregator {
    bid_price: Option<Decimal>,
    bid_size: Option<Decimal>,
    ask_price: Option<Decimal>,
    ask_size: Option<Decimal>,
    last_update: Option<DateTime<Utc>>,
}

impl QuoteAggregator {
    /// Create a new aggregator.
    pub fn new() -> Self {
        Self::default()
    }

    /// Process a tick and potentially emit an OrderBookL1.
    ///
    /// Returns `Some(OrderBookL1)` when any quote data is available after
    /// processing this tick.
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
                self.process_price_tick_type(&ps.price_tick_type, ps.price, now);
                self.process_size_tick_type(&ps.size_tick_type, ps.size, now);
                self.try_emit()
            }
            _ => None,
        }
    }

    fn update_price(&mut self, tick: &TickPrice, now: DateTime<Utc>) -> Option<OrderBookL1> {
        self.process_price_tick_type(&tick.tick_type, tick.price, now);
        self.try_emit()
    }

    fn process_price_tick_type(&mut self, tick_type: &TickType, price: f64, now: DateTime<Utc>) {
        // Convert and validate: NaN/Inf/out-of-range values must not overwrite valid prices
        let Some(price) = decimal_from_f64(price) else {
            return;
        };
        match tick_type {
            TickType::Bid | TickType::DelayedBid => {
                self.bid_price = Some(price);
                self.last_update = Some(now);
            }
            TickType::Ask | TickType::DelayedAsk => {
                self.ask_price = Some(price);
                self.last_update = Some(now);
            }
            _ => {}
        }
    }

    fn update_size(&mut self, tick: &TickSize, now: DateTime<Utc>) -> Option<OrderBookL1> {
        self.process_size_tick_type(&tick.tick_type, tick.size, now);
        self.try_emit()
    }

    fn process_size_tick_type(&mut self, tick_type: &TickType, size: f64, now: DateTime<Utc>) {
        // Convert and validate: NaN/Inf/out-of-range values must not overwrite valid sizes
        let Some(size) = decimal_from_f64(size) else {
            return;
        };
        match tick_type {
            TickType::BidSize | TickType::DelayedBidSize => {
                self.bid_size = Some(size);
                self.last_update = Some(now);
            }
            TickType::AskSize | TickType::DelayedAskSize => {
                self.ask_size = Some(size);
                self.last_update = Some(now);
            }
            _ => {}
        }
    }

    fn try_emit(&self) -> Option<OrderBookL1> {
        // Values already converted to Decimal at storage time
        let bid = self.bid_price.map(|price| Level {
            price,
            amount: self.bid_size.unwrap_or(Decimal::ZERO),
        });

        let ask = self.ask_price.map(|price| Level {
            price,
            amount: self.ask_size.unwrap_or(Decimal::ZERO),
        });

        if bid.is_some() || ask.is_some() {
            #[allow(clippy::expect_used)] // Invariant: last_update set when bid/ask present
            Some(OrderBookL1 {
                last_update_time: self
                    .last_update
                    .expect("last_update set when bid/ask present"),
                best_bid: bid,
                best_ask: ask,
            })
        } else {
            None
        }
    }
}

#[cfg(test)]
// Test code may unwrap freely since panics indicate test failure
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use ibapi::market_data::realtime::TickAttribute;
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

    #[test]
    fn partial_update_emits_partial() {
        let mut agg = QuoteAggregator::new();
        let now = Utc::now();

        let result = agg.update(&tick_price(TickType::Bid, 100.0), now);
        assert!(result.is_some());
        let l1 = result.unwrap();
        assert!(l1.best_bid.is_some());
        assert!(l1.best_ask.is_none());
    }

    #[test]
    fn complete_quote_emits() {
        let mut agg = QuoteAggregator::new();
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
        let mut agg = QuoteAggregator::new();
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
        let mut agg = QuoteAggregator::new();
        let now = Utc::now();

        let result = agg.update(&tick_price(TickType::Last, 100.0), now);
        assert!(result.is_none());
    }

    #[test]
    fn invalid_price_skipped() {
        let mut agg = QuoteAggregator::new();
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
        let mut agg = QuoteAggregator::new();
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
