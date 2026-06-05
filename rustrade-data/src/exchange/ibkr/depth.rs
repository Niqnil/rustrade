//! Depth aggregation for IB market depth updates.
//!
//! IB sends individual depth updates (insert, update, delete) for each price
//! level. This aggregator maintains the order book state and emits
//! [`OrderBookEvent`] updates.

use crate::{
    books::{Asks, Bids, Level, OrderBook, OrderBookSide},
    subscription::book::OrderBookEvent,
};
use ibapi::market_data::realtime::{MarketDepth, MarketDepths};
use rust_decimal::Decimal;

use super::decimal_from_f64;

/// IB API depth operation: delete level at price
const IB_DEPTH_OP_DELETE: i32 = 2;

/// Aggregates IB market depth updates into OrderBook snapshots.
///
/// Maintains local order book state and emits updates on each depth event.
/// Uses pre-sorted [`OrderBookSide`]s internally to avoid per-tick sorting.
#[derive(Debug, Default)]
pub struct DepthAggregator {
    bids: OrderBookSide<Bids>,
    asks: OrderBookSide<Asks>,
    sequence: u64,
}

impl DepthAggregator {
    /// Create a new aggregator.
    pub fn new() -> Self {
        Self::default()
    }

    /// Process a depth update and emit an OrderBookEvent.
    ///
    /// Returns `Some(OrderBookEvent)` for actual depth updates, `None` for
    /// MarketDepthL2 updates (which include market maker attribution that we
    /// don't track — we aggregate into a simple anonymous book).
    ///
    /// Note: as of ibapi 3.x, server notices are delivered through the
    /// subscription's `SubscriptionItem::Notice` arm rather than as a variant of
    /// [`MarketDepths`], so they never reach this method.
    pub fn update(&mut self, depth: &MarketDepths) -> Option<OrderBookEvent> {
        match depth {
            MarketDepths::MarketDepth(d) => self.process_depth(d),
            MarketDepths::MarketDepthL2(_) => {
                // MarketDepthL2 includes market maker attribution - we aggregate
                // into a simple book without tracking individual market makers
                tracing::trace!("Discarding MarketDepthL2 event (market maker data not tracked)");
                None
            }
        }
    }

    fn process_depth(&mut self, depth: &MarketDepth) -> Option<OrderBookEvent> {
        // Skip levels with invalid price (e.g., NaN, Inf, DBL_MAX sentinel)
        // Note: depth.position (IB's position-based addressing) is ignored;
        // we use price-keyed book maintenance instead.
        let price = decimal_from_f64(depth.price)?;

        // For delete operations, size is irrelevant and may be NaN.
        // Only validate size for insert/update operations.
        let size = if depth.operation == IB_DEPTH_OP_DELETE {
            Decimal::ZERO
        } else {
            decimal_from_f64(depth.size)?
        };

        // IB API: side 0=Ask, 1=Bid (per EWrapper.updateMktDepth documentation)
        match depth.side {
            0 => self.update_asks(price, size),
            1 => self.update_bids(price, size),
            other => {
                // Unknown side from IB: skip to avoid corrupting book state.
                // IB protocol is stable so this branch should never execute.
                tracing::warn!(
                    side = other,
                    price = %price,
                    "Unknown IB depth side, skipping"
                );
                return None;
            }
        }

        self.sequence += 1;
        Some(OrderBookEvent::Snapshot(self.to_order_book()))
    }

    fn update_bids(&mut self, price: Decimal, size: Decimal) {
        // IB API: operation 0=Insert, 1=Update, 2=Delete
        // Size is already Decimal::ZERO for deletes; upsert_single removes zero-amount levels
        let level = Level {
            price,
            amount: size,
        };
        self.bids
            .upsert_single(level, |existing| existing.price.cmp(&level.price).reverse());
    }

    fn update_asks(&mut self, price: Decimal, size: Decimal) {
        let level = Level {
            price,
            amount: size,
        };
        self.asks
            .upsert_single(level, |existing| existing.price.cmp(&level.price));
    }

    fn to_order_book(&self) -> OrderBook {
        OrderBook::from_sides(self.sequence, None, self.bids.clone(), self.asks.clone())
    }

    /// Clear all book state.
    ///
    /// Useful for reconnection scenarios where stale book state should be
    /// discarded before receiving fresh depth updates. Note: sequence resets to 0.
    pub fn clear(&mut self) {
        self.bids = OrderBookSide::default();
        self.asks = OrderBookSide::default();
        self.sequence = 0;
    }
}

#[cfg(test)]
// Test code may unwrap freely since panics indicate test failure
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn depth(side: i32, operation: i32, price: f64, size: f64) -> MarketDepths {
        MarketDepths::MarketDepth(MarketDepth {
            position: 0,
            operation,
            side,
            price,
            size,
        })
    }

    #[test]
    fn insert_bid() {
        let mut agg = DepthAggregator::new();

        let event = agg.update(&depth(1, 0, 100.0, 10.0)).unwrap();

        match event {
            OrderBookEvent::Snapshot(book) => {
                assert_eq!(book.bids().levels().len(), 1);
                assert_eq!(book.bids().levels()[0].price, dec!(100));
                assert_eq!(book.bids().levels()[0].amount, dec!(10));
                assert!(book.asks().levels().is_empty());
            }
            _ => panic!("Expected Snapshot"),
        }
    }

    #[test]
    fn insert_ask() {
        let mut agg = DepthAggregator::new();

        let event = agg.update(&depth(0, 0, 101.0, 5.0)).unwrap();

        match event {
            OrderBookEvent::Snapshot(book) => {
                assert_eq!(book.asks().levels().len(), 1);
                assert_eq!(book.asks().levels()[0].price, dec!(101));
                assert_eq!(book.asks().levels()[0].amount, dec!(5));
                assert!(book.bids().levels().is_empty());
            }
            _ => panic!("Expected Snapshot"),
        }
    }

    #[test]
    fn update_level() {
        let mut agg = DepthAggregator::new();

        agg.update(&depth(1, 0, 100.0, 10.0));
        let event = agg.update(&depth(1, 1, 100.0, 15.0)).unwrap();

        match event {
            OrderBookEvent::Snapshot(book) => {
                assert_eq!(book.bids().levels().len(), 1);
                assert_eq!(book.bids().levels()[0].amount, dec!(15));
            }
            _ => panic!("Expected Snapshot"),
        }
    }

    #[test]
    fn delete_level() {
        let mut agg = DepthAggregator::new();

        agg.update(&depth(1, 0, 100.0, 10.0));
        agg.update(&depth(1, 0, 99.0, 5.0));
        let event = agg.update(&depth(1, 2, 100.0, 0.0)).unwrap();

        match event {
            OrderBookEvent::Snapshot(book) => {
                assert_eq!(book.bids().levels().len(), 1);
                assert_eq!(book.bids().levels()[0].price, dec!(99));
            }
            _ => panic!("Expected Snapshot"),
        }
    }

    #[test]
    fn multiple_levels() {
        let mut agg = DepthAggregator::new();

        agg.update(&depth(1, 0, 100.0, 10.0));
        agg.update(&depth(1, 0, 99.0, 20.0));
        agg.update(&depth(0, 0, 101.0, 5.0));
        let event = agg.update(&depth(0, 0, 102.0, 3.0)).unwrap();

        match event {
            OrderBookEvent::Snapshot(book) => {
                assert_eq!(book.bids().levels().len(), 2);
                assert_eq!(book.asks().levels().len(), 2);

                // Verify sort order: bids descending, asks ascending
                let bids = book.bids().levels();
                assert!(
                    bids[0].price > bids[1].price,
                    "bids should be sorted descending: {:?}",
                    bids
                );
                let asks = book.asks().levels();
                assert!(
                    asks[0].price < asks[1].price,
                    "asks should be sorted ascending: {:?}",
                    asks
                );
            }
            _ => panic!("Expected Snapshot"),
        }
    }

    #[test]
    fn invalid_price_skipped() {
        let mut agg = DepthAggregator::new();

        // NaN price should be skipped
        let result = agg.update(&depth(1, 0, f64::NAN, 10.0));
        assert!(result.is_none());

        // Infinity should be skipped
        let result = agg.update(&depth(1, 0, f64::INFINITY, 10.0));
        assert!(result.is_none());

        // Valid price should work
        let result = agg.update(&depth(1, 0, 100.0, 10.0));
        assert!(result.is_some());
    }

    #[test]
    fn delete_with_nan_size_removes_level() {
        let mut agg = DepthAggregator::new();

        // Insert a bid level
        agg.update(&depth(1, 0, 100.0, 10.0));

        // Delete with NaN size should still remove the level (size irrelevant for deletes)
        let event = agg.update(&depth(1, 2, 100.0, f64::NAN)).unwrap();

        match event {
            OrderBookEvent::Snapshot(book) => {
                assert!(
                    book.bids().levels().is_empty(),
                    "Delete with NaN size should remove level"
                );
            }
            _ => panic!("Expected Snapshot"),
        }
    }

    #[test]
    fn clear_resets_state() {
        let mut agg = DepthAggregator::new();

        // Build up some book state
        agg.update(&depth(1, 0, 100.0, 10.0));
        agg.update(&depth(1, 0, 99.0, 5.0));
        agg.update(&depth(0, 0, 101.0, 8.0));
        assert_eq!(agg.sequence, 3);

        // Clear should reset everything
        agg.clear();

        assert_eq!(agg.sequence, 0, "sequence should reset to 0");
        assert!(agg.bids.levels().is_empty(), "bids should be empty");
        assert!(agg.asks.levels().is_empty(), "asks should be empty");

        // Next update should work normally with sequence 1
        let event = agg.update(&depth(1, 0, 50.0, 1.0)).unwrap();
        match event {
            OrderBookEvent::Snapshot(book) => {
                assert_eq!(book.sequence(), 1);
                assert_eq!(book.bids().levels().len(), 1);
            }
            _ => panic!("Expected Snapshot"),
        }
    }
}
