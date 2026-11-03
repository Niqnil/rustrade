//! The lowest and highest price of one instrument over a trailing window of the venue's clock:
//! what [`SpotMarginConfig::with_borrow_price_window`] values an asset at when the venue lends.
//!
//! [`SpotMarginConfig::with_borrow_price_window`]: crate::margin::SpotMarginConfig::with_borrow_price_window

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;

/// One instrument's prices over a trailing window, kept so that its lowest and highest from any
/// instant on are each read off the front of a queue.
///
/// Each queue holds only the prices that could still be an extreme of some window: `lows` drops a
/// price once a lower or equal one is recorded after it, since every window that holds the first
/// also holds the later one, and `highs` likewise for higher. So `lows` rises from front to back
/// and `highs` falls, both in the order recorded, and the first entry of either at or after an
/// instant is that queue's extreme from the instant on. Recording is amortised constant time: each
/// price enters and leaves each queue once.
///
/// Instants must be recorded in non-decreasing order, as the venue's clock, which never moves
/// back, records them.
///
/// Memory is the number of prices still a candidate extreme: at most every observation inside
/// the window, in a market that only rises or only falls, and far fewer in one that turns.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize, Serialize)]
pub(super) struct PriceWindow {
    /// Strictly rising prices, each with the instant it was recorded.
    lows: VecDeque<(DateTime<Utc>, Decimal)>,
    /// Strictly falling prices, each with the instant it was recorded.
    highs: VecDeque<(DateTime<Utc>, Decimal)>,
}

impl PriceWindow {
    /// Records that the instrument traded between `low` and `high` at `time`, and forgets what was
    /// recorded before `since`, which no later window reaches back to.
    ///
    /// `time` must be no earlier than any instant recorded before, and `low` no higher than
    /// `high`.
    pub(super) fn record(
        &mut self,
        time: DateTime<Utc>,
        low: Decimal,
        high: Decimal,
        since: DateTime<Utc>,
    ) {
        debug_assert!(low <= high, "a low of {low} above the high of {high}");
        debug_assert!(
            self.lows.back().is_none_or(|(last, _)| *last <= time),
            "a price recorded at {time}, before one already recorded"
        );

        while self.lows.back().is_some_and(|(_, price)| *price >= low) {
            self.lows.pop_back();
        }
        self.lows.push_back((time, low));
        while self.highs.back().is_some_and(|(_, price)| *price <= high) {
            self.highs.pop_back();
        }
        self.highs.push_back((time, high));

        for queue in [&mut self.lows, &mut self.highs] {
            while queue.front().is_some_and(|(recorded, _)| *recorded < since) {
                queue.pop_front();
            }
        }
    }

    /// The lowest price recorded at or after `since`, or `None` if none was.
    pub(super) fn low(&self, since: DateTime<Utc>) -> Option<Decimal> {
        first_since(&self.lows, since)
    }

    /// The highest price recorded at or after `since`, or `None` if none was.
    pub(super) fn high(&self, since: DateTime<Utc>) -> Option<Decimal> {
        first_since(&self.highs, since)
    }
}

/// The price of the first entry of `queue` recorded at or after `since`.
///
/// A binary search rather than a pop, since a valuation reads the venue without changing it. The
/// entries it passes over have fallen out of the window since the instrument's last
/// [`record`](PriceWindow::record), which may be long ago for an instrument no longer observed.
fn first_since(
    queue: &VecDeque<(DateTime<Utc>, Decimal)>,
    since: DateTime<Utc>,
) -> Option<Decimal> {
    // Recorded in time order, so the entries before `since` are a prefix.
    let first = queue.partition_point(|(recorded, _)| *recorded < since);
    queue.get(first).map(|(_, price)| *price)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use chrono::TimeDelta;
    use std::str::FromStr;

    fn at(seconds: i64) -> DateTime<Utc> {
        DateTime::<Utc>::UNIX_EPOCH + TimeDelta::seconds(seconds)
    }

    fn d(value: &str) -> Decimal {
        Decimal::from_str(value).unwrap()
    }

    #[test]
    fn an_empty_window_has_no_extremes() {
        let window = PriceWindow::default();
        assert_eq!(window.low(at(0)), None);
        assert_eq!(window.high(at(0)), None);
    }

    #[test]
    fn a_price_counts_from_the_instant_it_was_recorded_inclusive() {
        let mut window = PriceWindow::default();
        window.record(at(10), d("99"), d("101"), at(0));

        assert_eq!(window.low(at(10)), Some(d("99")));
        assert_eq!(window.high(at(10)), Some(d("101")));
        assert_eq!(window.low(at(11)), None);
        assert_eq!(window.high(at(11)), None);
    }

    #[test]
    fn matches_a_brute_force_minimum_and_maximum_over_every_window() {
        // A sequence with rises, falls, repeats and several prices at one instant.
        let prices = [
            (0, "5"),
            (1, "3"),
            (1, "7"),
            (2, "7"),
            (3, "2"),
            (5, "9"),
            (5, "9"),
            (6, "4"),
            (8, "4"),
            (9, "1"),
            (9, "8"),
            (12, "6"),
            (13, "10"),
            (13, "0.5"),
            (20, "3"),
        ];
        for span in [0, 1, 2, 3, 5, 8, 100] {
            let mut window = PriceWindow::default();
            for (index, (time, price)) in prices.iter().enumerate() {
                let now = at(*time);
                let since = now - TimeDelta::seconds(span);
                window.record(now, d(price), d(price), since);

                let inside = prices[..=index]
                    .iter()
                    .filter(|(recorded, _)| at(*recorded) >= since)
                    .map(|(_, price)| d(price));
                assert_eq!(
                    window.low(since),
                    inside.clone().min(),
                    "span {span}, at {time}"
                );
                assert_eq!(window.high(since), inside.max(), "span {span}, at {time}");
            }
        }
    }

    #[test]
    fn a_reading_after_the_last_record_skips_what_has_since_fallen_out() {
        let mut window = PriceWindow::default();
        window.record(at(0), d("1"), d("9"), at(0));
        window.record(at(5), d("4"), d("6"), at(0));

        assert_eq!(window.low(at(3)), Some(d("4")));
        assert_eq!(window.high(at(3)), Some(d("6")));
    }

    #[test]
    fn recording_forgets_what_fell_out_of_the_window() {
        let mut window = PriceWindow::default();
        window.record(at(0), d("1"), d("9"), at(0));
        window.record(at(1), d("2"), d("8"), at(0));
        window.record(at(10), d("5"), d("5"), at(5));

        assert_eq!(window.lows.len(), 1);
        assert_eq!(window.highs.len(), 1);
        assert_eq!(window.low(at(0)), Some(d("5")));
    }

    #[test]
    fn a_low_and_high_recorded_together_are_kept_apart() {
        // A bar's range: the low and the high of one instant.
        let mut window = PriceWindow::default();
        window.record(at(0), d("90"), d("110"), at(0));
        window.record(at(1), d("100"), d("100"), at(0));

        assert_eq!(window.low(at(0)), Some(d("90")));
        assert_eq!(window.high(at(0)), Some(d("110")));
        assert_eq!(window.low(at(1)), Some(d("100")));
        assert_eq!(window.high(at(1)), Some(d("100")));
    }
}
