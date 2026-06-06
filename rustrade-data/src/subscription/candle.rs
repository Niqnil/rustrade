use super::SubscriptionKind;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// Barter [`Subscription`](super::Subscription) [`SubscriptionKind`] that yields [`Candle`]
/// [`MarketEvent<T>`](crate::event::MarketEvent) events.
#[derive(
    Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default, Deserialize, Serialize,
)]
pub struct Candles;

impl SubscriptionKind for Candles {
    type Event = Candle;

    fn as_str(&self) -> &'static str {
        "candles"
    }
}

impl std::fmt::Display for Candles {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use chrono::Duration;

    /// Parse an RFC3339 UTC instant in tests.
    fn dt(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
    }

    #[test]
    fn fixed_step_adds_duration_exactly() {
        let open = dt("2024-01-15T12:00:00Z");
        assert_eq!(
            close_time_from_open(open, IntervalStep::Fixed(Duration::minutes(1))),
            Some(dt("2024-01-15T12:01:00Z"))
        );
        assert_eq!(
            close_time_from_open(open, IntervalStep::Fixed(Duration::hours(1))),
            Some(dt("2024-01-15T13:00:00Z"))
        );
        // Daily/weekly are exact fixed durations in UTC (no DST).
        assert_eq!(
            close_time_from_open(
                dt("2024-01-15T00:00:00Z"),
                IntervalStep::Fixed(Duration::days(1))
            ),
            Some(dt("2024-01-16T00:00:00Z"))
        );
        assert_eq!(
            close_time_from_open(
                dt("2024-01-15T00:00:00Z"),
                IntervalStep::Fixed(Duration::weeks(1))
            ),
            Some(dt("2024-01-22T00:00:00Z"))
        );
    }

    #[test]
    fn fixed_daily_step_crosses_month_boundary() {
        // A Jan 31 daily bar closes at Feb 1 00:00 UTC via Fixed(1 day).
        assert_eq!(
            close_time_from_open(
                dt("2024-01-31T00:00:00Z"),
                IntervalStep::Fixed(Duration::days(1))
            ),
            Some(dt("2024-02-01T00:00:00Z"))
        );
    }

    #[test]
    fn months_step_uses_calendar_arithmetic() {
        // Jan -> Feb (not +30 days = Jan 31).
        assert_eq!(
            close_time_from_open(dt("2024-01-01T00:00:00Z"), IntervalStep::Months(1)),
            Some(dt("2024-02-01T00:00:00Z"))
        );
        // Leap-year Feb -> Mar (Feb has 29 days in 2024).
        assert_eq!(
            close_time_from_open(dt("2024-02-01T00:00:00Z"), IntervalStep::Months(1)),
            Some(dt("2024-03-01T00:00:00Z"))
        );
        // Quarter = 3 months.
        assert_eq!(
            close_time_from_open(dt("2024-01-01T00:00:00Z"), IntervalStep::Months(3)),
            Some(dt("2024-04-01T00:00:00Z"))
        );
        // Year = 12 months.
        assert_eq!(
            close_time_from_open(dt("2024-01-01T00:00:00Z"), IntervalStep::Months(12)),
            Some(dt("2025-01-01T00:00:00Z"))
        );
    }

    #[test]
    fn months_step_clamps_jan_31_anchor() {
        // Monthly bar opens always land on the 1st from all known producers, so
        // this clamping is unreachable in practice; the test pins chrono's
        // documented behaviour for the variable-length-month edge case.
        // chrono clamps to the last valid day: Jan 31 + 1 month -> Feb 29 (leap year).
        assert_eq!(
            close_time_from_open(dt("2024-01-31T00:00:00Z"), IntervalStep::Months(1)),
            Some(dt("2024-02-29T00:00:00Z"))
        );
    }

    #[test]
    fn overflow_returns_none_not_panic() {
        let max = DateTime::<Utc>::MAX_UTC;
        assert_eq!(close_time_from_open(max, IntervalStep::Months(1)), None);
        assert_eq!(
            close_time_from_open(max, IntervalStep::Fixed(Duration::days(1))),
            None
        );
    }

    #[test]
    fn open_time_from_close_is_inverse() {
        // open = close − interval, for both Fixed and Months steps.
        assert_eq!(
            open_time_from_close(
                dt("2024-01-15T13:00:00Z"),
                IntervalStep::Fixed(Duration::hours(1))
            ),
            Some(dt("2024-01-15T12:00:00Z"))
        );
        // Feb 1 close of a January monthly bar → Jan 1 open.
        assert_eq!(
            open_time_from_close(dt("2024-02-01T00:00:00Z"), IntervalStep::Months(1)),
            Some(dt("2024-01-01T00:00:00Z"))
        );
        // Round-trip identity for the inputs this library actually produces:
        // monthly/quarterly closes always land on a calendar 1st, where chrono's
        // month arithmetic round-trips exactly. (It is NOT a universal identity —
        // `Months` day-clamping is asymmetric for non-1st anchors, e.g.
        // Feb 29 −1mo → Jan 29, +1mo → Feb 29; see `months_step_clamps_jan_31_anchor`.)
        let close = dt("2024-04-01T00:00:00Z");
        let open = open_time_from_close(close, IntervalStep::Months(3)).unwrap();
        assert_eq!(
            close_time_from_open(open, IntervalStep::Months(3)),
            Some(close)
        );
    }

    #[test]
    fn open_time_from_close_underflow_returns_none() {
        let min = DateTime::<Utc>::MIN_UTC;
        assert_eq!(open_time_from_close(min, IntervalStep::Months(1)), None);
        assert_eq!(
            open_time_from_close(min, IntervalStep::Fixed(Duration::days(1))),
            None
        );
    }
}

/// Normalised Barter OHLCV [`Candle`] model.
///
/// # `close_time` contract
///
/// `close_time` is the **exclusive end-of-period boundary** of the candle:
///
/// ```text
/// close_time == open_time + interval
/// ```
///
/// A candle aggregates the trades that fall in the **half-open interval**
/// `[close_time − interval, close_time)` — i.e. trades with
/// `open_time ≤ ts < close_time`. A trade landing exactly on `close_time`
/// belongs to the **next** candle, so `close_time` equals the next candle's
/// open instant.
///
/// Two distinct caveats apply to the boundary — do not conflate them:
///
/// - **Not session-aligned** (daily/weekly/monthly): the boundary is the UTC
///   period grid (`day → next 00:00 UTC`, etc.), **not** an exchange session
///   close. The library has no session calendar.
/// - **Variable-length calendar arithmetic** (month/quarter/year only): these
///   are nominal boundaries computed with calendar months (chrono [`Months`]),
///   not fixed [`Duration`]s. Daily and weekly are exact fixed durations in UTC
///   (no DST), exact to the millisecond.
///
/// `Candle` deliberately carries **neither `open_time` nor `interval`** — recover
/// them from the originating fetch request / subscription resolution
/// (`open_time ≡ close_time − interval`). Range-computing producers derive
/// `close_time` through [`close_time_from_open`] so the boundary is defined in
/// exactly one place (the Massive WS path uses the venue-supplied boundary
/// directly — see [`close_time_from_open`] for the full producer list).
///
/// [`Months`]: chrono::Months
/// [`Duration`]: chrono::Duration
#[derive(Copy, Clone, PartialEq, PartialOrd, Debug, Deserialize, Serialize)]
pub struct Candle {
    pub close_time: DateTime<Utc>,
    pub open: Decimal,
    pub high: Decimal,
    pub low: Decimal,
    pub close: Decimal,
    pub volume: Decimal,
    pub trade_count: u64,
}

/// One step from a candle's open instant to its exclusive close boundary.
///
/// Keyed on a primitive step type (not on any per-exchange interval enum) so
/// every producer — regardless of how it names its native intervals — maps to
/// the same two cases and routes through [`close_time_from_open`]. This is the
/// mechanism that makes the [`Candle::close_time`] contract *enforced by
/// construction* rather than merely documented.
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub enum IntervalStep {
    /// A fixed-length step (seconds through weeks — exact in UTC, no DST).
    Fixed(chrono::Duration),
    /// A variable-length calendar step in whole months. Covers calendar
    /// `month` (1), `quarter` (3) and `year` (12) — leap-year-correct via
    /// chrono's [`checked_add_months`](DateTime::checked_add_months).
    Months(u32),
}

/// Compute a candle's exclusive `close_time` boundary from its `open` instant.
///
/// This is the single shared boundary helper that every range-computing
/// [`Candle`] producer routes through (Massive REST, Hyperliquid, IBKR), so the
/// `close_time == open + interval` contract is computed in exactly one place. The
/// Massive WS path is the lone exception: it trusts the venue-supplied boundary
/// directly (see `WsAggregateMsg::into_candle` for the rationale).
///
/// - [`IntervalStep::Fixed`] adds a [`chrono::Duration`].
/// - [`IntervalStep::Months`] uses calendar-correct month arithmetic
///   ([`checked_add_months`](DateTime::checked_add_months)), so a Jan monthly
///   bar yields `Feb 1 00:00 UTC` and a leap-year Feb monthly bar yields
///   `Mar 1 00:00 UTC`.
///
/// # Returns
///
/// `None` on overflow — when the computed boundary falls outside the
/// representable [`DateTime<Utc>`] range. Callers **must** surface this as their
/// producer error type (an observable failure), **never** a silent fallback to a
/// plausible-but-wrong timestamp such as `UNIX_EPOCH`.
#[must_use]
pub fn close_time_from_open(open: DateTime<Utc>, step: IntervalStep) -> Option<DateTime<Utc>> {
    match step {
        IntervalStep::Fixed(duration) => open.checked_add_signed(duration),
        IntervalStep::Months(n) => open.checked_add_months(chrono::Months::new(n)),
    }
}

/// Inverse of [`close_time_from_open`]: recover a candle's `open` instant from its
/// exclusive `close_time` boundary (`open == close − interval`).
///
/// Used by range-bounded historical fetches to widen the venue request window:
/// the candle whose `close_time == start` has `open == start − interval`, so a
/// fetch that wants `close_time ∈ [start, end]` must ask the venue for opens down
/// to `start − interval` (then trim the result by `close_time`). See
/// [`Candle::close_time`].
///
/// # Returns
///
/// `None` on underflow (the computed open falls below the representable
/// [`DateTime<Utc>`] range).
///
/// For the range-widening use-case this `None` is **not** an error: it means the
/// candle whose `close_time == start` would have an unrepresentable open
/// (`start − interval` below [`DateTime<Utc>`] minimum) and therefore cannot
/// exist. Callers should fall back to the original lower bound — the un-widened
/// fetch already yields the complete, correct result set, so this is the right
/// outcome rather than a silent failure. (Contrast [`close_time_from_open`],
/// whose `None` *does* signal data loss for a real candle and must be surfaced
/// as an error.)
#[must_use]
pub fn open_time_from_close(close: DateTime<Utc>, step: IntervalStep) -> Option<DateTime<Utc>> {
    match step {
        IntervalStep::Fixed(duration) => close.checked_sub_signed(duration),
        IntervalStep::Months(n) => close.checked_sub_months(chrono::Months::new(n)),
    }
}
