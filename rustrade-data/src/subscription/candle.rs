use super::SubscriptionKind;
use chrono::{DateTime, Duration, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use std::str::FromStr;

/// Barter [`Subscription`](super::Subscription) [`SubscriptionKind`] that yields [`Candle`]
/// [`MarketEvent<T>`](crate::event::MarketEvent) events.
///
/// The [`interval`](Self::interval) is intrinsic to a candle subscription — it is
/// the resolution being streamed, so there is no meaningful default (a phantom
/// "1m" default is a silent-bug footgun); the field is always explicit.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Deserialize, Serialize)]
pub struct Candles {
    /// The candle resolution to subscribe to. See [`CandleInterval`].
    pub interval: CandleInterval,
}

impl SubscriptionKind for Candles {
    type Event = Candle;

    /// Returns the fixed kind tag `"candles"`, independent of [`interval`](Self::interval).
    /// The tag identifies the subscription *kind* for routing and stays stable across
    /// resolutions; it is **not** the interval. For the resolution string use
    /// [`CandleInterval::as_str`] on the [`interval`](Self::interval) field — note that
    /// [`Display`](std::fmt::Display) for `Candles` also yields only `"candles"`.
    fn as_str(&self) -> &'static str {
        "candles"
    }
}

impl std::fmt::Display for Candles {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
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

    #[test]
    fn candle_interval_all_covers_every_variant_in_ascending_order() {
        // `ALL`'s length is pinned to the variant count by both the
        // `[CandleInterval; 16]` type and this assertion. Full variant *coverage*
        // is not compile-enforced (Rust has no stable variant_count), so keep
        // `ALL` in sync when adding a variant.
        assert_eq!(CandleInterval::ALL.len(), 16);

        // Verify the documented ascending-duration ordering directly via `to_step`.
        // Comparing against the derived `Ord` would be tautological — that order is
        // declaration order, identical to `ALL`'s. Mapping through durations instead
        // actually fails if a variant is listed out of order.
        fn approx_secs(interval: CandleInterval) -> i64 {
            match interval.to_step() {
                IntervalStep::Fixed(d) => d.num_seconds(),
                // Only `Month1` is calendar-based; ~30d keeps it above `Week1` (7d).
                IntervalStep::Months(n) => i64::from(n) * 30 * 24 * 60 * 60,
            }
        }
        for pair in CandleInterval::ALL.windows(2) {
            assert!(
                approx_secs(pair[0]) < approx_secs(pair[1]),
                "ALL must be in strictly ascending duration order: {:?} !< {:?}",
                pair[0],
                pair[1]
            );
        }
    }

    #[test]
    fn candle_interval_display_equals_as_str_for_every_variant() {
        for interval in CandleInterval::ALL {
            assert_eq!(interval.to_string(), interval.as_str());
        }
    }

    #[test]
    fn candle_interval_as_str_matches_binance_exactly() {
        // Case-sensitive: `1M` (month) is the only uppercase form.
        assert_eq!(CandleInterval::Sec1.as_str(), "1s");
        assert_eq!(CandleInterval::Min1.as_str(), "1m");
        assert_eq!(CandleInterval::Hour6.as_str(), "6h");
        assert_eq!(CandleInterval::Month1.as_str(), "1M");
    }

    #[test]
    fn candle_interval_from_str_is_inverse_of_as_str() {
        for interval in CandleInterval::ALL {
            assert_eq!(interval.as_str().parse::<CandleInterval>(), Ok(interval));
        }
    }

    #[test]
    fn candle_interval_from_str_rejects_garbage() {
        assert!("".parse::<CandleInterval>().is_err());
        assert!("7m".parse::<CandleInterval>().is_err());
        // Case-sensitive: `1m` (minute) must not parse as `1M` (month) or vice versa.
        assert_eq!("1m".parse::<CandleInterval>(), Ok(CandleInterval::Min1));
        assert_eq!("1M".parse::<CandleInterval>(), Ok(CandleInterval::Month1));
    }

    #[test]
    fn candle_interval_serde_round_trips_every_variant() {
        for interval in CandleInterval::ALL {
            let json = serde_json::to_string(&interval).unwrap();
            // Serialises as the bare `as_str()` string.
            assert_eq!(json, format!("\"{}\"", interval.as_str()));
            let back: CandleInterval = serde_json::from_str(&json).unwrap();
            assert_eq!(back, interval);
        }
    }

    #[test]
    fn candles_kind_carries_interval_and_serde_round_trips() {
        let kind = Candles {
            interval: CandleInterval::Hour6,
        };
        let json = serde_json::to_string(&kind).unwrap();
        assert_eq!(json, r#"{"interval":"6h"}"#);
        let back: Candles = serde_json::from_str(&json).unwrap();
        assert_eq!(back, kind);
        // `SubscriptionKind::as_str` stays the kind tag, independent of interval.
        assert_eq!(kind.as_str(), "candles");
        assert_eq!(kind.to_string(), "candles");
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
/// # Using a `Candle` with the engine
///
/// When wrapping a `Candle` into a [`MarketEvent`](crate::event::MarketEvent) for
/// a consuming engine (live or backtest), set
/// [`time_exchange`](crate::event::MarketEvent::time_exchange) to this
/// `close_time` — it is the period-END instant, the only choice that avoids
/// lookahead (see that field's contract). The library's own candle producers
/// already do this.
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

/// Candle interval/resolution, shared across every venue that produces candles.
///
/// This is the **venue-agnostic union** of all supported candle resolutions — it
/// is deliberately *not* gated to any one exchange's capabilities. Per-venue
/// support rules (e.g. Hyperliquid rejecting `Sec1`/`Hour6`) live in the exchange
/// layer, not on this enum (separation of concerns). Because the enum is a union,
/// **each venue's interval guard must be re-reviewed whenever a variant is added.**
///
/// # String form
///
/// [`as_str`](Self::as_str) is the **single source of truth** for every string
/// representation: [`Display`](std::fmt::Display), [`Serialize`], [`FromStr`] and [`Deserialize`] all
/// delegate to it (or its inverse), so there is exactly one place mapping
/// variant↔string. The strings match Binance's kline `interval` parameter exactly
/// and are **case-sensitive** — note `Month1 → "1M"` (uppercase) vs `Min1 → "1m"`.
///
/// # Ordering
///
/// Variants are declared in **ascending duration** order. `Ord`/`PartialOrd` exist
/// only as a compile requirement (the type embeds in
/// [`Candles`], which must stay `Ord` to
/// preserve the derived `Ord` on `Subscription`); the chronological declaration
/// order makes the derived order at least sensible. Nothing currently sorts
/// intervals semantically.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Ord, PartialOrd)]
pub enum CandleInterval {
    /// 1 second
    Sec1,
    /// 1 minute
    Min1,
    /// 3 minutes
    Min3,
    /// 5 minutes
    Min5,
    /// 15 minutes
    Min15,
    /// 30 minutes
    Min30,
    /// 1 hour
    Hour1,
    /// 2 hours
    Hour2,
    /// 4 hours
    Hour4,
    /// 6 hours
    Hour6,
    /// 8 hours
    Hour8,
    /// 12 hours
    Hour12,
    /// 1 day
    Day1,
    /// 3 days
    Day3,
    /// 1 week
    Week1,
    /// 1 month
    Month1,
}

impl CandleInterval {
    /// Every [`CandleInterval`] variant, in ascending-duration declaration order.
    ///
    /// Lets variant-exhaustive tests (round-trip, channel-suffix drift guards)
    /// iterate without hand-listing the variants.
    ///
    /// When adding a [`CandleInterval`] variant, add it here too: the length
    /// literal and the `candle_interval_all_covers_every_variant_in_ascending_order`
    /// test pin `ALL`'s length to the variant count, but full coverage is not
    /// compile-enforced — the exhaustive `match`es elsewhere are the compile gate.
    pub const ALL: [CandleInterval; 16] = [
        Self::Sec1,
        Self::Min1,
        Self::Min3,
        Self::Min5,
        Self::Min15,
        Self::Min30,
        Self::Hour1,
        Self::Hour2,
        Self::Hour4,
        Self::Hour6,
        Self::Hour8,
        Self::Hour12,
        Self::Day1,
        Self::Day3,
        Self::Week1,
        Self::Month1,
    ];

    /// The exchange string form of this interval (e.g. `"1m"`, `"6h"`, `"1M"`).
    ///
    /// The **single source of truth** for all string representations — `Display`,
    /// `Serialize`, `FromStr` and `Deserialize` all key off this. Case-sensitive:
    /// `Month1 → "1M"`, every other variant lowercase.
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Sec1 => "1s",
            Self::Min1 => "1m",
            Self::Min3 => "3m",
            Self::Min5 => "5m",
            Self::Min15 => "15m",
            Self::Min30 => "30m",
            Self::Hour1 => "1h",
            Self::Hour2 => "2h",
            Self::Hour4 => "4h",
            Self::Hour6 => "6h",
            Self::Hour8 => "8h",
            Self::Hour12 => "12h",
            Self::Day1 => "1d",
            Self::Day3 => "3d",
            Self::Week1 => "1w",
            Self::Month1 => "1M",
        }
    }

    /// Map this interval to the shared [`IntervalStep`] used to compute a candle's
    /// exclusive `close_time` boundary via [`close_time_from_open`]. All intervals
    /// are fixed-length except `1M`, which is a calendar month.
    #[must_use]
    pub fn to_step(self) -> IntervalStep {
        match self {
            Self::Sec1 => IntervalStep::Fixed(Duration::seconds(1)),
            Self::Min1 => IntervalStep::Fixed(Duration::minutes(1)),
            Self::Min3 => IntervalStep::Fixed(Duration::minutes(3)),
            Self::Min5 => IntervalStep::Fixed(Duration::minutes(5)),
            Self::Min15 => IntervalStep::Fixed(Duration::minutes(15)),
            Self::Min30 => IntervalStep::Fixed(Duration::minutes(30)),
            Self::Hour1 => IntervalStep::Fixed(Duration::hours(1)),
            Self::Hour2 => IntervalStep::Fixed(Duration::hours(2)),
            Self::Hour4 => IntervalStep::Fixed(Duration::hours(4)),
            Self::Hour6 => IntervalStep::Fixed(Duration::hours(6)),
            Self::Hour8 => IntervalStep::Fixed(Duration::hours(8)),
            Self::Hour12 => IntervalStep::Fixed(Duration::hours(12)),
            Self::Day1 => IntervalStep::Fixed(Duration::days(1)),
            Self::Day3 => IntervalStep::Fixed(Duration::days(3)),
            Self::Week1 => IntervalStep::Fixed(Duration::weeks(1)),
            Self::Month1 => IntervalStep::Months(1),
        }
    }
}

impl std::fmt::Display for CandleInterval {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Error returned by [`CandleInterval::from_str`] for an unrecognised string.
///
/// The offending input is kept private and exposed via [`input`](Self::input) so
/// the error's representation can evolve (e.g. gaining context) without a
/// breaking change — mirroring `std`'s opaque parse-error types.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseCandleIntervalError {
    invalid: String,
}

impl ParseCandleIntervalError {
    /// The input string that failed to parse.
    #[must_use]
    pub fn input(&self) -> &str {
        &self.invalid
    }
}

impl std::fmt::Display for ParseCandleIntervalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "invalid candle interval: {:?}", self.invalid)
    }
}

impl std::error::Error for ParseCandleIntervalError {}

impl FromStr for CandleInterval {
    type Err = ParseCandleIntervalError;

    /// The inverse of [`CandleInterval::as_str`] — case-sensitive (`"1M"` is the
    /// only uppercase form). Keeps variant↔string mapping in exactly one place.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "1s" => Ok(Self::Sec1),
            "1m" => Ok(Self::Min1),
            "3m" => Ok(Self::Min3),
            "5m" => Ok(Self::Min5),
            "15m" => Ok(Self::Min15),
            "30m" => Ok(Self::Min30),
            "1h" => Ok(Self::Hour1),
            "2h" => Ok(Self::Hour2),
            "4h" => Ok(Self::Hour4),
            "6h" => Ok(Self::Hour6),
            "8h" => Ok(Self::Hour8),
            "12h" => Ok(Self::Hour12),
            "1d" => Ok(Self::Day1),
            "3d" => Ok(Self::Day3),
            "1w" => Ok(Self::Week1),
            "1M" => Ok(Self::Month1),
            other => Err(ParseCandleIntervalError {
                invalid: other.to_owned(),
            }),
        }
    }
}

impl Serialize for CandleInterval {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for CandleInterval {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = <std::borrow::Cow<'de, str>>::deserialize(deserializer)?;
        raw.parse().map_err(de::Error::custom)
    }
}
