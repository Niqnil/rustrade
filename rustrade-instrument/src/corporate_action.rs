use chrono::{DateTime, NaiveDate, NaiveTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// A corporate-action market fact — *what* happened to an instrument, independent of any
/// account, broker, or rounding policy.
///
/// This type carries **market facts only**. It deliberately contains no rounding policy, no
/// account context, and no resolved timestamp: a data source emitting a split knows the ratio,
/// not how a particular broker will round fractional shares or when a particular engine should
/// stamp the adjustment. Those concerns live with the consumer (see the engine-side
/// `SplitRoundingPolicy` and the `effective_time` resolved via [`split_effective_instant`]).
///
/// `#[non_exhaustive]`: further corporate actions (dividends, spin-offs, symbol changes, …) can
/// be added without breaking downstream exhaustive matches.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[non_exhaustive]
pub enum CorporateActionKind {
    /// A forward or reverse stock split, expressed as a single multiplicative `ratio`.
    ///
    /// `ratio = split_to / split_from`:
    /// - **Forward** split → `ratio > 1` (e.g. a 2-for-1 split is `2.0`; share count scales up,
    ///   per-share price scales down).
    /// - **Reverse** split → `ratio < 1` (e.g. a 1-for-10 reverse split is `0.1`; share count
    ///   scales down, per-share price scales up).
    StockSplit {
        /// `split_to / split_from`. See the variant docs for the forward/reverse convention.
        ratio: Decimal,
    },
}

/// Resolve a **stock split**'s *effective date* (a calendar `NaiveDate` market fact) to the exact
/// `DateTime<Utc>` instant at which the adjustment takes effect: **midnight (00:00) UTC** on that
/// date.
///
/// # Why midnight UTC
///
/// This is a convenience default tuned for **US equities**, where corporate actions take effect
/// at the start of the effective session. Midnight UTC falls in the overnight gap after the prior
/// session's close and before the effective session's open, so when the resulting instant is used
/// as a merge-sort key against intraday market events, the adjustment lands exactly where a broker
/// applies it — after the previous day's trading, before the effective day's first print. This
/// avoids look-ahead (the split is not applied to prior-session events) without skipping into the
/// effective session's data.
///
/// # When to override
///
/// Exchanges whose sessions are not naturally bracketed by midnight UTC (non-UTC venues) may need
/// a different resolution. Callers owning that context should construct the `DateTime<Utc>`
/// themselves rather than relying on this default.
///
/// This instant doubles as the **sort key** when interleaving a corporate-action event into a
/// time-ordered backtest replay stream.
pub fn split_effective_instant(date: NaiveDate) -> DateTime<Utc> {
    date.and_time(NaiveTime::MIN).and_utc()
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use chrono::Datelike;

    #[test]
    fn split_effective_instant_is_midnight_utc_on_the_date() {
        let date = NaiveDate::from_ymd_opt(2026, 6, 22).unwrap();
        let instant = split_effective_instant(date);

        assert_eq!(instant.date_naive(), date);
        assert_eq!(instant.time(), NaiveTime::MIN);
        assert_eq!(instant.naive_utc().year(), 2026);
        assert_eq!(instant.to_rfc3339(), "2026-06-22T00:00:00+00:00");
    }
}
