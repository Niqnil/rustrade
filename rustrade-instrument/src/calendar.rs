//! [`SessionCalendar`] answers one question, the first session close after an instant, which is
//! what a day order's lifetime needs. [`DailyClose`] answers it from a fixed rule: one close time
//! in a time zone, every weekday. It knows no holidays and no early closes.

use chrono::{DateTime, Datelike, NaiveDate, NaiveTime, TimeDelta, TimeZone, Utc, Weekday};
use serde::{Deserialize, Serialize};
use std::{cmp::Ordering, fmt::Debug};
use thiserror::Error;

/// The IANA time zone a [`DailyClose`] is in, re-exported so a caller needs no `chrono-tz`
/// dependency of its own: `"America/New_York".parse::<Tz>()`.
pub use chrono_tz::Tz;

/// When a market's trading sessions close.
///
/// Instants go in and come out in UTC. The time zone, the trading days and the close times are
/// the implementation's own business, so a fixed daily rule ([`DailyClose`]) and an exchange
/// calendar with holidays and early closes answer through the same method.
///
/// # Contract
/// [`next_close_after`](Self::next_close_after) answers a close **strictly after** `after`: at
/// exactly a close, it answers the next session's. A caller relies on this to step from one
/// close to the next, and may treat an answer not after `after` as a broken calendar.
pub trait SessionCalendar: Debug + Send + Sync {
    /// The first session close strictly after `after`.
    ///
    /// # Errors
    /// A [`CalendarError`] if the calendar cannot say: for example, `after` is past the last
    /// session it knows of.
    fn next_close_after(&self, after: DateTime<Utc>) -> Result<DateTime<Utc>, CalendarError>;
}

/// Why a [`SessionCalendar`] could not answer.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum CalendarError {
    /// The calendar knows of no session close after `after`.
    #[error("no session close is known after {after}")]
    #[non_exhaustive]
    OutOfRange {
        /// The instant the calendar was asked from.
        after: DateTime<Utc>,
    },
    /// The calendar could not be consulted, for example because the source it reads from
    /// failed.
    #[error("the session calendar is unavailable: {reason}")]
    #[non_exhaustive]
    Unavailable {
        /// What went wrong, for a log.
        reason: String,
    },
}

impl CalendarError {
    /// [`CalendarError::OutOfRange`].
    pub fn out_of_range(after: DateTime<Utc>) -> Self {
        Self::OutOfRange { after }
    }

    /// [`CalendarError::Unavailable`].
    pub fn unavailable(reason: impl Into<String>) -> Self {
        Self::Unavailable {
            reason: reason.into(),
        }
    }
}

/// A session close at one local time of day, every weekday: Monday to Friday by the date in
/// `zone`.
///
/// A fixed rule, not an exchange calendar. It has **no holidays and no early closes**, so on a
/// market holiday it still answers that day's close, and on a half day it answers the regular
/// close rather than the early one.
///
/// `zone` is an IANA zone, so the close moves with daylight saving: 16:00 in `America/New_York`
/// is 20:00 UTC in summer and 21:00 UTC in winter. A close falling in a daylight-saving gap or
/// overlap is placed as [`instant_at_local`] places it.
///
/// ```
/// use chrono::{NaiveTime, TimeZone, Utc};
/// use rustrade_instrument::calendar::{DailyClose, SessionCalendar};
///
/// let close = DailyClose::new(
///     NaiveTime::from_hms_opt(16, 0, 0).unwrap(),
///     chrono_tz::America::New_York,
/// );
/// // Friday 2026-11-13 after the close: the next close is Monday's.
/// let after = Utc.with_ymd_and_hms(2026, 11, 13, 22, 0, 0).unwrap();
/// assert_eq!(
///     close.next_close_after(after),
///     Ok(Utc.with_ymd_and_hms(2026, 11, 16, 21, 0, 0).unwrap())
/// );
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DailyClose {
    close: NaiveTime,
    zone: Tz,
}

impl DailyClose {
    /// A close at local time `close` in `zone`, every weekday.
    pub fn new(close: NaiveTime, zone: Tz) -> Self {
        Self { close, zone }
    }

    /// The local time of day of the close.
    pub fn close(&self) -> NaiveTime {
        self.close
    }

    /// The zone the close is in.
    pub fn zone(&self) -> Tz {
        self.zone
    }
}

/// Ordered by close time, then by zone name: [`Tz`] has no order of its own.
impl Ord for DailyClose {
    fn cmp(&self, other: &Self) -> Ordering {
        self.close
            .cmp(&other.close)
            .then_with(|| self.zone.name().cmp(other.zone.name()))
    }
}

impl PartialOrd for DailyClose {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl SessionCalendar for DailyClose {
    fn next_close_after(&self, after: DateTime<Utc>) -> Result<DateTime<Utc>, CalendarError> {
        // The close on `after`'s own local date may already have passed, and a weekend can lie
        // between, so a weekday close after `after` is within the next four local dates.
        let mut date = after.with_timezone(&self.zone).date_naive();
        for _ in 0..5 {
            let weekday = !matches!(date.weekday(), Weekday::Sat | Weekday::Sun);
            if weekday
                && let Some(close) = instant_at_local(&self.zone, date, self.close)
                && close > after
            {
                return Ok(close);
            }
            date = date.succ_opt().ok_or(CalendarError::out_of_range(after))?;
        }
        Err(CalendarError::out_of_range(after))
    }
}

/// The instant at local `time` on local `date` in `zone`.
///
/// A local time that falls in a daylight-saving gap does not exist, so it is taken at the first
/// local time after the gap, to the minute. One that falls in an overlap exists twice, so it is
/// taken the first time. `None` only if the instant is out of range.
pub fn instant_at_local<Z: TimeZone>(
    zone: &Z,
    date: NaiveDate,
    time: NaiveTime,
) -> Option<DateTime<Utc>> {
    let local = date.and_time(time);

    // A daylight-saving gap is at most a few hours wide; step past it a minute at a time.
    (0..=24 * 60).find_map(|minutes| {
        zone.from_local_datetime(&local.checked_add_signed(TimeDelta::minutes(minutes))?)
            .earliest()
            .map(|instant| instant.with_timezone(&Utc))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono_tz::{America::New_York, Europe::London};

    fn utc(y: i32, m: u32, d: u32, h: u32, min: u32, s: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(y, m, d, h, min, s).unwrap()
    }

    fn at(h: u32, m: u32) -> NaiveTime {
        NaiveTime::from_hms_opt(h, m, 0).unwrap()
    }

    fn new_york_close() -> DailyClose {
        DailyClose::new(at(16, 0), New_York)
    }

    #[test]
    fn the_close_is_the_same_days_before_it_and_the_next_weekdays_from_it_on() {
        let close = new_york_close();
        // Tuesday 2026-11-10, 16:00 EST = 21:00 UTC.
        let tuesday = utc(2026, 11, 10, 21, 0, 0);

        assert_eq!(
            close.next_close_after(utc(2026, 11, 10, 14, 0, 0)),
            Ok(tuesday)
        );
        assert_eq!(
            close.next_close_after(tuesday - TimeDelta::nanoseconds(1)),
            Ok(tuesday)
        );
        // Strictly after: at exactly the close, the next day's.
        assert_eq!(
            close.next_close_after(tuesday),
            Ok(utc(2026, 11, 11, 21, 0, 0))
        );
        // Before the open counts as the same day.
        assert_eq!(
            close.next_close_after(utc(2026, 11, 10, 9, 0, 0)),
            Ok(tuesday)
        );
    }

    #[test]
    fn a_weekend_answers_mondays_close() {
        let close = new_york_close();
        let monday = Ok(utc(2026, 11, 16, 21, 0, 0));

        // Friday after the close, Saturday, Sunday.
        assert_eq!(close.next_close_after(utc(2026, 11, 13, 21, 0, 0)), monday);
        assert_eq!(close.next_close_after(utc(2026, 11, 14, 12, 0, 0)), monday);
        assert_eq!(
            close.next_close_after(utc(2026, 11, 15, 23, 59, 59)),
            monday
        );
    }

    #[test]
    fn the_weekday_is_the_local_one_not_utcs() {
        let close = new_york_close();
        // Saturday 01:00 UTC is still Friday 20:00 in New York, after Friday's close: the next
        // close is Monday's, not Saturday's (UTC) nor a skipped Friday.
        assert_eq!(
            close.next_close_after(utc(2026, 11, 14, 1, 0, 0)),
            Ok(utc(2026, 11, 16, 21, 0, 0))
        );
        // Friday 23:00 UTC in Tokyo is already Saturday 08:00 there: the next close is Monday.
        let tokyo = DailyClose::new(at(15, 0), chrono_tz::Asia::Tokyo);
        assert_eq!(
            tokyo.next_close_after(utc(2026, 11, 13, 23, 0, 0)),
            Ok(utc(2026, 11, 16, 6, 0, 0))
        );
    }

    #[test]
    fn the_close_moves_with_daylight_saving() {
        let close = new_york_close();
        // US DST ends Sunday 2026-11-01: Friday 2026-10-30 closes at 20:00 UTC (EDT), Monday
        // 2026-11-02 at 21:00 UTC (EST).
        assert_eq!(
            close.next_close_after(utc(2026, 10, 30, 12, 0, 0)),
            Ok(utc(2026, 10, 30, 20, 0, 0))
        );
        assert_eq!(
            close.next_close_after(utc(2026, 10, 30, 20, 0, 0)),
            Ok(utc(2026, 11, 2, 21, 0, 0))
        );
        // US DST starts Sunday 2027-03-14: Friday 21:00 UTC, Monday 20:00 UTC.
        assert_eq!(
            close.next_close_after(utc(2027, 3, 12, 12, 0, 0)),
            Ok(utc(2027, 3, 12, 21, 0, 0))
        );
        assert_eq!(
            close.next_close_after(utc(2027, 3, 12, 21, 0, 0)),
            Ok(utc(2027, 3, 15, 20, 0, 0))
        );
    }

    #[test]
    fn a_close_in_a_gap_is_taken_after_it_and_one_in_an_overlap_the_first_time() {
        // London springs forward on Sunday 2027-03-28 at 01:00 GMT to 02:00 BST, so 01:30 does
        // not exist that day; it falls back on Sunday 2026-10-25 at 02:00 BST to 01:00 GMT, so
        // 01:30 happens twice. A weekday rule never lands on those Sundays, so the helper is
        // checked directly.
        let gap = NaiveDate::from_ymd_opt(2027, 3, 28).unwrap();
        assert_eq!(
            instant_at_local(&London, gap, at(1, 30)),
            Some(utc(2027, 3, 28, 1, 0, 0)),
            "02:00 BST, the first local time after the gap"
        );
        let overlap = NaiveDate::from_ymd_opt(2026, 10, 25).unwrap();
        assert_eq!(
            instant_at_local(&London, overlap, at(1, 30)),
            Some(utc(2026, 10, 25, 0, 30, 0)),
            "01:30 BST, the first of the two"
        );
        // An ordinary local time is placed exactly.
        assert_eq!(
            instant_at_local(&London, overlap, at(16, 30)),
            Some(utc(2026, 10, 25, 16, 30, 0))
        );
    }

    #[test]
    fn every_answer_is_the_first_weekday_close_after_the_instant() {
        let close = new_york_close();
        let mut after = utc(2026, 10, 25, 0, 0, 0);
        let end = utc(2027, 3, 31, 0, 0, 0);
        while after < end {
            let answer = close.next_close_after(after).unwrap();
            let local = answer.with_timezone(&New_York);
            assert!(answer > after, "{after} -> {answer}");
            assert_eq!(local.time(), at(16, 0), "{after} -> {answer}");
            assert!(
                !matches!(local.weekday(), Weekday::Sat | Weekday::Sun),
                "{after} -> {answer}"
            );
            // No weekday close lies strictly between: the answer's own local date's previous
            // weekday close is not after `after`.
            let mut previous = local.date_naive().pred_opt().unwrap();
            while matches!(previous.weekday(), Weekday::Sat | Weekday::Sun) {
                previous = previous.pred_opt().unwrap();
            }
            let previous_close = instant_at_local(&New_York, previous, at(16, 0)).unwrap();
            assert!(
                previous_close <= after,
                "{after} -> {answer} skips {previous_close}"
            );
            after += TimeDelta::minutes(97);
        }
    }

    #[test]
    fn the_last_representable_date_is_out_of_range_not_a_panic() {
        let close = DailyClose::new(at(16, 0), chrono_tz::UTC);
        assert_eq!(
            close.next_close_after(DateTime::<Utc>::MAX_UTC),
            Err(CalendarError::out_of_range(DateTime::<Utc>::MAX_UTC))
        );
    }

    #[test]
    fn a_daily_close_round_trips_through_json_and_orders_by_time_then_zone() {
        let close = new_york_close();
        let json = serde_json::to_string(&close).unwrap();
        assert_eq!(json, r#"{"close":"16:00:00","zone":"America/New_York"}"#);
        assert_eq!(serde_json::from_str::<DailyClose>(&json).unwrap(), close);

        let london = DailyClose::new(at(16, 0), London);
        assert!(london > close, "Europe/London sorts after America/New_York");
        assert!(DailyClose::new(at(15, 0), London) < close);
        assert_eq!(close.cmp(&close), Ordering::Equal);
    }
}
