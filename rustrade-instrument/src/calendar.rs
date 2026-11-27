//! [`SessionCalendar`] answers when trading sessions close: the first close after an instant,
//! which is what a day order's lifetime needs, and the close of the session on an instant's own
//! date, which is what a rule set before the close of a given day needs. [`DailyClose`] answers
//! both from a fixed rule: one close time in a time zone, every weekday, except the market
//! holidays and early closes listed in its [`SessionDates`].

use chrono::{DateTime, Datelike, NaiveDate, NaiveTime, Offset, TimeDelta, TimeZone, Utc, Weekday};
use serde::{Deserialize, Serialize};
use std::{cmp::Ordering, collections::BTreeSet, fmt::Debug};
use thiserror::Error;

/// The IANA time zone a [`DailyClose`] is in, re-exported so a caller needs no `chrono-tz`
/// dependency of its own: `"America/New_York".parse::<Tz>()`.
pub use chrono_tz::Tz;

/// When a market's trading sessions close.
///
/// Instants go in and come out in UTC. The trading days and the close times are the
/// implementation's own business, so a fixed daily rule ([`DailyClose`]) and an exchange calendar
/// with holidays and early closes answer through the same methods. The time zone is given by
/// [`zone`](Self::zone), for a rule set by local date or time of day, such as one that holds only
/// on an option's expiry day.
///
/// # Contract
/// [`next_close_after`](Self::next_close_after) answers a close **strictly after** `after`: at
/// exactly a close, it answers the next session's. A caller relies on this to step from one
/// close to the next, and may treat an answer not after `after` as a broken calendar.
///
/// [`closes_early_on`](Self::closes_early_on) answers `true` only for a date that has a session:
/// one [`session_close_on`](Self::session_close_on) answers a close for.
pub trait SessionCalendar: Debug + Send + Sync {
    /// The first session close strictly after `after`.
    ///
    /// # Errors
    /// A [`CalendarError`] if the calendar cannot say: for example, `after` is past the last
    /// session it knows of.
    fn next_close_after(&self, after: DateTime<Utc>) -> Result<DateTime<Utc>, CalendarError>;

    /// The close of the session on the date `instant` falls on, in the calendar's own time zone,
    /// or `None` if that date has no session (a weekend or a holiday).
    ///
    /// The close may be before or after `instant`: this answers which day `instant` belongs to,
    /// not what comes next. A calendar whose sessions run past midnight answers the session that
    /// closes on that date.
    ///
    /// # Errors
    /// A [`CalendarError`] if the calendar cannot say: for example, the date is outside the
    /// sessions it knows of.
    fn session_close_on(
        &self,
        instant: DateTime<Utc>,
    ) -> Result<Option<DateTime<Utc>>, CalendarError>;

    /// Whether the session on the date `instant` falls on, in the calendar's own time zone, closes
    /// early: `false` for a regular session and for a date with no session.
    ///
    /// For a rule set at a time of day that a venue moves on its half days, such as a deadline
    /// that is later on a regular day than on an early-close day.
    ///
    /// # Errors
    /// A [`CalendarError`] if the calendar cannot say, as for
    /// [`session_close_on`](Self::session_close_on).
    fn closes_early_on(&self, instant: DateTime<Utc>) -> Result<bool, CalendarError>;

    /// The time zone the calendar dates its sessions in: the zone whose local date
    /// [`session_close_on`](Self::session_close_on) reads an instant on.
    fn zone(&self) -> Tz;
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

/// A market's holidays and early-close days, by local date.
///
/// One list serves every class of instrument trading on the market: options that close at 16:00
/// and options that close at 16:15 keep the same holidays and close early on the same days, each
/// at its own early-close time. So the dates are kept apart from the close times, and one
/// `SessionDates` is given to the [`DailyClose`] of each class
/// ([`DailyClose::with_dates`]).
///
/// The dates are the user's to supply: none are built in, since an exchange sets its holidays a
/// year or so ahead and may add one at short notice.
///
/// ```
/// use chrono::NaiveDate;
/// use rustrade_instrument::calendar::SessionDates;
///
/// let date = |m, d| NaiveDate::from_ymd_opt(2026, m, d).unwrap();
/// // Thanksgiving, then the half day after it and Christmas Eve.
/// let dates = SessionDates::new([date(11, 26)], [date(11, 27), date(12, 24)]).unwrap();
/// assert!(dates.holidays().contains(&date(11, 26)));
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize)]
#[serde(try_from = "SessionDatesRepr", into = "SessionDatesRepr")]
pub struct SessionDates {
    holidays: BTreeSet<NaiveDate>,
    early_closes: BTreeSet<NaiveDate>,
}

impl SessionDates {
    /// These holidays and early-close days.
    ///
    /// # Errors
    /// - [`SessionDatesError::Weekend`] for a date on a Saturday or a Sunday, which has no session
    ///   to close or close early.
    /// - [`SessionDatesError::HolidayAndEarlyClose`] for a date in both lists.
    pub fn new(
        holidays: impl IntoIterator<Item = NaiveDate>,
        early_closes: impl IntoIterator<Item = NaiveDate>,
    ) -> Result<Self, SessionDatesError> {
        let holidays = holidays.into_iter().collect::<BTreeSet<_>>();
        let early_closes = early_closes.into_iter().collect::<BTreeSet<_>>();
        if let Some(&date) = holidays
            .iter()
            .chain(&early_closes)
            .find(|date| is_weekend(**date))
        {
            return Err(SessionDatesError::Weekend { date });
        }
        if let Some(&date) = holidays.intersection(&early_closes).next() {
            return Err(SessionDatesError::HolidayAndEarlyClose { date });
        }
        Ok(Self {
            holidays,
            early_closes,
        })
    }

    /// The days the market does not open.
    pub fn holidays(&self) -> &BTreeSet<NaiveDate> {
        &self.holidays
    }

    /// The days the market closes early.
    pub fn early_closes(&self) -> &BTreeSet<NaiveDate> {
        &self.early_closes
    }
}

/// [`SessionDates`]' serialised form, validated on the way in.
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct SessionDatesRepr {
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    holidays: BTreeSet<NaiveDate>,
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    early_closes: BTreeSet<NaiveDate>,
}

impl TryFrom<SessionDatesRepr> for SessionDates {
    type Error = SessionDatesError;

    fn try_from(repr: SessionDatesRepr) -> Result<Self, Self::Error> {
        Self::new(repr.holidays, repr.early_closes)
    }
}

impl From<SessionDates> for SessionDatesRepr {
    fn from(dates: SessionDates) -> Self {
        let SessionDates {
            holidays,
            early_closes,
        } = dates;
        Self {
            holidays,
            early_closes,
        }
    }
}

/// Why a [`SessionDates`] was refused.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum SessionDatesError {
    /// `date` is a Saturday or a Sunday.
    #[error("{date} is a weekend day, which has no session")]
    Weekend {
        /// The date.
        date: NaiveDate,
    },
    /// `date` is listed as both a holiday and an early close.
    #[error("{date} is listed as both a holiday and an early close")]
    HolidayAndEarlyClose {
        /// The date.
        date: NaiveDate,
    },
}

/// A session close at one local time of day, every weekday by the date in `zone`, except the
/// days its [`SessionDates`] list: no session on a holiday, and an early close on an early-close
/// day.
///
/// A fixed rule, not an exchange's own calendar: the holidays and early closes are what the user
/// lists ([`with_dates`](Self::with_dates)), and with none listed it answers a close on every
/// weekday, a market holiday included, and the regular close on a half day.
///
/// `zone` is an IANA zone, so the close moves with daylight saving: 16:00 in `America/New_York`
/// is 20:00 UTC in summer and 21:00 UTC in winter. A close falling in a daylight-saving gap or
/// overlap is placed as [`instant_at_local`] places it.
///
/// ```
/// use chrono::{NaiveDate, NaiveTime, TimeZone, Utc};
/// use rustrade_instrument::calendar::{DailyClose, SessionCalendar, SessionDates};
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
///
/// // Thanksgiving is a holiday and the day after it closes at 13:00.
/// let date = |d| NaiveDate::from_ymd_opt(2026, 11, d).unwrap();
/// let close = close
///     .with_dates(
///         SessionDates::new([date(26)], [date(27)]).unwrap(),
///         NaiveTime::from_hms_opt(13, 0, 0),
///     )
///     .unwrap();
/// // After Wednesday's close, the next close is Friday's early one, 13:00 EST.
/// let wednesday = Utc.with_ymd_and_hms(2026, 11, 25, 22, 0, 0).unwrap();
/// assert_eq!(
///     close.next_close_after(wednesday),
///     Ok(Utc.with_ymd_and_hms(2026, 11, 27, 18, 0, 0).unwrap())
/// );
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "DailyCloseRepr", into = "DailyCloseRepr")]
pub struct DailyClose {
    close: NaiveTime,
    zone: Tz,
    early_close: Option<NaiveTime>,
    dates: SessionDates,
}

impl DailyClose {
    /// A close at local time `close` in `zone`, every weekday, with no holidays and no early
    /// closes.
    pub fn new(close: NaiveTime, zone: Tz) -> Self {
        Self {
            close,
            zone,
            early_close: None,
            dates: SessionDates::default(),
        }
    }

    /// This rule, with no session on the holidays of `dates` and a close at local time
    /// `early_close` on its early-close days, replacing any dates and early close it had.
    ///
    /// `early_close` may be `None` only if `dates` lists no early-close day. One set with no
    /// early-close day to use it is kept, so a class keeps its early-close time in a year with no
    /// half day listed; such a rule answers as one without it but is not equal to it.
    ///
    /// # Errors
    /// - [`DailyCloseError::EarlyCloseMissing`] if `dates` lists an early-close day and
    ///   `early_close` is `None`.
    /// - [`DailyCloseError::EarlyCloseNotBeforeClose`] if `early_close` is not before the
    ///   regular close.
    pub fn with_dates(
        self,
        dates: SessionDates,
        early_close: Option<NaiveTime>,
    ) -> Result<Self, DailyCloseError> {
        match early_close {
            None if !dates.early_closes.is_empty() => Err(DailyCloseError::EarlyCloseMissing),
            Some(early_close) if early_close >= self.close => {
                Err(DailyCloseError::EarlyCloseNotBeforeClose {
                    early_close,
                    close: self.close,
                })
            }
            _ => Ok(Self {
                early_close,
                dates,
                ..self
            }),
        }
    }

    /// The local time of day of the regular close.
    pub fn close(&self) -> NaiveTime {
        self.close
    }

    /// The zone the close is in.
    pub fn zone(&self) -> Tz {
        self.zone
    }

    /// The local time of day of the close on an early-close day, if one is set.
    pub fn early_close(&self) -> Option<NaiveTime> {
        self.early_close
    }

    /// The holidays and early-close days.
    pub fn dates(&self) -> &SessionDates {
        &self.dates
    }

    /// The local time of day of the session's close on local `date`, or `None` if `date` has no
    /// session.
    fn close_time_on(&self, date: NaiveDate) -> Option<NaiveTime> {
        if is_weekend(date) || self.dates.holidays.contains(&date) {
            None
        } else if self.dates.early_closes.contains(&date) {
            match self.early_close {
                Some(early_close) => Some(early_close),
                None => unreachable!("with_dates refuses early-close days without an early close"),
            }
        } else {
            Some(self.close)
        }
    }
}

/// Ordered by close time, then by zone name, then by early close and dates: [`Tz`] has no order
/// of its own.
impl Ord for DailyClose {
    fn cmp(&self, other: &Self) -> Ordering {
        self.close
            .cmp(&other.close)
            .then_with(|| self.zone.name().cmp(other.zone.name()))
            .then_with(|| self.early_close.cmp(&other.early_close))
            .then_with(|| self.dates.cmp(&other.dates))
    }
}

impl PartialOrd for DailyClose {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl SessionCalendar for DailyClose {
    fn next_close_after(&self, after: DateTime<Utc>) -> Result<DateTime<Utc>, CalendarError> {
        // A session lies within `holidays + 1` weeks of `after`'s local date: past that date, whose
        // close may already have passed, those weeks hold at least `5 * weeks - 1` weekdays, more
        // than the holidays can take.
        let dates = 7 * (self.dates.holidays.len() + 1);
        let mut date = local_date(&self.zone, after).ok_or(CalendarError::out_of_range(after))?;
        for _ in 0..dates {
            if let Some(time) = self.close_time_on(date)
                && let Some(close) = instant_at_local(&self.zone, date, time)
                && close > after
            {
                return Ok(close);
            }
            date = date.succ_opt().ok_or(CalendarError::out_of_range(after))?;
        }
        Err(CalendarError::out_of_range(after))
    }

    fn session_close_on(
        &self,
        instant: DateTime<Utc>,
    ) -> Result<Option<DateTime<Utc>>, CalendarError> {
        let date = local_date(&self.zone, instant).ok_or(CalendarError::out_of_range(instant))?;
        self.close_time_on(date)
            .map(|time| {
                instant_at_local(&self.zone, date, time).ok_or(CalendarError::out_of_range(instant))
            })
            .transpose()
    }

    fn closes_early_on(&self, instant: DateTime<Utc>) -> Result<bool, CalendarError> {
        let date = local_date(&self.zone, instant).ok_or(CalendarError::out_of_range(instant))?;
        // `SessionDates` refuses a weekend or holiday early close, so a listed date has a session.
        Ok(self.dates.early_closes.contains(&date))
    }

    fn zone(&self) -> Tz {
        self.zone
    }
}

/// [`DailyClose`]'s serialised form, validated on the way in.
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct DailyCloseRepr {
    close: NaiveTime,
    zone: Tz,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    early_close: Option<NaiveTime>,
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    holidays: BTreeSet<NaiveDate>,
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    early_closes: BTreeSet<NaiveDate>,
}

impl TryFrom<DailyCloseRepr> for DailyClose {
    type Error = DailyCloseError;

    fn try_from(repr: DailyCloseRepr) -> Result<Self, Self::Error> {
        let DailyCloseRepr {
            close,
            zone,
            early_close,
            holidays,
            early_closes,
        } = repr;
        let dates = SessionDates::new(holidays, early_closes)?;
        Self::new(close, zone).with_dates(dates, early_close)
    }
}

impl From<DailyClose> for DailyCloseRepr {
    fn from(close: DailyClose) -> Self {
        let DailyClose {
            close,
            zone,
            early_close,
            dates:
                SessionDates {
                    holidays,
                    early_closes,
                },
        } = close;
        Self {
            close,
            zone,
            early_close,
            holidays,
            early_closes,
        }
    }
}

/// Why a [`DailyClose`] was refused.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum DailyCloseError {
    /// Early-close days are listed, but no early-close time.
    #[error("early-close days are listed, but no early-close time")]
    EarlyCloseMissing,
    /// The early close is not before the regular close.
    #[error("the early close {early_close} is not before the regular close {close}")]
    EarlyCloseNotBeforeClose {
        /// The early-close time.
        early_close: NaiveTime,
        /// The regular close time.
        close: NaiveTime,
    },
    /// The dates were refused.
    #[error(transparent)]
    Dates(#[from] SessionDatesError),
}

/// The local date of `instant` in `zone`, or `None` if it is past the last representable date.
fn local_date(zone: &Tz, instant: DateTime<Utc>) -> Option<NaiveDate> {
    let offset = zone.offset_from_utc_datetime(&instant.naive_utc()).fix();
    instant
        .naive_utc()
        .checked_add_offset(offset)
        .map(|local| local.date())
}

/// Whether `date` is a Saturday or a Sunday.
fn is_weekend(date: NaiveDate) -> bool {
    matches!(date.weekday(), Weekday::Sat | Weekday::Sun)
}

/// The instant at local `time` on local `date` in `zone`.
///
/// A local time that falls in a gap (a daylight-saving change, or a zone skipping a day) does
/// not exist, so it is taken at the first local time after the gap, to the minute. One that falls
/// in an overlap exists twice, so it is taken the first time. `None` only if the instant is out
/// of range.
pub fn instant_at_local<Z: TimeZone>(
    zone: &Z,
    date: NaiveDate,
    time: NaiveTime,
) -> Option<DateTime<Utc>> {
    let local = date.and_time(time);

    // A gap is at most a day wide (a daylight-saving change is an hour or so; a zone that moved
    // across the date line skipped a whole day); step past it a minute at a time.
    (0..=24 * 60).find_map(|minutes| {
        zone.from_local_datetime(&local.checked_add_signed(TimeDelta::minutes(minutes))?)
            .earliest()
            .map(|instant| instant.with_timezone(&Utc))
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)] // Test code: panics on bad input are acceptable
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
        // Samoa moved across the date line by skipping Friday 2011-12-30 whole: from midnight
        // that day, the first local time that exists is a full day later, 00:00 +14:00 on the
        // 31st.
        let skipped = NaiveDate::from_ymd_opt(2011, 12, 30).unwrap();
        assert_eq!(
            instant_at_local(&chrono_tz::Pacific::Apia, skipped, at(0, 0)),
            Some(utc(2011, 12, 30, 10, 0, 0)),
            "a gap a whole day wide is still stepped past"
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
        assert_eq!((london.close(), london.zone()), (at(16, 0), London));
    }

    fn date(y: i32, m: u32, d: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(y, m, d).unwrap()
    }

    /// New York at 16:00, with Thanksgiving 2026 (Thursday 11-26) a holiday and the day after it
    /// closing at 13:00.
    fn thanksgiving_close() -> DailyClose {
        new_york_close()
            .with_dates(
                SessionDates::new([date(2026, 11, 26)], [date(2026, 11, 27)]).unwrap(),
                Some(at(13, 0)),
            )
            .unwrap()
    }

    #[test]
    fn a_holiday_is_skipped_and_an_early_close_day_closes_early() {
        let close = thanksgiving_close();

        // After Wednesday's close: no session Thursday, Friday closes at 13:00 EST = 18:00 UTC.
        assert_eq!(
            close.next_close_after(utc(2026, 11, 25, 21, 0, 0)),
            Ok(utc(2026, 11, 27, 18, 0, 0))
        );
        // During the holiday: the same.
        assert_eq!(
            close.next_close_after(utc(2026, 11, 26, 15, 0, 0)),
            Ok(utc(2026, 11, 27, 18, 0, 0))
        );
        // Friday after the early close but before the regular one: Monday's regular close.
        assert_eq!(
            close.next_close_after(utc(2026, 11, 27, 19, 0, 0)),
            Ok(utc(2026, 11, 30, 21, 0, 0))
        );
        // Days the dates do not list keep the regular close.
        assert_eq!(
            close.next_close_after(utc(2026, 11, 24, 12, 0, 0)),
            Ok(utc(2026, 11, 24, 21, 0, 0))
        );
    }

    #[test]
    fn a_closure_longer_than_a_week_still_answers() {
        // Every weekday for three weeks from Monday 2026-11-02 is a holiday.
        let holidays = (0..21)
            .map(|days| date(2026, 11, 2) + TimeDelta::days(days))
            .filter(|date| !matches!(date.weekday(), Weekday::Sat | Weekday::Sun));
        let close = new_york_close()
            .with_dates(SessionDates::new(holidays, []).unwrap(), None)
            .unwrap();

        // From the Friday before: the Monday after the closure.
        assert_eq!(
            close.next_close_after(utc(2026, 10, 30, 21, 0, 0)),
            Ok(utc(2026, 11, 23, 21, 0, 0))
        );
    }

    #[test]
    fn only_a_listed_early_close_day_closes_early_by_its_local_date() {
        let close = thanksgiving_close();

        // The day after Thanksgiving, in its morning and after its close.
        assert_eq!(close.closes_early_on(utc(2026, 11, 27, 14, 0, 0)), Ok(true));
        assert_eq!(close.closes_early_on(utc(2026, 11, 27, 23, 0, 0)), Ok(true));
        // A regular day, the holiday, a weekend: none closes early.
        for instant in [
            utc(2026, 11, 24, 14, 0, 0),
            utc(2026, 11, 26, 15, 0, 0),
            utc(2026, 11, 28, 15, 0, 0),
        ] {
            assert_eq!(close.closes_early_on(instant), Ok(false));
        }
        // The date is New York's: Saturday 03:00 UTC is still the early-close Friday there, and
        // Friday 03:00 UTC is still Thanksgiving.
        assert_eq!(close.closes_early_on(utc(2026, 11, 28, 3, 0, 0)), Ok(true));
        assert_eq!(close.closes_early_on(utc(2026, 11, 27, 3, 0, 0)), Ok(false));
        // Out of range is an error, not a panic.
        let ahead = DailyClose::new(at(16, 0), chrono_tz::Etc::GMTMinus14);
        let last = DateTime::<Utc>::MAX_UTC;
        assert_eq!(
            ahead.closes_early_on(last),
            Err(CalendarError::out_of_range(last))
        );
    }

    #[test]
    fn the_session_close_on_an_instant_is_its_own_local_dates() {
        let close = thanksgiving_close();

        // Before and after Tuesday's close alike: Tuesday's close.
        let tuesday = Ok(Some(utc(2026, 11, 24, 21, 0, 0)));
        assert_eq!(close.session_close_on(utc(2026, 11, 24, 14, 0, 0)), tuesday);
        assert_eq!(close.session_close_on(utc(2026, 11, 24, 23, 0, 0)), tuesday);
        // The holiday has none; the early-close day closes early.
        assert_eq!(
            close.session_close_on(utc(2026, 11, 26, 15, 0, 0)),
            Ok(None)
        );
        assert_eq!(
            close.session_close_on(utc(2026, 11, 27, 23, 0, 0)),
            Ok(Some(utc(2026, 11, 27, 18, 0, 0)))
        );
        // A weekend has none.
        assert_eq!(
            close.session_close_on(utc(2026, 11, 28, 15, 0, 0)),
            Ok(None)
        );
        // The date is New York's: Tuesday 03:00 UTC is still Monday evening there.
        assert_eq!(
            close.session_close_on(utc(2026, 11, 24, 3, 0, 0)),
            Ok(Some(utc(2026, 11, 23, 21, 0, 0)))
        );
        // Out of range is an error, not a panic, even where the local date is past the last one
        // (`Etc/GMT-14` is 14 hours ahead of UTC).
        let ahead = DailyClose::new(at(16, 0), chrono_tz::Etc::GMTMinus14);
        let last = DateTime::<Utc>::MAX_UTC;
        assert_eq!(
            ahead.session_close_on(last),
            Err(CalendarError::out_of_range(last))
        );
        assert_eq!(
            ahead.next_close_after(last),
            Err(CalendarError::out_of_range(last))
        );
    }

    #[test]
    fn session_dates_refuse_a_weekend_and_a_date_in_both_lists() {
        // Saturday 2026-11-28.
        assert_eq!(
            SessionDates::new([date(2026, 11, 28)], []),
            Err(SessionDatesError::Weekend {
                date: date(2026, 11, 28)
            })
        );
        assert_eq!(
            SessionDates::new([], [date(2026, 11, 29)]),
            Err(SessionDatesError::Weekend {
                date: date(2026, 11, 29)
            })
        );
        assert_eq!(
            SessionDates::new([date(2026, 11, 27)], [date(2026, 11, 27)]),
            Err(SessionDatesError::HolidayAndEarlyClose {
                date: date(2026, 11, 27)
            })
        );
        let dates = SessionDates::new([date(2026, 11, 26)], [date(2026, 11, 27)]).unwrap();
        assert_eq!(
            (dates.holidays(), dates.early_closes()),
            (
                &BTreeSet::from([date(2026, 11, 26)]),
                &BTreeSet::from([date(2026, 11, 27)])
            )
        );
    }

    #[test]
    fn an_early_close_needs_a_time_before_the_regular_close() {
        let early = SessionDates::new([], [date(2026, 11, 27)]).unwrap();

        assert_eq!(
            new_york_close().with_dates(early.clone(), None),
            Err(DailyCloseError::EarlyCloseMissing)
        );
        assert_eq!(
            new_york_close().with_dates(early.clone(), Some(at(16, 0))),
            Err(DailyCloseError::EarlyCloseNotBeforeClose {
                early_close: at(16, 0),
                close: at(16, 0)
            })
        );
        // Holidays alone need no early close, and an early close may be set with no day using it.
        let holidays = SessionDates::new([date(2026, 11, 26)], []).unwrap();
        let close = new_york_close().with_dates(holidays.clone(), None).unwrap();
        assert_eq!((close.early_close(), close.dates()), (None, &holidays));
        let close = new_york_close()
            .with_dates(SessionDates::default(), Some(at(13, 0)))
            .unwrap();
        assert_eq!(
            (close.early_close(), close.dates()),
            (Some(at(13, 0)), &SessionDates::default())
        );
    }

    #[test]
    fn a_daily_close_with_dates_round_trips_and_is_validated_on_the_way_in() {
        let close = thanksgiving_close();
        let json = serde_json::to_string(&close).unwrap();
        assert_eq!(
            json,
            r#"{"close":"16:00:00","zone":"America/New_York","early_close":"13:00:00","holidays":["2026-11-26"],"early_closes":["2026-11-27"]}"#
        );
        assert_eq!(serde_json::from_str::<DailyClose>(&json).unwrap(), close);

        let refused = |json: &str| {
            serde_json::from_str::<DailyClose>(json)
                .unwrap_err()
                .to_string()
        };
        assert!(
            refused(r#"{"close":"16:00:00","zone":"UTC","early_closes":["2026-11-27"]}"#)
                .contains("no early-close time")
        );
        assert!(
            refused(r#"{"close":"16:00:00","zone":"UTC","holidays":["2026-11-28"]}"#)
                .contains("weekend")
        );
        assert!(
            refused(r#"{"close":"16:00:00","zone":"UTC","holiday":["2026-11-26"]}"#)
                .contains("unknown field")
        );

        let dates = SessionDates::new([date(2026, 11, 26)], []).unwrap();
        let json = serde_json::to_string(&dates).unwrap();
        assert_eq!(json, r#"{"holidays":["2026-11-26"]}"#);
        assert_eq!(serde_json::from_str::<SessionDates>(&json).unwrap(), dates);
        assert!(
            serde_json::from_str::<SessionDates>(r#"{"early_closes":["2026-11-26","2026-11-28"]}"#)
                .is_err()
        );
    }

    #[test]
    fn daily_closes_differing_only_in_dates_are_ordered_apart() {
        let plain = new_york_close();
        let early = new_york_close()
            .with_dates(SessionDates::default(), Some(at(13, 0)))
            .unwrap();
        let holiday = new_york_close()
            .with_dates(SessionDates::new([date(2026, 11, 26)], []).unwrap(), None)
            .unwrap();

        assert!(plain < early, "no early close sorts first");
        assert!(plain < holiday, "no dates sort first");
        assert_ne!(early.cmp(&holiday), Ordering::Equal);
    }

    #[test]
    fn a_monday_holiday_moves_fridays_next_close_to_tuesday() {
        // Monday 2027-01-18 is a holiday.
        let close = new_york_close()
            .with_dates(SessionDates::new([date(2027, 1, 18)], []).unwrap(), None)
            .unwrap();

        assert_eq!(
            close.next_close_after(utc(2027, 1, 15, 21, 0, 0)),
            Ok(utc(2027, 1, 19, 21, 0, 0))
        );
        assert_eq!(close.session_close_on(utc(2027, 1, 18, 15, 0, 0)), Ok(None));
    }

    #[test]
    fn an_early_close_follows_daylight_saving() {
        // Friday 2026-07-03 closes at 13:00 EDT = 17:00 UTC, an hour earlier in UTC than a winter
        // early close.
        let close = new_york_close()
            .with_dates(
                SessionDates::new([], [date(2026, 7, 3)]).unwrap(),
                Some(at(13, 0)),
            )
            .unwrap();

        assert_eq!(
            close.next_close_after(utc(2026, 7, 2, 21, 0, 0)),
            Ok(utc(2026, 7, 3, 17, 0, 0))
        );
        assert_eq!(
            close.session_close_on(utc(2026, 7, 3, 12, 0, 0)),
            Ok(Some(utc(2026, 7, 3, 17, 0, 0)))
        );
    }

    #[test]
    fn the_calendar_gives_its_zone() {
        assert_eq!(
            SessionCalendar::zone(&new_york_close()),
            chrono_tz::America::New_York
        );
    }
}
