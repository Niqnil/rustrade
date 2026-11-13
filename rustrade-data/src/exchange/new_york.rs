//! New York wall-clock times, for the providers that report US market times without an offset.

use chrono::{DateTime, LocalResult, NaiveDateTime, TimeZone, Utc};
use chrono_tz::America::New_York;

/// The UTC instant of a New York wall-clock time.
///
/// - **The repeated hour** (the first Sunday of November, 01:00 to 02:00 happens twice): the
///   **earlier** instant, still on daylight time.
/// - **The skipped hour** (the second Sunday of March, 02:00 to 03:00 does not exist): `None`,
///   rather than a shifted time.
#[inline]
pub(crate) fn to_utc(local: NaiveDateTime) -> Option<DateTime<Utc>> {
    match New_York.from_local_datetime(&local) {
        LocalResult::Single(time) => Some(time.with_timezone(&Utc)),
        LocalResult::Ambiguous(earlier, _later) => Some(earlier.with_timezone(&Utc)),
        LocalResult::None => None,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use chrono::NaiveDate;

    fn local(y: i32, m: u32, d: u32, h: u32, min: u32) -> NaiveDateTime {
        NaiveDate::from_ymd_opt(y, m, d)
            .unwrap()
            .and_hms_opt(h, min, 0)
            .unwrap()
    }

    fn utc(y: i32, m: u32, d: u32, h: u32, min: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(y, m, d, h, min, 0).unwrap()
    }

    #[test]
    fn winter_and_summer_times_take_their_own_offsets() {
        assert_eq!(
            to_utc(local(2026, 1, 15, 16, 30)),
            Some(utc(2026, 1, 15, 21, 30))
        );
        assert_eq!(
            to_utc(local(2026, 7, 15, 16, 30)),
            Some(utc(2026, 7, 15, 20, 30))
        );
    }

    #[test]
    fn the_repeated_hour_is_the_earlier_instant() {
        // 2026-11-01 01:30 happens on daylight time (UTC-4), then on standard time (UTC-5).
        assert_eq!(
            to_utc(local(2026, 11, 1, 1, 30)),
            Some(utc(2026, 11, 1, 5, 30))
        );
    }

    #[test]
    fn the_skipped_hour_has_no_instant() {
        assert_eq!(to_utc(local(2026, 3, 8, 2, 30)), None);
        assert_eq!(to_utc(local(2026, 3, 8, 3, 0)), Some(utc(2026, 3, 8, 7, 0)));
    }
}
