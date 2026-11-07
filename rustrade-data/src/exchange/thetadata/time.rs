//! ThetaData's New York wall-clock times, converted to UTC.
//!
//! Every ThetaData timestamp is the local time in New York with no offset
//! (`2024-11-04T09:30:00`, `2024-11-04T17:16:56.205`). Converting one to an instant needs the zone's
//! rules on that date, which move with daylight saving.

use chrono::{DateTime, LocalResult, NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Utc};
use chrono_tz::America::New_York;

/// The UTC instant of a New York wall-clock time.
///
/// # Daylight-saving transitions
/// - **The repeated hour** (the first Sunday of November, 01:00 to 02:00 happens twice): a wall time
///   in it names two instants an hour apart, and nothing in the row says which. The **earlier**
///   one (still on daylight time) is returned. US exchanges are closed then, so only an
///   extended-hours request can meet it.
/// - **The skipped hour** (the second Sunday of March, 02:00 to 03:00 does not exist): no instant
///   has that wall time, so the time is refused (`None`) rather than shifted.
pub fn new_york_to_utc(local: NaiveDateTime) -> Option<DateTime<Utc>> {
    match New_York.from_local_datetime(&local) {
        LocalResult::Single(time) => Some(time.with_timezone(&Utc)),
        LocalResult::Ambiguous(earlier, _later) => Some(earlier.with_timezone(&Utc)),
        LocalResult::None => None,
    }
}

/// Parse a ThetaData timestamp (`YYYY-MM-DDTHH:MM:SS` with an optional fraction) as New York time
/// and convert it to UTC.
pub(crate) fn parse_timestamp(text: &str) -> Result<DateTime<Utc>, String> {
    let local = NaiveDateTime::parse_from_str(text, "%Y-%m-%dT%H:%M:%S%.f")
        .map_err(|e| format!("timestamp {text:?}: {e}"))?;
    new_york_to_utc(local).ok_or_else(|| {
        format!("timestamp {text:?} falls in the hour New York skips for daylight saving")
    })
}

/// Parse a ThetaData date, `YYYY-MM-DD` or `YYYYMMDD` (both appear in responses).
pub(crate) fn parse_date(text: &str) -> Result<NaiveDate, String> {
    let text = text.trim();
    NaiveDate::parse_from_str(text, "%Y-%m-%d")
        .or_else(|_| NaiveDate::parse_from_str(text, "%Y%m%d"))
        .map_err(|e| format!("date {text:?}: {e}"))
}

/// Parse a ThetaData time of day, `HH:MM:SS` with an optional fraction.
pub(crate) fn parse_time(text: &str) -> Result<NaiveTime, String> {
    NaiveTime::parse_from_str(text, "%H:%M:%S%.f").map_err(|e| format!("time {text:?}: {e}"))
}

/// The date as a request parameter.
pub(crate) fn format_date(date: NaiveDate) -> String {
    date.format("%Y%m%d").to_string()
}

/// The time of day as a request parameter, to the millisecond.
pub(crate) fn format_time(time: NaiveTime) -> String {
    time.format("%H:%M:%S%.3f").to_string()
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn utc(y: i32, m: u32, d: u32, h: u32, min: u32, s: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(y, m, d, h, min, s).unwrap()
    }

    #[test]
    fn standard_and_daylight_time_use_their_own_offsets() {
        // EST is UTC-5, EDT is UTC-4.
        assert_eq!(
            parse_timestamp("2024-01-02T09:30:00").unwrap(),
            utc(2024, 1, 2, 14, 30, 0)
        );
        assert_eq!(
            parse_timestamp("2024-07-01T09:30:00").unwrap(),
            utc(2024, 7, 1, 13, 30, 0)
        );
    }

    #[test]
    fn fractions_of_any_length_parse() {
        let t = parse_timestamp("2024-11-04T17:16:56.205").unwrap();
        assert_eq!(t.timestamp_subsec_millis(), 205);
        let t = parse_timestamp("2024-01-04T17:17:06.02").unwrap();
        assert_eq!(t.timestamp_subsec_millis(), 20);
    }

    #[test]
    fn the_repeated_fall_back_hour_takes_the_earlier_instant() {
        // 2024-11-03 01:30 New York happens at 05:30 UTC (EDT) and again at 06:30 UTC (EST).
        assert_eq!(
            parse_timestamp("2024-11-03T01:30:00").unwrap(),
            utc(2024, 11, 3, 5, 30, 0)
        );
        // Either side of the repeated hour is unambiguous.
        assert_eq!(
            parse_timestamp("2024-11-03T00:59:59").unwrap(),
            utc(2024, 11, 3, 4, 59, 59)
        );
        assert_eq!(
            parse_timestamp("2024-11-03T02:00:00").unwrap(),
            utc(2024, 11, 3, 7, 0, 0)
        );
    }

    #[test]
    fn the_skipped_spring_forward_hour_is_refused() {
        // 2024-03-10 02:30 New York does not exist.
        let error = parse_timestamp("2024-03-10T02:30:00").unwrap_err();
        assert!(error.contains("skips"), "{error}");
        assert_eq!(
            parse_timestamp("2024-03-10T03:00:00").unwrap(),
            utc(2024, 3, 10, 7, 0, 0)
        );
    }

    #[test]
    fn malformed_timestamps_are_refused() {
        assert!(parse_timestamp("2024-11-04 09:30:00").is_err());
        assert!(parse_timestamp("").is_err());
    }

    #[test]
    fn dates_parse_in_both_spellings() {
        let date = NaiveDate::from_ymd_opt(2025, 11, 8).unwrap();
        assert_eq!(parse_date("2025-11-08").unwrap(), date);
        assert_eq!(parse_date("20251108").unwrap(), date);
        assert_eq!(parse_date(" 2025-11-08").unwrap(), date);
        assert!(parse_date("2025/11/08").is_err());
        assert_eq!(format_date(date), "20251108");
    }

    #[test]
    fn times_of_day_round_trip() {
        let time = parse_time("13:00:00").unwrap();
        assert_eq!(time, NaiveTime::from_hms_opt(13, 0, 0).unwrap());
        assert_eq!(format_time(time), "13:00:00.000");
        assert!(parse_time("1pm").is_err());
    }
}
