//! New York wall-clock times, for the providers that report US market times without an offset.

use chrono::{DateTime, LocalResult, NaiveDateTime, TimeZone, Utc};
use chrono_tz::America::New_York;

/// The UTC instant of a New York wall-clock time.
///
/// - **The repeated hour** (the first Sunday of November, 01:00 to 02:00 happens twice): the
///   **earlier** instant, still on daylight time.
/// - **The skipped hour** (the second Sunday of March, 02:00 to 03:00 does not exist): `None`,
///   rather than a shifted time.
pub(crate) fn to_utc(local: NaiveDateTime) -> Option<DateTime<Utc>> {
    match New_York.from_local_datetime(&local) {
        LocalResult::Single(time) => Some(time.with_timezone(&Utc)),
        LocalResult::Ambiguous(earlier, _later) => Some(earlier.with_timezone(&Utc)),
        LocalResult::None => None,
    }
}
