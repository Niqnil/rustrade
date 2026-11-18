//! A [`Duration`] as a whole number of milliseconds, for `#[serde(with = "…")]` on a config's
//! timeout, so a config file writes `10000` rather than the `{ secs, nanos }` serde gives it by
//! default.

use serde::{Deserialize, Deserializer, Serializer};
use std::time::Duration;

pub(crate) fn serialize<S: Serializer>(
    duration: &Duration,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    // Saturates rather than fails: no timeout needs more than u64::MAX milliseconds.
    serializer.serialize_u64(u64::try_from(duration.as_millis()).unwrap_or(u64::MAX))
}

pub(crate) fn deserialize<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Duration, D::Error> {
    u64::deserialize(deserializer).map(Duration::from_millis)
}
