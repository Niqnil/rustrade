//! A [`Duration`] as a whole number of milliseconds, for `#[serde(with = "…")]` on a config's
//! timeout (`deserialize_with` on a config that is never serialised), so a config file writes
//! `10000` rather than the `{ secs, nanos }` serde gives it by default.

#[cfg(feature = "ibkr")]
use serde::Serializer;
use serde::{Deserialize, Deserializer};
use std::time::Duration;

// Only `IbkrConfig` serialises: `AlpacaConfig` derives no `Serialize`, to keep its secret key out
// of any output, so it reads its timeout with `deserialize_with` alone.
#[cfg(feature = "ibkr")]
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
