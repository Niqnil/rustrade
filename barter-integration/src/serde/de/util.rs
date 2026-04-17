/// Determine the `DateTime<Utc>` from the provided `Duration` since the epoch.
pub fn datetime_utc_from_epoch_duration(
    duration: std::time::Duration,
) -> chrono::DateTime<chrono::Utc> {
    chrono::DateTime::<chrono::Utc>::from(std::time::UNIX_EPOCH + duration)
}

/// Deserialise a `String` as the desired type.
pub fn de_str<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    D: serde::de::Deserializer<'de>,
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    let data: &str = serde::de::Deserialize::deserialize(deserializer)?;
    data.parse::<T>().map_err(serde::de::Error::custom)
}

/// Deserialise a `u64` milliseconds value as `DateTime<Utc>`.
pub fn de_u64_epoch_ms_as_datetime_utc<'de, D>(
    deserializer: D,
) -> Result<chrono::DateTime<chrono::Utc>, D::Error>
where
    D: serde::de::Deserializer<'de>,
{
    serde::de::Deserialize::deserialize(deserializer).map(|epoch_ms| {
        datetime_utc_from_epoch_duration(std::time::Duration::from_millis(epoch_ms))
    })
}

/// Deserialise a &str "u64" milliseconds value as `DateTime<Utc>`.
pub fn de_str_u64_epoch_ms_as_datetime_utc<'de, D>(
    deserializer: D,
) -> Result<chrono::DateTime<chrono::Utc>, D::Error>
where
    D: serde::de::Deserializer<'de>,
{
    de_str(deserializer).map(|epoch_ms| {
        datetime_utc_from_epoch_duration(std::time::Duration::from_millis(epoch_ms))
    })
}

/// Deserialise a &str "f64" milliseconds value as `DateTime<Utc>`.
// cast_sign_loss: guard below ensures epoch_ms >= 0.0
// cast_possible_truncation: f64 as u64 saturates per Rust 1.45+; sub-ms precision discarded intentionally
#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
pub fn de_str_f64_epoch_ms_as_datetime_utc<'de, D>(
    deserializer: D,
) -> Result<chrono::DateTime<chrono::Utc>, D::Error>
where
    D: serde::de::Deserializer<'de>,
{
    de_str(deserializer).and_then(|epoch_ms: f64| {
        if !epoch_ms.is_finite() || epoch_ms < 0.0 {
            return Err(serde::de::Error::custom(format!(
                "invalid epoch_ms: {epoch_ms}"
            )));
        }
        Ok(datetime_utc_from_epoch_duration(
            std::time::Duration::from_millis(epoch_ms as u64),
        ))
    })
}

/// Deserialise a &str "f64" seconds value as `DateTime<Utc>`.
pub fn de_str_f64_epoch_s_as_datetime_utc<'de, D>(
    deserializer: D,
) -> Result<chrono::DateTime<chrono::Utc>, D::Error>
where
    D: serde::de::Deserializer<'de>,
{
    de_str(deserializer).and_then(|epoch_s: f64| {
        if !epoch_s.is_finite() || epoch_s < 0.0 {
            return Err(serde::de::Error::custom(format!(
                "invalid epoch_s: {epoch_s}"
            )));
        }
        Ok(datetime_utc_from_epoch_duration(
            std::time::Duration::from_secs_f64(epoch_s),
        ))
    })
}

/// Assists deserialisation of sequences by attempting to extract & parse the next element in the
/// provided sequence.
///
/// A [`serde::de::Error`] is returned if the element does not exist, or it cannot
/// be deserialised into the `Target` type inferred.
///
/// Example sequence: ["20180.30000","0.00010000","1661978265.280067","s","l",""]
pub fn extract_next<'de, SeqAccessor, Target>(
    sequence: &mut SeqAccessor,
    name: &'static str,
) -> Result<Target, SeqAccessor::Error>
where
    SeqAccessor: serde::de::SeqAccess<'de>,
    Target: serde::de::DeserializeOwned,
{
    sequence
        .next_element::<Target>()?
        .ok_or_else(|| serde::de::Error::missing_field(name))
}
