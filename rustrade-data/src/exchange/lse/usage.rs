//! Where an API key stands against its LSE Data 3.0 plan: [`LseUsage`], from
//! [`LseClient::usage`](super::client::LseClient::usage).
//!
//! # This type reports; it does not decide
//! Nothing in this integration retries, sleeps or throttles on the strength of these numbers. A
//! request past a limit surfaces as a terminal
//! [`LseError::RateLimited`](super::error::LseError::RateLimited) carrying the provider's code,
//! never as a silent stall or a blind retry; pacing against the figures here is the caller's call.

use chrono::NaiveDate;
use serde::{Deserialize, Deserializer, Serialize};
use smol_str::SmolStr;
use std::collections::BTreeMap;

/// A key's plan and what it has consumed, as `/v3/usage` reports them.
///
/// Unknown fields are ignored rather than rejected, and the struct is `#[non_exhaustive]`, so a
/// figure the provider adds stays a non-breaking change. The key's name, which the answer also
/// carries, is deliberately not kept.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct LseUsage {
    /// The plan, as the provider names it: `free`, or a paid plan (`cme:ended` once it ends).
    pub plan: String,
    /// The product the key belongs to (`global` for the provider's own markets).
    pub product: String,
    /// The markets the key may download, by folder name (`us_stocks`, `crypto`, ...).
    pub markets: Vec<SmolStr>,
    /// The download rate the key is paced to, in megabits per second.
    pub rate_mbit: f64,
    /// Downloads the key may hold open at once.
    pub connections: u32,
    /// Downloads the key holds open now.
    pub open: u32,
    /// Bytes sent to the key since the service started.
    pub bytes_since_start: u64,
    /// Calls the key may make per minute.
    pub calls_per_minute: u32,
    /// The plan's row allowance.
    pub rows: LseRowAllowance,
    /// The plan's weekly byte allowance.
    pub weekly: LseWeeklyAllowance,
    /// History windows per paid market, by folder name. Empty on the free plan, whose included
    /// markets have no window.
    #[serde(default)]
    pub history: BTreeMap<SmolStr, LseHistoryWindow>,
    /// Why the key is withheld (its plan ended, or it is beyond the plan's key allowance), or
    /// `None` when it is not. A withheld key still reads the catalogue and this usage.
    #[serde(default)]
    pub withheld: Option<String>,
}

/// The rows a key may read, per day and per export.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct LseRowAllowance {
    /// The UTC day `used_today` counts.
    pub day: NaiveDate,
    /// Rows the key may read per day, or `None` when the plan has no daily cap. Past it the
    /// provider answers `429 rows-exhausted`.
    #[serde(deserialize_with = "deserialize_cap")]
    pub per_day: Option<u64>,
    /// Rows one export may read, or `None` when uncapped. Past it the provider answers
    /// `413 export-too-wide`.
    #[serde(deserialize_with = "deserialize_cap")]
    pub per_export: Option<u64>,
    /// Rows read so far on `day`.
    pub used_today: u64,
    /// Seconds until the daily count resets.
    pub resets_in_seconds: u64,
}

impl LseRowAllowance {
    /// Rows left today, or `None` when the plan has no daily cap. Saturates at zero.
    #[must_use]
    pub fn remaining_today(&self) -> Option<u64> {
        self.per_day.map(|cap| cap.saturating_sub(self.used_today))
    }
}

/// The bytes a key may download per ISO week.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct LseWeeklyAllowance {
    /// The ISO week `used_bytes` counts, as the provider spells it (`2026-W48`).
    pub week: String,
    /// Bytes the key may download per week, or `None` when uncapped. Past it the provider answers
    /// `429 allowance-exhausted`.
    #[serde(deserialize_with = "deserialize_cap")]
    pub allowance_bytes: Option<u64>,
    /// Bytes downloaded so far this week.
    pub used_bytes: u64,
}

impl LseWeeklyAllowance {
    /// Bytes left this week, or `None` when uncapped. Saturates at zero.
    #[must_use]
    pub fn remaining_bytes(&self) -> Option<u64> {
        self.allowance_bytes
            .map(|cap| cap.saturating_sub(self.used_bytes))
    }
}

/// How far back a paid market's history may be downloaded, per data level.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct LseHistoryWindow {
    /// Months of Level 1 history, or `None` for the whole archive.
    #[serde(deserialize_with = "deserialize_cap")]
    pub l1: Option<u64>,
    /// Months of Level 2 history, or `None` for the whole archive.
    #[serde(deserialize_with = "deserialize_cap")]
    pub l2: Option<u64>,
    /// Months of Level 3 history, or `None` for the whole archive.
    #[serde(deserialize_with = "deserialize_cap")]
    pub l3: Option<u64>,
    /// The tier that sets these windows, as the provider names it.
    #[serde(default)]
    pub plan: Option<String>,
}

/// Read a cap the provider writes as `-1` when there is none.
///
/// Any negative figure is read as "no cap" rather than rejected: only `-1` is documented, and a
/// cap is not negative, so a different negative sentinel means the same thing. `null`, which is
/// how this crate serialises `None`, is also no cap, so a serialised value reads back unchanged.
fn deserialize_cap<'de, D>(deserializer: D) -> Result<Option<u64>, D::Error>
where
    D: Deserializer<'de>,
{
    let raw = Option::<i64>::deserialize(deserializer)?;
    Ok(raw.and_then(|raw| u64::try_from(raw).ok()))
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;

    /// The free plan's answer, in the shape measured on `/v3/usage` 2026-11-27 (figures invented).
    const FREE: &str = r#"{
        "key": "my key", "plan": "free", "product": "global",
        "markets": ["us_stocks", "crypto"], "rate_mbit": 50.0, "connections": 1, "open": 0,
        "bytes_since_start": 1024, "calls_per_minute": 60, "levels": {},
        "history": {"cme": {"l1": -1, "l2": 12, "l3": 0, "plan": "lite"}},
        "rows": {"day": "2026-11-27", "per_day": 12500000, "per_export": 2500000,
                 "resets_in_seconds": 3600, "used_today": 500},
        "weekly": {"allowance_bytes": 13421772800, "used_bytes": 100, "week": "2026-W48"},
        "withheld": null
    }"#;

    #[test]
    fn the_measured_shape_decodes() {
        let usage: LseUsage = serde_json::from_str(FREE).unwrap();

        assert_eq!(usage.plan, "free");
        assert_eq!(usage.markets, ["us_stocks", "crypto"]);
        assert_eq!(usage.connections, 1);
        assert_eq!(usage.calls_per_minute, 60);
        assert_eq!(usage.rows.per_day, Some(12_500_000));
        assert_eq!(usage.rows.per_export, Some(2_500_000));
        assert_eq!(usage.rows.remaining_today(), Some(12_499_500));
        assert_eq!(usage.weekly.remaining_bytes(), Some(13_421_772_700));
        assert_eq!(usage.withheld, None);
    }

    #[test]
    fn minus_one_is_no_cap() {
        let json = FREE
            .replace("\"per_day\": 12500000", "\"per_day\": -1")
            .replace(
                "\"allowance_bytes\": 13421772800",
                "\"allowance_bytes\": -1",
            );
        let usage: LseUsage = serde_json::from_str(&json).unwrap();

        assert_eq!(usage.rows.per_day, None);
        assert_eq!(usage.rows.remaining_today(), None);
        assert_eq!(usage.weekly.remaining_bytes(), None);
        let cme = &usage.history["cme"];
        assert_eq!((cme.l1, cme.l2, cme.l3), (None, Some(12), Some(0)));
    }

    #[test]
    fn a_serialised_usage_reads_back_unchanged_uncapped_figures_included() {
        let json = FREE.replace("\"per_day\": 12500000", "\"per_day\": -1");
        let usage: LseUsage = serde_json::from_str(&json).unwrap();
        let round_trip: LseUsage =
            serde_json::from_str(&serde_json::to_string(&usage).unwrap()).unwrap();

        assert_eq!(round_trip, usage);
        assert_eq!(round_trip.rows.per_day, None);
    }

    #[test]
    fn remaining_saturates_at_zero() {
        let json = FREE.replace("\"used_today\": 500", "\"used_today\": 99999999");
        let usage: LseUsage = serde_json::from_str(&json).unwrap();

        assert_eq!(usage.rows.remaining_today(), Some(0));
    }

    #[test]
    fn the_key_name_is_not_kept() {
        let usage: LseUsage = serde_json::from_str(FREE).unwrap();

        assert!(!format!("{usage:?}").contains("my key"));
    }

    #[test]
    fn a_withheld_key_carries_its_reason() {
        let json = FREE.replace("\"withheld\": null", "\"withheld\": \"plan ended\"");
        let usage: LseUsage = serde_json::from_str(&json).unwrap();

        assert_eq!(usage.withheld.as_deref(), Some("plan ended"));
    }
}
