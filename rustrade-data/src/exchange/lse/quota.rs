use serde::{Deserialize, Serialize};

/// Where an API key currently stands against London Strategic Edge's allowance.
///
/// Unlike every other provider in this crate, London Strategic Edge meters **streaming and bulk
/// export against one shared pool**, so a consumer running both must budget against a single
/// allowance rather than two independent ones.
///
/// # This type reports; it does not decide
///
/// Nothing in this integration retries, sleeps, or throttles on the strength of these numbers. The
/// allowance is surfaced so the caller can pace against it, because pacing policy — retry cadence,
/// wait-and-resume, cross-run budgeting — depends on what the caller is doing and cannot be chosen
/// correctly inside a library. A rejected request surfaces as a terminal
/// [`LseError::QuotaExceeded`](super::error::LseError::QuotaExceeded) carrying this status, never as
/// a silent stall or a blind retry.
///
/// # Shape
///
/// The fields mirror the provider's `/vault/usage` response exactly, including its asymmetries:
/// the allowance is **multi-dimensional** (bytes per month, bytes per week, exports per hour) and
/// there is **no reset timestamp on any dimension**. No `reset_at` is synthesised — the provider
/// does not report when a window rolls over, and inventing a plausible instant would be precisely
/// the kind of quiet fiction this integration exists to avoid.
///
/// Unknown fields are ignored rather than rejected, so a dimension added by the provider does not
/// break deserialisation. The struct is `#[non_exhaustive]` for the same reason: adding a field
/// here when that happens stays a non-breaking change.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct QuotaStatus {
    /// Bytes downloaded in the current calendar month.
    pub bytes_used_month: u64,
    /// Byte allowance for the current calendar month.
    pub bytes_cap_month: u64,
    /// Bytes downloaded in the current week.
    pub bytes_used_week: u64,
    /// Byte allowance for the current week.
    pub bytes_cap_week: u64,
    /// Export jobs submitted in the current hour.
    pub exports_this_hour: u32,
    /// Export-job allowance per hour.
    pub exports_cap_hour: u32,
    /// How far back history may be requested, in months, or `-1` for unlimited.
    ///
    /// Prefer [`historical_limit_months`](Self::historical_limit_months), which models the
    /// unlimited case as `None` instead of a negative count.
    pub historical_data_months: i32,
    /// Permitted request rate, in calls per minute.
    pub calls_per_minute: u32,
    /// Maximum rows a single request may return.
    ///
    /// Note this is a **silent** cap: an over-large range returns exactly this many rows with a
    /// `200` and no truncation marker of any kind. See
    /// [`fetch_candles`](super::vault::LseVaultClient::fetch_candles).
    pub max_rows_per_request: u32,
    /// Maximum concurrent vault requests.
    pub vault_concurrency: u32,
}

impl QuotaStatus {
    /// Bytes still available this month, saturating at zero.
    #[must_use]
    pub fn bytes_remaining_month(&self) -> u64 {
        self.bytes_cap_month.saturating_sub(self.bytes_used_month)
    }

    /// Bytes still available this week, saturating at zero.
    #[must_use]
    pub fn bytes_remaining_week(&self) -> u64 {
        self.bytes_cap_week.saturating_sub(self.bytes_used_week)
    }

    /// Export jobs still available this hour, saturating at zero.
    #[must_use]
    pub fn exports_remaining_hour(&self) -> u32 {
        self.exports_cap_hour.saturating_sub(self.exports_this_hour)
    }

    /// How far back history may be requested, or `None` when unlimited.
    ///
    /// The provider encodes "unlimited" as `-1`; this maps that sentinel to `None` so callers do
    /// not have to know it. Any other negative value is also treated as unlimited rather than
    /// silently wrapping into a huge positive count.
    #[must_use]
    pub fn historical_limit_months(&self) -> Option<u32> {
        u32::try_from(self.historical_data_months).ok()
    }

    /// Whether the **byte** allowance is exhausted, on either window.
    ///
    /// This is the one that gates reading data — streaming candles over REST, downloading an
    /// artifact — because bytes are what those spend.
    #[must_use]
    pub fn is_byte_allowance_exhausted(&self) -> bool {
        self.bytes_remaining_month() == 0 || self.bytes_remaining_week() == 0
    }

    /// Whether this hour's **export submit** allowance is exhausted.
    ///
    /// Gates [`submit_export`](super::vault::LseVaultClient::submit_export) and nothing else.
    /// Downloading an artifact from a job already submitted spends bytes, not exports.
    #[must_use]
    pub fn is_export_allowance_exhausted(&self) -> bool {
        self.exports_remaining_hour() == 0
    }

    /// Whether **any** metered dimension is exhausted.
    ///
    /// Covers only the dimensions the provider reports as used-against-cap (monthly bytes, weekly
    /// bytes, hourly exports). The static limits — [`calls_per_minute`](Self::calls_per_minute),
    /// [`max_rows_per_request`](Self::max_rows_per_request),
    /// [`vault_concurrency`](Self::vault_concurrency) — are request-shaping constraints with no
    /// running total, so they cannot be "exhausted" and are excluded.
    ///
    /// # Usually the wrong question
    /// The dimensions meter **different operations**, so the union is only right for a consumer
    /// that does all of them. Gating a candle backfill on this stops fetching for up to an hour
    /// after five exports, with terabytes of byte allowance untouched and nothing about the fetches
    /// actually blocked. Gate on the dimension the operation spends:
    /// [`is_byte_allowance_exhausted`](Self::is_byte_allowance_exhausted) for reading data,
    /// [`is_export_allowance_exhausted`](Self::is_export_allowance_exhausted) for submitting an
    /// export.
    #[must_use]
    pub fn is_exhausted(&self) -> bool {
        self.is_byte_allowance_exhausted() || self.is_export_allowance_exhausted()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;

    /// The exact `/vault/usage` payload shape, field-for-field.
    const USAGE_RESPONSE: &str = r#"{
        "bytes_used_month": 5962,
        "bytes_cap_month": 53687091200,
        "bytes_used_week": 5962,
        "bytes_cap_week": 16106127360,
        "exports_this_hour": 0,
        "exports_cap_hour": 5,
        "historical_data_months": -1,
        "calls_per_minute": 200,
        "max_rows_per_request": 5000,
        "vault_concurrency": 2
    }"#;

    fn parsed() -> QuotaStatus {
        serde_json::from_str(USAGE_RESPONSE).unwrap()
    }

    #[test]
    fn deserializes_the_real_usage_response() {
        let status = parsed();

        assert_eq!(status.bytes_used_month, 5962);
        assert_eq!(status.bytes_cap_month, 53_687_091_200);
        assert_eq!(status.bytes_used_week, 5962);
        assert_eq!(status.bytes_cap_week, 16_106_127_360);
        assert_eq!(status.exports_this_hour, 0);
        // The hourly export allowance is 5, not 10.
        assert_eq!(status.exports_cap_hour, 5);
        assert_eq!(status.calls_per_minute, 200);
        assert_eq!(status.max_rows_per_request, 5000);
        assert_eq!(status.vault_concurrency, 2);
    }

    #[test]
    fn unknown_fields_are_ignored_rather_than_rejected() {
        // The provider may add a dimension at any time; that must not break every existing caller.
        let with_extra = r#"{
            "bytes_used_month": 1, "bytes_cap_month": 2,
            "bytes_used_week": 3, "bytes_cap_week": 4,
            "exports_this_hour": 5, "exports_cap_hour": 6,
            "historical_data_months": 7, "calls_per_minute": 8,
            "max_rows_per_request": 9, "vault_concurrency": 10,
            "tokens_used_day": 11
        }"#;

        assert_eq!(
            serde_json::from_str::<QuotaStatus>(with_extra)
                .unwrap()
                .bytes_used_month,
            1
        );
    }

    #[test]
    fn unlimited_history_is_none_not_a_negative_count() {
        assert_eq!(parsed().historical_limit_months(), None);
    }

    #[test]
    fn a_bounded_history_limit_is_reported_as_a_count() {
        let mut status = parsed();
        status.historical_data_months = 24;

        assert_eq!(status.historical_limit_months(), Some(24));
    }

    #[test]
    fn remaining_saturates_instead_of_underflowing() {
        // Usage above cap is the provider's to report, not ours to panic on.
        let mut status = parsed();
        status.bytes_used_month = status.bytes_cap_month + 1;
        status.bytes_used_week = status.bytes_cap_week + 1;
        status.exports_this_hour = status.exports_cap_hour + 1;

        assert_eq!(status.bytes_remaining_month(), 0);
        assert_eq!(status.bytes_remaining_week(), 0);
        assert_eq!(status.exports_remaining_hour(), 0);
        assert!(status.is_exhausted());
    }

    #[test]
    fn a_fresh_allowance_is_not_exhausted() {
        let status = parsed();

        assert_eq!(status.bytes_remaining_month(), 53_687_091_200 - 5962);
        assert_eq!(status.exports_remaining_hour(), 5);
        assert!(!status.is_exhausted());
    }

    #[test]
    fn static_limits_alone_never_mark_the_allowance_exhausted() {
        // `calls_per_minute` / `max_rows_per_request` / `vault_concurrency` shape a request; they
        // carry no running total and so cannot be spent.
        let mut status = parsed();
        status.calls_per_minute = 0;
        status.max_rows_per_request = 0;
        status.vault_concurrency = 0;

        assert!(!status.is_exhausted());
    }
}
