//! Earnings releases: when an issuer reports a fiscal period's results, in one shape for every
//! provider.
//!
//! An [`EarningsRelease`] is one provider's statement about one release: the period it reports,
//! its date and time of day, and how certain the date is. Providers that report past releases
//! and providers that publish a calendar of coming ones both return it, so their records can be
//! merged.
//!
//! # Identity and revisions
//! A release is identified by its [`EarningsKey`]: the issuer's symbol and the last day of the
//! fiscal period it reports. Providers revise coming dates in place and keep no history of a
//! change, so a moved date shows up as a second record with the same key and a later
//! [`known_at`](EarningsRelease::known_at). Two records with the same key describe the same
//! release; the later one supersedes the earlier.
//!
//! # Point in time
//! [`known_at`](EarningsRelease::known_at) is the earliest instant the record's content is known
//! to have been public: the release itself for a past release, or the moment the library fetched
//! the record when the provider gives no better time. A backtest that only uses records whose
//! `known_at` is at or before its clock uses only what had been made public by then.
//!
//! That bounds when the content was public, not when a provider could return it. A provider can
//! publish a record well after its `known_at`: SEC EDGAR, for one, returns a release only once
//! the period's quarterly or annual report is filed, often weeks later. A backtest that must only
//! see what a given provider would have returned live has to apply that provider's delay itself.
//!
//! No provider records when an issuer announced a coming date, so a calendar's `known_at` is only
//! as early as its first fetch. Records fetched and kept over time are the only history of a
//! calendar.

use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use smol_str::SmolStr;

/// One provider's statement about one earnings release. See the [module documentation](self).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct EarningsRelease {
    /// The issuer's ticker symbol, as the caller named it to the provider.
    pub symbol: SmolStr,
    /// The last day of the fiscal period whose results the release reports. Issuers with 52- or
    /// 53-week years end their periods on a weekday rather than a month end.
    pub fiscal_period_end: NaiveDate,
    /// The release's date in the local time of the issuer's listing exchange (New York for US
    /// issuers). It can fall on a day without a session.
    pub date: NaiveDate,
    /// When in the trading day the release came, or is expected to come.
    pub timing: ReleaseTiming,
    /// How certain the date is.
    pub status: ReleaseStatus,
    /// The earliest instant this record's content is known to have been public. See
    /// ["Point in time"](self#point-in-time).
    pub known_at: DateTime<Utc>,
    /// Where the record came from.
    pub source: ReleaseSource,
}

impl EarningsRelease {
    /// The release this record describes: two records with the same key describe the same
    /// release, and the one with the later [`known_at`](Self::known_at) supersedes the other.
    pub fn key(&self) -> EarningsKey {
        EarningsKey {
            symbol: self.symbol.clone(),
            fiscal_period_end: self.fiscal_period_end,
        }
    }
}

/// The identity of one earnings release: the issuer and the fiscal period it reports.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct EarningsKey {
    /// The issuer's ticker symbol.
    pub symbol: SmolStr,
    /// The last day of the fiscal period the release reports.
    pub fiscal_period_end: NaiveDate,
}

/// When in the trading day of its [`date`](EarningsRelease::date) a release came, relative to the
/// regular session of the issuer's listing exchange.
///
/// It decides which session's prices first reflect the release: the same session's for
/// [`BeforeOpen`](Self::BeforeOpen), the next session's for [`AfterClose`](Self::AfterClose).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ReleaseTiming {
    /// Before the regular session opens.
    BeforeOpen,
    /// During the regular session.
    DuringSession,
    /// After the regular session closes.
    AfterClose,
    /// The provider does not say.
    Unknown,
}

/// How certain a release's date is.
///
/// `#[non_exhaustive]`: providers' calendars may need statuses added without a major-version bump.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[non_exhaustive]
pub enum ReleaseStatus {
    /// The release has happened on this date.
    Reported,
    /// The issuer has announced this date for a coming release.
    Confirmed,
    /// A coming date estimated by the provider, not announced by the issuer.
    Projected,
    /// The issuer has postponed the release from this date.
    Postponed,
    /// The issuer has canceled the release planned for this date.
    Canceled,
    /// The provider does not say whether the date is announced or estimated.
    Unknown,
}

/// Where an [`EarningsRelease`] came from.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ReleaseSource {
    /// The provider that reported the record.
    pub provider: EarningsProvider,
    /// The provider's own identifier for the record, where it has one (for SEC EDGAR, the
    /// filing's accession number).
    pub record_id: Option<SmolStr>,
    /// When the provider last changed the record, where it says.
    pub updated_at: Option<DateTime<Utc>>,
    /// When this library fetched the record.
    pub observed_at: DateTime<Utc>,
}

/// A provider of [`EarningsRelease`]s.
///
/// `#[non_exhaustive]`: providers may be added without a major-version bump.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[non_exhaustive]
pub enum EarningsProvider {
    /// The SEC's EDGAR filings: past releases, from the 8-K filings that furnish them.
    SecEdgar,
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn release(period_end: NaiveDate, known_at: DateTime<Utc>) -> EarningsRelease {
        EarningsRelease {
            symbol: SmolStr::new("XYZ"),
            fiscal_period_end: period_end,
            date: NaiveDate::from_ymd_opt(2026, 1, 28).unwrap(),
            timing: ReleaseTiming::AfterClose,
            status: ReleaseStatus::Confirmed,
            known_at,
            source: ReleaseSource {
                provider: EarningsProvider::SecEdgar,
                record_id: None,
                updated_at: None,
                observed_at: known_at,
            },
        }
    }

    #[test]
    fn the_key_is_the_symbol_and_the_period_end_whatever_the_date_or_known_at() {
        let period_end = NaiveDate::from_ymd_opt(2025, 12, 31).unwrap();
        let first = release(
            period_end,
            Utc.with_ymd_and_hms(2026, 1, 2, 0, 0, 0).unwrap(),
        );
        let mut moved = release(
            period_end,
            Utc.with_ymd_and_hms(2026, 1, 9, 0, 0, 0).unwrap(),
        );
        moved.date = NaiveDate::from_ymd_opt(2026, 1, 29).unwrap();
        assert_eq!(first.key(), moved.key());
        assert_eq!(
            first.key(),
            EarningsKey {
                symbol: SmolStr::new("XYZ"),
                fiscal_period_end: period_end,
            }
        );

        let next_quarter = release(
            NaiveDate::from_ymd_opt(2026, 3, 31).unwrap(),
            Utc.with_ymd_and_hms(2026, 1, 2, 0, 0, 0).unwrap(),
        );
        assert_ne!(first.key(), next_quarter.key());
    }
}
