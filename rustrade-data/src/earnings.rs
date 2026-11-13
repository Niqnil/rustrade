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
///
/// `#[non_exhaustive]`: fields may be added without a major-version bump. Build one with
/// [`EarningsRelease::new`].
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[non_exhaustive]
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
    /// A record of the release of `symbol`'s results for the fiscal period ending
    /// `fiscal_period_end`, reported by `source`.
    ///
    /// ```
    /// use chrono::{NaiveDate, TimeZone, Utc};
    /// use rustrade_data::earnings::{
    ///     EarningsProvider, EarningsRelease, ReleaseSource, ReleaseStatus, ReleaseTiming,
    /// };
    ///
    /// let known_at = Utc.with_ymd_and_hms(2026, 1, 28, 21, 30, 0).unwrap();
    /// let release = EarningsRelease::new(
    ///     "XYZ",
    ///     NaiveDate::from_ymd_opt(2025, 12, 31).unwrap(),
    ///     NaiveDate::from_ymd_opt(2026, 1, 28).unwrap(),
    ///     ReleaseTiming::AfterClose,
    ///     ReleaseStatus::Reported,
    ///     known_at,
    ///     ReleaseSource::new(EarningsProvider::SecEdgar, known_at).with_record_id("0000001234-26-000002"),
    /// );
    /// assert_eq!(release.key().symbol, "XYZ");
    /// ```
    pub fn new(
        symbol: impl Into<SmolStr>,
        fiscal_period_end: NaiveDate,
        date: NaiveDate,
        timing: ReleaseTiming,
        status: ReleaseStatus,
        known_at: DateTime<Utc>,
        source: ReleaseSource,
    ) -> Self {
        Self {
            symbol: symbol.into(),
            fiscal_period_end,
            date,
            timing,
            status,
            known_at,
            source,
        }
    }

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
///
/// `#[non_exhaustive]`: fields may be added without a major-version bump. Build one with
/// [`ReleaseSource::new`].
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[non_exhaustive]
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

impl ReleaseSource {
    /// A record from `provider`, fetched at `observed_at`, with no record identifier or update
    /// time.
    pub fn new(provider: EarningsProvider, observed_at: DateTime<Utc>) -> Self {
        Self {
            provider,
            record_id: None,
            updated_at: None,
            observed_at,
        }
    }

    /// With the provider's own identifier for the record.
    pub fn with_record_id(self, record_id: impl Into<SmolStr>) -> Self {
        Self {
            record_id: Some(record_id.into()),
            ..self
        }
    }

    /// With the time the provider last changed the record.
    pub fn with_updated_at(self, updated_at: DateTime<Utc>) -> Self {
        Self {
            updated_at: Some(updated_at),
            ..self
        }
    }
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
        EarningsRelease::new(
            "XYZ",
            period_end,
            NaiveDate::from_ymd_opt(2026, 1, 28).unwrap(),
            ReleaseTiming::AfterClose,
            ReleaseStatus::Confirmed,
            known_at,
            ReleaseSource::new(EarningsProvider::SecEdgar, known_at),
        )
    }

    #[test]
    fn a_source_starts_without_an_id_or_update_time_and_takes_each_on_its_own() {
        let observed_at = Utc.with_ymd_and_hms(2026, 1, 2, 0, 0, 0).unwrap();
        let updated_at = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        let source = ReleaseSource::new(EarningsProvider::SecEdgar, observed_at);
        assert_eq!(
            (source.provider, source.record_id.clone(), source.updated_at),
            (EarningsProvider::SecEdgar, None, None)
        );
        assert_eq!(source.observed_at, observed_at);

        let with_id = source.clone().with_record_id("A-1");
        assert_eq!(with_id.record_id.as_deref(), Some("A-1"));
        assert_eq!(with_id.updated_at, None);

        let with_both = with_id.with_updated_at(updated_at);
        assert_eq!(with_both.record_id.as_deref(), Some("A-1"));
        assert_eq!(with_both.updated_at, Some(updated_at));
        assert_eq!(with_both.observed_at, observed_at);
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
