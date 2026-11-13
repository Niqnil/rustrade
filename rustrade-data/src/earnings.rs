//! Earnings releases: when an issuer reports a fiscal period's results, in one shape for every
//! provider.
//!
//! An [`EarningsRelease`] is one provider's statement about one release: the period it reports,
//! its date and time of day, and how certain the date is. Providers that report past releases
//! and providers that publish a calendar of coming ones both return it, so their records can be
//! merged.
//!
//! # Identity and revisions
//! A release is identified by its [`EarningsKey`]: the issuer's symbol and the fiscal period it
//! reports, named by the calendar month end nearest the period's last day
//! ([`nearest_month_end`]). Providers disagree on that last day: one gives the exact date, such
//! as a Saturday for an issuer whose fiscal year has 52 or 53 weeks, and another rounds it to a
//! month end. Their records of the same release have the same key all the same.
//!
//! Providers revise coming dates in place and keep no history of a change, so a moved date shows
//! up as a second record with the same key and a later [`known_at`](EarningsRelease::known_at).
//! Two records with the same key describe the same release; the later one supersedes the earlier,
//! and a record that the release has happened supersedes any that does not say so.
//! [`EarningsStore`] keeps them all and applies that order.
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
//!
//! [`EarningsStore`] answers from such records the next and the previous release of a symbol as
//! of any instant, from the records public by then.

use chrono::{DateTime, Datelike, Months, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use smol_str::SmolStr;

mod store;

pub use store::EarningsStore;

/// One provider's statement about one earnings release. See the [module documentation](self).
///
/// `#[non_exhaustive]`: fields may be added without a major-version bump. Build one with
/// [`EarningsRelease::new`].
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[non_exhaustive]
pub struct EarningsRelease {
    /// The issuer's ticker symbol, as the caller named it to the provider.
    pub symbol: SmolStr,
    /// The last day of the fiscal period whose results the release reports, as the provider gives
    /// it. Issuers with 52- or 53-week years end their periods on a weekday near a month end; some
    /// providers give that day, and others round it to the month end.
    /// [`key`](Self::key) names the period by the nearest month end, which both agree on.
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
    /// assert_eq!(release.key().symbol(), "XYZ");
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
    /// release. Which one supersedes the other is set out in [`EarningsStore`]'s contract.
    pub fn key(&self) -> EarningsKey {
        EarningsKey::new(self.symbol.clone(), self.fiscal_period_end)
    }
}

/// The identity of one earnings release: the issuer and the fiscal period it reports, named by
/// the calendar month end nearest the period's last day. See
/// ["Identity and revisions"](self#identity-and-revisions).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(from = "KeyFields")]
pub struct EarningsKey {
    symbol: SmolStr,
    period: NaiveDate,
}

impl EarningsKey {
    /// The release of `symbol`'s results for the fiscal period whose last day is
    /// `fiscal_period_end`, exact or already rounded to a month end.
    pub fn new(symbol: impl Into<SmolStr>, fiscal_period_end: NaiveDate) -> Self {
        Self {
            symbol: symbol.into(),
            period: nearest_month_end(fiscal_period_end),
        }
    }

    /// The issuer's ticker symbol.
    pub fn symbol(&self) -> &SmolStr {
        &self.symbol
    }

    /// The month end nearest the fiscal period's last day, which names the period.
    pub fn period(&self) -> NaiveDate {
        self.period
    }
}

/// A deserialized key's fields, normalised on the way in so a stored key cannot hold a period
/// that is not a month end.
#[derive(Deserialize)]
struct KeyFields {
    symbol: SmolStr,
    period: NaiveDate,
}

impl From<KeyFields> for EarningsKey {
    fn from(fields: KeyFields) -> Self {
        Self::new(fields.symbol, fields.period)
    }
}

/// The calendar month end nearest `date`: the end of `date`'s own month, or of the month before
/// if that is closer (on a tie, its own).
///
/// It names a fiscal period whatever day a provider gives as its end. An issuer whose fiscal year
/// has 52 or 53 weeks ends each period on a fixed weekday: the last one in a month, up to six days
/// before the month ends, or the one nearest the month end, up to three days either side. All of
/// these, and the month end itself, map to the same month end:
///
/// ```
/// use chrono::NaiveDate;
/// use rustrade_data::earnings::nearest_month_end;
///
/// let day = |y, m, d| NaiveDate::from_ymd_opt(y, m, d).unwrap();
/// assert_eq!(nearest_month_end(day(2026, 9, 26)), day(2026, 9, 30)); // a Saturday, 4 days early
/// assert_eq!(nearest_month_end(day(2019, 9, 1)), day(2019, 8, 31)); // a Sunday, 1 day late
/// assert_eq!(nearest_month_end(day(2026, 9, 30)), day(2026, 9, 30));
/// ```
pub fn nearest_month_end(date: NaiveDate) -> NaiveDate {
    let first_of_month = date.with_day(1).unwrap_or(date);
    let previous_end = first_of_month.pred_opt().unwrap_or(first_of_month);
    let own_end = first_of_month
        .checked_add_months(Months::new(1))
        .and_then(|next_first| next_first.pred_opt())
        .unwrap_or(date);
    if own_end - date <= date - previous_end {
        own_end
    } else {
        previous_end
    }
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
    /// Alpha Vantage: its earnings calendar of coming releases, and its earnings history.
    AlphaVantage,
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
    fn an_exact_period_end_and_its_month_end_make_the_same_key() {
        let day = |y, m, d| NaiveDate::from_ymd_opt(y, m, d).unwrap();
        let exact = release(
            day(2026, 9, 26),
            Utc.with_ymd_and_hms(2026, 10, 30, 0, 0, 0).unwrap(),
        );
        let rounded = release(
            day(2026, 9, 30),
            Utc.with_ymd_and_hms(2026, 10, 31, 0, 0, 0).unwrap(),
        );
        assert_eq!(exact.key(), rounded.key());
        assert_eq!(exact.key().period(), day(2026, 9, 30));
        assert_eq!(exact.fiscal_period_end, day(2026, 9, 26));
    }

    #[test]
    fn the_nearest_month_end_rounds_either_way_and_across_a_year() {
        let day = |y, m, d| NaiveDate::from_ymd_opt(y, m, d).unwrap();
        let cases = [
            (day(2026, 9, 26), day(2026, 9, 30)),
            (day(2026, 9, 24), day(2026, 9, 30)),
            (day(2019, 9, 1), day(2019, 8, 31)),
            (day(2026, 1, 3), day(2025, 12, 31)),
            (day(2025, 12, 27), day(2025, 12, 31)),
            (day(2026, 2, 28), day(2026, 2, 28)),
            (day(2024, 2, 29), day(2024, 2, 29)),
            (day(2024, 3, 2), day(2024, 2, 29)),
            (day(2026, 3, 1), day(2026, 2, 28)),
            // The middle of a month: 14 days after the previous end, 16 before its own.
            (day(2026, 4, 14), day(2026, 3, 31)),
            // A tie (15 days each way) goes to its own month.
            (day(2026, 4, 15), day(2026, 4, 30)),
            (day(2026, 4, 16), day(2026, 4, 30)),
        ];
        for (date, expected) in cases {
            assert_eq!(nearest_month_end(date), expected, "{date}");
        }
    }

    #[test]
    fn a_deserialized_key_is_normalised() {
        let key: EarningsKey =
            serde_json::from_str(r#"{"symbol":"XYZ","period":"2026-09-26"}"#).unwrap();
        assert_eq!(
            key,
            EarningsKey::new("XYZ", NaiveDate::from_ymd_opt(2026, 9, 30).unwrap())
        );
        let json = serde_json::to_string(&key).unwrap();
        assert_eq!(json, r#"{"symbol":"XYZ","period":"2026-09-30"}"#);
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
        assert_eq!(first.key(), EarningsKey::new("XYZ", period_end));
        assert_eq!(first.key().symbol(), "XYZ");
        assert_eq!(first.key().period(), period_end);

        let next_quarter = release(
            NaiveDate::from_ymd_opt(2026, 3, 31).unwrap(),
            Utc.with_ymd_and_hms(2026, 1, 2, 0, 0, 0).unwrap(),
        );
        assert_ne!(first.key(), next_quarter.key());
    }
}
