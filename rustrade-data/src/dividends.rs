//! Cash dividends: what an issuer pays per share and on which dates, in one shape for every
//! provider, with the instant each became public.
//!
//! A [`Dividend`] is one provider's statement about one cash dividend: its ex-dividend date, its
//! amount per share as paid, its declaration, record and payment dates where the provider gives
//! them, and when it was known.
//!
//! # The dates
//! An issuer **declares** a dividend, saying the amount and the dates. The stock trades without
//! the right to it from the **ex-dividend date**, and its price falls by about the amount at that
//! day's open. Holders on the **record date** are paid on the **payment date**. Since US
//! settlement moved to one business day (May 2024), the record date is usually the ex-date;
//! before that the ex-date was a business day before the record date. Do not assume the order
//! ex, record, payment: in the US a distribution worth a quarter or more of the share price goes
//! ex on the business day after its payment date. Only the declaration comes before the rest.
//!
//! An option pricer needs the ex-date and the amount (see the `rustrade-pricing` crate's
//! `CashDividend`, whose time is the ex-date's open). Only the ex-date is always present.
//!
//! # Identity and revisions
//! A dividend is identified by its symbol and its ex-date. Two records with the same pair describe
//! the same dividend; a later one, such as a corrected amount, supersedes an earlier one.
//! [`DividendStore`] keeps every record and applies that order.
//!
//! # Point in time
//! [`known_at`](Dividend::known_at) is the earliest instant the record's content is known to have
//! been public. A dividend is public once declared: a provider that gives the declaration date
//! makes it known by the end of that day, the latest the date allows. One with no declaration
//! date is known only from the start of its ex-date, so a backtest does not see it coming and a
//! pricer misses it until it has gone ex. A record is never known later than it was fetched.
//!
//! A backtest that only uses records whose `known_at` is at or before its clock uses only
//! dividends declared by then. That bounds when the content was public, not when a provider could
//! return it.
//!
//! # Amounts
//! [`amount`](Dividend::amount) is per share as paid, in the currency the stock is quoted in. It
//! is not adjusted for later splits: a dividend of 0.80 before a 4-for-1 split stays 0.80, while
//! the bars a provider serves are often split-adjusted. Restating it on a split-adjusted basis,
//! and projecting dividends not yet declared, are the caller's.

use chrono::{DateTime, NaiveDate, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use smol_str::SmolStr;

mod store;

pub use store::DividendStore;

/// One provider's statement about one cash dividend. See the [module documentation](self).
///
/// The record every provider's dividend read returns, with the instant it became public. Not to
/// be confused with a provider's own response types, or with the `rustrade-pricing` crate's
/// `CashDividend`, which is a pricing input built from one of these.
///
/// `#[non_exhaustive]`: fields may be added without a major-version bump. Build one with
/// [`Dividend::new`] and the `with_*` setters.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Dividend {
    /// The issuer's ticker symbol, as the caller named it to the provider.
    pub symbol: SmolStr,
    /// The first day the stock trades without the right to the dividend, in the local time of
    /// its listing exchange (New York for US issuers).
    pub ex_date: NaiveDate,
    /// The amount per share as paid, in the currency the stock is quoted in, not adjusted for
    /// later splits.
    pub amount: Decimal,
    /// The day the issuer declared the dividend, where the provider gives it.
    pub declaration_date: Option<NaiveDate>,
    /// The day holders of record are entitled to the dividend, where the provider gives it.
    pub record_date: Option<NaiveDate>,
    /// The day the dividend is paid, where the provider gives it.
    pub payment_date: Option<NaiveDate>,
    /// The earliest instant this record's content is known to have been public. See
    /// ["Point in time"](self#point-in-time).
    pub known_at: DateTime<Utc>,
    /// Where the record came from.
    pub source: DividendProvenance,
}

impl Dividend {
    /// A record of `symbol`'s dividend of `amount` per share going ex on `ex_date`, known at
    /// `known_at`, reported by `source`, with no declaration, record or payment date.
    ///
    /// ```
    /// use chrono::{NaiveDate, TimeZone, Utc};
    /// use rust_decimal_macros::dec;
    /// use rustrade_data::dividends::{Dividend, DividendProvenance, DividendProvider};
    ///
    /// let day = |y, m, d| NaiveDate::from_ymd_opt(y, m, d).unwrap();
    /// let fetched = Utc.with_ymd_and_hms(2026, 11, 24, 12, 0, 0).unwrap();
    /// // Known from the end of its declaration date, 00:00 New York on 18 November.
    /// let known_at = Utc.with_ymd_and_hms(2026, 11, 18, 5, 0, 0).unwrap();
    /// let dividend = Dividend::new(
    ///     "XYZ",
    ///     day(2026, 12, 2),
    ///     dec!(0.25),
    ///     known_at,
    ///     DividendProvenance::new(DividendProvider::AlphaVantage, fetched),
    /// )
    /// .with_declaration_date(day(2026, 11, 17))
    /// .with_payment_date(day(2026, 12, 20));
    /// assert_eq!(dividend.record_date, None);
    /// ```
    pub fn new(
        symbol: impl Into<SmolStr>,
        ex_date: NaiveDate,
        amount: Decimal,
        known_at: DateTime<Utc>,
        source: DividendProvenance,
    ) -> Self {
        Self {
            symbol: symbol.into(),
            ex_date,
            amount,
            declaration_date: None,
            record_date: None,
            payment_date: None,
            known_at,
            source,
        }
    }

    /// With the day the issuer declared the dividend.
    pub fn with_declaration_date(self, date: NaiveDate) -> Self {
        Self {
            declaration_date: Some(date),
            ..self
        }
    }

    /// With the record date.
    pub fn with_record_date(self, date: NaiveDate) -> Self {
        Self {
            record_date: Some(date),
            ..self
        }
    }

    /// With the payment date.
    pub fn with_payment_date(self, date: NaiveDate) -> Self {
        Self {
            payment_date: Some(date),
            ..self
        }
    }
}

/// Where a [`Dividend`] record came from.
///
/// `#[non_exhaustive]`: fields may be added without a major-version bump. Build one with
/// [`DividendProvenance::new`].
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[non_exhaustive]
pub struct DividendProvenance {
    /// The provider that reported the record.
    pub provider: DividendProvider,
    /// The provider's own identifier for the record, where it has one.
    pub record_id: Option<SmolStr>,
    /// When this library fetched the record.
    pub observed_at: DateTime<Utc>,
}

impl DividendProvenance {
    /// A record from `provider`, fetched at `observed_at`, with no record identifier.
    pub fn new(provider: DividendProvider, observed_at: DateTime<Utc>) -> Self {
        Self {
            provider,
            record_id: None,
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
}

/// A provider of [`Dividend`]s.
///
/// `#[non_exhaustive]`: providers may be added without a major-version bump.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[non_exhaustive]
pub enum DividendProvider {
    /// Alpha Vantage: past and declared coming dividends.
    AlphaVantage,
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use chrono::TimeZone;
    use rust_decimal_macros::dec;

    #[test]
    fn a_dividend_starts_without_optional_dates_and_takes_each_on_its_own() {
        let day = |y, m, d| NaiveDate::from_ymd_opt(y, m, d).unwrap();
        let at = Utc.with_ymd_and_hms(2026, 1, 2, 0, 0, 0).unwrap();
        let source = DividendProvenance::new(DividendProvider::AlphaVantage, at);
        let dividend = Dividend::new("XYZ", day(2026, 2, 1), dec!(0.5), at, source.clone());
        assert_eq!(
            (
                dividend.declaration_date,
                dividend.record_date,
                dividend.payment_date
            ),
            (None, None, None)
        );
        assert_eq!(
            (
                dividend.symbol.as_str(),
                dividend.ex_date,
                dividend.amount,
                dividend.known_at
            ),
            ("XYZ", day(2026, 2, 1), dec!(0.5), at)
        );

        let declared = dividend.clone().with_declaration_date(day(2026, 1, 20));
        assert_eq!(
            (
                declared.declaration_date,
                declared.record_date,
                declared.payment_date
            ),
            (Some(day(2026, 1, 20)), None, None)
        );
        let recorded = dividend.clone().with_record_date(day(2026, 2, 2));
        assert_eq!(
            (
                recorded.declaration_date,
                recorded.record_date,
                recorded.payment_date
            ),
            (None, Some(day(2026, 2, 2)), None)
        );
        let paid = dividend.with_payment_date(day(2026, 2, 15));
        assert_eq!(
            (paid.declaration_date, paid.record_date, paid.payment_date),
            (None, None, Some(day(2026, 2, 15)))
        );

        assert_eq!(source.record_id, None);
        let with_id = source.with_record_id("R-1");
        assert_eq!(with_id.record_id.as_deref(), Some("R-1"));
        assert_eq!(
            (with_id.provider, with_id.observed_at),
            (DividendProvider::AlphaVantage, at)
        );
    }
}
