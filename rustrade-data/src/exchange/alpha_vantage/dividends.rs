//! Decoding Alpha Vantage's `DIVIDENDS` responses into [`Dividend`]s.

use super::{error::AlphaVantageError, records};
use crate::{
    dividends::{Dividend, DividendProvenance, DividendProvider},
    exchange::new_york,
};
use chrono::{DateTime, NaiveDate, NaiveTime, Utc};
use rust_decimal::Decimal;
use serde::Deserialize;
use std::str::FromStr;
use tracing::{debug, warn};

/// What Alpha Vantage writes in place of a date it does not have.
const NO_DATE: &str = "None";

#[derive(Deserialize)]
struct Dividends {
    data: Vec<Row>,
}

#[derive(Deserialize)]
struct Row {
    ex_dividend_date: String,
    #[serde(default)]
    declaration_date: Option<String>,
    #[serde(default)]
    record_date: Option<String>,
    #[serde(default)]
    payment_date: Option<String>,
    amount: String,
}

/// The dividends in a `DIVIDENDS` body for `symbol`, fetched at `observed_at`, by ex-date.
///
/// A dividend with a declaration date is known at the end of that date (00:00 New York the next
/// day); one without is known from the start of its ex-date (00:00 New York). Either is clamped
/// to `observed_at`, so no record is known later than it was fetched. A row whose amount is zero
/// is skipped and logged at DEBUG: Alpha Vantage lists such rows with full dates, and they pay
/// nothing.
///
/// # Errors
/// [`AlphaVantageError::Decode`] for a body without `data`, a row without an ex-date, a date that
/// is neither a date nor `"None"`, or an amount that is not a non-negative number.
pub(crate) fn dividends(
    body: &str,
    symbol: &str,
    observed_at: DateTime<Utc>,
) -> Result<Vec<Dividend>, AlphaVantageError> {
    let response: Dividends = serde_json::from_str(body)
        .map_err(|e| AlphaVantageError::decode(format!("dividends: {e}")))?;
    let source = DividendProvenance::new(DividendProvider::AlphaVantage, observed_at);
    let mut dividends = Vec::with_capacity(response.data.len());
    for row in &response.data {
        let ex_date = records::date(&row.ex_dividend_date, "ex_dividend_date")?;
        let amount = amount(&row.amount)?;
        if amount.is_zero() {
            debug!(%symbol, %ex_date, "Alpha Vantage dividend row pays nothing; skipping");
            continue;
        }
        let declaration_date = optional_date(row.declaration_date.as_deref(), "declaration_date")?;
        let known_at = match declaration_date {
            Some(declared) => midnight(declared.succ_opt().ok_or_else(|| {
                AlphaVantageError::decode(format!("declaration_date {declared} has no next day"))
            })?)?,
            None => midnight(ex_date)?,
        }
        .min(observed_at);
        let mut dividend = Dividend::new(symbol, ex_date, amount, known_at, source.clone());
        dividend.declaration_date = declaration_date;
        dividend.record_date = optional_date(row.record_date.as_deref(), "record_date")?;
        dividend.payment_date = optional_date(row.payment_date.as_deref(), "payment_date")?;
        dividends.push(dividend);
    }
    dividends.sort_by_key(|dividend| dividend.ex_date);
    if let Some(pair) = dividends.windows(2).find(|w| w[0].ex_date == w[1].ex_date) {
        warn!(
            %symbol,
            ex_date = %pair[0].ex_date,
            "Alpha Vantage lists two dividends on one ex-date; a DividendStore keeps one of them"
        );
    }
    Ok(dividends)
}

/// A date Alpha Vantage may leave out: `"None"`, empty or absent is `None`.
fn optional_date(text: Option<&str>, field: &str) -> Result<Option<NaiveDate>, AlphaVantageError> {
    match text.map(str::trim) {
        None | Some("") | Some(NO_DATE) => Ok(None),
        Some(text) => records::date(text, field).map(Some),
    }
}

/// A dividend amount: a number, not negative.
fn amount(text: &str) -> Result<Decimal, AlphaVantageError> {
    match Decimal::from_str(text.trim()) {
        Ok(amount) if !amount.is_sign_negative() => Ok(amount),
        _ => Err(AlphaVantageError::decode(format!(
            "amount {text:?} is not a non-negative number"
        ))),
    }
}

/// The instant `date` starts in New York.
fn midnight(date: NaiveDate) -> Result<DateTime<Utc>, AlphaVantageError> {
    // Midnight never falls in the hour New York skips for daylight saving (02:00).
    let local = date.and_time(NaiveTime::MIN);
    new_york::to_utc(local)
        .ok_or_else(|| AlphaVantageError::decode(format!("{local} does not exist in New York")))
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use chrono::TimeZone;
    use rust_decimal_macros::dec;

    fn day(y: i32, m: u32, d: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(y, m, d).unwrap()
    }

    /// 00:00 New York on the given day.
    fn ny_midnight(y: i32, m: u32, d: u32) -> DateTime<Utc> {
        new_york::to_utc(day(y, m, d).and_time(NaiveTime::MIN)).unwrap()
    }

    fn observed() -> DateTime<Utc> {
        // 2026-11-24 07:00 New York.
        Utc.with_ymd_and_hms(2026, 11, 24, 12, 0, 0).unwrap()
    }

    /// Synthetic rows in Alpha Vantage's shape, newest first as it sends them.
    const BODY: &str = r#"{"symbol": "XYZ", "data": [
        {"ex_dividend_date": "2026-12-02", "declaration_date": "2026-11-17",
         "record_date": "2026-12-02", "payment_date": "2026-12-20", "amount": "0.25"},
        {"ex_dividend_date": "2026-08-14", "declaration_date": "2026-07-30",
         "record_date": "2026-08-14", "payment_date": "2026-08-28", "amount": "0.0"},
        {"ex_dividend_date": "2026-05-08", "declaration_date": "2026-04-28",
         "record_date": "2026-05-11", "payment_date": "2026-05-22", "amount": "0.0025"},
        {"ex_dividend_date": "2014-02-06", "declaration_date": "None",
         "record_date": "None", "payment_date": "None", "amount": "3.05"}
    ]}"#;

    #[test]
    fn rows_are_read_oldest_first_known_from_their_declaration_and_zero_rows_skipped() {
        let dividends = dividends(BODY, "XYZ", observed()).unwrap();
        let rows: Vec<_> = dividends
            .iter()
            .map(|d| {
                (
                    d.ex_date,
                    d.amount,
                    d.declaration_date,
                    d.record_date,
                    d.payment_date,
                    d.known_at,
                )
            })
            .collect();
        assert_eq!(
            rows,
            vec![
                // No declaration date: known from the start of its ex-date.
                (
                    day(2014, 2, 6),
                    dec!(3.05),
                    None,
                    None,
                    None,
                    ny_midnight(2014, 2, 6)
                ),
                (
                    day(2026, 5, 8),
                    dec!(0.0025),
                    Some(day(2026, 4, 28)),
                    Some(day(2026, 5, 11)),
                    Some(day(2026, 5, 22)),
                    ny_midnight(2026, 4, 29)
                ),
                // A declared coming dividend, known from the end of its declaration date.
                (
                    day(2026, 12, 2),
                    dec!(0.25),
                    Some(day(2026, 11, 17)),
                    Some(day(2026, 12, 2)),
                    Some(day(2026, 12, 20)),
                    ny_midnight(2026, 11, 18)
                ),
            ]
        );
        assert!(dividends.iter().all(|d| d.symbol == "XYZ"
            && d.source.provider == DividendProvider::AlphaVantage
            && d.source.observed_at == observed()
            && d.source.record_id.is_none()));
    }

    #[test]
    fn no_record_is_known_later_than_it_was_fetched() {
        // Declared on the fetch day, and a coming dividend with no declaration date.
        let body = r#"{"symbol": "XYZ", "data": [
            {"ex_dividend_date": "2026-12-10", "declaration_date": "2026-11-24",
             "record_date": "2026-12-10", "payment_date": "2026-12-28", "amount": "0.30"},
            {"ex_dividend_date": "2026-12-03", "declaration_date": "None",
             "record_date": "None", "payment_date": "None", "amount": "0.10"}
        ]}"#;
        let dividends = dividends(body, "XYZ", observed()).unwrap();
        assert_eq!(
            dividends.iter().map(|d| d.known_at).collect::<Vec<_>>(),
            [observed(), observed()]
        );
        // Fetched exactly at the end of the declaration date, the bound stands.
        let at_bound = super::dividends(body, "XYZ", ny_midnight(2026, 11, 25)).unwrap();
        assert_eq!(at_bound[1].known_at, ny_midnight(2026, 11, 25));
    }

    #[test]
    fn an_empty_list_is_no_dividends_and_missing_fields_read_as_none() {
        assert_eq!(
            dividends(r#"{"symbol": "NOPE", "data": []}"#, "NOPE", observed()),
            Ok(vec![])
        );
        let body = r#"{"data": [{"ex_dividend_date": "2026-02-03", "declaration_date": "",
            "amount": " 0.5 "}]}"#;
        let dividends = dividends(body, "XYZ", observed()).unwrap();
        assert_eq!(
            (
                dividends[0].amount,
                dividends[0].declaration_date,
                dividends[0].record_date,
                dividends[0].payment_date
            ),
            (dec!(0.5), None, None, None)
        );
    }

    #[test]
    fn a_bad_row_or_body_is_a_decode_error() {
        for bad in [
            "{}",
            "not json",
            r#"{"data": [{"ex_dividend_date": "None", "amount": "0.5"}]}"#,
            r#"{"data": [{"declaration_date": "2026-01-20", "amount": "0.5"}]}"#,
            r#"{"data": [{"ex_dividend_date": "2026-02-03"}]}"#,
            r#"{"data": [{"ex_dividend_date": "2026-02-03", "amount": "-0.5"}]}"#,
            r#"{"data": [{"ex_dividend_date": "2026-02-03", "amount": "None"}]}"#,
            r#"{"data": [{"ex_dividend_date": "2026-02-03", "amount": "0.5",
                "payment_date": "soon"}]}"#,
            r#"{"data": [{"ex_dividend_date": "2026-02-03", "amount": "0.5",
                "record_date": "2026-13-01"}]}"#,
            r#"{"data": [{"ex_dividend_date": "2026-02-03", "amount": "0.5",
                "declaration_date": "20260120"}]}"#,
        ] {
            assert!(
                matches!(
                    dividends(bad, "XYZ", observed()),
                    Err(AlphaVantageError::Decode { .. })
                ),
                "{bad}"
            );
        }
    }

    #[test]
    fn two_rows_on_one_ex_date_are_both_returned() {
        let body = r#"{"data": [
            {"ex_dividend_date": "2026-02-03", "amount": "0.5"},
            {"ex_dividend_date": "2026-02-03", "amount": "1.0"}
        ]}"#;
        let dividends = dividends(body, "XYZ", observed()).unwrap();
        assert_eq!(
            dividends.iter().map(|d| d.amount).collect::<Vec<_>>(),
            [dec!(0.5), dec!(1.0)]
        );
    }
}
