//! Decoding Alpha Vantage's earnings responses into [`EarningsRelease`]s.

use super::error::{AlphaVantageError, cap};
use crate::{
    earnings::{EarningsProvider, EarningsRelease, ReleaseSource, ReleaseStatus, ReleaseTiming},
    exchange::new_york,
};
use chrono::{DateTime, NaiveDate, NaiveTime, Utc};
use serde::Deserialize;

/// The keys of the notices Alpha Vantage sends in place of data, with HTTP 200. The first two
/// refuse a request (rate, daily allowance, premium-only call); the last reports an error in it.
const REFUSAL_KEYS: [&str; 2] = ["Information", "Note"];
const ERROR_KEY: &str = "Error Message";

/// The calendar's columns this client reads.
const SYMBOL: &str = "symbol";
const REPORT_DATE: &str = "reportDate";
const FISCAL_DATE_ENDING: &str = "fiscalDateEnding";
const TIME_OF_THE_DAY: &str = "timeOfTheDay";

/// The regular session's open in New York, by which a release before the open is public.
const SESSION_OPEN: NaiveTime = match NaiveTime::from_hms_opt(9, 30, 0) {
    Some(time) => time,
    None => unreachable!(),
};

/// The error for a JSON body that is one of Alpha Vantage's notices, if it is one.
pub(crate) fn notice(body: &str) -> Option<AlphaVantageError> {
    let object: serde_json::Map<String, serde_json::Value> = serde_json::from_str(body).ok()?;
    let text = |key: &str| {
        object.get(key).map(|value| match value {
            serde_json::Value::String(text) => cap(text),
            other => cap(&other.to_string()),
        })
    };
    if let Some(message) = text(ERROR_KEY) {
        return Some(AlphaVantageError::Api {
            status: 200,
            message,
        });
    }
    REFUSAL_KEYS
        .iter()
        .find_map(|key| text(key))
        .map(|message| AlphaVantageError::Refused { message })
}

#[derive(Deserialize)]
struct History {
    symbol: Option<String>,
    #[serde(rename = "quarterlyEarnings", default)]
    quarterly: Vec<Quarter>,
}

#[derive(Deserialize)]
struct Quarter {
    #[serde(rename = "fiscalDateEnding")]
    fiscal_date_ending: String,
    #[serde(rename = "reportedDate")]
    reported_date: String,
    #[serde(rename = "reportTime", default)]
    report_time: Option<String>,
}

/// The releases in an `EARNINGS` body for `symbol`, fetched at `observed_at`, oldest first.
///
/// A release whose session bound has passed by `observed_at` is [`ReleaseStatus::Reported`],
/// public by that bound: the open on its date if it came before the open, else the end of its
/// date. Any other release, dated later or on the fetch day before its bound, is not known to have
/// happened when fetched: [`ReleaseStatus::Unknown`], known at `observed_at`. So no record's
/// `known_at` is later than `observed_at`.
pub(crate) fn history(
    body: &str,
    symbol: &str,
    observed_at: DateTime<Utc>,
) -> Result<Vec<EarningsRelease>, AlphaVantageError> {
    let history: History = serde_json::from_str(body)
        .map_err(|e| AlphaVantageError::decode(format!("earnings history: {e}")))?;
    if history.symbol.is_none() && history.quarterly.is_empty() {
        return Err(AlphaVantageError::NotFound {
            symbol: symbol.to_owned(),
        });
    }
    let source = ReleaseSource::new(EarningsProvider::AlphaVantage, observed_at);
    let mut releases = history
        .quarterly
        .iter()
        .map(|quarter| {
            let date = date(&quarter.reported_date, "reportedDate")?;
            let timing = timing(quarter.report_time.as_deref().unwrap_or(""))?;
            let bound = session_bound(date, timing)?;
            let (status, known_at) = if bound <= observed_at {
                (ReleaseStatus::Reported, bound)
            } else {
                (ReleaseStatus::Unknown, observed_at)
            };
            Ok(EarningsRelease::new(
                symbol,
                self::date(&quarter.fiscal_date_ending, "fiscalDateEnding")?,
                date,
                timing,
                status,
                known_at,
                source.clone(),
            ))
        })
        .collect::<Result<Vec<_>, AlphaVantageError>>()?;
    releases.sort_by_key(|release| (release.date, release.fiscal_period_end));
    Ok(releases)
}

/// The releases in an `EARNINGS_CALENDAR` body fetched at `observed_at`, by date then symbol.
/// Each is [`ReleaseStatus::Unknown`] (the calendar does not say whether a date is announced or
/// estimated) and known at `observed_at`.
pub(crate) fn calendar(
    body: &str,
    observed_at: DateTime<Utc>,
) -> Result<Vec<EarningsRelease>, AlphaVantageError> {
    if body.trim_start().starts_with('{') {
        return Err(notice(body).unwrap_or_else(|| {
            AlphaVantageError::decode("earnings calendar: JSON in place of CSV")
        }));
    }
    // `flexible`: a notice cut into the columns has as many fields as its key has characters, up
    // to the header's count. A short ordinary row still fails, on its first missing field.
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .flexible(true)
        .from_reader(body.as_bytes());
    let headers = reader
        .headers()
        .map_err(|e| AlphaVantageError::decode(format!("earnings calendar header: {e}")))?
        .clone();
    let column = |name: &str| {
        headers
            .iter()
            .position(|header| header == name)
            .ok_or_else(|| {
                AlphaVantageError::decode(format!("earnings calendar has no {name:?} column"))
            })
    };
    let (symbol, report_date, fiscal_date_ending, time_of_the_day) = (
        column(SYMBOL)?,
        column(REPORT_DATE)?,
        column(FISCAL_DATE_ENDING)?,
        column(TIME_OF_THE_DAY)?,
    );
    let source = ReleaseSource::new(EarningsProvider::AlphaVantage, observed_at);
    let mut releases = Vec::new();
    for record in reader.records() {
        let record =
            record.map_err(|e| AlphaVantageError::decode(format!("earnings calendar row: {e}")))?;
        if let Some(error) = cut_notice(&record) {
            return Err(error);
        }
        let field = |index: usize| record.get(index).unwrap_or("");
        releases.push(EarningsRelease::new(
            field(symbol),
            date(field(fiscal_date_ending), FISCAL_DATE_ENDING)?,
            date(field(report_date), REPORT_DATE)?,
            timing(field(time_of_the_day))?,
            ReleaseStatus::Unknown,
            observed_at,
            source.clone(),
        ));
    }
    releases.sort_by(|a, b| (a.date, &a.symbol).cmp(&(b.date, &b.symbol)));
    Ok(releases)
}

/// The error for a calendar row that is a notice cut into the CSV's columns, if it is one.
///
/// Alpha Vantage answers a refused CSV request with the CSV header and one row holding the first
/// characters of the notice's JSON key, one per column (`I,n,f,o,r,m,a` for `Information`), and
/// nothing of the notice's text.
fn cut_notice(record: &csv::StringRecord) -> Option<AlphaVantageError> {
    if record.iter().any(|field| field.chars().count() != 1) {
        return None;
    }
    let joined: String = record.iter().collect();
    let message = || {
        format!(
            "a notice in place of the CSV, cut to {joined:?}; Alpha Vantage sends a refused \
             CSV request this way, for a request rate, a daily allowance or a premium-only call"
        )
    };
    if REFUSAL_KEYS.iter().any(|key| key.starts_with(&joined)) {
        Some(AlphaVantageError::Refused { message: message() })
    } else if ERROR_KEY.starts_with(&joined) {
        Some(AlphaVantageError::Api {
            status: 200,
            message: message(),
        })
    } else {
        None
    }
}

fn date(text: &str, field: &str) -> Result<NaiveDate, AlphaVantageError> {
    NaiveDate::parse_from_str(text, "%Y-%m-%d")
        .map_err(|_| AlphaVantageError::decode(format!("{field} {text:?} is not a date")))
}

/// The release timing Alpha Vantage's `reportTime` / `timeOfTheDay` names.
fn timing(text: &str) -> Result<ReleaseTiming, AlphaVantageError> {
    match text {
        "pre-market" => Ok(ReleaseTiming::BeforeOpen),
        "post-market" => Ok(ReleaseTiming::AfterClose),
        "" => Ok(ReleaseTiming::Unknown),
        other => Err(AlphaVantageError::decode(format!(
            "release time {other:?} is not pre-market, post-market or empty"
        ))),
    }
}

/// When a release dated `date` was public by: the open on `date` if it came before the
/// open, else the end of `date` (00:00 New York the next day), the latest the date allows.
fn session_bound(
    date: NaiveDate,
    timing: ReleaseTiming,
) -> Result<DateTime<Utc>, AlphaVantageError> {
    let local = match timing {
        ReleaseTiming::BeforeOpen => date.and_time(SESSION_OPEN),
        _ => date
            .succ_opt()
            .ok_or_else(|| {
                AlphaVantageError::decode(format!("reportedDate {date} has no next day"))
            })?
            .and_time(NaiveTime::MIN),
    };
    // Neither 09:30 nor 00:00 falls in the hour New York skips for daylight saving (02:00).
    new_york::to_utc(local)
        .ok_or_else(|| AlphaVantageError::decode(format!("{local} does not exist in New York")))
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use chrono::TimeZone;
    use smol_str::SmolStr;

    fn day(y: i32, m: u32, d: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(y, m, d).unwrap()
    }

    fn symbols(releases: &[EarningsRelease]) -> Vec<SmolStr> {
        releases
            .iter()
            .map(|release| release.symbol.clone())
            .collect()
    }

    fn observed() -> DateTime<Utc> {
        // 2026-11-13 15:00 New York.
        Utc.with_ymd_and_hms(2026, 11, 13, 20, 0, 0).unwrap()
    }

    #[test]
    fn notices_are_refusals_or_errors_and_data_is_neither() {
        assert_eq!(
            notice(r#"{"Information": "Thank you for using Alpha Vantage! 25 requests per day"}"#),
            Some(AlphaVantageError::Refused {
                message: "Thank you for using Alpha Vantage! 25 requests per day".into()
            })
        );
        assert_eq!(
            notice(r#"{"Note": "slow down"}"#),
            Some(AlphaVantageError::Refused {
                message: "slow down".into()
            })
        );
        assert_eq!(
            notice(r#"{"Error Message": "Invalid API call."}"#),
            Some(AlphaVantageError::Api {
                status: 200,
                message: "Invalid API call.".into()
            })
        );
        assert_eq!(
            notice(r#"{"symbol": "XYZ", "quarterlyEarnings": []}"#),
            None
        );
        assert_eq!(notice("{}"), None);
        assert_eq!(notice("symbol,name"), None);
    }

    #[test]
    fn history_rows_are_dated_timed_and_bounded_by_their_session() {
        let body = r#"{
            "symbol": "XYZ",
            "annualEarnings": [{"fiscalDateEnding": "2025-12-31", "reportedEPS": "1.0"}],
            "quarterlyEarnings": [
                {"fiscalDateEnding": "2026-12-31", "reportedDate": "2027-01-28",
                 "reportedEPS": "None", "estimatedEPS": "1.1", "surprise": "None",
                 "surprisePercentage": "None", "reportTime": "post-market"},
                {"fiscalDateEnding": "2026-09-30", "reportedDate": "2026-10-29",
                 "reportedEPS": "1.0", "estimatedEPS": "0.9", "surprise": "0.1",
                 "surprisePercentage": "11", "reportTime": "post-market"},
                {"fiscalDateEnding": "2026-06-30", "reportedDate": "2026-07-14",
                 "reportedEPS": "1.0", "estimatedEPS": "0.9", "surprise": "0.1",
                 "surprisePercentage": "11", "reportTime": "pre-market"},
                {"fiscalDateEnding": "2001-03-31", "reportedDate": "2001-04-17",
                 "reportedEPS": "0.1", "estimatedEPS": "0.1", "surprise": "0",
                 "surprisePercentage": "0"}
            ]
        }"#;
        let releases = history(body, "XYZ", observed()).unwrap();
        let rows: Vec<_> = releases
            .iter()
            .map(|r| (r.fiscal_period_end, r.date, r.timing, r.status, r.known_at))
            .collect();
        let ny = |y, m, d, h, min| {
            new_york::to_utc(day(y, m, d).and_hms_opt(h, min, 0).unwrap()).unwrap()
        };
        assert_eq!(
            rows,
            vec![
                // No report time: bounded by the end of its day, like a release after the close.
                (
                    day(2001, 3, 31),
                    day(2001, 4, 17),
                    ReleaseTiming::Unknown,
                    ReleaseStatus::Reported,
                    ny(2001, 4, 18, 0, 0)
                ),
                (
                    day(2026, 6, 30),
                    day(2026, 7, 14),
                    ReleaseTiming::BeforeOpen,
                    ReleaseStatus::Reported,
                    ny(2026, 7, 14, 9, 30)
                ),
                (
                    day(2026, 9, 30),
                    day(2026, 10, 29),
                    ReleaseTiming::AfterClose,
                    ReleaseStatus::Reported,
                    ny(2026, 10, 30, 0, 0)
                ),
                // Dated after the fetch: not yet reported, known only from the fetch.
                (
                    day(2026, 12, 31),
                    day(2027, 1, 28),
                    ReleaseTiming::AfterClose,
                    ReleaseStatus::Unknown,
                    observed()
                ),
            ]
        );
        assert!(releases.iter().all(|r| r.symbol == "XYZ"
            && r.source.provider == EarningsProvider::AlphaVantage
            && r.source.observed_at == observed()
            && r.source.record_id.is_none()));
    }

    #[test]
    fn a_release_on_the_fetch_day_is_reported_only_once_its_bound_has_passed() {
        // Fetched at 15:00 New York: past the open, not yet the end of the day.
        let body = r#"{"symbol": "XYZ", "quarterlyEarnings": [
            {"fiscalDateEnding": "2026-09-30", "reportedDate": "2026-11-13", "reportTime": "pre-market"},
            {"fiscalDateEnding": "2026-08-31", "reportedDate": "2026-11-13", "reportTime": "post-market"},
            {"fiscalDateEnding": "2026-07-31", "reportedDate": "2026-11-13", "reportTime": ""}
        ]}"#;
        let releases = history(body, "XYZ", observed()).unwrap();
        let open = new_york::to_utc(day(2026, 11, 13).and_time(SESSION_OPEN)).unwrap();
        let rows: Vec<_> = releases
            .iter()
            .map(|r| (r.timing, r.status, r.known_at))
            .collect();
        assert_eq!(
            rows,
            vec![
                (ReleaseTiming::Unknown, ReleaseStatus::Unknown, observed()),
                (
                    ReleaseTiming::AfterClose,
                    ReleaseStatus::Unknown,
                    observed()
                ),
                (ReleaseTiming::BeforeOpen, ReleaseStatus::Reported, open),
            ]
        );
        // At its bound exactly, a release is reported.
        let at_bound = history(body, "XYZ", open).unwrap();
        assert_eq!(
            at_bound.iter().map(|r| r.status).collect::<Vec<_>>(),
            vec![
                ReleaseStatus::Unknown,
                ReleaseStatus::Unknown,
                ReleaseStatus::Reported
            ]
        );
    }

    #[test]
    fn an_empty_history_is_not_found_and_a_bad_row_is_a_decode_error() {
        assert_eq!(
            history("{}", "NOPE", observed()),
            Err(AlphaVantageError::NotFound {
                symbol: "NOPE".into()
            })
        );
        // A known symbol with no quarterly rows is an empty history, not an unknown symbol.
        assert_eq!(
            history(
                r#"{"symbol": "FUND", "quarterlyEarnings": []}"#,
                "FUND",
                observed()
            ),
            Ok(vec![])
        );
        for bad in [
            r#"{"symbol": "X", "quarterlyEarnings": [{"fiscalDateEnding": "2026-09-30", "reportedDate": "None"}]}"#,
            r#"{"symbol": "X", "quarterlyEarnings": [{"fiscalDateEnding": "2026-09-30", "reportedDate": "2026-10-29", "reportTime": "midday"}]}"#,
            r#"{"symbol": "X", "quarterlyEarnings": [{"reportedDate": "2026-10-29"}]}"#,
            "not json",
        ] {
            assert!(
                matches!(
                    history(bad, "X", observed()),
                    Err(AlphaVantageError::Decode { .. })
                ),
                "{bad}"
            );
        }
    }

    const HEADER: &str =
        "symbol,name,reportDate,fiscalDateEnding,estimate,currency,timeOfTheDay\r\n";

    #[test]
    fn calendar_rows_read_quoted_names_and_each_timing() {
        let body = format!(
            "{HEADER}\
             BBB,\"Bee, Inc.\",2026-12-03,2026-10-31,1.2,USD,post-market\r\n\
             AAA,Aye Corp,2026-11-20,2026-09-30,,USD,\r\n\
             CCC,Cee Co,2026-11-20,2026-09-30,0.5,USD,pre-market\r\n"
        );
        let releases = calendar(&body, observed()).unwrap();
        assert_eq!(symbols(&releases), ["AAA", "CCC", "BBB"]);
        let rows: Vec<_> = releases
            .iter()
            .map(|r| (r.fiscal_period_end, r.date, r.timing, r.status, r.known_at))
            .collect();
        assert_eq!(
            rows,
            vec![
                (
                    day(2026, 9, 30),
                    day(2026, 11, 20),
                    ReleaseTiming::Unknown,
                    ReleaseStatus::Unknown,
                    observed()
                ),
                (
                    day(2026, 9, 30),
                    day(2026, 11, 20),
                    ReleaseTiming::BeforeOpen,
                    ReleaseStatus::Unknown,
                    observed()
                ),
                (
                    day(2026, 10, 31),
                    day(2026, 12, 3),
                    ReleaseTiming::AfterClose,
                    ReleaseStatus::Unknown,
                    observed()
                ),
            ]
        );
    }

    #[test]
    fn a_header_only_calendar_is_empty() {
        assert_eq!(calendar(HEADER, observed()), Ok(vec![]));
    }

    #[test]
    fn a_notice_cut_into_the_csv_or_sent_as_json_is_its_error() {
        let refused = calendar(&format!("{HEADER}I,n,f,o,r,m,a\r\n"), observed());
        assert!(
            matches!(refused, Err(AlphaVantageError::Refused { .. })),
            "{refused:?}"
        );
        let note = calendar(&format!("{HEADER}N,o,t,e\r\n"), observed());
        assert!(
            matches!(note, Err(AlphaVantageError::Refused { .. })),
            "{note:?}"
        );
        let error = calendar(&format!("{HEADER}E,r,r,o,r, ,M\r\n"), observed());
        assert!(
            matches!(error, Err(AlphaVantageError::Api { status: 200, .. })),
            "{error:?}"
        );
        assert_eq!(
            calendar(r#"{"Note": "slow down"}"#, observed()),
            Err(AlphaVantageError::Refused {
                message: "slow down".into()
            })
        );
        assert!(matches!(
            calendar(r#"{"unexpected": 1}"#, observed()),
            Err(AlphaVantageError::Decode { .. })
        ));
    }

    #[test]
    fn a_calendar_row_that_is_not_a_notice_must_decode() {
        // Single characters that spell no notice key are an ordinary, undecodable row.
        let body = format!("{HEADER}X,Y,Z,W,V,U,T\r\n");
        assert!(matches!(
            calendar(&body, observed()),
            Err(AlphaVantageError::Decode { .. })
        ));
        let short = format!("{HEADER}AAA,Aye,2026-11-20\r\n");
        assert!(matches!(
            calendar(&short, observed()),
            Err(AlphaVantageError::Decode { .. })
        ));
        let missing = "symbol,name,reportDate\r\nAAA,Aye,2026-11-20\r\n";
        assert!(matches!(
            calendar(missing, observed()),
            Err(AlphaVantageError::Decode { .. })
        ));
        let bad_timing = format!("{HEADER}AAA,Aye,2026-11-20,2026-09-30,,USD,midday\r\n");
        assert!(matches!(
            calendar(&bad_timing, observed()),
            Err(AlphaVantageError::Decode { .. })
        ));
    }
}
