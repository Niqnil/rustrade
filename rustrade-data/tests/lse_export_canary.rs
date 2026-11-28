//! LSE Data 3.0 export canary (network + credential gated).
//!
//! # Why this exists
//!
//! Every in-repo test of the export path runs on synthetic data — `wiremock` for the download,
//! Parquet written by the `parquet` crate's own writer for the decoder — because the provider
//! prohibits passing its data on (<https://londonstrategicedge.com/licence>), so no downloaded file
//! may be committed. Those tests check the code against *our own assumptions* about the provider's
//! files. This canary checks those assumptions against the **real API**:
//!
//! - a download verifies (checksum, length) and lands whole, with no `.part` left;
//! - the Parquet schema still decodes: candles in milliseconds, the trade slice in nanoseconds,
//!   every trade-slice row of kind `trade`;
//! - **the same window decodes to the same candles from CSV and from Parquet**, which is what the
//!   decoder's number conversion claims;
//! - a window with nothing stored is `None`, not an error and not an empty file.
//!
//! Downloads go to a temporary directory that is deleted at the end: nothing is kept.
//!
//! # Skip vs. fail contract
//!
//! - `LSE_API_KEY` **unset** → **SKIP** (logged, test passes), so CI without secrets stays green.
//! - `LSE_API_KEY` set but unusable → **FAIL**.
//! - Key present but an assertion fails → **FAIL** (the real signal).
//!
//! Every test is `#[serial]`: the provider holds one download open per key and refuses a second.
//!
//! # Running
//!
//! ```bash
//! set -a && . ./.env && set +a
//! cargo test --test lse_export_canary --features lse-parquet -- --ignored --nocapture
//! ```
//!
//! Marked `#[ignore]` so a default test run never spends the key's allowance. Windows are kept to
//! minutes, since the allowance is shared with the key's real use.

#![cfg(feature = "lse-parquet")]
#![allow(clippy::unwrap_used, clippy::expect_used)] // Test code: panics on bad input are acceptable

use chrono::{DateTime, Datelike, Duration, NaiveDate, NaiveTime, Utc, Weekday};
use rustrade_data::event::DataKind;
use rustrade_data::exchange::lse::client::LseClient;
use rustrade_data::exchange::lse::export::{
    LseExistingFile, LseExportData, LseExportFile, LseExportFormat, LseExportRequest,
};
use rustrade_data::exchange::lse::historical::{LseCandleRequest, LseWindow};
use rustrade_data::exchange::lse::market::LseMarketCode;
use rustrade_data::exchange::lse::parquet::read_export_rows;
use rustrade_data::subscription::candle::{Candle, CandleInterval};
use serial_test::serial;
use std::path::Path;

const KEY_ENV: &str = "LSE_API_KEY";

/// Build a client, or `None` when the key is **absent** (skip rather than fail).
fn client() -> Option<LseClient> {
    if std::env::var_os(KEY_ENV).is_none() {
        println!("CANARY_SKIP: {KEY_ENV} is not set - skipping");
        return None;
    }

    Some(
        LseClient::from_env()
            .unwrap_or_else(|error| panic!("{KEY_ENV} is set but unusable: {error}")),
    )
}

/// The three most recent Tuesdays to Thursdays at least two days back, newest first: full US
/// sessions, long ingested. More than one, because any of them can be a market holiday
/// (Thanksgiving is a Thursday), which the provider answers with nothing stored.
fn settled_midweek_days() -> Vec<NaiveDate> {
    let mut day = Utc::now().date_naive() - Duration::days(2);
    let mut days = Vec::new();
    while days.len() < 3 {
        if matches!(day.weekday(), Weekday::Tue | Weekday::Wed | Weekday::Thu) {
            days.push(day);
        }
        day -= Duration::days(1);
    }
    days
}

/// Download `request(day)` for the first settled midweek day the provider stores data for.
async fn download_first_trading_day(
    client: &LseClient,
    directory: &Path,
    request: impl Fn(NaiveDate) -> LseExportRequest,
) -> (NaiveDate, LseExportFile) {
    for day in settled_midweek_days() {
        let request = request(day);
        let destination = directory.join(request.file_name());
        match client
            .download_export(&request, &destination, LseExistingFile::Refuse)
            .await
            .unwrap()
        {
            Some(file) => return (day, file),
            None => {
                println!("CANARY: nothing stored for {day} (a holiday?); trying an earlier day")
            }
        }
    }
    panic!(
        "none of the last three settled midweek days has data: {:?}",
        settled_midweek_days()
    );
}

fn at(day: NaiveDate, hour: u32, minute: u32) -> DateTime<Utc> {
    day.and_time(NaiveTime::from_hms_opt(hour, minute, 0).unwrap())
        .and_utc()
}

fn decode_candles(path: &Path, interval: CandleInterval) -> Vec<Candle> {
    read_export_rows(path, LseExportData::Candles(interval))
        .unwrap()
        .map(|row| match row.unwrap() {
            (time, DataKind::Candle(candle)) => {
                assert_eq!(time, candle.close_time);
                candle
            }
            other => panic!("candle expected, got {other:?}"),
        })
        .collect()
}

#[tokio::test]
#[ignore = "spends the key's allowance; run on demand"]
#[serial]
async fn minute_candles_download_verified_and_match_the_csv_path() {
    let Some(client) = client() else { return };
    let directory = tempfile::tempdir().unwrap();
    // 15:00-15:30 UTC: inside the US regular session all year.
    let (day, file) = download_first_trading_day(&client, directory.path(), |day| {
        LseExportRequest::new(
            LseMarketCode::UsStocks,
            "AAPL",
            LseExportData::Candles(CandleInterval::Min1),
            LseWindow::Times {
                from: at(day, 15, 0),
                to: at(day, 15, 30),
            },
        )
    })
    .await;
    let (from, to) = (at(day, 15, 0), at(day, 15, 30));
    let destination = file.path.clone();
    println!(
        "CANARY: {} bytes, {:?} rows charged, file name {:?}",
        file.info.bytes, file.info.rows_charged, file.info.file_name
    );
    assert_eq!(
        std::fs::read_dir(directory.path()).unwrap().count(),
        1,
        "a .part file was left beside the download"
    );

    let from_parquet = decode_candles(&destination, CandleInterval::Min1);
    assert!(
        (20..=30).contains(&from_parquet.len()),
        "{} one-minute bars in a regular-session half hour",
        from_parquet.len()
    );
    assert!(
        from_parquet
            .iter()
            .all(|candle| candle.close_time > from && candle.close_time <= to),
        "a bar closes outside the window it was asked for"
    );

    // The same bars through the CSV path, whose window is the same half hour by close.
    let from_csv = client
        .collect_candles(LseCandleRequest::new(
            LseMarketCode::UsStocks,
            "AAPL",
            CandleInterval::Min1,
            from + Duration::minutes(1),
            to,
        ))
        .await
        .unwrap();
    assert_eq!(
        from_parquet, from_csv,
        "the same window decodes differently from Parquet and from CSV"
    );
}

#[tokio::test]
#[ignore = "spends the key's allowance; run on demand"]
#[serial]
async fn the_trade_slice_decodes_as_trades_to_the_nanosecond() {
    let Some(client) = client() else { return };
    let directory = tempfile::tempdir().unwrap();
    let (_, file) = download_first_trading_day(&client, directory.path(), |day| {
        LseExportRequest::new(
            LseMarketCode::UsStocks,
            "AAPL",
            LseExportData::TradeSlice,
            LseWindow::Times {
                from: at(day, 15, 0),
                to: at(day, 15, 1),
            },
        )
    })
    .await;
    let destination = file.path;

    let mut trades = 0;
    let mut sub_millisecond = false;
    for row in read_export_rows(&destination, LseExportData::TradeSlice).unwrap() {
        let (time, kind) = row.unwrap();
        assert!(matches!(kind, DataKind::Trade(_)), "{kind:?}");
        sub_millisecond |= time.timestamp_subsec_nanos() % 1_000_000 != 0;
        trades += 1;
    }
    println!("CANARY: {trades} trades in one minute");
    assert!(
        trades > 100,
        "only {trades} AAPL trades in a session minute"
    );
    assert!(
        sub_millisecond,
        "no trade time below the millisecond: the slice is no longer in nanoseconds"
    );
}

#[tokio::test]
#[ignore = "spends the key's allowance; run on demand"]
#[serial]
async fn a_window_with_nothing_stored_is_none() {
    let Some(client) = client() else { return };
    let mut saturday = settled_midweek_days()[0];
    while saturday.weekday() != Weekday::Sat {
        saturday -= Duration::days(1);
    }
    let request = LseExportRequest::new(
        LseMarketCode::UsStocks,
        "AAPL",
        LseExportData::Candles(CandleInterval::Min1),
        LseWindow::Times {
            from: at(saturday, 15, 0),
            to: at(saturday, 15, 30),
        },
    )
    .with_format(LseExportFormat::Csv);

    let export = client.fetch_export(&request).await.unwrap();

    assert!(export.is_none(), "a Saturday window returned a file");
}
