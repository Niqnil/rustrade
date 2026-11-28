//! Cross-format test: one set of bars read as CSV and as Parquet must give the same candles.
//!
//! The CSV path (`LseClient::fetch_candles`) and the Parquet decoder (`parquet::read_export_rows`)
//! share their candle checks and their `close_time` rule, and convert a Parquet `f64` through its
//! shortest decimal text, which is how the provider spells the same number in CSV. This pins all
//! three: a bar must not change with the format it arrived in.
//!
//! Every value is **invented**, and both files are written by this test: no provider data is
//! committed (<https://londonstrategicedge.com/licence>). The Parquet schema is the measured one
//! (2026-11-28).
//!
//! Run with: `cargo test --test lse_parquet --features lse-parquet`

#![cfg(feature = "lse-parquet")]
#![allow(clippy::unwrap_used, clippy::expect_used)] // Test code: panics on bad input are acceptable

use chrono::{DateTime, NaiveDateTime, Utc};
use parquet::data_type::{DoubleType, Int64Type};
use parquet::file::properties::WriterProperties;
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::parser::parse_message_type;
use rustrade_data::event::DataKind;
use rustrade_data::exchange::lse::client::LseClient;
use rustrade_data::exchange::lse::export::LseExportData;
use rustrade_data::exchange::lse::historical::LseCandleRequest;
use rustrade_data::exchange::lse::market::LseMarketCode;
use rustrade_data::exchange::lse::parquet::read_export_rows;
use rustrade_data::subscription::candle::{Candle, CandleInterval};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use std::time::Duration;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// The measured candle schema.
const CANDLE_SCHEMA: &str = "message schema {
    REQUIRED INT64 time (TIMESTAMP(MILLIS, true));
    REQUIRED DOUBLE open; REQUIRED DOUBLE high; REQUIRED DOUBLE low; REQUIRED DOUBLE close;
    REQUIRED DOUBLE volume;
    REQUIRED INT64 trades (INTEGER(64, false));
}";

/// `(open time, open, high, low, close, volume, trades)` per bar, as the CSV would spell them.
///
/// The prices include sixteen significant digits, more than `Decimal::try_from(f64)` keeps, and a
/// fractional split-adjusted volume.
const BARS: [(&str, &str, &str, &str, &str, &str, u64); 3] = [
    (
        "2026-11-25 15:00:00",
        "101.25",
        "101.5",
        "100.75",
        "101",
        "1200",
        40,
    ),
    (
        "2026-11-25 15:01:00",
        "0.1234567890123456",
        "0.2",
        "0.1",
        "0.15",
        "12.5",
        3,
    ),
    (
        "2026-11-25 15:02:00",
        "1e-7",
        "0.0000002",
        "0.00000005",
        "0.0000001",
        "0",
        0,
    ),
];

fn at(text: &str) -> DateTime<Utc> {
    NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S")
        .unwrap()
        .and_utc()
}

fn csv() -> String {
    let mut body = String::from("time,open,high,low,close,volume,trades\n");
    for (time, open, high, low, close, volume, trades) in BARS {
        body.push_str(&format!(
            "{time},{open},{high},{low},{close},{volume},{trades}\n"
        ));
    }
    body
}

fn parquet_file() -> tempfile::NamedTempFile {
    let file = tempfile::NamedTempFile::new().unwrap();
    let mut writer = SerializedFileWriter::new(
        file.reopen().unwrap(),
        Arc::new(parse_message_type(CANDLE_SCHEMA).unwrap()),
        Arc::new(WriterProperties::builder().build()),
    )
    .unwrap();
    let mut group = writer.next_row_group().unwrap();

    let times: Vec<i64> = BARS
        .iter()
        .map(|bar| at(bar.0).timestamp_millis())
        .collect();
    let mut column = group.next_column().unwrap().unwrap();
    column
        .typed::<Int64Type>()
        .write_batch(&times, None, None)
        .unwrap();
    column.close().unwrap();

    let columns: [Vec<&str>; 5] = [
        BARS.iter().map(|bar| bar.1).collect(),
        BARS.iter().map(|bar| bar.2).collect(),
        BARS.iter().map(|bar| bar.3).collect(),
        BARS.iter().map(|bar| bar.4).collect(),
        BARS.iter().map(|bar| bar.5).collect(),
    ];
    for texts in columns {
        let values: Vec<f64> = texts.iter().map(|text| text.parse().unwrap()).collect();
        let mut column = group.next_column().unwrap().unwrap();
        column
            .typed::<DoubleType>()
            .write_batch(&values, None, None)
            .unwrap();
        column.close().unwrap();
    }

    let trades: Vec<i64> = BARS.iter().map(|bar| bar.6 as i64).collect();
    let mut column = group.next_column().unwrap().unwrap();
    column
        .typed::<Int64Type>()
        .write_batch(&trades, None, None)
        .unwrap();
    column.close().unwrap();

    group.close().unwrap();
    writer.close().unwrap();
    file
}

#[tokio::test]
async fn csv_and_parquet_give_the_same_candles() {
    let server = MockServer::start().await;
    let body = csv();
    Mock::given(method("GET"))
        .and(path("/v3/export"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header(
                    "x-checksum-sha256",
                    hex::encode(Sha256::digest(body.as_bytes())),
                )
                .set_body_raw(body, "text/csv; charset=utf-8"),
        )
        .mount(&server)
        .await;
    let client = LseClient::new("test-key")
        .unwrap()
        .with_base_url(format!("{}/v3", server.uri()))
        .with_pace(Duration::ZERO);

    let from_csv = client
        .collect_candles(LseCandleRequest::new(
            LseMarketCode::UsStocks,
            "TEST",
            CandleInterval::Min1,
            at("2026-11-25 15:01:00"),
            at("2026-11-25 15:03:00"),
        ))
        .await
        .unwrap();

    let file = parquet_file();
    let from_parquet: Vec<Candle> =
        read_export_rows(file.path(), LseExportData::Candles(CandleInterval::Min1))
            .unwrap()
            .map(|row| match row.unwrap() {
                (time, DataKind::Candle(candle)) => {
                    assert_eq!(time, candle.close_time);
                    candle
                }
                other => panic!("candle expected, got {other:?}"),
            })
            .collect();

    assert_eq!(from_csv.len(), BARS.len());
    assert_eq!(from_parquet, from_csv);
}
