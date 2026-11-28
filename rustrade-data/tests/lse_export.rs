//! Download tests for LSE Data 3.0 exports: verification, placement, cleanup.
//!
//! Every response here is **synthetic**, shaped from measurements of the live API (2026-11-28).
//! No provider data is committed — the provider prohibits redistribution
//! (<https://londonstrategicedge.com/licence>), so `wiremock` is the only in-repo option.
//!
//! What a change to `export.rs` must not regress:
//!
//! - A file is verified against its SHA-256 header before any of it is returned or placed.
//! - A download leaves either a whole, verified file at its destination or the destination as it
//!   was, and never a `.part` file, whether it fails, finds nothing or is cancelled.
//! - Refusing to replace an existing file spends no request.
//!
//! Run with: `cargo test --test lse_export --features lse`

#![cfg(feature = "lse")]
#![allow(clippy::unwrap_used, clippy::expect_used)] // Test code: panics on bad input are acceptable

use chrono::{TimeZone, Utc};
use rustrade_data::exchange::lse::client::LseClient;
use rustrade_data::exchange::lse::error::LseError;
use rustrade_data::exchange::lse::export::{
    LseExistingFile, LseExportData, LseExportFormat, LseExportRequest,
};
use rustrade_data::exchange::lse::historical::LseWindow;
use rustrade_data::exchange::lse::market::LseMarketCode;
use rustrade_data::subscription::candle::CandleInterval;
use sha2::{Digest, Sha256};
use std::path::Path;
use std::time::Duration;
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// A synthetic file body: what it holds does not matter to the download, only its bytes.
const BODY: &[u8] = b"PAR1 synthetic export body, not real data PAR1";

fn client(server: &MockServer) -> LseClient {
    LseClient::new("test-key")
        .unwrap()
        .with_base_url(format!("{}/v3", server.uri()))
        .with_pace(Duration::ZERO)
}

fn request() -> LseExportRequest {
    LseExportRequest::new(
        LseMarketCode::UsStocks,
        "TEST",
        LseExportData::Candles(CandleInterval::Min1),
        LseWindow::Times {
            from: Utc.with_ymd_and_hms(2026, 11, 25, 15, 0, 0).unwrap(),
            to: Utc.with_ymd_and_hms(2026, 11, 25, 16, 0, 0).unwrap(),
        },
    )
}

/// A file answer as the provider sends one: body, checksum, rows, id, name and adjustment.
fn file_answer(body: &[u8]) -> ResponseTemplate {
    ResponseTemplate::new(200)
        .insert_header("x-checksum-sha256", hex::encode(Sha256::digest(body)))
        .insert_header("x-export-rows", "60")
        .insert_header("x-export-id", "export-7")
        .insert_header("x-lse3-adjusted", "1")
        .insert_header(
            "content-disposition",
            "attachment; filename=\"TEST_us_stocks_1m.parquet\"",
        )
        .set_body_raw(body.to_vec(), "application/vnd.apache.parquet")
}

async fn mount(server: &MockServer, answer: ResponseTemplate) {
    Mock::given(method("GET"))
        .and(path("/v3/export"))
        .respond_with(answer)
        .mount(server)
        .await;
}

/// The files in `directory`, by name.
fn files(directory: &Path) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(directory)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    names.sort();
    names
}

#[tokio::test]
async fn fetch_export_sends_the_selection_and_returns_the_verified_body_and_headers() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v3/export"))
        .and(query_param("market", "us_stocks"))
        .and(query_param("symbol", "TEST"))
        .and(query_param("data", "candles"))
        .and(query_param("timeframe", "1m"))
        .and(query_param("from", "2026-11-25 15:00:00"))
        .and(query_param("to", "2026-11-25 16:00:00"))
        .and(query_param("format", "parquet"))
        .and(query_param("adjusted", "1"))
        .respond_with(file_answer(BODY))
        .expect(1)
        .mount(&server)
        .await;

    let export = client(&server)
        .fetch_export(&request())
        .await
        .unwrap()
        .unwrap();

    assert_eq!(export.body, BODY);
    assert_eq!(export.request, request());
    assert_eq!(export.info.bytes, BODY.len() as u64);
    assert_eq!(export.info.sha256, hex::encode(Sha256::digest(BODY)));
    assert_eq!(export.info.rows_charged, Some(60));
    assert_eq!(export.info.export_id.as_deref(), Some("export-7"));
    assert_eq!(
        export.info.file_name.as_deref(),
        Some("TEST_us_stocks_1m.parquet")
    );
    assert_eq!(export.info.adjusted, Some(true));
}

#[tokio::test]
async fn a_body_failing_its_checksum_is_refused() {
    let server = MockServer::start().await;
    mount(
        &server,
        file_answer(BODY).insert_header("x-checksum-sha256", hex::encode(Sha256::digest(b"x"))),
    )
    .await;

    let error = client(&server).fetch_export(&request()).await.unwrap_err();

    assert!(
        matches!(error, LseError::IntegrityMismatch { .. }),
        "{error}"
    );
}

#[tokio::test]
async fn an_answer_without_a_checksum_is_refused() {
    let server = MockServer::start().await;
    mount(
        &server,
        ResponseTemplate::new(200).set_body_raw(BODY.to_vec(), "application/vnd.apache.parquet"),
    )
    .await;

    let error = client(&server).fetch_export(&request()).await.unwrap_err();

    assert!(matches!(error, LseError::MissingChecksum), "{error}");
}

#[tokio::test]
async fn download_places_the_verified_file_and_leaves_no_part_file() {
    let server = MockServer::start().await;
    mount(&server, file_answer(BODY)).await;
    let directory = tempfile::tempdir().unwrap();
    let destination = directory.path().join(request().file_name());

    let file = client(&server)
        .download_export(&request(), &destination, LseExistingFile::Refuse)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(file.path, destination);
    assert_eq!(file.info.bytes, BODY.len() as u64);
    assert_eq!(std::fs::read(&destination).unwrap(), BODY);
    assert_eq!(files(directory.path()), [request().file_name()]);
}

/// An answer, and a test of the error it must produce.
type Failure = (ResponseTemplate, fn(&LseError) -> bool);

#[tokio::test]
async fn a_failed_download_leaves_neither_a_file_nor_a_part_file() {
    let directory = tempfile::tempdir().unwrap();
    let destination = directory.path().join("export.parquet");
    let failures: [Failure; 5] = [
        (
            file_answer(BODY).insert_header("x-checksum-sha256", hex::encode(Sha256::digest(b"x"))),
            |error| matches!(error, LseError::IntegrityMismatch { .. }),
        ),
        (
            ResponseTemplate::new(200).set_body_raw(BODY.to_vec(), "application/octet-stream"),
            |error| matches!(error, LseError::MissingChecksum),
        ),
        (
            file_answer(BODY).insert_header("x-lse3-adjusted", "0"),
            |error| matches!(error, LseError::ExportMismatch { .. }),
        ),
        (
            ResponseTemplate::new(429)
                .insert_header("retry-after", "5")
                .set_body_raw(
                    r#"{"code": "connections-exhausted", "message": "One download at a time."}"#,
                    "application/json",
                ),
            |error| matches!(error, LseError::RateLimited { .. }),
        ),
        (
            ResponseTemplate::new(413).set_body_raw(
                r#"{"code": "export-too-wide", "message": "Choose shorter dates."}"#,
                "application/json",
            ),
            |error| matches!(error, LseError::Api { status: 413, .. }),
        ),
    ];

    for (failure, expected) in failures {
        let server = MockServer::start().await;
        mount(&server, failure).await;

        let error = client(&server)
            .download_export(&request(), &destination, LseExistingFile::Refuse)
            .await
            .unwrap_err();

        assert!(expected(&error), "unexpected error: {error:?}");
        assert!(files(directory.path()).is_empty(), "{error}: left files");
    }
}

#[tokio::test]
async fn a_404_with_another_code_is_a_refusal_not_nothing_stored() {
    let server = MockServer::start().await;
    mount(
        &server,
        ResponseTemplate::new(404).set_body_raw(
            r#"{"code": "symbol-unknown", "message": "symbol is not available"}"#,
            "application/json",
        ),
    )
    .await;

    let error = client(&server).fetch_export(&request()).await.unwrap_err();

    assert!(
        matches!(
            &error,
            LseError::Api { status: 404, code: Some(code), .. } if code == "symbol-unknown"
        ),
        "{error:?}"
    );
}

#[tokio::test]
async fn nothing_stored_is_none_and_leaves_nothing_behind() {
    let server = MockServer::start().await;
    mount(
        &server,
        ResponseTemplate::new(404).set_body_raw(
            r#"{"code": "data-unavailable", "message": "No stored data for this selection."}"#,
            "application/json",
        ),
    )
    .await;
    let directory = tempfile::tempdir().unwrap();
    let destination = directory.path().join("export.parquet");

    let client = client(&server);
    assert!(client.fetch_export(&request()).await.unwrap().is_none());
    assert!(
        client
            .download_export(&request(), &destination, LseExistingFile::Refuse)
            .await
            .unwrap()
            .is_none()
    );
    assert!(files(directory.path()).is_empty());
}

#[tokio::test]
async fn refusing_an_existing_destination_spends_no_request() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v3/export"))
        .respond_with(file_answer(BODY))
        .expect(0)
        .mount(&server)
        .await;
    let directory = tempfile::tempdir().unwrap();
    let destination = directory.path().join("export.parquet");
    std::fs::write(&destination, b"kept").unwrap();

    let error = client(&server)
        .download_export(&request(), &destination, LseExistingFile::Refuse)
        .await
        .unwrap_err();

    assert!(
        matches!(error, LseError::DestinationExists { .. }),
        "{error}"
    );
    assert_eq!(std::fs::read(&destination).unwrap(), b"kept");
}

#[tokio::test]
async fn replacing_keeps_the_old_file_until_the_new_one_is_verified() {
    let directory = tempfile::tempdir().unwrap();
    let destination = directory.path().join("export.parquet");
    std::fs::write(&destination, b"old copy").unwrap();

    let damaged = MockServer::start().await;
    mount(
        &damaged,
        file_answer(BODY).insert_header("x-checksum-sha256", hex::encode(Sha256::digest(b"x"))),
    )
    .await;
    client(&damaged)
        .download_export(&request(), &destination, LseExistingFile::Replace)
        .await
        .unwrap_err();
    assert_eq!(std::fs::read(&destination).unwrap(), b"old copy");

    let healthy = MockServer::start().await;
    mount(&healthy, file_answer(BODY)).await;
    client(&healthy)
        .download_export(&request(), &destination, LseExistingFile::Replace)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(std::fs::read(&destination).unwrap(), BODY);
    assert_eq!(files(directory.path()), ["export.parquet"]);
}

#[tokio::test]
async fn a_stale_part_file_is_overwritten() {
    let server = MockServer::start().await;
    mount(&server, file_answer(BODY)).await;
    let directory = tempfile::tempdir().unwrap();
    let destination = directory.path().join("export.parquet");
    std::fs::write(
        directory.path().join("export.parquet.part"),
        b"left by a process that died mid-download, and longer than the new body",
    )
    .unwrap();

    client(&server)
        .download_export(&request(), &destination, LseExistingFile::Refuse)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(std::fs::read(&destination).unwrap(), BODY);
    assert_eq!(files(directory.path()), ["export.parquet"]);
}

#[tokio::test]
async fn a_cancelled_download_removes_its_part_file() {
    let server = MockServer::start().await;
    mount(
        &server,
        file_answer(BODY).set_delay(Duration::from_secs(30)),
    )
    .await;
    let directory = tempfile::tempdir().unwrap();
    let destination = directory.path().join("export.parquet");
    let client = client(&server);

    let request = request();
    let download = client.download_export(&request, &destination, LseExistingFile::Refuse);
    let cut = tokio::time::timeout(Duration::from_millis(200), download).await;

    assert!(cut.is_err(), "the download should still have been waiting");
    assert!(files(directory.path()).is_empty());
}

#[tokio::test]
async fn a_csv_export_asks_for_csv() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v3/export"))
        .and(query_param("format", "csv"))
        .respond_with(file_answer(b"time,price\n"))
        .expect(1)
        .mount(&server)
        .await;

    let export = client(&server)
        .fetch_export(&request().with_format(LseExportFormat::Csv))
        .await
        .unwrap()
        .unwrap();

    assert_eq!(export.body, b"time,price\n");
}
