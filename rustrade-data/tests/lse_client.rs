//! Transport, chunking and range-contract tests for the LSE Data 3.0 client.
//!
//! Every response here is **synthetic**, shaped from measurements of the live API (2026-11-27). No
//! provider data is committed — the provider prohibits redistribution
//! (<https://londonstrategicedge.com/licence>), so `wiremock` is the only in-repo option. The live
//! API is exercised separately by the shape canary, which asserts against the real service without
//! storing anything.
//!
//! Run with: `cargo test --test lse_client --features lse`

#![cfg(feature = "lse")]
#![allow(clippy::unwrap_used, clippy::expect_used)] // Test code: panics on bad input are acceptable

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use rustrade_data::exchange::lse::catalogue::LsePairSearch;
use rustrade_data::exchange::lse::client::LseClient;
use rustrade_data::exchange::lse::error::LseError;
use rustrade_data::exchange::lse::historical::LseCandleRequest;
use rustrade_data::exchange::lse::market::LseMarketCode;
use rustrade_data::subscription::candle::{Candle, CandleInterval};
use sha2::{Digest, Sha256};
use std::num::NonZeroU32;
use std::time::Duration;
use wiremock::matchers::{header, method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

const CANDLE_HEADER: &str = "time,open,high,low,close,volume,trades\n";

fn client(server: &MockServer) -> LseClient {
    LseClient::new("test-key")
        .unwrap()
        .with_base_url(format!("{}/v3", server.uri()))
        .with_pace(Duration::ZERO)
}

fn utc(raw: &str) -> DateTime<Utc> {
    raw.parse().unwrap()
}

/// A verified CSV answer, as the provider sends a file: SHA-256 and data row count in headers.
fn csv_answer(body: &str) -> ResponseTemplate {
    let rows = body.lines().count().saturating_sub(1);
    ResponseTemplate::new(200)
        .insert_header(
            "x-checksum-sha256",
            hex::encode(Sha256::digest(body.as_bytes())),
        )
        .insert_header("x-export-rows", rows.to_string())
        .set_body_raw(body.to_owned(), "text/csv; charset=utf-8")
}

/// A candle CSV of one-minute bars opening at each `HH:MM` on 2026-11-20.
fn minutes(opens: &[&str]) -> String {
    let mut body = String::from(CANDLE_HEADER);
    for open in opens {
        body.push_str(&format!("2026-11-20 {open}:00,1.5,2,1,1.75,10,3\n"));
    }
    body
}

fn request(interval: CandleInterval, start: &str, end: &str) -> LseCandleRequest {
    LseCandleRequest::new(
        LseMarketCode::UsStocks,
        "AAPL",
        interval,
        utc(start),
        utc(end),
    )
}

async fn mount_window(server: &MockServer, from: &str, to: &str, body: String) {
    Mock::given(method("GET"))
        .and(path("/v3/export"))
        .and(query_param("from", from))
        .and(query_param("to", to))
        .respond_with(csv_answer(&body))
        .expect(1)
        .mount(server)
        .await;
}

fn closes(candles: &[Candle]) -> Vec<DateTime<Utc>> {
    candles.iter().map(|candle| candle.close_time).collect()
}

// -----------------------------------------------------------------------------------------------
// The request
// -----------------------------------------------------------------------------------------------

/// Every parameter the selection needs is sent, with the key in its header and the window on the
/// interval's grid even though the range is not.
#[tokio::test]
async fn a_candle_request_sends_the_whole_selection_on_an_aligned_window() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v3/export"))
        .and(header("x-api-key", "test-key"))
        .and(query_param("market", "us_stocks"))
        .and(query_param("symbol", "AAPL"))
        .and(query_param("data", "candles"))
        .and(query_param("timeframe", "1h"))
        .and(query_param("from", "2026-11-20 13:00:00"))
        .and(query_param("to", "2026-11-20 16:00:00"))
        .and(query_param("format", "csv"))
        .and(query_param("adjusted", "1"))
        .respond_with(csv_answer(CANDLE_HEADER))
        .expect(1)
        .mount(&server)
        .await;

    let candles = client(&server)
        .collect_candles(request(
            CandleInterval::Hour1,
            "2026-11-20T14:30:00Z",
            "2026-11-20T16:30:00Z",
        ))
        .await
        .unwrap();

    assert!(candles.is_empty());
}

#[tokio::test]
async fn as_published_asks_for_unadjusted_prices() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v3/export"))
        .and(query_param("adjusted", "0"))
        .respond_with(csv_answer(CANDLE_HEADER))
        .expect(1)
        .mount(&server)
        .await;

    client(&server)
        .collect_candles(
            request(
                CandleInterval::Min1,
                "2026-11-20T15:00:00Z",
                "2026-11-20T15:01:00Z",
            )
            .as_published(),
        )
        .await
        .unwrap();
}

/// A daily fetch asks by whole dates, both ends inclusive, widened a day at the top for a first
/// trade on the evening before its session date: closes in 11-19..11-20 open in 11-18..11-19, so
/// their session dates run 11-18..11-20.
#[tokio::test]
async fn a_daily_request_asks_by_dates_and_trims_on_close() {
    let server = MockServer::start().await;
    // Forex-shaped: each bar opens at 22:04 the evening before its session date.
    let body = "time,open,high,low,close,volume,trades,date\n\
        2026-11-17 22:04:00,1,1,1,1,5,5,2026-11-18\n\
        2026-11-18 22:04:00,1,1,1,1,5,5,2026-11-19\n\
        2026-11-19 22:04:00,1,1,1,1,5,5,2026-11-20\n";
    mount_window(&server, "2026-11-18", "2026-11-20", body.to_owned()).await;

    let candles = client(&server)
        .collect_candles(LseCandleRequest::new(
            LseMarketCode::Forex,
            "EUR_USD",
            CandleInterval::Day1,
            utc("2026-11-19T00:00:00Z"),
            utc("2026-11-20T00:00:00Z"),
        ))
        .await
        .unwrap();

    // Closes 11-18 22:04, 11-19 22:04 and 11-20 22:04: only the middle one is in range.
    assert_eq!(closes(&candles), [utc("2026-11-19T22:04:00Z")]);
}

#[tokio::test]
async fn an_inverted_range_fails_before_any_request_is_sent() {
    let server = MockServer::start().await;

    let error = client(&server)
        .collect_candles(request(
            CandleInterval::Min1,
            "2026-11-20T16:00:00Z",
            "2026-11-20T15:00:00Z",
        ))
        .await
        .unwrap_err();

    assert!(matches!(error, LseError::InvalidInput { .. }), "{error:?}");
    assert!(server.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn an_unserved_resolution_is_rejected_before_any_request_is_sent() {
    let server = MockServer::start().await;

    let error = client(&server)
        .collect_candles(request(
            CandleInterval::Hour2,
            "2026-11-20T10:00:00Z",
            "2026-11-20T16:00:00Z",
        ))
        .await
        .unwrap_err();

    assert!(
        matches!(error, LseError::UnsupportedInterval { .. }),
        "{error:?}"
    );
    assert!(server.received_requests().await.unwrap().is_empty());
}

// -----------------------------------------------------------------------------------------------
// Chunks
// -----------------------------------------------------------------------------------------------

/// Chunks tile the range on the grid, so consecutive chunks meet with neither gap nor overlap.
#[tokio::test]
async fn chunks_seam_without_gap_or_duplicate() {
    let server = MockServer::start().await;
    mount_window(
        &server,
        "2026-11-20 15:00:00",
        "2026-11-20 15:02:00",
        minutes(&["15:00", "15:01"]),
    )
    .await;
    mount_window(
        &server,
        "2026-11-20 15:02:00",
        "2026-11-20 15:04:00",
        minutes(&["15:02", "15:03"]),
    )
    .await;

    let candles = client(&server)
        .with_chunk_bars(NonZeroU32::new(2).unwrap())
        .collect_candles(request(
            CandleInterval::Min1,
            "2026-11-20T15:01:00Z",
            "2026-11-20T15:04:00Z",
        ))
        .await
        .unwrap();

    assert_eq!(
        closes(&candles),
        [
            utc("2026-11-20T15:01:00Z"),
            utc("2026-11-20T15:02:00Z"),
            utc("2026-11-20T15:03:00Z"),
            utc("2026-11-20T15:04:00Z"),
        ]
    );
    assert_eq!(candles[0].trade_count, Some(3));
    assert_eq!(candles[0].volume, Some(Decimal::from(10)));
}

/// A bar opening outside its chunk's window means the window was not honoured: nothing of that
/// chunk is yielded, though earlier chunks were.
#[tokio::test]
async fn a_bar_outside_its_window_fails_its_chunk_before_any_of_it_is_yielded() {
    let server = MockServer::start().await;
    mount_window(
        &server,
        "2026-11-20 15:00:00",
        "2026-11-20 15:02:00",
        minutes(&["15:00", "15:01"]),
    )
    .await;
    mount_window(
        &server,
        "2026-11-20 15:02:00",
        "2026-11-20 15:04:00",
        minutes(&["15:02", "15:05"]),
    )
    .await;

    let client = client(&server).with_chunk_bars(NonZeroU32::new(2).unwrap());
    let results: Vec<_> = futures::StreamExt::collect(client.fetch_candles(request(
        CandleInterval::Min1,
        "2026-11-20T15:01:00Z",
        "2026-11-20T15:04:00Z",
    )))
    .await;

    assert_eq!(results.len(), 3, "{results:?}");
    assert!(results[..2].iter().all(Result::is_ok));
    assert!(
        matches!(results[2], Err(LseError::UnexpectedCandleRange { .. })),
        "{:?}",
        results[2]
    );
}

#[tokio::test]
async fn bars_stepping_backwards_are_a_terminal_error() {
    let server = MockServer::start().await;
    mount_window(
        &server,
        "2026-11-20 15:00:00",
        "2026-11-20 15:04:00",
        minutes(&["15:02", "15:01"]),
    )
    .await;

    let error = client(&server)
        .collect_candles(request(
            CandleInterval::Min1,
            "2026-11-20T15:01:00Z",
            "2026-11-20T15:04:00Z",
        ))
        .await
        .unwrap_err();

    assert!(
        matches!(error, LseError::NonMonotonicCandles { .. }),
        "{error:?}"
    );
}

/// Bars closer than the interval asked for are a finer resolution than requested.
#[tokio::test]
async fn bars_spaced_closer_than_the_interval_are_rejected_and_a_wider_gap_is_not() {
    let server = MockServer::start().await;
    let body = "time,open,high,low,close,volume,trades\n\
        2026-11-20 13:00:00,1,1,1,1,1,1\n2026-11-20 13:01:00,1,1,1,1,1,1\n";
    mount_window(
        &server,
        "2026-11-20 13:00:00",
        "2026-11-20 16:00:00",
        body.to_owned(),
    )
    .await;

    let error = client(&server)
        .collect_candles(request(
            CandleInterval::Hour1,
            "2026-11-20T14:00:00Z",
            "2026-11-20T16:00:00Z",
        ))
        .await
        .unwrap_err();
    assert!(
        matches!(error, LseError::UnexpectedCandleResolution { .. }),
        "{error:?}"
    );

    let server = MockServer::start().await;
    let body = "time,open,high,low,close,volume,trades\n\
        2026-11-20 13:00:00,1,1,1,1,1,1\n2026-11-20 15:00:00,1,1,1,1,1,1\n";
    mount_window(
        &server,
        "2026-11-20 13:00:00",
        "2026-11-20 16:00:00",
        body.to_owned(),
    )
    .await;
    let candles = client(&server)
        .collect_candles(request(
            CandleInterval::Hour1,
            "2026-11-20T14:00:00Z",
            "2026-11-20T16:00:00Z",
        ))
        .await
        .unwrap();
    assert_eq!(candles.len(), 2);
}

// -----------------------------------------------------------------------------------------------
// Verification
// -----------------------------------------------------------------------------------------------

/// A body that disagrees with its checksum never becomes data.
#[tokio::test]
async fn a_checksum_mismatch_yields_nothing() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v3/export"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("x-checksum-sha256", "00".repeat(32))
                .set_body_raw(minutes(&["15:00"]), "text/csv"),
        )
        .mount(&server)
        .await;

    let error = client(&server)
        .collect_candles(request(
            CandleInterval::Min1,
            "2026-11-20T15:01:00Z",
            "2026-11-20T15:01:00Z",
        ))
        .await
        .unwrap_err();

    let LseError::Api {
        status,
        code,
        message,
    } = &error
    else {
        panic!("expected Api, got {error:?}");
    };
    assert_eq!((*status, code), (200, &None));
    assert!(message.contains("SHA-256"), "{message}");
}

#[tokio::test]
async fn an_answer_without_a_checksum_is_refused() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v3/export"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(minutes(&["15:00"]), "text/csv"))
        .mount(&server)
        .await;

    let error = client(&server)
        .collect_candles(request(
            CandleInterval::Min1,
            "2026-11-20T15:01:00Z",
            "2026-11-20T15:01:00Z",
        ))
        .await
        .unwrap_err();

    assert!(
        matches!(&error, LseError::Api { status: 200, message, .. } if message.contains("x-checksum-sha256")),
        "{error:?}"
    );
}

/// `x-export-rows` is not the file's row count for an aggregated selection (a three-day hourly
/// read declared 4,178 and carried 73), so a disagreeing one is no refusal: the checksum and the
/// size already vouch for the body.
#[tokio::test]
async fn a_row_count_header_disagreeing_with_the_body_is_not_a_refusal() {
    let server = MockServer::start().await;
    let body = minutes(&["15:00"]);
    Mock::given(method("GET"))
        .and(path("/v3/export"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header(
                    "x-checksum-sha256",
                    hex::encode(Sha256::digest(body.as_bytes())),
                )
                .insert_header("x-export-rows", "60")
                .set_body_raw(body, "text/csv"),
        )
        .mount(&server)
        .await;

    let candles = client(&server)
        .collect_candles(request(
            CandleInterval::Min1,
            "2026-11-20T15:01:00Z",
            "2026-11-20T15:01:00Z",
        ))
        .await
        .unwrap();

    assert_eq!(closes(&candles), [utc("2026-11-20T15:01:00Z")]);
}

/// The provider answers a window it stores nothing for `404 data-unavailable`, not an empty file.
/// That chunk yields nothing and the fetch goes on, so one-bar chunks over a quiet stretch return
/// what one wide chunk would.
#[tokio::test]
async fn a_chunk_with_nothing_stored_yields_nothing_and_the_fetch_goes_on() {
    let server = MockServer::start().await;
    mount_window(
        &server,
        "2026-11-20 15:00:00",
        "2026-11-20 15:01:00",
        minutes(&["15:00"]),
    )
    .await;
    Mock::given(method("GET"))
        .and(path("/v3/export"))
        .and(query_param("from", "2026-11-20 15:01:00"))
        .respond_with(ResponseTemplate::new(404).set_body_string(
            r#"{"code": "data-unavailable", "message": "No stored data for this selection"}"#,
        ))
        .expect(1)
        .mount(&server)
        .await;
    mount_window(
        &server,
        "2026-11-20 15:02:00",
        "2026-11-20 15:03:00",
        minutes(&["15:02"]),
    )
    .await;

    let candles = client(&server)
        .with_chunk_bars(NonZeroU32::MIN)
        .collect_candles(request(
            CandleInterval::Min1,
            "2026-11-20T15:01:00Z",
            "2026-11-20T15:03:00Z",
        ))
        .await
        .unwrap();

    assert_eq!(
        closes(&candles),
        [utc("2026-11-20T15:01:00Z"), utc("2026-11-20T15:03:00Z")]
    );
}

// -----------------------------------------------------------------------------------------------
// Refusals
// -----------------------------------------------------------------------------------------------

#[tokio::test]
async fn a_refusal_carries_the_providers_code_and_sentence() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v3/export"))
        .respond_with(
            ResponseTemplate::new(404).set_body_string(
                r#"{"code": "symbol-unknown", "message": "symbol is not available"}"#,
            ),
        )
        .mount(&server)
        .await;

    let error = client(&server)
        .collect_candles(request(
            CandleInterval::Min1,
            "2026-11-20T15:01:00Z",
            "2026-11-20T15:01:00Z",
        ))
        .await
        .unwrap_err();

    let LseError::Api {
        status,
        code,
        message,
    } = error
    else {
        panic!("expected Api, got {error:?}");
    };
    assert_eq!(status, 404);
    assert_eq!(code.as_deref(), Some("symbol-unknown"));
    assert_eq!(message, "symbol is not available");
}

#[tokio::test]
async fn a_rate_limit_is_terminal_and_carries_its_code_and_retry_after() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v3/export"))
        .respond_with(
            ResponseTemplate::new(429)
                .insert_header("retry-after", "5")
                .set_body_string(
                    r#"{"code": "connections-exhausted", "message": "one at a time"}"#,
                ),
        )
        .expect(1)
        .mount(&server)
        .await;

    let error = client(&server)
        .collect_candles(request(
            CandleInterval::Min1,
            "2026-11-20T15:01:00Z",
            "2026-11-20T15:01:00Z",
        ))
        .await
        .unwrap_err();

    let LseError::RateLimited { retry_after, code } = error else {
        panic!("expected RateLimited, got {error:?}");
    };
    assert_eq!(retry_after, Some(Duration::from_secs(5)));
    assert_eq!(code.as_deref(), Some("connections-exhausted"));
}

/// The key rides every request as a custom header, which reqwest does not strip on a cross-host
/// redirect, so a followed redirect would hand it to whatever host the server named. The 3xx is
/// surfaced unfollowed, and the redirect target receives nothing.
#[tokio::test]
async fn a_fetch_does_not_follow_a_redirect_off_host() {
    let attacker = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(csv_answer(CANDLE_HEADER))
        .mount(&attacker)
        .await;

    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v3/export"))
        .respond_with(
            ResponseTemplate::new(302)
                .insert_header("location", format!("{}/v3/export", attacker.uri()).as_str()),
        )
        .mount(&server)
        .await;

    let error = client(&server)
        .collect_candles(request(
            CandleInterval::Min1,
            "2026-11-20T15:01:00Z",
            "2026-11-20T15:01:00Z",
        ))
        .await
        .unwrap_err();

    assert!(
        matches!(error, LseError::Api { status, .. } if (300..400).contains(&status)),
        "expected an unfollowed 3xx as LseError::Api, got {error:?}"
    );
    assert!(attacker.received_requests().await.unwrap().is_empty());
}

// -----------------------------------------------------------------------------------------------
// Usage and the catalogue
// -----------------------------------------------------------------------------------------------

#[tokio::test]
async fn usage_reports_the_plan() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v3/usage"))
        .respond_with(ResponseTemplate::new(200).set_body_string(
            r#"{"key": "k", "plan": "free", "product": "global", "markets": ["crypto"],
                "rate_mbit": 50.0, "connections": 1, "open": 0, "bytes_since_start": 0,
                "calls_per_minute": 60, "levels": {}, "history": {},
                "rows": {"day": "2026-11-27", "per_day": 100, "per_export": 10,
                         "resets_in_seconds": 5, "used_today": 40},
                "weekly": {"allowance_bytes": -1, "used_bytes": 3, "week": "2026-W48"},
                "withheld": null}"#,
        ))
        .mount(&server)
        .await;

    let usage = client(&server).usage().await.unwrap();

    assert_eq!(usage.calls_per_minute, 60);
    assert_eq!(usage.rows.remaining_today(), Some(60));
    assert_eq!(usage.weekly.remaining_bytes(), None);
}

#[tokio::test]
async fn pairs_are_searched_by_exact_symbol_or_text() {
    let server = MockServer::start().await;
    let page = r#"{"market": "crypto", "total": 0, "count": 0, "limit": 5, "offset": 10,
        "pairs": []}"#;
    Mock::given(method("GET"))
        .and(path("/v3/pairs"))
        .and(query_param("market", "crypto"))
        .and(query_param("symbol", "BTCUSDT"))
        .and(query_param("limit", "5"))
        .and(query_param("offset", "10"))
        .respond_with(ResponseTemplate::new(200).set_body_string(page))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/v3/pairs"))
        .and(query_param("q", "bitcoin"))
        .respond_with(ResponseTemplate::new(200).set_body_string(page))
        .expect(1)
        .mount(&server)
        .await;

    let client = client(&server);
    client
        .fetch_pairs(
            LseMarketCode::Crypto,
            Some(LsePairSearch::Symbol("BTCUSDT")),
            5,
            10,
        )
        .await
        .unwrap();
    client
        .fetch_pairs(
            LseMarketCode::Crypto,
            Some(LsePairSearch::Text("bitcoin")),
            5,
            10,
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn markets_leave_out_the_coming_soon_list() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v3/markets"))
        .respond_with(ResponseTemplate::new(200).set_body_string(
            r#"{"plan": "free", "markets": [{"code": "CRYPTO", "folder": "crypto",
                "name": "Crypto", "allowed": true, "time_zone": "UTC",
                "capabilities": {"candles": true, "tick": true}}],
                "coming_soon": [{"code": "XEUR", "folder": "xeur", "status": "coming_soon"}]}"#,
        ))
        .mount(&server)
        .await;

    let markets = client(&server).fetch_markets().await.unwrap();

    assert_eq!(markets.len(), 1);
    assert_eq!(markets[0].market().unwrap(), LseMarketCode::Crypto);
}
