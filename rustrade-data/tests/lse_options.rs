//! Paging and decoding tests for the LSE Data 3.0 option print tape, chain, candles and Greeks.
//!
//! Every response here is **synthetic**, shaped from measurements of the live API (2026-11-27). No
//! provider data is committed — the provider prohibits redistribution
//! (<https://londonstrategicedge.com/licence>), so `wiremock` is the only in-repo option. The live
//! API is exercised separately by the options shape canary.
//!
//! Run with: `cargo test --test lse_options --features lse`

#![cfg(feature = "lse")]
#![allow(clippy::unwrap_used, clippy::expect_used)] // Test code: panics on bad input are acceptable

use chrono::{DateTime, Utc};
use futures::StreamExt;
use rust_decimal_macros::dec;
use rustrade_data::exchange::lse::client::LseClient;
use rustrade_data::exchange::lse::error::LseError;
use rustrade_data::exchange::lse::options::LseOptionPrint;
use rustrade_data::subscription::candle::CandleInterval;
use sha2::{Digest, Sha256};
use std::time::Duration;
use wiremock::matchers::{method, path, query_param, query_param_is_missing};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn client(server: &MockServer) -> LseClient {
    LseClient::new("test-key")
        .unwrap()
        .with_base_url(format!("{}/v3", server.uri()))
        .with_pace(Duration::ZERO)
}

fn utc(raw: &str) -> DateTime<Utc> {
    raw.parse().unwrap()
}

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

/// One flow row. Values are invented; only the shape is measured.
fn print_row(ts: &str, underlying: &str, size: u64) -> String {
    format!(
        r#"{{"ts":"{ts}","underlying":"{underlying}","symbol":"{underlying}  240105C00010000",
            "ticker":"{underlying}240105C00010000","strike":10,"expiry":"2024-01-05",
            "contract_type":"call","dte":3,"pair_id":9,"price":1.5,"size":{size},"premium":300,
            "venue":"A","underlying_price":10.0,"iv":0.2,"delta":0.5,"gamma":0.1,"theta":-0.01,
            "vega":0.02}}"#
    )
}

fn flow_page(rows: &[String]) -> ResponseTemplate {
    ResponseTemplate::new(200).set_body_string(format!(
        r#"{{"underlying":"TEST","from":"x","to":"y","order":"asc","limit":5000,"offset":0,
            "returned":{},"rows":[{}]}}"#,
        rows.len(),
        rows.join(",")
    ))
}

/// One page of the tape, answered to a request starting at `from`.
async fn mount_from(server: &MockServer, from: &str, rows: &[String]) {
    Mock::given(method("GET"))
        .and(path("/v3/options/flow"))
        .and(query_param("from", from))
        .respond_with(flow_page(rows))
        .expect(1)
        .mount(server)
        .await;
}

async fn collect(
    client: &LseClient,
    underlying: Option<&str>,
    start: &str,
    end: &str,
) -> Vec<Result<LseOptionPrint, LseError>> {
    client
        .fetch_option_flow(underlying, utc(start), utc(end))
        .collect()
        .await
}

fn sizes(results: Vec<Result<LseOptionPrint, LseError>>) -> Vec<u64> {
    results
        .into_iter()
        .map(|print| print.unwrap().size)
        .collect()
}

// -----------------------------------------------------------------------------------------------
// The print tape
// -----------------------------------------------------------------------------------------------

/// Pages by time, oldest first. The prints at a page's last instant are held back and re-read
/// whole by the next page, which starts at that instant, so a group the provider returns in a new
/// order is taken from one read only. A short page is not taken as the end: only an empty one is.
#[tokio::test]
async fn pages_by_time_rereading_the_last_instant_until_an_empty_page() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v3/options/flow"))
        .and(query_param("from", "2024-01-02 15:00:00"))
        .and(query_param("to", "2024-01-02 15:00:05.500"))
        .and(query_param("order", "asc"))
        .and(query_param("limit", "5000"))
        .and(query_param_is_missing("offset"))
        .and(query_param("underlying", "TEST"))
        .respond_with(flow_page(&[
            print_row("2024-01-02 15:00:01.000000001", "TEST", 1),
            print_row("2024-01-02 15:00:02", "TEST", 2),
            print_row("2024-01-02 15:00:02", "TEST", 3),
        ]))
        .expect(1)
        .mount(&server)
        .await;
    // The 15:00:02 group again, in another order, then a later print.
    mount_from(
        &server,
        "2024-01-02 15:00:02",
        &[
            print_row("2024-01-02 15:00:02", "TEST", 3),
            print_row("2024-01-02 15:00:02", "TEST", 2),
            print_row("2024-01-02 15:00:03", "TEST", 4),
        ],
    )
    .await;
    mount_from(
        &server,
        "2024-01-02 15:00:03",
        &[print_row("2024-01-02 15:00:03", "TEST", 4)],
    )
    .await;
    mount_from(&server, "2024-01-02 15:00:03.000000001", &[]).await;

    let results = collect(
        &client(&server),
        Some("TEST"),
        "2024-01-02T15:00:00Z",
        "2024-01-02T15:00:05.5Z",
    )
    .await;

    assert_eq!(sizes(results), [1, 3, 2, 4]);
}

/// A page whose prints all share one instant and fill it cannot be paged past by time.
#[tokio::test]
async fn a_full_page_at_one_instant_is_an_error_not_a_loop() {
    let server = MockServer::start().await;
    let rows = vec![print_row("2024-01-02 15:00:00", "TEST", 1); 5000];
    mount_from(&server, "2024-01-02 15:00:00", &rows).await;

    let results = collect(
        &client(&server),
        Some("TEST"),
        "2024-01-02T15:00:00Z",
        "2024-01-02T15:01:00Z",
    )
    .await;

    assert!(
        matches!(results[..], [Err(LseError::UnexpectedOptionFlowRow { .. })]),
        "{} results, last {:?}",
        results.len(),
        results.last()
    );
}

/// One print short of a full page at one instant is a whole group: yielded, then the walk moves
/// past it.
#[tokio::test]
async fn a_short_page_at_one_instant_is_yielded_whole() {
    let server = MockServer::start().await;
    let rows = vec![print_row("2024-01-02 15:00:00", "TEST", 1); 4999];
    mount_from(&server, "2024-01-02 15:00:00", &rows).await;
    mount_from(&server, "2024-01-02 15:00:00.000000001", &[]).await;

    let results = collect(
        &client(&server),
        Some("TEST"),
        "2024-01-02T15:00:00Z",
        "2024-01-02T15:01:00Z",
    )
    .await;

    assert_eq!(sizes(results).len(), 4999);
}

/// A range longer than a day is walked in day windows, each paged from offset zero.
#[tokio::test]
async fn a_long_range_is_walked_a_day_at_a_time() {
    let server = MockServer::start().await;
    for (from, to) in [
        ("2024-01-01 00:00:00", "2024-01-02 00:00:00"),
        ("2024-01-02 00:00:00", "2024-01-02 12:00:00"),
    ] {
        Mock::given(method("GET"))
            .and(path("/v3/options/flow"))
            .and(query_param("from", from))
            .and(query_param("to", to))
            .respond_with(flow_page(&[]))
            .expect(1)
            .mount(&server)
            .await;
    }

    let results = collect(
        &client(&server),
        Some("TEST"),
        "2024-01-01T00:00:00Z",
        "2024-01-02T12:00:00Z",
    )
    .await;

    assert!(results.is_empty());
}

#[tokio::test]
async fn no_underlying_sends_no_underlying_filter() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v3/options/flow"))
        .and(query_param_is_missing("underlying"))
        .respond_with(flow_page(&[]))
        .expect(1)
        .mount(&server)
        .await;

    let results = collect(
        &client(&server),
        None,
        "2024-01-02T15:00:00Z",
        "2024-01-02T15:01:00Z",
    )
    .await;

    assert!(results.is_empty());
}

#[tokio::test]
async fn a_range_ending_inside_the_settle_margin_is_refused_without_a_request() {
    let server = MockServer::start().await;
    let end = Utc::now();
    let start = end - chrono::TimeDelta::minutes(5);

    let results: Vec<_> = client(&server)
        .fetch_option_flow(Some("TEST"), start, end)
        .collect()
        .await;

    assert!(
        matches!(results[..], [Err(LseError::InvalidInput { .. })]),
        "{results:?}"
    );
    assert!(server.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn an_inverted_range_is_invalid_input() {
    let server = MockServer::start().await;

    let results = collect(
        &client(&server),
        None,
        "2024-01-02T15:01:00Z",
        "2024-01-02T15:00:00Z",
    )
    .await;

    assert!(
        matches!(results[..], [Err(LseError::InvalidInput { .. })]),
        "{results:?}"
    );
}

/// A row outside the window means the window was ignored: nothing of that page is yielded.
#[tokio::test]
async fn a_row_outside_its_window_fails_the_page_before_any_of_it_is_yielded() {
    let server = MockServer::start().await;
    mount_from(
        &server,
        "2024-01-02 15:00:00",
        &[
            print_row("2024-01-02 15:00:01", "TEST", 1),
            print_row("2024-01-02 15:09:00", "TEST", 2),
        ],
    )
    .await;

    let results = collect(
        &client(&server),
        Some("TEST"),
        "2024-01-02T15:00:00Z",
        "2024-01-02T15:01:00Z",
    )
    .await;

    assert!(
        matches!(results[..], [Err(LseError::UnexpectedOptionFlowRow { .. })]),
        "{results:?}"
    );
}

#[tokio::test]
async fn a_row_for_another_underlying_is_rejected_but_case_is_not_a_mismatch() {
    let server = MockServer::start().await;
    mount_from(
        &server,
        "2024-01-02 15:00:00",
        &[print_row("2024-01-02 15:00:01", "OTHER", 1)],
    )
    .await;
    let results = collect(
        &client(&server),
        Some("TEST"),
        "2024-01-02T15:00:00Z",
        "2024-01-02T15:01:00Z",
    )
    .await;
    assert!(
        matches!(results[..], [Err(LseError::UnexpectedOptionFlowRow { .. })]),
        "{results:?}"
    );

    let server = MockServer::start().await;
    mount_from(
        &server,
        "2024-01-02 15:00:00",
        &[print_row("2024-01-02 15:00:01", "TEST", 1)],
    )
    .await;
    mount_from(
        &server,
        "2024-01-02 15:00:01",
        &[print_row("2024-01-02 15:00:01", "TEST", 1)],
    )
    .await;
    mount_from(&server, "2024-01-02 15:00:01.000000001", &[]).await;
    let results = collect(
        &client(&server),
        Some("test"),
        "2024-01-02T15:00:00Z",
        "2024-01-02T15:01:00Z",
    )
    .await;
    assert_eq!(sizes(results), [1]);
}

/// Prints stepping back in time mean the order was ignored, which paging by time cannot survive.
#[tokio::test]
async fn prints_stepping_back_in_time_are_rejected() {
    let server = MockServer::start().await;
    mount_from(
        &server,
        "2024-01-02 15:00:00",
        &[
            print_row("2024-01-02 15:00:02", "TEST", 1),
            print_row("2024-01-02 15:00:01", "TEST", 2),
        ],
    )
    .await;

    let results = collect(
        &client(&server),
        Some("TEST"),
        "2024-01-02T15:00:00Z",
        "2024-01-02T15:01:00Z",
    )
    .await;

    assert!(
        matches!(results[..], [Err(LseError::UnexpectedOptionFlowRow { .. })]),
        "{results:?}"
    );
}

#[tokio::test]
async fn a_print_decodes_its_contract_and_greeks() {
    let server = MockServer::start().await;
    mount_from(
        &server,
        "2024-01-02 15:00:00",
        &[print_row("2024-01-02 15:00:01", "TEST", 4)],
    )
    .await;
    mount_from(
        &server,
        "2024-01-02 15:00:01",
        &[print_row("2024-01-02 15:00:01", "TEST", 4)],
    )
    .await;
    mount_from(&server, "2024-01-02 15:00:01.000000001", &[]).await;

    let results = collect(
        &client(&server),
        Some("TEST"),
        "2024-01-02T15:00:00Z",
        "2024-01-02T15:01:00Z",
    )
    .await;
    let print = results.into_iter().next().unwrap().unwrap();

    assert_eq!(print.contract.ticker, "TEST240105C00010000");
    assert_eq!(print.contract.strike, dec!(10));
    assert_eq!((print.price, print.size, print.pair_id), (dec!(1.5), 4, 9));
    assert_eq!(print.greeks.delta, Some(0.5));
    assert_eq!(print.greeks.rho, None);
}

// -----------------------------------------------------------------------------------------------
// Chain, candles and Greeks
// -----------------------------------------------------------------------------------------------

#[tokio::test]
async fn the_chain_is_asked_for_one_page() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v3/options/chain"))
        .and(query_param("underlying", "TEST"))
        .and(query_param("limit", "2"))
        .and(query_param("offset", "4"))
        .respond_with(ResponseTemplate::new(200).set_body_string(
            r#"{"underlying":"TEST","underlying_price":10.5,"underlying_at":"2024-01-02 15:00:00",
                "contracts":9,"rows":[{"ticker":"TEST240105P00010000","underlying":"TEST",
                "pair_id":3,"last_price":null,"last_trade_at":null}]}"#,
        ))
        .expect(1)
        .mount(&server)
        .await;

    let chain = client(&server)
        .fetch_option_chain("TEST", 2, 4)
        .await
        .unwrap();

    assert_eq!(chain.contracts, 9);
    assert_eq!(chain.underlying_price, Some(dec!(10.5)));
    assert_eq!(chain.entries[0].contract.ticker, "TEST240105P00010000");
}

/// Option candles are the generic candle fetch, with the root padded as the selection requires.
#[tokio::test]
async fn option_candles_ask_for_the_padded_contract() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v3/export"))
        .and(query_param("market", "us_options"))
        .and(query_param("symbol", "TEST  240105C00010000"))
        .and(query_param("data", "candles"))
        .respond_with(csv_answer(
            "time,open,high,low,close,volume,trades\n2024-01-02 15:00:00,1,2,0.5,1.5,40,9\n",
        ))
        .expect(1)
        .mount(&server)
        .await;

    let candles: Vec<_> = client(&server)
        .fetch_option_candles(
            "TEST240105C00010000",
            CandleInterval::Min1,
            utc("2024-01-02T15:01:00Z"),
            utc("2024-01-02T15:01:00Z"),
        )
        .collect()
        .await;

    let candle = candles.into_iter().next().unwrap().unwrap();
    assert_eq!(candle.trade_count, Some(9));
}

#[tokio::test]
async fn a_ticker_that_is_not_osi_is_refused_before_any_request() {
    let server = MockServer::start().await;

    let candles: Vec<_> = client(&server)
        .fetch_option_candles(
            "TEST",
            CandleInterval::Min1,
            utc("2024-01-02T15:01:00Z"),
            utc("2024-01-02T15:01:00Z"),
        )
        .collect()
        .await;

    assert!(
        matches!(candles[..], [Err(LseError::InvalidInput { .. })]),
        "{candles:?}"
    );
    assert!(server.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn greeks_come_per_minute_stamped_at_its_close() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v3/export"))
        .and(query_param("data", "greeks"))
        .and(query_param("symbol", "TEST  240105C00010000"))
        .and(query_param("from", "2024-01-02 15:01:00"))
        .and(query_param("to", "2024-01-02 15:02:00"))
        .respond_with(csv_answer(
            "time,price_at,iv,delta,gamma,theta,vega,option_price,price_source,underlying_price,\
years_to_expiry,rate,dividends_pv\n\
2024-01-02 15:01:00,2024-01-02 15:01:10,0.3,0.6,0.1,-0.01,0.02,1.6,trade,10,0.01,0.04,0\n",
        ))
        .expect(1)
        .mount(&server)
        .await;

    let minutes: Vec<_> = client(&server)
        .fetch_option_greeks(
            "TEST240105C00010000",
            utc("2024-01-02T15:02:00Z"),
            utc("2024-01-02T15:02:00Z"),
        )
        .collect()
        .await;

    // The window asked for is the one minute closing on the range, [15:01, 15:02).
    let [Ok(minute)] = &minutes[..] else {
        panic!("one minute expected, got {minutes:?}");
    };
    assert_eq!(minute.close_time, utc("2024-01-02T15:02:00Z"));
    assert_eq!(minute.greeks.delta, Some(0.6));
}

/// A contract with no trade in the window is answered `404 data-unavailable`: no minutes, no error.
#[tokio::test]
async fn greeks_for_a_window_with_nothing_stored_are_empty_not_an_error() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v3/export"))
        .and(query_param("data", "greeks"))
        .respond_with(ResponseTemplate::new(404).set_body_string(
            r#"{"code": "data-unavailable", "message": "No stored data for this selection"}"#,
        ))
        .expect(1)
        .mount(&server)
        .await;

    let minutes: Vec<_> = client(&server)
        .fetch_option_greeks(
            "TEST240105C00010000",
            utc("2024-01-02T15:02:00Z"),
            utc("2024-01-02T15:02:00Z"),
        )
        .collect()
        .await;

    assert!(minutes.is_empty(), "{minutes:?}");
}
