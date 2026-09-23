//! Range-walk and decoding tests for the London Strategic Edge option print tape and option
//! candles.
//!
//! Every response here is **synthetic**, shaped from measurements of the live API. No provider
//! data is committed — the provider prohibits redistribution
//! (<https://londonstrategicedge.com/terms>), so `wiremock` is the only in-repo option. The live
//! API is exercised separately by the options shape canary.
//!
//! Run with: `cargo test --test lse_options --features lse`

#![cfg(feature = "lse")]
#![allow(clippy::unwrap_used, clippy::expect_used)] // Test code: panics on bad input are acceptable

use chrono::{DateTime, Utc};
use futures::StreamExt;
use rust_decimal_macros::dec;
use rustrade_data::exchange::lse::error::LseError;
use rustrade_data::exchange::lse::options::LseOptionPrint;
use rustrade_data::exchange::lse::vault::LseVaultClient;
use std::num::NonZeroU32;
use std::time::Duration;
use wiremock::matchers::{method, path, query_param, query_param_is_missing};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn client(server: &MockServer) -> LseVaultClient {
    LseVaultClient::new("test-key")
        .unwrap()
        .with_base_url(format!("{}/vault", server.uri()))
        .with_pace(Duration::ZERO)
}

fn utc(raw: &str) -> DateTime<Utc> {
    raw.parse().unwrap()
}

/// One flow row. Values are invented; only the shape is measured.
fn print_row(id: u64, ts: &str, underlying: &str) -> String {
    format!(
        r#"{{"id":{id},"ts":"{ts}","underlying":"{underlying}","ticker":"{underlying}240105C00010000",
            "strike":10,"expiry":"2024-01-05","contract_type":"call","last_price":1.5,"volume":2,
            "premium":300,"underlying_price":10.0,"dte":3,"iv":0.2,"delta":0.5,"gamma":0.1,
            "theta":-0.01,"vega":0.02,"rho":0.001}}"#
    )
}

/// Mount one flow window, keyed on the exact whole-second bounds the client must send.
async fn mount_window(server: &MockServer, start: &str, end: &str, rows: &[String]) {
    Mock::given(method("GET"))
        .and(path("/vault/options/flow"))
        .and(query_param("start", start))
        .and(query_param("end", end))
        .respond_with(ResponseTemplate::new(200).set_body_string(format!("[{}]", rows.join(","))))
        .expect(1)
        .mount(server)
        .await;
}

async fn collect(
    client: &LseVaultClient,
    underlying: Option<&str>,
    start: &str,
    end: &str,
) -> Vec<Result<LseOptionPrint, LseError>> {
    client
        .fetch_option_flow(underlying, utc(start), utc(end))
        .collect()
        .await
}

fn ids(results: Vec<Result<LseOptionPrint, LseError>>) -> Vec<u64> {
    results.into_iter().map(|print| print.unwrap().id).collect()
}

#[tokio::test]
async fn walks_forward_and_yields_each_window_oldest_first() {
    let server = MockServer::start().await;

    // The provider serves newest-first within a window.
    mount_window(
        &server,
        "2024-01-02 15:00:00",
        "2024-01-02 15:01:00",
        &[
            print_row(3, "2024-01-02 15:00:40.070000", "TEST"),
            print_row(2, "2024-01-02 15:00:10.070000", "TEST"),
            print_row(1, "2024-01-02 15:00:10.070000", "TEST"),
        ],
    )
    .await;
    mount_window(
        &server,
        "2024-01-02 15:01:00",
        "2024-01-02 15:02:00",
        &[print_row(4, "2024-01-02 15:01:05.070000", "TEST")],
    )
    .await;

    let results = collect(
        &client(&server),
        Some("TEST"),
        "2024-01-02T15:00:00Z",
        "2024-01-02T15:02:00Z",
    )
    .await;

    // Ordered by time, then by id where the batch stamp ties.
    assert_eq!(ids(results), vec![1, 2, 3, 4]);
}

#[tokio::test]
async fn a_full_window_is_halved_and_reread_rather_than_emitted_truncated() {
    let server = MockServer::start().await;

    // At a page limit of 4, four rows means the window may have been cut: the cap drops the OLDEST
    // rows, so none of this page may be yielded.
    mount_window(
        &server,
        "2024-01-02 15:00:00",
        "2024-01-02 15:01:00",
        &[
            print_row(9, "2024-01-02 15:00:50.070000", "TEST"),
            print_row(8, "2024-01-02 15:00:45.070000", "TEST"),
            print_row(7, "2024-01-02 15:00:40.070000", "TEST"),
            print_row(6, "2024-01-02 15:00:35.070000", "TEST"),
        ],
    )
    .await;
    mount_window(
        &server,
        "2024-01-02 15:00:00",
        "2024-01-02 15:00:30",
        &[
            print_row(2, "2024-01-02 15:00:20.070000", "TEST"),
            print_row(1, "2024-01-02 15:00:05.070000", "TEST"),
        ],
    )
    .await;
    mount_window(
        &server,
        "2024-01-02 15:00:30",
        "2024-01-02 15:01:00",
        &[
            print_row(9, "2024-01-02 15:00:50.070000", "TEST"),
            print_row(8, "2024-01-02 15:00:45.070000", "TEST"),
            print_row(7, "2024-01-02 15:00:40.070000", "TEST"),
        ],
    )
    .await;

    let client = client(&server).with_page_limit(NonZeroU32::new(4).unwrap());
    let results = collect(
        &client,
        Some("TEST"),
        "2024-01-02T15:00:00Z",
        "2024-01-02T15:01:00Z",
    )
    .await;

    assert_eq!(ids(results), vec![1, 2, 7, 8, 9]);
}

#[tokio::test]
async fn a_sparse_window_lets_the_next_one_grow() {
    let server = MockServer::start().await;

    mount_window(&server, "2024-01-02 15:00:00", "2024-01-02 15:01:00", &[]).await;
    // Doubled from sixty seconds after an empty window.
    mount_window(
        &server,
        "2024-01-02 15:01:00",
        "2024-01-02 15:03:00",
        &[print_row(1, "2024-01-02 15:02:00.070000", "TEST")],
    )
    .await;
    // Doubled again, then clamped to the end of the range.
    mount_window(&server, "2024-01-02 15:03:00", "2024-01-02 15:05:00", &[]).await;

    let results = collect(
        &client(&server),
        Some("TEST"),
        "2024-01-02T15:00:00Z",
        "2024-01-02T15:05:00Z",
    )
    .await;

    assert_eq!(ids(results), vec![1]);
}

#[tokio::test]
async fn a_saturated_second_is_a_typed_error_not_a_short_tape() {
    let server = MockServer::start().await;

    mount_window(
        &server,
        "2024-01-02 15:00:00",
        "2024-01-02 15:00:01",
        &[
            print_row(2, "2024-01-02 15:00:00.070000", "TEST"),
            print_row(1, "2024-01-02 15:00:00.070000", "TEST"),
        ],
    )
    .await;

    let client = client(&server).with_page_limit(NonZeroU32::new(2).unwrap());
    let results = collect(
        &client,
        Some("TEST"),
        "2024-01-02T15:00:00Z",
        "2024-01-02T15:00:01Z",
    )
    .await;

    assert_eq!(results.len(), 1);
    assert!(matches!(
        results[0],
        Err(LseError::OptionFlowWindowSaturated { rows: 2, .. })
    ));
}

#[tokio::test]
async fn sub_second_bounds_are_widened_to_whole_seconds_and_trimmed_back() {
    let server = MockServer::start().await;

    mount_window(
        &server,
        "2024-01-02 15:00:00",
        "2024-01-02 15:00:31",
        &[
            print_row(3, "2024-01-02 15:00:30.700000", "TEST"),
            print_row(2, "2024-01-02 15:00:10.070000", "TEST"),
            print_row(1, "2024-01-02 15:00:00.200000", "TEST"),
        ],
    )
    .await;

    let results = collect(
        &client(&server),
        Some("TEST"),
        "2024-01-02T15:00:00.5Z",
        "2024-01-02T15:00:30.5Z",
    )
    .await;

    // Row 1 precedes the start; row 3 is at or past the exclusive end.
    assert_eq!(ids(results), vec![2]);
}

#[tokio::test]
async fn a_range_ending_inside_the_settle_margin_is_refused_without_a_request() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_string("[]"))
        .expect(0)
        .mount(&server)
        .await;

    let end = Utc::now();
    let results: Vec<_> = client(&server)
        .fetch_option_flow(Some("TEST"), end - chrono::TimeDelta::minutes(5), end)
        .collect()
        .await;

    assert_eq!(results.len(), 1);
    let error = results.into_iter().next().unwrap().unwrap_err();
    assert!(matches!(error, LseError::InvalidInput { .. }));
    assert!(error.to_string().contains("settle margin"));
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

    assert!(matches!(results[0], Err(LseError::InvalidInput { .. })));
}

#[tokio::test]
async fn a_row_outside_its_window_fails_the_window_before_any_of_it_is_yielded() {
    let server = MockServer::start().await;

    mount_window(
        &server,
        "2024-01-02 15:00:00",
        "2024-01-02 15:01:00",
        &[
            // The provider answering with the default latest window, as it does for an ignored
            // parameter.
            print_row(2, "2024-01-02 20:15:00.070000", "TEST"),
            print_row(1, "2024-01-02 15:00:10.070000", "TEST"),
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

    assert_eq!(results.len(), 1);
    assert!(matches!(
        results[0],
        Err(LseError::UnexpectedOptionFlowRow { id: 2, .. })
    ));
}

#[tokio::test]
async fn a_row_for_another_underlying_is_rejected_but_case_is_not_a_mismatch() {
    let server = MockServer::start().await;

    mount_window(
        &server,
        "2024-01-02 15:00:00",
        "2024-01-02 15:01:00",
        &[
            print_row(2, "2024-01-02 15:00:20.070000", "OTHER"),
            print_row(1, "2024-01-02 15:00:10.070000", "TEST"),
        ],
    )
    .await;

    let results = collect(
        &client(&server),
        Some("test"),
        "2024-01-02T15:00:00Z",
        "2024-01-02T15:01:00Z",
    )
    .await;

    assert_eq!(results.len(), 1);
    assert!(matches!(
        results[0],
        Err(LseError::UnexpectedOptionFlowRow { id: 2, .. })
    ));
}

#[tokio::test]
async fn no_underlying_sends_no_underlying_filter() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/vault/options/flow"))
        .and(query_param_is_missing("underlying"))
        .respond_with(ResponseTemplate::new(200).set_body_string(format!(
            "[{},{}]",
            print_row(2, "2024-01-02 15:00:20.070000", "OTHER"),
            print_row(1, "2024-01-02 15:00:10.070000", "TEST"),
        )))
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

    assert_eq!(ids(results), vec![1, 2]);
}

#[tokio::test]
async fn a_print_decodes_its_contract_and_greeks() {
    let server = MockServer::start().await;

    mount_window(
        &server,
        "2024-01-02 15:00:00",
        "2024-01-02 15:01:00",
        &[print_row(1, "2024-01-02 15:00:10.070000", "TEST")],
    )
    .await;

    let print = collect(
        &client(&server),
        Some("TEST"),
        "2024-01-02T15:00:00Z",
        "2024-01-02T15:01:00Z",
    )
    .await
    .into_iter()
    .next()
    .unwrap()
    .unwrap();

    assert_eq!(print.contract.ticker, "TEST240105C00010000");
    assert_eq!(print.contract.strike, dec!(10));
    assert_eq!(print.price, dec!(1.5));
    assert_eq!(print.volume, 2);
    assert_eq!(print.greeks.rho, Some(0.001));
    assert_eq!(print.public_trade().amount, dec!(2));
}

/// One option candle row. `minute` is the bar's OPEN.
fn candle_row(minute: &str) -> String {
    format!(
        r#"{{"ticker":"TEST240105P00010000","underlying":"TEST","strike":10,"expiry":"2024-01-05",
            "contract_type":"put","minute":"{minute}","dte":3,"open":1.0,"high":2.0,"low":0.5,
            "close":1.5,"volume":40,"premium":6000,"print_count":9,"iv_avg":0.3,"delta_avg":-0.4,
            "gamma_avg":0.1,"theta_avg":-0.02,"vega_avg":0.05,"rho_avg":-0.001,
            "underlying_price":10.25}}"#
    )
}

#[tokio::test]
async fn option_candles_page_on_the_shared_vault_contract_keyed_by_ticker() {
    let server = MockServer::start().await;

    // The lower bound widens one minute so the bar closing exactly on `start` is included, and the
    // exclusive upper bound sits one second past the last bar's open -- the vault candle contract.
    Mock::given(method("GET"))
        .and(path("/vault/options/candles"))
        .and(query_param("ticker", "TEST240105P00010000"))
        .and(query_param("start", "2024-01-02 14:59:00"))
        .and(query_param("end", "2024-01-02 15:01:01"))
        .respond_with(ResponseTemplate::new(200).set_body_string(format!(
            "[{},{},{}]",
            candle_row("2024-01-02 14:59:00"),
            candle_row("2024-01-02 15:00:00"),
            candle_row("2024-01-02 15:01:00"),
        )))
        .expect(1)
        .mount(&server)
        .await;

    let candles = client(&server)
        .collect_option_candles(
            "TEST240105P00010000",
            utc("2024-01-02T15:00:00Z"),
            utc("2024-01-02T15:02:00Z"),
        )
        .await
        .unwrap();

    let closes: Vec<_> = candles.iter().map(|c| c.candle.close_time).collect();
    assert_eq!(
        closes,
        vec![
            utc("2024-01-02T15:00:00Z"),
            utc("2024-01-02T15:01:00Z"),
            utc("2024-01-02T15:02:00Z"),
        ]
    );
    assert_eq!(candles[0].candle.trade_count, Some(9));
    assert_eq!(candles[0].greeks.delta, Some(-0.4));
}
