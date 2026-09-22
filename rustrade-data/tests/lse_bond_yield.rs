//! Wire-level tests for the London Strategic Edge bond-yield endpoints.
//!
//! Every response here is **synthetic**, shaped from measurements of the live API. No provider data
//! is committed — the provider prohibits redistribution
//! (<https://londonstrategicedge.com/terms>), so `wiremock` is the only in-repo option. The live
//! API is exercised separately by `lse_bond_yield_canary`, which asserts against the real service
//! without storing anything.
//!
//! What these cover that the unit tests in `bond_yield.rs` cannot: the request that actually leaves
//! the client. The endpoint answers `200` with zero rows for an unknown country, an unknown tenor
//! and an empty window alike, so a parameter this client gets wrong is invisible in the response —
//! the only place to catch it is here, against a mock that asserts on the query string.
//!
//! Run with: `cargo test --test lse_bond_yield --features lse`

#![cfg(feature = "lse")]
#![allow(clippy::unwrap_used, clippy::expect_used)] // Test code: panics on bad input are acceptable

use chrono::NaiveDate;
use rust_decimal_macros::dec;
use rustrade_data::exchange::lse::bond_yield::{LseBondYieldQuery, LseBondYieldStats};
use rustrade_data::exchange::lse::data_api::LseDataApiClient;
use rustrade_data::exchange::lse::error::LseError;
use std::time::Duration;
use wiremock::matchers::{method, path, query_param, query_param_is_missing};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// Build a client pointed at `server` with pacing disabled so tests do not sleep.
fn client(server: &MockServer) -> LseDataApiClient {
    LseDataApiClient::new("test-key")
        .unwrap()
        .with_base_url(server.uri())
        .with_pace(Duration::ZERO)
}

fn date(year: i32, month: u32, day: u32) -> NaiveDate {
    NaiveDate::from_ymd_opt(year, month, day).unwrap()
}

/// A stats body shaped like the provider's, with invented dates and counts.
///
/// Reproduces the two structures the validation rules turn on: the United Kingdom keyed `UK` with
/// no `GB` entry, and a tenor (`US`/`10Y TIPS`) whose coverage starts long after its country's.
const STATS_BODY: &str = r#"{
    "total_countries": 2,
    "total_observations": 30,
    "columns": ["date", "symbol", "country_iso2", "country_name", "maturity",
                "maturity_days", "currency", "open", "high", "low", "close"],
    "countries": {
        "UK": {
            "code": "UK", "name": "United Kingdom", "currency": "GBP",
            "first_date": "2020-01-01", "last_date": "2024-12-31", "observations": 10,
            "maturities": [
                {"tenor": "5Y", "days": 1825, "first_date": "2020-01-01",
                 "last_date": "2024-12-31", "observations": 10}
            ]
        },
        "US": {
            "code": "US", "name": "United States", "currency": "USD",
            "first_date": "2000-01-01", "last_date": "2024-12-31", "observations": 20,
            "maturities": [
                {"tenor": "10Y", "days": 3650, "first_date": "2000-01-01",
                 "last_date": "2024-12-31", "observations": 15},
                {"tenor": "10Y TIPS", "days": 3650, "first_date": "2024-06-01",
                 "last_date": "2024-12-31", "observations": 5}
            ]
        }
    }
}"#;

/// One row, with every field a JSON string exactly as the provider sends it.
fn row(date: &str, close: &str) -> String {
    format!(
        r#"{{"date":"{date}","symbol":"UK5Y","country_iso2":"UK","country_name":"United Kingdom",
            "maturity":"5Y","maturity_days":"1825","currency":"GBP",
            "open":"1.0000","high":"2.0000","low":"0.5000","close":"{close}"}}"#
    )
}

fn envelope(rows: &[String]) -> String {
    format!(r#"{{"count":{},"data":[{}]}}"#, rows.len(), rows.join(","))
}

async fn mount_stats(server: &MockServer) {
    Mock::given(method("GET"))
        .and(path("/bond-yields/stats"))
        .and(query_param("format", "json"))
        .respond_with(ResponseTemplate::new(200).set_body_string(STATS_BODY))
        .mount(server)
        .await;
}

async fn stats(server: &MockServer) -> LseBondYieldStats {
    mount_stats(server).await;
    client(server).fetch_bond_yield_stats().await.unwrap()
}

#[tokio::test]
async fn stats_decode_from_the_providers_envelope() {
    let server = MockServer::start().await;
    let stats = stats(&server).await;

    assert_eq!(stats.total_countries, 2);
    assert_eq!(stats.total_observations, 30);
    assert_eq!(stats.country("GB").unwrap().code, "UK");
}

/// 🔴 The single most consequential assertion in this file. A caller's ISO-3166 `GB` must reach the
/// provider as `UK`; sending it unchanged returns `200` with zero rows and no error, which is
/// indistinguishable from a quiet market. The mock only answers when the query carries `UK`, so
/// sending `GB` fails the test rather than silently returning an empty `Vec`.
#[tokio::test]
async fn a_caller_supplied_gb_reaches_the_provider_as_uk() {
    let server = MockServer::start().await;
    let stats = stats(&server).await;

    Mock::given(method("GET"))
        .and(path("/bond-yields"))
        .and(query_param("country", "UK"))
        .and(query_param("maturity", "5Y"))
        .and(query_param("start_date", "2021-01-01"))
        .and(query_param("end_date", "2021-12-31"))
        .and(query_param("format", "json"))
        .respond_with(
            ResponseTemplate::new(200).set_body_string(envelope(&[row("2021-01-04", "1.2500")])),
        )
        .mount(&server)
        .await;

    let query =
        LseBondYieldQuery::new("GB", date(2021, 1, 1), date(2021, 12, 31)).with_maturity("5Y");
    let rows = client(&server)
        .fetch_bond_yields(&stats, &query)
        .await
        .unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].close, dec!(1.2500));
    assert_eq!(rows[0].observation_date().unwrap(), date(2021, 1, 4));
}

/// `format` is not optional: absent it, the endpoint serves CSV to a JSON decoder.
#[tokio::test]
async fn the_json_format_parameter_is_always_sent() {
    let server = MockServer::start().await;
    let stats = stats(&server).await;

    Mock::given(method("GET"))
        .and(path("/bond-yields"))
        .and(query_param("format", "json"))
        .respond_with(ResponseTemplate::new(200).set_body_string(envelope(&[])))
        .mount(&server)
        .await;

    let query = LseBondYieldQuery::new("UK", date(2021, 1, 1), date(2021, 12, 31));

    assert!(
        client(&server)
            .fetch_bond_yields(&stats, &query)
            .await
            .is_ok()
    );
}

/// An omitted tenor must be absent from the query string, not sent empty: an empty `maturity` is an
/// unknown one, and an unknown filter returns zero rows rather than an error.
#[tokio::test]
async fn an_omitted_maturity_is_absent_from_the_query_rather_than_empty() {
    let server = MockServer::start().await;
    let stats = stats(&server).await;

    Mock::given(method("GET"))
        .and(path("/bond-yields"))
        .and(query_param("country", "US"))
        .and(query_param_is_missing("maturity"))
        .respond_with(
            ResponseTemplate::new(200).set_body_string(envelope(&[row("2021-01-04", "1.0000")])),
        )
        .mount(&server)
        .await;

    let query = LseBondYieldQuery::new("US", date(2021, 1, 1), date(2021, 12, 31));

    assert_eq!(
        client(&server)
            .fetch_bond_yields(&stats, &query)
            .await
            .unwrap()
            .len(),
        1
    );
}

/// The whole answer arrives in one response, so a `count` that disagrees with the rows delivered is
/// the signal a silently introduced page cap would produce. It is an error rather than a short
/// result, because a short result is exactly the failure this integration cannot otherwise see.
#[tokio::test]
async fn a_count_that_disagrees_with_the_rows_delivered_is_an_error() {
    let server = MockServer::start().await;
    let stats = stats(&server).await;

    Mock::given(method("GET"))
        .and(path("/bond-yields"))
        .respond_with(ResponseTemplate::new(200).set_body_string(
            // Declares 5000, carries 1 -- the shape of a truncated page.
            format!(r#"{{"count":5000,"data":[{}]}}"#, row("2021-01-04", "1.0")),
        ))
        .mount(&server)
        .await;

    let query = LseBondYieldQuery::new("UK", date(2021, 1, 1), date(2021, 12, 31));
    let error = client(&server)
        .fetch_bond_yields(&stats, &query)
        .await
        .unwrap_err();

    match error {
        LseError::Api {
            status,
            ref message,
        } => {
            assert_eq!(status, 200);
            assert!(message.contains("5000"), "{message}");
            assert!(message.contains("truncated"), "{message}");
        }
        other => panic!("expected Api, got {other}"),
    }
}

/// 🔴 A rejected query must cost **no request at all**. The mock below is never mounted, so any
/// request to `/bond-yields` fails the test — which is the assertion: validation happens before the
/// wire, not after it.
#[tokio::test]
async fn an_invalid_query_never_reaches_the_network() {
    let server = MockServer::start().await;
    let stats = stats(&server).await;
    let client = client(&server);

    // Unknown country.
    assert!(matches!(
        client
            .fetch_bond_yields(
                &stats,
                &LseBondYieldQuery::new("ZZ", date(2021, 1, 1), date(2021, 12, 31))
            )
            .await
            .unwrap_err(),
        LseError::UnknownBondYieldCountry { .. }
    ));

    // Unknown tenor.
    assert!(matches!(
        client
            .fetch_bond_yields(
                &stats,
                &LseBondYieldQuery::new("US", date(2021, 1, 1), date(2021, 12, 31))
                    .with_maturity("99Y")
            )
            .await
            .unwrap_err(),
        LseError::UnknownBondYieldMaturity { .. }
    ));

    // Window disjoint from the tenor's own coverage, though the country spans it.
    assert!(matches!(
        client
            .fetch_bond_yields(
                &stats,
                &LseBondYieldQuery::new("US", date(2021, 1, 1), date(2021, 12, 31))
                    .with_maturity("10Y TIPS")
            )
            .await
            .unwrap_err(),
        LseError::BondYieldRangeOutsideCoverage { .. }
    ));

    // `wiremock` fails an unmatched request, so reaching this line proves none was made. Asserting
    // it explicitly makes the intent survive a future change to that default.
    assert!(
        server.received_requests().await.unwrap().len() == 1,
        "only the stats fetch should have reached the network"
    );
}

/// A non-success status carries the provider's own diagnostic through, unwrapped from its envelope.
#[tokio::test]
async fn a_provider_error_is_reported_with_its_status_and_detail() {
    let server = MockServer::start().await;
    let stats = stats(&server).await;

    Mock::given(method("GET"))
        .and(path("/bond-yields"))
        .respond_with(
            ResponseTemplate::new(503).set_body_string(r#"{"detail":"upstream unavailable"}"#),
        )
        .mount(&server)
        .await;

    let query = LseBondYieldQuery::new("UK", date(2021, 1, 1), date(2021, 12, 31));

    match client(&server)
        .fetch_bond_yields(&stats, &query)
        .await
        .unwrap_err()
    {
        LseError::Api { status, message } => {
            assert_eq!(status, 503);
            assert!(message.contains("upstream unavailable"), "{message}");
        }
        other => panic!("expected Api, got {other}"),
    }
}

/// A `429` maps to the typed rate limit, carrying `Retry-After` — the shared transport's behaviour,
/// pinned here because this host has its own client and could drift from the vault's.
#[tokio::test]
async fn a_rate_limit_is_typed_and_carries_its_retry_after() {
    let server = MockServer::start().await;
    let stats = stats(&server).await;

    Mock::given(method("GET"))
        .and(path("/bond-yields"))
        .respond_with(
            ResponseTemplate::new(429)
                .insert_header("retry-after", "42")
                .set_body_string(r#"{"detail":"slow down"}"#),
        )
        .mount(&server)
        .await;

    let query = LseBondYieldQuery::new("UK", date(2021, 1, 1), date(2021, 12, 31));

    match client(&server)
        .fetch_bond_yields(&stats, &query)
        .await
        .unwrap_err()
    {
        LseError::RateLimited { retry_after } => {
            assert_eq!(retry_after, Some(Duration::from_secs(42)));
        }
        other => panic!("expected RateLimited, got {other}"),
    }
}
