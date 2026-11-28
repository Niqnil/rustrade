//! Request tests for the LSE Data 3.0 macro reader.
//!
//! Every response here is **synthetic**, shaped from measurements of the live API (2026-11-28).
//! No provider data is committed (<https://londonstrategicedge.com/licence>).
//!
//! Run with: `cargo test --test lse_macro --features lse`

#![cfg(feature = "lse")]
#![allow(clippy::unwrap_used, clippy::expect_used)] // Test code: panics on bad input are acceptable

use chrono::NaiveDate;
use rust_decimal::Decimal;
use rustrade_data::exchange::lse::client::LseClient;
use rustrade_data::exchange::lse::error::LseError;
use rustrade_data::exchange::lse::macroeconomic::{LseMacroFilter, LseMacroKind};
use std::str::FromStr;
use std::time::Duration;
use wiremock::matchers::{method, path, query_param, query_param_is_missing};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn client(server: &MockServer) -> LseClient {
    LseClient::new("test-key")
        .unwrap()
        .with_base_url(format!("{}/v3", server.uri()))
        .with_pace(Duration::ZERO)
}

fn day(text: &str) -> NaiveDate {
    NaiveDate::from_str(text).unwrap()
}

fn json(body: &str) -> ResponseTemplate {
    ResponseTemplate::new(200).set_body_raw(body.to_owned(), "application/json")
}

#[tokio::test]
async fn a_series_read_sends_its_kind_code_and_window() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v3/macro/series"))
        .and(query_param("kind", "yield"))
        .and(query_param("code", "TEST10Y"))
        .and(query_param("from", "2026-11-01"))
        .and(query_param("to", "2026-11-25"))
        .respond_with(json(
            r#"{"code": "TEST10Y", "kind": "yield", "columns": ["day", "close"],
                "from": "2026-11-01", "to": "2026-11-25",
                "rows": [{"day": "2026-11-24", "close": 4.5}]}"#,
        ))
        .expect(1)
        .mount(&server)
        .await;

    let rows = client(&server)
        .fetch_macro_yields("TEST10Y", Some(day("2026-11-01")), Some(day("2026-11-25")))
        .await
        .unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].close, Decimal::from_str("4.5").unwrap());
}

#[tokio::test]
async fn an_open_window_sends_no_bounds_and_a_code_keeps_its_spaces() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v3/macro/series"))
        .and(query_param("kind", "release"))
        .and(query_param("code", "TEST.Note Auction"))
        .and(query_param_is_missing("from"))
        .and(query_param_is_missing("to"))
        .respond_with(json(
            r#"{"rows": [{"released_at": "2026-11-10 18:00:00", "actual": "5.02%",
                "previous": "5.3%", "consensus": null, "forecast": null}]}"#,
        ))
        .expect(1)
        .mount(&server)
        .await;

    let rows = client(&server)
        .fetch_macro_releases("TEST.Note Auction", None, None)
        .await
        .unwrap();

    let (number, unit) = rows[0].actual.as_ref().unwrap().parse().unwrap();
    assert_eq!((number, unit), (Decimal::from_str("5.02").unwrap(), "%"));
}

#[tokio::test]
async fn an_unknown_code_is_the_providers_typed_refusal() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v3/macro/series"))
        .respond_with(ResponseTemplate::new(404).set_body_raw(
            r#"{"code": "series-unknown", "message": "This series is not in the catalogue."}"#,
            "application/json",
        ))
        .mount(&server)
        .await;

    let error = client(&server)
        .fetch_macro_observations("TEST.nothing", None, None)
        .await
        .unwrap_err();

    let unknown = matches!(
        &error,
        LseError::Api { status: 404, code: Some(code), .. } if code == "series-unknown"
    );
    assert!(unknown, "{error}");
}

#[tokio::test]
async fn an_inverted_window_is_refused_before_anything_is_sent() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(json("{}"))
        .expect(0)
        .mount(&server)
        .await;

    let error = client(&server)
        .fetch_macro_observations("TEST", Some(day("2026-11-25")), Some(day("2026-11-24")))
        .await
        .unwrap_err();

    assert!(matches!(error, LseError::InvalidInput { .. }), "{error}");
}

#[tokio::test]
async fn the_catalogue_sends_its_kind_filter_and_page() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v3/macro"))
        .and(query_param("kind", "observation"))
        .and(query_param("country", "US"))
        .and(query_param("q", "inflation"))
        .and(query_param("limit", "2"))
        .and(query_param("offset", "4"))
        .and(query_param_is_missing("code"))
        .respond_with(json(
            r#"{"kind": "observation", "country": "US", "group": null, "total": 5,
                "count": 1, "limit": 2, "offset": 4, "series": [{"code": "US.test",
                "kind": "observation", "country": "US", "name": "Test", "unit": "%",
                "frequency": "monthly", "latest_value": 2.6}]}"#,
        ))
        .expect(1)
        .mount(&server)
        .await;

    let page = client(&server)
        .fetch_macro_catalogue(
            LseMacroKind::Observation,
            LseMacroFilter::default().country("US").search("inflation"),
            2,
            4,
        )
        .await
        .unwrap();

    assert_eq!(page.total, 5);
    assert_eq!(page.series[0].code, "US.test");
    assert_eq!(page.series[0].frequency.as_deref(), Some("monthly"));
}

#[tokio::test]
async fn a_one_day_window_is_not_inverted() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v3/macro/series"))
        .and(query_param("from", "2026-11-25"))
        .and(query_param("to", "2026-11-25"))
        .respond_with(json(
            r#"{"code": "TEST", "kind": "observation", "columns": ["day", "value"],
                "from": "2026-11-25", "to": "2026-11-25", "rows": []}"#,
        ))
        .expect(1)
        .mount(&server)
        .await;

    let rows = client(&server)
        .fetch_macro_observations("TEST", Some(day("2026-11-25")), Some(day("2026-11-25")))
        .await
        .unwrap();

    assert!(rows.is_empty());
}

#[tokio::test]
async fn the_catalogue_filters_by_one_code() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v3/macro"))
        .and(query_param("kind", "yield"))
        .and(query_param("code", "TEST10Y"))
        .and(query_param_is_missing("country"))
        .and(query_param_is_missing("q"))
        .respond_with(json(
            r#"{"kind": "yield", "country": null, "group": null, "total": 1,
                "count": 1, "limit": 1, "offset": 0, "series": [{"code": "TEST10Y",
                "kind": "yield", "country": "US", "name": "Test", "unit": "%",
                "frequency": "daily", "latest_value": 4.5}]}"#,
        ))
        .expect(1)
        .mount(&server)
        .await;

    let page = client(&server)
        .fetch_macro_catalogue(
            LseMacroKind::Yield,
            LseMacroFilter::default().code("TEST10Y"),
            1,
            0,
        )
        .await
        .unwrap();

    assert_eq!(page.series[0].code, "TEST10Y");
}
