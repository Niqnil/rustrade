//! Wire-level tests for the London Strategic Edge economic-calendar endpoints.
//!
//! Every response here is **synthetic**, shaped from measurements of the live API. No provider data
//! is committed — the provider prohibits redistribution
//! (<https://londonstrategicedge.com/terms>), so `wiremock` is the only in-repo option. The live
//! API is exercised separately by `lse_economic_calendar_canary`, which asserts against the real
//! service without storing anything.
//!
//! What these cover that the unit tests in `calendar.rs` cannot: the request that actually leaves
//! the client. The endpoint answers `200` with zero rows for an unknown country, an unknown impact
//! rating, a reversed range and an empty window alike, so a parameter this client gets wrong is
//! invisible in the response — the only place to catch it is here, against a mock that asserts on
//! the query string.
//!
//! 🔴 The single most important of those is the country list. The provider treats a **repeated**
//! `country` key by silently keeping only the last value, so a client that repeated the key would
//! return a wrong-but-plausible subset with no error anywhere. Only a query-string assertion
//! catches that.
//!
//! Run with: `cargo test --test lse_economic_calendar --features lse`

#![cfg(feature = "lse")]
#![allow(clippy::unwrap_used, clippy::expect_used)] // Test code: panics on bad input are acceptable

use chrono::NaiveDate;
use rust_decimal_macros::dec;
use rustrade_data::exchange::lse::calendar::{
    LseCalendarImpact, LseCalendarQuery, LseCalendarStats,
};
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

/// A stats body shaped like the provider's, with invented counts.
///
/// Reproduces the three structures the validation rules turn on: the United Kingdom keyed `UK`
/// with no `GB` entry, non-country codes (`EA`, `EU`) among the vocabulary, and the flat global
/// `earliest`/`latest` pair that is the *only* coverage the provider publishes.
const STATS_BODY: &str = r#"{
    "countries": ["EA", "EU", "JP", "UK", "US"],
    "impacts": ["High", "Low", "Medium", "None"],
    "earliest": "2014-12-31",
    "latest": "2026-03-24",
    "total_events": 124896
}"#;

/// One event, in the provider's encoding: every numeric a JSON string, absences spelled `null`.
fn event(event_date: &str, country: &str, impact: &str) -> String {
    format!(
        r#"{{
            "event_date": "{event_date}",
            "country": "{country}",
            "event": "CPI (YoY)",
            "currency": "USD",
            "previous": "2.9",
            "estimate": null,
            "actual": "-3.1",
            "change": "-0.2",
            "change_percentage": "-6.9",
            "impact": "{impact}",
            "unit": "%"
        }}"#
    )
}

fn envelope(events: &[String]) -> String {
    format!(
        r#"{{"count": {}, "data": [{}]}}"#,
        events.len(),
        events.join(",")
    )
}

async fn mount_stats(server: &MockServer) {
    Mock::given(method("GET"))
        .and(path("/economic-calendar/stats"))
        .and(query_param("format", "json"))
        .respond_with(ResponseTemplate::new(200).set_body_string(STATS_BODY))
        .mount(server)
        .await;
}

async fn stats(server: &MockServer) -> LseCalendarStats {
    mount_stats(server).await;
    client(server)
        .fetch_economic_calendar_stats()
        .await
        .unwrap()
}

#[tokio::test]
async fn stats_decode_from_the_providers_envelope() {
    let server = MockServer::start().await;
    let stats = stats(&server).await;

    assert_eq!(stats.total_events, 124_896);
    assert_eq!(stats.countries.len(), 5);
    assert_eq!(stats.earliest_date().unwrap(), date(2014, 12, 31));
    assert_eq!(stats.latest_date().unwrap(), date(2026, 3, 24));
    assert!(stats.publishes_country("GB"));
}

/// 🔴 The single most consequential assertion in this file. Several countries must reach the
/// provider as ONE comma-joined `country` parameter: a repeated key is silently reduced to its last
/// value, which would return a plausible subset with no error. The mock answers only the joined
/// form, so a repeated key fails the test instead of quietly halving the result.
#[tokio::test]
async fn several_countries_reach_the_provider_comma_joined_and_not_repeated() {
    let server = MockServer::start().await;
    let stats = stats(&server).await;

    Mock::given(method("GET"))
        .and(path("/economic-calendar"))
        .and(query_param("country", "US,UK"))
        .and(query_param("start_date", "2026-01-01"))
        .and(query_param("end_date", "2026-03-24"))
        .and(query_param("format", "json"))
        .respond_with(ResponseTemplate::new(200).set_body_string(envelope(&[
            event("2026-01-16 14:00:00+00:00", "US", "High"),
            event("2026-02-16 09:30:00+00:00", "UK", "Medium"),
        ])))
        .mount(&server)
        .await;

    let query =
        LseCalendarQuery::new(date(2026, 1, 1), date(2026, 3, 24)).with_countries(["US", "UK"]);
    let events = client(&server)
        .fetch_economic_calendar(&stats, &query)
        .await
        .unwrap();

    assert_eq!(events.len(), 2);
    assert_eq!(events[0].country, "US");
    assert_eq!(events[1].country, "UK");
}

/// 🔴 A caller's ISO-3166 `GB` must reach the provider as `UK`; sending it unchanged returns `200`
/// with zero events and no error, indistinguishable from a quiet window. The mock only answers when
/// the query carries `UK`.
#[tokio::test]
async fn a_caller_supplied_gb_reaches_the_provider_as_uk() {
    let server = MockServer::start().await;
    let stats = stats(&server).await;

    Mock::given(method("GET"))
        .and(path("/economic-calendar"))
        .and(query_param("country", "UK"))
        .and(query_param("format", "json"))
        .respond_with(ResponseTemplate::new(200).set_body_string(envelope(&[event(
            "2026-02-16 09:30:00+00:00",
            "UK",
            "High",
        )])))
        .mount(&server)
        .await;

    let query = LseCalendarQuery::new(date(2026, 1, 1), date(2026, 3, 24)).with_country("GB");
    let events = client(&server)
        .fetch_economic_calendar(&stats, &query)
        .await
        .unwrap();

    assert_eq!(events.len(), 1);
    assert_eq!(events[0].country, "UK");
}

/// An event decodes end to end: string-encoded numerics, negatives, a `null` estimate, and the
/// timestamp's time of day preserved rather than truncated to a date.
#[tokio::test]
async fn an_event_decodes_with_its_timestamp_and_string_encoded_numerics() {
    let server = MockServer::start().await;
    let stats = stats(&server).await;

    Mock::given(method("GET"))
        .and(path("/economic-calendar"))
        .respond_with(ResponseTemplate::new(200).set_body_string(envelope(&[event(
            "2026-03-24 14:30:00+00:00",
            "US",
            "High",
        )])))
        .mount(&server)
        .await;

    let query = LseCalendarQuery::new(date(2026, 1, 1), date(2026, 3, 24));
    let events = client(&server)
        .fetch_economic_calendar(&stats, &query)
        .await
        .unwrap();

    let event = &events[0];
    assert_eq!(event.previous, Some(dec!(2.9)));
    assert_eq!(event.estimate, None);
    assert_eq!(event.actual, Some(dec!(-3.1)));
    assert_eq!(event.impact, LseCalendarImpact::High);
    assert_eq!(event.unit.as_deref(), Some("%"));

    // The time of day is the point: 348 distinct ones occur, and a date would lose the ordering.
    assert_eq!(
        event.event_time().unwrap().to_rfc3339(),
        "2026-03-24T14:30:00+00:00"
    );
    assert_eq!(event.event_day().unwrap(), date(2026, 3, 24));
}

/// The impact filter must travel in the provider's own spelling, resolved from the published
/// vocabulary rather than passed through as the caller typed it.
#[tokio::test]
async fn an_impact_filter_is_sent_in_the_providers_spelling() {
    let server = MockServer::start().await;
    let stats = stats(&server).await;

    Mock::given(method("GET"))
        .and(path("/economic-calendar"))
        .and(query_param("impact", "High"))
        .respond_with(ResponseTemplate::new(200).set_body_string(envelope(&[])))
        .mount(&server)
        .await;

    let query = LseCalendarQuery::new(date(2026, 1, 1), date(2026, 3, 24)).with_impact("high");

    assert!(
        client(&server)
            .fetch_economic_calendar(&stats, &query)
            .await
            .is_ok()
    );
}

/// `format` is not optional: absent it, the endpoint serves CSV to a JSON decoder.
#[tokio::test]
async fn the_json_format_parameter_is_always_sent() {
    let server = MockServer::start().await;
    let stats = stats(&server).await;

    Mock::given(method("GET"))
        .and(path("/economic-calendar"))
        .and(query_param("format", "json"))
        .respond_with(ResponseTemplate::new(200).set_body_string(envelope(&[])))
        .mount(&server)
        .await;

    let query = LseCalendarQuery::new(date(2026, 1, 1), date(2026, 3, 24));

    assert!(
        client(&server)
            .fetch_economic_calendar(&stats, &query)
            .await
            .is_ok()
    );
}

/// An absent filter is omitted rather than sent empty. The endpoint answers an unknown filter with
/// zero rows rather than an error, so `country=` would be a silent empty result.
#[tokio::test]
async fn omitted_filters_are_absent_from_the_query_rather_than_empty() {
    let server = MockServer::start().await;
    let stats = stats(&server).await;

    Mock::given(method("GET"))
        .and(path("/economic-calendar"))
        .and(query_param_is_missing("country"))
        .and(query_param_is_missing("impact"))
        .respond_with(ResponseTemplate::new(200).set_body_string(envelope(&[])))
        .mount(&server)
        .await;

    let query = LseCalendarQuery::new(date(2026, 1, 1), date(2026, 3, 24));

    assert!(
        client(&server)
            .fetch_economic_calendar(&stats, &query)
            .await
            .is_ok()
    );
}

/// 🔴 The deliberate non-check. A published country with no events in the window returns an empty
/// envelope, and that must be `Ok(vec![])` rather than an error: the provider publishes no
/// per-country coverage, so the library cannot tell it from a quiet period.
#[tokio::test]
async fn an_empty_result_inside_the_published_range_is_not_an_error() {
    let server = MockServer::start().await;
    let stats = stats(&server).await;

    Mock::given(method("GET"))
        .and(path("/economic-calendar"))
        .respond_with(ResponseTemplate::new(200).set_body_string(envelope(&[])))
        .mount(&server)
        .await;

    // `UK` holds 56 events on the live feed, all in early 2026, so this window really is empty.
    let query = LseCalendarQuery::new(date(2020, 1, 1), date(2020, 12, 31)).with_country("UK");
    let events = client(&server)
        .fetch_economic_calendar(&stats, &query)
        .await
        .expect("an empty window inside the global range must not be an error");

    assert!(events.is_empty());
}

/// The truncation tripwire: `count` disagreeing with the events delivered is the signal a silently
/// introduced page cap would produce.
#[tokio::test]
async fn a_count_that_disagrees_with_the_events_delivered_is_an_error() {
    let server = MockServer::start().await;
    let stats = stats(&server).await;

    Mock::given(method("GET"))
        .and(path("/economic-calendar"))
        .respond_with(ResponseTemplate::new(200).set_body_string(format!(
            r#"{{"count": 9999, "data": [{}]}}"#,
            event("2026-03-24 14:00:00+00:00", "US", "High")
        )))
        .mount(&server)
        .await;

    let query = LseCalendarQuery::new(date(2026, 1, 1), date(2026, 3, 24));
    let error = client(&server)
        .fetch_economic_calendar(&stats, &query)
        .await
        .unwrap_err();

    let LseError::Api { status, message } = &error else {
        panic!("expected Api, got {error:?}");
    };
    assert_eq!(*status, 200);
    assert!(message.contains("truncated"), "{message}");
}

/// Validation runs before the network, so an invalid query costs no request. The mock is mounted
/// with an `expect(0)` so any request at all fails the test on drop.
#[tokio::test]
async fn an_invalid_query_never_reaches_the_network() {
    let server = MockServer::start().await;
    let stats = stats(&server).await;

    Mock::given(method("GET"))
        .and(path("/economic-calendar"))
        .respond_with(ResponseTemplate::new(200).set_body_string(envelope(&[])))
        .expect(0)
        .mount(&server)
        .await;

    let client = client(&server);

    // An unpublished country.
    let unknown_country =
        LseCalendarQuery::new(date(2026, 1, 1), date(2026, 3, 24)).with_country("ZZ");
    assert!(matches!(
        client
            .fetch_economic_calendar(&stats, &unknown_country)
            .await
            .unwrap_err(),
        LseError::UnknownCalendarCountry { .. }
    ));

    // An unpublished impact rating.
    let unknown_impact =
        LseCalendarQuery::new(date(2026, 1, 1), date(2026, 3, 24)).with_impact("Critical");
    assert!(matches!(
        client
            .fetch_economic_calendar(&stats, &unknown_impact)
            .await
            .unwrap_err(),
        LseError::UnknownCalendarImpact { .. }
    ));

    // 🔴 A forward-looking window, which on this frozen feed is every future window.
    let after_the_freeze = LseCalendarQuery::new(date(2026, 4, 1), date(2026, 12, 31));
    assert!(matches!(
        client
            .fetch_economic_calendar(&stats, &after_the_freeze)
            .await
            .unwrap_err(),
        LseError::CalendarRangeOutsideCoverage { .. }
    ));

    // A reversed range, which the provider answers `200 count=0` rather than rejecting.
    let inverted = LseCalendarQuery::new(date(2026, 3, 24), date(2026, 1, 1));
    assert!(matches!(
        client
            .fetch_economic_calendar(&stats, &inverted)
            .await
            .unwrap_err(),
        LseError::InvalidInput { .. }
    ));
}

#[tokio::test]
async fn a_provider_error_is_reported_with_its_status_and_detail() {
    let server = MockServer::start().await;
    let stats = stats(&server).await;

    // A malformed date is the one input this endpoint answers with a 500 rather than a `200`.
    // Typed dates make it unreachable from here, but the mapping is asserted regardless.
    Mock::given(method("GET"))
        .and(path("/economic-calendar"))
        .respond_with(
            ResponseTemplate::new(500).set_body_string(r#"{"detail": "Internal Server Error"}"#),
        )
        .mount(&server)
        .await;

    let query = LseCalendarQuery::new(date(2026, 1, 1), date(2026, 3, 24));
    let error = client(&server)
        .fetch_economic_calendar(&stats, &query)
        .await
        .unwrap_err();

    let LseError::Api { status, message } = &error else {
        panic!("expected Api, got {error:?}");
    };
    assert_eq!(*status, 500);
    assert!(message.contains("Internal Server Error"), "{message}");
}

#[tokio::test]
async fn a_rate_limit_is_typed_and_carries_its_retry_after() {
    let server = MockServer::start().await;
    let stats = stats(&server).await;

    Mock::given(method("GET"))
        .and(path("/economic-calendar"))
        .respond_with(
            ResponseTemplate::new(429)
                .insert_header("Retry-After", "30")
                .set_body_string(r#"{"detail": "rate limited"}"#),
        )
        .mount(&server)
        .await;

    let query = LseCalendarQuery::new(date(2026, 1, 1), date(2026, 3, 24));
    match client(&server)
        .fetch_economic_calendar(&stats, &query)
        .await
        .unwrap_err()
    {
        LseError::RateLimited { retry_after } => {
            assert_eq!(retry_after, Some(Duration::from_secs(30)));
        }
        other => panic!("expected RateLimited, got {other}"),
    }
}
