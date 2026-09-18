//! Massive REST pagination robustness tests (mocked HTTP, no live API key).
//!
//! Unlike `massive_integration.rs` (which is `#[ignore]` and hits the real Massive
//! API), these tests stand up a local `wiremock` server via
//! [`MassiveRestClient::with_base_url`] and drive the full paginated stream against
//! crafted `next_url` chains. They verify the [`PaginationGuard`] wiring end-to-end:
//!
//! - a server that keeps returning the same `next_url` is caught as a cycle
//!   (terminal `MassiveError::CyclicPagination`), and
//! - a normal finite `next_url` chain paginates to completion with no false
//!   positive.
//!
//! The cycle test is repeated across **every** paginated fetch rather than a
//! representative one. The guard's own logic is covered by unit tests in
//! `massive::pagination`; what these tests protect is the per-call-site *wiring* —
//! a `guard.observe(&url)?` omitted from one loop is exactly the copy-paste slip a
//! single representative test would miss, and each loop is hand-written.
//!
//! The numeric page cap (`MAX_PAGES`) is covered by fast, deterministic unit tests
//! in `massive::pagination`; exercising it here would mean serving thousands of
//! pages, so it is intentionally not re-tested through HTTP.

#![cfg(feature = "massive")]
// Tests use unwrap/expect for concise failure messages; panics are the intended failure mode.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use chrono::{Duration, Utc};
use futures_util::{Stream, StreamExt};
use rustrade_data::exchange::massive::{
    DividendQuery, MassiveError, MassiveRestClient, OptionContractQuery, OptionSnapshotQuery,
    SplitQuery, TickerQuery,
};
use std::pin::pin;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration as StdDuration;
use wiremock::matchers::{header, method};
use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

/// How long a guarded fetch may run before the test declares it non-terminating.
///
/// The cycle tests mount a mock that answers a self-referential `next_url` *forever*, so a
/// regression that drops the `guard.observe(&url)?` wiring from a pagination loop does not fail —
/// it pages indefinitely. Without this bound such a regression would hang until CI's job timeout,
/// an opaque failure far removed from its cause. This turns "the guard is gone" into an explicit,
/// fast assertion.
///
/// Generous relative to a local wiremock round-trip (milliseconds), so it can only fire on genuine
/// non-termination, never on a slow machine.
const TERMINATION_TIMEOUT: StdDuration = StdDuration::from_secs(10);

/// Serves pre-configured JSON pages in registration order.
///
/// Each request advances an atomic counter and returns the next page body,
/// panicking if called more times than pages were supplied — so an unexpected
/// extra request surfaces as an explicit test failure rather than a stale reply.
struct Sequential {
    call: AtomicU32,
    pages: Vec<serde_json::Value>,
}

impl Sequential {
    fn new(pages: Vec<serde_json::Value>) -> Self {
        Self {
            call: AtomicU32::new(0),
            pages,
        }
    }
}

impl Respond for Sequential {
    fn respond(&self, _: &Request) -> ResponseTemplate {
        let i = self.call.fetch_add(1, Ordering::Relaxed) as usize;
        let body = self.pages.get(i).unwrap_or_else(|| {
            panic!(
                "Sequential: request #{i} has no configured response (only {} page(s) supplied)",
                self.pages.len()
            )
        });
        ResponseTemplate::new(200).set_body_json(body)
    }
}

fn client_for(server: &MockServer) -> MassiveRestClient {
    MassiveRestClient::new("test_api_key")
        .expect("client builds")
        .with_base_url(server.uri())
        .expect("mock server uri is a valid base url")
}

/// Mount a catch-all mock that answers every request with an empty page whose `next_url`
/// points at one fixed URL under the mock's own origin — so following it revisits the same
/// URL and only the [`PaginationGuard`] can end the stream.
///
/// The body is a superset of the fields every Massive page response deserializes
/// (`resultsCount`/`results`/`next_url`/`status`); each endpoint's response type takes
/// `results` as an `Option<Vec<_>>` and ignores the fields it does not declare, so one shape
/// serves all of them.
async fn mount_self_referential_page(server: &MockServer) {
    let loop_url = format!("{}/loop", server.uri());
    let body = serde_json::json!({
        "resultsCount": 0,
        "results": [],
        "next_url": loop_url,
        "status": "OK",
    });
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_json(body))
        .mount(server)
        .await;
}

/// Drive `stream` to completion, returning its terminal error if it yielded one.
///
/// Fails the test if the stream has not terminated within [`TERMINATION_TIMEOUT`] — see that
/// constant for why an unbounded drain would fail opaquely against the cycle mocks.
async fn drain_to_terminal_error<T>(
    stream: impl Stream<Item = Result<T, MassiveError>>,
) -> Option<MassiveError> {
    let drain = async {
        let mut stream = pin!(stream);
        while let Some(item) = stream.next().await {
            if let Err(error) = item {
                return Some(error);
            }
        }
        None
    };
    tokio::time::timeout(TERMINATION_TIMEOUT, drain)
        .await
        .expect("stream must terminate — a hang means the PaginationGuard wiring is missing")
}

/// [`drain_to_terminal_error`]'s counterpart for the `Vec`-returning fetches, which collect
/// eagerly instead of streaming and so have no terminal item to drain to.
async fn await_bounded<T>(
    fetch: impl Future<Output = Result<T, MassiveError>>,
) -> Result<T, MassiveError> {
    tokio::time::timeout(TERMINATION_TIMEOUT, fetch)
        .await
        .expect("fetch must terminate — a hang means the PaginationGuard wiring is missing")
}

/// Assert that `error` is the terminal cycle error, with the fetch's name in the failure message
/// so a regression names the offending call site directly.
#[track_caller]
fn assert_cycle_detected(fetch: &str, error: Option<MassiveError>) {
    assert!(
        matches!(error, Some(MassiveError::CyclicPagination { .. })),
        "{fetch}: expected CyclicPagination, got {error:?}"
    );
}

// ============================================================================
// Cycle detection — one test per paginated call site
// ============================================================================

/// A `next_url` that always points back to the same page must terminate the stream
/// with `CyclicPagination` rather than looping forever.
#[tokio::test]
async fn aggregates_stream_detects_next_url_cycle() {
    let server = MockServer::start().await;
    mount_self_referential_page(&server).await;

    let client = client_for(&server);
    let to = Utc::now();
    let from = to - Duration::minutes(5);
    let error =
        drain_to_terminal_error(client.fetch_aggregates("X:BTCUSD", 1, "minute", from, to)).await;

    assert_cycle_detected("fetch_aggregates", error);
}

#[tokio::test]
async fn trades_stream_detects_next_url_cycle() {
    let server = MockServer::start().await;
    mount_self_referential_page(&server).await;

    let client = client_for(&server);
    let to = Utc::now();
    let from = to - Duration::minutes(5);
    let error = drain_to_terminal_error(client.fetch_trades("X:BTCUSD", from, to)).await;

    assert_cycle_detected("fetch_trades", error);
}

#[tokio::test]
async fn quotes_stream_detects_next_url_cycle() {
    let server = MockServer::start().await;
    mount_self_referential_page(&server).await;

    let client = client_for(&server);
    let to = Utc::now();
    let from = to - Duration::minutes(5);
    let error = drain_to_terminal_error(client.fetch_quotes("X:BTCUSD", from, to)).await;

    assert_cycle_detected("fetch_quotes", error);
}

#[tokio::test]
async fn dividends_stream_detects_next_url_cycle() {
    let server = MockServer::start().await;
    mount_self_referential_page(&server).await;

    let client = client_for(&server);
    let query = DividendQuery::new();
    let error = drain_to_terminal_error(client.fetch_dividends(&query)).await;

    assert_cycle_detected("fetch_dividends", error);
}

#[tokio::test]
async fn splits_stream_detects_next_url_cycle() {
    let server = MockServer::start().await;
    mount_self_referential_page(&server).await;

    let client = client_for(&server);
    let query = SplitQuery::new();
    let error = drain_to_terminal_error(client.fetch_splits_raw(&query)).await;

    assert_cycle_detected("fetch_splits_raw", error);
}

#[tokio::test]
async fn tickers_stream_detects_next_url_cycle() {
    let server = MockServer::start().await;
    mount_self_referential_page(&server).await;

    let client = client_for(&server);
    let query = TickerQuery::new();
    let error = drain_to_terminal_error(client.fetch_tickers(&query)).await;

    assert_cycle_detected("fetch_tickers", error);
}

/// The `Vec`-returning fetches (`fetch_option_contracts` / `fetch_option_chain_snapshot`)
/// collect eagerly rather than streaming, so the guard error must propagate through the
/// plain `async fn -> Result<Vec<_>>` return path. A self-referential `next_url` must fail
/// with `CyclicPagination` instead of growing the `Vec` without bound.
#[tokio::test]
async fn option_contracts_detects_next_url_cycle() {
    let server = MockServer::start().await;
    mount_self_referential_page(&server).await;

    let client = client_for(&server);
    let query = OptionContractQuery::new();
    let result = await_bounded(client.fetch_option_contracts(&query)).await;

    assert_cycle_detected("fetch_option_contracts", result.err());
}

#[tokio::test]
async fn option_chain_snapshot_detects_next_url_cycle() {
    let server = MockServer::start().await;
    mount_self_referential_page(&server).await;

    let client = client_for(&server);
    let query = OptionSnapshotQuery::new();
    let result = await_bounded(client.fetch_option_chain_snapshot("AAPL", &query)).await;

    assert_cycle_detected("fetch_option_chain_snapshot", result.err());
}

// ============================================================================
// Origin validation, redirects, and the no-false-positive baseline
// ============================================================================

/// A normal finite `next_url` chain paginates to completion: every item is yielded,
/// the stream ends cleanly (no error), and the guard does not false-positive on
/// legitimately distinct page URLs.
#[tokio::test]
async fn tickers_stream_paginates_finite_chain_without_false_positive() {
    let server = MockServer::start().await;
    let page2_url = format!("{}/v3/reference/tickers?cursor=page2", server.uri());
    Mock::given(method("GET"))
        .respond_with(Sequential::new(vec![
            serde_json::json!({
                "results": [{ "ticker": "AAA" }],
                "next_url": page2_url,
                "status": "OK",
            }),
            serde_json::json!({
                "results": [{ "ticker": "BBB" }],
                "next_url": serde_json::Value::Null,
                "status": "OK",
            }),
        ]))
        .mount(&server)
        .await;

    let client = client_for(&server);
    let query = TickerQuery::new();
    let mut stream = pin!(client.fetch_tickers(&query));

    let mut tickers = Vec::new();
    while let Some(item) = stream.next().await {
        tickers.push(item.expect("no error on a well-formed finite chain"));
    }

    let symbols: Vec<_> = tickers.iter().map(|t| t.ticker.as_str()).collect();
    assert_eq!(symbols, vec!["AAA", "BBB"]);
    assert_eq!(
        server.received_requests().await.unwrap().len(),
        2,
        "exactly two pages should have been fetched"
    );
}

/// A `next_url` pointing at a *different origin* must terminate the stream with
/// `UntrustedNextUrl` and — the load-bearing guarantee (#182) — the client must
/// never issue a request to that origin, so the `Authorization: Bearer` token is
/// never leaked.
///
/// The two mock servers share host `127.0.0.1` but bind different ports, so they
/// are distinct origins (origin = scheme + host + port); the malicious page under
/// the trusted origin hands back a `next_url` on the attacker's origin.
#[tokio::test]
async fn aggregates_stream_rejects_cross_origin_next_url_without_leaking_token() {
    // The attacker-controlled origin. Its catch-all mock would happily answer
    // (and thus would have received the bearer token) *if* the client ever
    // fetched it — the test asserts it never does.
    let attacker = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "resultsCount": 0,
            "results": [],
            "next_url": serde_json::Value::Null,
        })))
        .mount(&attacker)
        .await;

    // The trusted origin returns one empty page whose `next_url` points off-origin
    // at the attacker.
    let primary = MockServer::start().await;
    let evil_next = format!("{}/v2/aggs/steal?cursor=abc", attacker.uri());
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "resultsCount": 0,
            "results": [],
            "next_url": evil_next,
        })))
        .mount(&primary)
        .await;

    let client = client_for(&primary);
    let to = Utc::now();
    let from = to - Duration::minutes(5);
    let terminal_err =
        drain_to_terminal_error(client.fetch_aggregates("X:BTCUSD", 1, "minute", from, to)).await;

    assert!(
        matches!(terminal_err, Some(MassiveError::UntrustedNextUrl { .. })),
        "expected UntrustedNextUrl, got {terminal_err:?}"
    );
    assert!(
        attacker.received_requests().await.unwrap().is_empty(),
        "client must not issue any request to the untrusted origin (token would leak)"
    );
}

/// The positive counterpart to the cross-origin test: on the trusted origin the
/// client *must* attach the `Authorization: Bearer <key>` header (#198 moved it off
/// the reqwest default headers to a per-request attachment). The mock only matches
/// when that header is present, so a clean stream completion proves the token rode
/// the request; had it not been attached, the request would miss the mock and come
/// back as a `MassiveError::Api { status: 404 }`.
#[tokio::test]
async fn aggregates_request_attaches_bearer_token_to_trusted_origin() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(header("authorization", "Bearer test_api_key"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "resultsCount": 0,
            "results": [],
            "next_url": serde_json::Value::Null,
        })))
        .mount(&server)
        .await;

    let client = client_for(&server);
    let to = Utc::now();
    let from = to - Duration::minutes(5);
    let mut stream = pin!(client.fetch_aggregates("X:BTCUSD", 1, "minute", from, to));

    while let Some(item) = stream.next().await {
        item.expect("request with bearer token should be accepted by the trusted origin");
    }

    assert_eq!(
        server.received_requests().await.unwrap().len(),
        1,
        "expected exactly one authenticated request to the trusted origin"
    );
}

/// The origin guard vets the URL the client *requests*, but a trusted server could
/// still answer with a 3xx redirect to another origin. The client disables redirect
/// following (`redirect::Policy::none()`), so the token can never ride a
/// server-issued redirect off-origin: the 3xx is returned unfollowed and surfaces
/// as a terminal `MassiveError::Api`, and — the load-bearing guarantee — the
/// attacker origin receives no request. This keeps the "token never leaves the
/// trusted origin" property from depending on reqwest's internal header stripping.
#[tokio::test]
async fn aggregates_stream_does_not_follow_redirect_off_origin() {
    // Same catch-all attacker origin as the cross-origin test — it would answer
    // (and receive the bearer token) if the client ever followed the redirect.
    let attacker = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "resultsCount": 0,
            "results": [],
            "next_url": serde_json::Value::Null,
        })))
        .mount(&attacker)
        .await;

    // The trusted origin redirects the very first request off-origin at the attacker.
    let primary = MockServer::start().await;
    let evil_location = format!("{}/v2/aggs/steal", attacker.uri());
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(302).insert_header("location", evil_location.as_str()))
        .mount(&primary)
        .await;

    let client = client_for(&primary);
    let to = Utc::now();
    let from = to - Duration::minutes(5);
    let terminal_err =
        drain_to_terminal_error(client.fetch_aggregates("X:BTCUSD", 1, "minute", from, to)).await;

    assert!(
        matches!(terminal_err, Some(MassiveError::Api { status, .. }) if (300..400).contains(&status)),
        "expected an unfollowed 3xx as MassiveError::Api, got {terminal_err:?}"
    );
    assert!(
        attacker.received_requests().await.unwrap().is_empty(),
        "client must not follow the redirect to the untrusted origin (token would leak)"
    );
}
