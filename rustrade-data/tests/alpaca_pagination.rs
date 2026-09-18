//! Alpaca REST pagination robustness tests (mocked HTTP, no live API key).
//!
//! Unlike `alpaca_corporate_actions.rs` (which is `#[ignore]` and hits the real
//! Alpaca API), these tests stand up a local `wiremock` server via
//! [`AlpacaRestClient::with_base_urls`] / [`AlpacaOptionsClient::with_base_urls`]
//! and drive the full paginated fetches against crafted `page_token` chains. They
//! verify the `PaginationGuard` wiring end-to-end:
//!
//! - a server that keeps returning the same `next_page_token` is caught as a
//!   cycle (terminal `AlpacaRestError::CyclicPagination`), and
//! - a normal finite `page_token` chain paginates to completion with no false
//!   positive.
//!
//! The cycle test is repeated across **every** paginated fetch rather than a
//! representative one. The guard's own logic is covered by unit tests in
//! `alpaca::pagination`; what these tests protect is the per-call-site *wiring* —
//! a `guard.observe(..)?` omitted from one loop is exactly the copy-paste slip a
//! single representative test would miss, and each loop is hand-written.
//!
//! The numeric page cap (`MAX_PAGES`) is covered by fast, deterministic unit
//! tests in `alpaca::pagination`; exercising it here would mean serving hundreds
//! of pages, so it is intentionally not re-tested through HTTP.

#![cfg(feature = "alpaca")]
// Tests use unwrap/expect for concise failure messages; panics are the intended failure mode.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use futures::{Stream, StreamExt};
use rustrade_data::exchange::alpaca::options::{
    AlpacaOptionContractQuery, AlpacaOptionFeed, AlpacaOptionsClient,
};
use rustrade_data::exchange::alpaca::{AlpacaRestClient, AlpacaRestError, CorporateActionsQuery};
use std::pin::pin;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration as StdDuration;
use wiremock::matchers::method;
use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

/// How long a guarded fetch may run before the test declares it non-terminating.
///
/// The cycle tests mount a mock that answers with a self-repeating `next_page_token` *forever*, so
/// a regression that drops the `guard.observe(..)?` wiring from a pagination loop does not fail —
/// it pages until the (large) page cap instead of erroring on the second round-trip, and with the
/// cap also gone it pages indefinitely. Without this bound such a regression would hang until CI's
/// job timeout, an opaque failure far removed from its cause.
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

fn rest_client_for(server: &MockServer) -> AlpacaRestClient {
    AlpacaRestClient::new("test-key-id", "test-secret", true)
        .expect("client builds with ASCII credentials")
        .with_base_urls(server.uri(), server.uri())
}

fn options_client_for(server: &MockServer) -> AlpacaOptionsClient {
    AlpacaOptionsClient::new("test-key-id", "test-secret", true)
        .expect("client builds with ASCII credentials")
        .with_base_urls(server.uri(), server.uri())
}

/// Mount a catch-all mock whose page always advertises the same `next_page_token` —
/// so echoing it back re-requests an already-used cursor and only the pagination
/// guard can end the fetch.
///
/// The body is a superset of the fields every paginated Alpaca response
/// deserializes (`corporate_actions` / `option_contracts` / `snapshots` /
/// `next_page_token`); each response type takes its payload as defaulted/optional
/// and ignores the fields it does not declare, so one shape serves all of them.
async fn mount_self_repeating_page(server: &MockServer) {
    let body = serde_json::json!({
        "corporate_actions": {},
        "option_contracts": [],
        "snapshots": {},
        "next_page_token": "repeated-cursor",
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
    stream: impl Stream<Item = Result<T, AlpacaRestError>>,
) -> Option<AlpacaRestError> {
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
    fetch: impl Future<Output = Result<T, AlpacaRestError>>,
) -> Result<T, AlpacaRestError> {
    tokio::time::timeout(TERMINATION_TIMEOUT, fetch)
        .await
        .expect("fetch must terminate — a hang means the PaginationGuard wiring is missing")
}

/// Assert that `error` is the terminal cycle error, with the fetch's name in the failure message
/// so a regression names the offending call site directly.
#[track_caller]
fn assert_cycle_detected(fetch: &str, error: Option<AlpacaRestError>) {
    assert!(
        matches!(error, Some(AlpacaRestError::CyclicPagination { .. })),
        "{fetch}: expected CyclicPagination, got {error:?}"
    );
}

// ============================================================================
// Cycle detection — one test per paginated call site
// ============================================================================

/// A `next_page_token` that never changes must terminate the splits stream with
/// `CyclicPagination` rather than paging until the cap.
#[tokio::test]
async fn splits_stream_detects_page_token_cycle() {
    let server = MockServer::start().await;
    mount_self_repeating_page(&server).await;

    let client = rest_client_for(&server);
    let query = CorporateActionsQuery::new().symbols(["NVDA"]);
    let error = drain_to_terminal_error(client.fetch_splits_raw(&query)).await;

    assert_cycle_detected("fetch_splits_raw", error);
    // Page 1 (no cursor) + page 2 (cursor's first use) are fetched; the third
    // request — the cursor's second use — is rejected before it is issued.
    assert_eq!(
        server.received_requests().await.unwrap().len(),
        2,
        "the cycle must be detected before a third request is issued"
    );
}

#[tokio::test]
async fn option_contracts_detects_page_token_cycle() {
    let server = MockServer::start().await;
    mount_self_repeating_page(&server).await;

    let client = options_client_for(&server);
    let query = AlpacaOptionContractQuery::new(vec!["AAPL".into()]);
    let result = await_bounded(client.fetch_contracts(&query)).await;

    assert_cycle_detected("fetch_contracts", result.err());
}

#[tokio::test]
async fn option_snapshots_detects_page_token_cycle() {
    let server = MockServer::start().await;
    mount_self_repeating_page(&server).await;

    let client = options_client_for(&server);
    let symbols = vec!["AAPL240119C00150000".to_string()];
    let result =
        await_bounded(client.fetch_snapshots(&symbols, AlpacaOptionFeed::Indicative)).await;

    assert_cycle_detected("fetch_snapshots", result.err());
}

// ============================================================================
// No-false-positive baselines (also prove each base-URL override is honored)
// ============================================================================

/// A normal finite `page_token` chain paginates to completion: every item is yielded, the
/// stream ends cleanly (no error), and the guard does not false-positive on legitimately
/// distinct tokens. Also proves the `data_base` override routes `fetch_splits_raw`.
#[tokio::test]
async fn splits_stream_paginates_finite_chain_without_false_positive() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(Sequential::new(vec![
            serde_json::json!({
                "corporate_actions": {
                    "forward_splits": [{
                        "symbol": "NVDA", "new_rate": 10, "old_rate": 1,
                        "ex_date": "2024-06-10",
                    }]
                },
                "next_page_token": "page-2",
            }),
            serde_json::json!({
                "corporate_actions": {
                    "reverse_splits": [{
                        "symbol": "ATRA", "new_rate": 1, "old_rate": 25,
                        "ex_date": "2024-06-20",
                    }]
                },
                "next_page_token": serde_json::Value::Null,
            }),
        ]))
        .mount(&server)
        .await;

    let client = rest_client_for(&server);
    let query = CorporateActionsQuery::new();
    let mut stream = pin!(client.fetch_splits_raw(&query));

    let mut splits = Vec::new();
    while let Some(item) = stream.next().await {
        splits.push(item.expect("no error on a well-formed finite chain"));
    }

    let symbols: Vec<_> = splits.iter().map(|s| s.symbol.as_str()).collect();
    assert_eq!(symbols, vec!["NVDA", "ATRA"]);
    assert_eq!(
        server.received_requests().await.unwrap().len(),
        2,
        "exactly two pages should have been fetched"
    );
}

/// The `Vec`-returning counterpart for the options side. Also proves the `broker_base`
/// override routes `fetch_contracts` (the one paginated fetch on the broker API).
#[tokio::test]
async fn option_contracts_paginate_finite_chain_without_false_positive() {
    let server = MockServer::start().await;
    let contract = |symbol: &str| {
        serde_json::json!({
            "id": format!("id-{symbol}"),
            "symbol": symbol,
            "name": format!("{symbol} option"),
            "status": "active",
            "tradable": true,
            "expiration_date": "2024-01-19",
            "root_symbol": "AAPL",
            "underlying_symbol": "AAPL",
            "underlying_asset_id": "asset-1",
            "type": "call",
            "style": "american",
            "strike_price": "150",
            "size": "100",
        })
    };
    Mock::given(method("GET"))
        .respond_with(Sequential::new(vec![
            serde_json::json!({
                "option_contracts": [contract("AAPL240119C00150000")],
                "next_page_token": "page-2",
            }),
            serde_json::json!({
                "option_contracts": [contract("AAPL240119C00155000")],
                "next_page_token": serde_json::Value::Null,
            }),
        ]))
        .mount(&server)
        .await;

    let client = options_client_for(&server);
    let query = AlpacaOptionContractQuery::new(vec!["AAPL".into()]);
    let contracts = await_bounded(client.fetch_contracts(&query))
        .await
        .expect("no error on a well-formed finite chain");

    let symbols: Vec<_> = contracts.iter().map(|c| c.symbol.as_str()).collect();
    assert_eq!(symbols, vec!["AAPL240119C00150000", "AAPL240119C00155000"]);
    assert_eq!(
        server.received_requests().await.unwrap().len(),
        2,
        "exactly two pages should have been fetched"
    );
}
