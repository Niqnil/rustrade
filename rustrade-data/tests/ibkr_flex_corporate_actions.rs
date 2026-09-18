//! IBKR Flex Web Service corporate-action reconciliation integration tests.
//!
//! These drive the live [`IbkrFlexClient`] against IBKR's Flex Web Service: SendRequest → poll →
//! GetStatement → parse the Corporate Actions section. Unlike the Alpaca corporate-actions tests
//! (which assert against stable, market-wide historical splits), a Flex statement returns whatever
//! corporate actions *this account* experienced over the saved query's date range — which may be
//! none. So these tests assert the round-trip succeeds and parses, and log the rows, rather than
//! asserting specific symbols.
//!
//! # Status
//!
//! Marked `#[ignore]` so they never run (or fail) in CI without credentials. The always-run,
//! hermetic coverage (XML parsing, reorg-type mapping, the 2-call flow's response classification /
//! error paths) lives in the `ibkr::flex` unit tests (`cargo test -p rustrade-data --lib flex
//! --features ibkr`).
//!
//! # Prerequisites
//!
//! 1. An IBKR account (paper is fine).
//! 2. A Flex Web Service token and a saved Activity Flex query that **includes the Corporate
//!    Actions section** (Account Management → Reports → Flex Queries).
//! 3. Environment variables (see `.env.template`):
//!    - `IBKR_FLEX_TOKEN`
//!    - `IBKR_FLEX_QUERY_ID`
//!
//! # Running
//!
//! ```bash
//! source .env
//! cargo test --test ibkr_flex_corporate_actions --features ibkr -- --ignored
//! ```

#![cfg(feature = "ibkr")]
// Test code: unwrap/expect panics are the correct failure mode for test assertions.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use rustrade_data::exchange::ibkr::IbkrFlexClient;

#[tokio::test]
#[ignore = "requires IBKR_FLEX_TOKEN/IBKR_FLEX_QUERY_ID + network"]
async fn fetch_corporate_actions_live() {
    let client =
        IbkrFlexClient::from_env().expect("IBKR_FLEX_TOKEN/IBKR_FLEX_QUERY_ID must be set");

    let actions = client
        .fetch_corporate_actions()
        .await
        .expect("Flex corporate-actions fetch should succeed");

    eprintln!("Fetched {} corporate-action row(s)", actions.len());
    for action in &actions {
        eprintln!(
            "  {:?} {:?} qty_delta={} report_date={:?}",
            action.action_type, action.symbol, action.quantity_delta, action.report_date,
        );
    }
    // No symbol assertion: the rows are whatever this account experienced (possibly none). The
    // value here is proving the live SendRequest → poll → GetStatement → parse round-trip works.
}

#[tokio::test]
#[ignore = "requires IBKR_FLEX_TOKEN/IBKR_FLEX_QUERY_ID + network"]
async fn fetch_statement_xml_live_is_a_flex_query_response() {
    let client =
        IbkrFlexClient::from_env().expect("IBKR_FLEX_TOKEN/IBKR_FLEX_QUERY_ID must be set");

    let xml = client
        .fetch_statement_xml()
        .await
        .expect("Flex statement fetch should succeed");

    assert!(
        xml.contains("<FlexQueryResponse"),
        "the polled statement should be a <FlexQueryResponse> document"
    );
}
