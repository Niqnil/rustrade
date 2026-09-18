//! Alpaca corporate-actions (stock-split) source integration tests.
//!
//! These tests drive the live `StockSplitSource` impl on `AlpacaRestClient` against Alpaca's
//! `GET /v1beta1/corporate-actions` endpoint. The endpoint is historical reference data and is
//! available on the **free/Basic (paper) plan** — unlike SIP/OPRA, it is NOT gated behind Algo
//! Trader Plus.
//!
//! # Status
//!
//! Marked `#[ignore]` so they never run (or fail) in CI without credentials. The always-run,
//! hermetic coverage (ratio mapping, the `ex_date`-vs-`payable_date` provenance pin, response /
//! pagination parsing, degenerate-ratio skip) lives in the `alpaca::reference` and
//! `alpaca::corporate_action` unit tests.
//!
//! # Prerequisites
//!
//! 1. Alpaca account (paper is fine; https://app.alpaca.markets).
//! 2. Environment variables (see `.env.template`):
//!    - `ALPACA_API_KEY`
//!    - `ALPACA_SECRET_KEY`
//!
//! # Running
//!
//! ```bash
//! source .env
//! cargo test --test alpaca_corporate_actions --features alpaca -- --ignored
//! ```
//!
//! The fixtures these assert against are real, stable historical splits, so unlike the market-data
//! streaming tests they do not depend on market hours.

#![cfg(feature = "alpaca")]
// Test code: unwrap/expect panics are the correct failure mode for test assertions.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use chrono::NaiveDate;
use futures::StreamExt;
use rust_decimal::Decimal;
use rustrade_data::exchange::alpaca::AlpacaRestClient;
use rustrade_instrument::corporate_action::{CorporateAction, CorporateActionKind, SplitRatio};
use rustrade_integration::corporate_action::{CorporateActionFilter, StockSplitSource};
use smol_str::SmolStr;

/// Drain a [`StockSplitSource`] stream, panicking on the first per-item error.
async fn collect<S>(source: &S, filter: &CorporateActionFilter) -> Vec<CorporateAction<SmolStr>>
where
    S: StockSplitSource,
    S::Error: std::fmt::Debug,
{
    let stream = source.fetch_splits(filter);
    futures::pin_mut!(stream);

    let mut out = Vec::new();
    while let Some(item) = stream.next().await {
        out.push(item.expect("fetch_splits yielded an error"));
    }
    out
}

fn year_2024(symbol: &str) -> CorporateActionFilter {
    CorporateActionFilter::new(
        vec![SmolStr::new(symbol)],
        NaiveDate::from_ymd_opt(2024, 1, 1),
        NaiveDate::from_ymd_opt(2024, 12, 31),
    )
}

#[tokio::test]
#[ignore = "requires ALPACA_API_KEY/ALPACA_SECRET_KEY + network"]
async fn nvda_forward_split_2024_via_source() {
    let client = AlpacaRestClient::from_env().expect("ALPACA credentials must be set");

    let actions = collect(&client, &year_2024("NVDA")).await;

    // NVIDIA's 10-for-1 forward split took effect (ex-date) on 2024-06-10.
    let nvda = actions
        .iter()
        .find(|a| a.effective_date == NaiveDate::from_ymd_opt(2024, 6, 10))
        .expect("NVDA 2024-06-10 split should be present in the result set");

    assert_eq!(nvda.instrument, SmolStr::new("NVDA"));
    assert_eq!(
        nvda.kind,
        CorporateActionKind::StockSplit {
            ratio: SplitRatio::new(Decimal::from(10)).unwrap()
        }
    );
}

#[tokio::test]
#[ignore = "requires ALPACA_API_KEY/ALPACA_SECRET_KEY + network"]
async fn atra_reverse_split_2024_via_source() {
    let client = AlpacaRestClient::from_env().expect("ALPACA credentials must be set");

    let actions = collect(&client, &year_2024("ATRA")).await;

    // Atara Biotherapeutics' 1-for-25 reverse split took effect (ex-date) on 2024-06-20 → ratio 0.04.
    let atra = actions
        .iter()
        .find(|a| a.effective_date == NaiveDate::from_ymd_opt(2024, 6, 20))
        .expect("ATRA 2024-06-20 reverse split should be present in the result set");

    assert_eq!(
        atra.kind,
        CorporateActionKind::StockSplit {
            ratio: SplitRatio::new(Decimal::new(4, 2)).unwrap()
        }
    );
}

#[tokio::test]
#[ignore = "requires ALPACA_API_KEY/ALPACA_SECRET_KEY + network"]
async fn raw_inherent_client_returns_nvda_split() {
    use rustrade_data::exchange::alpaca::CorporateActionsQuery;

    let client = AlpacaRestClient::from_env().expect("ALPACA credentials must be set");
    let query = CorporateActionsQuery::new()
        .symbols(["NVDA"])
        .start(NaiveDate::from_ymd_opt(2024, 1, 1).unwrap())
        .end(NaiveDate::from_ymd_opt(2024, 12, 31).unwrap());

    let stream = client.fetch_splits_raw(&query);
    futures::pin_mut!(stream);

    let mut splits = Vec::new();
    while let Some(item) = stream.next().await {
        splits.push(item.expect("inherent fetch_splits_raw yielded an error"));
    }

    let nvda = splits
        .iter()
        .find(|s| s.ex_date == NaiveDate::from_ymd_opt(2024, 6, 10).unwrap())
        .expect("NVDA 2024-06-10 split should be present");
    assert_eq!(nvda.new_rate, Decimal::from(10));
    assert_eq!(nvda.old_rate, Decimal::from(1));
}
