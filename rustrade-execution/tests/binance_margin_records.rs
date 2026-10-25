//! Live, read-only check of `BinanceMargin`'s interest and forced-liquidation reads, and of what
//! Binance's two history endpoints return, where its documentation is silent.
//!
//! Binance margin has no testnet, so this runs against production. It places no orders and moves
//! nothing. It prints only counts and field names, never an amount, so its output can be shared:
//!
//! - which `asset` and `rawAsset` an interest record of each `type` names, to learn which of the
//!   two holds BNB on a record converted into BNB;
//! - whether a cross forced-liquidation query returns records marked isolated;
//! - whether the forced-liquidation endpoint refuses a span of two years.
//!
//! The reads cost about 20 of Binance's per-IP request weight, of the 12000 a minute allows, plus
//! one per page of records.
//!
//! The variables are named apart from `BINANCE_API_KEY`, which the other Binance tests read as a
//! testnet key:
//!
//! - `BINANCE_MARGIN_API_KEY`, `BINANCE_MARGIN_SECRET_KEY`: a production key that may read margin
//!   data. It needs no trading permission.
//!
//! ```bash
//! cargo test -p rustrade-execution --features binance --test binance_margin_records -- --ignored --nocapture
//! ```
#![allow(clippy::unwrap_used, clippy::expect_used)] // Live check: a panic is the report.

use binance_sdk::{
    common::config::ConfigurationRestApi,
    margin_trading::{
        MarginTradingRestApi,
        rest_api::{GetForceLiquidationRecordParams, GetInterestHistoryParams},
    },
};
use chrono::{TimeDelta, Utc};
use rustrade_execution::{
    cash_flow::CashFlowKind,
    client::{
        ExecutionClient,
        binance::{BinanceMargin, BinanceMarginConfig},
    },
};
use std::collections::BTreeMap;

fn env(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("{name} env var required"))
}

fn credentials() -> (String, String) {
    (
        env("BINANCE_MARGIN_API_KEY"),
        env("BINANCE_MARGIN_SECRET_KEY"),
    )
}

#[tokio::test]
#[ignore]
async fn margin_interest_and_forced_liquidations_read_back() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("rustrade_execution=info")
        .try_init();
    let (api_key, secret_key) = credentials();
    let client = BinanceMargin::new(BinanceMarginConfig::cross_margin(api_key, secret_key));
    let now = Utc::now();

    let read = client
        .fetch_margin_interest(now - TimeDelta::days(90), now)
        .await
        .expect("fetch_margin_interest");
    assert!(
        read.held_since.is_some(),
        "90 days back is a minute past what the read covers"
    );
    let interest = read.flows;
    let mut by_asset = BTreeMap::<String, usize>::new();
    for flow in &interest {
        *by_asset.entry(flow.asset.to_string()).or_default() += 1;
        assert!(flow.amount.is_sign_negative(), "a charge is paid");
        assert!(flow.id.is_some(), "Binance gives each charge an id");
        assert!(matches!(flow.kind, CashFlowKind::MarginInterest { .. }));
    }
    println!(
        "fetch_margin_interest, 90 days: {} charges, by asset {by_asset:?}",
        interest.len()
    );
    assert!(
        interest
            .windows(2)
            .all(|pair| pair[0].time_exchange <= pair[1].time_exchange),
        "oldest first"
    );

    let liquidations = client
        .fetch_forced_liquidations(now - TimeDelta::days(90), now)
        .await
        .expect("fetch_forced_liquidations");
    println!(
        "fetch_forced_liquidations, 90 days: {} records",
        liquidations.len()
    );
}

/// What Binance's raw records say, where its documentation does not.
#[tokio::test]
#[ignore]
async fn margin_history_endpoints_answer_what_the_docs_do_not() {
    let (api_key, secret_key) = credentials();
    let rest = MarginTradingRestApi::production(
        ConfigurationRestApi::builder()
            .api_key(api_key)
            .api_secret(secret_key)
            .build()
            .unwrap(),
    );
    let now = Utc::now();

    // Interest: which `asset` and `rawAsset` each `type` of record names, over the 90 days served,
    // in windows of 29 days. Field names and counts only.
    let mut kinds = BTreeMap::<(String, String, String), usize>::new();
    let mut end = now;
    for _ in 0..3 {
        let start =
            (end - TimeDelta::days(29)).max(now - TimeDelta::days(90) + TimeDelta::hours(1));
        for current in 1..=50 {
            let params = GetInterestHistoryParams::builder()
                .start_time(start.timestamp_millis())
                .end_time(end.timestamp_millis())
                .current(current)
                .size(100)
                .build()
                .unwrap();
            let page = rest
                .get_interest_history(params)
                .await
                .expect("interestHistory")
                .data()
                .await
                .unwrap();
            let rows = page.rows.unwrap_or_default();
            for row in &rows {
                let key = (
                    row.r#type.clone().unwrap_or_else(|| "-".into()),
                    row.asset.clone().unwrap_or_else(|| "-".into()),
                    row.raw_asset.clone().unwrap_or_else(|| "-".into()),
                );
                *kinds.entry(key).or_default() += 1;
            }
            if rows.len() < 100 {
                break;
            }
        }
        end = start;
    }
    println!("interestHistory records by (type, asset, rawAsset): {kinds:?}");

    // Forced liquidations, cross: does a query naming no pair return records marked isolated?
    let params = GetForceLiquidationRecordParams::builder()
        .start_time((now - TimeDelta::days(90)).timestamp_millis())
        .end_time(now.timestamp_millis())
        .size(100)
        .build()
        .unwrap();
    let page = rest
        .get_force_liquidation_record(params)
        .await
        .expect("forceLiquidationRec")
        .data()
        .await
        .unwrap();
    let mut marked = BTreeMap::<String, usize>::new();
    for row in page.rows.unwrap_or_default() {
        *marked.entry(format!("{:?}", row.is_isolated)).or_default() += 1;
    }
    println!(
        "forceLiquidationRec, cross, 90 days: total {:?}, by isIsolated {marked:?}",
        page.total
    );

    // Forced liquidations: is a span of two years refused?
    let params = GetForceLiquidationRecordParams::builder()
        .start_time((now - TimeDelta::days(730)).timestamp_millis())
        .end_time(now.timestamp_millis())
        .size(100)
        .build()
        .unwrap();
    match rest.get_force_liquidation_record(params).await {
        Ok(response) => println!(
            "forceLiquidationRec, 730 days: accepted, total {:?}",
            response.data().await.unwrap().total
        ),
        Err(error) => println!("forceLiquidationRec, 730 days: refused: {error}"),
    }
}
