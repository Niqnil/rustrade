//! Live, read-only check of `BinanceMargin`'s account stream.
//!
//! Binance margin has no testnet, so this runs against production. It places no orders and moves
//! nothing: it gets a margin listen key, opens the account stream, and listens long enough for
//! the heartbeat to need a pong from a quiet connection (two checks, 30 s apart, the first of
//! which is a grace). It then checks the stream is still open and never reported its end. It
//! prints only the kinds of the events that arrive, never their contents.
//!
//! Run it on an account that nothing else trades, so that a quiet stream is expected. To see
//! frames arrive, move a small amount in or out of the margin account while it listens.
//!
//! - `BINANCE_SUBACCOUNT_READONLY_API_KEY`, `BINANCE_SUBACCOUNT_READONLY_API_SECRET`: a production
//!   key that may read margin data. It needs no trading permission. The account must have cross
//!   margin activated, and the isolated test also its pair.
//! - `BINANCE_MARGIN_STREAM_SYMBOL`: the isolated test's pair, `BTCUSDT` if unset.
//! - `BINANCE_MARGIN_STREAM_SECS`: how long each test listens, 75 if unset.
//!
//! ```bash
//! cargo test -p rustrade-execution --features binance --test binance_margin_stream -- --ignored --nocapture --test-threads=1
//! ```
#![allow(clippy::unwrap_used, clippy::expect_used)] // Live check: a panic is the report.

use futures::StreamExt;
use rustrade_execution::{
    AccountEventKind,
    client::{
        ExecutionClient,
        binance::{BinanceMargin, BinanceMarginConfig},
    },
};
use rustrade_instrument::instrument::name::InstrumentNameExchange;
use std::time::Duration;

fn env(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("{name} env var required"))
}

fn keys() -> (String, String) {
    (
        env("BINANCE_SUBACCOUNT_READONLY_API_KEY"),
        env("BINANCE_SUBACCOUNT_READONLY_API_SECRET"),
    )
}

fn listen_for() -> Duration {
    let secs = std::env::var("BINANCE_MARGIN_STREAM_SECS")
        .map(|secs| {
            secs.parse()
                .expect("BINANCE_MARGIN_STREAM_SECS is whole seconds")
        })
        .unwrap_or(75);
    assert!(
        secs >= 65,
        "listen past the second heartbeat check, at 60 s, so a pong must keep the stream open"
    );
    Duration::from_secs(secs)
}

/// Opens the stream, listens, and fails if it ended or reported its end.
async fn stream_stays_open(config: BinanceMarginConfig, instruments: &[InstrumentNameExchange]) {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("rustrade_execution=info")
        .try_init();
    let client = BinanceMargin::new(config);
    let mut stream = client
        .account_stream(&[], instruments)
        .await
        .expect("account_stream opens");

    let deadline = tokio::time::Instant::now() + listen_for();
    let mut kinds = Vec::new();
    loop {
        match tokio::time::timeout_at(deadline, stream.next()).await {
            Err(_elapsed) => break,
            Ok(None) => panic!("the stream ended after {kinds:?}"),
            Ok(Some(event)) => {
                let kind = match &event.kind {
                    AccountEventKind::StreamTerminated(reason) => {
                        panic!("the stream reported its end after {kinds:?}: {reason:?}")
                    }
                    AccountEventKind::BalanceStreamUpdate(_) => "BalanceStreamUpdate",
                    AccountEventKind::InstrumentBalanceUpdate(_) => "InstrumentBalanceUpdate",
                    AccountEventKind::OrderSnapshot(_) => "OrderSnapshot",
                    AccountEventKind::OrderCancelled(_) => "OrderCancelled",
                    AccountEventKind::Trade(_) => "Trade",
                    AccountEventKind::MarginDebt(_) => "MarginDebt",
                    AccountEventKind::Notice(_) => "Notice",
                    _ => "Other",
                };
                println!("stream event: {kind}");
                kinds.push(kind);
            }
        }
    }
    println!("SUMMARY open after {:?}, events: {kinds:?}", listen_for());
}

#[tokio::test]
#[ignore]
async fn cross_margin_stream_stays_open_through_the_heartbeat() {
    let (api_key, secret_key) = keys();
    stream_stays_open(
        BinanceMarginConfig::cross_margin(api_key, secret_key),
        &[InstrumentNameExchange::new("BTCUSDT")],
    )
    .await;
}

#[tokio::test]
#[ignore]
async fn isolated_margin_stream_stays_open_through_the_heartbeat() {
    let (api_key, secret_key) = keys();
    let pair = InstrumentNameExchange::new(
        std::env::var("BINANCE_MARGIN_STREAM_SYMBOL").unwrap_or_else(|_| "BTCUSDT".to_owned()),
    );
    stream_stays_open(
        BinanceMarginConfig::isolated(api_key, secret_key, vec![pair]),
        &[],
    )
    .await;
}
