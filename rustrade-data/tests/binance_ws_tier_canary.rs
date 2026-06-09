//! Binance WebSocket tier-routing canary (network-gated, on-demand diagnostic).
//!
//! # Why this exists
//!
//! Binance has *demonstrated* that it changes which WS stream is served on which routed path
//! tier (the `fstream.binance.com` `/public` vs `/market` split — see the `BinanceFuturesUsd`
//! vs `BinanceFuturesUsdMarket` server types). A wrong tier does **not** error: the handshake
//! returns HTTP `101` and the server then pushes **zero frames**, silently. rustrade's futures
//! `PublicTrades` additionally rides the *undocumented* `@trade` stream. A tier change would
//! therefore produce a silent dead stream in production with no compile-time or runtime error.
//!
//! This canary re-verifies the live tier map and fails loudly if a stream Binance should be
//! serving has gone dark. It exercises rustrade's **typed `Streams` API** (not raw sockets) so it
//! also catches a wrong `websocket_url()` / channel-string regression on *our* side.
//!
//! # When to run it
//!
//! This is an **on-demand diagnostic**, not a scheduled job — there is no weekly CI workflow.
//! Continuous protection in production is the **consumer's** job: a staleness watchdog on the live
//! stream catches a tier change in seconds, on a non-geoblocked IP, on the data that actually
//! matters (see the caller-obligation rustdoc on [`BinanceFuturesUsd`]). GitHub-hosted runners are
//! Azure/US and geoblocked by Binance (`451`), so an automated canary there only ever skips-neutral
//! and adds no signal. Run this by hand (from a Binance-reachable host) to confirm the tier map
//! when a prod staleness alert fires, or when validating a Binance routing change.
//!
//! [`BinanceFuturesUsd`]: rustrade_data::exchange::binance::futures::BinanceFuturesUsd
//!
//! # Tier map under test (see decision 8 in the design notes)
//!
//! | Stream                              | Server type               | Path         |
//! |-------------------------------------|---------------------------|--------------|
//! | futures `@trade` (`PublicTrades`)   | `BinanceFuturesUsd`       | `/public/ws` |
//! | futures `continuousKline_<i>`       | `BinanceFuturesUsdMarket` | `/market/ws` |
//! | spot `@kline_<i>`                   | `BinanceSpot`             | `/ws`        |
//!
//! On failure of the futures-trade canary, the known fix is migrating it to `@aggTrade` on
//! `BinanceFuturesUsdMarket` (`/market`).
//!
//! # Skip vs. fail contract
//!
//! Binance geoblocks some cloud / US IPs, so the WS handshake itself may fail (`451`/`403`/
//! connection refused) for reasons that have nothing to do with tier routing. Because a *tier*
//! change always presents as **`101` + zero frames** (a *successful* connect), a handshake
//! failure is *never* a tier change — it is always a connectivity / geoblock artifact. So:
//!
//! - `init()` returns `Err` (e.g. `DataError::Socket`)  → **SKIP** (logged, test passes).
//! - `init()` succeeds but no frame within the timeout  → **FAIL** (the real tier-change signal).
//!
//! Because this is run by hand from a Binance-reachable host, an all-SKIP run means *you* are
//! geoblocked (check the logged `CANARY_SKIP` lines) — rerun from a non-geoblocked network rather
//! than reading SKIP as a pass.
//!
//! Known limitation: a malformed-but-routable `websocket_url()` that returned a non-`101` status
//! would also be classified SKIP here rather than FAIL. That failure mode is covered by the
//! per-surface unit/frame tests in the live-WS phase; this canary targets *tier routing*, where
//! the failure mode is provably `101` + silence.
//!
//! # Running
//!
//! ```bash
//! cargo test --test binance_ws_tier_canary -- --ignored          # all three
//! cargo test --test binance_ws_tier_canary spot_candles -- --ignored
//! ```
//!
//! All endpoints are public / unauthenticated — no secrets required.

// Integration test: panics on missing data are the intended failure signal.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use futures_util::{Stream, StreamExt};
use rustrade_data::{
    error::DataError,
    exchange::binance::{
        futures::{BinanceFuturesUsd, BinanceFuturesUsdMarket},
        spot::BinanceSpot,
    },
    streams::{
        Streams,
        reconnect::{Event, stream::ReconnectingStream},
    },
    subscriber::WebSocketSubscriber,
    subscription::{
        candle::{CandleInterval, Candles},
        trade::PublicTrades,
    },
};
use rustrade_instrument::{
    exchange::ExchangeId, instrument::market_data::kind::MarketDataInstrumentKind,
};
use std::time::Duration;
use tokio::time::Instant;
use tracing_subscriber::{EnvFilter, fmt};

/// Per-stream window to observe a live frame. Generous: the dense streams under test (1s candles,
/// `btcusdt` trades) deliver within a second or two when healthy, so a 30s window is pure margin.
const FRAME_TIMEOUT_SECS: u64 = 30;

fn init_logging() {
    let _ = fmt()
        .with_env_filter(
            EnvFilter::builder()
                .with_default_directive(tracing::Level::INFO.into())
                .from_env_lossy(),
        )
        .try_init();
}

/// Classify an `init()` failure as a connectivity / geoblock SKIP (not a tier-change FAIL).
///
/// A tier change presents as a *successful* connect with zero frames, never as a handshake
/// failure, so any `init()` error here is treated as a non-signal and the test passes.
fn skip_on_connect_failure(label: &str, error: &DataError) {
    tracing::warn!(
        %label,
        ?error,
        "CANARY_SKIP: WS handshake/connect failed — treating as connectivity/geoblock artifact, \
         NOT a tier change (a tier change presents as 101 + zero frames). Test passes."
    );
}

/// Drive a `select_all()` stream until the first [`Event::Item`] (a delivered frame) or the
/// timeout. A timeout is the canary's FAIL signal: connected, but the expected stream is silent.
async fn assert_frame_within<S, T>(mut stream: S, label: &str)
where
    S: Stream<Item = Event<ExchangeId, T>> + Unpin,
    T: std::fmt::Debug, // required by the `?item` log below
{
    let deadline = Instant::now() + Duration::from_secs(FRAME_TIMEOUT_SECS);

    // A single deadline governs the whole wait; `timeout_at` fires once, so there is no manual
    // per-iteration clock check and no chance of spinning.
    let delivered = tokio::time::timeout_at(deadline, async {
        loop {
            match stream.next().await {
                // A delivered market event: the tier still serves this stream. Canary passes.
                Some(Event::Item(item)) => {
                    tracing::info!(%label, ?item, "CANARY_OK: live frame delivered");
                    return true;
                }
                // Reconnection notice — log (so a mid-window reconnect is visible in CI, and a
                // timeout caused by reconnect backoff is distinguishable from a tier change) and
                // keep waiting within the deadline.
                Some(Event::Reconnecting(origin)) => {
                    tracing::warn!(%label, ?origin, "CANARY: stream reconnecting mid-test");
                }
                // Stream ended before any frame: treat as silence → FAIL.
                None => return false,
            }
        }
    })
    .await;

    match delivered {
        // Frame delivered within the deadline — canary passes.
        Ok(true) => {}
        // The infinite reconnect loop should never exhaust; if it did, the consumer task likely
        // panicked or its channel was dropped.
        Ok(false) => panic!(
            "{label}: stream terminated before delivering any frame (unexpected — the reconnect \
             loop should never exhaust; check for a panicked consumer task / dropped channel)"
        ),
        // Connected (handshake OK) but no frame in time: the canary's real FAIL signal.
        Err(_) => panic!(
            "{label}: connected (handshake OK) but received NO frames within \
             {FRAME_TIMEOUT_SECS}s — Binance WS tier routing has likely changed",
        ),
    }
}

#[tokio::test]
#[ignore = "network-gated canary; run via --ignored"]
async fn futures_public_trades_delivers() {
    init_logging();
    let label = "futures PublicTrades @trade on BinanceFuturesUsd (/public/ws)";

    let streams = Streams::<PublicTrades>::builder()
        .subscribe(
            WebSocketSubscriber,
            [(
                BinanceFuturesUsd::default(),
                "btc",
                "usdt",
                MarketDataInstrumentKind::Perpetual,
                PublicTrades,
            )],
        )
        .init()
        .await;

    match streams {
        Err(error) => skip_on_connect_failure(label, &error),
        Ok(streams) => {
            let stream = streams
                .select_all()
                .with_error_handler(|error| tracing::warn!(?error, "MarketStream error"));
            assert_frame_within(stream, label).await;
        }
    }
}

#[tokio::test]
#[ignore = "network-gated canary; run via --ignored"]
async fn futures_candles_delivers() {
    init_logging();
    let label = "futures Candles continuousKline_1s on BinanceFuturesUsdMarket (/market/ws)";

    let streams = Streams::<Candles>::builder()
        .subscribe(
            WebSocketSubscriber,
            [(
                BinanceFuturesUsdMarket::default(),
                "btc",
                "usdt",
                MarketDataInstrumentKind::Perpetual,
                Candles {
                    interval: CandleInterval::Sec1,
                },
            )],
        )
        .init()
        .await;

    match streams {
        Err(error) => skip_on_connect_failure(label, &error),
        Ok(streams) => {
            let stream = streams
                .select_all()
                .with_error_handler(|error| tracing::warn!(?error, "MarketStream error"));
            assert_frame_within(stream, label).await;
        }
    }
}

#[tokio::test]
#[ignore = "network-gated canary; run via --ignored"]
async fn spot_candles_delivers() {
    init_logging();
    let label = "spot Candles @kline_1s on BinanceSpot (/ws)";

    let streams = Streams::<Candles>::builder()
        .subscribe(
            WebSocketSubscriber,
            [(
                BinanceSpot::default(),
                "btc",
                "usdt",
                MarketDataInstrumentKind::Spot,
                Candles {
                    interval: CandleInterval::Sec1,
                },
            )],
        )
        .init()
        .await;

    match streams {
        Err(error) => skip_on_connect_failure(label, &error),
        Ok(streams) => {
            let stream = streams
                .select_all()
                .with_error_handler(|error| tracing::warn!(?error, "MarketStream error"));
            assert_frame_within(stream, label).await;
        }
    }
}
