#![allow(clippy::unwrap_used, clippy::expect_used)] // Example code: panics acceptable for demonstration

//! Live Binance klines (candles) over WebSocket, on the typed `Streams` API.
//!
//! Demonstrates:
//! - **Spot** klines via `BinanceSpot` (`@kline_<interval>` on `stream.binance.com:9443/ws`).
//! - **Futures** klines via `BinanceFuturesUsdMarket` (`@continuousKline_<interval>` on the
//!   `/market` tier `fstream.binance.com/market/ws`) — note the dedicated market-tier exchange
//!   type; routing futures klines through `BinanceFuturesUsd` would be a compile error.
//!
//! Only **closed** candles are emitted (no repaint/lookahead); an in-progress kline yields no
//! event. `close_time` is the exact period-end boundary (`open + interval`), computed
//! library-side. All endpoints are public/unauthenticated — no secrets required.

use futures_util::StreamExt;
use rustrade_data::{
    exchange::binance::{futures::BinanceFuturesUsdMarket, spot::BinanceSpot},
    streams::{Streams, reconnect::stream::ReconnectingStream},
    subscriber::WebSocketSubscriber,
    subscription::candle::{CandleInterval, Candles},
};
use rustrade_instrument::instrument::market_data::kind::MarketDataInstrumentKind;
use tracing::{info, warn};

#[rustfmt::skip]
#[tokio::main]
async fn main() {
    init_logging();

    let streams = Streams::<Candles>::builder()
        // Spot 1s candles on BinanceSpot's `/ws` tier.
        .subscribe(WebSocketSubscriber, [
            (BinanceSpot::default(), "btc", "usdt", MarketDataInstrumentKind::Spot, Candles { interval: CandleInterval::Sec1 }),
            (BinanceSpot::default(), "eth", "usdt", MarketDataInstrumentKind::Spot, Candles { interval: CandleInterval::Min1 }),
        ])
        // Futures continuous (perpetual) 1s candles on the `/market` tier.
        .subscribe(WebSocketSubscriber, [
            (BinanceFuturesUsdMarket::default(), "btc", "usdt", MarketDataInstrumentKind::Perpetual, Candles { interval: CandleInterval::Sec1 }),
        ])
        .init()
        .await
        .unwrap();

    let mut joined_stream = streams
        .select_all()
        .with_error_handler(|error| warn!(?error, "MarketStream generated error"));

    while let Some(event) = joined_stream.next().await {
        info!("{event:?}");
    }
}

// Initialise an INFO `Subscriber` for `Tracing` Json logs and install it as the global default.
fn init_logging() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::filter::EnvFilter::builder()
                .with_default_directive(tracing_subscriber::filter::LevelFilter::INFO.into())
                .from_env_lossy(),
        )
        .with_ansi(cfg!(debug_assertions))
        .json()
        .init()
}
