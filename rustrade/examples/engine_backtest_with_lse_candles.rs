#![allow(clippy::unwrap_used, clippy::expect_used)] // Example code: panics acceptable for demonstration

//! Backtest driven by **streamed** London Strategic Edge candles.
//!
//! # ⚠️ Licensing — the data is NOT redistributable
//!
//! This example's **code** is MIT-licensed like the rest of this repository. **The data it
//! retrieves is not.** London Strategic Edge permits use for your own research, trading and model
//! training — including commercially — but **prohibits redistributing, reselling, or otherwise
//! making the data available to third parties**, in bulk or through any competing feed, download
//! service or interface. Terms: <https://londonstrategicedge.com/terms>
//!
//! In practice: do not commit what this fetches to a public repository, do not publish it as
//! fixtures or an example dataset, and do not re-serve it.
//!
//! # Running
//!
//! Requires a free API key (no account, no card) from <https://londonstrategicedge.com/data>, in
//! `LSE_API_KEY`:
//!
//! ```bash
//! export LSE_API_KEY=...
//! cargo run --release --example engine_backtest_with_lse_candles --features lse
//! ```
//!
//! # What this demonstrates
//!
//! 1. **[`MarketDataStreamed`] over a provider factory.** The dataset is never resident: candles
//!    are fetched, merged and consumed on demand. Read-ahead is bounded here because each page is
//!    awaited — a *blocking* source needs
//!    [`stream_blocking_iter`](rustrade_data::streams::blocking::stream_blocking_iter) to get the
//!    same property. Compare `engine_backtest_with_candle_market_data.rs`, which holds everything in
//!    a `Vec`.
//! 2. **Multi-instrument replay through one stream.** [`BacktestMarketData`] exposes exactly one
//!    stream, so N per-symbol fetches are k-way merged into a single time-ordered feed with each
//!    event tagged with its own `InstrumentIndex`. That merge lives in the factory — the engine
//!    crate knows nothing about providers.
//! 3. **The default instrument state now consumes candles.** No custom `InstrumentDataState` is
//!    needed: [`DefaultInstrumentMarketData`] tracks the latest candle and exposes its close as the
//!    instrument price.
//!
//! # ⚠️ Cost — this is the wrong shape for a parameter sweep
//!
//! [`MarketDataStreamed`] calls its factory once per backtest, so `run_backtests` with N strategy
//! configurations would re-fetch the entire range N times, against a provider whose streaming and
//! export allowances come out of one shared pool. This example therefore runs a **single**
//! `backtest`. For repeated runs over one range, fetch once to local storage and stream from that —
//! or, for a slice small enough to hold, use `collect_candles` with `MarketDataInMemory`.
//!
//! # Suitability
//!
//! These are the provider's own series, not an execution venue's book, and the engine is filling
//! against `MockExchange`. That is sound for research; before risking capital, note that deciding
//! on one venue's prices while filling on another's is a basis mismatch you are opting into.
//!
//! # ⚠️ This backtest fills at the BID on FX, which flatters every buy
//!
//! The provider's FX candles are **bid** candles: reconciled against the tick tape for the same
//! day, open/high/low/close matched the bid series on 1421 of 1421 minutes and matched the mid or
//! the ask on none. Filling at the candle close — which is what this example does — therefore buys
//! at the bid, systematically favourable by a full spread (roughly 0.8 pip on `EUR/USD`) on every
//! buy. Nothing in the library can correct that without inventing a spread, so a strategy whose
//! edge is near spread-sized will look profitable here and lose live. Model the spread yourself
//! before believing a result. Equity candles track the trade tape instead, so the bias is
//! FX-specific.
//!
//! Two related properties of the same data: candle `volume` is unreliable where it is published at
//! all (a majority of sampled one-minute equity bars report zero in minutes that demonstrably had
//! trades), and non-trading days arrive as **flat** `o == h == l == c` bars rather than being
//! omitted, so a daily backtest sees a tradeable price on a closed market. See the
//! `rustrade_data::exchange::lse` module documentation.

use chrono::{DateTime, Utc};
use futures::StreamExt;
use rust_decimal::Decimal;
use rustrade::{
    backtest::{
        BacktestArgsConstant, BacktestArgsDynamic,
        aux_events::NoAuxEvents,
        backtest,
        market_data::{BacktestMarketData, MarketDataStreamed},
    },
    engine::state::{
        EngineState, builder::EngineStateBuilder, global::DefaultGlobalData,
        instrument::data::DefaultInstrumentMarketData, trading::TradingState,
    },
    error::BarterError,
    risk::DefaultRiskManager,
    statistic::time::Daily,
    strategy::DefaultStrategy,
    system::config::SystemConfig,
};
use rustrade_data::{
    event::DataKind,
    exchange::lse::{
        backtest::{LseCandleSource, replay_candles},
        vault::LseVaultClient,
    },
    subscription::candle::CandleInterval,
};
use rustrade_instrument::{exchange::ExchangeId, index::IndexedInstruments};
use serde::Deserialize;
use std::{fs::File, io::BufReader, sync::Arc};

const CONFIG_PATH: &str = "rustrade/examples/config/lse_backtest_config.json";
const INTERVAL: CandleInterval = CandleInterval::Hour1;

#[derive(Deserialize)]
pub struct Config {
    pub risk_free_return: Decimal,
    pub system: SystemConfig,
}

#[tokio::main]
async fn main() {
    rustrade::logging::init_logging();

    let Config {
        risk_free_return,
        system: SystemConfig {
            instruments,
            executions,
        },
    } = load_config();

    // The config declares the instruments; `IndexedInstruments` assigns indices in order. The
    // provider is a data source, not a registry — as with every connector, you declare what you
    // want rather than having 4,000 instruments auto-registered.
    let instruments = IndexedInstruments::new(instruments);

    let client = Arc::new(
        LseVaultClient::from_env()
            .expect("set LSE_API_KEY - get a free key at https://londonstrategicedge.com/data"),
    );

    // Each source pairs a vault display symbol with the index and exchange its instrument was
    // registered under. `resolve` derives the index from the registry above rather than taking a
    // literal, which is what you want: a wrong index would silently attribute one symbol's prices
    // to a different instrument, and an unregistered exchange panics the engine. It also checks the
    // quote asset, so a `.L` listing registered in `gbp` rather than `gbx` -- pence booked as
    // pounds, every notional 100x wrong -- is rejected here instead of running to completion.
    let sources = ["AAPL", "MSFT"]
        .into_iter()
        .map(|symbol| {
            LseCandleSource::resolve(&instruments, ExchangeId::LseEquities, symbol)
                .expect("each symbol to be registered in the config above, priced in its own unit")
        })
        .collect::<Vec<_>>();

    let start: DateTime<Utc> = "2024-01-02T00:00:00Z".parse().unwrap();
    let end: DateTime<Utc> = "2024-03-01T00:00:00Z".parse().unwrap();

    // The factory: called once at construction to resolve the first event's time, then once per
    // backtest. Everything provider-specific — fetching, pagination, the k-way merge, the
    // `InstrumentIndex` tagging — lives in here, behind a plain `Stream`.
    let market_data = MarketDataStreamed::<_, DataKind>::new(move || {
        let client = Arc::clone(&client);
        let sources = sources.clone();
        async move {
            Ok(replay_candles(client, sources, INTERVAL, start, end).map(
                // The engine crate's error type; `DataError` converts into it directly.
                |event| event.map_err(BarterError::from),
            ))
        }
    })
    .await
    .expect("failed to open the LSE candle stream");

    let time_engine_start = market_data.time_first_event().await.unwrap();

    // No custom state needed: `DefaultInstrumentMarketData` consumes `DataKind::Candle`, using the
    // latest bar's close as the instrument price when no L1 book is present.
    let engine_state = EngineStateBuilder::new(&instruments, DefaultGlobalData, |_| {
        DefaultInstrumentMarketData::default()
    })
    .time_engine_start(time_engine_start)
    .trading_state(TradingState::Enabled)
    .build();

    let args_constant = Arc::new(BacktestArgsConstant {
        instruments,
        executions,
        market_data,
        summary_interval: Daily,
        engine_state,
        aux_events: NoAuxEvents,
    });

    // `DefaultStrategy`/`DefaultRiskManager` are no-ops — this example demonstrates the streaming
    // market-data wiring, not a strategy.
    let args_dynamic = BacktestArgsDynamic {
        id: "lse-streamed-candles".into(),
        risk_free_return,
        strategy: DefaultStrategy::<
            EngineState<DefaultGlobalData, DefaultInstrumentMarketData>,
        >::default(),
        risk: DefaultRiskManager::<
            EngineState<DefaultGlobalData, DefaultInstrumentMarketData>,
        >::default(),
    };

    // A source failure aborts the run and surfaces here — a summary is never produced over a
    // partially-read dataset.
    let summary = backtest(args_constant, args_dynamic)
        .await
        .expect("backtest failed")
        .summary;

    println!("\nBacktest complete (BacktestId = {})", summary.id);
    summary.trading_summary.print_summary();
}

pub fn load_config() -> Config {
    let file = File::open(CONFIG_PATH).expect("Failed to open config file");
    let reader = BufReader::new(file);
    serde_json::from_reader(reader).expect("Failed to parse config file")
}
