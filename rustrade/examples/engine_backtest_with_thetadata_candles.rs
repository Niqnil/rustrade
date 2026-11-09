#![allow(clippy::unwrap_used, clippy::expect_used)] // Example code: panics acceptable for demonstration

//! Backtest on ThetaData candles: daily candles from end-of-day reports (free plan), or one-minute
//! bars (value plan).
//!
//! # ⚠️ Licensing: the data is NOT redistributable
//!
//! This example's **code** is MIT-licensed like the rest of this repository. **The data it
//! retrieves is not.** ThetaData's individual plans license the data for personal, non-commercial
//! use only, and its terms forbid archiving, reproducing, distributing or publishing it, or making
//! it available to anyone else. Terms: <https://www.thetadata.net/terms-and-conditions>
//!
//! In practice: do not commit what this fetches to a public repository, do not publish it as
//! fixtures or an example dataset, and do not re-serve it.
//!
//! # Running
//!
//! Requires a running [Theta Terminal](https://docs.thetadata.us/Articles/Getting-Started/Getting-Started.html)
//! logged in to a ThetaData account. A free account serves the end-of-day reports this example
//! uses by default. Keep the Terminal's port off the network: its API has no authentication (see
//! the `thetadata` module documentation). Set `THETADATA_BASE_URL` if the Terminal is not at
//! `http://127.0.0.1:25503/v3`.
//!
//! ```bash
//! # Daily candles from end-of-day reports (free plan)
//! cargo run --release --example engine_backtest_with_thetadata_candles --features thetadata
//!
//! # One-minute bars of the regular session (value plan)
//! cargo run --release --example engine_backtest_with_thetadata_candles --features thetadata -- --minute-bars
//! ```
//!
//! # What this demonstrates
//!
//! 1. **End-of-day reports as daily candles.** Each [`EndOfDay`] carries the day's [`Candle`],
//!    closing at 00:00 UTC after the trading day, after ThetaData generated the report. The engine
//!    enters each candle at its `close_time`, so no strategy sees a day's figures before they
//!    existed.
//! 2. **One-minute bars** with `--minute-bars`: [`Bar::candle`] closes at the end of its minute.
//! 3. **Instruments registered under [`ExchangeId::ThetaDataStocks`]**, with a mock exchange of the
//!    same id doing the fills. A backtest that also trades options would register them under
//!    [`ExchangeId::ThetaDataOptions`], so the two can have their own fee model and expiry
//!    handling.
//!
//! # Why the candles are fetched first, not streamed
//!
//! A free account allows one request in flight. A stream from [`ThetaDataClient`] holds that
//! request's slot until it ends, while a time-ordered merge of several symbols' streams waits for
//! a row from every one of them before it emits anything. At a limit of one, the merge would wait
//! forever on the second symbol. So this example drains each symbol in turn, then sorts the
//! candles by `close_time` into one [`MarketDataInMemory`]. Daily candles are small. A long range
//! of minute bars over many symbols is not: on a plan with more concurrent requests, set
//! [`ThetaDataClient::with_max_concurrent_requests`] to at least the number of symbols and merge
//! the streams instead, with `rustrade_data::streams::merge::merge_time_sorted`.
//!
//! # Zero-price candles are skipped
//!
//! ThetaData reports an interval with no trades, or a day without one, as a candle of zero prices.
//! [`DefaultInstrumentMarketData`] takes the latest candle's close as the instrument's price, so a
//! zero candle would price the instrument at zero. This example drops such candles and logs how
//! many it dropped.
//!
//! # Suitability
//!
//! These are consolidated prices, and the engine fills against a mock exchange at the candle's
//! close. That is sound for research, but a daily close is not a price anyone could trade at once
//! the candle arrives: the next trade comes at the following session.
//!
//! The config charges no fees (`"Zero"`), as commission-free US stock brokers do. Before adding a
//! strategy, set the fee model to your broker's schedule.

use chrono::NaiveDate;
use futures::{Stream, StreamExt};
use rust_decimal::Decimal;
use rustrade::{
    backtest::{
        BacktestArgsConstant, BacktestArgsDynamic,
        aux_events::NoAuxEvents,
        backtest,
        market_data::{BacktestMarketData, MarketDataInMemory},
    },
    engine::state::{
        EngineState, builder::EngineStateBuilder, global::DefaultGlobalData,
        instrument::data::DefaultInstrumentMarketData, trading::TradingState,
    },
    risk::DefaultRiskManager,
    statistic::time::Daily,
    strategy::DefaultStrategy,
    system::config::SystemConfig,
};
use rustrade_data::{
    event::{DataKind, MarketEvent},
    exchange::thetadata::{
        Bar, DEFAULT_BASE_URL, EndOfDay, Interval, ThetaDataClient, ThetaDataError,
    },
    streams::consumer::MarketStreamEvent,
    subscription::candle::Candle,
};
use rustrade_instrument::{
    exchange::ExchangeId,
    index::IndexedInstruments,
    instrument::{InstrumentIndex, name::InstrumentNameExchange},
};
use serde::Deserialize;
use std::{fs::File, io::BufReader, sync::Arc};

const CONFIG_PATH: &str = "rustrade/examples/config/thetadata_backtest_config.json";
const EXCHANGE: ExchangeId = ExchangeId::ThetaDataStocks;
const SYMBOLS: [&str; 2] = ["SPY", "QQQ"];

#[derive(Deserialize)]
pub struct Config {
    pub risk_free_return: Decimal,
    pub system: SystemConfig,
}

#[tokio::main]
async fn main() {
    rustrade::logging::init_logging();
    let minute_bars = std::env::args().any(|arg| arg == "--minute-bars");

    let Config {
        risk_free_return,
        system: SystemConfig {
            instruments,
            executions,
        },
    } = load_config();
    let instruments = IndexedInstruments::new(instruments);

    let base_url =
        std::env::var("THETADATA_BASE_URL").unwrap_or_else(|_| DEFAULT_BASE_URL.to_owned());
    let client = ThetaDataClient::new()
        .unwrap()
        .with_base_url(base_url)
        .unwrap();

    // A range inside the free plan's history; end-of-day history goes back further on paid plans.
    let start = NaiveDate::from_ymd_opt(2025, 1, 2).unwrap();
    let end = NaiveDate::from_ymd_opt(2025, 6, 30).unwrap();

    let mut events = Vec::new();
    for symbol in SYMBOLS {
        let instrument = instrument_index(&instruments, symbol);
        let candles: Vec<Candle> = if minute_bars {
            collect_candles(
                client.stock_ohlc_history(symbol, start, end, Interval::M1, None),
                |bar: Bar| bar.candle,
            )
            .await
        } else {
            collect_candles(
                client.stock_end_of_day_history(symbol, start, end),
                |eod: EndOfDay| eod.candle,
            )
            .await
        }
        .unwrap_or_else(|error| panic!("fetching {symbol}: {error}"));

        let fetched = candles.len();
        let priced: Vec<Candle> = candles
            .into_iter()
            .filter(|candle| !candle.close.is_zero())
            .collect();
        println!(
            "{symbol}: {} candles, {} with no trades skipped",
            priced.len(),
            fetched - priced.len()
        );
        events.extend(priced.into_iter().map(|candle| (instrument, candle)));
    }

    // One timeline: every candle at its `close_time`. The sort is stable, so candles closing at the
    // same instant keep `SYMBOLS` order and the replay is the same every run.
    events.sort_by_key(|(_, candle)| candle.close_time);
    let events: Vec<MarketStreamEvent<InstrumentIndex, DataKind>> = events
        .into_iter()
        .map(|(instrument, candle)| {
            MarketStreamEvent::Item(MarketEvent {
                time_exchange: candle.close_time,
                // A replay has no receipt instant of its own.
                time_received: candle.close_time,
                exchange: EXCHANGE,
                instrument,
                kind: DataKind::Candle(candle),
            })
        })
        .collect();

    let market_data = MarketDataInMemory::new(Arc::new(events));
    let time_engine_start = market_data.time_first_event().await.unwrap();

    // `DefaultInstrumentMarketData` consumes `DataKind::Candle`, taking the latest candle's close
    // as the instrument price.
    let engine_state = EngineStateBuilder::new(&instruments, DefaultGlobalData, |_| {
        DefaultInstrumentMarketData::default()
    })
    .time_engine_start(time_engine_start)
    .trading_state(TradingState::Enabled)
    .build();

    let args_constant = Arc::new(BacktestArgsConstant {
        instruments,
        venue_options: Default::default(),
        executions,
        market_data,
        summary_interval: Daily,
        engine_state,
        aux_events: NoAuxEvents,
    });

    // `DefaultStrategy`/`DefaultRiskManager` place no orders: this example shows the market-data
    // wiring, not a strategy.
    let args_dynamic = BacktestArgsDynamic {
        id: "thetadata-candles".into(),
        risk_free_return,
        strategy: DefaultStrategy::<
            EngineState<DefaultGlobalData, DefaultInstrumentMarketData>,
        >::default(),
        risk: DefaultRiskManager::<
            EngineState<DefaultGlobalData, DefaultInstrumentMarketData>,
        >::default(),
    };

    let summary = backtest(args_constant, args_dynamic)
        .await
        .expect("backtest failed")
        .summary;

    println!("\nBacktest complete (BacktestId = {})", summary.id);
    summary.trading_summary.print_summary();
}

/// Every row of `rows`, as a candle, or the first error.
async fn collect_candles<Row>(
    rows: impl Stream<Item = Result<Row, ThetaDataError>>,
    candle: impl Fn(Row) -> Candle,
) -> Result<Vec<Candle>, ThetaDataError> {
    let mut rows = std::pin::pin!(rows);
    let mut candles = Vec::new();
    while let Some(row) = rows.next().await {
        candles.push(candle(row?));
    }
    Ok(candles)
}

/// The index of `symbol`'s instrument on [`EXCHANGE`], as registered in the config.
fn instrument_index(instruments: &IndexedInstruments, symbol: &str) -> InstrumentIndex {
    let wanted = InstrumentNameExchange::new(symbol);
    instruments
        .instruments()
        .iter()
        .find(|keyed| keyed.value.exchange.value == EXCHANGE && keyed.value.name_exchange == wanted)
        .map(|keyed| keyed.key)
        .unwrap_or_else(|| panic!("{symbol} is not registered on {EXCHANGE} in the config"))
}

pub fn load_config() -> Config {
    let file = File::open(CONFIG_PATH).expect("Failed to open config file");
    let reader = BufReader::new(file);
    serde_json::from_reader(reader).expect("Failed to parse config file")
}
