#![allow(clippy::unwrap_used, clippy::expect_used)] // Example code: panics acceptable for demonstration

use rust_decimal::Decimal;
use rustrade::{
    backtest::{
        BacktestArgsConstant, BacktestArgsDynamic,
        aux_events::NoAuxEvents,
        market_data::{BacktestMarketData, MarketDataInMemory},
        run_backtests,
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
use rustrade_data::streams::consumer::MarketStreamEvent;
use rustrade_instrument::index::IndexedInstruments;
use serde::Deserialize;
use smol_str::{SmolStr, ToSmolStr};
use std::{
    fs::File,
    io::{BufRead, BufReader},
    sync::Arc,
};

const CONFIG_PATH: &str = "rustrade/examples/config/backtest_config.json";
const FILE_PATH_MARKET_DATA_INDEXED: &str =
    "rustrade/examples/data/binance_spot_trades_l1_btcusdt_ethusdt_solusdt.json";
const NUM_BACKTESTS: usize = 10000;

#[derive(Deserialize)]
pub struct Config {
    pub risk_free_return: Decimal,
    pub system: SystemConfig,
}

#[tokio::main]
async fn main() {
    // Initialise Tracing
    rustrade::logging::init_logging();

    let Config {
        risk_free_return,
        system: SystemConfig {
            instruments,
            executions,
        },
    } = load_config();

    // Construct IndexedInstruments
    let instruments = IndexedInstruments::new(instruments);

    // Initialise MarketData
    let market_events = market_data_from_file(FILE_PATH_MARKET_DATA_INDEXED);
    let market_data = MarketDataInMemory::new(Arc::new(market_events));
    let time_engine_start = market_data.time_first_event().await.unwrap();

    // Construct EngineState
    let engine_state = EngineStateBuilder::new(&instruments, DefaultGlobalData, |_| {
        DefaultInstrumentMarketData::default()
    })
    .time_engine_start(time_engine_start)
    .trading_state(TradingState::Enabled)
    .build();

    // Construct constant backtest arguments
    let args_constant = Arc::new(BacktestArgsConstant {
        instruments,
        executions,
        market_data,
        summary_interval: Daily,
        engine_state,
        aux_events: NoAuxEvents,
    });

    // Define dummy dynamic backtest arguments
    let dynamic_arg = BacktestArgsDynamic {
        id: SmolStr::default(),
        risk_free_return,
        strategy: DefaultStrategy::<EngineState<DefaultGlobalData, DefaultInstrumentMarketData>>::default(),
        risk: DefaultRiskManager::<EngineState<DefaultGlobalData, DefaultInstrumentMarketData>>::default(),
    };

    // Generate dummy iterator of cloned dynamic arguments
    // Note that concurrent backtests should be run with different BacktestArgsDynamic!
    let args_dynamic_iter = (0..NUM_BACKTESTS).map(|index| {
        let mut dynamic_args = dynamic_arg.clone();
        dynamic_args.id = index.to_smolstr();
        dynamic_args
    });

    let mut summary = run_backtests(args_constant, args_dynamic_iter)
        .await
        .unwrap();

    // Analyse backtest summaries...
    println!("\nNum Backtests: {}", summary.num_backtests);
    println!("Duration: {:?}", summary.duration);
    // For example, find the backtest with the highest cumulative PnL
    summary.summaries.sort_by(|a, b| {
        let backtest_a_total_pnl = a
            .trading_summary
            .instruments
            .values()
            .map(|tear| tear.pnl)
            .sum::<Decimal>();
        let backtest_b_total_pnl = b
            .trading_summary
            .instruments
            .values()
            .map(|tear| tear.pnl)
            .sum::<Decimal>();

        backtest_a_total_pnl.cmp(&backtest_b_total_pnl).reverse()
    });
    let best_cumulative_sharpe = summary.summaries.first().unwrap();

    println!(
        "\nBest Cumulative Sharpe: BacktestId = {}",
        best_cumulative_sharpe.id
    );
    best_cumulative_sharpe.trading_summary.print_summary()
}

pub fn load_config() -> Config {
    let file = File::open(CONFIG_PATH).expect("Failed to open config file");
    let reader = BufReader::new(file);
    serde_json::from_reader(reader).expect("Failed to parse config file")
}

/// Reads a newline-delimited JSON market data file, ordered ready for
/// [`MarketDataInMemory`](rustrade::backtest::market_data::MarketDataInMemory).
///
/// # Why the sort is here
///
/// `MarketDataInMemory::new` requires its events to be globally sorted ascending by
/// `time_exchange`, and asserts it, because the backtest merges them against auxiliary events on
/// one timeline — an unsorted stream would drive the clock backwards and silently produce wrong
/// results.
///
/// A recorded multi-instrument capture does not satisfy that on its own. The file this example
/// reads interleaves three instruments and holds over ten thousand inversions, so handing it
/// straight to `MarketDataInMemory::new` panics before the first backtest starts. Any real capture
/// you substitute will need the same treatment.
///
/// The sort is stable, so events sharing a timestamp keep their recorded order.
pub fn market_data_from_file<InstrumentKey, Kind>(
    file_path: &str,
) -> Vec<MarketStreamEvent<InstrumentKey, Kind>>
where
    InstrumentKey: for<'de> Deserialize<'de>,
    Kind: for<'de> Deserialize<'de>,
{
    let file = File::open(file_path).unwrap();
    let reader = BufReader::new(file);

    let mut events = reader
        .lines()
        .map(|line_result| {
            let line = line_result.unwrap();
            serde_json::from_str::<MarketStreamEvent<InstrumentKey, Kind>>(&line).unwrap()
        })
        .collect::<Vec<_>>();

    // `Reconnecting` carries no timestamp, so the sort key maps it to `None` — which `Option`'s
    // `Ord` places before every `Some`, hoisting it to the front of the run whatever time it
    // actually occurred at. This corpus is `Item`-only; assert it, so a capture containing
    // reconnects fails loudly here rather than being quietly reordered.
    assert!(
        events
            .iter()
            .all(|event| matches!(event, MarketStreamEvent::Item(_))),
        "market_data_from_file expects an Item-only corpus: the time sort cannot order \
         Reconnecting events. Filter or stable-partition them first."
    );
    events.sort_by_key(|event| match event {
        MarketStreamEvent::Item(event) => Some(event.time_exchange),
        MarketStreamEvent::Reconnecting(_) => None,
    });

    events
}
