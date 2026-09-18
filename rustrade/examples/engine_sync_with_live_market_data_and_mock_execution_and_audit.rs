#![allow(clippy::unwrap_used, clippy::expect_used)] // Example code: panics acceptable for demonstration

//! Live market data driving a `MockExecution` system, with an audit stream.
//!
//! Every instrument here is priced and traded on the same venue. Two variations on that come up
//! often enough to be worth knowing before adapting this example, and they are different things:
//!
//! # One instrument, priced somewhere other than where it trades
//!
//! The same shares, contract or pair, quoted by a data provider and executed at a broker. Give the
//! `Instrument` a `DataVenue` — market data is then sourced from the data venue (under that venue's
//! own symbol, if it spells it differently) while orders still route to the execution venue, and
//! both roles stay on one instrument, so one position and one `pnl_unrealised`.
//!
//! # Two instruments, one priced and one traded
//!
//! A CFD or index proxy driving orders on the corresponding future, say. These are *different*
//! economic instruments, with their own expiries, multipliers and basis, so they are registered as
//! two ordinary `Instrument`s — not collapsed onto one with a `DataVenue`. The strategy reads the
//! priced instrument's state and emits orders naming the traded one; nothing in `AlgoStrategy`
//! couples the two. They keep separate ledgers: the position lives on the traded instrument, the
//! priced instrument holds none, and the conversion between them (hedge ratio, quantity, basis) is
//! yours to own.
//!
//! This is how an instrument kind no execution client accepts can still drive live trading: it
//! prices the decision, and a different, executable instrument carries the order.
//!
//! Register execution clients only for the venues actually traded on. A venue that only supplies
//! prices has no account to connect to, and `SystemBuilder` derives that from the execution clients
//! registered here — so a data-only venue does not hold global connectivity at `Reconnecting`.

use futures::StreamExt;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use rustrade::{
    engine::{
        clock::LiveClock,
        state::{
            global::DefaultGlobalData,
            instrument::{data::DefaultInstrumentMarketData, filter::InstrumentFilter},
            trading::TradingState,
        },
    },
    logging::init_logging,
    risk::DefaultRiskManager,
    statistic::time::Daily,
    strategy::DefaultStrategy,
    system::{
        builder::{AuditMode, EngineFeedMode, SystemArgs, SystemBuilder},
        config::SystemConfig,
    },
};
use rustrade_data::{
    streams::builder::dynamic::indexed::init_indexed_multi_exchange_market_stream,
    subscription::SubKind,
};
use rustrade_instrument::index::IndexedInstruments;
use rustrade_integration::Terminal;
use std::{fs::File, io::BufReader, time::Duration};
use tracing::debug;

const FILE_PATH_SYSTEM_CONFIG: &str = "rustrade/examples/config/system_config.json";
const RISK_FREE_RETURN: Decimal = dec!(0.05);

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialise Tracing
    init_logging();

    // Load SystemConfig
    let SystemConfig {
        instruments,
        executions,
    } = load_config()?;

    // Construct IndexedInstruments
    let instruments = IndexedInstruments::new(instruments);

    // Initialise MarketData Stream
    let market_stream = init_indexed_multi_exchange_market_stream(
        &instruments,
        &[SubKind::PublicTrades, SubKind::OrderBooksL1],
    )
    .await?;

    // Construct System Args
    let args = SystemArgs::new(
        &instruments,
        executions,
        LiveClock,
        DefaultStrategy::default(),
        DefaultRiskManager::default(),
        market_stream,
        DefaultGlobalData,
        |_| DefaultInstrumentMarketData::default(),
    );

    // Build & run full system:
    // See SystemBuilder for all configuration options
    let mut system = SystemBuilder::new(args)
        // Engine feed in Sync mode (Iterator input)
        .engine_feed_mode(EngineFeedMode::Iterator)
        // Audit feed is enabled (Engine sends audits)
        .audit_mode(AuditMode::Enabled)
        // Engine starts with TradingState::Disabled
        .trading_state(TradingState::Disabled)
        // Build System, but don't start spawning tasks yet
        .build()?
        // Init System, spawning component tasks on the current runtime
        .init_with_runtime(tokio::runtime::Handle::current())
        .await?;

    // Take ownership of the Engine audit snapshot with updates
    let audit = system.audit.take().unwrap();

    // Run dummy asynchronous AuditStream consumer
    // Note: you probably want to use this Stream to replicate EngineState, or persist events, etc.
    //  --> eg/ see examples/engine_sync_with_audit_replica_engine_state
    let audit_task = tokio::spawn(async move {
        let mut audit_stream = audit.updates.into_stream();
        while let Some(audit) = audit_stream.next().await {
            debug!(?audit, "AuditStream consumed AuditTick");
            if audit.event.is_terminal() {
                break;
            }
        }
        audit_stream
    });

    // Enable trading
    system.trading_state(TradingState::Enabled);

    // Let the example run for 5 seconds...
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Before shutting down, CancelOrders and then ClosePositions
    system.cancel_orders(InstrumentFilter::None);
    system.close_positions(InstrumentFilter::None);

    // Shutdown
    let (engine, _shutdown_audit) = system.shutdown().await?;
    let _audit_stream = audit_task.await?;

    // Generate TradingSummary<Daily>
    let trading_summary = engine
        .trading_summary_generator(RISK_FREE_RETURN)
        .generate(Daily);

    // Print TradingSummary<Daily> to terminal (could save in a file, send somewhere, etc.)
    trading_summary.print_summary();

    Ok(())
}

fn load_config() -> Result<SystemConfig, Box<dyn std::error::Error>> {
    let file = File::open(FILE_PATH_SYSTEM_CONFIG)?;
    let reader = BufReader::new(file);
    let config = serde_json::from_reader(reader)?;
    Ok(config)
}
