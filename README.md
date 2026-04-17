# Barter

> **This is a public fork of [barter-rs/barter-rs](https://github.com/barter-rs/barter-rs).**
> It adds live execution clients for **Binance** and **Alpaca** that are not available in the upstream repository.
> For questions or discussion, please use [GitHub Discussions](https://github.com/Niqnil/barter-rs/discussions).

Barter is an algorithmic trading ecosystem of Rust libraries for building high-performance live-trading, paper-trading
and back-testing systems.
* **Fast**: Written in native Rust. Minimal allocations. Data-oriented state management system with direct index lookups.
* **Robust**: Strongly typed. Thread safe. Extensive test coverage.
* **Customisable**: Plug and play Strategy and RiskManager components that facilitates most trading strategies (MarketMaking, StatArb, HFT, etc.).
* **Scalable**: Multithreaded architecture with modular design. Leverages Tokio for I/O. Memory efficient data structures.

[![MIT licensed][mit-badge]][mit-url]

[mit-badge]: https://img.shields.io/badge/license-MIT-blue.svg
[mit-url]: https://github.com/Niqnil/barter-rs/blob/develop/LICENSE

## Disclaimer

This software is for educational purposes only. USE THE SOFTWARE AT YOUR OWN RISK. THE AUTHORS AND ALL AFFILIATES ASSUME NO RESPONSIBILITY FOR YOUR TRADING RESULTS.

## Overview
Barter is an algorithmic trading ecosystem of Rust libraries for building high-performance live-trading, paper-trading
and back-testing systems. It is made up of several easy-to-use, extensible crates:
* **Barter**: Algorithmic trading Engine with feature rich state management system.
* **Barter-Instrument**: Exchange, Instrument and Asset data structures and utilities.
* **Barter-Data**: Stream public market data from financial venues. Easily extensible via the MarketStream interface.
* **Barter-Execution**: Stream private account data and execute orders. Includes live clients for **Binance** (spot) and **Alpaca** (equities, options, crypto). Easily extensible via the ExecutionClient interface.
* **Barter-Integration**: Low-level frameworks for flexible REST/WebSocket integrations.

## Notable Features
- Stream public market data from financial venues via the [`Barter-Data`] library.
- Stream private account data, execute orders (live or mock) via the [`Barter-Execution`] library.
- **Live execution clients for Binance (spot) and Alpaca** — not available in the upstream repo.
- Plug and play Strategy and RiskManager components that facilitate most trading strategies.
- Backtest utilities for efficiently running thousands of concurrent backtests.
- Flexible Engine that facilitates trading strategies that execute on many exchanges simultaneously.
- Use mock MarketStream or Execution components to enable back-testing on a near-identical trading system as live-trading.
- Centralised cache friendly state management system with O(1) constant lookups using indexed data structures.
- Robust Order management system - use stand-alone or with Barter.
- Trading summaries with comprehensive performance metrics (PnL, Sharpe, Sortino, Drawdown, etc.).
- Turn on/off algorithmic trading from an external process (eg/ UI, Telegram, etc.) whilst still processing market/account data.
- Issue Engine Commands from an external process (eg/ UI, Telegram, etc.) to initiate actions (CloseAllPositions, OpenOrders, CancelOrders, etc.).
- EngineState replica manager that processes the Engine AuditStream to facilitate non-hot path monitoring components (eg/ UI, Telegram, etc.).

[`Barter`]: https://crates.io/crates/barter
[`Barter-Instrument`]: https://crates.io/crates/barter-instrument
[`Barter-Data`]: https://crates.io/crates/barter-data
[`Barter-Execution`]: https://crates.io/crates/barter-execution
[`Barter-Integration`]: https://crates.io/crates/barter-integration
[API Documentation]: https://docs.rs/barter/latest/barter/
[barter-examples]: https://github.com/Niqnil/barter-rs/tree/develop/barter/examples

## Examples
* See [here][barter-examples] for the compilable example including imports.
* See sub-crates for further examples of each library.

#### Paper Trading With Live Market Data & Mock Execution

```rust,no_run
const FILE_PATH_SYSTEM_CONFIG: &str = "barter/examples/config/system_config.json";
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
        .build::<EngineEvent, DefaultGlobalData, DefaultInstrumentMarketData>()?

        // Init System, spawning component tasks on the current runtime
        .init_with_runtime(tokio::runtime::Handle::current())
        .await?;

    // Take ownership of Engine audit receiver
    let audit_rx = system.audit_rx.take().unwrap();

    // Run dummy asynchronous AuditStream consumer
    // Note: you probably want to use this Stream to replicate EngineState, or persist events, etc.
    //  --> eg/ see examples/engine_sync_with_audit_replica_engine_state
    let audit_task = tokio::spawn(async move {
        let mut audit_stream = audit_rx.into_stream();
        while let Some(audit) = audit_stream.next().await {
            debug!(?audit, "AuditStream consumed AuditTick");
            if let EngineAudit::Shutdown(_) = audit.event {
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
```

## Getting Help
See if the answer to your question can be found in the [API Documentation]. If not, open a [Discussion](https://github.com/Niqnil/barter-rs/discussions) on GitHub.

## Contributing
Contributions are welcome. Please open a PR targeting the `develop` branch. See [CONTRIBUTING.md](CONTRIBUTING.md) for the full workflow.

### Licence
This project is licensed under the [MIT license].

[MIT license]: https://github.com/Niqnil/barter-rs/blob/develop/LICENSE

### Contribution License Agreement

Any contribution you intentionally submit for inclusion in Barter workspace crates shall be:
1. Licensed under MIT
2. Subject to the disclaimer above
3. Provided without any additional terms or conditions

By submitting a contribution, you certify that you have the right to do so under these terms.
