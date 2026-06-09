# rustrade

A Rust ecosystem for building high-performance algorithmic trading systems.

[![MIT licensed][mit-badge]][mit-url]
[![Crates.io][crates-badge]][crates-url]
[![docs.rs][docs-badge]][docs-url]
[![Alpaca Integration][alpaca-badge]][alpaca-url]
[![Alpaca Weekly][alpaca-weekly-badge]][alpaca-weekly-url]

[mit-badge]: https://img.shields.io/badge/license-MIT-blue.svg
[mit-url]: https://github.com/Niqnil/rustrade/blob/main/LICENSE
[crates-badge]: https://img.shields.io/crates/v/rustrade.svg
[crates-url]: https://crates.io/crates/rustrade
[docs-badge]: https://img.shields.io/docsrs/rustrade
[docs-url]: https://docs.rs/rustrade
[alpaca-badge]: https://github.com/Niqnil/rustrade/actions/workflows/alpaca-integration.yml/badge.svg
[alpaca-url]: https://github.com/Niqnil/rustrade/actions/workflows/alpaca-integration.yml
[alpaca-weekly-badge]: https://github.com/Niqnil/rustrade/actions/workflows/alpaca-weekly.yml/badge.svg
[alpaca-weekly-url]: https://github.com/Niqnil/rustrade/actions/workflows/alpaca-weekly.yml

## Overview

rustrade is a collection of Rust libraries for live-trading, paper-trading, and backtesting. It provides:

* **Fast**: Native Rust with minimal allocations. Data-oriented state management with O(1) lookups.
* **Robust**: Strongly typed, thread-safe, with extensive test coverage.
* **Customisable**: Plug-and-play Strategy and RiskManager components.
* **Scalable**: Multithreaded architecture leveraging Tokio for async I/O.

### Crates

| Crate | Description |
|-------|-------------|
| [`rustrade`][rustrade-crate] | Algorithmic trading engine with state management |
| [`rustrade-data`][rustrade-data-crate] | Stream public market data from exchanges |
| [`rustrade-execution`][rustrade-execution-crate] | Stream account data and execute orders |
| [`rustrade-instrument`][rustrade-instrument-crate] | Exchange, instrument, and asset data structures |
| [`rustrade-integration`][rustrade-integration-crate] | Low-level REST/WebSocket integration framework |

[rustrade-crate]: https://crates.io/crates/rustrade
[rustrade-data-crate]: https://crates.io/crates/rustrade-data
[rustrade-execution-crate]: https://crates.io/crates/rustrade-execution
[rustrade-instrument-crate]: https://crates.io/crates/rustrade-instrument
[rustrade-integration-crate]: https://crates.io/crates/rustrade-integration

### Supported Exchanges

| Exchange | Market Data | Execution | Notes |
|----------|-------------|-----------|-------|
| **Binance** | ✅ Spot, USD-M Futures | ✅ Spot, Margin (cross/isolated) | WebSocket + REST |
| **Alpaca** | ✅ Equities (IEX/SIP), Crypto, Options | ✅ Equities, Options, Crypto | WebSocket + REST |
| **Hyperliquid** | ✅ Perps, Spot | ✅ Perps, Spot | WebSocket + REST |
| **Interactive Brokers** | ✅ All asset classes | ✅ All asset classes | TWS/Gateway API |

### Data Providers

| Provider | Asset Classes | Notes |
|----------|---------------|-------|
| **Massive** | Stocks, Crypto, Forex, Options, Futures | Historical + live streaming |
| **Databento** | Equities, Futures, Options | Nanosecond precision, DBN format |

## Quick Start

Add to your `Cargo.toml`:

```toml
[dependencies]
rustrade = "0.3"
rustrade-data = { version = "0.3", features = ["hyperliquid"] }
rustrade-execution = { version = "0.3", features = ["binance"] }
```

See the [examples](https://github.com/Niqnil/rustrade/tree/main/rustrade/examples) for complete working code.

## Minimum Supported Rust Version

Rust 1.95 or later.

## Disclaimer

This software is for educational purposes only. USE THE SOFTWARE AT YOUR OWN RISK. THE AUTHORS AND ALL AFFILIATES ASSUME NO RESPONSIBILITY FOR YOUR TRADING RESULTS.

## Fork Attribution

This project is a fork of [barter-rs](https://github.com/barter-rs/barter-rs), originally developed by Just A Stream, Inc. and the Barter Ecosystem Contributors. See [NOTICE](NOTICE) for full attribution.

Fork history:
- Original: [barter-rs/barter-rs](https://github.com/barter-rs/barter-rs)
- Intermediate: [Niqnil/barter-rs](https://github.com/Niqnil/barter-rs)
- Current: [Niqnil/rustrade](https://github.com/Niqnil/rustrade)

## Getting Help

Check the [API Documentation](https://docs.rs/rustrade). If your question isn't answered there, open a [Discussion](https://github.com/Niqnil/rustrade/discussions) on GitHub.

## Contributing

Contributions welcome! Please open a PR targeting the `develop` branch. See [CONTRIBUTING.md](CONTRIBUTING.md).

## License

MIT License. See [LICENSE](LICENSE).
