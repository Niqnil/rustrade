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
| **Bitfinex** | ✅ Spot | ❌ | WebSocket |
| **BitMEX** | ✅ Perpetual | ❌ | WebSocket |
| **Bybit** | ✅ Spot, Perpetual | ❌ | WebSocket |
| **Coinbase** | ✅ Spot | ❌ | WebSocket |
| **Gate.io** | ✅ Spot, Futures, Perpetual, Options | ❌ | WebSocket |
| **Kraken** | ✅ Spot | ❌ | WebSocket |
| **OKX** | ✅ Spot, Futures, Perpetual, Options | ❌ | WebSocket |

An exchange with no execution client is market-data only. See
[`rustrade-data`](rustrade-data/README.md) for the exact connector types and subscription
kinds each one serves, and [`rustrade-execution`](rustrade-execution/README.md) for the order
types each execution client accepts.

### Data Providers

| Provider | Asset Classes | Notes |
|----------|---------------|-------|
| **Massive** | Stocks, Crypto, Forex, Options, Futures | Historical + live streaming |
| **Databento** | Equities, Futures, Options | Nanosecond precision, DBN format |
| **London Strategic Edge** | FX, Crypto, Equities/ETFs, Futures proxies, CFDs | Live ticks, historical candles, bulk export. ⚠️ Data is **not redistributable** |

> **⚠️ London Strategic Edge data is NOT redistributable.** This integration's *code* is MIT
> like the rest of the repository; the *data* it retrieves is not. LSE permits use for your own
> research, trading and model training — including commercially — but prohibits redistributing,
> reselling, or otherwise making the data available to third parties, in bulk or through any
> competing feed, download service or interface. Do not commit retrieved data, publish it as
> fixtures or example datasets, or re-serve it. Terms: <https://londonstrategicedge.com/terms>

## Quick Start

Add to your `Cargo.toml`:

```toml
[dependencies]
rustrade = "0.6"
rustrade-data = { version = "0.6", features = ["hyperliquid"] }
rustrade-execution = { version = "0.6", features = ["binance"] }
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
