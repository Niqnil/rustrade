# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **Option Greeks support** (Phase 5): Real-time and computed Greeks for IBKR options
  - `DataKind::OptionGreeks(OptionGreeks)` variant for the unified market data stream
  - `IbkrSubscriptionKind::OptionGreeks` for live streaming via `market_data()` subscription
  - `OptionGreeks` struct (`subscription::greeks`): `delta`, `gamma`, `theta`, `vega`, `implied_volatility`,
    `theoretical_price`, `underlying_price` (all `Option<f64>`); marked `#[non_exhaustive]`
  - `OptionGreeks::has_any_greek()` returns true when at least one first-order Greek is present
    (excludes `theoretical_price` / `underlying_price`)
  - `IbkrHistoricalData::calculate_theoretical_greeks(contract, volatility, underlying_price)`:
    IB-side Greeks calculator from user-supplied IV and underlying
  - `IbkrHistoricalData::calculate_implied_volatility(contract, option_price, underlying_price)`:
    IB-side IV calculator from user-supplied option/underlying prices
  - `IbkrHistoricalData::fetch_option_chain(symbol, exchange, security_type, contract_id)` returning
    `Vec<OptionChainEntry>` with available expirations, strikes, trading classes, and exchanges
  - `OptionChainEntry` struct (`exchange::ibkr::options`): marked `#[non_exhaustive]`; `strikes` is
    `Vec<rust_decimal::Decimal>` (financial values must use `Decimal` per project standard)
  - `IbkrMarketStream` rejects non-`SecurityType::Option` contracts on `OptionGreeks` subscription
    with `DataError::Socket` (fail-fast over silent zero events)
- **Historical tick data APIs** for IBKR: `fetch_historical_ticks`, `fetch_historical_bid_ask`
- Cargo `required-features` declarations for feature-gated examples
  (`download_databento_fixtures`, `hyperliquid_*`, `ibkr_*`); `cargo check --all-targets`
  no longer fails on default features
- **Stop and Trailing Stop order types** (TG13 Phase 1+2):
  - `OrderKind::Stop { trigger_price }`: Stop market orders
  - `OrderKind::StopLimit { trigger_price }`: Stop-limit orders
  - `OrderKind::TrailingStop { offset, offset_type }`: Trailing stop orders
  - `OrderKind::TrailingStopLimit { offset, offset_type, limit_offset }`: Trailing stop-limit orders
  - `TrailingOffsetType` enum: `Absolute`, `Percentage`, `BasisPoints`
  - IBKR connector: Full support for all stop/trailing order types
  - Binance/Alpaca connectors: Return `UnsupportedOrderType` error (support planned)
- `OrderError::UnsupportedOrderType`: New error variant for connectors that don't support certain order types
- **Massive market data connector**: Historical, live, and reference data via `massive` feature
  - `MassiveRestClient`: Historical aggregates, trades, quotes with streaming pagination
  - `MassiveLive`: Real-time WebSocket streaming for trades, quotes, and aggregates
  - Reference data: `fetch_tickers()`, `fetch_ticker_details()`, `fetch_exchanges()`, `fetch_market_status()`, `fetch_market_holidays()`
  - Corporate actions: `fetch_dividends()`, `fetch_splits()` for stocks/ETFs
  - `TickerQuery` builder for filtering ticker searches
  - `ExchangeId::Massive` variant
  - Supports all asset classes: stocks, crypto, forex, options, indices, futures
- **Databento market data connector**: Historical and live data via `databento` feature
  - `DatabentoHistorical`: One-shot queries for trades and quotes in DBN format
  - `DatabentoLive<K>`: Real-time WebSocket streaming with `PitSymbolMap` symbol resolution
  - `ExchangeId` variants: `DatabentoGlbx`, `DatabentoXnas`, `DatabentoXnys`, `DatabentoDbeq`, `DatabentoOpra`
  - Nanosecond-precision timestamps and lossless Decimal price conversion
  - **Note**: Live data integration is NOT TESTED — Databento does not offer development/sandbox keys and we do not have a subscription.
    Offline fixture tests verify transformation logic; network integration is unverified.
- **Alpaca market data connector**: Real-time trades and quotes via WebSocket
  - `AlpacaIex`: Free IEX feed for US equities
  - `AlpacaSip`: Paid consolidated SIP feed for US equities
  - `AlpacaCrypto`: Crypto market data
- **Quotes subscription kind**: Generic top-of-book quotes (`SubKind::Quotes`)
- `ExchangeId::AlpacaBroker`: Dedicated variant for Alpaca execution client
  (distinct from market data feed identifiers)

### Changed

- **BREAKING**: `PublicTrade::side` changed from `Side` to `Option<Side>`.
  - Crypto connectors (Binance, Hyperliquid, Alpaca Crypto, etc.): `Some(side)`
  - Equities connectors (Alpaca IEX/SIP, IBKR): `None` — taker side not available
  - Databento: `Some(side)` for 'A'/'B', `None` for 'N' (no side specified)
  - Migration: Match on `Some(side)` to handle the `None` case explicitly, or use
    `.is_some_and(|s| s == Side::Buy)` for boolean checks. (`Side` does not implement
    `Default`, so `unwrap_or_default()` will not compile.)
- **BREAKING**: `OptionChainEntry::expirations` changed from `Vec<String>` to `Vec<NaiveDate>`.
  - Removes IBKR wire format leakage (YYYYMMDD strings) from caller code
  - Invalid expiration strings are now filtered during `from_ib()` conversion
  - Migration: Replace string parsing with direct `NaiveDate` usage
- **BREAKING**: `PublicTrade`, `Quote`, `Candle`, and `Liquidation` price/amount fields
  changed from `f64` to `rust_decimal::Decimal` for financial precision.
  - `PublicTrade`: `price`, `amount` now `Decimal`
  - `Quote`: `bid_price`, `ask_price`, `bid_amount`, `ask_amount` now `Decimal`
  - `Candle`: `open`, `high`, `low`, `close`, `volume` now `Decimal`
  - `Liquidation`: `price`, `quantity` now `Decimal`
  - Migration: Use `dec!()` macro for literals, `> Decimal::ZERO` for positivity checks.
    For string-typed JSON fields, use `de_str` deserializer or `.parse::<Decimal>()`.
    Use `Decimal::try_from(f64)` only when the source is already `f64` (e.g., IBKR API).

### Deprecated

- `ExchangeId::Alpaca`: Use `AlpacaIex`, `AlpacaSip`, or `AlpacaCrypto` instead.
  The bare `Alpaca` variant will be removed in the next major version.
  Migration: Replace `ExchangeId::Alpaca` with `ExchangeId::AlpacaIex` for US equities.

## [0.1.0] - 2026-05-01

Initial release of rustrade, a fork of [barter-rs](https://github.com/barter-rs/barter-rs).

### Added

- **Hyperliquid support**: Full perpetuals and spot trading via `hyperliquid` feature
- **Interactive Brokers support**: Market data and execution via `ibkr` feature
- **Alpaca support**: Equities, options, and crypto execution via `alpaca` feature
- **Binance support**: Spot market data and execution via `binance` feature
- Structured error types with transient/permanent classification for retry logic
- Order state tracking with `Filled`, `Cancelled`, and `Expired` variants

### Changed

- Renamed crate ecosystem from `barter-*` to `rustrade-*`
- Bumped all crate versions to 0.1.0 for fresh namespace
- Updated minimum supported Rust version to 1.95

### Fork Attribution

This release is based on barter-rs v0.12.4. See [NOTICE](NOTICE) for full attribution.
