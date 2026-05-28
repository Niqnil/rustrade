# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.1] - 2026-05-28

### Added

- **Binance conditional order support** ([#93](https://github.com/Niqnil/rustrade/issues/93))
  - `Stop` → Binance `STOP_LOSS` (market order triggered at stop price)
  - `StopLimit` → Binance `STOP_LOSS_LIMIT` (limit order triggered at stop price)
  - `TakeProfit` → Binance `TAKE_PROFIT` (market order triggered at take-profit price)
  - `TakeProfitLimit` → Binance `TAKE_PROFIT_LIMIT` (limit order triggered at take-profit price)
  - `TrailingStop` with `BasisPoints` or `Percentage` offset → Binance `STOP_LOSS` with `trailingDelta`
    - `BasisPoints`: value passed directly as `trailingDelta` (1 bp = 0.01%)
    - `Percentage`: value multiplied by 100 before sending (e.g., 2% → 200 trailingDelta)
  - Note: `TrailingStop` with `Absolute` offset returns `UnsupportedOrderType` (manual conversion required: `(absolute / price) * 10000`)
  - Note: `TrailingStopLimit` returns `UnsupportedOrderType` (Binance doesn't support)

- **Hyperliquid conditional order support** ([#94](https://github.com/Niqnil/rustrade/issues/94))
  - `Stop` → Hyperliquid trigger order (`tpsl: "sl"`, `is_market: true`)
  - `StopLimit` → Hyperliquid trigger order (`tpsl: "sl"`, `is_market: false`)
  - `TakeProfit` → Hyperliquid trigger order (`tpsl: "tp"`, `is_market: true`)
  - `TakeProfitLimit` → Hyperliquid trigger order (`tpsl: "tp"`, `is_market: false`)
  - Trigger orders require UUID-format client order ID (`ClientOrderId::uuid()`) for cancellation support
  - Cancellation via `cancel_by_cloid()` for trigger orders (uses UUID), `cancel()` for regular orders (uses OID)
  - Note: `TrailingStop`, `TrailingStopLimit`, and `Market` return `UnsupportedOrderType`
  - Note: SDK limitation — `fetch_open_orders` and `account_stream` cannot distinguish trigger orders from limit orders (SDK structs lack trigger fields). Track `OrderKind` from placement response.

## [0.2.0]

### Added

- **Databento streaming variants** ([#46](https://github.com/Niqnil/rustrade/issues/46))
  - `DatabentoHistorical::fetch_trades_stream()`: Stream trades without collecting into memory
  - `DatabentoHistorical::fetch_quotes_stream()`: Stream quotes without collecting into memory
  - Avoids memory spikes for large historical queries (millions of records)

### Changed

- **BREAKING: Migrate from `async_trait` to native AFIT** ([#85](https://github.com/Niqnil/rustrade/issues/85))
  - `Subscriber`, `SubscriptionValidator`, `ExchangeTransformer`, and `MarketStream` traits now use native async fn in trait (Rust 1.75+)
  - Removed `async-trait` crate dependency
  - Additional `Sync` bounds added to some generic parameters where required
  - Return type changed from `Pin<Box<dyn Future + Send>>` to opaque `impl Future + Send`
  - No code changes required for most downstream users unless explicitly naming future types

- **Databento structured error types** ([#47](https://github.com/Niqnil/rustrade/issues/47))
  - New `DatabentoErrorKind` enum: `Authentication`, `RateLimit`, `Network`, `Decode`, `Api`
  - New `DataError::Databento { kind, context, message }` variant for programmatic error handling
  - Enables proper retry logic: don't retry auth errors, backoff on rate limits, retry network errors
  - All Databento errors now use structured types instead of `DataError::Socket(String)`

- **Databento `Arc<K>` performance documentation** ([#45](https://github.com/Niqnil/rustrade/issues/45))
  - Documented that instrument keys are cloned per record
  - Recommended `Arc<K>` for high-frequency scenarios to avoid per-record heap allocations
  - Added examples in rustdoc for `fetch_trades`, `fetch_quotes`, and `DatabentoLive`

- **BREAKING: Stateful `Subscriber` trait for credential injection** ([#43](https://github.com/Niqnil/rustrade/issues/43))
  - `Subscriber::subscribe` now takes `&self` instead of being a static method
  - `Subscriber` trait requires `Clone + Send + Sync` bounds
  - `StreamBuilder::subscribe()` now requires a subscriber instance as first argument:
    - Unauthenticated: `.subscribe(WebSocketSubscriber, [...])`
    - Authenticated (Alpaca): `.subscribe(AlpacaSubscriber::from_env()?, [...])`
  - `init_market_stream()` now takes subscriber as second argument
  - `AlpacaSubscriber` is now stateful with `AlpacaCredentials`:
    - `AlpacaSubscriber::new(credentials)`: Create with explicit credentials
    - `AlpacaSubscriber::from_env()`: Load from `ALPACA_API_KEY`/`ALPACA_SECRET_KEY`
    - `AlpacaCredentials::new(key, secret)`: Create credentials explicitly
    - `AlpacaCredentials::from_env()`: Load from environment
  - Auth errors now fail at construction time (fast fail) instead of first reconnect
  - Credentials are cloned into reconnect closure, available on every reconnect

### Added

- **BracketOrderClient supertrait**: Unified trait for bracket orders
  - `BracketOrderClient` trait extending `ExecutionClient` for exchanges supporting native bracket orders
  - `RequestOpenBracket` struct: Common request parameters (side, quantity, prices, TIF)
  - `BracketOrderRequest<ExchangeKey, InstrumentKey>` type alias using `OrderEvent`
  - `BracketOrderResult` with `Option<Order>` for child legs (documents API divergence)
  - `BracketOrderRequestBuilder` for fluent request construction
  - Implemented for `IbkrClient` (returns all 3 legs) and `AlpacaClient` (returns parent only)
  - Enables generic code: `T: ExecutionClient + BracketOrderClient`
- **Option Greeks support**: Real-time and computed Greeks for IBKR options
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
- **Stop and Trailing Stop order types**:
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
  - **Testing**: NOT TESTED in CI; offline fixture tests verified locally; live integration untested (requires paid subscription)
- **Alpaca market data connector**: Real-time trades and quotes via WebSocket
  - `AlpacaIex`: Free IEX feed for US equities
  - `AlpacaSip`: Paid consolidated SIP feed for US equities
  - `AlpacaCrypto`: Crypto market data
  - **Testing**: IEX and crypto feeds are tested with paper credentials; SIP requires Algo Trader Plus (paid subscription) and is NOT TESTED
- **Alpaca options market data**: REST-based option discovery and Greeks snapshots
  - `AlpacaOptionsClient`: Options market data client with rate limiting and pagination
  - `AlpacaOptionContractQuery`: Builder for filtering contracts by underlying, expiration, strike, type, style
  - `fetch_contracts(query)`: Discover option contracts via `GET /v2/options/contracts`
  - `AlpacaOptionSnapshot`: Option snapshot with quote and Greeks data
  - `fetch_snapshots(symbols, feed)`: Fetch snapshots with Greeks via `GET /v1beta1/options/snapshots`
  - `fetch_chain_snapshots(underlying, feed)`: Convenience method for entire option chains
  - `AlpacaOptionFeed`: `Opra` (real-time, paid) or `Indicative` (15-min delayed, free)
  - **Testing**: Indicative feed is tested; OPRA requires Algo Trader Plus (paid subscription) and is NOT TESTED
  - **Note**: Greeks streaming is NOT available — Alpaca only provides REST snapshots for Greeks data
- **Quotes subscription kind**: Generic top-of-book quotes (`SubKind::Quotes`)
- `ExchangeId::AlpacaBroker`: Dedicated variant for Alpaca execution client
  (distinct from market data feed identifiers)

### Changed

- **deps(ibkr)**: Bump `ibapi` from 2.11.4 to 2.12.0 — fixes TWS error surfacing on
  subscription channels ([rust-ibapi#567](https://github.com/wboayue/rust-ibapi/pull/567),
  closes [#78](https://github.com/Niqnil/rustrade/issues/78))
- **perf(alpaca)**: Pre-allocate `/v2/orders` endpoint URL at `AlpacaClient` construction,
  eliminating 2 heap allocations per order placement (`open_order_inner`, `open_bracket_order`).
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
- **BREAKING**: `RequestOpen.price` and `Order.price` changed from `Decimal` to `Option<Decimal>`.
  - Market, Stop, and TrailingStop orders: `price: None` (no limit price)
  - Limit, StopLimit, and TrailingStopLimit orders: `price: Some(limit_price)`
  - Removes the `dec!(0)` sentinel convention: Market/Stop orders now carry an explicit `None`
    rather than a placeholder zero, so callers can no longer plumb a meaningless price through
    them. (Note: `Some(price)` for a Market order still compiles — this is a clarity win, not a
    compiler-enforced invariant.)
  - Migration: For `Limit`, `StopLimit`, and `TrailingStopLimit` orders, wrap the
    limit price in `Some()`. For `Market`, `Stop`, and `TrailingStop` orders, use `None`.
- **BREAKING**: Removed `ExchangeId::Alpaca`.
  - Use `AlpacaIex`, `AlpacaSip`, or `AlpacaCrypto` for market data feeds
  - Use `AlpacaBroker` for execution
  - Migration: Replace `ExchangeId::Alpaca` with the appropriate specific variant
- **BREAKING**: `AlpacaBracketOrderRequest` and `AlpacaBracketOrderResult` marked `#[non_exhaustive]`
  ([#69](https://github.com/Niqnil/rustrade/issues/69)).
  - Allows future field additions without breaking downstream code
  - Struct literal construction no longer works; use `AlpacaBracketOrderRequest::new()` constructor
  - Optional stop-loss limit price: chain `.with_stop_loss_limit_price(price)` after construction

### Fixed

- **IBKR integration tests no longer leave zombie connections** ([#63](https://github.com/Niqnil/rustrade/issues/63)):
  - Added `disconnect()` method to `IbkrHistoricalData`, `IbkrMarketStream`, and `IbkrClient`
    for explicit connection cleanup
  - Added `Drop` implementations that call `disconnect()` to ensure IB Gateway releases
    client IDs even when tests panic or exit abruptly
  - Added `#[serial]` attribute to all IBKR integration tests to prevent parallel execution
    conflicts when sharing IB Gateway connections
  - Previously, repeated test runs would fail with "client id already in use" until IB Gateway
    was restarted

## [0.1.0]

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
