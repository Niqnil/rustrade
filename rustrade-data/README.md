# rustrade-data

Integration library for streaming public market data from exchanges and data providers.

## Supported Exchanges

| Exchange | Constructor | InstrumentKinds | SubscriptionKinds |
|:--------:|:-----------:|:---------------:|:-----------------:|
| **BinanceSpot** | `BinanceSpot::default()` | Spot | PublicTrades, OrderBooksL1, OrderBooksL2, Candles |
| **BinanceFuturesUsd** | `BinanceFuturesUsd::default()` | Perpetual | PublicTrades, OrderBooksL1, OrderBooksL2 |
| **BinanceFuturesUsdMarket** | `BinanceFuturesUsdMarket::default()` | Perpetual | Liquidations, Candles |
| **Bitfinex** | `Bitfinex` | Spot | PublicTrades |
| **Bitmex** | `Bitmex` | Perpetual | PublicTrades |
| **BybitSpot** | `BybitSpot::default()` | Spot | PublicTrades, OrderBooksL1, OrderBooksL2 |
| **BybitPerpetualsUsd** | `BybitPerpetualsUsd::default()` | Perpetual | PublicTrades, OrderBooksL1, OrderBooksL2 |
| **Coinbase** | `Coinbase` | Spot | PublicTrades |
| **GateioSpot** | `GateioSpot::default()` | Spot | PublicTrades |
| **GateioFuturesUsd** | `GateioFuturesUsd::default()` | Future | PublicTrades |
| **GateioFuturesBtc** | `GateioFuturesBtc::default()` | Future | PublicTrades |
| **GateioPerpetualsUsd** | `GateioPerpetualsUsd::default()` | Perpetual | PublicTrades |
| **GateioPerpetualsBtc** | `GateioPerpetualsBtc::default()` | Perpetual | PublicTrades |
| **GateioOptions** | `GateioOptions::default()` | Option | PublicTrades |
| **Kraken** | `Kraken` | Spot | PublicTrades, OrderBooksL1 |
| **Okx** | `Okx` | Spot, Future, Perpetual, Option | PublicTrades |
| **Hyperliquid** | `Hyperliquid::default()` | Perpetual | PublicTrades, OrderBooksL2 |
| **HyperliquidSpot** | `HyperliquidSpot::default()` | Spot | PublicTrades, OrderBooksL2 |
| **IBKR** | `IbkrMarketStream::init(config, contracts, subscriptions)` | Spot, Future, Option | PublicTrades, Quotes, OrderBooksL1, OrderBooksL2, Candles, OptionGreeks |
| **AlpacaIex** | `AlpacaIex::default()`, with `AlpacaSubscriber` | Spot (Equities) | PublicTrades, Quotes, OrderBooksL1 |
| **AlpacaSip** | `AlpacaSip::default()`, with `AlpacaSubscriber` | Spot (Equities) | PublicTrades, Quotes, OrderBooksL1 |
| **AlpacaCrypto** | `AlpacaCrypto::default()`, with `AlpacaSubscriber` | Spot (Crypto) | PublicTrades, Quotes, OrderBooksL1 |
| **AlpacaOptionsClient** | `AlpacaOptionsClient::from_env()` | Option | Quotes, OptionGreeks (REST snapshots) |

> **Binance USD-M futures WebSocket tiers:** Binance routes futures streams across mutually-exclusive
> WebSocket tiers, so the typed `Streams` path splits them across two server types. `Liquidations`
> (`@forceOrder`) and `Candles` (`@continuousKline_`) are served only on the `/market` tier, exposed as
> `BinanceFuturesUsdMarket`; trades and order books stay on `BinanceFuturesUsd`. The `DynamicStreams` /
> `ExchangeId` path handles this routing automatically.

> **Authenticated feeds:** Alpaca, Massive and London Strategic Edge each take a subscriber that
> carries the credentials (`AlpacaSubscriber`, `MassiveSubscriber`, `LseSubscriber`, each with
> `new(credentials)` and `from_env()`). Clones of one subscriber share its connections, and each
> provider limits how many connections a key may hold. The `DynamicStreams` path does not build
> these subscribers: pass them in through `DynamicSubscribers::with_alpaca`, `with_massive` and
> `with_lse`, then call `DynamicStreams::init_with`.

## Data Providers

| Provider | Constructor | InstrumentKinds | SubscriptionKinds |
|:--------:|:-----------:|:---------------:|:-----------------:|
| **Massive** (REST) | `MassiveRestClient::from_env()` | Spot, Future, Option | PublicTrades, Quotes, Candles (historical) |
| **MassiveStocks** / **MassiveCrypto** / **MassiveOptions** | `MassiveStocks::default()` etc., with `MassiveSubscriber` | Spot (Equities, Crypto), Option | PublicTrades, Quotes, OrderBooksL1, Candles (1s, 1m) |
| **MassiveForex** | `MassiveForex::default()`, with `MassiveSubscriber` | Spot (FX) | Quotes, OrderBooksL1, Candles (1s, 1m) |
| **Databento** | `DatabentoHistorical` / `DatabentoLive` | Spot, Future, Option | PublicTrades, Quotes, Candles |
| **LseFx** | `LseFx::default()`, with `LseSubscriber` | Spot (FX) | PublicTrades, OrderBooksL1 |
| **LseCrypto** | `LseCrypto::default()`, with `LseSubscriber` | Spot (Crypto) | PublicTrades, OrderBooksL1 |
| **LseEquities** | `LseEquities::default()`, with `LseSubscriber` | Spot (Equities, ETFs) | PublicTrades, OrderBooksL1 |
| **LseFutures** | `LseFutures::default()`, with `LseSubscriber` | Cfd (continuous front-month proxies) | PublicTrades, OrderBooksL1 |
| **LseCfd** | `LseCfd::default()`, with `LseSubscriber` | Cfd (indices, commodities, rates, volatility) | PublicTrades, OrderBooksL1 |
| **LseOptions** | `LseOptions::default()`, with `LseSubscriber` | Option (US equity and ETF contracts) | PublicTrades |
| **LSE historical** | `LseVaultClient::from_env()` | — | Candles (REST, not a subscription) |

> **⚠️ London Strategic Edge data is NOT redistributable.** This integration's *code* is MIT
> like the rest of the repository; the *data* it retrieves is not. LSE permits use for your own
> research, trading and model training — including commercially — but prohibits redistributing,
> reselling, or otherwise making the data available to third parties, in bulk or through any
> competing feed, download service or interface. Do not commit retrieved data, publish it as
> fixtures or example datasets, or re-serve it. Terms: <https://londonstrategicedge.com/terms>

> **The LSE feed has properties that will silently mislead if assumed away** — FX candles are
> BID candles, candle volume is unreliable, non-trading days are emitted as flat bars, the live
> tick is a quote rather than a print (except on options), and live volume is fabricated on
> `LseFx` and `LseCfd`. See the `exchange::lse` module documentation before relying on any of it.

## Build memory

A debug build of this crate with all features needs several GB of memory in a single `rustc`
process, almost all of it for debug info. Cargo applies your workspace's `dev` profile to
dependencies too, so this cost reaches you. If it matters, reduce this crate's debug info in the
`Cargo.toml` at your workspace root; Cargo ignores `[profile]` sections in any other manifest:

```toml
[profile.dev.package.rustrade-data]
debug = "line-tables-only"
```

This keeps file and line numbers in backtraces, and brings the peak to about 1 GB.

See the [workspace README](../README.md) for documentation, examples, and contributing guidelines.
