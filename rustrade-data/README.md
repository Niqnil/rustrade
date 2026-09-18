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
| **IBKR** | `IbkrMarketStream::connect()` | Spot, Future, Option | PublicTrades, Quotes, OrderBooksL1, OrderBooksL2, Candles, OptionGreeks |
| **AlpacaIex** | `AlpacaIex::new(credentials)` | Spot (Equities) | PublicTrades, Quotes |
| **AlpacaSip** | `AlpacaSip::new(credentials)` | Spot (Equities) | PublicTrades, Quotes |
| **AlpacaCrypto** | `AlpacaCrypto::new(credentials)` | Spot (Crypto) | PublicTrades, Quotes |
| **AlpacaOptionsClient** | `AlpacaOptionsClient::new(credentials)` | Option | Quotes, OptionGreeks (REST snapshots) |

> **Binance USD-M futures WebSocket tiers:** Binance routes futures streams across mutually-exclusive
> WebSocket tiers, so the typed `Streams` path splits them across two server types. `Liquidations`
> (`@forceOrder`) and `Candles` (`@continuousKline_`) are served only on the `/market` tier, exposed as
> `BinanceFuturesUsdMarket`; trades and order books stay on `BinanceFuturesUsd`. The `DynamicStreams` /
> `ExchangeId` path handles this routing automatically.

## Data Providers

| Provider | Constructor | InstrumentKinds | SubscriptionKinds |
|:--------:|:-----------:|:---------------:|:-----------------:|
| **Massive** | `MassiveRestClient` / `MassiveLive` | Spot, Future, Option | PublicTrades, Quotes, Candles |
| **Databento** | `DatabentoHistorical` / `DatabentoLive` | Spot, Future, Option | PublicTrades, Quotes, Candles |
| **LseFx** | `LseFx::default()` | Spot (FX) | PublicTrades, OrderBooksL1 |
| **LseCrypto** | `LseCrypto::default()` | Spot (Crypto) | PublicTrades, OrderBooksL1 |
| **LseEquities** | `LseEquities::default()` | Spot (Equities, ETFs) | PublicTrades, OrderBooksL1 |
| **LseFutures** | `LseFutures::default()` | Cfd (continuous front-month proxies) | PublicTrades, OrderBooksL1 |
| **LseCfd** | `LseCfd::default()` | Cfd (indices, commodities, rates, volatility) | PublicTrades, OrderBooksL1 |
| **LSE historical** | `LseVaultClient::from_env()` | — | Candles (REST, not a subscription) |

> **⚠️ London Strategic Edge data is NOT redistributable.** This integration's *code* is MIT
> like the rest of the repository; the *data* it retrieves is not. LSE permits use for your own
> research, trading and model training — including commercially — but prohibits redistributing,
> reselling, or otherwise making the data available to third parties, in bulk or through any
> competing feed, download service or interface. Do not commit retrieved data, publish it as
> fixtures or example datasets, or re-serve it. Terms: <https://londonstrategicedge.com/terms>

> **The LSE feed has properties that will silently mislead if assumed away** — FX candles are
> BID candles, candle volume is unreliable, non-trading days are emitted as flat bars, the live
> tick is a quote rather than a print, and live volume is fabricated on two of the five
> datasets. See the `exchange::lse` module documentation before relying on any of it.

See the [workspace README](../README.md) for documentation, examples, and contributing guidelines.
