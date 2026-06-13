# rustrade-data

Integration library for streaming public market data from exchanges and data providers.

## Supported Exchanges

| Exchange | Constructor | InstrumentKinds | SubscriptionKinds |
|:--------:|:-----------:|:---------------:|:-----------------:|
| **BinanceSpot** | `BinanceSpot::default()` | Spot | PublicTrades, Quotes, OrderBooksL1, OrderBooksL2, Candles |
| **BinanceFuturesUsd** | `BinanceFuturesUsd::default()` | Perpetual | PublicTrades, Quotes, OrderBooksL1, OrderBooksL2 |
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

See the [workspace README](../README.md) for documentation, examples, and contributing guidelines.
