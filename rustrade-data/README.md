# rustrade-data

WebSocket integration library for streaming public market data from exchanges.

## Supported Exchanges

| Exchange | Constructor | InstrumentKinds | SubscriptionKinds |
|:--------:|:-----------:|:---------------:|:-----------------:|
| **BinanceSpot** | `BinanceSpot::default()` | Spot | PublicTrades, OrderBooksL1, OrderBooksL2 |
| **BinanceFuturesUsd** | `BinanceFuturesUsd::default()` | Perpetual | PublicTrades, OrderBooksL1, OrderBooksL2, Liquidations |
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
| **IBKR** | `IbkrMarketStream::connect()` | Spot, Future, Option | PublicTrades, OrderBooksL1, OrderBooksL2, Candles |

See the [workspace README](../README.md) for documentation, examples, and contributing guidelines.
