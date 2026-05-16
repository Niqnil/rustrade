# rustrade-execution

Execution client library for streaming private account data and executing orders (live or mock).

## Supported Exchanges

| Exchange | Constructor | InstrumentKinds | Features |
|:--------:|:-----------:|:---------------:|:--------:|
| **Binance** | `BinanceClient::connect()` | Spot | Orders, Balances, Positions |
| **Alpaca** | `AlpacaClient::connect()` | Spot (Equities, Crypto), Option | Orders, Balances, Positions, BracketOrders |
| **Hyperliquid** | `HyperliquidClient::connect()` | Perpetual | Orders, Balances, Positions |
| **HyperliquidSpot** | `HyperliquidSpotClient::connect()` | Spot | Orders, Balances, Positions |
| **IBKR** | `IbkrClient::connect()` | Spot, Future, Option | Orders, Balances, Positions, BracketOrders |

## Order Types

All connectors support Market and Limit orders. Additional order types:

| Order Type | IBKR | Alpaca | Binance | Hyperliquid |
|:----------:|:----:|:------:|:-------:|:-----------:|
| Stop | ✅ | ✅ | ❌ | ❌ |
| StopLimit | ✅ | ✅ | ❌ | ❌ |
| TrailingStop | ✅ | ✅ | ❌ | ❌ |
| TrailingStopLimit | ✅ | ❌ | ❌ | ❌ |

See the [workspace README](../README.md) for documentation, examples, and contributing guidelines.
