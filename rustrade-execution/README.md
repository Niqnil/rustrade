# rustrade-execution

Execution client library for streaming private account data and executing orders (live or mock).

## Supported Exchanges

| Exchange | Constructor | InstrumentKinds | Features |
|:--------:|:-----------:|:---------------:|:--------:|
| **Binance** | `BinanceClient::connect()` | Spot | Orders, Balances, Positions |
| **BinanceMargin** | `BinanceMargin::new(config)` | Spot (cross/isolated margin) | Orders, Balances, Positions |
| **Alpaca** | `AlpacaClient::connect()` | Spot (Equities, Crypto), Option | Orders, Balances, Positions, BracketOrders |
| **Hyperliquid** | `HyperliquidClient::connect()` | Perpetual | Orders, Balances, Positions |
| **HyperliquidSpot** | `HyperliquidSpotClient::connect()` | Spot | Orders, Balances, Positions |
| **IBKR** | `IbkrClient::connect()` | Spot, Future, Option | Orders, Balances, Positions, BracketOrders |

## Order Types

All connectors support Limit orders. Market orders are supported everywhere except
Hyperliquid (use a Limit order with `ImmediateOrCancel` time-in-force instead).
Additional order types:

| Order Type | IBKR | Alpaca | Binance | Hyperliquid |
|:----------:|:----:|:------:|:-------:|:-----------:|
| Stop | ✅ | ✅ | ✅ | ✅ |
| StopLimit | ✅ | ✅ | ✅ | ✅ |
| TakeProfit | ❌ | ❌ | ✅ | ✅ |
| TakeProfitLimit | ❌ | ❌ | ✅ | ✅ |
| TrailingStop | ✅ | ✅ | ⚠️ | ❌ |
| TrailingStopLimit | ✅ | ❌ | ❌ | ❌ |

⚠️ Binance `TrailingStop` supports `BasisPoints` and `Percentage` offsets only;
`Absolute` offsets are rejected as unsupported. Hyperliquid trigger orders (Stop,
StopLimit, TakeProfit, TakeProfitLimit) require a UUID-format client order ID
(`ClientOrderId::uuid()`). `BinanceMargin` matches Binance spot except that both
`TrailingStop` and `TrailingStopLimit` are rejected as unsupported (the SDK margin
binding omits `trailingDelta`).

See the [workspace README](../README.md) for documentation, examples, and contributing guidelines.
