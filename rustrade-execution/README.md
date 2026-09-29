# rustrade-execution

Execution client library for streaming private account data and executing orders (live or mock).

## Supported Exchanges

| Exchange | Constructor | InstrumentKinds | Features |
|:--------:|:-----------:|:---------------:|:--------:|
| **BinanceSpot** | `BinanceSpot::new(BinanceSpotConfig)` | Spot | Orders, Balances, Positions |
| **BinanceMargin** | `BinanceMargin::new(BinanceMarginConfig)` | Spot (cross/isolated margin) | Orders, Balances, Positions |
| **Alpaca** | `AlpacaClient::new(AlpacaConfig)` | Spot (Equities, Crypto), Option | Orders, Balances, Positions, BracketOrders |
| **Hyperliquid** | `HyperliquidClient::connect(HyperliquidConfig)` | Perpetual | Orders, Balances, Positions |
| **HyperliquidSpot** | `HyperliquidSpotClient::connect(HyperliquidConfig)` | Spot | Orders, Balances, Positions |
| **IBKR** | `IbkrClient::connect_sync(IbkrConfig)` | Spot, Future, Option | Orders, Balances, Positions, BracketOrders |

The `new` constructors are `ExecutionClient::new`. Each connector is behind a Cargo feature, and
none is enabled by default: `alpaca`, `binance` (Spot and Margin), `hyperliquid` (perpetuals and
spot), and `ibkr`. The mock client is always available.

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
| TrailingStop | ✅ | ⚠️ | ⚠️ | ❌ |
| TrailingStopLimit | ✅ | ❌ | ❌ | ❌ |

⚠️ Binance `TrailingStop` supports `BasisPoints` and `Percentage` offsets only;
`Absolute` offsets are rejected as unsupported. ⚠️ Alpaca `TrailingStop` supports
`Percentage` and `Absolute` offsets only; `BasisPoints` is rejected as unsupported.
Hyperliquid requires every order it accepts to carry a client order ID in
`ClientOrderId::uuid()` form; an order with any other ID is rejected. `BinanceMargin`
matches Binance spot except that both `TrailingStop` and `TrailingStopLimit` are
rejected as unsupported (the SDK margin binding omits `trailingDelta`).

See the [workspace README](../README.md) for documentation, examples, and contributing guidelines.
