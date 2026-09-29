# rustrade-execution

Execution client library for streaming private account data and executing orders (live or mock).

## Supported Exchanges

| Exchange | Constructor | InstrumentKinds | Features |
|:--------:|:-----------:|:---------------:|:--------:|
| **BinanceSpot** | `BinanceSpot::new(BinanceSpotConfig)` | Spot | Orders, Balances |
| **BinanceMargin** | `BinanceMargin::new(BinanceMarginConfig)` | Spot (cross/isolated margin) | Orders, Balances |
| **Alpaca** | `AlpacaClient::new(AlpacaConfig)` | Spot (Equities, Crypto), Option | Orders, Balances, BracketOrders |
| **Hyperliquid** | `HyperliquidClient::connect(HyperliquidConfig)` | Perpetual | Orders, Balances, Positions |
| **HyperliquidSpot** | `HyperliquidSpotClient::connect(HyperliquidConfig)` | Spot | Orders, Balances |
| **IBKR** | `IbkrClient::connect_sync(IbkrConfig)` | Spot, Future, Option | Orders, Balances, BracketOrders |

**Positions** means `account_snapshot` reports each open position in
`InstrumentAccountSnapshot::position`: signed quantity, entry price, unrealised PnL, margin,
liquidation price and leverage. Only Hyperliquid perpetuals do. On Binance (Spot and Margin) and
Hyperliquid Spot a holding is an asset balance instead. Two clients report less than the account
holds:

- **Alpaca** reports crypto holdings as balances, and a USD balance whose total is account equity
  and whose free amount is buying power. Equity and option positions are counted in that total but
  not reported one by one.
- **IBKR** lists the instruments that hold a position, without their size or cost.

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
