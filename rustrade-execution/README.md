# rustrade-execution

Execution client library for streaming private account data and executing orders (live or mock).

## Supported Exchanges

| Exchange | Constructor | InstrumentKinds | Features |
|:--------:|:-----------:|:---------------:|:--------:|
| **BinanceSpot** | `BinanceSpot::new(BinanceSpotConfig)` | Spot | Orders, Balances |
| **BinanceMargin** | `BinanceMargin::new(BinanceMarginConfig)` | Spot (cross/isolated margin) | Orders, Balances |
| **Alpaca** | `AlpacaClient::new(AlpacaConfig)` | Spot (Equities, Crypto), Option | Orders, Balances, Positions, BracketOrders |
| **Hyperliquid** | `HyperliquidClient::connect(HyperliquidConfig)` | Perpetual | Orders, Balances, Positions |
| **HyperliquidSpot** | `HyperliquidSpotClient::connect(HyperliquidConfig)` | Spot | Orders, Balances |
| **IBKR** | `IbkrClient::connect_sync(IbkrConfig)` | Spot, Future, Option | Orders, Balances, Positions, BracketOrders |

**Positions** means `account_snapshot` reports each open position in
`InstrumentAccountSnapshot::position`: signed quantity, entry price and unrealised PnL, plus
margin, liquidation price and leverage where the venue has them. On Binance (Spot and Margin) and
Hyperliquid Spot a holding is an asset balance instead.

- **Hyperliquid** reports every field for its perpetuals.
- **Alpaca** reports equity and option holdings as positions, each with its entry price (for an
  option, the premium per share, not per contract) and its unrealised PnL in USD. Crypto holdings
  are balances of the base asset, since Alpaca crypto is spot-only. The USD balance's total is
  cash, which is negative while the account borrows on margin, and its free amount is the cash
  that can be spent without borrowing: the lesser of cash and non-marginable buying power.
  Account equity is cash plus the positions' value, and is not reported separately.
- **IBKR** reports each position's quantity and entry price (its average cost divided by the
  contract multiplier, commissions included). It reports no unrealised PnL. With several
  accounts, the first account holding an instrument is kept; positions are never summed.

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
