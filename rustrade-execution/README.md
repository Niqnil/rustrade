# rustrade-execution

Execution client library for streaming private account data and executing orders (live or mock).

## Supported Exchanges

| Exchange | Constructor | InstrumentKinds | Features |
|:--------:|:-----------:|:---------------:|:--------:|
| **BinanceSpot** | `BinanceSpot::new(BinanceSpotConfig)` | Spot | Orders, Balances, OrderRecovery |
| **BinanceMargin** | `BinanceMargin::new(BinanceMarginConfig)` | Spot (cross/isolated margin) | Orders, Balances, OrderRecovery, Shortability, BorrowCapacity |
| **Alpaca** | `AlpacaClient::new(AlpacaConfig)` | Spot (Equities, Crypto), Option | Orders, Balances, Positions, BracketOrders, OrderRecovery, Shortability |
| **Hyperliquid** | `HyperliquidClient::connect(HyperliquidConfig)` | Perpetual | Orders, Balances, Positions, OrderRecovery |
| **HyperliquidSpot** | `HyperliquidSpotClient::connect(HyperliquidConfig)` | Spot | Orders, Balances, OrderRecovery |
| **IBKR** | `IbkrClient::connect_sync(IbkrConfig)` | Spot, Future, Option | Orders, Balances, Positions, BracketOrders, OrderRecovery, Shortability |

**Positions** means `account_snapshot` reports each open position in
`InstrumentAccountSnapshot::position`: signed quantity, entry price and unrealised PnL, plus
margin, liquidation price and leverage where the venue has them, as `PositionReport::Open`. Each
requested instrument whose position the client can establish is listed, as
`PositionReport::Flat` when it holds none; an instrument left out is unknown, never flat. On Binance (Spot and
Margin) and Hyperliquid Spot a holding is an asset balance instead, and `position` is
`PositionReport::Unreported`.

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

**OrderRecovery** means the client implements `OrderStatusClient`. Its `fetch_ended_orders` reads
how the listed orders ended at the venue; after an account-stream outage, each client uses it to
report the orders that ended while the stream was down. Its `fetch_order_status` says whether one
order is live, has ended, or is unknown to the venue; the `ExecutionManager`, which requires
`OrderStatusClient`, uses it to look up an order whose open had an unknown outcome.

**Shortability** means the client implements `ShortabilityClient::fetch_shortability`, which asks
the venue whether it would lend an instrument for a short sale: whether it is shortable and easy to
borrow, how much the venue has to lend, the borrow fee, and when the venue will stop lending it
(Binance margin reports its delisting schedule), each where the venue says. The answer
is advisory, so a short sale must still handle `ApiError::BorrowRejected`. **BorrowCapacity** means
the client implements `BorrowCapacityClient::fetch_borrow_capacity`, which reports how much more of
the asset an order on a given side would borrow this account can borrow now, and its limit. Each
implementation's rustdoc states what a query costs: IBKR's holds a market data line for up to
`IbkrConfig::shortability_timeout` (10 s by default), and Binance's use request weight.

The `new` constructors are `ExecutionClient::new`. Each connector is behind a Cargo feature, and
none is enabled by default: `alpaca`, `binance` (Spot and Margin), `hyperliquid` (perpetuals and
spot), and `ibkr`. The mock client is always available.

- **Hyperliquid** perpetuals are named `{coin}-{collateral}-PERP` (`BTC-USDC-PERP`,
  `xyz:TSLA-USDC-PERP`). `HyperliquidClient::connect` reads the default DEX, plus each
  builder-deployed (HIP-3) DEX in `HyperliquidConfig::dexes` (`with_dexes`, or `HYPERLIQUID_DEXES`
  for `from_env`), on the configured `network`. Spot pairs are named `{base}-{quote}-SPOT` from
  the pair's tokens, spelled as Hyperliquid spells them (`kPEPE-USDC-SPOT`, not
  `KPEPE-USDC-SPOT`); `HyperliquidSpotClient` refuses an order on a `{base}-{quote}-SPOT` name
  Hyperliquid does not list, a case variant included, unsent, with `ApiError::InstrumentInvalid`.
- **IBKR** `connect_sync` fails if any contract in `IbkrConfig::contracts` cannot be built,
  resolved or registered, listing every one. `connect_sync_lenient` connects without them and
  returns them instead.

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
| TrailingStopFixedLimit | ❌ | ❌ | ⚠️ | ❌ |

A trailing stop's `activation_price` is supported by Binance only (sent as `stopPrice`); the
others reject `Some`. `TrailingStopFixedLimit` is Binance's `STOP_LOSS_LIMIT` with a
`trailingDelta`: its limit stays at the request's `price` while the trigger trails.

⚠️ Binance `TrailingStop` and `TrailingStopFixedLimit` support `BasisPoints` and `Percentage` offsets only;
`Absolute` offsets are rejected as unsupported, and an offset that does not come to a
positive whole number of basis points is refused (`OrderError::InvalidPrecision`), not
rounded. ⚠️ Alpaca `TrailingStop` supports
`Percentage` and `Absolute` offsets only; `BasisPoints` is rejected as unsupported.
Hyperliquid requires every order it accepts to carry a client order ID in
`ClientOrderId::uuid()` form; an order with any other ID is rejected. It also refuses, unsent,
an order whose quantity or price is not positive or has more precision than the market
allows (`OrderError::InvalidPrecision`), and never rounds one: read the rules with
`order_precision` and round first. `BinanceSpot` and `BinanceMargin` refuse, unsent, an order
that breaks the symbol's tick, lot or notional rules from `exchangeInfo`
(`OrderError::InvalidPrecision` or `OrderError::OutOfBounds`), and never round one; set
`pre_send_rules` to `PreSendRules::Off` to let Binance decide. `BinanceMargin` matches Binance
spot, except that a cross margin account refuses a `TrailingStop` (a trailing `STOP_LOSS`) with
`OrderError::UnsupportedOrderType`: place a `TrailingStopFixedLimit` there. Its REST order
queries do not report a trailing delta, so `fetch_open_orders` and `account_snapshot` leave out
its conditional orders (stop, stop-limit, take-profit and take-profit-limit). `account_snapshot`
reports such a listing incomplete; `fetch_open_orders` has no completeness flag, so there only a
warning shows it.
The account stream reports these orders in full.

`RequestOpen::margin_effect` says whether one order borrows (`MarginEffect::Borrow`), repays
(`Repay`) or does neither (`NoSideEffect`); `None` takes the client's default. `BinanceMargin`
honours all three, with `NoSideEffect` as its default, and `BinanceMargin::open_margin_order`
also reports what the order borrowed (`MarginPlacement::borrowed`). Binance spot honours
`NoSideEffect` only.
Alpaca, Hyperliquid and IBKR refuse every `Some`, unsent, with
`OrderError::UnsupportedMarginEffect`, since margin is set per account there.

See the [workspace README](../README.md) for documentation, examples, and contributing guidelines.
