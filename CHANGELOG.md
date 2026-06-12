# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **Named config constructors and env loading for Alpaca and Binance Spot**
  (`rustrade-execution`, `alpaca` / `binance` features). Added `AlpacaConfig::from_env()` and
  `BinanceSpotConfig::from_env()` plus typed config errors (`AlpacaConfigError`,
  `BinanceSpotConfigError`) for missing credentials and invalid boolean env values.
- **Caller-selectable `BalanceBasis` for asset statistics** (`rustrade`). Asset drawdown and the
  end-of-session balance row can now be computed from either gross holdings (`Balance::total`, the
  default) or net asset value (`Balance::net_asset()`, i.e. `total - borrowed`). Select it once via
  the new `EngineStateBuilder::balance_basis(BalanceBasis)` builder method (mirrors `oms_mode`); the
  basis flows to every asset's tear-sheet generator and is reported on the `TradingSummary` (its
  asset-table "Balance" row labels itself "Balance (gross)" / "Balance (net asset)"). **Default is
  `Gross`, so existing and cash-only users see no change.** `NetAsset` is only well-defined while net
  asset stays strictly positive — a zero or negative net peak makes the drawdown ratio undefined and
  the sample is silently dropped; see the `BalanceBasis::NetAsset` docs for this precondition and the
  snapshot-freshness caveat.
- **In-band stream-termination signal** (`rustrade-execution`). New
  `AccountEventKind::StreamTerminated(StreamTerminationReason)` variant delivers *why* an account
  event stream ended — `ReconnectBudgetExhausted { attempts, last_error }` (venues with
  library-managed reconnection) or `Error(String)` (unrecoverable, no retry) — on the existing
  account feed, so stream death is a programmatic signal rather than something inferred from channel
  EOF or read from logs. The engine surfaces it via `warn!` instead of dropping it. The
  `#[non_exhaustive]` `StreamTerminationReason` carries only terminations the library can deliver
  in-band (a consumer-initiated drop is excluded — the channel is already closed by the time it is
  observed, so the signal would be undeliverable). This change adds the type plumbing; emitting the
  variant at each venue's terminal stream site is a follow-up.
- **`StreamTerminated` is now emitted at every venue's terminal stream death** (`rustrade-execution`).
  Each integration client emits the variant in-band on the account feed when its event stream truly
  dies: `ReconnectBudgetExhausted { attempts, last_error }` after a venue's library-managed
  reconnection gives up (Binance spot/margin, Alpaca), and `Error(String)` for unrecoverable closes
  with no retry (IBKR, Hyperliquid perp/spot, Mock). A consumer-initiated drop emits nothing — the
  channel is already closed by the time it is observed. All venues funnel through one feature-agnostic
  `emit_stream_terminated` helper, so silent-EOF is now a programmatic signal at every venue. Closes #123.
- **Databento OHLCV candles** (`rustrade-data`). The Databento integration now produces normalised
  `Candle`s from Databento's native OHLCV schemas, both historical and live, alongside its existing
  trades + L1. Historical: `DatabentoHistorical::fetch_candles` / `fetch_candles_stream` take a typed
  `DatabentoOhlcvParams { dataset, symbols, time_range, interval }` (chrono types only — no
  `databento`/`time` types or caller-supplied `Schema`); the DBN schema is derived internally from
  the interval so the interval/schema pair cannot diverge. Live: `DatabentoLive::subscribe_candles`
  streams `DataKind::Candle` events, deriving each bar's interval from its own record `rtype` so one
  connection may carry multiple OHLCV intervals. Bars are stamped at the **open** instant and
  normalised to the shared `close_time = open + interval` contract via `close_time_from_open`.
  Databento's native intervals are `1s`/`1m`/`1h`/`1d`; the other 12 `CandleInterval` variants are
  rejected with `DataError::UnsupportedInterval`. Live is scoped to `1s`/`1m` (the larger bars are
  historical-only, as Databento's live gateway does not reliably stream them); `ohlcv-eod` and the
  deprecated OHLCV rtype are out of scope and skipped observably. `OhlcvMsg` carries no trade count,
  so `Candle::trade_count` is reported as `0` rather than fabricated. Enables Databento's `chrono`
  feature.

### Changed

- **Breaking (`rustrade-execution`, `alpaca` / `binance` features):** `AlpacaConfig::new` and
  `BinanceSpotConfig::new` now take credentials only. Optional live-vs-safety knobs moved to named
  constructors: `AlpacaConfig::paper` / `AlpacaConfig::production` and
  `BinanceSpotConfig::testnet` / `BinanceSpotConfig::production`. The credentials-only constructors
  default to paper trading for Alpaca and testnet for Binance Spot.
- **Breaking (`rustrade`):** the `BalanceBasis` work changes two signatures. `generate_empty_indexed_asset_states`
  gains a `basis: BalanceBasis` parameter (the `EngineStateBuilder` is the intended construction path
  and threads it for you). The `TradingSummary` output struct gains a `basis` field
  (`#[serde(default)]`, so summaries serialised before this change still deserialize as `Gross`);
  the `TearSheetAssetGenerator` likewise gains a `#[serde(default)] basis` field. No behavior change
  under the default `Gross` basis.
- **Dynamic-streams `SubKind` rejection is now exhaustive** (`rustrade-data`, internal). The
  `Channels::try_from` match that allocates per-`SubKind` channels no longer uses a catch-all
  wildcard for unsupported kinds; it lists the rejected kinds explicitly, so a future `SubKind`
  variant is a compile error here rather than a silent runtime fall-through. Unsupported dynamic
  subscriptions now return `DataError::Unsupported { exchange, sub_kind }` (matching the sibling
  stream-init path), so the error names the exchange as well as the kind. No behavior change for
  supported kinds.
- **IBKR historical tick fetches now warn on suspiciously short reads** (`rustrade-data`, `ibkr`
  feature). `fetch_historical_ticks` / `fetch_historical_bid_ask` emit a `warn!` when fewer ticks
  are returned than requested — a best-effort flag for possible silent truncation. A short read can
  also be a legitimate end-of-data, so treat it as a prompt to investigate, not a precise error
  signal.
- **Breaking (`rustrade-execution`):** removed the `AccountEventKind::StreamError(String)` variant.
  It was non-terminal (the stream continued after it), already `error!`-logged at each emit site,
  and dropped unprocessed by the engine — no consumer reacted to it. It is superseded by the
  terminal, structured `StreamTerminated`. Transient venue errors now remain in logs only.
- **IBKR contract config now rejects incomplete/unsupported configs instead of silently
  fabricating a wrong contract** (`rustrade-execution`, `ibkr` feature). `ContractConfig::to_contract`
  previously filled missing fields with silent defaults that produced a *different* contract than
  intended; each is now a hard error (the startup registration loop already warns-and-skips on a bad
  config, so a rejected contract is logged and omitted rather than mis-registered):
  - a missing option `right` on an `OPT` contract no longer defaults to **Call** (`"C"`);
  - a missing `strike` on an `OPT` no longer defaults to `0.0`;
  - a missing `last_trade_date` on a `FUT`/`OPT` no longer defaults to `""`;
  - an unrecognized `security_type` no longer silently falls back to a **stock** (`STK`).
- **Breaking (`rustrade-execution`, `ibkr` feature):** the `contract::InvalidOptionRight` error type
  is replaced by a `#[non_exhaustive]` `contract::ContractConfigError` enum
  (`MissingOptionRight` / `UnrecognizedOptionRight { right }` / `MissingStrike` /
  `MissingLastTradeDate` / `UnrecognizedSecurityType { security_type }`). `option_contract` now
  returns `Result<Contract, ContractConfigError>`.
- **BinanceSpot user-data WS deserialization is now single-pass** (`rustrade-execution`, `binance`
  feature, internal). The per-frame account-stream path no longer builds a full `serde_json::Value`
  DOM and re-parses the matched variant out of it; it reads the `e` discriminator from a borrowed
  view of the frame, then deserializes only the matched event type from the same slice (mirroring
  the BinanceMargin path). No behavior or API change — variant coverage and the harmless
  fall-through for unhandled/unknown event types are preserved.

## [0.3.0] - 2026-06-09

### Added

- **Live Binance klines (candles) over WebSocket** (`rustrade-data`, `SubKind::Candles { interval }`)
  - Spot via `@kline_<interval>` on `BinanceSpot`; USD-M perpetual futures via
    `@continuousKline_<interval>` on a new `BinanceFuturesUsdMarket` exchange-server type routed to
    the `/market` WebSocket tier (the only tier that delivers `@continuousKline_` frames).
  - Closed-candles-only delivery (no repaint/lookahead): in-progress klines (`x == false`) yield no
    event; the exclusive `close_time` boundary is recomputed library-side as `open + interval`
    rather than taken from Binance's `period-end − 1ms` wire `T`.
  - OHLCV parsed JSON-string → `Decimal` (never through an `f64` intermediate), preserving exchange
    precision. New public wire models `BinanceKline`, `BinanceContinuousKline`, `BinanceKlineData`.
  - `Candles` is wired through `DynamicStreams`, so `ExchangeId`-keyed dynamic subscriptions can mix
    candle intervals alongside trades / order books.

- **Binance historical klines (candles) over public REST** (`rustrade-data`,
  `BinanceHistoricalClient`) — free historical OHLCV for research/backtest, no API key.
  - Spot via `/api/v3/klines` (`BinanceHistoricalClient::spot()`) and USD-M perpetual futures via
    `/fapi/v1/continuousKlines` (`BinanceHistoricalClient::futures()`); the continuous-contract
    surface unlocks **`1s`** candles on futures (the symbol surface `/fapi/v1/klines` returns
    `400 Invalid interval` for sub-minute). Both surfaces share one row→`Candle` mapping.
  - Returns a paginated `Stream<Item = Result<Candle, BinanceDataError>>` (+ a `collect`-to-`Vec`
    convenience); `close_time` is recomputed library-side as `open + interval`, and OHLCV is parsed
    JSON-string → `Decimal` (never via `f64`). Server-side gap-filled zero-trade candles (`V = 0`)
    are **delivered, not filtered** (filtering would be consumer policy).
  - New dedicated `BinanceDataError` (`RateLimited { retry_after }` / `Api { status, message }`):
    on `429`/`418` the stream **yields `RateLimited` and ends** — it does not sleep, retry, run a
    global limiter, or emit metrics. The consumer owns retry/backoff and **resumes** by re-calling
    `fetch_candles` with `start` advanced to `last_close_time + 1ms` — the next candle's open. The
    `[start, end]` range is `close_time`-inclusive, so resuming exactly at the last `close_time`
    would re-yield that candle; the `+1ms` step is lossless and duplicate-free (pagination keys off
    `open_time`).
  - A bounded, `tracing`-observable, caller-overridable **proactive inter-page pace** is on by
    default (`BinanceHistoricalClient::with_pace(Duration)`), sized per surface to keep a single
    backfill within Binance's weight budget (spot flat weight 2/req; futures `continuousKlines`
    weight 10/req at the 1500/page max against a lower IP budget). It never inspects a 429 — purely
    good-client courtesy, orthogonal to the surface-and-end rate-limit contract above.

- **Binance Margin execution client** (`BinanceMargin`, `binance` feature) — **cross and isolated**
  - Implements the full `ExecutionClient` trait, so callers do not branch on spot-vs-margin
    transport: order submission/cancel and account snapshot / balance / open-order / trade queries
    over the margin REST API, plus a live account event stream.
  - `BinanceMarginConfig` with `MarginSideEffect` borrow/repay policy (`AutoBorrowRepay` default /
    `NoBorrow`), set once per client (`sideEffectType`). Mode is selected by `is_isolated`, with
    `BinanceMarginConfig::cross_margin(api_key, secret_key)` and
    `BinanceMarginConfig::isolated(api_key, secret_key, symbols)` convenience constructors.
  - Live user-data stream is hand-rolled over the `userListenToken` model (the legacy margin
    listen-key API was retired by Binance on 2026-02-20): token acquisition, renew-before-expiry,
    auto-reconnect, exponential backoff, heartbeat monitoring, fill recovery, and dedup —
    spot-equivalent resilience.
  - Limitations: `TrailingStop`/`TrailingStopLimit` return `UnsupportedOrderType` (the SDK margin
    binding omits `trailingDelta`); Binance margin/SAPI has no testnet (a `testnet: true` config is
    inert and resolves to production, logged at construction).
- **Binance Isolated Margin support** (per-pair sub-accounts; `is_isolated = true` + `isolated_symbols`)
  - `BinanceMarginConfig::isolated_symbols: Vec<InstrumentNameExchange>` declares the per-pair
    universe (the authoritative symbol set for the isolated tokens/stream, fixed for the stream's
    lifetime — pairs added later require a restart). `BinanceMargin::new` **panics** if
    `is_isolated = true` with an empty `isolated_symbols`.
  - Per-pair balances and risk are surfaced **per-instrument** on
    `InstrumentAccountSnapshot.isolated` — a single `Option<IsolatedInstrumentState>` field carrying
    base/quote `AssetBalance` plus `risk` — rather than folded into the asset-keyed `AccountSnapshot.balances`
    (which would collide on shared assets). New public types `IsolatedInstrumentState` and
    `IsolatedMarginRisk` (`margin_level` / `margin_ratio` / `liquidation_price`, snapshot-fresh, no
    live stream twin). Under isolated, `fetch_balances` returns an empty `Vec` (per-pair balances are
    per-instrument, not asset-keyed); snapshot/open-order/trade queries cover one identical effective
    set (`isolated_symbols`, or `instruments ∩ isolated_symbols` with out-of-set instruments skipped
    with a warning).
  - Live per-pair `free`/`locked` arrives over the isolated stream as the new
    `AccountEventKind::InstrumentBalanceUpdate` (base + quote per pair). The engine deliberately does
    **not** store it (mirroring the snapshot's `isolated` field): consumers read it off the raw
    account-event stream, not via `EngineState` / a `StateReplicaManager` replica. The public
    `Balance::apply_stream_update` utility single-sources the no-clobber merge (apply WS `free`/`locked`,
    preserve REST-snapshot debt).
  - Transport: per-symbol `userListenToken`s are **multiplexed onto a single WS-API socket**; all
    tokens are acquired, connected, and subscribed before `account_stream` returns (any failure →
    `Err`, nothing spawned), with planned-reconnect token renewal. The cross stream is a separate,
    untouched manager.
  - Known limitation: all events are stamped `ExchangeId::BinanceMargin`, so a single engine should
    run at most one `BinanceMargin` client (cross + isolated concurrently need separate engines).
- **Margin-aware universal `Balance`**
  - `MarginDetails { borrowed, interest }` and `Balance.margin: Option<MarginDetails>`; the per-asset
    debt model generalises across CEX per-asset-margin venues (cash/no-debt venues leave `margin: None`).
  - `Balance::net_asset()` returns `total` when there is no margin and `total - borrowed` when present
    (a short is negative net asset in the base). Reflects debt only as fresh as the last
    `BalanceSnapshot` for that asset.
  - `Balance::new_margin(total, free, borrowed, interest)` constructor alongside `Balance::new`.
- **REST/WS balance event split** to prevent silently clobbering debt
  - `BalanceUpdate { free, locked }` / `AssetBalanceUpdate` model the WS partial (free/locked only),
    and a new `AccountEventKind::BalanceStreamUpdate(Snapshot<AssetBalanceUpdate>)` carries it.
  - REST snapshots remain the full `BalanceSnapshot(Snapshot<AssetBalance>)` (replace); WS updates
    apply free/locked while **preserving** existing `margin`, so a partial update structurally cannot
    overwrite known debt.
- **Shared `Candle` time-boundary helpers** (`rustrade-data`, `subscription::candle`) — the single
  source of truth every range-computing candle producer routes through (the Massive WS path is the
  exception: it trusts the venue-supplied boundary directly), so the `close_time` contract is computed
  in exactly one place:
  - `IntervalStep { Fixed(chrono::Duration), Months(u32) }` — a primitive step type (`Months` covers
    calendar `month`/`quarter`/`year`).
  - `close_time_from_open(open, step) -> Option<DateTime<Utc>>` — computes a candle's exclusive
    end-of-period boundary (`open + interval`); calendar months use leap-year-correct
    `checked_add_months`. Returns `None` on overflow (callers surface it as their error type, never a
    silent fallback).
  - `open_time_from_close(close, step) -> Option<DateTime<Utc>>` — the inverse (`close − interval`),
    used by range-bounded fetches to widen the venue request window. It round-trips exactly for the
    closes this library produces (monthly boundaries always land on a calendar 1st); it is not a
    universal identity, since `Months` day-clamping is asymmetric for non-1st anchors.
- **`OrderBook` liveness timestamps** (`rustrade-data`): new accessors give a maintained L2
  `OrderBook` a usable liveness signal on every venue (previously `time_engine()` was the only
  timestamp and was `None` for a Binance-spot book's entire life).
  - `OrderBook::time_exchange() -> Option<DateTime<Utc>>` — the venue's latest event/broadcast time
    (`"E"` on Binance, `ts` on Bybit). Feed-lag-aware staleness where present (`now - time_exchange`
    catches data that is old despite being just received). `None` when the venue supplies no
    broadcast timestamp (IBKR; Binance spot REST seed before the first diff) — a capability signal,
    not a defect. Note the asymmetry with `MarketEvent::time_exchange` (non-`Option`, with a local
    fallback): on `OrderBook`, `None` means "the venue gave nothing".
  - `OrderBook::time_received() -> DateTime<Utc>` — the local ingestion wall-clock, **always
    present** once a revision is applied, on **every** venue (including IBKR, where it is the only
    liveness signal). The universal liveness floor; skew-immune (`now - time_received` is a
    same-clock comparison). Prefer it as the fallback when `time_exchange()` is `None`. A
    default/pre-population book reports the epoch (1970), so it reads as stale until the first
    revision — the intended fail-closed behaviour.
  - `OrderBook::times() -> OrderBookTimes` — convenience accessor returning all three revision
    timestamps as a single `Copy` value, for forwarding the whole set in one move.

### Changed

- **Binance USD-M futures WebSocket tier routing** (`rustrade-data`). Binance split the futures
  WebSocket into mutually-exclusive routed tiers; subscribing on the wrong tier silently connects
  (`101`) then delivers zero frames. To make the tier a compile-time property:
  - Existing futures streams (trades, L1/L2 order books) migrated from `/ws` to `/public/ws`.
  - `Liquidations` (`@forceOrder`) and the new `Candles` (`@continuousKline_`) `StreamSelector`
    implementations now live on the new `/market`-tier `BinanceFuturesUsdMarket` server type, **not**
    on `BinanceFuturesUsd`. This is a breaking change for the typed `Streams` path: callers
    subscribing to futures liquidations via `BinanceFuturesUsd` must switch to
    `BinanceFuturesUsdMarket`. The `DynamicStreams` / `ExchangeId` path is unaffected. Spot is
    unaffected.
  - The blanket `StreamSelector<_, PublicTrades>` / `StreamSelector<_, OrderBooksL1>` impls on
    `Binance<Server>` are now **explicit per-server** impls (`BinanceSpot` + `BinanceFuturesUsd`
    only — never `BinanceFuturesUsdMarket`), so a `/market`-tier trade / L1 subscription is a
    compile error instead of a silent dead stream, mirroring the already-per-server `OrderBooksL2`.
    Breaking for any downstream user with their own `Binance<CustomServer>`: code that previously
    compiled by resolving `PublicTrades` / `OrderBooksL1` through the blanket impl now fails to
    compile. Migration is mechanical — add an explicit `impl StreamSelector<_, PublicTrades> for
    Binance<CustomServer>` (and likewise `OrderBooksL1`) for each kind that server actually
    supports.
- **Bumped `ibapi` from `2.12.0` to `3.0.1`** (`ibkr` feature). ibapi 3.0 is a major release with
  breaking API changes; the IBKR market-data (`rustrade-data`) and execution (`rustrade-execution`)
  connectors were migrated to the new surface. Notable upstream changes absorbed: `Subscription<T>`
  iteration now yields `Result<SubscriptionItem<T>, Error>` (most data loops use `iter_data()`,
  surfacing subscription errors instead of silently ending — the exception is `TickSubscription`,
  which yields `T` directly and has no error accessor; see the `fetch_historical_ticks` /
  `fetch_historical_bid_ask` doc comments for that silent-truncation caveat); builder-style
  market-data requests
  (`historical_data`/`historical_ticks`/`market_depth`/`tick_by_tick`); `Contract.right` is now
  `Option<OptionRight>`; `OrderStatus.status` is now the `OrderStatusKind` enum; and `Execution.side`
  is now the `ExecutionSide` enum. Two small `ibkr`-feature public API changes accompany this
  migration (see the BREAKING sub-entries below). (Downstream code that constructs the re-exported
  `ibapi::contracts::Contract` via struct literals directly must also update `right` from `String`
  to `Option<OptionRight>`; callers using the `rustrade-execution` contract builders are unaffected.)
  - **Operational requirement:** ibapi 3.x speaks only the protobuf transport and refuses to
    connect to a TWS/IB Gateway older than **server version 213** (it errors with
    *"server version 213 required … please upgrade"*). Operators of the `ibkr` connector must
    run a recent ("latest"-channel) TWS/Gateway build; older Gateways that worked with ibapi 2.x
    will no longer connect.
  - **Order placement no longer misreports IB informational order messages as rejections.**
    Under ibapi 3.x, any TWS message outside the warning range (`2100..=2169`) — including IB's
    informational "Order Message" code 399 (e.g. *"your order will not be placed at the exchange
    until 09:30 US/Eastern"* for an order accepted and **held** until regular trading hours) — is
    delivered as a stream-terminating `Err` on the placement subscription. The order-placement
    paths now classify these: known informational codes are reported as live-but-pending (the
    order's authoritative status is resolved via the order-update/account stream) rather than as a
    hard rejection, while genuine rejections and transport errors still fail observably. Placement
    loops also gained a bounded wait so a silent Gateway cannot hang them indefinitely.
  - **Immediately-filled orders are no longer misreported as rejections.** A marketable order can
    fill before any working status is delivered, in which case ibapi 3.x sends `OrderStatus(Filled)`
    directly on the placement subscription. Placement now classifies `Filled` as accepted (the order
    is live; its authoritative fill is resolved via the order-update/account stream) and retains the
    order-id mapping, rather than returning a hard rejection and dropping the order's later
    execution/commission events.
  - **BREAKING (`ibkr`): `client::ibkr::contract::option_contract` now returns
    `Result<Contract, InvalidOptionRight>`** instead of `Contract`. An unrecognized or empty option
    `right` is now an observable error at construction (new public error type
    `client::ibkr::contract::InvalidOptionRight`) rather than a silently right-less `Contract` that
    IBKR only rejects later at submission. Migration: handle the `Result` (e.g. `?` or `match`) at
    call sites; the other builders (`stock_contract`/`futures_contract`/`forex_contract`) are
    unchanged.
  - **BREAKING (`ibkr`): removed `client::ibkr::execution::parse_ib_side`.** `Execution.side` is now
    the typed `ExecutionSide` enum upstream, so the string parser is obsolete — map the enum directly
    (`ExecutionSide::Bought` → `Side::Buy`, `ExecutionSide::Sold` → `Side::Sell`).
- **BREAKING: `Balance` gained a public `margin: Option<MarginDetails>` field.** Direct struct-literal
  construction (`Balance { total, free }`) no longer compiles. Migration: use `Balance::new(total, free)`
  for cash balances or `Balance::new_margin(..)` for margin balances. `const` sites that cannot use
  `..Default::default()` need an explicit `margin: None`.
- **Binance spot WS balance events now emit `BalanceStreamUpdate` instead of `BalanceSnapshot`.**
  Spot's `outboundAccountPosition` was always a free/locked partial; it now uses the same
  REST→snapshot / WS→update model as margin. Engine balance state is updated via
  `AssetState::apply_balance_update` (sets `free`, recomputes `total = free + locked`, preserves
  `margin`). No behavioural change for spot (which carries no debt) beyond the event variant.
- **Binance `GoodUntilEndOfDay` (GTD) time-in-force is now rejected as `UnsupportedOrderType`** instead of being silently coerced to `GoodTillCancelled` (GTC). Binance has no native end-of-day order, and coercing to GTC dropped the EOD auto-cancel semantics — risking an unintended resting order. This affects both the spot and margin clients.
- **Binance margin user-data frames are parsed without a full JSON DOM.** The WS receive path now deserializes a borrowed envelope (`serde_json::value::RawValue` for the inner `event`) and reads the event discriminator from a raw slice, so only the matched event type pays for a single typed pass — no intermediate `serde_json::Value` tree is built per frame on this hot path. Internal only; no public API change (the `binance` feature now enables `serde_json/raw_value`).
- **`InstrumentAccountSnapshot` gained a public `isolated: Option<IsolatedInstrumentState>` field**, and **`AccountEventKind` gained an `InstrumentBalanceUpdate` variant** (both for isolated margin). Both are additive on the wire (`Option` + `#[serde(default)]` / `#[non_exhaustive]` enum), but `InstrumentAccountSnapshot::new()`'s arity went 3→4 (struct-literal / `::new()` call sites must pass the new field) and the library's `indexer.rs` gained one match arm — a minor breaking change for code that directly constructs `InstrumentAccountSnapshot`. The new field sorts/hashes last (`None` before `Some`), so it acts only as a tie-breaker; the cross stream/snapshot paths are unchanged.
- **Documented the `Candle.close_time` contract** (`rustrade-data`): `close_time` is the **exclusive
  end-of-period boundary** (`close_time == open_time + interval`); a candle aggregates the half-open
  window `[close_time − interval, close_time)`, so `close_time` equals the next candle's open instant.
  The boundary is the UTC period grid, **not** the exchange session close (the library has no session
  calendar); `month`/`quarter`/`year` use nominal calendar arithmetic. `Candle` carries neither
  `open_time` nor `interval` — recover them from the originating fetch/subscription.
- **Documented the `MarketEvent.time_exchange` contract** (`rustrade-data`): `time_exchange` is the
  event's position on the consuming engine's timeline (the historical/backtest clock derives "current
  time" and replays events in `time_exchange` order). For point-in-time payloads it is the venue event
  time; for **aggregated/windowed payloads (candles/OHLCV) it must be the period END (`close_time`)**,
  never the period start — stamping the open makes a completed bar enter the timeline before it could
  exist (silent lookahead). Applies to any windowed payload, including a custom event type fed to the
  engine without this crate's producers. Cross-referenced from the engine `EngineClock`/`TimeExchange`
  traits and the `Candle.close_time` docs. Documentation only — no behaviour change. A new
  `engine_backtest_with_candle_market_data` example demonstrates wrapping candles into `MarketEvent`s
  (stamping `time_exchange = close_time`) and the custom `InstrumentDataState` needed to consume them
  (the default instrument state tracks only trades + L1).
- **BREAKING (`ibkr`): IBKR candle `close_time` is now the end-of-period boundary, not the bar start.**
  `bar_to_candle` previously stuffed the bar's own start timestamp into `close_time` (off by one full
  interval); it now computes `close_time = bar_open + interval` via the shared helper. **Call out:** an
  IBKR **daily** bar's `close_time` is now the **next** day's `00:00 UTC` (e.g. a Jan 15 daily bar →
  `Jan 16 00:00 UTC`), so `close_time.date()` shifts forward by one day — any `group_by(close_time.date())`
  must subtract one interval (the bar's own date `= close_time − interval`). Monthly bars use calendar
  arithmetic (`Jan → Feb 1 00:00 UTC`).
- **BREAKING: standardized the historical-fetch range contract on `close_time`.** `fetch_candles`
  (Hyperliquid) and `fetch_aggregates` (Massive) now return exactly the candles whose `close_time`
  falls within the requested `[start, end]` (inclusive) — matched on `close_time`, the field consumers
  receive — by widening the venue request one interval and trimming the result. Previously both matched
  the venue-native **open-time** (Hyperliquid by open-time bucket, Massive/Polygon by the bar's
  open-time), so the candle set near the range boundaries changes. IBKR is unaffected: its venue API is
  duration-based (`end_date` + `duration`), documented as the exception (its candles still carry the
  corrected `close_time`).
- **BREAKING (`massive`): `AggregateBar` candle conversion is now fallible and keyed on `IntervalStep`.**
  `into_candle_with_duration(Duration) -> Candle` was renamed to `into_candle_with_step(IntervalStep) ->
  Result<Candle, MassiveError>`, and `into_candle(multiplier, timespan)` likewise now returns
  `Result<Candle, MassiveError>` (a computed `close_time` overflow is surfaced rather than silently
  wrapped). Migration: pass an `IntervalStep` (via `timespan_to_step`) instead of a `Duration`, and
  handle the `Result`. The free function `timespan_to_duration` was correspondingly replaced by
  `timespan_to_step`.
- **BREAKING (`rustrade-data`): the `Candles` subscription kind gained a mandatory
  `interval: CandleInterval` field and no longer implements `Default`.** The unit struct `Candles`
  is now `Candles { pub interval: CandleInterval }`; the interval is intrinsic to a candle
  subscription, so a phantom `Default` (silently `1m`) was removed as a footgun. A new shared
  `CandleInterval` enum (`subscription::candle`) is the venue-agnostic union of candle resolutions
  (`as_str`/`Display`/`FromStr`/`Serialize`/`Deserialize` all single-sourced; strings match
  Binance's kline `interval`). Migration: replace `Candles` / `Candles::default()` with
  `Candles { interval: CandleInterval::Min1 }` (or the desired resolution). Note: the serialized
  representation also changes (e.g. JSON `null`/`"candles"` → `{"interval":"1h"}`), so persisted or
  transmitted `Candles` values from older versions are not deserialization-compatible and must be
  re-serialized.
- **BREAKING (`rustrade-data`): the `SubKind::Candles` enum variant gained a mandatory
  `interval: CandleInterval` field.** Mirroring the marker `Candles` kind above, the dynamic-subscription
  `SubKind` enum's unit variant `Candles` is now `Candles { interval: CandleInterval }`, so exhaustive
  matches on `SubKind` must bind the field. The serde form also changes: `SubKind` is an
  externally-tagged enum, so the representation goes from `"Candles"` to `{"Candles":{"interval":"1m"}}`
  (the `derive_more::Display` tag stays the fixed `"candles"`, interval-independent).
  Migration: replace `SubKind::Candles` with
  `SubKind::Candles { interval: CandleInterval::Min1 }` (or the desired resolution). The
  `DynamicStreams` stream builder now collects per-exchange candle streams symmetrically with the other
  data kinds (new public field `candles` and accessors `select_candles` / `select_all_candles`, and a new
  `MarketStreamResult<_, Candle>: Into<Output>` bound on `select_all`). Binance spot and USD-M perpetual
  futures candles are wired through the dynamic path (`exchange_supports_instrument_kind_sub_kind` accepts
  them), so `select_candles` / `select_all_candles` yield live candle streams; venues without a candle
  producer remain rejected.
- **BREAKING (`rustrade-data`): `OrderBook` now stores a nested `OrderBookTimes` instead of a bare
  `time_engine`.** The new public `OrderBookTimes` struct groups the three revision timestamps
  (`time_engine` + `time_exchange` + `time_received`) and serves double duty as both the constructor
  argument and the stored field (its named fields prevent transposing the two same-typed `Option`
  times).
  - `OrderBook::new` and `OrderBook::from_sides` now take an `OrderBookTimes` in place of the former
    `time_engine: Option<DateTime<Utc>>` argument. Callers constructing `OrderBook`s directly must
    migrate (e.g. `OrderBookTimes { time_engine, time_exchange, time_received }`, or
    `OrderBookTimes::default()`).
  - The serialized shape changes: the timestamps are now nested under a `times` object rather than a
    flat `time_engine` field. (Cross-version reads of serialized `OrderBook`s are out of scope, so
    there is no wire back-compat path.)
  - `time_engine()`'s signature and "matching-engine time" contract are unchanged, **but its value
    on Bybit and Hyperliquid changes from `Some(broadcast_time)` to `None`.** Those venues only
    broadcast an event time, which previously leaked into `time_engine()` (conflating broadcast with
    matching-engine time); it now lives solely in the new `time_exchange()`. Read `time_exchange()`
    for that value instead.
  - `OrderBook` equality (`PartialEq`/`Eq`) is still derived over all fields, so it now also reflects
    `time_exchange`/`time_received`. Two content-identical books observed at different instants
    compare **unequal** — compare via the accessors (`sequence()`/`bids()`/`asks()`) for content
    equality.
  - `DepthAggregator::update` (IBKR, `ibkr` feature) now takes a second argument
    `time_received: DateTime<Utc>`, the local ingestion wall-clock stamped into the produced
    `OrderBook`'s `time_received`. Callers must pass the same timestamp used for the wrapping
    `MarketEvent`.

### Fixed

- **Binance USD-M futures liquidation stream (`@forceOrder`) delivers again** (`rustrade-data`).
  Binance routed `@forceOrder` to its `/market` WebSocket tier and decommissioned `/market`
  delivery on the unrouted legacy `/ws` on 2026-04-23, leaving the existing futures `Liquidations`
  stream (which connected to `/ws`) **silently dead in production** — a `101` handshake followed by
  zero frames, no error. It now connects via the new `BinanceFuturesUsdMarket` server type on
  `fstream.binance.com/market/ws`. No auth/listenKey is required (per-symbol `<sym>@forceOrder` was
  confirmed live on a public `/market` socket).

- **`BybitPerpetualsUsd` L1/L2 order books in `DynamicStreams` now use the perpetuals connector.**
  The `(BybitPerpetualsUsd, OrderBooksL1)` and `(BybitPerpetualsUsd, OrderBooksL2)` arms of the
  dynamic stream builder constructed their `Subscription` with `BybitSpot::default()`, so a caller
  subscribing to perpetuals order books was wired to the Bybit **spot** WebSocket endpoint and
  payload format. Both arms now use `BybitPerpetualsUsd::default()`, matching the perpetuals
  `PublicTrades` arm.
- **Binance `fetch_open_orders` now honours the `ExecutionClient` "return all" contract** for an empty `instruments` slice. Both the spot and margin clients previously iterated the (empty) slice and returned an empty `Vec`, silently violating the trait contract that an empty slice must return open orders across all instruments. They now issue a single no-symbol query (`GET /api/v3/openOrders`, `GET /sapi/v1/margin/openOrders`), recovering each order's instrument from its own `symbol` field. The `fetch_trades` per-symbol limitation (Binance `myTrades` requires a symbol, so an empty slice returns empty) is now an explicitly documented deviation on both clients.
- Corrected the order-type support matrix in `rustrade-execution/README.md` to reflect Binance and Hyperliquid conditional order support (Stop, StopLimit, TakeProfit, TakeProfitLimit), Binance trailing-stop offset limitations, and Hyperliquid's lack of native market orders.
- **`rustrade-execution` docs.rs builds now use `all-features`.** Every connector module is feature-gated behind `default = []`, so docs.rs previously published a crate documenting no connectors and the connector-comparison intra-doc links broke. The full client surface is now documented and those links resolve.
- **Resolved broken intra-doc links in `rustrade-data`** surfaced under `--all-features` (`OptionGreeks`, `Stream`, `AlpacaCredentials`/`AlpacaIex`/`AlpacaSip`/`AlpacaCrypto`, `DatabentoHistorical`/`DatabentoLive`, `MassiveRestClient`/`MassiveLive`): module/header docs referenced these types by short name where they were not in scope. They now use explicit paths, so the published docs link correctly.
- **Binance REST auth-failure errors now carry the numeric Binance code.** `401`/`403` (`UnauthorizedError`/`ForbiddenError`) rejections splice the code into the `ApiError::Unauthenticated` message, so callers can distinguish auth subtypes (e.g. `-2014` invalid key vs `-2015` IP/permission), matching the existing behaviour for client-error rejections.
- **BREAKING: Massive monthly/quarterly/yearly candle `close_time` now uses calendar arithmetic**
  (`rustrade-data`). `month`/`quarter`/`year` aggregates previously approximated the boundary as a
  fixed `+30/91/365 days`, so a January monthly bar's `close_time` was `Jan 31`, not `Feb 1` — it did
  not equal the next candle's open and did not align with Binance `1M` / IBKR monthly boundaries. They
  now use leap-year-correct `Months` arithmetic (a January monthly bar → `Feb 1 00:00 UTC`). Fixed
  intervals (`second`…`week`) are unchanged. Breaking for consumers comparing Massive coarse-interval
  timestamps.
- **BREAKING: Hyperliquid candle `close_time` is now computed library-side as `time_open + interval`**
  (`rustrade-data`), instead of the venue's raw `time_close`. Hyperliquid reports `time_close` as
  `period-end − 1ms` (the inclusive-last-ms convention, verified against the live API), which does not
  satisfy the `close_time == open + interval` contract; the boundary is now computed via the shared
  helper so Hyperliquid aligns with the other producers. Breaking by `+1ms` for consumers comparing
  Hyperliquid candle timestamps against the raw venue value.

## [0.2.1] - 2026-05-28

### Added

- **Binance conditional order support** ([#93](https://github.com/Niqnil/rustrade/issues/93))
  - `Stop` → Binance `STOP_LOSS` (market order triggered at stop price)
  - `StopLimit` → Binance `STOP_LOSS_LIMIT` (limit order triggered at stop price)
  - `TakeProfit` → Binance `TAKE_PROFIT` (market order triggered at take-profit price)
  - `TakeProfitLimit` → Binance `TAKE_PROFIT_LIMIT` (limit order triggered at take-profit price)
  - `TrailingStop` with `BasisPoints` or `Percentage` offset → Binance `STOP_LOSS` with `trailingDelta`
    - `BasisPoints`: value passed directly as `trailingDelta` (1 bp = 0.01%)
    - `Percentage`: value multiplied by 100 before sending (e.g., 2% → 200 trailingDelta)
  - Note: `TrailingStop` with `Absolute` offset returns `UnsupportedOrderType` (manual conversion required: `(absolute / price) * 10000`)
  - Note: `TrailingStopLimit` returns `UnsupportedOrderType` (Binance doesn't support)

- **Hyperliquid conditional order support** ([#94](https://github.com/Niqnil/rustrade/issues/94))
  - `Stop` → Hyperliquid trigger order (`tpsl: "sl"`, `is_market: true`)
  - `StopLimit` → Hyperliquid trigger order (`tpsl: "sl"`, `is_market: false`)
  - `TakeProfit` → Hyperliquid trigger order (`tpsl: "tp"`, `is_market: true`)
  - `TakeProfitLimit` → Hyperliquid trigger order (`tpsl: "tp"`, `is_market: false`)
  - Trigger orders require UUID-format client order ID (`ClientOrderId::uuid()`) for cancellation support
  - Cancellation via `cancel_by_cloid()` for trigger orders (uses UUID), `cancel()` for regular orders (uses OID)
  - Note: `TrailingStop`, `TrailingStopLimit`, and `Market` return `UnsupportedOrderType`
  - Note: SDK limitation — `fetch_open_orders` and `account_stream` cannot distinguish trigger orders from limit orders (SDK structs lack trigger fields). Track `OrderKind` from placement response.

## [0.2.0]

### Added

- **Databento streaming variants** ([#46](https://github.com/Niqnil/rustrade/issues/46))
  - `DatabentoHistorical::fetch_trades_stream()`: Stream trades without collecting into memory
  - `DatabentoHistorical::fetch_quotes_stream()`: Stream quotes without collecting into memory
  - Avoids memory spikes for large historical queries (millions of records)

### Changed

- **BREAKING: Migrate from `async_trait` to native AFIT** ([#85](https://github.com/Niqnil/rustrade/issues/85))
  - `Subscriber`, `SubscriptionValidator`, `ExchangeTransformer`, and `MarketStream` traits now use native async fn in trait (Rust 1.75+)
  - Removed `async-trait` crate dependency
  - Additional `Sync` bounds added to some generic parameters where required
  - Return type changed from `Pin<Box<dyn Future + Send>>` to opaque `impl Future + Send`
  - No code changes required for most downstream users unless explicitly naming future types

- **Databento structured error types** ([#47](https://github.com/Niqnil/rustrade/issues/47))
  - New `DatabentoErrorKind` enum: `Authentication`, `RateLimit`, `Network`, `Decode`, `Api`
  - New `DataError::Databento { kind, context, message }` variant for programmatic error handling
  - Enables proper retry logic: don't retry auth errors, backoff on rate limits, retry network errors
  - All Databento errors now use structured types instead of `DataError::Socket(String)`

- **Databento `Arc<K>` performance documentation** ([#45](https://github.com/Niqnil/rustrade/issues/45))
  - Documented that instrument keys are cloned per record
  - Recommended `Arc<K>` for high-frequency scenarios to avoid per-record heap allocations
  - Added examples in rustdoc for `fetch_trades`, `fetch_quotes`, and `DatabentoLive`

- **BREAKING: Stateful `Subscriber` trait for credential injection** ([#43](https://github.com/Niqnil/rustrade/issues/43))
  - `Subscriber::subscribe` now takes `&self` instead of being a static method
  - `Subscriber` trait requires `Clone + Send + Sync` bounds
  - `StreamBuilder::subscribe()` now requires a subscriber instance as first argument:
    - Unauthenticated: `.subscribe(WebSocketSubscriber, [...])`
    - Authenticated (Alpaca): `.subscribe(AlpacaSubscriber::from_env()?, [...])`
  - `init_market_stream()` now takes subscriber as second argument
  - `AlpacaSubscriber` is now stateful with `AlpacaCredentials`:
    - `AlpacaSubscriber::new(credentials)`: Create with explicit credentials
    - `AlpacaSubscriber::from_env()`: Load from `ALPACA_API_KEY`/`ALPACA_SECRET_KEY`
    - `AlpacaCredentials::new(key, secret)`: Create credentials explicitly
    - `AlpacaCredentials::from_env()`: Load from environment
  - Auth errors now fail at construction time (fast fail) instead of first reconnect
  - Credentials are cloned into reconnect closure, available on every reconnect

### Added

- **BracketOrderClient supertrait**: Unified trait for bracket orders
  - `BracketOrderClient` trait extending `ExecutionClient` for exchanges supporting native bracket orders
  - `RequestOpenBracket` struct: Common request parameters (side, quantity, prices, TIF)
  - `BracketOrderRequest<ExchangeKey, InstrumentKey>` type alias using `OrderEvent`
  - `BracketOrderResult` with `Option<Order>` for child legs (documents API divergence)
  - `BracketOrderRequestBuilder` for fluent request construction
  - Implemented for `IbkrClient` (returns all 3 legs) and `AlpacaClient` (returns parent only)
  - Enables generic code: `T: ExecutionClient + BracketOrderClient`
- **Option Greeks support**: Real-time and computed Greeks for IBKR options
  - `DataKind::OptionGreeks(OptionGreeks)` variant for the unified market data stream
  - `IbkrSubscriptionKind::OptionGreeks` for live streaming via `market_data()` subscription
  - `OptionGreeks` struct (`subscription::greeks`): `delta`, `gamma`, `theta`, `vega`, `implied_volatility`,
    `theoretical_price`, `underlying_price` (all `Option<f64>`); marked `#[non_exhaustive]`
  - `OptionGreeks::has_any_greek()` returns true when at least one first-order Greek is present
    (excludes `theoretical_price` / `underlying_price`)
  - `IbkrHistoricalData::calculate_theoretical_greeks(contract, volatility, underlying_price)`:
    IB-side Greeks calculator from user-supplied IV and underlying
  - `IbkrHistoricalData::calculate_implied_volatility(contract, option_price, underlying_price)`:
    IB-side IV calculator from user-supplied option/underlying prices
  - `IbkrHistoricalData::fetch_option_chain(symbol, exchange, security_type, contract_id)` returning
    `Vec<OptionChainEntry>` with available expirations, strikes, trading classes, and exchanges
  - `OptionChainEntry` struct (`exchange::ibkr::options`): marked `#[non_exhaustive]`; `strikes` is
    `Vec<rust_decimal::Decimal>` (financial values must use `Decimal` per project standard)
  - `IbkrMarketStream` rejects non-`SecurityType::Option` contracts on `OptionGreeks` subscription
    with `DataError::Socket` (fail-fast over silent zero events)
- **Historical tick data APIs** for IBKR: `fetch_historical_ticks`, `fetch_historical_bid_ask`
- Cargo `required-features` declarations for feature-gated examples
  (`download_databento_fixtures`, `hyperliquid_*`, `ibkr_*`); `cargo check --all-targets`
  no longer fails on default features
- **Stop and Trailing Stop order types**:
  - `OrderKind::Stop { trigger_price }`: Stop market orders
  - `OrderKind::StopLimit { trigger_price }`: Stop-limit orders
  - `OrderKind::TrailingStop { offset, offset_type }`: Trailing stop orders
  - `OrderKind::TrailingStopLimit { offset, offset_type, limit_offset }`: Trailing stop-limit orders
  - `TrailingOffsetType` enum: `Absolute`, `Percentage`, `BasisPoints`
  - IBKR connector: Full support for all stop/trailing order types
  - Binance/Alpaca connectors: Return `UnsupportedOrderType` error (support planned)
- `OrderError::UnsupportedOrderType`: New error variant for connectors that don't support certain order types
- **Massive market data connector**: Historical, live, and reference data via `massive` feature
  - `MassiveRestClient`: Historical aggregates, trades, quotes with streaming pagination
  - `MassiveLive`: Real-time WebSocket streaming for trades, quotes, and aggregates
  - Reference data: `fetch_tickers()`, `fetch_ticker_details()`, `fetch_exchanges()`, `fetch_market_status()`, `fetch_market_holidays()`
  - Corporate actions: `fetch_dividends()`, `fetch_splits()` for stocks/ETFs
  - `TickerQuery` builder for filtering ticker searches
  - `ExchangeId::Massive` variant
  - Supports all asset classes: stocks, crypto, forex, options, indices, futures
- **Databento market data connector**: Historical and live data via `databento` feature
  - `DatabentoHistorical`: One-shot queries for trades and quotes in DBN format
  - `DatabentoLive<K>`: Real-time WebSocket streaming with `PitSymbolMap` symbol resolution
  - `ExchangeId` variants: `DatabentoGlbx`, `DatabentoXnas`, `DatabentoXnys`, `DatabentoDbeq`, `DatabentoOpra`
  - Nanosecond-precision timestamps and lossless Decimal price conversion
  - **Testing**: NOT TESTED in CI; offline fixture tests verified locally; live integration untested (requires paid subscription)
- **Alpaca market data connector**: Real-time trades and quotes via WebSocket
  - `AlpacaIex`: Free IEX feed for US equities
  - `AlpacaSip`: Paid consolidated SIP feed for US equities
  - `AlpacaCrypto`: Crypto market data
  - **Testing**: IEX and crypto feeds are tested with paper credentials; SIP requires Algo Trader Plus (paid subscription) and is NOT TESTED
- **Alpaca options market data**: REST-based option discovery and Greeks snapshots
  - `AlpacaOptionsClient`: Options market data client with rate limiting and pagination
  - `AlpacaOptionContractQuery`: Builder for filtering contracts by underlying, expiration, strike, type, style
  - `fetch_contracts(query)`: Discover option contracts via `GET /v2/options/contracts`
  - `AlpacaOptionSnapshot`: Option snapshot with quote and Greeks data
  - `fetch_snapshots(symbols, feed)`: Fetch snapshots with Greeks via `GET /v1beta1/options/snapshots`
  - `fetch_chain_snapshots(underlying, feed)`: Convenience method for entire option chains
  - `AlpacaOptionFeed`: `Opra` (real-time, paid) or `Indicative` (15-min delayed, free)
  - **Testing**: Indicative feed is tested; OPRA requires Algo Trader Plus (paid subscription) and is NOT TESTED
  - **Note**: Greeks streaming is NOT available — Alpaca only provides REST snapshots for Greeks data
- **Quotes subscription kind**: Generic top-of-book quotes (`SubKind::Quotes`)
- `ExchangeId::AlpacaBroker`: Dedicated variant for Alpaca execution client
  (distinct from market data feed identifiers)

### Changed

- **deps(ibkr)**: Bump `ibapi` from 2.11.4 to 2.12.0 — fixes TWS error surfacing on
  subscription channels ([rust-ibapi#567](https://github.com/wboayue/rust-ibapi/pull/567),
  closes [#78](https://github.com/Niqnil/rustrade/issues/78))
- **perf(alpaca)**: Pre-allocate `/v2/orders` endpoint URL at `AlpacaClient` construction,
  eliminating 2 heap allocations per order placement (`open_order_inner`, `open_bracket_order`).
- **BREAKING**: `PublicTrade::side` changed from `Side` to `Option<Side>`.
  - Crypto connectors (Binance, Hyperliquid, Alpaca Crypto, etc.): `Some(side)`
  - Equities connectors (Alpaca IEX/SIP, IBKR): `None` — taker side not available
  - Databento: `Some(side)` for 'A'/'B', `None` for 'N' (no side specified)
  - Migration: Match on `Some(side)` to handle the `None` case explicitly, or use
    `.is_some_and(|s| s == Side::Buy)` for boolean checks. (`Side` does not implement
    `Default`, so `unwrap_or_default()` will not compile.)
- **BREAKING**: `OptionChainEntry::expirations` changed from `Vec<String>` to `Vec<NaiveDate>`.
  - Removes IBKR wire format leakage (YYYYMMDD strings) from caller code
  - Invalid expiration strings are now filtered during `from_ib()` conversion
  - Migration: Replace string parsing with direct `NaiveDate` usage
- **BREAKING**: `PublicTrade`, `Quote`, `Candle`, and `Liquidation` price/amount fields
  changed from `f64` to `rust_decimal::Decimal` for financial precision.
  - `PublicTrade`: `price`, `amount` now `Decimal`
  - `Quote`: `bid_price`, `ask_price`, `bid_amount`, `ask_amount` now `Decimal`
  - `Candle`: `open`, `high`, `low`, `close`, `volume` now `Decimal`
  - `Liquidation`: `price`, `quantity` now `Decimal`
  - Migration: Use `dec!()` macro for literals, `> Decimal::ZERO` for positivity checks.
    For string-typed JSON fields, use `de_str` deserializer or `.parse::<Decimal>()`.
    Use `Decimal::try_from(f64)` only when the source is already `f64` (e.g., IBKR API).
- **BREAKING**: `RequestOpen.price` and `Order.price` changed from `Decimal` to `Option<Decimal>`.
  - Market, Stop, and TrailingStop orders: `price: None` (no limit price)
  - Limit, StopLimit, and TrailingStopLimit orders: `price: Some(limit_price)`
  - Removes the `dec!(0)` sentinel convention: Market/Stop orders now carry an explicit `None`
    rather than a placeholder zero, so callers can no longer plumb a meaningless price through
    them. (Note: `Some(price)` for a Market order still compiles — this is a clarity win, not a
    compiler-enforced invariant.)
  - Migration: For `Limit`, `StopLimit`, and `TrailingStopLimit` orders, wrap the
    limit price in `Some()`. For `Market`, `Stop`, and `TrailingStop` orders, use `None`.
- **BREAKING**: Removed `ExchangeId::Alpaca`.
  - Use `AlpacaIex`, `AlpacaSip`, or `AlpacaCrypto` for market data feeds
  - Use `AlpacaBroker` for execution
  - Migration: Replace `ExchangeId::Alpaca` with the appropriate specific variant
- **BREAKING**: `AlpacaBracketOrderRequest` and `AlpacaBracketOrderResult` marked `#[non_exhaustive]`
  ([#69](https://github.com/Niqnil/rustrade/issues/69)).
  - Allows future field additions without breaking downstream code
  - Struct literal construction no longer works; use `AlpacaBracketOrderRequest::new()` constructor
  - Optional stop-loss limit price: chain `.with_stop_loss_limit_price(price)` after construction

### Fixed

- **IBKR integration tests no longer leave zombie connections** ([#63](https://github.com/Niqnil/rustrade/issues/63)):
  - Added `disconnect()` method to `IbkrHistoricalData`, `IbkrMarketStream`, and `IbkrClient`
    for explicit connection cleanup
  - Added `Drop` implementations that call `disconnect()` to ensure IB Gateway releases
    client IDs even when tests panic or exit abruptly
  - Added `#[serial]` attribute to all IBKR integration tests to prevent parallel execution
    conflicts when sharing IB Gateway connections
  - Previously, repeated test runs would fail with "client id already in use" until IB Gateway
    was restarted

## [0.1.0]

Initial release of rustrade, a fork of [barter-rs](https://github.com/barter-rs/barter-rs).

### Added

- **Hyperliquid support**: Full perpetuals and spot trading via `hyperliquid` feature
- **Interactive Brokers support**: Market data and execution via `ibkr` feature
- **Alpaca support**: Equities, options, and crypto execution via `alpaca` feature
- **Binance support**: Spot market data and execution via `binance` feature
- Structured error types with transient/permanent classification for retry logic
- Order state tracking with `Filled`, `Cancelled`, and `Expired` variants

### Changed

- Renamed crate ecosystem from `barter-*` to `rustrade-*`
- Bumped all crate versions to 0.1.0 for fresh namespace
- Updated minimum supported Rust version to 1.95

### Fork Attribution

This release is based on barter-rs v0.12.4. See [NOTICE](NOTICE) for full attribution.
