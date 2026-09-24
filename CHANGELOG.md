# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **London Strategic Edge option prints and option candles, with
  `LseVaultClient::fetch_option_flow` / `collect_option_flow` and `fetch_option_candles` /
  `collect_option_candles`** (`rustrade-data`, feature `lse`). US equity and ETF options: every
  executed print as an `LseOptionPrint` carrying its contract (`ticker`, `underlying`, `kind`,
  `strike`, `expiry`), price, size in contracts, notional premium and the greeks computed at that
  print, including `rho`; and one-minute premium bars as an `LseOptionCandle`, with the print count
  as `trade_count` and greeks **averaged over the minute** rather than sampled at its close. Both
  convert into the engine's `DataKind` events via `into_market_events` on the new
  `ExchangeId::LseOptions`: a print is stamped at the print, a candle at its close so the minute's
  outcome is never visible before it ends.

  **The flow fetch is oldest-first, which the endpoint is not.** `/options/flow` answers
  newest-first, truncates silently at a 5,000-row cap, ignores `offset`, and accepts only
  whole-second bounds, so a range can only be read in windows small enough to come back whole. The
  fetch walks forward in adaptive windows — a full page is treated as a truncated one, halved and
  re-read; a sparse one lets the next window grow — and emits each window in order, holding at most
  one page in memory. A single second too dense for one page is a typed
  `LseError::OptionFlowWindowSaturated` rather than a short tape. Because a full page is the only
  sign of truncation, each fetch first reads the provider's own `max_rows_per_request` from
  `usage()` and treats the smaller of that and `with_page_limit` as a full page — so a page limit
  raised past the provider's cap cannot hide a truncated window. The live canary checks that the
  walk, forced to halve repeatedly, reproduces a single-page read print for print, and that the cap
  `usage()` reports is the one `/options/flow` actually enforces.

  **Recent data is refused, not returned short.** The provider's ingestion lag is variable and
  episodic: a closed window was measured returning zero rows thirty seconds after closing, and
  another holding a fifth of its final count across two consecutive reads, both with a `200`. A
  partial window holds steady, so no amount of polling detects it. A range ending within
  `OPTION_FLOW_SETTLE_MARGIN` (60 s) of now is therefore `LseError::InvalidInput`.

  The endpoint silently ignores parameters it does not recognise — a `ticker` filter returns every
  contract — so there is no per-contract flow fetch (select from the result), and a row outside its
  requested window or underlying fails that window with `LseError::UnexpectedOptionFlowRow` before
  any of it is yielded. Option candles reuse the vault candle pager, whose range semantics the
  options endpoint was measured to share.

  ⚠️ **Greeks on this feed are print-triggered.** They arrive only with a trade, so a contract's
  greeks are as old as its last print and an unheld, untraded contract has none. The provider's
  chain snapshot endpoint, the only continuous alternative, serves stale stored fields and is
  deliberately not wrapped. There are no quotes on these paths, and timestamps are whole-second
  batch stamps: key on the print `id`. Strikes can be fractional on adjusted contracts, so they are
  `Decimal`. Exercise style is not reported and none is claimed.

  A live canary (`lse_options_canary`) is wired into `lse-weekly.yml`. It reads a window days old
  and fails on an empty tape, so a stopped feed cannot pass on nothing.

  ⚠️ Option data is provider data and may not be redistributed or committed as fixtures. See
  <https://londonstrategicedge.com/terms>.

- **`OptionInstrumentMarketData`** (`rustrade`): an `InstrumentDataState` for option contracts that
  wraps `DefaultInstrumentMarketData` and holds the contract's most recent `OptionGreeks` with the
  instant they were stamped. Greeks never contribute a price; marking is delegated unchanged. Nothing
  ages the held value out, and the rustdoc says so: check its stamp against the engine clock before
  treating it as a current risk figure. An update carrying no greek does not overwrite a real one.

- **`OptionGreeks::rho`** (`rustrade-data`). `OptionGreeks` is `#[non_exhaustive]`, so the field is
  additive, and greeks serialised before it existed still deserialise, as `None`. Alpaca published
  rho already and discarded it for want of a field; it is now mapped through. IBKR and Massive
  publish none and report `None`. `has_any_greek` now counts `rho`.

- **`ExchangeId::LseOptions`** (`rustrade-instrument`), appended at the end of the enum so no
  existing index is renumbered. It supports the `Option` instrument kind and, for now, **no
  subscription kind**: the provider's WebSocket delivers option prints under a
  subscribe-by-underlying handshake this integration does not yet implement.
  *Note:* `ExchangeId` is not `#[non_exhaustive]`, so downstream exhaustive `match`es need a new arm.

- **`LseCalendarEvent` and the economic-calendar fetch, with
  `LseDataApiClient::fetch_economic_calendar` and `fetch_economic_calendar_stats`**
  (`rustrade-data`, feature `lse`). The provider's archive of scheduled macroeconomic releases —
  124,896 events across 108 country codes, each carrying the consensus estimate, the previous
  figure and the actual outcome. Added to the existing `LseDataApiClient` rather than a new client,
  since it is the same host as `/bond-yields`.

  🔴 **The feed stopped on 2026-03-24, and that is documented as a limitation rather than a
  footnote.** It was measured frozen to the unit two months of wall-clock apart — `total_events`
  124,896 and `latest` 2026-03-24 on both readings — and confirmed from the other side: any window
  after that date returns `200` with zero events across all 108 countries. The forward-looking use
  case an economic calendar exists for is therefore **not served at all**. What remains is a genuine
  historical archive spanning 2014-12-31 to 2026-03-24, with an `estimate` on 74,843 events, which
  is useful for backtesting and event studies and is what this models. The module rustdoc says so
  first, and `LseCalendarStats::latest` is the live figure rather than a constant so a caller can
  detect a revival.

  Like `/bond-yields`, the endpoint validates nothing: an unknown country, an unknown impact rating,
  a reversed range and a window past the freeze all answer an identical `200 count=0`. So
  `fetch_economic_calendar` takes an `LseCalendarStats` as a **required** argument. But the
  validation it can offer is genuinely weaker, and the difference is documented rather than papered
  over: `/economic-calendar/stats` publishes **no per-country coverage**, only a flat global
  `earliest`/`latest`, while coverage is wildly uneven — 88 of 108 countries hold fewer than 100
  events and `UK` holds 56. An empty result inside the global range is therefore a legitimate answer
  and deliberately **not** an error, because inventing one would mean inventing knowledge the
  provider does not publish.

  Two silent wire traps are closed by construction. A repeated `country` key makes the provider keep
  only the **last** value — `country=US&country=UK` returns the 56 UK events and drops 36,421 US
  ones, with no error — so `LseCalendarQuery` joins with commas, which is a genuine OR, and nothing
  in the API can express the broken form. And `format` defaults to **CSV**, so every request sends
  `format=json` explicitly.

  Field choices are measurements. `event_date` is a `DateTime<Utc>` rather than the `NaiveDate`
  `/bond-yields` uses — the opposite call from its sibling, because every event carries a real time
  of day and 348 distinct ones occur, so a date would destroy intraday ordering. Numerics are
  `Option<Decimal>` via `rust_decimal::serde::str_option`: every numeric arrives as a JSON string,
  all ~462,000 non-null values parse, and negatives are abundant on `change` and
  `change_percentage`. Absence is always `null` and never `""` — the empty-string count is zero for
  every one of the eleven fields across the whole corpus. `impact` is a closed `LseCalendarImpact`
  enum in which **`None` is a literal provider rating carried by 592 events, not an absent value**,
  which is why the field is not an `Option`.

  `UK`-not-`GB` carries over from `/bond-yields`, so `normalise_country` applies the same single
  documented alias — but the vocabularies are **not** the same set: the calendar's 108 codes include
  `EA` and `EU`, which are not countries.

  A live canary (`lse_economic_calendar_canary`) is wired into `lse-weekly.yml` as a fourth REST
  canary. Its assertions are structural so they hold on a frozen feed, and it **records**
  `total_events`/`latest` in the log rather than asserting them — a revival is the outcome we want
  and must not fail the build.

  ⚠️ Calendar events are provider data and may not be redistributed or committed as fixtures. See
  <https://londonstrategicedge.com/terms>.

- **`LseDataApiClient` and the London Strategic Edge bond-yield endpoints** (`rustrade-data`,
  feature `lse`). Daily open/high/low/close sovereign yields for 34 countries — 716,820
  observations at the last measurement — via `fetch_bond_yield_stats` and `fetch_bond_yields`.
  These live on the provider's *second* host, `data-api.londonstrategicedge.com`, because a
  bond-yield symbol has no candle data on the vault at all (`GET /vault/candles?symbol=UK5Y`
  answers `404`). `LseDataApiClient` is a second thin wrapper over the transport introduced
  alongside it, so both hosts share auth, agent, timeouts, redirect policy, rationing and error
  mapping.

  **The stats handle is a required argument to `fetch_bond_yields`, not an option**, because the
  endpoint performs no validation of its own. Measured: an unknown country (`ZZ`), the ISO-3166
  spelling of a country the provider keys differently (`GB`), and a nonsense tenor (`99Y`) each
  answer `200` with `count: 0` — an envelope identical to a genuinely quiet window. Making
  validation unskippable in the type system is the only way a wrong query fails loudly rather than
  returning an empty `Vec`.

  Validation checks the requested window against the **tenor's own** coverage, not merely that the
  `(country, maturity)` pair exists. Membership alone is insufficient: `US 10Y TIPS` and `TR 2Y`
  both exist and both return nothing for 2023–2024, because neither was published before
  2025-07-07. Three typed errors distinguish the three causes a zero-row response otherwise
  conflates — `LseError::UnknownBondYieldCountry`, `UnknownBondYieldMaturity` (carrying the
  country's published tenor list) and `BondYieldRangeOutsideCoverage` (carrying the window that
  does exist).

  ⚠️ **The provider keys the United Kingdom `UK`, not `GB`**, despite naming the column
  `country_iso2`; `GB` is absent from all 34 keys, while the vault's catalog spells the same
  country `GB`. It is the only divergence in the set. `LseBondYieldStats::normalise_country` maps
  it, and every entry point applies the mapping before the request is sent.

  ⚠️ **A tenor is a provider label, not a duration.** Each US TIPS tenor reports the same
  `maturity_days` as its nominal twin (`5Y` and `5Y TIPS` both `1825`), so keying a series on
  `(country, maturity_days)` silently merges a real yield with an inflation-linked one. `maturity`
  identifies the series; `maturity_days` is a derived hint.

  Every field of a row arrives as a JSON **string**, numerics included, so prices decode with
  `rust_decimal::serde::str` — the `rust_decimal::serde::float` helper used by `AlpacaStockSplit`
  expects a JSON number and fails here. The response is **not paged**: a request for one tenor's
  full published history returned all 10,004 rows in a single response, matching what the stats
  endpoint reports, so the fetch returns a `Vec` rather than inventing pagination for a surface
  that has none. The envelope's own `count` is checked against the rows delivered, which is the
  signal a silently introduced page cap would produce.

  A live canary (`lse_bond_yield_canary`, `#[ignore]`d, wired into the weekly drift workflow)
  asserts each of those premises against the real API rather than against a fixture.

  ⚠️ Bond-yield rows are provider data and may not be redistributed or committed as fixtures. See
  <https://londonstrategicedge.com/terms>.

- **`LseCatalogEntry`, the London Strategic Edge catalog record, with
  `LseVaultClient::fetch_catalog`** (`rustrade-data`, feature `lse`). The provider's index of
  everything it publishes — 22,966 entries at the last measurement — as a provider-shaped type in
  the same spirit as `AlpacaStockSplit`, not a provider-agnostic abstraction. Nothing is wired into
  the engine: reference series never stream.

  `LseCatalogEntry::class` separates price datasets from reference series using the provider's own
  `frequency`/`category` pair. The rule is not a heuristic — across all 22,966 entries it classifies
  every row with none left over (15,537 reference / 7,429 price) — so a dataset the provider adds
  later classifies itself with no list here to update.

  Three field choices are measurements rather than taste. `ticks` is `u64` because the largest
  observed count is 96.5% of `u32::MAX` on a tape that is still growing. `frequency` stays the
  provider's own string because the vocabulary is dirty — ten spellings including both `biannually`
  and `bi-annually`, both `quarterly` and `quarter` — so a closed enum over the obvious six values
  would have silently mishandled seven rows; the label also fails to predict observed spacing, so it
  must not be read as a cadence contract. `first_tick`/`last_tick` are parsed on demand rather than
  at decode, so one malformed timestamp surfaces on its own entry instead of failing a whole fetch.

  `LseCatalogEntry::price_dataset` resolves to an `LseDataset` where one exists and returns `None`
  otherwise. `None` does not mean "reference data": `options` is a price dataset with no
  `LseDataset` variant, and it accounts for 3,186 of the 7,429 price entries, so callers pair this
  with `class` rather than reading `None` as a classification. `LseDataset::from_catalog_str`'s
  documented `UnknownDataset` contract is left exactly as it was.

  ⚠️ Catalog contents are provider data and may not be redistributed or committed as fixtures. See
  <https://londonstrategicedge.com/terms>.

### Changed

- **BREAKING: connectivity state is read-only outside `rustrade`** (`ConnectivityStates`,
  `ConnectivityState`). `ConnectivityStates::update_from_account_reconnecting` is crate-private. So
  are the fields `ConnectivityStates::{global, exchanges}` and `ConnectivityState::{market_data,
  account, role}`, and the `connectivity_mut` and `connectivity_index_mut` accessors.
  `EngineState::connectivity` is public. A caller could therefore mark an exchange's account
  connection as reconnecting without arming its instruments for the account resync, which only
  `EngineState::update_from_account_reconnecting` does. The next complete snapshot then could not
  retire an order that ended while the stream was down. A direct field write also left the cached
  `global` health out of step with the venues it summarises. To migrate:
  - Read through the new getters of the same names: `global()`, `exchanges()`, `market_data()`,
    `account()` and `role()`.
  - Report an account disconnect through `EngineState::update_from_account_reconnecting`.
  - Build a `ConnectivityState` with `ConnectivityState::new`.

- **BREAKING: the Hyperliquid clients refuse an order whose client id is not a UUID in
  `ClientOrderId::uuid()` form** (`rustrade-execution`, `HyperliquidClient` and
  `HyperliquidSpotClient`). Hyperliquid names an order by the 16-byte `cloid` it was placed with,
  and only a lowercase, hyphenated UUID comes back from it as the same id; an order placed under any
  other id could never be matched to its own updates. Previously only trigger orders required a
  UUID, and other orders were sent without a `cloid`. `open_order` now answers such a request with
  `OrderError::Rejected` before anything is sent, whatever the order kind. `common::cid_to_cloid`
  returns `None` for every other spelling of a UUID, uppercase or unhyphenated included. To
  migrate, generate Hyperliquid client ids with `ClientOrderId::uuid()`.

- **BREAKING: `InstrumentAccountSnapshot` gains `orders_complete`, a client's statement that its
  `orders` list is every order open at the venue for that instrument** (`rustrade-execution`). A
  snapshot's list could previously miss an open order without saying so: Alpaca drops a notional
  order on conversion, IBKR's list is always empty, and Hyperliquid reports each order under its
  venue `oid` rather than the id it was placed with. So nothing could read an order's absence as
  meaning it was gone. `orders_complete: true` now states that it can, which the engine relies on
  to retire vanished orders (see Fixed). The field is `#[serde(default)]` false, the answer that
  claims nothing, and `ExecutionClient::account_snapshot` now documents what a client must
  guarantee before setting it. Binance Spot and Margin set it per symbol, only when every
  `openOrders` row converted under its own `clientOrderId`; the mock venue sets it always, and
  Hyperliquid per instrument (see the entry below). Alpaca and IBKR set it `false` until the gaps
  above are closed (#369, #371). To migrate, pass the new
  argument to `InstrumentAccountSnapshot::new` after `orders`, or add the field to a struct literal;
  `false` keeps the previous behaviour.

  `InstrumentState` gains `orders_open_at_resync` (`#[serde(default)]`), and
  `EngineState::update_from_account_reconnecting` now handles an account stream's reconnect notice.
  The engine and the audit replica both call it in place of
  `ConnectivityStates::update_from_account_reconnecting`, which it wraps and which is now
  crate-private (see the entry above).

- **BREAKING: `Subscriber` gains an associated `Transport: Send` type, and `Subscribed` is generic
  over it** (`rustrade-data`). `Subscribed<InstrumentKey, Transport = WebSocket>` names what a successful
  subscribe hands the stream, and its `websocket` field is renamed `transport`. Every in-tree
  subscriber sets `type Transport = WebSocket`, so behaviour is unchanged; the standard
  `ExchangeWsStream` initialisation is bounded on `Subscriber<Transport = WebSocket>`. This is the
  seam that lets a subscriber whose streams share one connection hand each stream a view of it
  rather than a socket of its own, for providers that allow a key a single connection. To migrate a
  custom `Subscriber`, add `type Transport = WebSocket;` and construct `Subscribed` with
  `transport:` in place of `websocket:`.

- **BREAKING: `Open::id` and `RequestCancel::id` now carry a `VenueOrderId`, which distinguishes an
  order the venue named from one it did not** (`rustrade-execution`). Both fields previously held a
  plain `OrderId`, and that field had come to mean two different things. A venue that accepts an
  order without assigning an identifier of its own — Hyperliquid, for one resting but not yet
  triggered — leaves it addressable only by the client id sent with it, and the Hyperliquid client
  stored that client id in the venue-id field. Nothing marked which kind an `OrderId` held, so the
  cancel path recovered the distinction by parsing: numeric meant a venue oid, UUID-shaped meant a
  client id. Two identifiers told apart by the shape of their text is not a distinction a caller can
  rely on.

  `VenueOrderId::Assigned(OrderId)` and `VenueOrderId::ClientAssigned` now state it outright.
  `VenueOrderId::assigned` yields the venue's identifier or `None`; `is_same_order_as` and
  `contradicts` answer identity without treating two absent identifiers as a match, which plain `==`
  on the old field could not avoid. `RequestCancel::id` is `Option<VenueOrderId>`, keeping three
  cases apart that would otherwise collapse into two: nothing acknowledged yet, cancel by venue
  identifier, and cancel by client id. Hyperliquid's cancel now reads the variant instead of parsing.

  Migration: construct `Open` with `VenueOrderId::Assigned(order_id)` where a venue assigned one,
  and `VenueOrderId::ClientAssigned` where it did not; `From<OrderId>` is available for the common
  case. Replace `open.id == some_order_id` with `open.id.assigned() == Some(&some_order_id)`, and any
  "is this the same order" test with `is_same_order_as` or `contradicts`. `Open` serialises
  differently as a result, so persisted order state from an earlier version will not load. The
  terminal states — `Cancelled`, `Filled` and `Expired` — keep a plain `OrderId`: they are records
  of an order that has ended rather than handles for addressing one, and nothing compares them for
  identity.

- **Successive `Open` states for one order are now ordered by cumulative fill as well as by
  `Open::time_exchange`** (`rustrade`, `rustrade-execution`). The new `Open::is_superseded_by`
  replaces the bare timestamp comparison that `OrderManager::update_from_order_snapshot` used at
  its three merge points. Cumulative fill is append-only for a single venue order, which makes it
  a stronger ordering signal than a timestamp the venue may not supply: an update reporting
  strictly less filled than the tracked state is refused however it is stamped, and one reporting
  strictly more is admitted however it is stamped.

  Two behaviours change as a result. A snapshot stamped earlier than the tracked state but
  reporting more filled is now applied rather than discarded — this is what lets a reconciliation
  fetch pinned to an order's creation time deliver the cumulative it alone holds. And a snapshot
  reporting an order fully filled now retires it even when stamped earlier, because an order the
  venue has once reported complete cannot become live again. Consumers that relied on the tracked
  `time_exchange` never moving backwards should note that adopting an earlier-stamped update
  carries its stamp with it; the state is taken as the venue reported it rather than recombined.

- **The London Strategic Edge candle path now documents two limitations it had been passing on in
  silence** (`rustrade-data`, `rustrade`; `lse` feature; documentation only, no behaviour change).

  **Volume can be wrong by three to four orders of magnitude, in opposite directions.** The module
  documentation already recorded that a majority of sampled one-minute equity bars report `0` in
  minutes that demonstrably had trades. Since 2026-04-27 ETF bars have additionally been reported
  over-stating volume enormously — one `QQQ` minute published at roughly 5,700× that session's
  entire consolidated volume, repeated across `SPY`, `IWM`, `SMH`, `XLE`, `XLF` and `TLT` on every
  trading day sampled over two months — while equity daily totals ran at 24–45% of the
  consolidated tape against 65–80% before the same date. Those bars are structurally valid, so
  neither `fetch_candles` nor any shape check on its output can distinguish them from correct
  ones. `fetch_candles`, the module documentation and both candle examples now say so, and say
  that a volume-derived quantity must be reconciled against a second source before it is trusted.

  **History depth varies per symbol and per dataset, and a range exceeding it is not an error.**
  Equities are stated to reach back to 2004, while an ETF has been reported carrying a first tick
  of 2026-04-27 — about three months of spot. The provider publishes a first tick and a coverage
  span for every catalog entry, but the catalog is on its discovery host and this integration does
  not wrap it, so nothing here can check a requested range against it. A fetch starting before a
  symbol's coverage returns the bars that exist and nothing to indicate the remainder was never
  published, which in a backtest presents as a successful run over a shorter period than the one
  asked for. Both the API documentation and the backtest example now state this and direct callers
  to establish depth per symbol first.

- **The London Strategic Edge HTTP plumbing now lives in one internal transport shared by the
  provider's hosts** (`rustrade-data`; `lse` feature; internal refactor, no public API or behaviour
  change). The auth header, the `User-Agent` their CDN requires, the timeouts, the no-redirect
  policy that keeps the key off a server-named host, the concurrency-and-pacing gate and the
  status-to-error mapping were all defined inside `LseVaultClient`. The provider serves reference
  data from a second host that needs every one of them, so they moved to an `LseHttpCore` that
  takes its base URL from whichever client wraps it; `LseVaultClient` keeps its own base URL and
  page limit and is otherwise a thin wrapper.

  `LseVaultClient`'s public surface, its `Debug` output and its rationing semantics are unchanged —
  a core is still one ration pool shared by clones, so two clients still ration independently. The
  `User-Agent` requirement is now documented as measured on both hosts rather than on the vault
  alone: each answers a request carrying the default agent of a common HTTP client with `403`
  `error code: 1010` at the edge, before it reaches the API. The one observable difference is a
  `debug`-level log line, which reads `lse response received` in place of `vault response
  received` now that it is emitted for either host.

  Reading `LSE_API_KEY` is now shared too. The REST clients and the WebSocket connector had
  separate copies of the variable name and of the redaction that keeps a mis-encoded key out of
  the error message — `VarError`'s non-UTF-8 arm embeds the raw value, so interpolating it would
  put essentially the whole key into a string callers log. They now read through one helper and
  wrap its failure in their own error type, so that redaction has a single definition and cannot
  drift between the two surfaces. The messages themselves are unchanged.

### Fixed

- **Hyperliquid reports each order under the client id it was placed with** (#368,
  `rustrade-execution`). The account snapshot and `fetch_open_orders` reported every open order
  under its venue `oid`, because the SDK's type for the `openOrders` response drops the `cloid`
  the venue sends. Order updates on the account stream reported the `cloid` as the venue echoes it,
  `0x` and 32 hex digits, which is not the id the order was placed with either. So no order state
  reached the order the engine tracked: each snapshot inserted a duplicate of every open order,
  keyed by its `oid` and owned by `StrategyId::unknown()`, while the tracked order was never
  advanced by a partial fill or retired when cancelled. Both paths now turn the `cloid` back into
  the id the order was placed with; an order placed without one, such as from the web app, is
  still reported under its `oid`. Open orders now also carry their filled quantity, from the
  `origSz` the SDK type dropped.

  With every order identifiable, the account snapshot declares its order list complete
  (`orders_complete`), so the engine retires an order that ended while the account stream was
  down. The snapshot now has an entry for every requested instrument, open orders or not, so that
  covers the instrument whose last order ended. An order that does not convert is logged at `warn`
  and leaves its instrument's list incomplete, rather than being left out silently.

- **Hyperliquid order updates that end an order for a stated reason now end it**
  (`rustrade-execution`). Only `open`, `filled` and `canceled` were recognised. Every other status
  Hyperliquid sends, such as `marginCanceled`, `selfTradeCanceled`, `reduceOnlyCanceled`,
  `scheduledCancel` and the `…Rejected` family, was logged and dropped, leaving the order tracked
  as open. A status ending in `Canceled` now cancels the order, one ending in `Rejected` rejects
  it, and `triggered` keeps a trigger order open until its own ending arrives.

- **The London Strategic Edge canaries no longer print provider data when they fail**
  (`rustrade-data` tests). `lse-weekly.yml` runs them with `--nocapture` in a public repository,
  so their failure messages are published in the workflow log. Three put provider data there:
  - the bond-yield canary's OHLC checks printed a row's yields;
  - the economic-calendar canary's checks debug-printed a whole event, readings included;
  - the WebSocket canary's stream-error line printed a decode failure's raw frame.

  Each now names the row, event or field that failed and nothing else. The frame cut is pinned by
  an ordinary test, so a change to the error's wording fails CI rather than leaking a frame. LSE
  data may not be redistributed; see <https://londonstrategicedge.com/terms>.

- **The engine retires an order that a complete account snapshot no longer lists** (`rustrade`).
  A snapshot applied only the orders it listed, and `ExecutionManager` re-reads one on every
  account-stream reconnect. So an order that filled, was cancelled or expired while the stream was
  down stayed active in `Orders` indefinitely, and a strategy could go on treating it as working
  liquidity. The snapshot a reconnect produces now retires each tracked `Open` order that its
  instrument's list leaves out, when the client declares that list complete, and logs each
  retirement at `warn`. Absence cannot say how an order ended, so the order's fill is left to the
  fill path, and a late fill still routes to its position.

  Only an order that was already `Open` when the reconnect began is eligible. The venue is re-read
  while order requests are still being answered, so an order accepted just after the read can
  reach the engine as `Open` before the snapshot that cannot list it; the engine records which
  orders were `Open` at the reconnect notice and leaves every other order alone. An order in flight
  is never retired, since its request's answer settles it, and neither is anything in a snapshot
  that does not declare its list complete, including the one that starts a run. An order is retired
  only when its venue order id proves it is the order recorded, so a client id reused for a new
  order before the snapshot arrives keeps the new order, and so does an order the venue never
  assigned an id. Each order kept that way is logged at `warn` too, since it may be gone. (#364)

- **A Binance fill recovered after a disconnect now advances its order** (`rustrade-execution`,
  feature `binance`; Spot and Margin). Recovery reads missed fills from REST `myTrades`, which
  reports executions only, so a recovered `Trade` carried no `order_filled_quantity`. It moved the
  position and left the order's `filled_quantity` where it stood before the gap, while the same
  fill arriving live over the WebSocket advanced it. Recovery now reads each recovered order's
  executions from its first (`myTrades` by `orderId`, one extra request per order, up to four
  orders at a time per instrument) and sets the cumulative as of each fill, the same figure the
  WebSocket reports as `z`. An order that fills completely during an outage is therefore retired
  by its recovered fills.

  The lookups have their own budget, half of the 30-second recovery timeout, so they never cost a
  fill. A fill whose order was not looked up in time, or whose lookup failed or came back unusable,
  goes out with `order_filled_quantity: None` as before, logged at `warn`. Alpaca's recovery
  already carried the cumulative (activity `cum_qty`). Order cancellations during an outage are
  still not recovered; see #364 for the engine side of reconciling them.

- **A Binance REST order that is no longer live can no longer become an `Open` order**
  (`rustrade-execution`, feature `binance`; Spot and Margin). The shared open-order converter read
  every field but the order's status, so it treated any row it was given as resting. The two
  `openOrders` call sites only ever serve live orders, but the converter also accepts Spot
  `allOrders` rows, where a cancelled order that had partly filled would have converted to `Open`
  with quantity remaining and rested in engine state indefinitely. The converter now admits only
  `NEW`, `PARTIALLY_FILLED` and `PENDING_NEW` (an order-list leg waiting on its working order), and
  drops any other or missing status with a `warn`, matching the guard the `executionReport` path
  already applies. (#329)

- **An IBKR market-depth RESET no longer leaves a silently stale order book** (`rustrade-data`,
  feature `ibkr`). IB sends notice 317, *"Market depth data has been RESET"*, when TWS discards the
  book on its side; every level held locally is stale from that moment. `ibapi` 4.1.0 reclassified
  317 from an error to a data advisory, which is the correct reading — but the depth loop consumed
  the subscription through `iter_data()`, and that iterator drops notices. The reset therefore
  became invisible, and `DepthAggregator` went on applying updates to a book the venue had already
  thrown away. Before 4.1.0 the same notice ended the stream, so the book was rebuilt by accident
  rather than by design.

  The loop now reads the subscription through `iter()` and handles the notice. `DepthAggregator`
  gains `on_venue_reset`, which empties both sides and returns the emptied snapshot so it is
  forwarded immediately rather than after the next depth row — the window in between is exactly
  when a consumer would still be holding levels that no longer exist. It advances the sequence
  counter instead of resetting it, which `clear` does and which would have been wrong here: a
  consumer ordering by sequence reads a book renumbered to 0 as older than the stale one it
  replaces, and keeps the stale one. Notice 316 (*"HALTED"*) remains terminal and is unchanged.

  A book that looks live and is not is worse than no book, so this is a correctness fix rather than
  a robustness one. It is unit-tested; confirming it end to end needs a live level-2 subscription
  that receives a reset, which CI does not run.

  `IB_MARKET_DEPTH_RESET_CODE` is exported alongside it: `DepthAggregator` is public, so a caller
  driving one from their own subscription loop needs the code that triggers `on_venue_reset`.
  `ibapi` names no constant for it, exposing only membership in `DATA_ADVISORY_CODES`.


- **An out-of-sequence order snapshot can no longer rewind an order's state at a venue that
  reports no timestamp of its own** (`rustrade`). IBKR's `orderStatus` callback carries no
  timestamp field, so the client stamps `Utc::now()` as it processes each one. Those stamps record
  arrival rather than the venue's own sequence and rise monotonically, so ordering on the stamp
  alone admitted every snapshot and left the venue with last-writer-wins: a snapshot that overtook
  a newer one in flight silently overwrote newer state with older, including the order's cumulative
  filled quantity. Ordering now also rests on that cumulative, which is append-only for one venue
  order, so the overtaken snapshot is recognised as out of sequence and refused. This is a
  venue-independent change to engine state, not an IBKR one; IBKR is where the absence of a usable
  timestamp made it load-bearing.

- **An order snapshot is no longer applied to a tracked order it does not belong to** (`rustrade`).
  `OrderManager::update_from_order_snapshot` resolves a snapshot to a tracked order by
  `ClientOrderId` and nothing else, so two exchange orders sharing one client id occupy the same
  slot. `Orders::update_from_fill` had long refused a fill whose venue identifier disagreed with the
  order it would advance; the snapshot path, which writes the order's price, quantity, kind, time in
  force and cumulative fill and can retire it outright, had no equivalent check. The three arms that
  merge an incoming `Open` into a state the venue has already named now refuse an update that names
  a different venue order, and report it. The test is for contradiction rather than inequality: an
  order the venue has not yet named carries nothing to disagree with, and must still be able to
  adopt the identifier a later snapshot brings — refusing that would strand such an order on its
  placeholder for the rest of its life, never learning its venue identifier and never learning its
  fills.

- **A Binance Margin fill now advances its order** (`rustrade-execution`, `binance` feature). A
  Binance `executionReport` of type `TRADE` carries two facts: the execution print (`l`/`L`) and
  the order's new cumulative filled quantity (`z`). The margin client emitted only the first, and
  an execution on its own never moves an order — `Orders::update_from_fill` writes
  `Open::filled_quantity` and does nothing at all when the order is not already tracked, which is
  the case for an order placed out of band, after an engine restart mid-order, or for a fill
  arriving inside the documented subscribe/listener race. A margin `TRADE` now emits the paired
  `OrderSnapshot` alongside the `Trade`, as Binance Spot has since 0.6.0, unless the report's own
  order status (`X`) says the order is no longer working — emitting one then would resurrect an
  order the engine has already retired. The snapshot is stamped with the execution's transaction
  time (`T`), which is also what gives the engine's staleness gate an ordering key on the margin
  WebSocket path.

- **Binance Spot and Margin now share one `executionReport` converter** (`rustrade-execution`,
  `binance` feature). The margin client carried a hand-maintained copy of spot's WebSocket
  user-data converter, and the fix above is the third correction to reach spot and not margin. The
  new `BinanceExecutionReportFields` trait names the eighteen fields the conversion reads — they
  are identical in name and type across `spot::websocket_api::ExecutionReport` and
  `margin_trading::websocket_streams::ExecutionReport`, even though the structs around them are
  not (55 fields against 50, with eight sharing a name while differing in type) — and a single
  converter parameterised by `ExchangeId` now serves both clients. This mirrors
  `BinanceOrderFields` on the REST path. No public API changes; diagnostics from this path now
  carry the venue as a structured `exchange` field rather than a hard-coded message prefix.

- **A Binance Margin REST order snapshot is now stamped with when the order last changed, not when
  it was created** (`rustrade-execution`, `binance` feature). The engine orders an order's states by
  `Open::time_exchange` and discards any snapshot no newer than the state it already tracks. Margin
  stamped every snapshot with the venue's creation time, which is identical across every snapshot of
  one order, so margin had no usable ordering key: reconciliation snapshots compared equal and were
  applied in whatever order they arrived, and any snapshot would be discarded outright as soon as
  something advanced the order past creation — precisely the partially filled orders a reconciliation
  fetch exists to repair. The converter now prefers the venue's `updateTime`, falling back to
  creation time only where the venue omits it. Binance Spot has behaved this way since 0.6.0.

- **Binance Spot and Margin now share one REST order-response converter** (`rustrade-execution`,
  `binance` feature). The margin client carried a hand-maintained copy of the spot converter, and
  the fix above is the second correction that reached spot and not margin. The `BinanceOrderFields`
  trait, which names the eleven fields the conversion reads, now covers the margin open-orders
  response alongside both spot responses, and the single converter is parameterised by `ExchangeId`.
  Those eleven fields are identical in name and type across all three SDK types even though the
  structs around them are not, so naming the read subset is what makes one converter safe to share.
  No public API changes; diagnostics from this path now carry the venue as a structured `exchange`
  field rather than a hard-coded message prefix.

- **The London Strategic Edge vault canary no longer races the provider's concurrency cap**
  (`rustrade-data`, `lse` feature). The vault permits two concurrent requests and
  `LseVaultClient` never retries a `429` by design, so running the canary's four tests in
  parallel — the harness default — could put three requests in flight and fail whichever test
  lost the race with `LseError::RateLimited`. The failure was indistinguishable from the
  provider-side drift the canary exists to detect. The tests are now `#[serial]`, matching the
  Alpaca and Massive live tests, which holds the binary to one in-flight request. Test-only; no
  library behaviour changes.

- **The London Strategic Edge WebSocket canary could not be run as a file, and now runs weekly**
  (`rustrade-data`, `lse` feature). The provider permits **one** WebSocket connection per API key
  and answers a second with `TOO_MANY_CONNECTIONS`. Rust's harness runs a file's tests in
  parallel and four of these five open a socket, so running the file exactly as its own
  documentation prescribes failed four tests inside their `expect`, before any assertion — while
  the one that won the race passed. The tests are now `#[serial]`, matching the vault canary and
  the Massive WebSocket tests, which holds the binary to one socket at a time; the full file now
  passes in about seventy seconds.

  The canary was also wired into the weekly live-API workflow, which until now ran only the vault
  and export canaries — so this surface had never been scheduled at all, independently of that
  workflow's own reachability. The job's timeout rises to thirty minutes, since serialised tests
  sum their timeouts rather than overlapping them.

- **The London Strategic Edge WebSocket canary no longer reports green on a throttled stream**
  (`rustrade-data`, `lse` feature). It asserted that every subscribed symbol delivers a tick, that
  each tick's decoded instant is plausible, and that no frame failed to decode — none of which a
  throttled connection violates. A session serving a couple of percent of its normal tick rate,
  with the socket up and no error frame, satisfies all three: every symbol still delivers, and
  every timestamp on what arrives is genuinely fresh. A new test counts ticks over a fixed window
  on the continuously-traded crypto tape and fails below a floor set more than an order of
  magnitude beneath the slowest rate ever measured on that feed.

  The floor is held against crypto alone, and the canary's documentation now says so outright. On
  a venue that keeps market hours the same low count is produced by a shut market and by a
  throttled feed alike, so a floor there would fail for a closure and would end up muted, taking
  the signal with it. A passing run therefore reports that the crypto tape is flowing and says
  nothing about throughput on the equities, ETF, FX and CFD venues, which stay covered only
  against a subscription that never ticks and a frame that cannot be read. Test-only; no library
  behaviour changes.

### Security

- **The dead `RUSTSEC-2024-0436` (`paste`) suppression has been dropped from `deny.toml` and the
  CI audit job's ignore list.** `paste` left the graph when `parquet` moved to 59.2.0; it appears
  zero times in `Cargo.lock`, so the entry has been suppressing an advisory for a crate the build
  no longer contains. Its justification had gone stale on both counts — it still read *"Transitive
  via parquet 59.1.0 (latest)"* while the manifest is on 60.0.0.

  Removing it is bookkeeping, not a behaviour change: an ignore for an absent crate can never fire,
  so no advisory becomes newly visible and no gate becomes newly strict. It is removed because a
  suppression list is only readable as a list of accepted risks if every line on it is a risk that
  still exists. The two lists stay synchronised at ten ids each, which is the property the CI job's
  *"Synced with deny.toml"* comment asserts.

  The neighbouring `rkyv` (`RUSTSEC-2026-0235`) entry is deliberately kept: unlike `paste` it is
  still in `Cargo.lock`, as an unenabled optional dependency of `rust_decimal` that the build never
  compiles but the feature-agnostic lockfile still records.

## [0.6.0] - 2026-09-18

### Added

- **An unroutable Hedging fill is now counted, not only logged** (`rustrade`). In
  `OmsMode::Hedging`, a fill whose `PositionId` cannot be resolved opens a position keyed by its raw
  exchange `OrderId`, splitting that order's PnL across two position slots. Until now the only trace
  was a `warn!`, so nothing downstream could tell a split session from a clean one.

  `TearSheet` and `TearSheetGenerator` gain three fields, all `#[serde(default)]` so sheets
  serialised before they existed still load:
  - `fills_routed_by_fallback` — an order was found, but nothing said where its fills belong. This
    is the split, and a non-zero value means every per-position statistic on the sheet is computed
    over a partition the strategy never chose.
  - `fills_unmatched` — no order matched at all, so the fill is external or was removed by snapshot
    reconciliation. Counted apart because one position per external order is a defensible reading
    rather than a split; an account traded from elsewhere should expect this to be non-zero.
  - `first_fallback_detail` — why the first fill of either kind could not be routed, so diagnosing a
    split position does not require re-running with logging enabled.
  - `fallback_positions` — the distinct positions those fills opened, so a consumer can look them up
    instead of parsing the detail string. Deduplicated and capped at `MAX_FALLBACK_POSITIONS` (16);
    the counters stay exact when the list saturates.

  `TradingSummary` carries the two counters summed over every instrument, alongside the existing
  `orders_opened` / `orders_rejected`, plus a `has_split_positions()` helper. `print_summary` prints
  a `split_position_notice()` banner above the tables, beside the existing rejection banner and for
  the same reason: those tables are computed per position, so an order whose PnL was divided between
  two slots is reported as two ordinary-looking partial results.

  The banner and the helper key off `fills_routed_by_fallback` alone. `fills_unmatched` is reported
  but never raises it, because an account also traded by hand or by another system produces unmatched
  fills during correct operation, and a banner that fires on a healthy session is one people learn to
  scroll past.

  Both counters stay `0` in `OmsMode::Netting`, where a single position key makes the failure
  unreachable. Routing behaviour is unchanged. Two things reach the fallback and only one is fixable:
  the late-fill window documented on `cleanup_routing_tables`, which is deferred rather than
  impossible; and the corporate-action split path, which deliberately drops a resting order's
  `PositionId` mapping because retaining it would let a late fill reopen a floored-out position. The
  contract is now stated on `InstrumentState::update_from_trade`.

- **`SimulatedVenue` caps a taker fill by the size on offer, so an order can fill in part**
  (`rustrade-execution`). An arriving order that aggresses now trades at most what the book says is
  available on the far side, and draws that size down as it takes it — so two orders arriving
  between two market observations share one displayed size instead of each taking the whole of it.

  The new `MarketDepth` carries that size alongside a `MarketSnapshot` rather than inside it: a
  snapshot says what the market is worth and is what a `FillModel` prices from, while a size is what
  the venue's own arithmetic bounds a quantity by. `SimulatedVenue::apply_market` takes it as a new
  parameter, and `VenueMarketUpdate` (in `rustrade`) grows a `depth` method that defaults to
  "no size information".

  **Absent size means unlimited, not unfillable.** A trades-only feed, a candle feed, a price-only
  export and a `VenueRegime::RequestPriced` venue all supply no size and are capped by nothing, so
  price-only backtests and `MockExecution` results are unchanged. A level that carries a price and a
  *zero* amount is read the same way, because that is what a feed publishing prices without sizes
  looks like on every row.

  Consequences, each covered by a test:
  - **`ImmediateOrCancel` and `FillOrKill` no longer coincide.** Immediate-or-cancel keeps what the
    book could give it and cancels the rest; fill-or-kill refuses a partial fill and trades nothing.
    Their agreement has been documented as conditional since they were accepted, and this is that
    condition expiring.
  - **A marketable limit order the book cannot fill whole now fills in part and rests the
    remainder**, carrying what already traded in `Open::filled_quantity`.
  - **A market order's unfillable remainder is cancelled**, carrying `Cancelled::filled_quantity` —
    which is the first time this venue makes that field non-zero for an order it booked itself.
  - **An order that cannot fund both legs is refused whole.** A capped order costs more than the
    same order filling outright, because its remainder is held at the order's own limit while the
    fill struck a better price, so an account can afford the whole order and not the split. It is
    rejected rather than filled for the part it could pay for.

  Only takers are capped. A resting order still fills its whole remaining quantity when the market
  reaches it: capping a maker honestly needs the volume that actually printed through its limit,
  which this venue's feed does not carry.


- **A simulated request is booked when it reaches its venue, not when the `Engine` sends it**
  (`rustrade`). `SimRunner` queues each `ExecutionRequest` at `time + to_venue` alongside the
  account events its venues produce, and runs it only once the queue reaches that instant — by which
  point every market event up to it has already been routed to the venue. `to_venue` therefore
  becomes price-relevant rather than a pure delivery delay.

  Three outcomes that were previously unrepresentable:

  - **A tick that prints while a request is in flight no longer fills it.** The order is not on the
    book yet, so liquidity that was gone before the order existed can no longer trade against it.
  - **An order that is marketable when it arrives crosses as the aggressor**, paying the book,
    instead of resting at a price the market had already left and being filled there later.
  - **A cancel can lose to a fill.** A fill struck between a cancel being sent and arriving retires
    the order first, and the cancel is answered `ApiError::OrderAlreadyFullyFilled`. Booking on
    observation made the cancel win every such race, at any latency.

  **Results are unchanged at `latency_ms: 0`**, where the two instants coincide on every request,
  and the committed tear sheet is byte-identical at the fixture's `latency_ms: 100` because a market
  order is still priced from the snapshot its own request carried. Of the entries merged at one
  instant, a venue-bound action now leads — which is what the previous booking-on-observation order
  did structurally, and what keeps this change from moving an existing result.

- **The simulated venue honours `post_only`, `ImmediateOrCancel`, `FillOrKill` and `GoodTillDate`**
  (`rustrade-execution`, `rustrade`). A post-only order that would take liquidity on arrival is
  cancelled rather than filled, and rests otherwise. An immediate-or-cancel or fill-or-kill order
  fills in full if it is marketable and otherwise retires with `filled_quantity` zero — the two
  coincide **because** this venue models no order size, and will diverge only once an order can fill
  in part. A `GoodTillDate` order rests until its stated instant and is then retired as
  `Inactive(Expired)`, releasing what was held against it.

  `GoodUntilEndOfDay`, `AtOpen` and `AtClose` remain rejected on a limit order: honouring them needs
  a session calendar this venue has not got, and treating them as good-until-cancelled would leave
  an order working that its sender asked to have cancelled.

  **A deadline is swept, not scheduled.** This venue has no timer, so a `GoodTillDate` order is
  retired when a driver next calls `advance_time` or `apply_market`, whichever comes first — and
  the sweep spans every instrument, because a deadline is a property of the clock rather than of a
  book. A deadline is an unconditional cutoff: an order is retired even by the very tick that would
  have crossed it, and one that arrives already past its deadline never rests and never trades.

- **`SimulatedVenue::advance_time` returns the orders its advance retired, and is `#[must_use]`**
  (`rustrade-execution`, `rustrade`). ⚠️ **Breaking**: it previously returned `()`. Per retired order
  the events are `[balance, order]` — the released reservation, then the terminal snapshot — or
  `[order]` alone for an order the venue never took a reservation for. A driver that advances the
  clock and drops the result has silently eaten the expiries, which is why the value must be used.

- **`ApiError::OrderAlreadyExpired`** (`rustrade-execution`): the state conflict a cancel hits when
  its order's own deadline retired it first. Distinct from `OrderAlreadyCancelled` because nobody
  asked for it, which is what a caller reconciling its local state needs to know. `ApiError` is
  `#[non_exhaustive]`, so this is additive for downstream matches.

- **The simulated venue rejects a `TimeInForce` a market order cannot keep** (`rustrade-execution`).
  ⚠️ **Behaviour change**: `AtOpen`, `AtClose` and `GoodUntilCancelled { post_only: true }` on an
  `OrderKind::Market` order were accepted and filled on arrival; they are now rejected. The first two
  say *when* an order executes rather than how long it works — this library's own IBKR client turns
  them into real market-on-close and limit-on-close orders — so filling one immediately is a wrong
  fill rather than an ignored flag. The third is a promise never to take liquidity, which is all a
  market order does. Every `TimeInForce` that merely bounds how long an order works is still
  accepted on a market order, which fills in full on arrival and so honours all of them.

- **`MockExchange` emits everything the venue produces through one ordered queue**
  (`rustrade-execution`). Expiries swept when the clock advances, and the balance a cancel releases,
  now go through the same drain that already ordered filled opens, rather than around it. A
  `SimulatedVenue` balance is an absolute restatement, so an event overtaking a batch still waiting
  out its latency leaves the client holding the wrong balance, not merely a late one. The cancel
  path was previously dropping its events entirely.

- **The simulated venue accepts `OrderKind::Limit`, rests what the market has not reached, and
  matches it against its own view** (`rustrade-execution`, `rustrade`). A limit order that is
  already marketable fills on arrival; one that is not rests on the book and fills when a later
  market update crosses it. `SimulatedVenue::apply_market` now returns the account events those
  fills produced — `[balance, trade, order]` per filled order, trade before the terminal snapshot —
  and is `#[must_use]`, so a driver that applies a market and drops the result is a compiler
  warning rather than a silently eaten fill.

  **Two pricing rules, and they are not the same rule.** A marketable-on-arrival limit is the
  aggressor: it is priced by the `FillModel` and then *clamped* to its own limit, and charged
  `Liquidity::Taker`. A resting order is the passive side: it fills at **its own limit price
  exactly**, never reaching the `FillModel`, and is charged `Liquidity::Maker`. Deriving a resting
  fill's price from the book would credit price improvement no maker can obtain — a maker is paid
  the price it quoted, and improvement accrues to whoever crossed it. The two agree exactly where
  the market meets the limit, and diverge only where the order genuinely is, or is not, marketable.

  **Market-order pricing does not move.** A market order is still priced from the snapshot its own
  request carried, so the committed tear sheet is byte-identical and this release's result-changing
  surface is confined to orders that could not be placed before.

  Marketability is judged on `best_ask`/`best_bid`, falling back to `last_price` when the feed
  supplies no book at all — with a trades-only feed there is no book, so a book-only rule would
  never match anything.

  **Known limitations**, stated on `SimulatedVenue`: no queue position, no size cap and therefore no
  partial fills, so a crossing order fills its whole quantity at one price.

- **`VenueRegime`, and a second `SimulatedVenue` constructor** (`rustrade-execution`).
  `SimulatedVenue::new` keeps today's behaviour and rejects limit orders;
  `SimulatedVenue::new_market_driven` marks a venue whose driver feeds `apply_market`, and only that
  regime accepts them. `SimExecutionBuilder` uses it; `MockExchange` does not.

  The gate is the point. Without it, a limit order placed through a driver with no market feed would
  be accepted and then rest forever with nothing that could ever match it —
  accepted-and-silently-never-filled, which is worse than a flat rejection naming the reason.

- **`SimulatedVenue::cancel_order` is real** (`rustrade-execution`). It takes a resting order off the
  book, releases what was held against it, restates the balance, and returns
  `Cancelled { filled_quantity }`. A cancel that finds no resting order says *which* nothing it
  found: `ApiError::OrderAlreadyFullyFilled` when the fill won the race — which a non-zero
  `to_venue` latency makes reachable, and which tells the caller to reconcile rather than retry —
  `ApiError::OrderAlreadyCancelled` for a second cancel, and a rejection naming the id only for one
  the venue never booked.

- **`AccountState::release`** (`rustrade-execution`): returns a reserved amount to `free` without
  moving `total`, which is what a cancelled order does to the balance held against it.

- **A simulated venue holds its own view of each instrument it trades** (`rustrade-execution`,
  `rustrade`). `SimulatedVenue::apply_market` records a `MarketSnapshot` and the instant it was
  observed, readable through `SimulatedVenue::market`; `SimRunner` routes every source market event
  to the venues trading that instrument **before** returning it to the `Engine`.

  Routing before is the whole ordering rule: an order the `Engine` sends in reaction to the tick at
  `T` must be matched against the market as of `T`, not `T-1`. Returning first and routing on the
  next poll would reverse that at zero latency.

  This is the substrate resting orders need; `apply_market` now also matches them, and returns the
  fills it caused. **No fill price moves for a market order**: it is still priced from the snapshot
  its own request carried, and the committed tear sheet is byte-identical. An order that rests is
  the thing that cannot be priced that way, because the market it must be matched against has not
  happened when the request is made.

  `MockExchange` has no market feed, so a venue driven by it reports `market` as `None` forever.
  That difference is documented as a property of the type rather than left as an accident of wiring.

- **`VenueMarketUpdate`** (`rustrade`), implemented for `DataKind`: how a stream of market events
  becomes the venue's view. Stateful by necessity — a trade carries no book, an L1 update no trade
  price — so it names an associated `State` rather than being a plain conversion.

  The shipped implementation **is** `DefaultInstrumentMarketData`, delegating to the same
  `Processor` and `market_snapshot` the engine calls, so the venue's market and the engine's agree
  by construction rather than by two derivations happening to match. `tests/test_sim_venue_market_agreement.rs`
  pins it over the full 50,000-event fixture, comparing after every event.

  This matters more than it reads: `last_price` is **not** the last trade price. With both sides of
  an L1 book present it is the volume-weighted mid — the microprice. A venue that re-derived it as
  "the most recent trade" would diverge by a median of 1.5–1.7x the *half-spread*, exceeding the
  half-spread around 60% of the time on the committed fixture.

- **Balance reservations for resting orders** (`rustrade-execution`). A resting order holds
  **exactly** what its fill will settle — computed once, at the order's own limit and its own
  liquidity side — rather than a conservative over-estimate. A conservative reservation would have
  to be released and re-debited on the fill, producing two or three balance restatements for one
  fill and reporting balances the account never held, while `Balance::used` misreported for the
  order's whole life.

  A resting order seeded by a configured `initial_state` is the one exception: the venue never took
  that balance, so it holds nothing against it and releases nothing when it is cancelled. Such an
  order still matches, and its fill debits the ledger then.

- **`SimRunner` — backtests are now driven by a deterministic discrete-event simulator**
  (`rustrade`). A `Stream<Item = EngineEvent>` that merges the time-ordered market/auxiliary source
  with the account events its own `SimulatedVenue`s produce, and is polled **inline** by the
  `Engine`'s own task. There is deliberately no channel and no forwarding task between the feed and
  the engine: the `Engine` sends its execution requests synchronously inside `process`, so by the
  time `process` returns they are already queued on this runner's receivers, and the next poll books
  them and schedules what they produce — all before the next market event is drawn.

  The ordering within one simulated instant is now a stated contract rather than a scheduling
  accident: auxiliary events (0) lead, then account events (1), then market data (2). Account before
  market is the rule the type exists for — a fill stamped at `T` must reach the `Engine` before the
  market event at `T` marks the resulting position to that price. Within one instant and class,
  delivery follows booking order, so "the `Engine` has the response" implies "the `Engine` has
  already seen every account event for that order". Simulated latency is applied to the queue key
  and never slept on, so a run's wall-clock duration does not scale with the latency being modelled.

- **`SimExecutionBuilder` / `SimExecutionBuild`** (`rustrade`), the deterministic counterpart to
  `ExecutionBuilder`. The difference is what each does with a venue's request receiver:
  `ExecutionBuilder` hands it to an `ExecutionManager` on its own task, which is what made response
  timing a function of the tokio scheduler, while this builder keeps it for `SimRunner` to drain
  inline. Nothing is spawned, connected or awaited — `build` is synchronous.

- **`BarterError::SimFeedbackLoop`, `SimRunner::with_feedback_limit` and `DEFAULT_FEEDBACK_LIMIT`**
  (`rustrade`). A strategy that opens an order in response to its own fill, against a venue whose
  simulated round trip is **zero**, is a zero-delay feedback cycle: the response is stamped at the
  very instant of the request that provoked it, so it outranks every later source event, simulated
  time never advances and the market source is never drawn again. No discrete-event simulator can
  resolve that by scheduling alone — there is no instant to place the response at that is both after
  its cause and before the next input. Such a run is now abandoned after
  `DEFAULT_FEEDBACK_LIMIT` (10,000) account events with no intervening source event, and reports the
  venue and the instant time stopped at. The asynchronous path does not report this; it masks the
  cycle by racing an unpaced market stream ahead of the engine, which is the non-determinism below.

- **`MarketSnapshot`, and `RequestOpen::market` to carry it — a simulated venue can now price a
  market order** (`rustrade-execution`). `MockExchange` keeps no book of its own and a market order
  carries no limit price, so it could not price a fill at all and rejected every one: no backtest
  against it could ever fill anything. `RequestOpen` gains `market: Option<MarketSnapshot>`
  (`best_bid`, `best_ask`, `last_price`, each optional), which the `Engine` stamps at the instant it
  emits the request. Sampling at the *engine* rather than at the venue is deliberate and not an
  implementation detail: a venue holding its own market-data tee would drain the unbounded, unpaced
  backtest market channel ahead of the `Engine` and fill at end-of-history prices, which is
  unbounded look-ahead. The emit instant is the one point with a well-defined position on the
  simulated timeline. The `Engine` always overwrites the field when it sends, so a strategy that
  clones an old request cannot smuggle a stale price into a fill. **Live venues must ignore it** —
  they have a real book, and the field describes what the *engine* saw when it decided. It is
  `#[serde(default)]`, so requests serialised before it existed still load. `None` (no snapshot
  supplied) and an empty snapshot are reported as different rejections on purpose: the first is a
  wiring bug in the caller, the second a cold start or a thin instrument, and the fixes differ.
  (#279)

- **`InstrumentDataState::market_snapshot`** (`rustrade`), which supplies the above. It is
  **defaulted** to `MarketSnapshot::from_last_price(self.price())`, so it is purely additive for
  existing custom implementors; `DefaultInstrumentMarketData` overrides it to carry its L1 best bid
  and ask as well as the last traded price.

- **`MarketSnapshotSource`** (`rustrade`), with a blanket implementation on `EngineState`, resolving
  an instrument's snapshot at each point the `Engine` emits an order request.

- **`ExecutionRequest::Drain`** (`rustrade`), the graceful counterpart to `ExecutionRequest::Shutdown`.
  `Shutdown` abandons whatever is in flight — what a live stop wants, and what `System::shutdown`
  still does — while `Drain` finishes the in-flight requests, forwards the account events they
  produced, and only then closes the manager's channel. These were previously the same message, so
  a drained backtest shutdown and an abrupt live one could not be told apart at the manager.
  *Note:* `ExecutionRequest` is not `#[non_exhaustive]`, so downstream exhaustive `match`es need a
  new arm.

- **`AFTER_DRAIN_DEADLINE`** (`rustrade`), bounding how long `System::shutdown_after_backtest` waits
  for the account feed to drain. A backtest finishes well inside it; it exists so that calling that
  method against a *live* system — whose AccountStream reconnects indefinitely and so never ends —
  fails loudly instead of hanging.

- **Order-rejection counters on the trading summary** (`rustrade`). `TearSheet` gains
  `orders_opened`, `orders_rejected` and `first_rejection_reason`; `TradingSummary` gains the
  session totals and a `rejected_every_order()` helper. Without these a session that *could not*
  trade reads exactly like one that *chose not to* — every ratio in the sheet is computed over zero
  fills either way — and the exchange's reason was recorded nowhere, so diagnosing an empty session
  meant re-running it with logging enabled. `print_summary` now prints a banner above the tables
  when every open was rejected. All fields are `#[serde(default)]`, so sheets serialised before
  they existed still load.

- **`InstrumentKind::Cfd` — contracts-for-difference are now modelled** (`rustrade-instrument`).
  CFDs previously had no correct representation: `Spot` would pollute every downstream `Spot`
  filter — including the corporate-action and option-settlement scans, which mean specifically
  *the deliverable equity* — with instruments that are never deliverable, while `Future` requires
  an expiry a CFD does not have, and a fabricated expiry is not inert (it becomes a
  subscription-binding key and drives contract-expiry settlement). The variant carries a
  `CfdContract { contract_size, settlement_asset }` rather than being a unit variant, because both
  fields are load-bearing: `contract_size` feeds fee computation, unrealised PnL and risk notional
  (real CFDs are commonly per-point multipliers, so a unit variant would hard-code `Decimal::ONE`
  into the money path), and `IndexedInstrumentsBuilder` registers a settlement asset only for kinds
  that report one, so a CFD reporting `None` could never have its account currency indexed. The
  market-data twin `MarketDataInstrumentKind::Cfd` is a unit variant — it has no expiry or strike
  to bind on — and is kept distinct from `Spot` because one connector can serve both a spot
  instrument and a CFD on the same `(exchange, base, quote)`, where folding them would make
  subscription binding resolve to whichever iterated first.
  *Note:* neither enum is `#[non_exhaustive]`, so downstream exhaustive `match`es need a new arm.

- **London Strategic Edge exchange identifiers** (`rustrade-instrument`): `LseFx`, `LseCrypto`,
  `LseEquities`, `LseFutures` and `LseCfd`, one per dataset family, so `MarketEvent.exchange`
  carries provenance and each dataset declares its own support. Appended at the end of `ExchangeId`
  deliberately: the enum derives `Ord` from declaration order and `IndexedInstrumentsBuilder`
  sorts by it, so inserting mid-enum renumbers `ExchangeIndex` and `InstrumentIndex` for existing
  configurations — indices that are serialized into engine state, the audit replica and backtest
  replay streams. **Instrument indices are stable across releases only while variants are
  appended.**

- **London Strategic Edge bulk export** (`rustrade-data`, `lse` feature): submit, poll and download
  the provider's asynchronous export jobs. This is the **only** path to the raw tick tape — neither
  REST nor WebSocket reaches it. Downloads resume via `Range`, verify the job's SHA-256, and rename
  atomically. On a verification failure the destination is left absent and what happens to the
  partial file follows from *which* check failed: one **shorter** than the artifact is a valid
  prefix, so it is kept and a repeated call resumes from it, which for a multi-gigabyte artifact is
  the difference between finishing and starting over. One that is **longer**, or that is already as
  long as the artifact will ever get and still fails the digest, cannot be repaired by fetching
  more, so it is discarded and a repeated call restarts — keeping it would fail identically forever.
  `LseError::IntegrityMismatch` reports which happened via its `discarded` field. A resumed
  transfer is checked at the seam: a `206` is accepted only when its `Content-Range` begins at the
  requested byte (the job's `bytes`/`sha256` are both optional, so they cannot be relied on to catch
  a mis-ranged response), and a `416` is read as "the partial file already holds the whole artifact"
  rather than as a failure, so a run interrupted between the final write and the rename converges
  instead of re-requesting an unsatisfiable range forever.
  **⚠️ The export allowance is five per hour and a *rejected* submit still consumes one**, so an
  export request validates everything checkable before anything is sent: unknown resolutions,
  candle resolutions against the provider's tick-only dataset classes, blank symbols, and inverted
  ranges are all rejected client-side. In particular `symbol: "all"` is **not** a request for every
  symbol — it is a literal that matches nothing, and an export naming it returns a valid but
  **empty** Parquet artifact with no error, so it is rejected outright. Measured on both the candle
  and the tick path, and omitting the symbol is a hard error, so **every artifact this provider
  produces is single-symbol**: combining instruments means merging several files. (The rejection is
  case-sensitive — `ALL` is Allstate's real ticker.) An exhausted allowance is
  reported as `LseError::QuotaExceeded` carrying the allowance position, distinct from the
  per-minute `RateLimited`. Range `end` is **exclusive**, and the range is date-granular by type.
  **⚠️ Exported data is not redistributable** — see <https://londonstrategicedge.com/terms>.

- **London Strategic Edge export decoder** (`rustrade-data`, new `lse-parquet` feature, off by
  default): decode a downloaded export artifact into an iterator of `MarketEvent`s. The Parquet
  dependency is behind its own feature, so consumers who only want files on disk pay nothing for
  it. **The event type is decided by the columns present, not by the caller**, because the
  provider's tick schema varies by dataset: `bid`+`ask` and `price`+`ask` both decode to
  `OrderBookL1` (`price` *is* the bid — the provider's own price endpoint returns `price == bid`
  exactly, on every symbol tested), while `price`+`volume` with no ask decodes to a trade. An
  unrecognised schema is a typed error rather than a mis-decode, and so is a recognised column of
  the wrong type: the resolved layout's columns are type-checked up front (`LseError::
  UnsupportedColumnType`), which is the only place a `ts` that is *not* UTC-adjusted is
  distinguishable — read as epoch microseconds, a local-time column shifts every event by the
  venue's offset with nothing downstream able to notice. A schema that is not flat is rejected
  rather than mis-mapped, since columns are located by leaf index and one nested group shifts every
  index after it. Decoding runs on Parquet's column-reader API in bounded batches rather than its
  record API: the record API allocated a `Vec` of fields, a `String` per column *name* and a
  `String` for the dictionary-encoded symbol on **every row** — a large fraction of decode time in
  local profiling, though no in-tree benchmark pins the figure — and read
  a whole row group at a time, which would have made the streaming source's bounded-memory
  contract depend on how the provider chose to write the file. A candle's `time_exchange` is its
  derived exclusive `close_time`, not the artifact's open-time `ts`, matching the candle replay
  path — stamping the open would let a strategy act on a completed bar at the instant its period
  began. Ascending timestamps are enforced by the decoder, since the streaming backtest source
  delegates that obligation rather than checking it; the comparison permits ties, which are the
  common case on an equity tape. `lse::market::instrument_index_for` derives the `InstrumentIndex`
  from the registry the engine was built with, so a fabricated or typo'd index is unrepresentable
  for callers who use it — `read_export` takes the index on trust and cannot check it itself — and
  every row's symbol is checked against the descriptor. The iterator **ends at its first error**: the
  symbol and ordering checks are verdicts on the whole file rather than per-row conditions, so
  continuing would hand a caller who discards errors a silently truncated view of an artifact
  already proven corrupt.
  **⚠️ Known properties of the data, not of this decoder, that will silently mislead:** FX candles
  are **bid** candles — reconciled against the tick tape, OHLC matched the bid series on 1421 of
  1421 minutes and the mid or ask on none, so a backtest filling at the candle close fills at the
  bid, favourable by a full spread on every buy. Candle `volume` is **not dependable**: a majority
  of sampled one-minute equity bars report `0` in minutes the tick tape shows real trades, and a
  daily series carried a contiguous band roughly 2,000× too large; a literal `0` is passed through
  faithfully as `Some(0)` rather than rewritten to `None`, which would be inventing a fact.
  Non-trading days are emitted as **flat** `o == h == l == c` bars rather than omitted, so daily
  series are not sparse and a backtest sees a tradeable price on a closed market. And a decoded
  trade may not be a print — that layout carries no ask, so a quote is not constructible, but the
  price is likely a bid-side observation.
  **⚠️ Decoded data is not redistributable** — see <https://londonstrategicedge.com/terms>.

- **London Strategic Edge symbology** (`rustrade-data`, new `lse` feature, off by default):
  dataset → `(ExchangeId, MarketDataInstrumentKind)` mapping, display-symbol → `(base, quote)`
  resolution, and a fallible dataset-slug helper. **⚠️ London (`.L`) listings are quoted in pence**,
  and the provider reports no unit for them, so they quote in **GBX** — an asset distinct from GBP,
  with prices passed through unscaled. Quoting them in GBP would inflate notional, fees, unrealised
  PnL and every balance by 100×, silently. `.A`/`.B` are US share classes rather than venue
  suffixes. Slug derivation is `symbol -> Result<_, _>` rather than a string transformation because
  the mapping is not injective — thirteen futures symbols resolve to a slug shared with a different
  series, and the provider answers `200` for it.
  **⚠️ Licensing:** this crate is MIT-licensed; **the data this integration retrieves is not
  redistributable**. London Strategic Edge permits use for your own research, trading and model
  training, including commercially, but prohibits redistributing, reselling or otherwise making the
  data available to third parties in any form. See <https://londonstrategicedge.com/terms>.

- **London Strategic Edge historical candles** (`rustrade-data`, `lse` feature): an authenticated
  vault REST client (`LseVaultClient`, `x-api-key`, or `from_env` on `LSE_API_KEY`, whose errors
  name the variable and never its value, so a mis-encoded key cannot reach a log line) with a paged
  `fetch_candles` stream and a `collect_candles` convenience. Candles are keyed on the **display
  symbol** (`EUR/USD`, `AAPL`, `ES.F`), not a dataset slug. The provider serves 14 of
  `CandleInterval`'s variants — it publishes no `2h`/`6h`/`8h`/`12h`/`3d`, and spells one month
  `1mo` rather than the shared enum's `1M` — and an unserved resolution is rejected before the
  request is sent rather than relayed as a `400`.
  `fetch_candles` follows the same range contract as the crate's other historical fetches: candles
  whose exclusive `close_time` falls in `[start, end]`, both inclusive, matched on `close_time`.
  This required mapping from the vault's own range, which is expressed on the bar's **open** time
  with an **exclusive** upper bound. Several provider behaviours are handled that would otherwise
  be silent: the resolution parameter is `timeframe` (the vault ignores unknown parameters and
  defaults to 1-minute bars, returning a byte-identical shape, so a misspelling yields the wrong
  resolution with a `200`); the reported timestamp is the bar's open, so `close_time` is derived
  through the shared boundary helper rather than passed through; and the 5,000-row cap is applied
  with no envelope, cursor or marker, so pagination continues to an empty page rather than treating
  a short page as terminal. Each page is scanned in full rather than stopping at the first bar past
  the upper bound — ascending rows are what the vault serves, not what it guarantees, and stopping
  early would drop any in-range bar sitting behind an out-of-range one, ending the stream `Ok` on a
  silently truncated series. Sparseness differs by resolution: **intraday**, zero-activity periods are
  absent rather than gap-filled (unlike Binance's REST klines), while **daily** is not sparse and
  emits non-trading days as *flat* bars (`open == high == low == close`), so "no bar" must not be read
  as "the market was closed". FX candles report `volume: None` — the vault omits the field, and a
  synthetic zero would aggregate into a legitimate-looking total at every derived resolution;
  `trade_count` is `None` for every dataset, as the vault reports none.

- **London Strategic Edge allowance reporting** (`rustrade-data`, `lse` feature): `QuotaStatus` and
  `LseVaultClient::usage()`. Unlike every other provider here, London Strategic Edge meters
  streaming **and** bulk export against a single shared allowance, so a consumer doing both must
  budget against one pool. The type mirrors the provider's response, which is multi-dimensional
  (bytes per month, bytes per week, exports per hour, plus static request-shaping limits) and
  carries **no reset timestamp** — none is synthesised, because a plausible invented instant would
  be worse than an absent one. Consistent with this crate's separation of concerns, the allowance
  is *reported, never acted on*: nothing retries, sleeps or throttles on the caller's behalf, and a
  `429` surfaces as a terminal `LseError::RateLimited` carrying `Retry-After` when present. Pacing
  between pages is proactive courtesy only, defaulting to the provider's documented rate and
  overridable via `with_pace` (including `Duration::ZERO` to disable).
  **⚠️ Licensing:** as above — the retrieved data is **not redistributable**, whatever this crate's
  own licence says. See <https://londonstrategicedge.com/terms>.

- **`MarketDataStreamed` — a lazily streamed `BacktestMarketData` source** (`rustrade`,
  `backtest::market_data`). The counterpart to `MarketDataInMemory` for datasets that cannot be
  resident: a multi-gigabyte export, a compressed tick archive, a paginated provider fetch. It is
  parameterised over a caller-supplied stream **factory**, which is where every source-specific
  concern lives — opening files, decoding, resolving instrument keys, merging sources — so the
  engine crate stays ignorant of file formats, compression and providers, and no provider feature
  leaks into it. The first event's timestamp is resolved once at construction and cached, satisfying
  the trait's coherence obligation without letting `time_first_event` consume the cursor `stream`
  needs. **Cost model, documented on the type:** the factory runs once per `stream()` call, so
  `run_backtests` with N configurations performs 1 + N full source reads — the deliberate price of
  O(1) memory, and the wrong trade against a metered network source.

- **`merge_time_sorted` and `tag_events` — replay N historical sources as one feed**
  (`rustrade-data`, `streams::merge`). Historical data arrives one instrument or one file at a time,
  while a backtest harness exposes exactly one stream. These lazily k-way merge N time-sorted market
  streams into one, holding at most one buffered event per input, so memory is O(N) in the number of
  inputs and O(1) in the size of the dataset. Nothing is emitted until every input has either
  buffered an event or ended — an input that has not yet produced might be about to yield something
  earlier, and an out-of-order stream is undetectable downstream. Ties resolve to the earliest-listed
  input, so a given input ordering replays identically every time. Provider-agnostic: usable for
  Databento, Binance and any other historical source.

- **London Strategic Edge multi-instrument candle replay** (`rustrade-data`, `lse` feature):
  `replay_candles` and `LseCandleSource`. Turns N per-symbol vault fetches into one time-ordered
  `MarketStreamEvent` stream, each event tagged with the caller's own `InstrumentIndex` and
  `ExchangeId` — the bridge between the per-symbol historical API and an engine that consumes a
  single feed. `time_exchange` is the candle's `close_time`, never its open, since a completed bar
  entering the timeline at the instant its period began is lookahead. A failed fetch on any source
  surfaces immediately rather than silently shortening the replay. Paired with `MarketDataStreamed`,
  this is a runnable multi-instrument backtest over a range far larger than memory
  (`engine_backtest_with_lse_candles`, `--features lse`).
  **⚠️ Licensing:** as above — the retrieved data is **not redistributable**, whatever this crate's
  own licence says. See <https://londonstrategicedge.com/terms>.

- **London Strategic Edge live market data** (`rustrade-data`, `lse` feature): a WebSocket connector
  per dataset family — `LseFx`, `LseCrypto`, `LseEquities`, `LseFutures`, `LseCfd` — each serving
  both `PublicTrades` and `OrderBooksL1`. The provider publishes exactly **one** data frame, a
  price with a bid, an ask and a size, so the two kinds are two decodings of the same tick rather
  than two channels; the subscription kind decides what an event becomes, not what is asked for on
  the wire. There is **no candle channel at all** — this provider's candles are a REST-only product,
  so no `Candles` support is declared. Authentication is a *message* rather than a header and
  subscriptions are accepted only after the server answers it, so the integration ships its own
  `LseSubscriber` (`LseCredentials`, or `from_env` on `LSE_API_KEY`, whose errors name the variable
  and never its value). Timestamps arrive in three encodings — RFC-3339 with `T`, the same with a
  space separator, and a bare float epoch on replayed ticks — and a decoder accepting only the first
  would silently drop five of the thirteen dataset families. Every spelling is quantised to
  **microseconds**, the provider's own resolution and the one its replay filter honours: an
  RFC-3339 string carrying seven fractional digits otherwise decodes to 100 ns while the float
  spelling of the same instant cannot, and the resume arithmetic compares instants for equality.
  Symbols are decoded as owned strings rather than borrowed slices: every symbol on this feed
  contains a `/`, RFC 8259 permits spelling that `\/`, and a borrowed decode cannot unescape one.
  **Pre-subscribe guards, because this surface fails quietly.** An unknown symbol is *confirmed*
  rather than rejected and then never ticks, and the connection-wide 16-subscription cap is reported
  without naming the symbol that breached it. So the whole batch is validated against the symbol
  list the authentication response already carries, before anything is sent: an unoffered symbol or
  an over-cap batch fails loudly and consumes **no** subscription slot. The response's per-symbol
  category is cross-checked only when it is both present *and* a label some dataset is known by,
  since it is absent for roughly half the catalog: neither a missing field nor a label this
  integration does not recognise is a contradiction — only one belonging to a *different* dataset
  is evidence of anything. A rejection raised *after* the handshake completes — a later
  `LIMIT_REACHED`, an `INVALID_START` on a resumed symbol, a credential that expired mid-connection
  — is decoded on the stream and logged rather than discarded: the subscription validator has
  stopped reading by then, and because the provider names no symbol in a rejection, the only other
  sign it ever gave would be a subscription that quietly stopped ticking.
  **Opt-in resumption across a reconnect** via `LseSubscriber::with_resume(Arc<LseResumeState>)`.
  A reconnect re-runs the subscribe, so a fixed replay window would re-deliver it in full on every
  attempt; instead the watermark records, per symbol, the newest delivered instant **and how many
  events already delivered carry it**, and the resumed subscribe skips exactly that prefix. Both
  halves are load-bearing: `start` is inclusive and filters at microsecond resolution, so resuming
  one tick later would silently drop every event at that instant not yet consumed, while flooring it
  to a coarser resolution would re-serve a whole millisecond of already-delivered events on the
  families that carry microseconds. It is **off by default** — a resumed subscription is served its
  entire historical window before a single live tick, and one hour of a busy crypto symbol replayed
  107,395 ticks. Watermarks are keyed by **dataset, symbol and subscription kind**, because two
  axes collapse onto the provider's wire identifier: both kinds are decodings of one data frame, and
  the five dataset connectors share one endpoint and one identifier construction — so `AAPL` as
  trades and as top-of-book, or `AAPL` on `LseEquities` and on `LseFutures`, all resolve to
  `tick|AAPL`. Sharing a watermark across either axis lets whichever stream ran ahead set the resume
  point for the one behind, which then resumes past events it never delivered. Both are therefore
  closed structurally, and one state serves any set of streams differing in any of the three. What
  no key can separate is two streams duplicating all three, so **each duplicated
  `(dataset, symbol, kind)` triple needs its own state** — the one obligation the caller carries,
  and it is documented on `LseResumeState`.
  The provider retains 24 hours and clamps a longer request **silently**; the honoured window is
  announced in a `replay_started` frame that was measured to arrive *after* the subscription
  confirmation, so the comparison against what was asked for is made on the stream rather than
  during the handshake, and a clamp is warned about rather than swallowed. This covers reconnects,
  not process restarts: recovering across a restart needs consumer-side acknowledgement of what was
  durably stored.
  **⚠️ Known properties of the feed, not of this decoder, that will silently mislead:** the tick is
  a **QUOTE, not a print** — its `price` equalled its `bid` on 3,966 of 3,966 sampled ticks across
  every dataset family — so a `PublicTrade` decoded from it is a bid-side quote wearing a trade's
  shape and is not evidence a transaction occurred; it carries no trade identifier and no aggressor
  side, and neither is inferable. `volume` is **genuine on `LseCrypto` and `LseEquities`** (summing
  it reproduces the provider's own one-minute candle volume to ratio `1.000`) and a **hard-coded
  `1.0` on `LseFx` and `LseCfd`**, with no in-band signal separating them — a plausible-looking
  placeholder that aggregates into a legitimate-looking total, making volume-weighted prices and
  size filters on those two families meaningless rather than imprecise. Identical consecutive ticks
  are **genuine and are never de-duplicated**: barely a third of a sampled run was unique on
  `(ts, price, bid, ask, volume)`, yet removing the repeats destroyed volume that otherwise
  reconciles exactly, so a test pins that both are emitted. Both `OrderBookL1` levels carry a
  **zero size** — the feed publishes prices only — which prices correctly, since the volume-weighted
  mid falls back to the plain mid, but must not be read as available quantity. A zero **price** is
  the opposite case and decodes to `None` rather than to a level: the provider publishes `0.0` for a
  side it holds no quote for, and it is exactly the fallback that makes the zero *sizes* harmless
  which makes a zero price dangerous — `mid_price` requires only that both sides be `Some` and then
  averages whatever prices they carry, so a real ask of `42001` against a published zero bid would
  price the instrument at `21000.5`, indistinguishably from a genuine quote.
  Runnable end to end: `lse_market_data` (`rustrade-data`) streams the raw feed, and
  `engine_sync_with_lse_market_data_and_mock_execution` (`rustrade`) prices engine instruments from
  it while routing orders to a different, mocked venue — this provider offers no execution, so a
  `DataVenue` is how a live system uses it.
  **⚠️ Licensing:** as above — the retrieved data is **not redistributable**, whatever this crate's
  own licence says. See <https://londonstrategicedge.com/terms>.

- **`CandleInterval` gains the sub-minute resolutions `Sec5`, `Sec15` and `Sec30`**
  (`rustrade-data`, `subscription::candle`). `CandleInterval` is the venue-agnostic *union* of
  every resolution any connector serves, and providers exist that publish `5s`/`15s`/`30s` bars
  natively; the enum previously jumped straight from `Sec1` to `Min1`, so those resolutions were
  inexpressible. `ALL`, `as_str`/`FromStr` (`"5s"`/`"15s"`/`"30s"`, and therefore `Display` and
  the serde impls), and `to_step` all cover the new variants. Because the union is a superset of
  any one venue's menu, each connector's interval guard was re-reviewed: Databento (whose OHLCV
  schemas are `1s`/`1m`/`1h`/`1d` only) and Hyperliquid (whose `candleSnapshot` menu starts at
  `1m`) reject all three with the existing `DataError::UnsupportedInterval`. Binance publishes no
  `5s`/`15s`/`30s` kline either, and its channel-name mapping is infallible by the `Identifier`
  contract, so the new `binance::supports_candle_interval` is the pre-flight gate;
  `exchange_supports_instrument_kind_sub_kind` now consults it, checking `SubKind::Candles`
  support **per interval** rather than treating every resolution alike.
  *Note:* `CandleInterval` is not `#[non_exhaustive]`, so a downstream exhaustive `match` on it
  must gain arms for the three new variants.

- **`aggregate_candles` candle→candle OHLCV aggregation helper** (`rustrade-data`,
  `subscription::candle`). A pure, venue-agnostic batch primitive that rolls fixed-interval
  `Candle`s up into a coarser fixed interval (e.g. Binance-native `1s` bars → `3s` bars no venue
  serves), with epoch-anchored bucketing, `Decimal`-exact OHLCV/trade-count aggregation, and the
  bucket `close_time` derived through the shared `close_time_from_open` boundary helper. Empty
  buckets are omitted (gap-fill stays a consumer policy, composing correctly on either side of the
  call) and invalid arguments or non-monotonic input surface as the new `#[non_exhaustive]`
  `AggregateCandlesError` — never a silently wrong bar.

- **Bounded, cycle-safe pagination for Alpaca REST fetches** (`rustrade-data`, `alpaca` feature).
  Every `page_token`-paginated Alpaca fetch (`fetch_splits_raw`, `fetch_contracts`,
  `fetch_snapshots` / `fetch_chain_snapshots`) previously stopped at its page cap with only a
  `warn!`, returning a silently truncated result — indistinguishable from a genuinely small one
  for a market-data client. Each fetch now fails loudly instead, mirroring the Massive pagination
  hardening: exceeding the page cap or receiving a `next_page_token` that repeats an already-used
  cursor surfaces as a terminal error on the fetch. Two new `AlpacaRestError` variants carry the
  diagnosis: `PaginationLimitExceeded { pages, limit }` and `CyclicPagination { page_token }`
  (the retained token bounded to a diagnostic prefix, as it is server-supplied).
  `AlpacaRestError` is `#[non_exhaustive]`, so the new variants are additive. Also new:
  `AlpacaRestClient::with_base_urls` / `AlpacaOptionsClient::with_base_urls` to point a client at
  API-compatible non-production endpoints (mock servers in tests, proxies).

- **Richer IBKR Flex transport diagnostics + configurable poll budget** (`rustrade-data`, `ibkr`
  feature). `IbkrFlexClient` now reads the HTTP response body **before** branching on status, so a
  non-2xx response's diagnostic body (IBKR/proxy/CDN error pages) is preserved instead of being
  discarded by `error_for_status`. A non-success status whose body is not a recognizable Flex
  envelope surfaces as the new `IbkrFlexError::HttpStatus { status, body }` — the body bounded and
  **token-scrubbed** (a proxy that echoes the request URL cannot leak the `t=` Flex token); an IBKR
  application error that arrives under a non-2xx status still surfaces as the richer
  `IbkrFlexError::Flex`. As a side effect this also fixes a retry-path bypass: a `1019` ("statement
  still generating") returned under a non-2xx status is now honored as retryable rather than aborting
  the poll. Poll timing is now configurable via the new `FlexPollPolicy { initial_delay, interval,
  max_attempts }` (set with `IbkrFlexClient::with_poll_policy`); a short `initial_delay` before the
  first poll (5 s by default) keeps the near-certain first `1019` from consuming one of the bounded
  attempts. `IbkrFlexError` is `#[non_exhaustive]`, so the new variant is additive.

- **Bounded, cycle-safe pagination for Massive REST fetches** (`rustrade-data`). Every Massive
  `next_url`-paginated fetch (`fetch_aggregates`, `fetch_trades`, `fetch_quotes`, `fetch_tickers`,
  `fetch_dividends`, `fetch_splits_raw`, `fetch_option_contracts`, `fetch_option_chain_snapshot`)
  now caps the number of pages it will follow and detects a `next_url` that revisits an
  already-fetched page, yielding a terminal error instead of paginating without bound. Because a
  silently truncated result is indistinguishable from a genuinely small one for a market-data
  client, incomplete pagination fails loudly rather than returning a partial result. Three new
  `MassiveError` variants carry the diagnosis: `PaginationLimitExceeded { pages, limit }`,
  `CyclicPagination { url }` and `PaginationUrlTooLong { len, limit, prefix }`. `CyclicPagination`
  and `UntrustedNextUrl` store their URL in full but bound it when rendered via `Display`, so an
  oversized server-supplied URL cannot flood a log line; a bounded render ends in `...` so a cut is
  never mistaken for the whole URL.

- **Corporate-action stock-split processing** (`rustrade`). The engine now handles
  `EngineEvent::CorporateAction` for stock/reverse splits, adjusting every open position on the
  target Spot instrument (the same per-position rescale as `Position::apply_split`) and emitting
  observables — `SplitRemainder`
  (cash-in-lieu of the fractional sliver disposed under `SplitRoundingPolicy::Floor`),
  `OpenOrdersAtSplit` (resting orders are reported, never engine-cancelled),
  `UnsupportedCorporateAction`, and `CorporateActionAlreadyProcessed`. Application is idempotent
  per-instrument via a caller-assigned action `id` — a re-submitted `id` is a non-mutating no-op that
  emits the observable `CorporateActionAlreadyProcessed` (distinct from the retryable
  `UnsupportedCorporateAction` rejections, so an audit-stream consumer can tell an idempotent skip
  from a successful split with nothing to adjust); a reverse split that floors a position to zero
  quantity closes it with a `PositionExit`. `Position::apply_split` takes a validated `SplitRatio`
  and is fallible (`Result<SplitResult, SplitError>`, with a companion `Position::validate_split`);
  the engine **pre-computes** every affected position rescale and option-strike division before
  committing any (a two-phase prepare/commit, single-sourced across the live handler and the audit
  replica), so an arithmetically-unrepresentable ratio, an option strike that would overflow on
  division, or a corrupted (non-integer) option contract count rejects the whole action atomically —
  emitting `UnsupportedCorporateAction` with the new
  `UnsupportedCorporateActionReason::ArithmeticOverflow` / `PositionStateInvalid` reasons, leaving the
  `id` unrecorded and nothing partially mutated. The eager post-split `pnl_unrealised` recompute
  degrades gracefully: an extreme last price that would overflow `Decimal` zeroes the (derived,
  self-correcting) value and flags `SplitResult::pnl_unrealised_overflowed` rather than panicking
  part-way through the adjustment, preserving that atomicity.
- **Corporate-action handling for option positions on a splitting underlying** (`rustrade`,
  `rustrade-instrument`). When a split targets an underlying equity, the engine now also handles open
  option positions on that underlying. A **standard** split (a whole-number forward split, per the OCC
  option-adjustment rules) adjusts each option position **in place** — strike ÷ ratio, contract count
  × ratio, deliverable/multiplier unchanged — emitting one new `EngineOutput::OptionPositionAdjustedForSplit`
  per adjusted position, **plus an `OpenOrdersAtSplit` for the option's own resting orders**
  (now stale-priced; reported, never engine-cancelled, exactly as on the equity path — surfaced for
  **held and unheld** options alike, since an unheld option can carry a working order to open a
  position that the split silently re-strikes). A **non-standard**
  split (every reverse split, every fractional forward split) requires a new contract identity the
  engine does not register at runtime, so it emits the new `EngineOutput::OptionPositionsRequireIdentityChange`
  and leaves the options at their pre-split terms; the underlying equity split is still applied and its
  `id` recorded (so, unlike `UnsupportedCorporateAction`, it is **not** retryable — the wrapper closes
  the listed options and/or trades a pre-declared new identity). New `CorporateActionKind::split_kind`
  method (`rustrade-instrument`) returns `Option<SplitAdjustmentKind>` (`Standard`/`NonStandard`),
  classifying the action per the OCC rule.
- **Backtest auxiliary-event injection seam** (`rustrade`). New `AuxEventSource` trait, `NoAuxEvents`
  (negligible-overhead default), and `AuxEventsInMemory` interleave non-market `EngineEvent`s (e.g. corporate
  actions, contract expiries) with the market stream in simulated-time order during a backtest — the
  backtest equivalent of live trading's direct `EngineEvent` injection. The harness pre-merges the
  two sources into one time-ordered stream before the engine feed, so an injected event lands at the
  correct point in the timeline (aux events win ties).
- **Corporate-action PULL sourcing abstraction** (`rustrade-instrument`, `rustrade-integration`,
  `rustrade-data`). New `StockSplitSource` trait + `CorporateActionFilter` (`rustrade-integration`,
  behind the new `corporate-action` feature, on by default) model fetching splits by symbol +
  effective-date range; they yield the new generic `CorporateAction<K>` descriptor
  (`rustrade-instrument`), keyed by an unresolved provider symbol (`SmolStr`) at the source boundary.
  Both `CorporateActionFilter` and `CorporateAction<K>` are `#[non_exhaustive]`, so adding a field is
  non-breaking for downstream code that only reads or matches them (matches must use `..`). They are
  constructed via the derived `::new`, whose arity grows with each field — so a new field is still a
  breaking change for direct `::new` callers (`CorporateActionFilter` also derives `Default`, making
  `Default::default()` + per-field assignment its forward-compatible construction path). `#[non_exhaustive]`
  shields only Rust-code matching/construction: `CorporateAction<K>` also derives serde `Deserialize`,
  so adding a field without `#[serde(default)]` is still a breaking change at the data layer (it fails
  to deserialize payloads written before the field existed).
  A shared `CorporateActionKind::stock_split(split_to, split_from)` helper computes the ratio
  identically across providers as a validated `SplitRatio` newtype (strictly `> 0`, making a
  degenerate ratio unconstructible; build it via `SplitRatio::new` → `Option` or
  `TryFrom<Decimal>` → `InvalidSplitRatio`, with transparent, validated serde). A reference implementation for the Massive REST client is feature-
  gated behind `massive` in `rustrade-data`. The action *kind* is encoded in the trait name (a
  `DividendSource` sibling is the future path) rather than a unified trait with a kind filter; push /
  account-scoped sources (e.g. IBKR) are intentionally out of this PULL trait. A runnable example
  (`rustrade`, `corporate_action_sourcing`, `--features massive`/`--features alpaca`) shows
  source → resolve → inject.
- **`rustrade` crate-root re-exports for the corporate-action API.** `CorporateActionKind`,
  `SplitRatio`, `InvalidSplitRatio`, `SplitAdjustmentKind`, and `split_effective_instant` are now
  re-exported from `rustrade` (alongside the existing `SplitRoundingPolicy`), and `SplitRatio` gains
  an infallible `From<SplitRatio> for Decimal` conversion — so callers can build and handle
  `EngineEvent::CorporateAction` without a direct `rustrade_instrument` dependency.
- **Alpaca `StockSplitSource` implementation** (`rustrade-data`, feature-gated `alpaca`). A second
  reference source: `impl StockSplitSource for AlpacaRestClient` wraps Alpaca's
  `GET /v1beta1/corporate-actions` endpoint (available on the free/Basic plan), with a new
  `CorporateActionsQuery` builder, the nested `forward_splits`/`reverse_splits` response shape, a
  normalised `AlpacaStockSplit`, and automatic `page_token` pagination. `effective_date` maps onto
  Alpaca's `ex_date` (the market-execution / price re-basing date), deliberately **not**
  `payable_date` — which can precede `ex_date` for forward splits and would apply the split early
  (pinned by a provenance test). New shared `AlpacaRestClient` / `AlpacaRestError` transport (auth +
  rate-limit retry) factored out of the options client so every Alpaca REST surface shares one
  client; the existing `AlpacaOptionsError` is now a type alias of the (`#[non_exhaustive]`)
  `AlpacaRestError`, which gains an `InvalidCredential` variant (see the Changed entry below for the
  error-variant change this implies for the options client).
- **IBKR Flex Web Service corporate-action reconciliation** (`rustrade-data`, feature-gated `ibkr`).
  A new `rustrade_data::exchange::ibkr::flex` surface fetches an account's *Activity* Flex statement
  over HTTPS (`IbkrFlexClient` / `IbkrFlexConfig`, env vars `IBKR_FLEX_TOKEN` / `IBKR_FLEX_QUERY_ID`)
  via the 2-call SendRequest → poll → GetStatement flow, and parses the Corporate Actions section
  into faithful raw `IbkrFlexCorporateAction` records (`IbkrReorgType` enum + a standalone
  `parse_corporate_actions` XML parser). This is a broker-confirmed **reconciliation / audit** source
  — account-scoped and post-hoc — **not** a `StockSplitSource`: it derives no split ratio (the raw
  `principal_adjust_factor` is surfaced but is a TIPS field, not a ratio), leaving ratio
  derivation/verification and reconcile policy to the caller. XML is parsed with the new `quick-xml`
  dependency (pulled in only by the `ibkr` feature). The Flex `token` is passed as a `t=` URL query
  parameter, so transport errors strip the request URL before it reaches `IbkrFlexError::Http`
  (`reqwest`'s `Error: Display` would otherwise embed the full URL, leaking the token into logs); the
  variant is documented to never carry the URL. The HTTP client also enforces HTTPS-only transport
  and rejects redirects (`https_only(true)` + `redirect::Policy::none()`), so the token cannot leak
  via an HTTPS→HTTP downgrade redirect. `IbkrFlexConfig::new` / `from_env` trim both credentials and
  reject an empty (or whitespace-only) value up front (`IbkrFlexError::InvalidCredential`), so a
  malformed token fails observably at construction rather than as an opaque IBKR `1003` "invalid
  token" at fetch time. A runnable example
  (`ibkr_flex_corporate_actions`, `--features ibkr`) sketches the wrapper-side reconcile.

- **BREAKING**: **A distinct market-data venue on `Instrument`** (`rustrade-instrument`,
  `rustrade-data`, `rustrade`). `Instrument` gains `data_venue: Option<DataVenue<ExchangeKey>>`, so
  one instrument can be priced on one venue and executed on another — the single `exchange` field
  could not express that at all, which made live trading on a third-party price feed structurally
  impossible. Modelling it as two separate instruments is not equivalent: position and
  `pnl_unrealised` are strictly `InstrumentIndex`-scoped, so the traded instrument would never
  receive the price updates landing on the other index, and its PnL would sit permanently stale.
  `DataVenue` carries the exchange **and** an optional `name_exchange`, because two venues routinely
  spell one instrument differently — a venue-only field would subscribe under the execution venue's
  symbol and silently receive nothing, or another instrument's prices. Read it through the new
  `Instrument::data_exchange()` / `data_name_exchange()` accessors, which apply the
  fall-back-to-the-execution-venue rule in one place, and set it with `with_data_venue()`.
  `IndexedInstrumentsBuilder` now registers both venues into `exchanges` — but deliberately **not**
  into `assets`, since a data-only venue holds no balances and registering it would seed balance
  state for a venue that can never report one — and market-data subscription batching groups by
  `data_exchange()`. Migration: `Instrument::new` / `Instrument::spot` are unchanged and set the
  field to `None`, so only struct literals and destructuring patterns need touching. The same field
  is added to `system::config::InstrumentConfig`, which has no constructor to shield literals from
  it — it is `#[serde(default)]`, so configs written before it existed still deserialise. The field
  is declared **last** deliberately: `Instrument` derives `Ord` and `IndexedInstrumentsBuilder` sorts
  before assigning `InstrumentIndex`, so appending renumbers no existing configuration.
  *Known limitation:* `index_market_data_subscription_batches` resolves both the assets and the
  instrument against the **execution** venue, so it cannot honour a `DataVenue`. Naming the data
  venue fails loudly (`IndexError`), since that venue holds no registered assets. Naming the
  execution venue **succeeds** — the returned `InstrumentIndex` is correct, but the subscription it
  is attached to names the wrong feed, and the function has no way to tell that the instrument is
  priced elsewhere. Its rustdoc states both cases, and a test pins each.

- **`VenueRole { DataOnly, ExecutionOnly, Both }`** (`rustrade`), naming which connection dimensions
  a venue provides, alongside a `ConnectivityState::new(role)` constructor. See the **BREAKING**
  Changed entry on `ConnectivityState` gaining the field for why it exists and how to migrate.

- **`EngineStateBuilder::execution_venues`** (`rustrade`) declares which venues have a registered
  execution client, so only those are given an account connection to wait on. `SystemBuilder` calls
  it automatically, and `backtest` reconciles the roles itself (see the entry below); provide it by
  hand when building an `EngineState` for an engine you drive directly.
  See the Fixed entry on the account connection dimension for what it corrects.

- **`connectivity::reconcile_venue_roles`** (`rustrade`) re-derives every tracked venue's
  `VenueRole` from the venues that have a registered execution client, leaving connection `Health`
  and the positional `ExchangeIndex` order untouched. For callers that must build an `EngineState`
  before their execution clients exist — `backtest` builds one set of clients per run from a state
  supplied once — this corrects the roles after the fact instead of obliging the caller to declare
  venues it cannot yet know. Its one caller obligation — that `instruments` is the collection
  `states` was built from — is `assert!`ed by comparing the venue keys in `ExchangeIndex`
  order. **This panics in release builds too**, deliberately: the failure it guards is silent, in
  the same way `assert_aux_events_sorted`'s is. Every venue would otherwise be handed a plausible
  but wrong role, `global` is re-derived from those roles, and a strategy gated on global health
  then trades against a link that never came up. A pair holding the *same* venues with different
  instruments is not
  detectable this way, but also misaligns every `InstrumentIndex` in the engine, so venue roles are
  not the symptom that surfaces first.

- **`ExecutionClient::SUPPORTED_KINDS`** (`rustrade-execution`, `rustrade`), a required associated
  const listing the `InstrumentKindDiscriminant`s a client can trade, alongside the existing
  `const EXCHANGE`. `rustrade-execution` had no kind guard whatsoever, so an instrument of a kind a
  venue cannot encode reached that venue as a silent wrong-symbol order. `ExecutionBuilder::add_execution`
  now rejects the build naming the offending client and kind. The new
  `InstrumentKindDiscriminant { Spot, Perpetual, Future, Option, Cfd }` in `rustrade-instrument`
  (plus `InstrumentKind::discriminant()`) exists because `InstrumentKind` carries data on four of its
  five variants and so cannot itself be declared against; `MarketDataInstrumentKind` is deliberately
  not reused, as it answers a different question. Declared support: Binance spot and margin `Spot`;
  Hyperliquid perpetual `Perpetual` and spot `Spot`; Alpaca `Spot, Option`; IBKR `Spot, Future,
  Option`; mock `Spot, Cfd`. IBKR omits `Cfd` deliberately — the client builds no security type for
  them, so a CFD could only ever reach that venue as some other contract. Scope: the guard is
  checked against `Instrument::kind`, so it catches a kind a client can never represent; it does
  **not** validate a client's own symbol registry against the instrument model — IBKR's
  `ContractConfig.security_type`, for one, stays caller-supplied and unchecked.
  **BREAKING** for external `ExecutionClient` implementors: the const has no default.

- **`ConnectivityDimension` and `UntrackedExchange`** (`rustrade`), the reported detail when a market
  or account event names an exchange the engine does not track — see the Fixed entry below.

- **`UnsupportedCorporateActionReason::AmbiguousSplitTarget`** (`rustrade`), rejecting a stock split
  whose target is not the unique deliverable instrument on its `(base, quote, exchange)` — see the
  Fixed entry below. Appended last to the (`#[non_exhaustive]`) enum, which derives `Ord`.

- **`SimulatedVenue` and `VenueOutcome`** (`rustrade-execution`), the simulated venue's state
  machine split out from the async exchange that drives it — see the Changed entry below.

- **`OrderKey::into_owned_instrument` and `OrderEvent::into_owned_instrument`**
  (`rustrade-execution`), cloning a borrowed instrument key into an owned one. An index that maps an
  `InstrumentIndex` back to an exchange-native name hands out a borrow of the name it owns, so
  anything that stores that key, sends it to another task, or hands it to a venue has to rebuild it
  field by field. `rustrade-execution` already carried one private copy of that rebuild; a second
  driver for the simulated venue would have needed another.

### Changed

- **BREAKING: `Trade` carries the order's cumulative filled quantity** (`rustrade-execution`). A
  fill states two things — that an execution happened, and what it left the order at — and a
  consumer that tracks order state needs both. Until now only the first crossed the API boundary,
  so an order could be advanced only by a separate order snapshot. Where a venue sends no such
  message, or sends one the consumer never receives, the order stood still while its position moved.

  `Trade` gains `order_filled_quantity: Option<Decimal>`, placed after `quantity` — which remains
  the size of *this* execution. `Trade::new` therefore takes one more argument. The field is
  `#[serde(default)]`, so trades serialised before it existed still deserialise, as `None`.

  `InstrumentState::update_from_trade` now advances the order the fill was against to that figure,
  retiring it once nothing remains. Three properties make this safe to drive from a fill stream:

  - **Idempotent.** The order advances to `max(current, reported)` and is never incremented, so a
    re-delivered fill, or two arriving out of order, cannot double-count. Accumulating each
    execution's size would.
  - **Cannot resurrect.** It only ever updates an order already tracked as `Open` with that exact
    exchange id. A fill arriving after its order retired is inert — unlike an order snapshot, which
    reaches an arm that inserts, and which is why that path needs a liveness gate at the client.
  - **Nothing is fabricated.** A venue that does not report a cumulative sends `None`, and the order
    is left for a snapshot or a reconciliation fetch. `None` is not a claim that nothing is filled.

  `Option` is the point rather than an accommodation: whether a venue tells you this is now a visible
  fact in the data instead of an invisible difference between code paths. Reported by Binance Spot
  and Margin over WebSocket (`z`), by Alpaca over WebSocket (`order.filled_qty`) and by Interactive
  Brokers (`cumulative_quantity`, previously discarded); not by Hyperliquid's `userFills`, nor by
  either venue's REST trade history. The simulated venue reports it, so a backtest and a live
  session now agree about what is still working.

  Migration: pass `None` at any call site building a `Trade` that is not a venue-reported execution
  — the engine's own position-flip and expiry-settlement trades do exactly that.

- **Account-event deduplication moved to `client::dedup`** (`rustrade-execution`), shared by the
  Binance and Hyperliquid clients instead of living inside `client::binance::shared`. Crate-internal
  only — nothing public moved, and Binance call sites are unchanged via a re-export. The cache's
  documentation is now venue-neutral, and says why the key carries the instrument: venue ids are
  routinely per-symbol rather than global.

- **`SimulatedVenue` mints trade ids from a sequence of their own** (`rustrade-execution`). They
  were derived from the order's id, which was unique only while one order produced at most one
  fill — no longer true now that an order can fill in part on arrival and again when its remainder
  is crossed, where the two trades would have collided. `Trade::order_id` is still how a trade says
  which order it belongs to.

  `TradeId` also gains the documentation it never had, and a `Display` impl for parity with the
  other id types. Note what it now states: **uniqueness is the venue's to define and is often
  narrower than global** (Binance's are per symbol), and the derived `Ord` is lexicographic byte
  order carrying no chronological meaning — order trades by `Trade::time_exchange`.

- **`AccountState::debit_filled` is replaced by `AccountState::commit`** (`rustrade-execution`),
  taking a `Debit` that names the asset once and splits an arrival into what it settled and what it
  holds. **Breaking.** One arrival is now one fallible ledger operation and one balance
  restatement, including the arrival that both settles a fill and holds against a resting
  remainder. Settling and then reserving as two calls could commit the first and be refused the
  second, leaving the venue's ledger permanently short with no event to say so; asking for the whole
  requirement up front makes that state unreachable rather than merely avoided.


- **A market-driven simulated venue prices a market order from its own book**
  (`rustrade-execution`, `rustrade`). ⚠️ **Behaviour change, and it moves backtest results at a
  non-zero `latency_ms`.** A `SimulatedVenue` in `VenueRegime::MarketDriven` — the regime `SimRunner`
  drives — now prices every fill from the market its driver feeds it, as a live venue prices from
  its own book. It previously used `RequestOpen::market`, the snapshot the sender stamped at
  decision time.

  Combined with booking each request at the instant it arrives, this is what makes `to_venue`
  price-relevant: an order pays the market it reaches, not the market it was decided against. While
  a market order was priced from its request, `latency_ms` changed only *when* a result was
  delivered and never *what* it was, so a backtest could raise it to any value and report identical
  fills. `RequestOpen::market` is now decision-time provenance alone on this path, which is what it
  is documented to be, and the gap between it and the fill is implementation shortfall — a quantity
  that was identically zero while the two were the same snapshot.

  `VenueRegime::RequestPriced` is unchanged and still prices from the request, so `MockExchange` and
  `MockExecution` results do not move. **Nor does anything at `latency_ms: 0`**, where the two
  snapshots are the same instant's.

  A `MarketDriven` venue that has never been fed an instrument now **rejects** a market order in it
  as unpriceable rather than falling back to the requester's snapshot. Falling back would make the
  fill depend on which of the two happened to hold a price.

  **The committed tear sheet moved by one number**: closing USDT on the 40-order fixture, by
  0.0029 on 1312.42 spent — 0.022bp, the price drift over one 50ms outbound leg. Nothing else in the
  artifact changed, because the fixture opens positions and never closes them, so no realised PnL
  or return statistic depends on the fill price.


- **BREAKING**: **`FillModel::fill_price` takes a single `FillContext`, and a fill model no longer
  sees the order's limit price** (`rustrade-execution`).

  ```rust
  // before
  fn fill_price(&self, side, order_price, best_bid, best_ask, last_price) -> Option<Decimal>;
  // after
  fn fill_price(&self, fill: &FillContext<'_>) -> Option<Decimal>;
  ```

  Two problems went with the old signature. Three prices of one type in a fixed order make a
  transposition a silent mispricing rather than a compile error, and every increase in the
  simulated venue's fidelity — sizes at the touch, depth, a queue position — would have had to
  arrive as yet another argument, breaking every implementation again. **`FillContext` is
  `#[non_exhaustive]`**, so this is the last such break: an implementation only reads it, and all
  of those can arrive as added fields.

  The limit price is gone rather than moved. A limit constrains the *result*, not the pricing: a
  model reads the book and answers where a taker prints, and bounding that by the order's own terms
  is the venue's job. Letting the model see the limit is how two of the three shipped models came
  to return the limit *itself* for a marketable order — a buy limit of 51,000 arriving against an
  ask of 50,000 filled at 51,000, a worse price than a market order got under the same
  configuration — while `MidpointFillModel` returned the midpoint even where that sat above a buy's
  limit. The trait's own docs acknowledged the latter and pushed the obligation onto the caller.
  Removing the parameter makes that class of error unwritable instead of merely documented.

  The venue also used to fold the order's limit into the `last_price` argument, handing the model a
  market that reported the order's own price as a trade that happened. It now passes its snapshot
  as it holds it.

  **No behaviour changes on any path reachable today.** A simulated venue accepts only
  `OrderKind::Market`, which carries no price by construction, so every model was already being
  called with `order_price: None` — the branches deleted here were unreachable. The committed tear
  sheet is byte-identical.

  Migration is mechanical: `fill_price(side, _, market)` becomes `fill_price(fill)`, reading
  `fill.side` and `fill.market`.

- **BREAKING: a simulated venue's open orders are held in price-time priority**
  (`rustrade-execution`). `AccountState`'s open orders move from a
  `FnvHashMap<ClientOrderId, _>` to `OpenOrders`, which keeps the map for lookup by id and adds a
  per-instrument, per-side queue ordered by `(price, arrival, insertion)`.
  `AccountState::orders_open` keeps its iterator signature; `AccountState::new` now takes
  `OpenOrders`, and `AccountState::orders`/`orders_mut` expose it.

  `OpenOrders::insert` takes the `Reservation` held against the order — `None` only for one the
  venue did not book itself — and `OpenOrders::remove` hands both back as a `RestingOrder`. The
  reservation is a field of the order rather than a parallel map because the two have exactly one
  lifetime: an order leaving the book takes its reservation with it, and a reservation outliving its
  order is held against nothing.

  A tick that crosses two resting orders with only enough balance to fill one fills whichever comes
  first. Read off a hash map that is an arbitrary choice, which a rebuild or a different
  `ClientOrderId` can silently reverse — so two runs of one backtest need not agree. Price-time
  priority is both the reproducible rule and the one real venues use. It has to be the structure
  rather than a sort at the call site, or matching sorts every open order on every tick.

  Insertion sequence is part of the key, not decoration: two orders at one price and one instant
  otherwise compare equal, and a `BTreeMap` would keep only one — an order vanishing from the book
  rather than merely being mis-ranked.

  An open order with no limit price is kept for reporting but never queued. An order with no price
  to wait at has nothing to wait for; it can only arrive through a configured `initial_state`.

  Nothing rests yet, so no result moves and the committed tear sheet is byte-identical.

- **BREAKING: `SimRunner` and `backtest` require `MarketKind: VenueMarketUpdate`**
  (`rustrade`). Satisfied by `DataKind`, so no in-tree caller changes. A custom market event kind
  needs one small implementation — see `VenueMarketUpdate`, and prefer delegating to the engine's
  own `InstrumentDataState` over re-deriving prices.

- **BREAKING: `FeeModel::compute_fee` takes a `Liquidity`** (`rustrade-execution`), naming whether
  the fill made or took liquidity. `PercentageFeeModel` gains an optional `maker_rate`, and
  `PercentageFeeModel::new` / `::maker_taker` replace literal construction.

  Venues price the two sides differently, often by a wide margin, and a maker rebate is the whole
  economics of quoting. Without the flag every fill is charged the taker rate — which
  systematically overstates the cost of exactly the strategies that rest orders to earn the maker
  side, and does it silently. `Liquidity::Taker` is the default and the conservative direction: it
  overstates cost rather than inventing profit.

  **Existing behaviour and configuration are unchanged.** `maker_rate` defaults to absent, in which
  case `rate` is charged on both sides, and a config written before the field existed deserialises
  and prices identically. Both current call sites pass `Liquidity::Taker`: every order the
  simulated venue accepts is marketable on arrival, and `InstrumentState` has no maker/taker
  information to read, since a `Trade` does not carry one. A maker rebate is a negative
  `maker_rate`.

- **A simulated venue's open orders keep their arrival stamps** (`rustrade-execution`).
  `AccountState::update_time_exchange` rewrote `Open::time_exchange` on every open order each time
  the venue's clock advanced. Only balances are restated now.

  An order does not become a different order because time passed, and the stamp is the instant the
  venue accepted it. Rewriting it was invisible only because nothing rests: every order fills on
  arrival, so the orders it could reach were those an `initial_state` seeded — which it moved to
  whenever the clock last ticked, and the further the run got, the wronger they were. It was also
  O(open orders) on every event.

- **The simulated venue's ledger models a reserved balance** (`rustrade-execution`). `free` is what
  an order may draw on, `total` is what the account holds, and the difference is held against
  something — the split `Balance` has always described and this ledger could not previously
  represent.

  An `initial_state` copied from a live account with margin reserved is now usable as configured.
  It was formerly refused outright with `SimulatedVenue cannot model a reserved balance`, because a
  ledger in which every order fills on arrival had no way to express an amount held back, and the
  fill path wrote `free` and `total` from one number — so carrying the configuration would have
  erased the reserved portion silently. `AccountState` gains `reserve`, `settle` and `debit_filled`,
  and the two hand-rolled balance arms in `open_order` collapse onto them.

  **Reported results are unchanged**, which is checked rather than asserted: a market order fills on
  arrival, so it reserves and settles in one step and emits exactly **one** balance restatement, as
  before. A balance is an absolute restatement rather than a delta, so emitting the intermediate
  state would have reported a balance the account never held. The committed tear-sheet artifact in
  `rustrade/tests/data/` is byte-identical across the change.

- **`quick-xml` 0.41 → 0.42** (`rustrade-data`, `ibkr` feature). The bump's headline break is that
  `QName<'a>` now wraps `&'a str` rather than `&'a [u8]`, with `AsRef<str>` replacing
  `AsRef<[u8]>`. Our exposure is a single line: `root_element_name` in the IBKR Flex parser no
  longer wraps the element name in `String::from_utf8_lossy`. **No behaviour change** — that reader
  is built with `Reader::from_str`, so its input was already guaranteed UTF-8 and the lossy
  conversion could never have substituted a replacement character. The two `quick_xml::de::from_str`
  call sites, which do the bulk of Flex parsing, are untouched.

  The bump adds and removes no dependency: the lockfile delta is the version line alone, and
  `quick-xml`'s own `[dependencies]` section is byte-identical across the two releases. We continue
  to take `features = ["serialize"]` only, so `encoding`/`encoding_rs` — the non-UTF-8 decoding path
  this release reworks — is still not compiled. `#![forbid(unsafe_code)]` remains crate-wide. The
  tier-2 re-review this crate's entry mandates is recorded in `.github/tier2-dependencies.txt`.

- **`binance-sdk` 69.1.0 → 69.2.3** (`rustrade-execution`, `binance` feature). No code change: this
  bump needs none, unlike 60.0.0 → 69.1.0. The three `binance_sdk::common` internals the margin
  user-data stream couples to were re-verified before merge, and the first two hold by construction
  — `common/websocket.rs` is byte-identical to 69.1.0, so `WebsocketApi::send_message`, the
  `WebsocketMessageSendOptions` signing fields and `WebsocketEventEmitter::subscribe`'s sequential
  event drain are unchanged. `common::utils::send_request` keeps its 7-argument shape and still
  injects `timestamp`, signs with the URL-encoded `get_signature`, appends `signature` and sets
  `X-MBX-APIKEY`, so the hand-rolled `userListenToken` POST is still signed as intended.

  One behaviour change is worth knowing even though it does not reach us: `build_websocket_api_message`
  now signs a **signed** WebSocket frame over the plain payload (`get_signature_unencoded`) rather
  than the URL-encoded one. Binance's WS API signs the plain payload, so this is a fix — but every
  `send_message` call here passes `WebsocketMessageSendOptions::new()`, i.e. unsigned with no API
  key, because the listen token is the sole auth on those frames. The signed branch is unreachable
  from this crate today; 69.2.3 is simply the first version in which it would be correct.

  The new `stocks` feature, and the `wss://nbstream.binance.com/equity` host that comes with it, are
  not enabled — we take `spot` and `margin_trading` only.

- **`ibapi` 3.3.0 → 4.0.1** (`rustrade-data`, `rustrade-execution`, `ibkr` feature). A major upgrade
  with four consumer-visible changes. **The outbound order wire format is unchanged and provably so**
  — ibapi's entire order encoder (`src/orders/common/encoders.rs`) is byte-identical across the two
  versions. All of the 4.0 order-side churn is on the decode and routing side.

- **BREAKING: `IbkrHistoricalData::fetch_option_chain` takes `exchange: Option<&str>`**
  (`rustrade-data`), replacing `&str` where `""` meant "all exchanges". The parameter is TWS's
  `fut_fop_exchange`, a *futures*-options filter: for any underlying that is not a future, pass
  `None` and TWS returns one entry per listing exchange. Naming a routing exchange — `Some("SMART")`
  included — filters the result to zero rows, which the old signature made easy to do by accident.
  `None` is now omitted from the wire entirely rather than sent as an empty string; TWS treats the
  two identically. Migration: pass `None` where you passed `""`.

- **Historical IBKR tick sizes are decimal, and a tick with no size is dropped** (`rustrade-data`).
  ibapi 4.0 types `TickLast::size` and `TickBidAsk::size_bid`/`size_ask` as `Option<f64>` instead of
  `i32`, because IBKR models sizes as decimals on the wire and the old integer parse silently
  truncated them — a crypto tick of `0.5` decoded as `0`. Fractional sizes now survive into
  `PublicTrade::amount` and `OrderBookL1` levels. `None` means TWS sent no value at all, which is
  distinct from a real `Some(0.0)`; since neither type can encode "size unknown", such a tick is
  dropped with a `warn!` rather than fabricated as zero. **Also note**: the synthetic id in
  `PublicTrade::id` is derived from the tick's size, so the same logical tick now hashes to a
  different id than it did under ibapi 3.x.

- **An unrecognised IBKR order status is reported live rather than rejected** (`rustrade-execution`).
  ibapi 4.0 preserves a status string it does not model as `OrderStatusKind::Unknown(raw)` instead of
  failing the whole subscription with `Error::Parse` — which previously could take down the
  long-lived order-update stream the moment IBKR shipped a new status. rustrade treats that variant
  as active/`Open` and logs it at `warn!` with the raw string. That keeps the order-id mapping alive
  so the account stream can resolve the order's real state; treating it as a rejection would drop the
  mapping for an order that may well be live in the market and discard its executions and
  commissions. An order that is in fact dead is reaped by the existing stale-mapping sweep. During
  placement the same status yields "held/pending" rather than falling through to the placement
  timeout, which on the bracket-order path would have cancelled all three legs.

- **Behaviour change to know about, with no compile-time signal**: ibapi 4.0 classifies a code-399
  order message carrying a `Warning:` line as a warning, and routes a warning owned by a
  request-bound subscription as a non-terminal notice — which `iter_data()`/`timeout_iter_data()`
  drop. A held-until-RTH 399 therefore no longer closes a `place_order` subscription; it is dropped,
  the subscription stays open, and the following `OrderStatus(PreSubmitted)` now arrives there
  instead of only on the account stream. The 399 form *without* a `Warning:` line still surfaces as
  an error and is still treated as "held/pending". Both paths are handled. IBKR integration tests
  require a live TWS gateway and are `#[ignore]`d, so this bump has **not** been exercised against a
  real gateway.

- **The balance assertions in `SimulatedVenue` now panic on the engine's thread** (`rustrade`).
  Nothing about when they fire has changed — an unfunded quote asset was always a panic — but with
  no task between the venue and the engine, the panic surfaces on the caller's thread rather than
  inside a spawned task whose `JoinError` the caller had to interpret. `cfd_panics_when_the_quote_asset_is_unfunded`
  pins the message, which is unchanged.

- **The simulated venue's state machine is split from the async exchange that drives it**
  (`rustrade-execution`). `MockExchange` was one type doing two jobs: it owned the ledger, the
  pricing and the fill accounting, *and* the channels, the spawned tasks and the latency sleeps.
  Everything in the first group moves to a new `SimulatedVenue` — synchronous, transport-free and
  latency-free — which `MockExchange` now holds as `pub venue: SimulatedVenue` alongside
  `latency_ms`, `request_rx` and `event_tx`. What a backtest observes is unchanged; what changes is
  that the venue can be driven by something other than a `tokio` task without reimplementing it, and
  that a request's **ordering obligation is carried by the venue's return type** rather than by the
  comments on one driver's queue.

  `SimulatedVenue::open_order` and `cancel_order` return
  `VenueOutcome<Response> { events: Vec<UnindexedAccountEvent>, response: Response }`, whose rustdoc
  states that obligation: every event must reach the client **before** the response, and in the
  order given. A balance here is an absolute restatement rather than a delta — successive fills
  report `9_999_500`, `9_999_000`, `9_998_500` — so a driver that delivers two out of order does not
  merely reorder history, it leaves the client holding the wrong number. The guarantee is the
  venue's to state; enforcing it stays each driver's to keep.

  **This is not a pure refactor, and it breaks the public API:**
  - `MockExchange`'s `exchange`, `fee_model`, `fill_model`, `instruments` and `account` fields moved
    onto the venue — `exchange.account` becomes `exchange.venue.account`. `order_sequence` and
    `time_exchange_latest` are **no longer public**: resetting the sequence mints duplicate
    `OrderId`s and `TradeId` is derived from it, so the duplicate would reach the trade ledger.
    Read them through `SimulatedVenue::order_sequence` and `SimulatedVenue::time_exchange`; the
    clock advances only through `SimulatedVenue::advance_time`, which takes the instant the caller's
    own latency model produced. `instruments` stays public — a consumer mutating it is a supported
    bypass, and the rustdoc says so.
  - `MockExchange::open_order`, `cancel_order`, `account_snapshot`, `time_exchange`,
    `validate_order_kind_supported` and `find_instrument_data` are gone; call them on `venue`. They
    are deliberately **not** forwarded, and `MockExchange` deliberately does **not** `Deref` to its
    venue: with either, `mock_exchange.open_order(..)` would still compile and book a fill whose
    events never enter the emission queue — silently reintroducing the out-of-order delivery fixed
    under #294 below, by way of a typo.
  - `open_order` takes **only** the request. It previously also took the market snapshot as a
    separate argument while `RequestOpen::market` already carried one, so a single path held two
    copies that could disagree with nothing to arbitrate. Set the snapshot on the request.
  - `OpenOrderNotifications` is no longer public: it was `open_order`'s second return value, and
    what it described is now `VenueOutcome::events`.
  - `cancel_order` is replaced rather than moved. Its body was `unimplemented!()` — the only one in
    either crate — and it returned a type its own request channel cannot accept: a seven-field
    `Order<.., Result<Cancelled, _>>` where `MockExchangeRequestKind::CancelOrder` wants the
    two-field `UnindexedOrderResponseCancel`. It could not answer the channel it existed for. The
    venue now returns a correctly typed rejection: only Market orders are accepted and they fill on
    arrival, so no order ever rests to be cancelled, and that is a property of the venue rather than
    of any transport carrying it. A driver forwards the rejection instead of dropping the caller's
    `oneshot`.
  - The venue acknowledges its own fill. `ack_trade` previously ran in the driver, so the ledger and
    the events describing it were written on opposite sides of the seam and a driver could update
    one without the other. A later request on the venue now sees the trade regardless of when its
    events are delivered.
  - The venue's rejection reasons and its unfunded-balance panic now name `SimulatedVenue` rather
    than `MockExchange`, because that is the type that produces them and a second driver will have
    no `MockExchange` anywhere in the picture. Code matching on those strings needs updating; the
    `ExecutionBuilder` panic that screens instrument kinds is unrelated and unchanged.

  Groundwork for [#289](https://github.com/Niqnil/rustrade/issues/289) and Stage 3 of
  [#279](https://github.com/Niqnil/rustrade/issues/279): a deterministic backtest driver needs to
  decide *when* a fill's events land relative to market events, which it can only do if something
  other than a spawned task can ask the venue what those events are.

- **`HistoricalClock` no longer mixes wall-clock time into simulated time** (`rustrade`).
  `time()` returned the most recent event's `time_exchange` *plus the real time elapsed since that
  event was processed*; it now returns that timestamp verbatim. The interpolation existed so the
  clock would not appear frozen on a sparse feed — a presentation property bought at the cost of
  reproducibility, because this clock is not merely read for display. It is closed into the
  simulated exchange client, which calls it on every request, and the resulting instant stamps
  `Filled::time_exchange`, `Trade::time_exchange` and `AssetBalance::time_exchange`; it also seeds
  `TradingSummary::time_engine_start`/`time_engine_end`, the denominator of every annualised
  statistic. A fill's timestamp was therefore `last_event.time_exchange + wall_clock_elapsed +
  latency_ms / 2`, and it fed back — `process` advances the clock off `trade.time_exchange`, so the
  drift ratcheted simulated time forward and produced the out-of-order-event warnings the clock
  logs about itself. Measured on a three-event backtest, 50 runs previously produced **50 distinct**
  sets of terminal timestamps; they now produce **one**. Backtests run concurrently
  (`run_backtests` joins them), so sibling runs were perturbing each other's results.

  **Behaviour change to know about:** between events the clock does not advance. A strategy that
  reads `time()` twice without an intervening event sees one instant, and a sparse feed leaves it
  standing still for as long as the data does. That is the correct reading of simulated time — no
  simulated time passes where no data does — but code that measures elapsed real time by
  differencing `time()` will now measure the dataset instead. `LiveClock` is unchanged and remains
  the right clock for live trading. No serialised format changes: the affected field was private
  and `HistoricalClock` derives only `Debug`/`Clone`.

  This removes wall-clock dependence from the *timestamps* in a backtest. It does not make a
  backtest reproducible on its own — where a fill lands among market events is still decided by
  task scheduling ([#289](https://github.com/Niqnil/rustrade/issues/289)), which is why the same
  fixture still varies between three and four orders opened. (#289)

- **A drained shutdown now ends from the execution side rather than on request quiescence**
  (`rustrade`). `Shutdown::AfterDrain` previously terminated the `Engine` as soon as nothing was
  `OpenInFlight` or `CancelInFlight`. That signal is wrong: an order's *response* is what clears it
  from flight, but the `Trade` and the balance that the fill actually consists of are delivered
  separately and may not have been read yet. `Shutdown::AfterDrain` now only marks the `Engine`
  draining and signals every `ExecutionManager`; each manager finishes what it owes, forwards the
  account events those produced, and only then closes its channel, which ends the `Engine`'s feed
  and with it the run. `System::shutdown_after_backtest` correspondingly **awaits**
  `account_to_engine` instead of aborting it. `Shutdown::Immediate`, which is what live trading
  uses, is unchanged. (#281)

- **`ExecutionManager` owns its AccountStream; `init` no longer returns it separately**
  (`rustrade`). `init` returned the AccountStream merged with the response channel, which ended the
  combined stream as soon as *either* side finished and left the manager unable to sequence the
  two — it did not hold the thing it had to drain. It now returns a single `Stream` carrying
  everything the manager emits, responses and account events alike, which ends exactly when the
  manager has finished. `ExecutionManager` gains an `account_stream` field, and its `RequestStream`
  and `Client` parameters gain a `'static` bound (both already had to satisfy it in practice, since
  the manager is spawned as a task). Its `Debug` implementation is now hand-written rather than
  derived, because a boxed `Stream` is not `Debug`.

- **`MockExchange::open_order` takes the market snapshot as an explicit second argument**
  (`rustrade-execution`), so a downstream mock wrapper can override the price the venue fills at.
  `MockExchange::run` reads it off the request before forwarding.

- **`MockExchange` emits a filled order's events from one ordered task** (`rustrade-execution`).
  It previously spawned two independent tasks per fill — one for the response, one for the balance
  and trade notifications — which slept for the same latency and then raced. It now sleeps once and
  sends balance, then trade, then the response. That mirrors a real venue (a trade is booked before
  the order can be reported `FullyFilled`) and makes "the client has its response" imply "every
  account event for this order has already been sent".

- **`backtest` now fails a run in which every open request was rejected** (`rustrade`), returning
  the new `BarterError::BacktestAllOrdersRejected { rejected, reason }` — carrying the exchange's
  own first reason — instead of a summary of zeros that looks like a flat strategy. Partial
  rejections are deliberately left alone, since an insufficient balance is a legitimate simulated
  outcome a strategy should experience, and a strategy that sends no requests at all never trips it.

- **`Shutdown` is now an enum** (`rustrade`): `Shutdown::Immediate` keeps today's behaviour — stop
  at once, abandoning anything in flight — and the new `Shutdown::AfterDrain` stops generating
  orders and terminates only once no order is left `OpenInFlight` or `CancelInFlight`.
  `System::shutdown_after_backtest` sends `AfterDrain`; `System::shutdown` and `System::abort`
  still send `Immediate`, so live shutdown does not start waiting on a venue that may be slow or
  unreachable. The drain is bounded by each `ExecutionManager`'s `request_timeout`, and orders
  merely *resting* at the venue do not hold it up.
  *Breaking:* `Shutdown` was a unit struct, so every construction site needs a variant — which is a
  compile error rather than a silent change of behaviour. `EngineEvent::shutdown()` is unchanged
  and still means `Immediate`; `EngineEvent::shutdown_after_drain()` is new.

- **`ProcessAudit` carries a `shutdown` flag** (`rustrade`) recording that the `Engine` stopped for
  a reason the audited event does not itself express — currently a completed `AfterDrain` drain,
  where the terminating event is the ordinary account update that resolved the last in-flight
  order. Kept on the audit so an `AuditManager` replica reaches the same stop decision from the
  stream alone. `#[serde(default)]`, but `ProcessAudit`'s derived `new` gains an argument.

- **`EngineMeta` carries a `draining` flag** (`rustrade`) for the same barrier, cleared by
  `Engine::reset_metadata`. `#[serde(default)]`.

- **The `databento` module now documents that the data it retrieves is not redistributable**
  (`rustrade-data`). Our MIT licence covers *our code* and confers no rights in a provider's data —
  the same split already recorded for London Strategic Edge. The module rustdoc now states the
  restriction, spells out what it means in practice (retrieved records are for internal use; do not
  commit them as fixtures, example datasets, CI artifacts or golden files) and links the terms. The
  `download_databento_fixtures` example previously instructed the reader to commit its output; it
  now writes to a gitignored `local-data/databento/` and says plainly that committing it is a
  breach.

- **`binance-sdk` 60.0.0 → 69.1.0** (`rustrade-execution`, `binance` feature). Nine majors, but the
  version number tracks Binance's REST surface rather than dependency churn: the SDK's dependency
  set is byte-for-byte identical across the whole range, and the only manifest change is the removal
  of an `nft` feature we never enabled. The bump is **behaviour-preserving on the wire** — no request
  this crate sends changes shape or value — and the entire migration is a consequence of the SDK
  replacing stringly-typed parameters with generated enums:
  - **`isIsolated`, order `type` and `sideEffectType` are now typed enums**, one generated per
    endpoint, where they were `String`. Every variant serialises to the string it replaces
    (`"TRUE"`/`"FALSE"`, `"LIMIT"`, `"AUTO_BORROW_REPAY"`, …), so the margin cross/isolated mode,
    order kinds and borrow policy all map exactly as before — the compiler now checks what was
    previously a hand-built string.
  - **`GET /api/v3/openOrders` returns its own response struct** (`GetOpenOrdersResponseInner`)
    rather than sharing `allOrders`' `AllOrdersResponseInner`. The two are structurally identical;
    the open-order converter is now generic over the ten fields it actually reads, so a future SDK
    change to any *other* field cannot silently affect open-order parsing.

  No public API of this crate changes: the SDK enums are confined to the client internals, and
  `MarginSideEffect` / `BinanceOrderType` keep their existing shape. The three
  `binance_sdk::common` internals the margin user-data stream depends on were re-verified against
  the 69.1.0 source and all still hold — `common/websocket.rs` is in fact byte-identical between the
  two versions, so the sequential-callback guarantee the margin `account_stream` relies on for
  soundness is unchanged. **Known, pre-existing and not introduced by this bump:** binance-sdk logs
  the full signed WebSocket request — including `apiKey` and `signature` — at `DEBUG` level
  (`api_secret` is never logged). Enabling `DEBUG` tracing for that crate will put short-lived
  credentials in your logs.

- **`ibapi` 3.2.0 → 3.3.0** (`rustrade-data`, `rustrade-execution`, `rustrade-instrument`, `ibkr`
  feature). Two consequences reach this crate's public API, because `ibapi` types are exposed
  directly on it — `HistoricalRequest::bar_size` is an `ibapi` `BarSize` and `ToDuration` is
  re-exported from `rustrade-data::exchange::ibkr::historical`:
  - **`BarSize` gains a `Min4` variant**, mapped here to a fixed 4-minute `IntervalStep` so
    `Candle::close_time` stays the exclusive `open + interval` boundary at that resolution. The enum
    is **not** `#[non_exhaustive]`, so downstream exhaustive `match`es over `BarSize` need a new arm.
    The variant is inserted between `Min3` and `Min5` — at index 8, not appended — but every
    existing variant keeps its exact TWS wire string (`Display` drives the wire, matched on variant
    identity, never on ordinal) and its serde *name*. So nothing shifts for the TWS protocol or for
    a name-based format (JSON, TOML, YAML). **A positional serde format is the exception**: under
    `bincode`/`postcard` every variant from index 8 onward moves by one, and any `BarSize` persisted
    that way needs migrating. rustrade itself uses no such format and does not persist `BarSize`.
  - **`ibapi`'s `fundamental` module is gone** — `Client::fundamental_data`, `FundamentalData`,
    `FundamentalReportType`, and `TickType::FundamentalRatios` (tick id 47, which now decodes as
    `Unknown`). IBKR removed `reqFundamentalData` from the TWS API in 10.47. A breaking removal in
    an upstream *minor* release; rustrade never used any of it, so nothing here changed, but a
    downstream crate reaching through to those items via its own `ibapi` dependency will not compile
    against 3.3.0.

  Also inherited, and worth knowing if you run the `ibkr` feature against a live gateway: **the
  handshake now advertises TWS server version 225 instead of 221**, so a modern TWS may negotiate
  newer field layouts. No decoder in `ibapi` branches on the four new version constants, and the
  order wire is unchanged — 3.x places orders over protobuf, and the encoder delta is two optional
  fields that are omitted at their default values, so the bytes rustrade sends for an order are
  byte-identical to 3.2.0. But this repo's IBKR integration tests all require a live TWS and are
  `#[ignore]`d, so **the bump has not been exercised against a real gateway.**

  `ibapi` is tier-2 and pinned; re-reviewed at 3.3.0 with the dependency surface confirmed
  unchanged — see `.github/tier2-dependencies.txt`.

- **Silent assumptions in the new LSE and streaming code are now observable.** None of these change
  a decoded value; each replaces a quiet assumption with something a caller can see.
  - `merge_time_sorted` trips a `debug_assert!` naming the offending input when one is not sorted
    ascending (`rustrade-data`). Enforcing the obligation would need buffering, defeating the O(1)
    memory the merge exists for; *detecting* it needs only the previous timestamp per input. The
    check and its state are compiled out of release builds entirely.
  - The Parquet decoder `warn!`s once per artifact when a `price`/`ask` tick export populates the
    `volume` column, which that layout discards — an L1 quote has no undifferentiated size field.
    The column was previously never opened, so a provider that started populating it would have been
    dropped with zero observability (`rustrade-data`, `lse-parquet` feature).
  - A `429` response now drains its body before being converted to `LseError::RateLimited`, so the
    connection returns to the pool instead of being closed and re-handshaked on the caller's retry
    (`rustrade-data`, `lse` feature).

- **A candle page carrying a bar past the requested `end` is now a typed error, not a silent trim**
  (`rustrade-data`, `lse` feature). `fetch_candles` trimmed such a bar away and ended the stream
  `Ok`. The upper bound sent to the vault is `end - interval + 1s` against a parameter that is
  *exclusive* on open time, so the newest bar a compliant page can carry is the one closing exactly
  on `end` — a later one can only come from a vault that ignored the range it was given, which is
  the same silently-ignored-parameter failure as a page repeating its cursor, already terminal.
  Trimming and continuing returned a series that looked complete while the response that produced
  it was untrustworthy. Now surfaced as `LseError::UnexpectedCandleRange`, carrying the symbol,
  cursor, page and `end`. Every in-range bar on the offending page is yielded first, so nothing the
  response did contain is lost. The lower bound is deliberately **not** symmetric: it is widened by
  one interval on purpose, so bars closing before `start` are still trimmed without comment.
  **⚠️ Behaviour change**: a fetch that previously returned a short-but-successful series against
  such a provider now ends with an `Err` after the in-range bars.

- **`ExchangeId`'s `Display` now renders the canonical `snake_case` name** (`rustrade-instrument`).
  It derived `derive_more::Display` with no format attribute, so `format!("{}", BinanceSpot)` was
  `"BinanceSpot"` while `as_str()`, serde and every configuration file said `"binance_spot"`. Two
  spellings for one identity is a defect rather than a formatting preference: it is the root cause
  of the `InstrumentNameInternal` divergence fixed below, and it leaked into user-facing
  diagnostics — `SocketError::Unsupported` and `IndexError::ExchangeIndex` named an exchange
  matching nothing the user had written. `Display` now delegates to `as_str`, so the two cannot
  drift again.
  **⚠️ Behaviour change**: anything that formats an `ExchangeId` — log lines, error strings, and
  any key or filename built by interpolating one — changes spelling. Code that needs the variant
  name instead should use `{:?}`.

- **`InstrumentConfig` now derives `name_internal` from `name_exchange`, not from the underlying
  pair** (`rustrade`, `system::config`). `From<InstrumentConfig>` built the identity key from
  `(exchange, base, quote)` and ignored both `kind` and `name_exchange`, so two configurations
  differing only in `kind` produced the same `InstrumentNameInternal`. That is not hypothetical:
  `ExchangeId::Okx` serves spot, futures, perpetuals and options under one variant, and an exchange
  offering both a stock and a CFD on one symbol is the reason `InstrumentKind::Cfd` is distinct from
  `Spot` at all. Since `IndexedInstrumentsBuilder` now rejects a duplicate `name_internal`, such a
  pair was inexpressible through `SystemConfig` — it failed at startup with no way around it.
  The exchange-side name is what the venue itself uses to tell the two apart (Okx `BTC-USDT` vs
  `BTC-USDT-SWAP`; IBKR `AAPL` vs `AAPL.CFD`), so identity now derives from it and discriminates
  wherever the venue does.
  **⚠️ This renames every config-derived instrument**, e.g. `binance_spot-btc_usdt` →
  `binance_spot-btcusdt` for a config whose `name_exchange` is `BTCUSDT`. The same persisted-state
  migration applies as for the `InstrumentNameInternal` fix below — see that entry.
  **⚠️ It also renumbers `InstrumentIndex`.** `IndexedInstrumentsBuilder` sorts instruments by
  `name_internal`, so changing that key changes the sort order and therefore which instrument each
  index denotes. Indices are read **positionally** into engine state, the audit replica and
  backtest replay streams, so any state persisted under the old naming — snapshots, recorded
  audit streams, serialized `InstrumentIndex` values — refers to different instruments after this
  change and cannot be loaded against a new build. Rebuild it from the config rather than
  migrating it in place. Ordering is unaffected only where a config's `name_exchange` happens to
  sort identically to its old `base`/`quote` derivation, which is not something to rely on.

- **`load_trades_from_dbn` / `load_quotes_from_dbn` now document a caller obligation**
  (`rustrade-data`, `databento` feature; documentation only, no behaviour change). Both tag every
  record with the caller-supplied instrument key and never read the per-record `instrument_id` from
  the DBN header — which the live path *does* resolve, via `PitSymbolMap`. A multi-instrument file
  therefore decodes silently with every event attributed to one instrument. The rustdoc now states
  that these are correct only for single-instrument files.

- **`DefaultInstrumentMarketData` now consumes `DataKind::Candle`** (`rustrade`). It previously
  tracked only trades and L1 and ignored every other variant behind a catch-all `_ => {}`, so an
  engine fed a candle-only feed was silently inert — `price()` returned `None`, no position could be
  valued, and nothing reported a problem. Candle-first data sources therefore required a custom
  `InstrumentDataState` that every user had to copy from an example.
  **This changes behaviour for existing feeds.** An instrument receiving candles (or candles and
  trades) with no L1 book now has a price where it previously had none, moving `pnl_unrealised` and
  anything derived from it. An instrument with only an L1 book and trades — no candles — is
  unaffected: the book still wins outright.
  The rule ranks the two *regimes* differently, because the three stamps are not drawn from one
  clock. **Within the tick regime, fixed precedence:** the L1 book wins over the last trade
  whenever it has a mid to contribute (a one-sided or never-received book contributes nothing and
  falls through to the trade, so the precedence never costs a price). `OrderBookL1::last_update_time`
  is a *payload* field and several venues publish no book instant at all, so their connectors stamp
  it from the host clock, while a trade's `time_exchange` is the venue's own — ranking those against
  each other would let a host running a few hundred milliseconds behind the venue flip every mark
  from the book mid to the last trade, and an NTP step flip it back, with no log and no error.
  **Across regimes, recency:** a candle wins when its `close_time` is strictly later than the held
  tick's stamp. A fixed order there would let a stale input shadow a fresh one indefinitely — on a
  mixed feed one provider alone can produce, such as a session of quote ticks followed by a year of
  daily bars, a year-old book would mark every position after it and the tear sheet would look
  entirely normal. That gap is usually a regime change measured in days to years, which survives
  clock skew of any realistic size — though subscribing to L1 and candles concurrently is supported,
  and on a venue whose book instant is host-stamped that narrows it to the bar interval, leaving a
  skew-sized window after each bar close. An exact tie goes to the tick, since `close_time` is the
  exclusive period end and an observation stamped at that instant belongs to the next period.
  *Known limitation, stated on the method:* the flip side of the fixed precedence is that an L1 book
  which **freezes while trades keep arriving** shadows those trades until it resumes. A strategy
  needing trade-led marks on such a feed should implement its own `InstrumentDataState`.
  The struct gains a `candle: Option<Candle>` field, so its positional `new()` and its serialized
  shape both change. `#[serde(default)]` is applied to **that field only, not to the container**, so
  state written by a build predating the field still deserializes with `candle: None` instead of
  failing on a missing key — serde requires an absent field to be declared defaultable even when its
  type is `Option`, and deriving `Default` does not change that. Per field rather than
  container-wide because `l1`'s `Default` is not a neutral value: a container-level default would
  make a snapshot truncated before `l1` load as an epoch-stamped empty book, which `price()` reports
  as "no price yet" — indistinguishable from an instrument that never ticked. Such a snapshot is
  now rejected.
  `OrderBook` (L2), `Liquidation` and `OptionGreeks` remain excluded, now with a
  stated reason each in place of the catch-all; `Liquidation` in particular is a forced fill at a
  potentially dislocated price and must never reach `price()`.

- **`OrderBookL1::last_update_time` now documents a producer obligation, and names the in-tree
  producers that cannot meet it** (`rustrade-data`). It should carry the venue's own instant and
  match the wrapping `MarketEvent::time_exchange`. Downstream state orders L1 updates on this field
  rather than on the event's, so a connector stamping it from a local clock gets a staleness guard
  keyed on it that can reject legitimately newer updates and freeze the held book.
  **Not every in-tree producer satisfies it, and the docs now say so.** Binance Spot's `bookTicker`
  payload carries no venue timestamp (the deserializer defaults the field to `Utc::now()`), and the
  IBKR quote path stamps it from a host-clock parameter. Both are venue limitations rather than
  connector defects — there is no instant to carry — so the field is documented as best-effort with
  the two exceptions named at the site. `DefaultInstrumentMarketData::price` was changed to stop
  ranking this stamp against a venue trade stamp for exactly this reason; see that entry.

- **`BacktestMarketData::stream()` items are now `Result<_, BarterError>`, and a source failure
  aborts the backtest** (`rustrade`). The item type was infallible, which was adequate only while
  the sole implementation was fully in-memory. A source that reads incrementally — a file, a
  decoder, a paginated fetch — can fail after the stream has opened, and with no error channel the
  only options were to truncate the stream or panic. Truncating is the dangerous one: the run would
  complete and return a perfectly normal-looking `BacktestSummary` computed over however much of the
  dataset happened to be read, with nothing to distinguish it from a complete run.
  `backtest` now returns that error instead of a result, and never produces a summary over a
  partially-read dataset. `MarketDataInMemory` is unaffected behaviourally (it yields `Ok`) and its
  constructor is unchanged, so callers that only *use* it need no edit; custom implementations of
  the trait must wrap their items.

- **`MockExchange` now supports `InstrumentKind::Cfd`** (`rustrade`). `generate_mock_exchange_instruments`
  panicked on any non-`Spot` kind via a catch-all, so backtesting or paper-trading a CFD instrument
  failed at execution-build time — making CFD-quoted datasets unusable despite being correctly
  modelled. A CFD fill is the same price × quantity arithmetic as spot (the `contract_size`
  multiplier is applied engine-side), so the mock now maps it through, re-resolving the CFD's
  settlement asset to its exchange name. Other kinds still panic; that is a capability limit of the
  mock, not a statement about which kinds are executable.

- **`IndexedInstrumentsBuilder` now rejects duplicate `InstrumentNameInternal`s**
  (`rustrade-instrument`). `InstrumentStates` is keyed on `InstrumentNameInternal` but read
  *positionally* by `InstrumentIndex`, and nothing enforced that the two agreed. The existing
  `Instrument` dedup does not cover it — it removes only instruments equal in *every* field, so two
  genuinely different instruments sharing a name survived it, then collapsed into one map entry.
  Every `InstrumentIndex` past the collision then resolved to the wrong instrument's state, with
  positions, unrealised PnL, orders and tear sheets attaching to the wrong instrument and only the
  final index panicking. The invariant is now checked at build time: `build`/`new` panic naming the
  duplicate, and the new fallible `IndexedInstrumentsBuilder::try_build` /
  `IndexedInstruments::try_new` return `IndexError::DuplicateInstrumentNameInternal` instead.
  `IndexError` is now `#[non_exhaustive]`; downstream exhaustive `match`es need a wildcard arm.

- **`InstrumentKind::eq_market_data_instrument_kind` is exhaustive on `self`**
  (`rustrade-instrument`). Its `_ => false` fallthrough meant a new `InstrumentKind` variant
  compiled clean and then silently failed to bind its market-data subscription — an instrument
  configured, indexed, and permanently dataless, invisible to both `cargo build` and clippy. A
  missing arm is now a compile error. Behaviour for existing variants is unchanged.

- **Corporate-action split eligibility is single-sourced as `InstrumentKind::is_split_eligible`**
  (`rustrade`). The live handler and its audit replica previously carried hand-mirrored
  `matches!(kind, InstrumentKind::Spot)` guards whose drift was caught only after the fact by a
  parity test. The rule being pinned is *the deliverable equity*; `Spot` is only its current
  spelling. No behaviour change.

- **BREAKING: `Candle.volume` and `Candle.trade_count` are now `Option` (`Option<Decimal>` /
  `Option<u64>`)** (`rustrade-data`, `subscription::candle`). A candle producer that carries no
  consolidated volume or no trade count must now say so with `None` — an un-ignorable "unknown" —
  rather than fabricating a `0` a consumer cannot distinguish from a genuine zero-volume /
  zero-trade bar (the direct precedent is `PublicTrade.side: Option<Side>`). Per-producer: Binance
  klines/REST and Hyperliquid carry real values (`Some`, including a venue-reported `Some(0)` on a
  gap-filled bar); Databento OHLCV has no trade-count field, now `trade_count: None` (was `0`); IBKR
  maps its `-1` "not available" sentinel on volume/count to `None` (was a clamped `0`); Massive now
  passes its already-optional trade count through unchanged (was `unwrap_or(0)`); the two London
  Strategic Edge producers report `volume` only where the dataset carries it (the FX quote tape
  reports neither field) and never synthesise a count. `aggregate_candles`
  propagates absence: any `None` constituent makes the aggregated bucket's `volume`/`trade_count`
  `None` (an unknown component makes the sum unknown, never a silent under-count). `Candle` also now
  derives `Eq` and `Hash` (all fields qualify), so it can be embedded in `Eq`/`Hash` engine state.
  Migration: **match on the `Option` and decide per call site.**
  ```rust
  match candle.volume {
      Some(volume) => /* a real, venue-reported figure */,
      None => /* the venue reports none; propagate the unknown, do not substitute */,
  }
  ```
  The hazard to check first is **aggregation**. `unwrap_or_default()` compiles everywhere and is
  the wrong answer precisely where this change matters: summing, averaging or ratio-ing volume
  across bars, where a substituted `0` silently under-counts the total and reads as a real result.
  If a total must stay meaningful, propagate the absence the way `aggregate_candles` does — any
  `None` constituent makes the aggregate `None`. Reach for `unwrap_or_default()` only for display
  or for a call site where a fabricated zero is genuinely indistinguishable from the truth.
  The **serde contract** moves with the type: an absent `volume` / `trade_count` key now
  deserializes to `None` where it used to be a hard error, so a payload this crate previously
  rejected is now accepted as "unknown"; a pre-migration `{"volume": 0}` still reads back as
  `Some(0)`, and `None` serializes as an explicit `null` rather than being skipped.

- **BREAKING: Massive aggregates now declare what they were built from, and forex bars report
  `volume: None` *and* `trade_count: None`** (`rustrade-data`, `massive` feature). Two provider
  facts were being read as if they said something else, and both produced numbers that look
  ordinary:
  - A forex aggregate's `v` and `n` are **not traded volume and not a transaction count**. The
    provider generates forex bars "from quoted bid/ask prices rather than executed trades", so `v`
    counts quote updates — a quantity with no units in common with a share count, which a VWAP, a
    volume filter or a liquidity screen would consume as though it had. `n` is documented just as
    generically ("the number of transactions in the aggregate window"), and the same fact overrides
    it: a bar generated from quotes contains no transactions, so `n` is a quote-update count too —
    the same quantity as `v`, differing only in units. Gating one and passing the other through
    would leave `candle.trade_count` reporting quote ticks in the hundreds per minute regardless of
    liquidity, so a `trade_count >= 50` liquidity filter would pass on every forex bar and look like
    it was working — bit-for-bit the defect gating `v` removes. Forex bars now report **both** as
    `None`, the crate's existing "the venue reports none" signal, on the REST and the WebSocket
    path alike.
  - The WebSocket aggregate's `z` is documented verbatim as *"The average trade size for this
    aggregate window"* and was being decoded as a trade **count**. `WsAggregateMsg.trade_count:
    Option<u64>` is therefore renamed `average_trade_size: Option<Decimal>`. The `u64` was also a
    latent parse failure: `z` is normally fractional, so the field failed to deserialize and
    `parse_ws_message` discarded **the entire aggregate** as an unknown event type. WebSocket
    aggregates now report `trade_count: None`; REST keeps its `n`.

  `AggregateBar::into_candle`, `AggregateBar::into_candle_with_step` and
  `WsAggregateMsg::into_candle` gain a leading `AggregateProvenance` parameter (`TradeTape` /
  `QuoteTape`) so the classification is made once, at the call site that knows the ticker, rather
  than re-derived per bar. The type names the bar's **source**, not one of its fields, because that
  is what the provider fact is about and what decides the meaning of every activity count on the
  bar. `AggregateProvenance::for_ticker` applies the provider's own `C:` prefix convention, matched
  ASCII case-insensitively since nothing upstream normalises a caller-supplied ticker. Migration:
  pass `AggregateProvenance::for_ticker(ticker)`, or `AggregateProvenance::TradeTape` if the ticker
  is known to be an equity.

- **BREAKING: `IbkrHistoricalData::fetch_option_chain` now returns `OptionChainResult`, and no
  longer discards already-decoded entries on a mid-stream IB error** (`rustrade-data`, `ibkr`
  feature). The method previously failed fast on the first error yielded mid-enumeration,
  returning `Err` and dropping every `OptionChainEntry` already received — even though each entry
  is decoded from one complete IB message and is valid in isolation. It now mirrors the historical
  tick methods: entries received before the error are returned in the new `#[non_exhaustive]`
  `OptionChainResult { entries, truncation_error }`, with `truncation_error: Some(reason)` flagging
  a confirmed early end of enumeration (`None` on a clean end — which also disambiguates a
  legitimately empty catalog, e.g. a routing-`exchange` filter, from a truncated one).
  Migration: read the entries via `result.entries` (or iterate the result directly —
  `IntoIterator` yields the entries, preserving the old `for chain in chains` ergonomics) and
  check `result.truncation_error` where completeness matters. (#184)
- **`MassiveError` is now `#[non_exhaustive]`** (`rustrade-data`). Lets new variants (such as the
  pagination-robustness errors above) be added without further breaking changes. **Breaking** for
  downstream code matching `MassiveError` exhaustively — add a wildcard (`_`) arm.
- **BREAKING: `MassiveRestClient::with_base_url` now returns `Result<Self, MassiveError>`**
  (`rustrade-data`). The base URL is parsed into its trusted origin once, at construction, and cached
  on the client — so a base URL that is not a valid URL, or one whose scheme is not `http`/`https`,
  now fails fast with `MassiveError::InvalidInput` here instead of surfacing as a deferred error on
  the first request, and every `next_url` origin check compares against the cached origin rather than
  re-parsing the base URL per page. (Rejecting non-`http(s)` schemes at construction avoids a
  confusing failure mode: a scheme such as `file:` parses but yields an opaque origin that never
  compares equal, which would otherwise brick every request with a misleading `UntrustedNextUrl`.)
  Migration: append `?` or `.expect(..)` to existing `with_base_url(..)` calls. (#198)
- **`EngineEvent` is now `#[non_exhaustive]` and gains a `CorporateAction` variant** (`rustrade`).
  Marking it non-exhaustive lets future engine-driven event variants be added without further
  breaking changes. **Breaking** for downstream code matching `EngineEvent` exhaustively — add a
  wildcard (`_`) arm. **serde/replay note:** the new variant changes the serialized form of
  `EngineEvent`. Audit logs written by this version may contain `CorporateAction` ticks that older
  library versions cannot deserialize (unknown variant), so wrappers that persist and replay the
  audit stream across this version boundary must account for it.
- **`EngineOutput` is now `#[non_exhaustive]`** (`rustrade`). New engine-driven outputs (e.g. the
  corporate-action observables above) can be added without further breaking changes. **Breaking**
  for downstream code matching `EngineOutput` exhaustively — add a wildcard (`_`) arm.
- **Corporate-action `ratio` fields are typed `SplitRatio`, not `Decimal`** (`rustrade`). The new
  `EngineOutput::OptionPositionAdjustedForSplit` and `OptionPositionsRequireIdentityChange` outputs
  carry `ratio: SplitRatio`, preserving the strictly-`> 0` type invariant across the observation
  boundary (it was previously discarded back to a raw `Decimal`). `InvalidSplitRatio`'s rejected
  value is now read via `.rejected()` rather than a public tuple field. Relevant only to code
  tracking this unreleased corporate-action line.
- **`BacktestArgsConstant` gains a required `aux_events` field, and `run_backtests` / `backtest` gain
  an `AuxEvents` generic** (`rustrade`). **Breaking** for downstream code constructing
  `BacktestArgsConstant` via a struct literal — add `aux_events: NoAuxEvents` (the struct's new
  `AuxEvents` parameter defaults to `NoAuxEvents`) or supply a custom `AuxEventSource`. The
  `run_backtests` / `backtest` **functions** also gain a trailing `Aux` type parameter; function type
  parameters cannot carry a default, so callers that spell the generics explicitly (turbofish, or a
  fully type-annotated function pointer) must account for the extra argument. Callers that rely on
  inference are unaffected.
- **`backtest` now returns `BacktestResult { summary, engine_state }`, not a bare `BacktestSummary`**
  (`rustrade`). The terminal `EngineState` is returned alongside the summary so callers can inspect
  post-run state directly — open positions, balances, instrument state — which the aggregate
  `TradingSummary` (closed-position statistics only) cannot express. This makes the net effect of a
  notional-preserving corporate action assertable through the public async `backtest` path (e.g. a
  stock split's rescaling of a position left open at shutdown). **Breaking**: replace `summary` with
  `result.summary` at the call site (e.g. `backtest(..).await?.summary`); the new terminal state is
  `result.engine_state`. `run_backtests` is **unchanged** — it still returns `MultiBacktestSummary`
  and does not retain per-run `EngineState` (drive `backtest` directly when the terminal state is
  needed).
- **`ContractExpiry` now advances the backtest clock to the contract's expiry instant** (`rustrade`).
  When the engine processes an `EngineEvent::ContractExpiry`, the `HistoricalClock` now advances to
  the expiring instrument's `expiry` (read from its `InstrumentKind`) before synthesising the
  settlement fill, so the fill — and the resulting `PositionExit` — is stamped at the expiry instant
  rather than the prior market tick. Previously the event carried no timestamp on its payload and
  left the clock unmoved (unlike `CorporateAction`, which advances to its `effective_time`).
  Non-breaking: adds an `EngineClock::advance_to` **default** method (a no-op, so `LiveClock` and any
  downstream `EngineClock` impl are unaffected) and a new `InstrumentKind::expiry()` accessor
  (`rustrade-instrument`, `Some` for `Future`/`Option`, `None` otherwise). A backtest that injects a
  `ContractExpiry` via an `AuxEventSource` must now position it in the merged stream by the target
  instrument's own `expiry` (the harness enforces `Timed::time == expiry` with a hard pre-merge
  panic, mirroring the existing `CorporateAction` `effective_time` check), so a mismatch fails loudly
  instead of silently ordering the expiry at one instant while settling it at another.
- **Alpaca client error variants** (`rustrade-data`, feature-gated `alpaca`). `AlpacaOptionsError` is
  now a type alias of the shared `AlpacaRestError`, which adds an `InvalidCredential` variant. The
  `AlpacaOptionsClient` constructor now reports a credential that cannot be encoded as an HTTP header
  value as `InvalidCredential`, where the credential-error path previously surfaced as `EnvVar`.
  `AlpacaRestError` is `#[non_exhaustive]`, so exhaustive matchers already require a wildcard arm;
  only callers that branched on the specific credential-error variant need to update. `AlpacaRestError`
  also gains a `NotCloneable` variant: the shared retry helper now **returns** it (instead of
  panicking) when a request body cannot be cloned for a retry — unreachable for the GET/query-string
  requests the client issues today, but recoverable for a future streaming-body caller.
- **`MarketDataInMemory::new` now hard-asserts sorted input** (`rustrade`). Construction `assert!`s —
  in all builds, not behind `debug_assert!` — that events are sorted ascending by
  `MarketEvent::time_exchange`; previously unsorted input was accepted silently and would yield a
  non-monotonic backtest clock and out-of-order engine feed. Mirrors `AuxEventsInMemory::new`.
  **Breaking** only for callers that were (incorrectly) supplying unsorted data — observable failure
  over silent corruption.
- **`alpaca` and `massive` features no longer enable global float `Decimal` serialization**
  (`rustrade-data`). Both features previously enabled `rust_decimal/serde-float`, a **global**
  feature whose side effect — through Cargo feature unification across the workspace — was to flip
  every `Decimal`'s default `Serialize`/`Deserialize` to a lossy `f64`. They now enable the
  field-level `rust_decimal/serde-with-float` instead, so `Decimal` keeps its deterministic,
  lossless string wire format everywhere by default. **Breaking** for any downstream that built with
  `--features alpaca` / `--features massive` and relied (intentionally or as a side effect) on the
  global float form: fields that must serialize as floats now need an explicit
  `#[serde(with = "rust_decimal::serde::float")]` (or `float_option`). The integration clients that
  require it already carry this attribute.
- **IBKR historical tick fetches now surface mid-stream errors** (`rustrade-data`, feature-gated
  `ibkr`). Bumping `ibapi` to 3.2.0 adopts its new `SubscriptionItem` tick envelope, which exposes
  IB errors mid-stream that the previous pin (3.0.1) dropped silently. On such an error,
  `fetch_historical_ticks` and `fetch_historical_bid_ask` now log a `warn!` and stop, returning the
  ticks collected so far rather than an unexplained short batch. **Breaking**: both methods now
  return `HistoricalTicks<T>` instead of `Vec<T>` — a `#[non_exhaustive]` struct exposing the
  collected `ticks` plus a `truncation_error: Option<String>` carrying the formatted IB error when a
  fetch was cut short, so callers can distinguish a confirmed mid-stream error from a normal short
  end-of-data batch (and see *why*) programmatically instead of by parsing logs. `HistoricalTicks<T>`
  also implements `IntoIterator` (yielding the ticks) for callers that only need the data. Migration:
  read `.ticks` for the previous `Vec<T>`, or iterate the value directly.
- **`EngineOutput` shrunk ~936 → ~232 B** (`rustrade`). Its `AlgoOrders` and `Commanded` variants
  embedded large order aggregates (`GenerateAlgoOrdersOutput` directly, `ActionOutput` via
  `Commanded`) that pinned the enum's size, so the `ProcessAudit`/`AuditTick` value copied on
  **every** processed event was ~936 B — even on the common no-order market tick. Root-boxing those
  aggregates' order payloads (see the `SendRequestsOutput`/`GenerateAlgoOrdersOutput` entry below)
  shrinks them to ~144 B / ~96 B, both well under the ~232 B `PositionExit` variant that now floors
  the enum, so both variants are carried **inline** — no per-variant heap allocation and no pointer
  indirection on the audit path — and `EngineOutput`'s stack size (and the per-tick copy) drops
  proportionally. The variant shapes are unchanged (`AlgoOrders(GenerateAlgoOrdersOutput)`,
  `Commanded(ActionOutput)`), so matching/constructing them needs no box wrapper or deref. **No wire
  change**: the audit format is byte-identical.
- **`ActionOutput::GenerateAlgoOrders` variant removed** (`rustrade`). The variant was never
  constructed by the engine — `Command` has no algo-order variant, `Engine::action()` only emits
  `CancelOrders`/`OpenOrders`/`ClosePositions`, and the per-tick algo path builds
  `EngineOutput::AlgoOrders` directly — so it was reachable only through the derived `From` impl. Its
  ~928 B `GenerateAlgoOrdersOutput` payload set `ActionOutput`'s size floor; removing it drops the
  enum ~928 → ~608 B (largest remaining variant `ClosePositions`). **Breaking** only for downstream
  code that matched or constructed `ActionOutput::GenerateAlgoOrders` (no in-tree or known downstream
  use). Migration: delete any such match arm; algo-order work is surfaced via `EngineOutput::AlgoOrders`.
- **`SendRequestsOutput` and `GenerateAlgoOrdersOutput` order payloads are now boxed** (`rustrade`).
  Each `OrderEvent`/error/refusal is stored boxed inside its `NoneOneOrMany` field
  (`SendRequestsOutput::sent`/`errors`, `GenerateAlgoOrdersOutput::cancels_refused`/`opens_refused`),
  shrinking `GenerateAlgoOrdersOutput` ~928 → ~144 B and `ActionOutput` ~608 → ~96 B (small enough
  that its `#[allow(clippy::large_enum_variant)]` is removed). Shrinking the payloads at the root is
  what lets `EngineOutput` carry those aggregates inline (above) instead of behind an outer `Box`.
  **Breaking** for downstream code that destructures these public fields: the collection item type is
  now `Box<…>`, so bind and
  deref — e.g. `output.sent.iter().map(|order| &**order)` (or the provided `output.sent_iter()`
  helper, which yields `&OrderEvent`), or `NoneOneOrMany::One(Box::new(order))` when constructing.
  Field/read access is unchanged (auto-derefs through the box: `order.key`, `order.state`).
  **No wire change**: `Box<T>` serializes identically to `T`.

- **BREAKING: `DataError::Lse` carries a machine-readable `kind`** (`rustrade-data`, `lse`
  feature). The variant was `Lse(String)`, so a consumer could not tell a resumable
  `LseError::RateLimited` from a terminal `LseError::Deserialize` without substring-matching a
  `Display` — a classification that changes on any wording edit. It is now
  `Lse { kind: LseErrorKind, message: String }`, matching the shape the sibling `DataError::Databento`
  already uses. `LseError` itself cannot be nested structurally (it wraps a `reqwest::Error`, which
  is neither `Clone` nor serialisable), so the flattening stays — but the classification now
  survives it. The new public `LseErrorKind` is `#[non_exhaustive]` with eight variants
  (`Authentication`, `RateLimit`, `Network`, `Timeout`, `Api`, `Decode`, `InvalidInput`, `Io`) and
  an `is_resumable()` predicate; `LseError::kind()` derives it through an **exhaustive** match, so a
  new `LseError` variant is a compile error here rather than a silent fall-through to a default.
  Migration: `DataError::Lse(message)` → `DataError::Lse { message, .. }`, or match `kind` where the
  distinction matters.

- **BREAKING: `DataError` is now `#[non_exhaustive]`** (`rustrade-data`). Variants are added as
  integrations land, and two of them (`Databento`, `Lse`) exist only under their own feature — so
  the variant set a downstream `match` sees already depended on the feature selection it built with,
  and a wildcard arm was already required in practice. The attribute makes that a compile-time
  contract instead of something a consumer discovers when a feature flag or a release moves, and
  matches the sibling `LseError`, `IndexError` and the new `LseErrorKind`. Matches inside
  `rustrade-data` stay exhaustively checked, so a new variant is still a compile error at every
  in-crate site. Bundled with the `DataError::Lse` reshape above so downstream matchers absorb one
  breaking window rather than two. Migration: add a `_ => ...` arm to any exhaustive `match` on
  `DataError`.

- **BREAKING: `IndexedInstruments` now rejects a non-positive `contract_size`** (`rustrade-instrument`).
  `contract_size` multiplies every money quantity derived from an instrument — quote notional,
  realised and unrealised PnL, and any notional-scaled fee model — and neither degenerate value
  failed anywhere downstream. Zero makes all three zero, so a backtest trades freely, is charged
  nothing, never moves, and reads as a strategy that found no edge; a negative value inverts the
  sign of PnL, so a losing strategy reports a profit. `try_build`/`try_new` now return the new
  `IndexError::InvalidContractSize` (and `build`/`new` panic with it, as they already do for a
  duplicate name). Checked through `InstrumentKind::contract_size`, so it covers `Perpetual`,
  `Future`, `Option` and `Cfd` uniformly — `Spot` reports a hard `Decimal::ONE` and can never trip
  it. **Breaking** for any configuration that was silently carrying a zero or negative multiplier;
  such a collection now fails at build time rather than producing a meaningless run.
  `IndexError` is `#[non_exhaustive]`, so the variant itself is additive.

- **`LseVaultClient::submit_export` rejects a wholly-future range** (`rustrade-data`, `lse`
  feature). The export allowance is five per hour and a rejected submit still consumes one, so a
  fat-fingered year burned an export and returned a valid, complete-schema, **zero-row** artifact
  with no error at any layer — the provider's silent-empty trap. The new pure predicate
  `LseExportRange::is_wholly_after(today)` is checked at submit against `Utc::now().date_naive()`,
  leaving the constructor pure and testable. It allows **one full day of slack**: the maximum real
  UTC offset is +14, so a caller's local "today" can be at most UTC's tomorrow. It therefore cannot
  false-positive on a legitimately-today range while still catching a wrong year.

- **The `"ALL"` export rejection is now scoped to the equity datasets** (`rustrade-data`, `lse`
  feature). `"ALL"` is a literal that matches nothing rather than a request for every symbol, so an
  export naming it spends one of five hourly exports on an empty artifact — but `ALL` is also
  Allstate's real ticker, which is why the rejection is case-sensitive. It now applies only to
  `LseDataset::Stocks | Etf`, the classes that share the venue and symbology where that listing
  exists; on any other dataset `"ALL"` is rejected without the Allstate caveat in the message. The
  error text is dataset-aware for the same reason.

- **`QuotaStatus::is_exhausted` split into two predicates** (`rustrade-data`, `lse` feature).
  London Strategic Edge meters streaming and bulk export against one shared allowance, but they are
  separate dimensions: a consumer gating a candle backfill on the combined answer stopped fetching
  for up to an hour after five exports, with terabytes of byte allowance left. `is_byte_allowance_exhausted()`
  and `is_export_allowance_exhausted()` are the ones to branch on; `is_exhausted()` remains as their
  union, now documented as usually the wrong question.

- **`take_market` rejects a backwards `time_exchange` in release, not only under `debug_assert`**
  (`rustrade`, `backtest`). `MarketDataInMemory::new` hard-asserts ascending order in release,
  commented that an unsorted stream "would silently produce a non-monotonic clock and wrong
  simulation results in release". `MarketDataStreamed` pushed that obligation to the caller's
  factory, where the only check was a `debug_assert` inside `merge_time_sorted` — compiled out of
  release, and present at all only if the factory happened to use that helper. An out-of-order event
  is now recorded as `BarterError::BacktestMarketData` and ends the stream through the existing
  source-failure path, so a wrong run fails rather than returning `Ok` with a normal-looking
  summary. The message names both instants. Repeated timestamps are still accepted (ties are the
  common case on a tape) and `MarketStreamEvent::Reconnecting` is not treated as out-of-order.
  `TimedMergeStream` is now **fused** and implements `FusedStream`, without which the abort was not
  an abort: the next poll fell through to draining the aux stream, emitting events at instants the
  market data never reached.

- **BREAKING**: **`ConnectivityState` gains a `role: VenueRole` field** (`rustrade`), and
  `all_healthy()` now consults only the dimensions that role says the venue actually has. Without
  this, any venue supplying market data without an account — which the new `data_venue` makes
  ordinary — would hold its `account` at `Reconnecting` forever, so `ConnectivityStates::global`
  could never reach `Healthy`, and everything gated on global health would be permanently degraded.
  The role is derived per dimension rather than per provider, which matters: marking only the data
  venue `DataOnly` fixes half the trap, leaving the *execution* venue waiting on a market-data
  subscription that is never made. Migration: struct literals and exhaustive destructuring patterns
  must supply the field — use the new `ConnectivityState::new(role)`, or `..Default::default()`,
  which yields the pre-existing `VenueRole::Both` semantics. The field is `#[serde(default)]` to
  `Both`, so state persisted before it existed deserialises unchanged.

- **BREAKING**: **`ConnectivityStates::exchanges` is an `FnvIndexMap`** (`rustrade`), not a
  SipHash-keyed `indexmap::IndexMap`. It is looked up by `ExchangeId` on **every** market event now
  that resolution precedes the global-health short-circuit (see the Fixed entry on the
  untracked-exchange panic), and a fieldless enum key is exactly the shape SipHash's fixed
  per-call setup cost is worst for. This also matches every sibling per-event keyed structure —
  `InstrumentStates`, `AssetStates`, `MultiExchangeTxMap`. Iteration order is unchanged: `IndexMap`
  is insertion-ordered regardless of hasher. Migration: the field is public, so code naming its type
  or building one directly needs `rustrade_integration::collection::FnvIndexMap`; `IndexMap`'s API is
  otherwise identical.

- **BREAKING**: **The `ExchangeId`-keyed connectivity updates are now fallible** (`rustrade`).
  `ConnectivityStates::update_from_market_event`, `update_from_market_reconnecting` and
  `update_from_account_reconnecting` return `Result<(), UntrackedExchange>`, and
  `EngineState::update_from_market` propagates it — see the Fixed entry on the untracked-exchange
  panic. The `ExchangeIndex`-keyed accessors stay panicking, deliberately: those keys are derived
  from the same `IndexedInstruments` the collection was built from, so an out-of-range one is a
  library bug rather than a misconfigured input, and is not something a caller could handle.
  `UpdateFromMarketOutput` and `UpdateFromAccountOutput` each gain an `UntrackedExchange` variant and
  are now `#[non_exhaustive]` (adding the variant was already breaking, so the future-proofing is
  absorbed here rather than costing a second break later).

- **BREAKING**: **`generate_empty_indexed_connectivity_states` takes the execution venues**
  (`rustrade`), as `Option<&FnvHashSet<ExchangeId>>`. Pass `None` to keep the previous
  instrument-derived behaviour. See the Fixed entry on the account connection dimension.

- **BREAKING**: **`Instrument::map_exchange_key` takes a lookup closure**, not a pre-resolved key
  (`rustrade-instrument`): `(self, exchange: NewExchangeKey)` becomes
  `(self, find_exchange: impl Fn(&ExchangeKey) -> NewExchangeKey)`. One key cannot serve two venues:
  the old shape could only stamp the execution venue's key onto the data venue, which is wrong by
  construction once the two differ. Migration: pass a closure returning the key you used to pass —
  `instrument.map_exchange_key(|_| key.clone())` reproduces the old behaviour exactly for an
  instrument with no `data_venue`. `DataVenue::map_exchange_key` takes the same closure shape, but is
  new API rather than a change.

- **`UnindexedAccountSnapshot`s omit data-only venues instead of reporting them empty** (`rustrade`).
  `From<&EngineState> for FnvHashMap<ExchangeId, UnindexedAccountSnapshot>` iterated every tracked
  exchange, so a venue that only supplies market data produced an **empty** snapshot. The two claims
  are not the same: an empty snapshot asserts a known, drained account, which a caller cannot
  distinguish from a real one that has gone to zero — and seeding a `MockExecutionConfig::initial_state`
  from it would stand up a mock account at a venue nothing trades on. A missing `ExchangeId` now means
  "no account here", so callers must not assume every tracked exchange appears as a key.

- **`EngineOutput::CorporateActionAlreadyProcessed` has two emitters** (`rustrade`), distinguished by
  its `instrument` field: the pre-existing target-level replay suppression, and the new per-option
  one described in the Fixed entry below. Its rustdoc previously claimed no other observable
  accompanies it, which is no longer true — a suppressed chain adjustment emits one per skipped
  option alongside an aggregate warning.

### Removed

- **`MarketPrices`** (`rustrade-execution`), structurally identical to the new `MarketSnapshot` that
  replaces it, and **`market_prices` from `MockExchangeRequestKind::OpenOrder`**. Once `RequestOpen`
  carried the snapshot, the channel message held it twice — two copies that could disagree with
  nothing to arbitrate between them — and the variant grew to three times the size of the next
  largest. The venue now reads the snapshot off the request it was sent.

- **Committed Databento DBN test fixtures** (`rustrade-data`). `es_trades_sample.dbn.zst` and
  `es_quotes_sample.dbn.zst` held real CME `GLBX.MDP3` records. Databento licenses market data per
  subscriber, and its [User Agreement](https://databento.com/legal/databento-user-agreement) defines
  "Redistribution" to cover the publication or distribution of covered data and "all other means of
  furnishing such data or other information derived from the same to entities other than Customer",
  requiring use to stay internal absent prior written approval from **both** Databento and the
  relevant exchange. Shipping those captures in a public MIT repository was exactly that, so they
  are gone and the paths are gitignored to stop them returning. The offline transformer tests now
  generate synthetic DBN at run time instead, which also makes them stricter: every record is known,
  so they assert that *all* records transform rather than merely that some do. The trade-off is
  honest and recorded in the test module — encoding and decoding with the same `dbn` version is a
  round trip, so wire-format drift affecting both halves equally would not be caught. Consumers are
  unaffected: fixtures were never part of the published crate.

- **`EngineOutput::OptionPositionsUnadjustedForSplit`** (`rustrade`). The placeholder option-split
  signal is removed in favour of the precise pair `OptionPositionAdjustedForSplit` (standard, applied)
  and `OptionPositionsRequireIdentityChange` (non-standard, wrapper-handled). Relevant only to code
  tracking this unreleased development line, where both the old and new variants are pre-release.

- **BREAKING**: **`Ord`/`PartialOrd` on `Timed<T>` — and transitively on
  `DefaultInstrumentMarketData` (`Ord`/`PartialOrd`) and `AssetState` (`PartialOrd`)** (`rustrade`).
  The derived `Timed` ordering compared `value` before `time`, so `Vec<Timed<_>>::sort()` silently
  produced a value-sorted, non-chronological order — and a time-first ordering is no safer (it would
  collapse same-instant entries in `BTreeSet`/`BinaryHeap` via the `Ord`/`Eq` contract). Ordering is
  now intentionally not provided; sorting on a chosen field fails to compile instead of silently
  misbehaving. Migration: `events.sort_by_key(|timed| timed.time)` for chronological order (the
  in-tree practice already everywhere). The two containing types lose their (equally meaningless,
  lexicographic field-order) derived orderings with it.

### Fixed

- **BREAKING: an Alpaca subscription is now confirmed symbol by symbol, not by counting replies**
  (`rustrade-data`). Alpaca answers a multi-symbol subscribe with a single frame listing the
  symbols it actually registered. The client used the standard validator, which succeeds once it
  has seen the expected *number* of non-error replies — and `expected_responses` was 1. A
  confirmation naming one of two requested symbols was therefore indistinguishable from one naming
  both, and the caller was handed a stream silently subscribed to less than it asked for. The same
  gap let `[{"T":"success", ...}]`, which names no symbols at all, satisfy the subscribe.

  `AlpacaWebSocketSubValidator` replaces the counting validator: every requested subscription must
  be named in a confirmation before `init()` returns, and anything still outstanding when the
  timeout expires fails the subscribe and is reported by name.

  **This can surface as a subscribe error where one previously succeeded** — that is the point, but
  it is a behaviour change for anyone who was unknowingly running a partial subscription. The
  `Connector::SubValidator` associated type for the Alpaca connectors changes accordingly, and the
  `expected_responses` override is gone, since coverage rather than a count now decides.

  Note this does **not** mean a confirmed symbol will produce data promptly. Alpaca's crypto feed
  publishes a quote on top-of-book change, and the delay before a symbol first ticks is large and
  variable — 1s, 15s, 96s and 132s for four symbols confirmed on one connection in a single 300s
  window. Absence of data is not evidence of a failed subscription.

- **An Alpaca fill recovered after a disconnect now reports where it left the order**
  (`rustrade-execution`). Recovery reads the account-activities endpoint, whose FILL activities
  carry the order's cumulative filled quantity as `cum_qty`. The client discarded that field, so a
  recovered fill advanced the position and left the order untouched — precisely the orders a
  reconnect exists to repair. It is now parsed into `Trade::order_filled_quantity`, so the order
  advances from the fill itself, with no extra API call and no reconciliation fetch.

  **This also corrects a dedup mis-keying between the two paths.** Both key a fill as
  `"{order_id}:{cumulative}"`, but only the WebSocket path used the venue's own cumulative;
  recovery reconstructed one by counting executions from zero *within the recovery batch*. For an
  order that had already partly filled before the window, the two paths therefore produced
  different keys for the same execution, and it could be delivered twice. The key is now taken from
  `cum_qty`, so it agrees with the WebSocket path by construction rather than by reconstruction.
  Where Alpaca omits the field the previous counting behaviour is kept unchanged, including its
  limitations.

  Binance Spot and Margin recovery is unaffected and unchanged: REST `myTrades` reports executions
  only, with no cumulative, so a recovered fill there still reports `None` rather than a figure
  invented from a second endpoint. Both clients now say so at the call site, and
  `Trade::order_filled_quantity` documents which producers report it.

- **A Binance Spot fill's order snapshot now reaches the consumer** (`rustrade-execution`). The
  account stream deduplicates events before forwarding them, and an order snapshot's dedup key was
  the order's exchange id alone. An order's acknowledgement and every fill against it carry that one
  id, so the acknowledgement — which reports nothing filled — suppressed each fill snapshot behind
  it and `Open::filled_quantity` never moved. The defect fixed below was therefore corrected in the
  converter and undone one stage later, on Binance Spot only; Alpaca derives dedup keys from
  executions alone and was unaffected.

  The key now carries the order's cumulative filled quantity, so successive states of one order stay
  distinct while a genuinely re-delivered frame still collapses. That suppression is load-bearing
  rather than an optimisation: a replayed acknowledgement for an order that has since retired is a
  partially-filled snapshot for an untracked order, which the engine inserts as a live resting order.

  Every test covering the change below drives the converter directly, which is upstream of the gate
  that discarded its output. The new tests drive a frame through convert-then-deduplicate — the seam
  where this failed.

- **A REST order snapshot is stamped with when the order last changed, not when it was created**
  (`rustrade-execution`, Alpaca and Binance Spot). The engine orders an order's states by
  `Open::time_exchange` and discards a snapshot older than the state it already tracks. Both clients
  stamped a fetched order with its creation time, which is identical across every snapshot of that
  order — so once a WebSocket fill had advanced the tracked order, a reconciliation fetch was thrown
  away as stale, silently, for exactly the partially-filled orders it exists to repair. Reconciling
  order state after a reconnect is a documented caller obligation on both clients, and it had stopped
  discharging anything.

  Both clients now prefer the venue's last-update field (Binance `updateTime`, Alpaca `updated_at`),
  falling back to creation time only where the venue omits it. `Open::time_exchange` and
  `Open::filled_quantity` now document what a producer must put in them.

- **A WebSocket partial fill now advances `Open::filled_quantity`** (`rustrade-execution`,
  Alpaca and Binance Spot). Both clients mapped one venue frame to at most one `AccountEvent`, and
  `filled_quantity` is only ever carried into engine state by an order snapshot. The fill arms
  emitted the execution and nothing else, so `filled_quantity` was written only when the order was
  first acknowledged — where it is `0`. Between acknowledgement and the next REST reconciliation, a
  half-executed order read as having its full original quantity still working, and anything sizing,
  netting or risk-checking off `quantity_remaining()` over-counted by the amount already filled.

  A fill frame genuinely carries two facts — the execution print and the order's new cumulative
  filled quantity — so it now maps to both a `Trade` and an `OrderSnapshot`. The execution is always
  emitted first: a fully-filled snapshot retires its order, and routing a fill against an order that
  has already been retired is a strictly harder problem than routing it against a live one.

  This also settles a completed order. A `fill` (not just a `partial_fill`) reports nothing left to
  fill, which is how the engine learns the order is done; previously neither client's WS path ever
  moved an order out of `Open`, leaving a resting order that no longer existed at the venue until
  REST reconciliation removed it.

  A fill is written back as an `Open` snapshot only when the frame's own order status says the order
  was still working — `partially_filled`/`filled` on Alpaca, `PARTIALLY_FILLED`/`FILLED` in Binance's
  `X` field. A fill arriving after its order's terminal frame therefore still reports its execution,
  but cannot resurrect a retired order as a resting one. IBKR and Hyperliquid were never affected:
  both venues send order state and executions as separate messages, so each already mapped to its own
  event.

  The two internal converters return `[Option<UnindexedAccountEvent>; 2]` rather than
  `Option<UnindexedAccountEvent>`. Both are private; no public API changed.

- **A Hedging fill arriving after its order retired now reaches the position the strategy chose**
  (`rustrade`). In `OmsMode::Hedging`, routing-table lifetime was coupled to membership of
  `InstrumentState::orders`: retiring an order pruned the `exchange_id → ClientOrderId → PositionId`
  chain that a fill arriving *after* its terminal snapshot needs, so the fill opened a position under
  its raw exchange `OrderId` and that order's PnL was split across two slots with nothing to rejoin
  them. `update_from_order_snapshot` compensated — but by restoring those entries into the same live
  maps, so the next retirement pruned them again. The window held **one** order per instrument, and
  only for an order retired as `Open`-with-nothing-left or `FullyFilled`. A **cancelled** order got no
  compensation at all, so an IOC's fill reported after the cancel that closed it never routed — which
  is the case `Cancelled::filled_quantity` exists for.

  `cleanup_routing_tables` now **demotes** that chain into a new `InstrumentState::retired_routing`
  instead of dropping it, and `update_from_trade` consults it once both live lookups have missed. The
  window spans `MAX_RETIRED_ROUTING` (64) retirements per instrument, evicting oldest first; a fill
  beyond that bound falls back exactly as before.

  Only a chain that resolves end to end is demoted, and that is what keeps the corporate-action split
  path out of it. That path prunes `position_ids` **by value** while leaving the order resting, so the
  join finds no `PositionId`, nothing is kept, and a late fill there still reaches the fallback —
  deliberately, because routing it would reopen a position a reverse split floored to zero. The
  one-order compensation is retained rather than replaced: an order that fills fully on ack has no
  `exchange_id_to_cid` entry at the moment `cleanup_routing_tables` runs, so there is nothing to
  demote, and the entries it restores are what the *next* retirement demotes.

  Two consequences beyond the split itself are also closed. Because both live lookups missed, the fill
  reached the no-order-matched arm and was counted under `TearSheet::fills_unmatched` — conflating it
  with a genuinely external fill, for which one position per order is a defensible reading, and
  leaving the split-position banner silent on a real split. And before falling back, that arm buffers
  a fill into `pending_fills` whenever any *other* order is `OpenInFlight`; a buffered fill is only
  released by an ack for an order still tracked in `orders`, so a fill for an already-retired order
  was never replayed and never dropped, growing that `Vec` for the rest of the run. The retired map is
  consulted before the buffer, so neither happens.

  `retired_routing` is `#[serde(default)]` so existing snapshots load, stays empty under
  `OmsMode::Netting` where one position key makes the failure unreachable, and is cleared alongside
  the other routing tables on contract expiry in both the live handler and the audit replica.

- **Hyperliquid trade ids identify the fill rather than the transaction** (`rustrade-execution`).
  A `Trade`'s id came from `fill.hash`, which is the L1 transaction hash. One aggressive order
  sweeping several resting orders produces several fills under a single hash, so those fills all
  arrived carrying the same `TradeId`. Anything reconciling on it — which is what a caller does with
  `fetch_trades` after a reconnect — treated a multi-level sweep as one fill and dropped the rest,
  understating both filled quantity and fees. The id is now the venue's `tid`, which is per-fill.

  On the REST path this required parsing `userFills` into this client's own `UserFill` rather than
  `hyperliquid_rust_sdk`'s `UserFillsResponse`, which models neither `tid` nor `feeToken` and — with
  no `deny_unknown_fields` — discards both silently. The request goes through the SDK's own HTTP
  client, so base URL, TLS and error mapping are unchanged. A response without `tid` now fails
  loudly instead of falling back to the hash, because a silent fallback restores the defect
  invisibly.

  Hyperliquid documents `tid` as unique per fill but qualified by coin rather than globally, so
  callers reconciling across instruments should qualify it the same way.

- **Hyperliquid spot fills report the fee asset the venue charged** (`rustrade-execution`). The REST
  path inferred it from the side — base for a buy, quote for a sell — because the SDK's response
  type does not expose `feeToken`. It is available now that this client parses the response itself,
  and the inference survives only as a fallback. The stream path already used `feeToken` and is
  unchanged. The quote-equivalence test alongside it became case-insensitive, matching the stream:
  the fee asset is no longer a substring of `coin` and so is no longer byte-equal to the quote asset
  by construction.

- **Hyperliquid deduplicates fills on the account stream** (`rustrade-execution`). The `userFills`
  subscription opens with a snapshot of recent fills and the SDK resubscribes on reconnect, so every
  reconnect redelivered fills already sent. A trade is a delta the consumer accumulates, so each
  redelivery double-counted filled quantity and fees. The module documentation claimed
  deduplication was "SDK-managed; no custom dedup cache needed", which was never true.

  Order updates are deliberately not deduplicated: they assert absolute state, so a replayed one is
  idempotent while a dropped one could strand a consumer on stale state. `fetch_trades` does not
  deduplicate either — it answers the window it was asked for.

  This had to land with the id fix rather than after it. Keyed on the old hash-derived id, the cache
  would have discarded the second and subsequent fills of every sweep.

- **A replaced resting order no longer leaks the balance held against it** (`rustrade-execution`).
  Re-opening a `ClientOrderId` that was already resting replaced the order under it, and
  `OpenOrders::insert` returns the displaced order's reservation precisely because it is still held
  against the account — but `SimulatedVenue` discarded it. Nothing else would ever release it, so
  `free` stayed down and `Balance::used` overstated by that amount for the rest of the run,
  silently. It is now released, and `OpenOrders::insert` is `#[must_use]` so it cannot be dropped
  again without a warning.

- **A resting order that arrived part-filled no longer settles and prints its whole quantity**
  (`rustrade-execution`). `SimulatedVenue`'s matching built its settlement, its `Trade` and its
  terminal snapshot from an order's full `quantity`, ignoring the `Open::filled_quantity` the order
  carried. An order that reached the book with part of its quantity already done — which a
  configured `initial_state` copied out of a live account may seed — therefore paid for that part a
  second time in the balance ledger, and printed a trade larger than the quantity that was left to
  trade. It now settles and prints the remainder alone.

  A fill that completes such an order also reports no `avg_price`. This venue struck one of the
  fills behind the order's total and never saw the other, so its own limit is that fill's price
  rather than the mean of both — `Filled::avg_price` is optional for exactly that, and a consumer
  needing the mean has both trades to compute it from. An order that reached the book with nothing
  done still reports the price it filled at, unchanged.

  Resting holds the same invariant from the other side: an order is booked reserving against its
  unfilled remainder and carrying what it has already done, so what is held and what is later
  settled are the same quantity at the same price. Both are the whole quantity for every order this
  venue books itself, which fills nothing before it rests — so no existing result moves.


- **An order reported as fully filled by an `Open` snapshot is no longer tracked as active**
  (`rustrade`). `OrderManager`'s `Open` → `Open` transition overwrote the tracked state without
  checking whether anything remained to fill, so an order the engine held as `Open` that received a
  snapshot with `filled_quantity == quantity` stayed active indefinitely.

  The check was already applied on the two other transitions that accept an `Open` update — an
  untracked order declines to be inserted, and an `OpenInFlight` order is removed — so the arm for
  an order already `Open` was the one place a completed order could survive. The consequence is a
  phantom resting order: a strategy asking whether it already has an order working answers yes
  forever, a later cancel is rejected as unknown or terminal, and the entry never leaves the map.

  Not reachable from a simulated venue, which reports a completed market order as `FullyFilled`
  directly and so never holds it as `Open` first. A live venue may legitimately report completion
  as an order snapshot rather than as a distinct state, which is how the gap arises in practice.

  The staleness gate still outranks the new check: an out-of-sequence snapshot claiming a full fill
  does not retire an order that is still live.

- **A fill that arrives after its own order's terminal snapshot now routes to the strategy's
  chosen `PositionId`** in `OmsMode::Hedging` (`rustrade`). Routing-table lifetime is coupled to
  membership of the active-order map, so retiring an order dropped the
  `exchange OrderId → ClientOrderId` entry that a later fill for that order needs.
  `InstrumentState::update_from_order_snapshot` already compensated when the order was retired
  straight out of `OpenInFlight`; it now does the same when a terminal update retires an order the
  engine was tracking as `Open`.

  Both orderings occur. A venue may report the fill before the terminal state or after it: IBKR
  reports a working order status carrying `filled == quantity` while deliberately withholding the
  trade until the matching commission report lands, and REST reconciliation can serialise an order
  that completed mid-request on any venue. Before this change such a fill fell through to a raw
  `OrderId` position key — opening a position the strategy never asked for — or, if another order
  was in flight at the time, was parked awaiting an ack that would never name it and was lost
  without trace.

  The fix that precedes it is what exposed this one. Retiring a fully-filled `Open` order is
  correct, but it removed that encoding's accidental immunity: while the completed order stayed
  tracked, its routing entry stayed with it. The explicit `FullyFilled` encoding — what every
  in-tree integration emits — never had that immunity and mis-routed already.

  **This narrows the window rather than closing it.** Exactly one retired order per instrument
  keeps its routing entry, and the next order update on that instrument prunes it, which is what
  keeps both maps bounded. A fill for an order retired before the most recent one still opens a
  position under the raw `OrderId` with a warning — see the known-limitation note on
  `InstrumentState::update_from_order_snapshot`. Closing the class requires decoupling routing-table
  lifetime from the active-order map, which is not attempted here.

  `OmsMode::Netting` is unaffected throughout: it resolves every fill to `PositionId::NETTING`
  without consulting either routing table. It does share the tracking fix above.

- **The `backtests_concurrent` example runs again** (`rustrade`). It panicked at startup, before the
  first backtest, with `MarketDataInMemory events must be sorted ascending by
  MarketEvent::time_exchange`. `MarketDataInMemory::new` requires and asserts global time ordering
  because the backtest merges market and auxiliary events onto one timeline; the recorded
  three-instrument capture the example reads is interleaved and holds over ten thousand inversions,
  so it never satisfied that. The example's `market_data_from_file` helper now sorts (stably) before
  constructing, which is what the benches reading the same file already did, and its rustdoc explains
  why any substituted capture needs the same. Also asserts the corpus is `Item`-only, since the sort
  key collapses `Reconnecting` to `None` and would otherwise hoist a reconnect to the front of the
  run. Introduced when the assertion arrived with the auxiliary-event seam; examples are linked by
  CI but not executed, so nothing caught it.

- **A simulated venue's opening balances are stamped at the session start** (`rustrade`), rather than
  carrying whatever `time_exchange` the configured initial state recorded. `SimRunner` queues each
  venue's seeding snapshot ahead of every source event, but the `HistoricalClock` is seeded from the
  market and auxiliary sources — so a balance captured before the replayed data, which is the
  ordinary case, arrived behind a clock already wound forward to the first market event. That was
  reported as `HistoricalClock received out-of-order events` at **ERROR**, once per venue per run,
  for a configuration that was never wrong: `time_engine_start` is derived from the dataset at run
  time, so no static configuration can be written to agree with it.

  A seeding snapshot is not an observation on the simulated timeline — it is the account's opening
  condition, and the session opens at the clock's instant by definition. Orders carried in a
  configured initial state keep their own stamps.

  **This corrects a reported statistic.** `DrawdownGenerator::init` takes its first peak's instant
  from the first equity point, which is the seeding balance — so every drawdown was measured from
  whenever the configured balances happened to be captured, not from the session start. On the
  three-minute example fixture, whose configured stamp sits 25.6 hours before the data,
  `TearSheetAsset::drawdown_mean` reported a mean drawdown duration of **92,304,199 ms (25.6 hours)
  over a 185-second backtest**; it is now **83,484 ms**. `Drawdown::time_start` and the
  `MaxDrawdown` window move with it. Drawdown *magnitudes* are unaffected, as are fill prices, PnL,
  fees and closing balances. The affected backtest examples still print
  byte-identical tear sheets — `TradingSummary::print_summary` renders drawdown magnitudes only,
  never a duration or a window start — so the wrong figure was reachable through the struct rather
  than through the printed table.

- **Backtests are reproducible: the same dataset and strategy now produce the same result, run to
  run** (`rustrade`). `backtest()` previously assembled its engine through `SystemBuild`, which
  inserts two forwarding tasks feeding one unbounded channel, and an always-ready market source
  raced arbitrarily far ahead of the engine on another task. Account events therefore landed at a
  scheduler-determined index within the market stream. Balance and position *quantities* survived
  that, because they do not depend on where in the sequence a fill lands — every time-derived
  statistic did not. A fill could be delivered after the market events that should have marked it,
  leaving a position that was never priced: a terminal `pnl_unrealised` of zero for a position the
  run genuinely held, a `Position::time_exchange_update` of the entry instant, tear-sheet series
  drawn against the wrong bars, and an order count that varied between runs of the same input.

  `backtest()` no longer uses `SystemBuild`. It polls a `SimRunner` inline instead, so fill
  placement is a function of the dataset alone. Fifty runs of the same fixture now produce one
  outcome where they previously varied. Paper trading is unaffected: the asynchronous
  `MockExchange`/`MockExecution` path is untouched, and only `backtest()` switches. (#289)

- **No wall-clock timing remains on the backtest path** (`rustrade`). The simulated venue's latency
  was a real `tokio::time::sleep`, and `ExecutionBuilder` applied a 1-second wall-clock timeout to
  each dummy execution request. Both made a run's *outcome* depend on how fast the machine executing
  it happened to be, which is the same class of defect as the non-determinism above rather than a
  performance note. `SimRunner` applies latency as an offset on its queue key and has no timeouts
  at all. `shutdown_after_backtest` and `AFTER_DRAIN_DEADLINE` remain public and still serve the
  asynchronous path. (#280)

- **A backtest can no longer end while a fill it provoked is still owed** (`rustrade`). Exhausting
  the market source now yields `Shutdown::AfterDrain` before the queue is drained, so the `Engine`
  stops generating new orders while everything already booked is delivered in full, and the stream
  ends only once nothing is outstanding. Suppression happens at the `Engine` rather than by
  discarding requests at the venue, so a request that was accepted is always answered.

- **A simulated account snapshot ordered one instrument's orders arbitrarily** (`rustrade-execution`).
  `account_snapshot` sorts the account's open and cancelled orders with an *unstable* sort keyed on
  `instrument` alone, then chunks by instrument. Every order on one instrument therefore shares a
  key, and an unstable sort leaves tied elements in whatever order they arrived in — here, a hash
  map's iteration order. Two snapshots of the same account could list the same orders differently,
  which makes them incomparable between runs and makes any golden hash over one a flake. The key is
  now `(instrument, cid)`; `cid` is unique per order, so it is total.

- **`MockExchange` emitted each fill from its own task, so absolute balance snapshots could reach
  the client out of the order the venue booked them** (`rustrade-execution`). A mock balance is a
  full restatement rather than a delta — successive fills report `9_999_500`, `9_999_000`,
  `9_998_500` — so applying them out of order does not merely reorder history, it yields the wrong
  balance. Every filled open was handed to its own `tokio::spawn`; at `latency_ms: 0` those tasks
  all became runnable at once and raced, and the snapshot that arrived last won rather than the one
  booked last. Fills are now queued and drained by a **single** emitter task, so emission order
  equals booking order across fills, not merely within one — each fill's latency is still measured
  from the instant it was booked, so queueing adds no delay of its own. `MockExchange::run` closes
  the queue and awaits the emitter before returning, so shutting the venue down cannot strand a
  booked fill.

  This was invisible in practice because the timestamp each snapshot carries derives from
  `HistoricalClock`, which mixes in wall-clock time and so happened to hand every snapshot a unique,
  strictly increasing value — letting the engine's staleness guard discard the out-of-order ones and
  leave the correct balance standing. **Correctness rested on that accident**, which is a bug of its
  own ([#280](https://github.com/Niqnil/rustrade/issues/280)) that any fix to simulated-time
  reproducibility removes; with a pure clock, fills booked between two market events share a
  timestamp, the guard can no longer discriminate, and a backtest reports a wrong final balance. The
  ordering is the venue's contract to keep, because by the time the engine sees two snapshots out of
  order the information needed to sequence them is already gone. Regression test asserts the
  broadcast order directly: it fails on every run without the fix. (#294)

- **A backtest's fills were truncated non-deterministically at shutdown** (`rustrade`). Every run of
  the same deterministic backtest could end in a different state. A filled order produces three
  separate things — the balance it debits, the `Trade` it consists of, and the response reporting it
  filled — and only the last of those clears the request from flight. Ending the run there cut off
  whatever had not yet been read, and because the response channel and the account stream were
  merged round-robin, the two ledgers were truncated by *different* amounts: observed runs debited
  three fills' worth of quote while holding two fills' worth of position, and one debited without
  opening a position at all. Balances, position quantity, trade counts and trade arrival order all
  varied run to run. Fixed by the ordering and shutdown-sequencing changes described above; the
  regression test asserts that quote debited equals the notional the position ledger holds, which
  failed on roughly a third of runs before. Fill *timing* within the market feed is a separate,
  still-open defect ([#289](https://github.com/Niqnil/rustrade/issues/289)), so anything derived
  from when a fill was priced against the feed — `pnl_unrealised`, `time_exchange_update`,
  tear-sheet series — is not yet deterministic. (#281)

- **Fills were silently lost when a venue acknowledged an order as already filled**
  (`rustrade`). In `OmsMode::Hedging`, a `Trade` that arrives before the order acknowledgement is
  parked in `pending_fills` and replayed once the acknowledgement resolves the exchange `OrderId`.
  That replay was armed only on the `OpenInFlight -> Active(Open)` transition, so an
  acknowledgement whose terminal state was `Inactive(FullyFilled)` never triggered it and the fill
  was never applied. This is not an edge case: a venue that answers the REST open with an
  already-filled order reports the fill and the acknowledgement in one response and never publishes
  an intermediate `Open` — Binance (`newOrderRespType=FULL`), Alpaca and IBKR all do this for
  marketable orders — and the corresponding websocket `Trade` normally wins the race against the
  REST round trip. The position therefore never opened while the balance was still debited, leaving
  the two ledgers disagreeing for the rest of the session, reported only as an unreplayed entry in
  `pending_fills`. `Netting` mode was unaffected, since it resolves `PositionId::NETTING` without
  consulting orders at all.

- **Backtests discarded every order response** (`rustrade`). No backtest could observe a fill, a
  rejection, a balance update or a trade: each run reported a tear sheet of zeros indistinguishable
  from a strategy that chose to stay flat. The `Engine` reads market and account events from a
  single FIFO feed, and `System::shutdown_after_backtest` enqueued its stop as soon as the market
  stream had been *forwarded* into that feed rather than *processed* out of it — so the stop sat
  ahead of every account event the run was about to produce, and `sync_run` terminated on it
  without ever reading them. Reproduced identically at `latency_ms: 0`, so the mock exchange's
  wall-clock latency model was not the cause. Ordering alone cannot fix this, because the responses
  are provoked *by* processing the final market events and are therefore always behind any marker
  placed after them; the fix is a quiescence barrier — see `Shutdown::AfterDrain` under *Changed*.
  This is also why the mock exchange's missing fill price went unnoticed: the rejection it produces
  was being generated all along and then discarded here.

- **Twenty-one rustdoc link defects across the workspace** (`rustrade`, `rustrade-execution`,
  `rustrade-integration`), each of which rendered as dead or misdirected text on docs.rs. **Six**
  resolved to nothing: `EngineOutput::Commanded` and `EngineOutput::AlgoOrders` pointed at
  `super::command::Command` / `super::audit::ProcessAudit`, but those docs live in `crate::engine`,
  where `super::` is the crate root — both items are already imported in that module, so the bare
  labels resolve; and `rustrade-integration`'s `corporate_action` module doc referenced `SmolStr`,
  `StockSplitSource` and `CorporateAction` by bare name, which does not resolve there because that
  module's documentation is split between the `pub mod` statement in `lib.rs` and the file's own
  `//!` block. **Five** pointed public documentation at private items a reader cannot navigate to
  (`DEFAULT_HTTP_REQUEST_TIMEOUT`, `ContractConfig::to_contract`, and the two `assert_aux_*` harness
  checks named in `AuxEventSource`'s caller obligations), and are now plain code spans that still
  name the mechanism. **Ten** carried an explicit target the label already implied. Documentation
  only — no behaviour change.

- **CI now gates rustdoc links.** No job ran `cargo doc` with denied lints, which is why the above
  accumulated unnoticed — and why only `rustrade` had ever been checked at all, leaving the defects
  in `rustrade-execution` and `rustrade-integration` invisible. A `cargo doc` job now denies
  `broken_intra_doc_links`, `private_intra_doc_links` and `redundant_explicit_links` across the whole
  workspace. It runs `--all-features` deliberately: `rustrade-execution` has `default = []` with
  every client feature-gated, so a bare `cargo doc` documents none of that code and would pass
  vacuously.

- **`reconcile_venue_roles` now re-derives `ConnectivityStates::global` from the roles it
  corrected** (`rustrade`). `global` is a cached aggregate over `ConnectivityState::all_healthy`,
  which is a *function of* `ConnectivityState::role` — so correcting a role silently changes what
  the cached value should already have been, and the event paths that normally maintain it cannot
  repair the difference, because each short-circuits once its own dimension is `Health::Healthy`. On
  a state whose connections had already reported in, a role change therefore stranded `global` wrong
  in whichever direction it moved, permanently and with no warning: **stale-`Healthy`** when a role
  widened, where a consumer gating on `global` trades on for the rest of the run against a link that
  never came up, or **stuck-`Reconnecting`** when one narrowed, where every health-gated strategy
  stays gated for the whole run — the same footgun the function was added to remove. Reachable
  through public API: `BacktestArgsConstant::engine_state` is caller-supplied, and `backtest` returns
  its terminal `engine_state` precisely so it can be inspected and reused, so chaining one window's
  terminal state into the next window's args (a walk-forward sweep) hits it whenever the execution
  client set differs between windows. The freshly-built path is unaffected — every connection starts
  `Reconnecting`, so the re-derive is a no-op there. An **empty** `ConnectivityStates` re-derives to
  `Reconnecting` whatever it arrived with — a convention on a degenerate input rather than a safety
  property, since neither `Health` variant is honest over zero venues and a venue-less state holds no
  instrument to protect. `Reconnecting` is chosen to agree with
  `generate_empty_indexed_connectivity_states`, which already returns it for a zero-exchange
  collection, and to leave the postcondition total: on return `global` is `Healthy` iff the state is
  non-empty and every venue in it is healthy under its corrected role.

- **`generate_empty_indexed_connectivity_states` now warns on an empty venue set** (`rustrade`). A
  venue-less engine leaves `global` at `Reconnecting` forever, which a consumer gating on it cannot
  distinguish from a routine reconnect — so an empty configuration presents as an indefinite wait
  with nothing naming the cause. It now reports the misconfiguration directly, matching the existing
  warning for a venue that provides neither market data nor execution.

- **`ConnectivityState::market_data`'s rustdoc no longer overstates it as a role-mismatch detector**
  (`rustrade`). It claimed that a market event arriving for a venue declared `ExecutionOnly` would
  set the field `Healthy` and so surface the wrong role. That holds only before
  `ConnectivityStates::global` reaches `Healthy`: after convergence `update_from_market_event`
  short-circuits on the cached aggregate and returns before writing, so a role error whose first
  market event arrives later is not observable through this field. Documentation only — no behaviour
  change.

- **A disconnect on a dimension a venue does not provide no longer wedges global connectivity**
  (`rustrade`). `ConnectivityStates::update_from_account_reconnecting` and
  `update_from_market_reconnecting` checked only that the `ExchangeId` was *tracked*, then wrote
  `ConnectivityStates::global = Health::Reconnecting` unconditionally, without consulting the
  venue's `VenueRole`. In a split configuration both venues are single-dimension, so a
  `MarketStreamEvent::Reconnecting` naming the **execution** venue — or an
  `AccountStreamEvent::Reconnecting` naming the **data** venue — degraded global health over a
  connection that venue never had, and nothing could then restore it: no event can arrive for a
  dimension a venue does not provide, and `update_from_account_event` / `update_from_market_event`
  each return early once their own dimension is already `Healthy`, so the convergence check never
  re-ran. Every strategy gated on global health stayed gated for the remainder of the session, after
  a single `warn!` line. Both paths now re-derive `global` through `ConnectivityState::all_healthy`,
  which does not consult a dimension the role does not own, and `warn!` the role mismatch itself as
  a misconfiguration in its own right. The market-side case is reachable without any wrong role at
  all: a `SystemBuild`'s market stream is caller-supplied and forwarded verbatim, so one built per
  `IndexedInstruments::exchanges()` rather than per *data* venue emits exactly this on its first
  reconnect.
  The aggregate is now derived in **one** place, shared by both disconnect paths, both event paths
  and `reconcile_venue_roles`, so the cached value cannot disagree with the states it summarises.
  One log change follows from that: a `global` transition is reported as
  `"EngineState updating global connectivity"` with its previous and new value, in both directions,
  replacing a message emitted only on the transition to `Healthy`.

- **The three `UntrackedExchange` rejection paths now log** (`rustrade`). Each returned
  `Err(UntrackedExchange)` silently, while the *routine* disconnect on the adjacent line logged a
  `warn!` — so the anomaly was quieter than the ordinary event it replaced. The structured
  observable (`EngineOutput::UntrackedExchange`) rides the audit stream, but only the
  `*_with_audit` runners retain per-event ticks: a consumer on `sync_run`/`async_run` got **no**
  signal at all, which is a differently-shaped version of the intermittent blind spot this release
  set out to close. The level is chosen by how often each path can fire: `warn!` on
  `update_from_account_reconnecting` and `update_from_market_reconnecting`, which are bounded by the
  disconnect rate and now match the level of their tracked-venue counterparts, and `debug!` on
  `update_from_market_event`, which fires once per market event and at `warn!` would emit at tick
  rate for a stream wired to an unconfigured exchange. A consumer that must not miss this on a
  non-audit runner should enable `debug!` for `rustrade::engine::state::connectivity` or run an
  `*_with_audit` variant and count the output.

- **`backtest` now derives venue roles from the execution clients it builds** (`rustrade`), instead
  of requiring the caller to declare them on the state it is handed. A backtest that priced an
  instrument on one venue and executed on another had that pricing venue approximated as
  `VenueRole::Both` unless the caller passed `EngineStateBuilder::execution_venues`, so it waited
  forever on an account connection nothing would establish, held `ConnectivityStates::global` at
  `Health::Reconnecting`, and gated every strategy that consults global health for the entire run —
  without erroring. `backtest` builds its own execution clients per run, so it now reconciles the
  roles on its own clone of the state (see `reconcile_venue_roles`); the caller's `engine_state` is
  still never modified, and declaring the venues upfront remains supported and reconciles to
  itself. A venue that neither prices an instrument nor has an execution client still cannot
  converge — it is now reported with a warning naming it, rather than left to be inferred from a
  `global` that never leaves `Reconnecting`.

- **LSE candle pages are now checked for ordering and resolution instead of assumed correct**
  (`rustrade-data`, `lse` feature). Two provider behaviours the module compensates for were pinned
  by documentation only. Each page is now scanned in a **pre-pass** before any bar is yielded, and
  two new `LseError` variants reject the whole page:
  - `NonMonotonicCandlePage` — ascending order is how the vault answers today, not something it
    guarantees, and the pagination cursor depends on it: the cursor advances past the newest open a
    page carried, so a page served newest-first would advance it to the end of the range, make the
    next page empty (the documented end-of-data signal), and return `Ok` with 5,000 of 3,000,000
    bars. `previous_open` is carried **across** pages, so the check spans the seam.
  - `UnexpectedCandleResolution` — the vault answers `200` to a misspelled `timeframe` and silently
    defaults to 1-minute bars in a byte-identical response shape. A `Day1` request would then
    receive 1,440 one-minute rows per day, each stamped `close_time = open + 24h` by this module's
    own boundary arithmetic: overlapping, wrong, and invisible to every other check, since the range
    bounds are still satisfied and the series still ascends. Spacing is the one property that
    distinguishes it — consecutive bars of a compliant fixed-interval series are exactly one
    interval apart, and a gap only ever makes spacing *wider*. Checked for
    `IntervalStep::Fixed` resolutions only: a calendar step (`month`, `quarter`, `year`) has no
    single width to compare against, and February would false-positive against a 31-day one.
  Structural properties of a page reject the whole page; the per-row *range* trim stays in the yield
  loop, so a well-formed page still yields its in-range bars before failing. `LseError` is
  `#[non_exhaustive]`, so both variants are additive.

- **The Parquet decoder's monotonicity rule is now layout-dependent** (`rustrade-data`,
  `lse-parquet` feature). The rule was `<=` for every layout — correct for a tick or quote tape,
  where several prints routinely share a microsecond, and wrong for a **candle** artifact, where two
  bars of one series cannot share an open and therefore cannot share the close derived from it. A
  tie there means the file holds more than one series, or repeats a row. Candle artifacts at a fixed
  resolution are now required to be strictly ascending; tapes still permit ties, and so do calendar
  intervals — month arithmetic clamps day-of-month, so `2024-01-30` and `2024-01-31` both close on
  `2024-02-29` and strictness would reject a valid file.
  Ordering alone cannot catch a mis-declared `interval` — a fixed interval is a constant shift from
  open to close, so it cancels out of that comparison and a file of 1-minute bars declared `Day1`
  still ascends strictly. That is now a separate check; see below.

- **⚠️ The Parquet decoder now rejects a candle artifact finer than the resolution it was decoded
  at** (`rustrade-data`, `lse-parquet` feature). Nothing in a Parquet artifact records the
  resolution it was written at, so `read_export` derives every `close_time` from a
  caller-supplied `interval` — cross-checked only by `verify_job_covers_request`, which is skipped
  when the job echoes no timeframe and absent altogether for `LseExport::new`, the documented entry
  point for a file obtained out of band. Declaring `Day1` over a file of 1-minute bars therefore
  produced 390 "daily" bars per trading day, each claiming a 24-hour period overlapping the next
  1,439, ascending and without error.
  `read_export` now measures **bar spacing** against the declared interval — the same detector the
  vault path runs, raising the same `LseError::UnexpectedCandleResolution`. Consecutive bars of a
  compliant fixed-interval series are exactly one interval apart, and a gap only ever makes spacing
  *wider*, so spacing narrower than the declared interval means the file is finer than it was said
  to be. Compared on the opens the `ts` column carries rather than the closes the decoder derives,
  so the error names instants a caller can find in their own file.
  **This is a new rejection path on a public decode API**: an artifact that previously decoded may
  now yield an error mid-iteration.
  Its reach, which the rustdoc now states rather than leaving implied: it catches **finer than
  declared**, and cannot catch **coarser** than declared — daily bars decoded as `Min1` are spaced
  wider than 60 seconds, indistinguishable from a genuine gap, and each bar then enters the timeline
  a minute after its day opened rather than at the end of it, a full day of lookahead. It also
  cannot fire on a single-row artifact, and calendar intervals (`Month1`) are exempt for the same
  reason they are exempt from strict ascent.

- **The LSE GBX protection now runs on the candle-replay path, and is stated as the export path's
  caller obligation**
  (`rustrade-data`, `lse` feature). `lse::market::quote_asset` computed the right answer but had no
  caller outside its own tests, so a user registering `BP.L` with quote asset `gbp` got pence booked
  as pounds — `BP.L` prints ~548 where BP trades around £5.48, so every notional, fee, unrealised
  PnL and balance was 100× wrong with nothing downstream able to notice. The check now runs inside
  `instrument_index_for`, where the provider's view of a symbol and the caller's registry meet, and
  a disagreement is the new `LseError::QuoteAssetMismatch`. It compares the instrument's **pricing**
  asset (honouring `InstrumentQuoteAsset::UnderlyingBase`) on `AssetNameInternal`, the identity the
  engine keys assets by — so `GBX` and `gbx` are one asset and `GBP` is caught.
  The check reaches exactly as far as its callers: the new `LseCandleSource::resolve` routes through
  it on the candle-replay path, while `read_export` takes its `instrument` argument on trust — its
  rustdoc now directs callers to obtain that argument here. Both shipped examples resolve rather
  than writing an `InstrumentIndex` literal. `LseCandleSource::new` remains for callers with no registry to check against, and its
  rustdoc says plainly that it carries no protection.

- **⚠️ BREAKING: `instrument_index_for` moved from `lse::parquet` to `lse::market`**
  (`rustrade-data`, `lse` feature). It is registry symbology rather than artifact decoding, and its
  old home is gated behind `lse-parquet` — so a consumer building with `--features lse` got the
  entire candle-replay API and **could not compile a call to the guard at all**, which is why the
  candle path had no protection to route through. Update imports from
  `exchange::lse::parquet::instrument_index_for` to `exchange::lse::market::instrument_index_for`;
  the signature and behaviour are unchanged.

- **`LseVaultClient::await_export` could panic on a large `poll_interval`** (`rustrade-data`, `lse`
  feature). The deadline was computed with `checked_add`, commented that `Instant + Duration` panics
  on overflow and that `Duration::MAX` is a plausible "no timeout" sentinel — and the next line then
  used a bare `+` on the caller's `poll_interval`. `.min(deadline)` could not protect it, because
  the addition is evaluated first. A public library method therefore aborted the caller's task on an
  argument its own docs called plausible. Now `checked_add(...).map_or(deadline, |i| deadline.min(i))`,
  with both sentinels covered by the rustdoc and by tests.

- **Two per-row costs removed from the LSE Parquet decode loop** (`rustrade-data`, `lse-parquet`
  feature). The `price`+`ask` layout opened and decoded an entire extra `DOUBLE` column — around
  400 MB uncompressed on a 50M-row artifact — solely to check that `volume` stayed `0.0` for a
  one-shot `warn!`, and the check latched off after the first hit while the column kept being read.
  That check now reads the row group's column-chunk **statistics** instead of decoding per row. And
  the symbol column was cloned per row: `ByteArray` wraps `Option<bytes::Bytes>`, so a
  dictionary-backed clone plus its drop is two atomic read-modify-writes — roughly 100M of them on a
  50M-row export — for a value that was only compared against the expected symbol's bytes and
  dropped. `BatchedColumn::take` is now a `peek`/`advance` split, which removes the clone
  (`symbols_in_export` uses it too). The same rows decode to the same events. One behaviour change,
  recorded on the method: the statistics-based check cannot see a row group whose writer emitted no
  column statistics, nor a NaN — writers exclude NaN from `min`/`max` by spec, where the old per-row
  `value != 0.0` caught it. Both are accepted losses on an observability aid for a column the layout
  discards either way.

- **`transfer_export` no longer writes an unbounded body when the job reports its size**
  (`rustrade-data`, `lse` feature). A misbehaving proxy streaming 500 GB against a job declaring
  1 MB filled the disk before the integrity check could fire, and on `ENOSPC` the oversized `.part`
  was *retained*. The write loop now stops once the download exceeds `job.bytes`. The offending
  chunk is written **before** the break deliberately, so the caller observes `downloaded > expected`
  and takes the existing "longer than the artifact → corrupt → discard" branch rather than reporting
  a misleading digest mismatch.

- **`lse::market::quote_asset` returned USD for a lowercase venue suffix, the 100× error it exists
  to prevent** (`rustrade-data`, `lse` feature). The venue arms were exact string literals, so
  `quote_asset("bp.l")` fell through to the USD default while `"BP.L"` correctly returned `GBX`.
  London listings are quoted in **pence**: `BP.L` prints ~548 where BP trades around £5.48, so a
  mis-cased symbol inflated notional, fees, unrealised PnL and every balance by 100×, silently and
  with no log. All seven venues were affected (`.L`, `.T`, `.HK`, `.NS`, `.AX`, `.KS`, `.TW`). The
  suffix is now case-normalised before matching, as the sibling pair-shaped and `underlying` paths
  already were.

- **`lse::market::slug` accepted an ambiguous symbol spelled with a lowercase `.f`**
  (`rustrade-data`, `lse` feature). The `.F` strip ran *before* the lowercasing, so it never matched
  `.f`: the stem stayed `fbtp.f`, matched no entry in the ambiguous-stem list, and was returned as a
  slug instead of the documented `LseError::AmbiguousSlug`. The transformation now runs in the order
  its rustdoc states — lowercase, then strip. The existing regression test only covered the
  uppercase `.F` spelling, which is why it passed; it now covers every casing of both spellings.

- **A panic on the blocking decode thread ended `stream_blocking_iter` as if the source had
  finished** (`rustrade-data`). The `JoinHandle` was detached, so an `init` or iterator panic
  dropped the sender and the stream yielded `None` — indistinguishable from clean end-of-stream.
  Since this is the documented driver for the Parquet decoder, a corrupt artifact could truncate a
  backtest and still produce a normal-looking summary, which is the exact failure mode the
  `BacktestMarketData::stream()` change below was written to eliminate. The panic is now re-raised
  on the task polling the stream, with its original payload, matching `futures::stream::iter` over
  the same iterator. Items decoded before the panic are still yielded first.
  **⚠️ Behaviour change**: a caller that previously saw a silent end now observes a panic. That is
  the point; a source cannot report a panic as `Err` because the error type is unconstrained.

- **`MockExchange` filled `Perpetual`, `Future` and `Option` instruments as physically-settled
  spot** (`rustrade-execution`). `MockExchange` and its `instruments` map are both public, so a
  consumer constructing one directly bypassed the `rustrade` builder that screens kinds — the
  instrument then took the spot path, with its `contract_size` multiplier applied to a delivery that
  cannot happen and no funding, margin or expiry settlement anywhere. `open_order` now rejects
  unsupported kinds with `ApiError::InstrumentInvalid` rather than filling them.
  **⚠️ Behaviour change**: a consumer constructing `MockExchange` directly and submitting a
  `Perpetual`, `Future` or `Option` now receives a rejected order where one previously filled as
  spot. Anything reaching `MockExchange` through the `rustrade` builder is unaffected — that path
  already screened kinds.

- **`PercentageFeeModel` ignored `contract_size`, understating fees by the multiplier**
  (`rustrade-execution`). The model computed `rate * price * quantity`, discarding the
  `contract_size` argument both call sites pass it. Percentage-of-notional is *the* natural fee
  model for a CFD, and a CFD's notional is the per-point multiplier times the price: a €25/point
  index CFD at 5000, quantity 1, at 0.1% was charged **€5 instead of €125** — silently, on every
  fill, in the direction that flatters a backtest. The formula is now
  `rate * price * quantity.abs() * contract_size`.
  **⚠️ Behaviour change**: fees increase by `contract_size` for any instrument whose multiplier is
  not one — every `Cfd`, and any `Future`/`Perpetual` configured with a real contract size. `Spot`
  is unaffected (its multiplier is `Decimal::ONE`), and `PerContractFeeModel` still deliberately
  ignores the multiplier, because that fee genuinely is per contract rather than per underlying
  unit. Backtest results for affected instruments will change; that difference is the error being
  removed.

- **A failed download could leave a corrupt artifact at the file's true length**
  (`rustrade-data`, `lse` feature). `download_export` derived "there is nothing left to fetch" only
  from a `416` on a resume request, so a *fresh* transfer that produced the whole artifact was still
  treated as resumable. When the server answered with the right number of bytes and the wrong
  content — a provider regenerating the artifact between the status poll and the download — the
  length check passed, the digest check failed, and the `.part` was **kept**: a corrupt file sitting
  at exactly the artifact's length, indistinguishable at rest from a legitimate one, for any caller
  that treats the hard `IntegrityMismatch` as terminal. It now also counts the transfer complete
  when the job's reported `bytes` have been received, so that file is discarded and a re-call starts
  clean. No corrupt bytes ever reached the destination — the rename does not run — so this was a
  disk-hygiene and contract defect rather than data corruption.

- **A candle whose `close_time` was after its own `time_exchange` injected lookahead into the
  engine** (`rustrade`). `DefaultInstrumentMarketData` keys everything candle-shaped on
  `close_time`, while the merge and the `HistoricalClock` key on `MarketEvent::time_exchange`. The
  two are the same instant for every producer in this crate, which stamps a bar with its derived
  close — but nothing enforced it, and a producer stamping the bar *open* would have delivered a
  completed bar's high, low and close at the moment its period began, priced positions from them,
  and left no trace: simulated time never moves backwards, so no monotonicity check fires. Such a
  candle is now **dropped** with a `tracing::warn!` and the previously stored candle is left intact,
  rather than being admitted and quietly biasing every downstream statistic. A candle closing
  *before* its `time_exchange` (a late-delivered bar) is unaffected — that direction is ordinary.
  The drop is unconditional; the warning is rate-limited to the 1st, 2nd, 4th, 8th … occurrence,
  because a producer that mis-stamps one bar mis-stamps all of them, and one line per candle is
  millions of lines on a large backtest. Each line carries the running total.

- **The LSE vault's pacing was a per-fetch claim, so a multi-instrument replay multiplied it**
  (`rustrade-data`, `lse` feature). `LseVaultClient`'s 300 ms pace was applied between the pages of
  one fetch, derived from the provider's documented 200 calls/minute. `replay_candles` drives N of
  those fetches at once — guaranteed, not incidental, since the k-way merge polls every source on
  every `poll_next` — so the aggregate rate was N × the budget against a measured
  `vault_concurrency` of **2**, and `LseError::RateLimited` is terminal by design: a ten-instrument
  replay would likely abort partway through. Both bounds now live on the client, behind a gate
  **shared by its clones**, and every request passes through it — candle page, export submit, status
  poll and artifact download alike. New `with_concurrency` sets the in-flight ceiling (default 2,
  matching the provider's reported `vault_concurrency`); `with_pace` now spaces the starts of *all*
  requests rather than only successive pages, so the "200 calls per minute" derivation holds however
  many sources a caller passes. A large N therefore makes a replay slower rather than louder.
  New `with_page_limit` completes the set, overriding the rows requested per page (default 5,000,
  matching the provider's reported `max_rows_per_request`) for a key on a plan allowing more, or to
  bound per-page memory. It takes a `NonZeroU32` where `with_concurrency` clamps: a concurrency of
  `0` parks every request and is visibly broken, whereas a page limit of `0` returns an empty page
  that pagination reads as end-of-data — a fetch that *succeeds* having returned nothing.

- **A pagination cursor overflow named an addend the code never used** (`rustrade-data`, `lse`
  feature). Advancing the cursor past a page's newest bar steps forward one second; when that
  overflowed, `fetch_candles` reported `LseError::TimestampOverflow { open, interval }`, which
  displays as `candle boundary overflow: open time {open} + {interval} is not representable`. Two
  things in that sentence were wrong: the overflow is not a candle boundary, and the interval is
  not the addend — it is the *candle's* period length, which that variant legitimately names at its
  other call sites, where `close_time == open + interval` really is the arithmetic performed. A
  reader diagnosing this was pointed at a calculation the code never ran. New
  `LseError::CursorOverflow { last_open }` names the one-second cursor step that actually overflowed.
  Only reachable within one second of `DateTime::MAX_UTC`; `LseError` is `#[non_exhaustive]`, so
  the added variant is not a breaking change, but code matching on `TimestampOverflow` to catch
  this specific case will no longer see it.

- **The aux-seam benchmark's baseline arm did work the production arm does not** (`rustrade`,
  benches only). `Backtest AuxSeam` compares `backtest()` (arm A) against `backtest_market_only`
  (arm B) to price the `TimedMergeStream` seam at a few ns/event. Once the market stream became
  fallible, arm B unwrapped each item with a `filter_map` combinator plus a `Ready` future per
  event, while arm A unwraps inline inside `poll_next` — so the *baseline* was slowed by the
  comparison, understating the seam cost the group exists to guard, possibly to zero. Arm B now
  replays the fixture's own `Arc<Vec<_>>` through an infallible stream, leaving the merge as the
  single difference between the arms. Benchmark-only; no library behaviour changes. Absolute figures
  still are not comparable across the fallible-stream change — re-baseline.

- **The duplicate-`name_internal` error printed nothing that distinguished the colliding
  instruments** (`rustrade-instrument`). Both names it interpolated were `name_exchange`, which the
  two instruments frequently share — a spot and a CFD on one symbol reported as *"ibkr-aapl is
  shared by the distinct instruments AAPL and AAPL on ibkr"*, asserting they are distinct while
  showing nothing that says how. The message now carries each instrument's `kind` alongside its
  name, which for the expiring kinds also surfaces the differing expiry.

- **A two-sided order book carrying no sizes panicked the engine** (`rustrade-data`, `rustrade`).
  `volume_weighted_mid_price` divides by the two amounts summed, and `Decimal`'s `Div` **panics** on
  a zero divisor — so any book quoting prices without sizes took down whatever polled it.
  `DefaultInstrumentMarketData::price` calls it first and unconditionally, and
  `InstrumentState::update_from_market` calls `price()` on every market event, so the panic landed in
  the engine task; the graceful shutdown then panicked a second time on its own
  `expect("Engine cannot drop Feed receiver")`, reporting an unrelated message and hiding the cause.
  Reachable from three producers that publish prices without sizes — an LSE tick export, and the
  Massive and IBKR quote paths, which substitute a zero amount when the venue omits one — the first
  of which makes it deterministic rather than venue-dependent. **`volume_weighted_mid_price` now
  returns `Option<Decimal>`** (`None` when the amounts sum to zero: the weighting is genuinely
  undefined there, and a size-less book is a real feed shape rather than a degenerate input), and
  `price()` falls back to the plain mid, which *is* well defined — so such a feed marks positions
  instead of either panicking or silently never producing a price. A one-sided book still contributes
  nothing: half a book has no mid, and picking whichever side is quoted is not a judgement the
  library makes for the caller. **Breaking:** the return type changed from `Decimal` to
  `Option<Decimal>`; callers must handle `None`.

- **The documented O(1) backtest memory guarantee did not hold for a blocking source**
  (`rustrade-data`, `rustrade`). `BacktestMarketData` and `MarketDataStreamed` both stated memory
  overhead was O(1) in the dataset size on the grounds that the harness never collects the stream.
  Laziness is not sufficient: the harness forwards the stream into the engine's **unbounded** feed
  channel with a synchronous send, so peak memory tracks how far the source runs ahead of the engine
  — and a blocking iterator wrapped in `futures::stream::iter` never returns `Poll::Pending`, so it
  runs ahead by the *entire dataset* before the engine handles one event. That is precisely the shape
  the Parquet decoder's own rustdoc example recommended: a 10M-row artifact parks its whole decoded
  self in the channel. Both doc blocks now state that bounding read-ahead is the implementation's
  obligation, that the obligation can only be discharged as far as the merge — the feed channel is
  harness-side and still unbounded — and the decoder example is repointed at the bridge below.

- **Added: `streams::blocking::stream_blocking_iter`** (`rustrade-data`) — bridges a blocking,
  fallible iterator into a bounded `Stream`. The source is opened and driven on a
  `spawn_blocking` thread, and a bounded channel parks it whenever it gets ahead of the consumer.
  This fixes two things for a local decoder: the blocking decode leaves the async runtime's workers
  (a hazard the Parquet module warned about while the adjacent example walked into it), and decoding
  overlaps engine processing instead of preceding it in one uninterruptible burst. It does **not** on
  its own bound a backtest's peak memory — the harness still forwards the merged stream into the
  engine's unbounded feed channel with a synchronous, non-waiting send, so events accumulate there
  whenever the engine is the slower side. The guarantee is that the decoder stays within `capacity`
  items of *its own* consumer; making it end-to-end needs the feed channel to apply back-pressure,
  tracked in [#220](https://github.com/Niqnil/rustrade/issues/220). A failure to open the source
  arrives as the stream's first `Err`, so open and mid-stream failures are handled on one path.

- **`MockExchange` accounted a CFD fill as if `contract_size` were 1** (`rustrade-execution`,
  `rustrade`). Admitting `InstrumentKind::Cfd` to the mock's instrument projection broke an invariant
  its arithmetic silently relied on: every kind it accepted before was `Spot`, where
  `contract_size == 1`. Four consequences, all silent. The notional was `fill_price × quantity` with
  no multiplier, so a 1-contract fill of a `contract_size = 25` CFD debited 1/25 of the true notional
  while the engine's position accounting applied the full 25 — every balance-derived return, drawdown
  and Sharpe wrong by that factor, with no failure point. A CFD **short** debited the base asset by
  the quantity, as though shorting an index required borrowing it, so opening one returned
  `BalanceInsufficient` unless the caller funded a phantom balance in an instrument that cannot be
  held. The fee call hard-coded `Decimal::ONE` for the multiplier. And the `debug_assert` excluding
  `PerContract` fees rested on the mock being spot-only, which it no longer was.
  The mock now models a CFD as what it is: a cash-settled position on a price, carrying
  `contract_size` into both the notional and the fee, debiting the **quote** asset in both
  directions, and requiring no base inventory to short. Five tests drive fills through it — the gap
  that let this land was that nothing did.
  **`CfdContract::settlement_asset` is deliberately not settled in**: a CFD routinely settles in an
  account currency that is not the quote asset, which needs a conversion rate this mock has no source
  for and will not invent. Callers must fund the **quote** asset of every instrument traded; the
  limitation, and the panic that an unfunded quote balance still produces, are now stated on
  `MockExchange` itself.

- **A stale L1 book shadowed every later candle and trade** (`rustrade`).
  `DefaultInstrumentMarketData::price` gave L1 an unconditional win, and `process` never clears it,
  so once any L1 arrived it decided the price forever. On a mixed feed — a session of quote ticks
  followed by a long run of bars, which one provider alone can produce — open positions marked to a
  book that had stopped updating, `pnl_unrealised` stopped moving, and the tear sheet looked normal.
  This is the same failure the candle-vs-trade rule was already recency-based to prevent, applied to
  the third input. A candle now wins whenever it closed strictly later than the input the tick regime
  holds, so a book that has stopped updating no longer decides the price forever; within that regime
  L1 keeps a fixed precedence over the last trade, so an L1-only feed behaves exactly as before. See
  the `Changed` entry on `DefaultInstrumentMarketData` above for why the two regimes are ranked
  differently. The L1 staleness guard keys on the payload's `last_update_time` — the same instant
  `price()` orders on — so the two cannot disagree.

- **A failing run in `run_backtests` leaked every cancelled sibling's task tree** (`rustrade`).
  `run_backtests` short-circuits on the first `Err`, dropping the other runs' futures — and dropping
  a `JoinHandle` *detaches* its task rather than cancelling it. The cancelled run's engine,
  execution-manager, mock-exchange and account-forwarding tasks therefore survived the drop, and
  could not finish on their own: the engine ends only on the explicit `Shutdown` that the graceful
  shutdown sends, and the account-forwarding task only on the explicit abort it performs, both of
  which the drop skips. What was left was a permanently parked task group per cancelled run, still
  holding its `EngineState`, plus a market source that kept fetching — and, on a metered provider,
  kept spending — for a result no caller could ever read. Each failing sweep in a long-lived process
  added another set. `backtest` now holds an abort guard over its `System`'s task tree for the
  duration of the run, so a cancelled run is torn down where it stands. Reachable only since the
  market stream became fallible: with an in-memory source a run could not fail mid-stream, so the
  short-circuit was unreachable.

- **`cargo doc` failed for `rustrade-data`, so the crate would have published no documentation**
  (`rustrade-data`). The crate denies `rustdoc::private_intra_doc_links`, and two public items
  linked to private ones (`fetch_candles` → `PAGE_LIMIT`, `slug` → `AMBIGUOUS_SLUG_STEMS`), which
  is a hard error rather than a warning. Both now state the fact inline, or link the public item
  the private one mirrors. Fixed alongside every remaining broken intra-doc link in the crate: a
  module that carried **both** an outer `///` on its `pub mod` declaration and its own `//!`
  documentation had the file's links resolved in the *parent's* scope, so each one rendered as dead
  text instead of a hyperlink. The redundant outer doc is removed wherever the module documents
  itself, and the convention is stated at the declaration site. `cargo doc` is now clean — no
  errors and no warnings — under `--features lse`, `--features lse-parquet` and `--all-features`.

- **`InstrumentNameInternal`'s two constructors produced different names for the same instrument**
  (`rustrade-instrument`). `new_from_exchange_underlying` interpolated the `ExchangeId` directly,
  which renders the bare *variant* name (`BinanceSpot` → `binancespot-btc_usdt`), while
  `new_from_exchange` used the canonical `ExchangeId::as_str` (`binance_spot-btc_usdt`).
  `InstrumentNameInternal` is an identity key — it keys the engine's instrument state map and is
  the lookup argument of `InstrumentStates::instrument`, which panics when absent — so an
  instrument declared through a JSON configuration and the same instrument built in-library never
  resolved to each other. Both constructors now use `as_str`. The divergence was invisible because
  every existing test constructed names through the second constructor.
  **⚠️ Migration — persisted state built before this release will not load cleanly.** The name is
  part of the on-disk identity of an instrument, so any `EngineState` snapshot, audit replica or
  replay stream taken against a multi-word exchange (`BinanceSpot`, `GateioSpot`, `BybitSpot`,
  `AlpacaBroker`, …) carries the old `binancespot-btc_usdt` spelling. Restoring one against an
  index rebuilt on this release resolves nothing for those instruments: `InstrumentStates::instrument`
  **panics** on the missing key, and any path that instead defaults the state silently attaches
  live positions to freshly-zeroed state. There is no in-library upgrade step — rewrite the
  exchange segment of every persisted `name_internal` from the concatenated variant name to the
  `snake_case` spelling, or rebuild the state from scratch.

- **Three undocumented caller obligations are now stated at the API boundary** (`rustrade-data`;
  documentation only, no behaviour change).
  - `download_export` names the `<destination>.<job id>.part` glob and states that abandoned partial
    files are never reclaimed. `.part` is scoped to the job id — a sound invariant, and the reason a
    resumed transfer cannot pick up a stale file — but the only deletion path is a discarding
    integrity failure, so a killed process or a re-export (new job id, new `.part`) orphans the old
    one permanently. A 7 GB export killed at 6 GB leaves 6 GB. No cleanup API is offered, because
    enumerating and deleting files a caller chose the location of is the caller's decision.
  - `stream_blocking_iter` states that it holds **one blocking thread per call for the whole
    decode**, and `merge_time_sorted` states that its inputs must be able to progress independently.
    Composed, N artifacts start N blocking tasks at call time (not first poll), and `blocking_send`
    parks the OS thread without returning it to the pool. Past tokio's default `max_blocking_threads`
    of 512, tasks 513+ never get a thread, their streams stay `Pending`, the merge — which requires
    every input to buffer or end — returns `Pending` forever, nothing drains, and no thread is
    released: a permanent stall with no error and no log. A 512-instrument universe is not exotic.
    Both ways out are documented (raise `max_blocking_threads`, or batch the inputs).
  - `stream_blocking_iter`'s claim that "every consumer reaches this stream through
    `merge_time_sorted`" is removed: it has no in-tree production caller, so the `FusedStream`
    choice is a design judgement rather than an observation, and is now described as one.

- **Published rustdoc no longer points at items readers cannot open** (`rustrade-data`). Twelve
  public items across the IBKR Flex, Massive and Alpaca surfaces linked to private items, which
  render on docs.rs as dead, non-hyperlinked code — a reader following "see `X`" found nothing. Each
  now states the fact inline (e.g. a cap's literal value) or links a public item instead. Two
  genuinely broken links in `ibkr::historical` are fixed too: they referenced
  `HistoricalTicks::truncated_by_error`, a field renamed to `truncation_error`. Most substantively,
  `IbkrFlexCorporateAction`'s limitations — that it is a reconciliation record and not a
  split-ratio source, that it is post-hoc and cannot drive a live split, and that its
  `quantity_delta` is account-scoped — were previously only in a private module's docs and so were
  never published; they now appear on the type itself. `rustdoc::private_intra_doc_links` is denied
  crate-wide so this cannot silently recur. Documentation only; no API or behaviour change.
- **`Position::pnl_unrealised` fee units** (`rustrade`). The per-tick unrealised-PnL exit-fee
  estimate now uses the quote-equivalent entry fee (`fees_enter.fees_quote`, falling back to raw
  `fees` only when no quote-equivalent is derivable), matching the realised-PnL convention. Fixes a
  dimensionally-inconsistent `pnl_unrealised` when the entry fee was paid in a base asset (e.g. BTC)
  rather than the quote asset. (#165)
- **`Position::pnl_unrealised` recompute no longer panics on an extreme price** (`rustrade`). The
  per-tick recompute now routes through checked `Decimal` arithmetic instead of the panicking
  unchecked path, so a corrupted feed price near `Decimal::MAX` can no longer bring down the engine.
  On overflow the market path **holds** the last-good value (a tick does not change the cost basis,
  so the prior estimate beats a fabricated `0`) and logs a `warn!`, while the post-trade and split
  paths degrade to `0` (the basis has just changed). **Breaking:** `Position::update_pnl_unrealised`
  now returns `#[must_use] PnlUnrealisedUpdate { Updated, Overflowed }` (was `()`); direct callers
  must bind the result. (#177)
- **`Position::pnl_unrealised` now updates on market ticks** (`rustrade`). `EngineState::update_from_market`
  previously refreshed only the instrument's market-data state, never the open positions, so
  `pnl_unrealised` stayed frozen at its last post-fill value (e.g. `0` for a freshly opened position)
  no matter how far the market moved — contradicting the field's documented per-tick contract. It now
  routes through `InstrumentState::update_from_market`, so every open position is revalued and its
  `time_exchange_update` advanced on each priced market event. The audit-replica path shares this
  method and revalues identically. (#186)
- **Public `calculate_pnl_unrealised` no longer panics on `Decimal` overflow** (`rustrade`). The free
  function `engine::state::position::calculate_pnl_unrealised` now performs checked arithmetic and
  returns `Option<Decimal>` (`None` on overflow); the previously-private checked twin is folded into
  it, leaving a single public arithmetic core that every `pnl_unrealised` recompute routes through.
  Removes a latent panic vector for external callers that computed unrealised PnL directly.
  **Breaking:** the return type changed from `Decimal` to `Option<Decimal>`; callers must handle
  `None`.
- **`Position::pnl_realised` accumulation no longer panics on `Decimal` overflow** (`rustrade`).
  `calculate_pnl_realised` and `calculate_pnl_return` now use checked arithmetic and return
  `Option<Decimal>` (`None` on overflow). `Position::update_pnl_realised` checks both the closed
  delta and its accumulation into the running total, and on overflow **holds** the last-good
  cumulative `pnl_realised` (the failing close's contribution is not applied — there is no safe
  fallback for a monotonic ledger) and logs a `warn!`; the entry-fee deduction on the position-
  increase path and the statistics `PnLReturns::update` accumulation are hardened the same way, with
  the returns path skipping the affected data point. **Breaking:** `calculate_pnl_realised` and
  `calculate_pnl_return` now return `Option<Decimal>` (was `Decimal`); `Position::update_pnl_realised`
  now returns `#[must_use] PnlRealisedUpdate { Updated, Overflowed }` (was `()`); direct callers must
  handle the new return values.
- **Backtest benches no longer panic at setup** (`rustrade`, benches only). The shared
  `market_data_from_file` helper now sorts the recorded fixture ascending by `time_exchange` before
  constructing `MarketDataInMemory`, which hard-asserts sorted input. The committed fixture is
  interleaved across three instruments (not globally sorted), so the `bench_backtest` and
  `bench_backtests_concurrent` groups previously panicked during setup and could not run — meaning any
  prior `--save-baseline` numbers for those two groups are not comparable across this change (they
  never completed). Benchmark-only; no library behaviour changes.

- **An event from an untracked exchange is reported instead of panicking the engine** (`rustrade`).
  A market or account event naming an exchange with no `ConnectivityState` hit
  `panic!("ConnectivityStates does not contain: {key}")`. Worse than a panic, it was an
  *intermittent* one: the lookup sat behind a `global == Healthy` early-return, so a misconfigured
  exchange was silently ignored for as long as everything else stayed healthy and only brought the
  engine down once a reconnect dragged global health back down — hours into a run. The exchange is
  now resolved on **every** market event, including the healthy fast path, so the report fires on the
  first event from that exchange in every run; the early-return still skips the state mutation. An
  untracked exchange mutates nothing — including `global`, which previously went to `Reconnecting`
  immediately before the panic — and surfaces as a non-mutating `EngineOutput::UntrackedExchange`.
  `EngineState::update_from_market` returns before the positional `instrument_index_mut` lookup,
  which would otherwise panic or, worse, misattribute the print to whichever instrument occupied that
  slot. Untracked reconnects also skip `Strategy::on_disconnect`, since the strategy has no link to
  that venue to react to. The audit replica reaches the same verdict by replaying the event rather
  than mirroring the output. Cost of the unconditional lookup, measured rather than assumed: below
  the noise floor of a ~50,000-market-event backtest bench (127.28 ms → 126.66 ms, within the
  criterion noise threshold).

- **The account connection dimension is derived from execution clients, not the instrument model**
  (`rustrade`). A venue that supplies prices without executing anything was assigned
  `VenueRole::Both`, so it waited forever on an account connection nothing would ever establish and
  held `ConnectivityStates::global` at `Reconnecting` for the life of the run. The two dimensions
  have different sources of truth: market data is instrument-derived and exactly so, since
  subscriptions are generated from the same `data_exchange` field, but account events reach the
  engine only from a registered `ExecutionManager`, and `Instrument::exchange` names the venue an
  instrument *would* trade on — not whether anything is wired up to trade there. `SystemBuilder`
  now reads the registered venues from the `MultiExchangeTxMap` it has already built, so the live
  path is correct without the caller doing anything. This matters for any configuration pricing one
  instrument on a venue it never trades on, including a strategy that decides on one instrument and
  trades another. A venue providing neither dimension — instruments priced elsewhere, no execution
  client registered — is reachable for the first time as a result; it stays `Both`, withholding
  health rather than granting it to a venue with no known connection, and is reported with a warning.

- **A stock split no longer double-adjusts an option chain** (`rustrade`).
  `prepare_corporate_action_split` derived `(base, quote, exchange)` from the split target and
  adjusted every option matching it, with nothing verifying the target was the unique deliverable
  instrument on that underlying. Two instruments sharing that identity each acted as a valid trigger
  for adjusting the whole chain, and the second pass was silent *and* unrecorded: unheld options took
  the strike correction with no position event, and `corporate_actions_processed` was consulted only
  on the target, so the mutated options carried no record at all. A re-delivered action therefore
  halved an already-halved strike, surfacing months later as mis-settlement at contract expiry. Both
  doors are now closed, because they are different doors — the existing idempotency guard is keyed on
  action `id` alone and read only from the target's set, so recording alone is defeated by a distinct
  `id` per instrument, while uniqueness alone leaves a same-`id` replay unrecorded on the options it
  mutated:
  - the split is rejected with `UnsupportedCorporateActionReason::AmbiguousSplitTarget` if any other
    registered instrument is split-eligible on the target's `(base, quote, exchange)`, checked before
    any mutation and before the ratio arithmetic, so an ambiguous target is reported as such even
    when the arithmetic would also fail;
  - each adjusted option records the action `id` in its **own** `corporate_actions_processed`,
    written before the unheld early-out so a silent registry fix leaves the same evidence a held
    position does, and the prepare pass filters options already carrying the `id` out of the plan.
  Suppressed options are reported, not silently skipped — one
  `EngineOutput::CorporateActionAlreadyProcessed` each plus a single aggregate warning, since a full
  chain can be large. Dropping them would have reproduced the defect's own signature. Both guards
  live in the shared prepare pass rather than being hand-mirrored, so the live handler and the audit
  replica agree by construction.

### Security

- **`cmov` and `rand` bumped to their patched releases, retiring one advisory suppression.**
  `Cargo.lock` moves `cmov` 0.5.3 → 0.5.4 and `rand` 0.9.2 → 0.9.3. Nothing else in the graph
  changes: 660 packages before and after, with no crate added or removed.

  `cmov` (CVE-2026-50185 / GHSA-3rjw-m598-pq24) could return **wrong results** on `aarch64` when the
  high bits of a register were set, in the constant-time conditional-move primitives reached through
  `hmac`, `sha1` and `sha2` — which on this workspace is request signing. `x86_64`, where CI runs,
  was never affected; this is a library, and consumers run `aarch64`.

  `rand` (GHSA-cq8v-f236-94qc) is an unsoundness whose precondition — the `log` feature plus a custom
  logger that itself draws from `rand::rng()` — is not met anywhere here. The bump matters for a
  different reason: it removes the last vulnerable `rand` from the lockfile, which is the exit
  criterion `deny.toml` recorded for `RUSTSEC-2026-0097`. That suppression is now dropped from both
  `deny.toml` and the CI audit job's ignore list rather than left to rot.

  `deny.toml` also gains a note on a limitation neither gate can work around: `cargo audit` and
  `cargo deny` read the RustSec database, which is a subset of GHSA, so an advisory carrying a CVE
  but no RUSTSEC id cannot fail either of them. `cmov` was exactly that case. Dependabot reads GHSA
  and is the only detector for that class.

- **The London Strategic Edge vault client no longer follows redirects, so `x-api-key` cannot be
  forwarded to another host** (`rustrade-data`, `lse` feature). The client was built with no
  `.redirect(...)`, so `reqwest` applied its default `Policy::limited(10)`. On a cross-host
  redirect reqwest strips only `Authorization`, `Cookie`, `Cookie2`, `Proxy-Authorization` and
  `WWW-Authenticate` — a custom `x-api-key` installed via `default_headers` is not in that set and
  survives the hop. (`set_sensitive` affects `Debug` redaction only, not redirect behaviour.) The
  exposure is concrete: `GET /vault/export/{id}/download` is the multi-gigabyte endpoint, exactly
  the kind that hands off to object storage with a `302`, and reqwest would have re-issued the GET
  carrying the live key. It also broke an invariant the module states twice in its own docs — that
  this client only ever requests URLs it builds itself, which is why the provider's own
  `download_url` is deliberately ignored — since a followed redirect *is* a server-supplied URL.
  `LseVaultClient::new` now sets `redirect::Policy::none()`, matching the two sibling API-key-bearing
  clients in this crate (`massive::rest` and `ibkr::flex`), and both `new` and `with_client` document
  it. **Behaviour change:** a `3xx` from the vault now surfaces as an `LseError::Api` carrying that
  status instead of being followed silently.

- **Fixed a bearer-token leak in Massive REST pagination** (`rustrade-data`). The client attaches its
  API key as an `Authorization: Bearer` default header sent with every request, and validated each
  server-supplied `next_url` with a `starts_with(base_url)` prefix check before following it. A
  look-alike host that merely shares the prefix — `https://api.massive.com.attacker.example` or even
  the separator-less `https://api.massive.comevil.example` — passed that check and would have received
  the token. Pagination now parses both URLs and compares their [origins][url-origin]
  (scheme + host + port), rejecting any `next_url` whose origin differs or that fails to parse
  (fail-closed) so the token is never sent to an untrusted host. The check moved into the single
  request chokepoint (`fetch_page_body`) so it cannot be bypassed by a future paginated fetch, and the
  client now disables HTTP redirect following (`redirect::Policy::none()`) so a server-issued 3xx
  cannot bounce an origin-validated request to another host behind the guard — the guarantee no longer
  depends on reqwest's internal cross-origin header stripping. A new
  `MassiveError::UntrustedNextUrl { next_url, expected_origin }` variant carries the diagnosis
  (`MassiveError` is `#[non_exhaustive]`, so the addition is not a breaking change).

  [url-origin]: https://docs.rs/url/latest/url/struct.Url.html#method.origin

- **Hardened Massive REST authentication against future token leaks** (`rustrade-data`;
  defense-in-depth follow-up to the origin-validation fix above). The API key is no longer installed
  as a client-wide `Authorization: Bearer` default header on the underlying `reqwest::Client`, where
  it rode every request regardless of destination host. It is now attached per-request inside the
  single origin-validated request chokepoint (`fetch_page_body`), only *after* the destination origin
  passes `validate_next_url`. The credential is thus coupled to the origin check by construction — no
  request path can carry it to a host that has not been validated, even if a future paginated fetch
  omitted the guard. `reqwest`'s `bearer_auth` additionally marks the header sensitive (redacted in
  its logs). No public API change and no behaviour change for well-behaved responses. (#198)
- **Bounded error-path response reads for the REST clients** (`rustrade-data`; defense-in-depth). On
  a non-success HTTP status, the Massive (`fetch_page_body`), IBKR Flex (`get_with_query`) and
  Binance historical (`fetch_page`) clients now read the diagnostic body only up to a fixed cap
  instead of buffering it in full. Previously a pathological proxy/CDN returning an unbounded error
  body was downloaded entirely before being truncated for the error message; the cap bounds that
  memory use while staying far above any legitimate error/status envelope (so a Flex `1019`/error
  response is never truncated). All three REST clients are covered — Binance is the one that is not
  feature-gated, so it is compiled into every dependent build. Success bodies (real payload) are
  still read in full. No public API or error-message change.
- **Bounded the memory a misbehaving Massive origin can force during pagination** (`rustrade-data`).
  A page's `next_url` is server-supplied and success bodies are read in full, so the pagination
  guard previously retained up to `MAX_PAGES` (10,000) unbounded URL strings per stream for cycle
  detection, and `MassiveError::CyclicPagination` / `UntrustedNextUrl` rendered one in full into
  every log line. The guard now records a fixed-size fingerprint per page instead of the URL itself,
  and rejects any URL past a new byte cap up front with the additive
  `MassiveError::PaginationUrlTooLong { len, limit, prefix }`. Fingerprints are keyed by a
  per-stream random hash key rather than the fixed-key `DefaultHasher`, so a server cannot
  precompute two colliding `next_url`s to force a spurious `CyclicPagination`. Cycle detection is
  unchanged for well-behaved servers: the fingerprint covers the whole URL, so two long URLs sharing
  a prefix remain distinct pages.
- **Token-scrubbed and bounded `IbkrFlexError::Parse`** (`rustrade-data`, `ibkr` feature). An XML
  deserialiser embeds fragments of the offending input in its error message, so a `Parse` message's
  length tracked the *document* rather than the failure, and a body that reflected the request line
  could carry the `t=` Flex token into the stored error — the protection
  `IbkrFlexError::HttpStatus` already had. Parse messages raised while interpreting a SendRequest or
  GetStatement response now go through the same redaction and bounding. The statement re-parse
  behind `parse_corporate_actions` is covered too: called directly, with no token in scope to
  redact, it still bounds its message; called from `IbkrFlexClient::fetch_corporate_actions` the
  token is threaded into the parse, so redaction runs while the message is still unbounded. That
  ordering is load-bearing rather than incidental — redaction matches the full token, so scrubbing
  an already-bounded message would leave a credential straddling the cap present as an unmatched
  prefix fragment, and both cuts use the same width, leaving no protective gap.
  `Parse` is now bounded on every path and scrubbed on every path where a token is in scope.
- **Token-scrubbed and bounded `IbkrFlexError::Flex`** (`rustrade-data`, `ibkr` feature). A terminal
  Flex error's `message` is the `<ErrorMessage>` element lifted verbatim from the same server-supplied
  body that `HttpStatus`/`Parse` already scrub, so a proxy reflecting the request line into it could
  carry the `t=` token into the stored error, and an oversized element could bloat it. Both fields
  (`code` and `message`) now pass through the same redact-before-bound path (capped at 1 KiB) when the
  interpreter finalises a Flex error; the scrub is a no-op for a real numeric Flex code/message. No
  public API change and no change for well-behaved responses.
- Upgraded `anyhow` to 1.0.103 and `quick-xml` to 0.41.0 to clear three RUSTSEC advisories:
  RUSTSEC-2026-0190 (unsoundness in `anyhow::Error::downcast_mut`), RUSTSEC-2026-0194 (quadratic
  run time when checking a start tag for duplicate attribute names) and RUSTSEC-2026-0195
  (unbounded namespace-declaration allocation in `NsReader`, a memory-exhaustion DoS). `quick-xml`
  backs the IBKR Flex XML parser behind the `ibkr` feature; the bump is API-compatible for the
  `Reader`/`de::from_str` surface in use.
- Updated `crossbeam-epoch` to 0.9.20 to clear RUSTSEC-2026-0204 (invalid pointer dereference in the
  `fmt::Pointer` impl for `Atomic`/`Shared` when the underlying pointer is null). A transitive
  dependency (via `ibapi` and, in dev builds, `rayon`/`criterion`); the bump is a `Cargo.lock`-only
  patch within the existing `0.9` constraint.
- Updated `lru` to 0.18.2 to clear RUSTSEC-2026-0253 (`LruCache::pop()` was not panic-safe: a
  panicking key `Drop` skipped `detach()`, leaving a dangling pointer in the internal linked list
  for a later eviction to write through — use-after-free and potential double-free). `lru` backs
  the WebSocket event-deduplication cache behind the `alpaca` and `binance` features; reaching the
  bug additionally requires `catch_unwind` around a panicking key `Drop`, and the cache is keyed on
  plain owned data, so the workspace could not trigger it. The bump is a `Cargo.lock`-only patch
  within the existing `0.18` constraint.
- Updated `h2` to 0.4.19 to clear RUSTSEC-2026-0258 (empty `DATA` frames were accepted and queued
  without limit: unbounded memory growth if streams are not actively drained, or a panic if the
  queued length overflows). A transitive dependency via `hyper` 1.x and `reqwest` 0.12; the bump is
  a `Cargo.lock`-only patch within the existing `0.4` constraint (upstream patched it in 0.4.16).
  A second, older `h2` 0.3.27 remains in the tree and has **no** patched release — upstream shipped
  the fix only on the `0.4` line, so no bump is available. Every path to it runs through the
  `hyperliquid` feature (`hyperliquid_rust_sdk` 0.6 and the optional `ethers` 2.0, both gated on it)
  into `reqwest` 0.11 and `hyper` 0.14, so it is not compiled under the default feature set. It is
  recorded as an accepted exception in `deny.toml` next to the other `hyperliquid_rust_sdk`
  advisories, to be dropped once that SDK moves off `reqwest` 0.11.

- Updated `rustls` to 0.23.45 to clear RUSTSEC-2026-0285 (TLS 1.3 handshake messages were accepted
  at the wrong encryption level when they followed a key-changing message in the same record — for
  example a plaintext `EncryptedExtensions` packed into the `ServerHello` record. RFC 8446 s5.1
  requires terminating such a connection with `unexpected_message`. The handshake transcript stays
  authenticated, so a network-position attacker cannot alter or complete a handshake; the practical
  effect is that a peer can send in plaintext handshake messages that should have been encrypted,
  without rustls rejecting the connection. Functionally the same bug as Go's CVE-2025-61730).
  Unlike the transitive advisories above this one is squarely on a hot path: `rustls` 0.23 is the
  TLS implementation behind every HTTPS and WSS connection the workspace makes, via `reqwest`,
  `tokio-tungstenite`, `hyper-rustls` and `rustls-platform-verifier`.

  Two details worth recording. First, `cargo update -p rustls` alone resolves to 0.23.43, which is
  **still vulnerable** — the patched range is `>=0.23.45`, so the bump needs `--precise`. Second,
  reaching 0.23.45 also moves `aws-lc-rs` 1.16.3 -> 1.18.1, `aws-lc-sys` 0.40.0 -> 0.45.0 and
  `rustls-webpki` 0.103.13 -> 0.103.15, so this is not the single-package lockfile patch the
  entries above were; `aws-lc-sys` in particular is a cryptographic C library built through a
  `build.rs`. All four crates declare `rust-version = "1.71"`, well under the workspace MSRV of
  1.95, and the change is still confined to `Cargo.lock` with no manifest constraint edited.

  The tree also carries an older `rustls` 0.21.12, which the advisory lists as unaffected
  (`<0.23.13`).

## [0.5.0] - 2026-06-19

### Changed

- **`RestRequest::timeout` is now an instance method (`&self`)** (`rustrade-integration`). Previously a
  receiverless associated function, it could only return a compile-time constant; taking `&self` lets an
  implementation derive the per-request timeout from instance/config state (e.g. an operator-tunable
  timeout captured at construction). The default still returns the compile-time
  `DEFAULT_HTTP_REQUEST_TIMEOUT`, so implementations relying on the default are unaffected. **Breaking
  only** for impls that explicitly override `timeout()` — add `&self` to the signature.

## [0.4.0] - 2026-06-13

### Added

- **`impl Borrow<str> for SubscriptionId`** (`rustrade-integration`). An instrument map keyed on
  `SubscriptionId` can now be queried with a borrowed `&str` key without allocating an owned
  `SubscriptionId` per lookup.
- **Named config constructors and env loading for Alpaca and Binance Spot**
  (`rustrade-execution`, `alpaca` / `binance` features). Added `AlpacaConfig::from_env()` and
  `BinanceSpotConfig::from_env()` plus typed config errors (`AlpacaConfigError`,
  `BinanceSpotConfigError`) for missing credentials and invalid boolean env values.
- **`from_env()` now distinguishes non-UTF-8 credential vars from absent ones**
  (`rustrade-execution`, `alpaca` / `binance` / `hyperliquid` features). New error variants —
  `AlpacaConfigError::{InvalidApiKey, InvalidSecretKey}`,
  `BinanceSpotConfigError::{InvalidApiKey, InvalidSecretKey}`, and
  `HyperliquidConfigError::{InvalidPrivateKeyVar, InvalidTestnet}` — flag a non-UTF-8 environment
  variable explicitly instead of collapsing it into "not set". The non-UTF-8 **credential** variants
  carry no payload, so corrupt secret/key bytes are never echoed into an error message or log; the
  non-secret network-toggle variants (`InvalidPaper` / `InvalidTestnet`) instead echo the offending
  value (lossily for non-UTF-8) so "must be true or false, got …" stays actionable. Rustdoc added to
  every
  `new`/`paper`/`testnet`/`production`/`from_env` constructor spelling out caller obligations
  (`production`/mainnet = real funds; `from_env` returns `Err`, never panics).
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
- **CI: non-blocking early-warning build against latest dependencies.** A new weekly
  scheduled workflow resolves the newest semver-compatible versions of every dependency
  (ignoring the committed `Cargo.lock`) and runs `cargo check --workspace --all-targets
  --all-features`, giving early warning when an upstream release breaks the build. It never
  gates PRs; on failure it opens — and on recovery closes — a single deduped tracking issue.
  Complements the committed-lockfile/`--locked` CI by exercising the versions downstream
  consumers actually resolve, which `--locked` no longer does.

### Changed

- **`Cargo.lock` is now committed and CI builds run `--locked`.** Previously the lockfile was
  gitignored, so CI resolved fresh transitive dependencies on every run and a bad upstream release
  could turn CI red with no change on our side (e.g. `time 0.3.48`'s coherence-breaking `From` impl,
  [time-rs/time#783](https://github.com/time-rs/time/issues/783)). Committing the lockfile makes CI
  reproducible; consumers are unaffected since `Cargo.lock` does not propagate to downstream crates.
- **Breaking (`rustrade-data`):** Binance kline routing keys are now baked at deserialize.
  `BinanceKline` and `BinanceContinuousKline` replace their public `symbol` / `pair` string fields
  with a single `subscription_id: SubscriptionId` (the instrument-map key `{channel}|{MARKET}`),
  built once via the same `ExchangeSub::id` used at subscribe time. This makes the subscribe-time
  and frame-time keys a single source of truth that cannot drift and silently misroute. `Serialize`
  is no longer derived on the Binance decode-only wire types `BinanceKline`, `BinanceContinuousKline`,
  `BinanceKlineData`, `BinanceTrade`, `BinanceOrderBookL1`, `BinanceSpotOrderBookL2Update`, and
  `BinanceFuturesOrderBookL2Update`: their custom field deserialization (`deserialize_with`) meant the
  derived `Serialize` output never round-tripped, and nothing serializes these types.
- **Breaking (`rustrade-execution`, `alpaca` / `binance` features):** `AlpacaConfig::new` and
  `BinanceSpotConfig::new` now take credentials only. Optional live-vs-safety knobs moved to named
  constructors: `AlpacaConfig::paper` / `AlpacaConfig::production` and
  `BinanceSpotConfig::testnet` / `BinanceSpotConfig::production`. The credentials-only constructors
  default to paper trading for Alpaca and testnet for Binance Spot.
- **Breaking (`rustrade-execution`, `hyperliquid` feature):** `HyperliquidConfig::from_env()` now
  defaults to the **safe testnet** environment when `HYPERLIQUID_TESTNET` is absent (previously
  defaulted to **mainnet**, the dangerous foot-gun), matching Alpaca/Binance Spot. The `"1"`
  truthy special-case is dropped — `HYPERLIQUID_TESTNET` is now `true`/`false`-only across every
  venue. An invalid or non-UTF-8 toggle is a hard `HyperliquidConfigError::InvalidTestnet(String)`
  rather than a silent `false` (mainnet). `HyperliquidConfigFile`'s `testnet` field likewise now
  defaults to `true` (safe testnet) when absent from a config file. Set `HYPERLIQUID_TESTNET=false`
  to opt into mainnet (real funds).
- **Breaking (`rustrade-execution`, `hyperliquid` feature):** the config error type is renamed
  `ConfigError` → `HyperliquidConfigError` and re-exported as `client::hyperliquid::HyperliquidConfigError`,
  matching the venue-scoped naming of `AlpacaConfigError` / `BinanceSpotConfigError`.
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
