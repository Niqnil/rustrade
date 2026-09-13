# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

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

### Changed

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
