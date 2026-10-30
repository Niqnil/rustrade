//! Binance execution clients.
//!
//! - [`BinanceSpot`] — Binance Spot `ExecutionClient` (REST + signed WebSocket API).
//! - [`BinanceMargin`] — Binance Margin (cross and isolated) `ExecutionClient` (REST
//!   orders/queries + the margin user-data stream, read with a listen key). Configured via
//!   [`BinanceMarginConfig`] and [`MarginEffect`](crate::margin::MarginEffect).
//!
//! Both clients share exchange-agnostic infrastructure (reconnect/backoff,
//! rate-limit tracking, event deduplication, error parsing, and the
//! Binance-string parsers) from the `shared` module, so resilience behaviour is
//! implemented once and reused rather than duplicated per client.
//!
//! # Orders are checked against the symbol's rules before they are sent
//!
//! Each client reads the symbol's tick, lot, notional and trailing-offset rules from Binance's
//! spot `exchangeInfo` on the symbol's first order, and refuses an order that breaks them without
//! sending it: a price or quantity off its grid as [`OrderError::InvalidPrecision`](crate::error::OrderError::InvalidPrecision)
//! with [`NotOnGrid`](crate::error::PrecisionLimit::NotOnGrid), and one outside its range as
//! [`OrderError::OutOfBounds`](crate::error::OrderError::OutOfBounds). Nothing is rounded.
//!
//! - **What is checked** is in [`OrderRules::check`](crate::rules::OrderRules::check). A pass is
//!   not a guarantee: the filters that need a reference price or account state stay the venue's.
//! - **Loading.** Rules refresh hourly. A failed refresh keeps the old rules for up to 24 hours,
//!   and a symbol with no rules is sent unchecked, with a warning. `load_rules` on each client
//!   loads a set of symbols ahead of their first order.
//! - **Margin** has no rules endpoint of its own, so [`BinanceMargin`] uses the pair's spot
//!   rules, which a margin order obeys.
//! - **Turning it off.** `pre_send_rules` on each config takes
//!   [`PreSendRules::Off`](crate::rules::PreSendRules::Off) to send every order and leave the
//!   rules to Binance, for when the loaded rules could be out of date.
//!
//! # Every fill on the account is delivered
//!
//! Both clients' account streams report every fill on the account, whoever placed the order:
//! this client, another API client, another program, or the venue's own app. Binance marks none
//! of them as another client's, so each arrives as [`TradeOrigin::Order`](crate::trade::TradeOrigin::Order).
//! Fill recovery after a reconnect and `fetch_trades` read the account's fills the same way.
//!
//! A consumer that trades the account outside the engine it feeds therefore filters those fills
//! out itself. [`Trade::client_order_id`](crate::trade::Trade::client_order_id) carries the client
//! order id each fill's order was placed with, so orders placed outside the engine under ids
//! of their own, such as with a prefix, can be recognised by their fills.
//!
//! # Account stream reconnection
//!
//! Both clients' account streams reconnect by themselves, with the same budget, when the
//! connection drops or goes silent. The heartbeat is checked every 30 s: a check that finds no
//! frame or ping since the last one reconnects, except the first check on each connection, which
//! is a grace. Silence is therefore noticed after 30 to 60 s. Each reconnect recovers the fills
//! and order endings the stream missed (see each client's `account_stream`), then restates its
//! balances (see [below](#balance-restatement-after-a-reconnect)).
//!
//! The span of fills a reconnect recovers is placed on Binance's clock, as the times Binance
//! stamped on the stream's events show it (each event's `E`), counted on from their arrival on a
//! monotonic clock. So a host clock ahead of Binance's does not start the span after fills Binance
//! stamped during the outage, and one stepped back before the reconnect does not end it early.
//! Before the stream's first event the span starts from this host's time.
//!
//! - **Attempts.** A dropped connection, and each reconnect that fails after it, spends one
//!   attempt: a wait, then a reconnect. A reconnect fails when its connect, its user-data
//!   subscribe, or for [`BinanceMargin`] its listen-key request fails. The waits double from 1 s
//!   up to 30 s: 1, 2, 4, 8, 16, then 30 s.
//! - **Reset.** A heartbeat check that finds a frame or ping clears the attempts spent before it,
//!   so only failures in a row count. A connection the venue accepts and then drops or leaves
//!   silent before such a check counts as a failed reconnect. The answer to the subscribe is not
//!   a frame for this purpose.
//! - **Giving up.** Once 10 reconnects in a row have failed, after about three minutes of waiting
//!   in all, the stream ends with
//!   [`AccountEventKind::StreamTerminated`](crate::AccountEventKind::StreamTerminated) carrying
//!   [`ReconnectBudgetExhausted`](crate::error::StreamTerminationReason::ReconnectBudgetExhausted)
//!   and the last failure.
//! - **Rate limits.** A reconnect is never attempted during a rate-limit cooldown. A cooldown of
//!   10 s or less is waited out without spending an attempt. A longer one ends the stream at once
//!   with [`RateLimited`](crate::error::StreamTerminationReason::RateLimited), carrying the time
//!   left. See each client's "Rate limits" docs.
//! - **Opening is not retried.** A failed `account_stream` call returns its error. Whether and
//!   when to try again is the caller's decision, as it is after the stream ends.
//!
//! [`BinanceMargin`] also reconnects with a new listen key when Binance says the old one has
//! expired, which spends an attempt like any other failure. Its key is kept alive as each
//! connection opens and every 30 minutes after, which Binance documents as enough to keep it
//! valid.
//!
//! # Balance restatement after a reconnect
//!
//! A balance or debt event the venue sends while an account stream is down is lost, so each
//! reconnect restates the account's balances. Once the gap's fills have been read, the stream
//! reads the balances over REST and delivers each of the `assets` it was opened for (all of them
//! when empty) as a [`BalanceSnapshot`](crate::AccountEventKind::BalanceSnapshot), debt included.
//! An isolated margin stream delivers its pairs' base and quote as
//! [`InstrumentBalanceSnapshot`](crate::AccountEventKind::InstrumentBalanceSnapshot)s instead:
//! each pair's base, then its quote, the pairs in no particular order.
//!
//! - **Stamp.** The earlier of this host's time and the venue's time at the read's start, as the
//!   stream's own events show the venue's clock, less 5 s for Binance's REST view trailing its
//!   stream. A stream event newer than the state the read shows then still applies over it. The
//!   read starts no sooner than 5 s, on the venue's clock, after the latest balance or debt event
//!   the stream delivered, and its stamp is never earlier than that event's: an earlier one would
//!   leave the engine keeping that event's state.
//! - **Failure.** A read that fails, or does not finish within 30 s (waits for rate limits
//!   included), is logged at `warn` and retried while connected, after 5 s and then twice as long
//!   each time, up to 2 minutes. It is never given up. A later reconnect replaces it with a read
//!   due at once.
//! - **Fills not yet recovered.** A gap whose fill read failed does not hold the restatement
//!   back, so the balances can include fills the stream has yet to deliver. They follow when the
//!   gap's retry reads them, or never if it is given up (see
//!   [`FillRecoveryGaveUp`](crate::AccountEventKind::FillRecoveryGaveUp)).
//! - **Retries wait on it.** The restatement, the gaps' fill reads and the order checks are retried
//!   one at a time, so a restatement read can delay a retry due meanwhile by up to its 30 s.
//! - **Cost.** One account read per reconnect: weight 20 on spot, 10 on cross margin, and 10 per
//!   five isolated pairs.
//!
//! # Signed requests are stamped on Binance's clock
//!
//! Binance refuses a signed request stamped 1 s or more ahead of its clock, or more than
//! `recvWindow` (5 s) behind it, with `-1021`. So both clients stamp their signed requests with
//! Binance's clock as read from `GET /api/v3/time` (weight 1), rather than with this host's: the
//! earliest Binance's clock can read, counted on from the read on a monotonic clock. A stamp is
//! then never ahead of Binance's clock, trails it by about the time the read's answer took to
//! arrive, and does not move when this host's wall clock steps. [`BinanceMargin`] reads the time
//! from the spot API, as margin has no endpoint for it.
//!
//! - **First request.** Each client's first request waits for one read. If the read fails, it is
//!   logged at `warn`, and requests are stamped with this host's clock until a read succeeds.
//! - **Refresh.** A request that finds the last read 5 minutes old starts another in the
//!   background, and is sent without waiting for it.
//! - **`-1021`.** The client drops its reading and reads again before it returns the error, so a
//!   retry is stamped on the new reading. The error is unchanged: a transient connectivity error
//!   for a query, a rejection for an order. Whether to retry stays the caller's decision.
//! - **Observing it.** `host_clock_lead` on each client reports how far this host's clock leads
//!   Binance's, negative when it trails. A read that finds the clocks 1 s or more apart logs at
//!   `warn`.
//! - **Account streams.** The reads also bound Binance's clock for the account streams, which place
//!   fill recovery and balance restatement on Binance's time (see above), before a stream's first
//!   event as well as after.
//! - **Turning it off.** `server_time_sync` on each config, `true` by default, set to `false`
//!   stamps signed requests with this host's clock.
//!
//! # Logging: `binance_sdk` writes request credentials at `DEBUG`
//!
//! Both clients run on the `binance-sdk` crate, whose WebSocket API client logs every outgoing
//! request at `DEBUG`, after it has been signed. [`BinanceSpot`] places and cancels orders and
//! subscribes to its user-data stream with signed requests, so each of those log lines carries
//! the account's **API key** and the request's **signature**.
//!
//! [`BinanceMargin`] uses only the SDK's REST client, which logs no requests. Its user-data
//! stream sends the listen key in a subscribe frame, which is not logged, and this client masks
//! the key in the warnings it writes.
//!
//! The API secret and private key are never logged. The SDK logs these lines with `tracing`
//! under the `binance_sdk` target.
//!
//! This library installs no subscriber, so what reaches your logs is decided by the filter your
//! application builds. A bare `RUST_LOG=debug` enables these lines. To keep them out by default,
//! seed a `binance_sdk=info` directive **before** your own:
//!
//! ```
//! use tracing_subscriber::EnvFilter;
//!
//! let user = std::env::var("RUST_LOG").unwrap_or_else(|_| "info".to_owned());
//! let filter = EnvFilter::try_new(format!("binance_sdk=info,{user}"))?;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
//!
//! `EnvFilter` applies the most specific matching directive, and a later directive for the same
//! target replaces an earlier one. So a bare `debug` cannot lift the seeded `binance_sdk=info`,
//! while an explicit `RUST_LOG=binance_sdk=debug` still can when you need the SDK's traffic.
//! Adding the directive *after* reading the environment, as in
//! `EnvFilter::from_default_env().add_directive(..)`, gets this backwards: it silently overrides
//! an explicit request.

mod commission;
mod margin;
mod restatement;
mod rules;
mod server_time;
mod shared;
mod spot;

pub use commission::{CommissionDiscount, CommissionPart, CommissionRates};
pub use margin::{BinanceMargin, BinanceMarginConfig, MarginForcedLiquidation};
pub use spot::{BinanceSpot, BinanceSpotConfig, BinanceSpotConfigError};
