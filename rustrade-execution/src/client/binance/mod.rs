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
//! Each client reads the symbol's tick, lot and notional rules from Binance's spot `exchangeInfo`
//! on the symbol's first order, and refuses an order that breaks them without sending it: a price
//! or quantity off its grid as [`OrderError::InvalidPrecision`](crate::error::OrderError::InvalidPrecision)
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
//! and order endings the stream missed (see each client's `account_stream`).
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
//! expired, which spends an attempt like any other failure. Its key is kept alive every 30
//! minutes, which Binance documents as enough to keep it valid.
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

mod margin;
mod rules;
mod shared;
mod spot;

pub use margin::{BinanceMargin, BinanceMarginConfig, MarginForcedLiquidation};
pub use spot::{BinanceSpot, BinanceSpotConfig, BinanceSpotConfigError};
