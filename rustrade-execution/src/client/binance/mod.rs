//! Binance execution clients.
//!
//! - [`BinanceSpot`] — Binance Spot `ExecutionClient` (REST + signed WebSocket API).
//! - [`BinanceMargin`] — Binance Cross Margin `ExecutionClient` (REST orders/queries +
//!   a hand-rolled `userListenToken` user-data stream). Configured via
//!   [`BinanceMarginConfig`]/[`MarginSideEffect`].
//!
//! Both clients share exchange-agnostic infrastructure (reconnect/backoff,
//! rate-limit tracking, event deduplication, error parsing, and the
//! Binance-string parsers) from the `shared` module, so resilience behaviour is
//! implemented once and reused rather than duplicated per client.
//!
//! # Logging: `binance_sdk` writes request credentials at `DEBUG`
//!
//! Both clients run on the `binance-sdk` crate, whose WebSocket API client logs every outgoing
//! request at `DEBUG`, after it has been signed:
//!
//! - [`BinanceSpot`] places and cancels orders and subscribes to its user-data stream with signed
//!   requests, so each of those log lines carries the account's **API key** and the request's
//!   **signature**.
//! - [`BinanceMargin`] subscribes to its user-data stream with an unsigned request whose only
//!   credential is the **listen token**, which is logged in the same way.
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
mod order_recovery;
mod shared;
mod spot;

pub use margin::{BinanceMargin, BinanceMarginConfig, MarginSideEffect};
pub use spot::{BinanceSpot, BinanceSpotConfig, BinanceSpotConfigError};
