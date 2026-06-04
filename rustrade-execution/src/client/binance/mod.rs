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

mod margin;
mod shared;
mod spot;

pub use margin::{BinanceMargin, BinanceMarginConfig, MarginSideEffect};
pub use spot::{BinanceSpot, BinanceSpotConfig};
