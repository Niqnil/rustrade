//! Binance execution clients.
//!
//! - [`BinanceSpot`] — Binance Spot `ExecutionClient` (REST + signed WebSocket API).
//! - `margin` — Binance Cross Margin `ExecutionClient` (added in a later phase).
//!
//! Both clients share exchange-agnostic infrastructure (reconnect/backoff,
//! rate-limit tracking, event deduplication, error parsing, and the
//! Binance-string parsers) from the `shared` module, so resilience behaviour is
//! implemented once and reused rather than duplicated per client.

mod margin;
mod shared;
mod spot;

pub use spot::{BinanceSpot, BinanceSpotConfig};
