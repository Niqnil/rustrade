//! Binance execution clients.
//!
//! - [`BinanceSpot`] — Binance Spot `ExecutionClient` (REST + signed WebSocket API).
//! - [`BinanceMargin`] — Binance Cross Margin client. Currently provides identity and
//!   configuration ([`BinanceMarginConfig`], [`MarginSideEffect`]); order/stream support and
//!   the `ExecutionClient` trait impl are added in follow-up work.
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
