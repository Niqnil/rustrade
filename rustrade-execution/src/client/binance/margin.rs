//! Binance Cross Margin execution client.
//!
//! Placeholder module. The `BinanceMargin` client (sharing the auth/signing,
//! rate-limit, dedup, and reconnect infrastructure in `super::shared`) is added in a
//! later phase. It implements the same `ExecutionClient` trait as `super::spot` so
//! callers do not branch on spot-vs-margin transport.
