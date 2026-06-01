//! Binance Cross Margin execution client.
//!
//! [`BinanceMargin`] is a margin counterpart to [`super::spot::BinanceSpot`]. It shares the
//! exchange-agnostic infrastructure in [`super::shared`] (rate-limit tracking, reconnect/backoff,
//! event deduplication, error parsing) and is intended to implement the same `ExecutionClient`
//! trait so callers do not branch on spot-vs-margin transport.
//!
//! ## Scope
//! This module currently provides the client's *identity and configuration* only: the
//! [`BinanceMargin`] struct, its [`BinanceMarginConfig`], and the [`MarginSideEffect`] borrow
//! policy. Order submission, account snapshots, and the live user-data stream are added in
//! follow-up work; until then `BinanceMargin` is not yet a usable `ExecutionClient`.
//!
//! ## Borrow/repay
//! Margin orders carry a `sideEffectType` controlling whether the venue borrows on entry and
//! repays on close. This is configured once per client via [`MarginSideEffect`] (see its docs for
//! the silent-borrow footgun under the default [`MarginSideEffect::AutoBorrowRepay`]).
//!
//! ## No testnet
//! Binance margin/SAPI exists on **no** testnet — the SDK exposes only
//! `MarginTradingRestApi::production`. A `testnet: true` config is therefore inert for margin and
//! always resolves to production endpoints; the constructor logs a warning so this is observable
//! rather than silent. See [`BinanceMarginConfig::testnet`].

use super::shared::RateLimitTracker;
use binance_sdk::{
    common::config::{ConfigurationRestApi, ConfigurationWebsocketApi},
    margin_trading::{MarginTradingRestApi, rest_api::RestApi},
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::warn;

// ---------------------------------------------------------------------------
// Margin sideEffectType
// ---------------------------------------------------------------------------

/// Margin `sideEffectType` borrow/repay policy, fixed once per [`BinanceMargin`] client.
///
/// Only the two *intent-agnostic* modes are exposed as client-level variants. The borrow-vs-repay
/// decision derives from open/close direction — position state the library deliberately does not
/// track — so the venue is left to infer it from the account's loan state. The per-order modes
/// (`MARGIN_BUY` / `AUTO_REPAY`) are intentionally **not** modelled here; they belong to a future
/// generic per-order `RequestOpen::margin_effect` field that all margin adapters would map, added
/// only when a venue with genuine per-order borrow intent (e.g. another CEX margin) appears.
/// Mixed borrow appetite on a single venue today means running two clients (one per mode).
///
/// The `Serialize`/`Deserialize` wire form is the config-file value (`"auto_borrow_repay"` /
/// `"no_borrow"`); this is distinct from the Binance API value returned by
/// [`as_binance_str`](Self::as_binance_str) (`"AUTO_BORROW_REPAY"` / `"NO_SIDE_EFFECT"`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MarginSideEffect {
    /// `AUTO_BORROW_REPAY` — the venue borrows on entry and repays on close as needed.
    ///
    /// # Warning
    /// This is the default because it makes shorting work out of the box, but it means a
    /// **mis-sized order silently takes on debt**: an order larger than the free balance borrows
    /// the shortfall without any further opt-in (same footgun class as `RequestOpen::reduce_only`).
    /// Use [`MarginSideEffect::NoBorrow`] for a client that must never silently borrow.
    #[default]
    AutoBorrowRepay,
    /// `NO_SIDE_EFFECT` — never borrow or repay; orders that would require borrowing are rejected
    /// by the venue. The conservative opt-out from the [`AutoBorrowRepay`](Self::AutoBorrowRepay)
    /// silent-borrow behaviour.
    NoBorrow,
}

impl MarginSideEffect {
    /// The Binance `sideEffectType` wire string for this policy.
    pub fn as_binance_str(self) -> &'static str {
        match self {
            MarginSideEffect::AutoBorrowRepay => "AUTO_BORROW_REPAY",
            MarginSideEffect::NoBorrow => "NO_SIDE_EFFECT",
        }
    }
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for the [`BinanceMargin`] execution client.
///
/// Mirrors [`BinanceSpotConfig`](super::spot::BinanceSpotConfig)'s credential handling (private
/// keys, secret-redacting [`Debug`], `Deserialize`-only) and adds margin-specific knobs.
// Serialize intentionally omitted — would expose secret_key in plaintext
#[derive(Clone, Deserialize)]
pub struct BinanceMarginConfig {
    // not pub — prevents accidental credential exposure via struct access.
    // Use BinanceMarginConfig::new() to construct, or deserialize from config file.
    api_key: String,
    secret_key: String,
    /// Use testnet endpoints instead of production.
    ///
    /// **Inert for margin:** Binance margin/SAPI has no testnet, so this field is always treated
    /// as production. Retained to mirror the spot config shape; [`BinanceMargin::new`] warns if it
    /// is set to `true`.
    pub testnet: bool,
    /// Isolated margin when `true`; cross margin (account-wide) when `false`.
    ///
    /// Defaults to `false` (cross). Isolated support is a follow-up; cross is the primary mode.
    ///
    /// # Warning
    /// Setting this to `true` is **not yet honoured**: the client currently operates as cross
    /// margin regardless, logging a warning at construction (see [`BinanceMargin::new`]) rather
    /// than failing silently. Isolated execution is a separate follow-up.
    #[serde(default)]
    pub is_isolated: bool,
    /// Borrow/repay policy applied to every order placed by this client.
    ///
    /// Defaults to [`MarginSideEffect::AutoBorrowRepay`] — see its `# Warning` on silent borrowing.
    #[serde(default)]
    pub side_effect: MarginSideEffect,
}

// custom Debug to avoid leaking credentials in logs
impl std::fmt::Debug for BinanceMarginConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BinanceMarginConfig")
            .field("api_key", &"***")
            .field("secret_key", &"***")
            .field("testnet", &self.testnet)
            .field("is_isolated", &self.is_isolated)
            .field("side_effect", &self.side_effect)
            .finish()
    }
}

impl BinanceMarginConfig {
    /// Construct a [`BinanceMarginConfig`] with explicit control over every field.
    ///
    /// Prefer [`BinanceMarginConfig::cross_margin`] for the common case, or deserialize from a
    /// config file to keep credentials out of source. The required positional args are the
    /// credentials plus any non-defaultable knobs; defaultable knobs (`is_isolated`, `side_effect`)
    /// are exposed here for full control and via `#[serde(default)]` for the file path.
    pub fn new(
        api_key: String,
        secret_key: String,
        testnet: bool,
        is_isolated: bool,
        side_effect: MarginSideEffect,
    ) -> Self {
        Self {
            api_key,
            secret_key,
            testnet,
            is_isolated,
            side_effect,
        }
    }

    /// Convenience constructor for the common case: cross margin, production endpoints, and the
    /// default [`MarginSideEffect::AutoBorrowRepay`] borrow/repay policy.
    ///
    /// Equivalent to [`new`](Self::new) with `testnet = false`, `is_isolated = false`, and the
    /// default `side_effect`. Use [`new`](Self::new) when any of those need to differ.
    pub fn cross_margin(api_key: String, secret_key: String) -> Self {
        Self::new(
            api_key,
            secret_key,
            false,
            false,
            MarginSideEffect::default(),
        )
    }

    /// Read-only access to the API key (e.g. for logging or header construction).
    pub fn api_key(&self) -> &str {
        &self.api_key
    }
}

// ---------------------------------------------------------------------------
// BinanceMargin client
// ---------------------------------------------------------------------------

/// Binance Cross Margin execution client using the official binance-sdk.
///
/// Places orders and queries account state over the margin REST API
/// (`margin_trading::rest_api`). The live user-data stream (added later) uses a hand-rolled
/// `userListenToken` flow over the WS API, not the SDK's retired listen-key path.
///
/// See the [module docs](self) for scope, borrow/repay behaviour, and the no-testnet caveat.
#[derive(Clone)]
pub struct BinanceMargin {
    config: Arc<BinanceMarginConfig>,
    // REST client for orders/queries. Consumed by the order-submission and account-snapshot
    // methods added in follow-up work.
    #[allow(dead_code)] // wired up by the REST order/query methods added in follow-up work
    rest: Arc<RestApi>,
    // WS-API configuration (credentials → common-layer config). Held here so the live user-data
    // stream — built later as a directly-constructed `common::websocket::WebsocketApi`, which needs
    // a connection pool that only exists at connect time — can consume it without re-reading creds.
    #[allow(dead_code)] // consumed when the user-data stream connection is built in follow-up work
    ws_config: ConfigurationWebsocketApi,
    // shared rate-limit tracker across all REST calls
    #[allow(dead_code)] // applied around REST calls by the order/query methods in follow-up work
    rate_limiter: Arc<RateLimitTracker>,
}

impl std::fmt::Debug for BinanceMargin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BinanceMargin")
            .field("testnet", &self.config.testnet)
            .field("is_isolated", &self.config.is_isolated)
            .field("side_effect", &self.config.side_effect)
            .finish_non_exhaustive()
    }
}

impl BinanceMargin {
    /// Construct a `BinanceMargin` client from its configuration.
    ///
    /// # Panics
    /// Panics if the binance-sdk configuration builder fails (e.g. empty or malformed
    /// API key/secret), matching [`BinanceSpot`](super::spot::BinanceSpot)'s startup contract.
    pub fn new(config: BinanceMarginConfig) -> Self {
        if config.testnet {
            warn!(
                "BinanceMarginConfig.testnet = true is ignored: Binance margin has no testnet; \
                 using production endpoints"
            );
        }
        if config.is_isolated {
            warn!(
                "BinanceMarginConfig.is_isolated = true is not yet supported: isolated margin is a \
                 follow-up; operating as cross margin"
            );
        }
        let rest = Self::build_rest(&config);
        let ws_config = Self::build_ws_config(&config);
        Self {
            config: Arc::new(config),
            rest,
            ws_config,
            rate_limiter: Arc::new(RateLimitTracker::new()),
        }
    }

    /// # Panics
    /// Panics if the binance-sdk configuration builder fails (invalid credentials format).
    #[allow(clippy::expect_used)] // Documented panic: invalid credentials detected at startup
    fn build_rest(config: &BinanceMarginConfig) -> Arc<RestApi> {
        let rest_config = ConfigurationRestApi::builder()
            .api_key(config.api_key.clone())
            .api_secret(config.secret_key.clone())
            .build()
            .expect("failed to build Binance margin REST configuration");

        // Margin/SAPI has no testnet — `MarginTradingRestApi` exposes only `production`.
        Arc::new(MarginTradingRestApi::production(rest_config))
    }

    /// Build the WS-API configuration for the user-data stream.
    ///
    /// The connection itself is established later (it requires a live connection pool); this only
    /// captures the credentials at the common-layer config so the stream task need not re-read them.
    ///
    /// # Panics
    /// Panics if the binance-sdk configuration builder fails (invalid credentials format).
    #[allow(clippy::expect_used)] // Documented panic: invalid credentials detected at startup
    fn build_ws_config(config: &BinanceMarginConfig) -> ConfigurationWebsocketApi {
        ConfigurationWebsocketApi::builder()
            .api_key(config.api_key.clone())
            .api_secret(config.secret_key.clone())
            .build()
            .expect("failed to build Binance margin WebSocket configuration")
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;

    #[test]
    fn margin_side_effect_default_is_auto_borrow_repay() {
        assert_eq!(
            MarginSideEffect::default(),
            MarginSideEffect::AutoBorrowRepay
        );
    }

    #[test]
    fn margin_side_effect_wire_strings() {
        assert_eq!(
            MarginSideEffect::AutoBorrowRepay.as_binance_str(),
            "AUTO_BORROW_REPAY"
        );
        assert_eq!(
            MarginSideEffect::NoBorrow.as_binance_str(),
            "NO_SIDE_EFFECT"
        );
    }

    #[test]
    fn margin_side_effect_serde_round_trip() {
        // serde/config-file form is snake_case, distinct from the Binance wire string (as_binance_str).
        assert_eq!(
            serde_json::to_string(&MarginSideEffect::AutoBorrowRepay).unwrap(),
            r#""auto_borrow_repay""#
        );
        assert_eq!(
            serde_json::to_string(&MarginSideEffect::NoBorrow).unwrap(),
            r#""no_borrow""#
        );
        assert_eq!(
            serde_json::from_str::<MarginSideEffect>(r#""auto_borrow_repay""#).unwrap(),
            MarginSideEffect::AutoBorrowRepay
        );
        assert_eq!(
            serde_json::from_str::<MarginSideEffect>(r#""no_borrow""#).unwrap(),
            MarginSideEffect::NoBorrow
        );
    }

    #[test]
    fn config_debug_redacts_secrets() {
        let config = BinanceMarginConfig::new(
            "my_api_key".to_string(),
            "my_secret_key".to_string(),
            false,
            false,
            MarginSideEffect::default(),
        );
        let debug = format!("{config:?}");
        assert!(!debug.contains("my_api_key"));
        assert!(!debug.contains("my_secret_key"));
        assert!(debug.contains("***"));
    }

    #[test]
    fn cross_margin_uses_common_case_defaults() {
        let config = BinanceMarginConfig::cross_margin("k".to_string(), "s".to_string());
        assert!(!config.testnet);
        assert!(!config.is_isolated);
        assert_eq!(config.side_effect, MarginSideEffect::AutoBorrowRepay);
        assert_eq!(config.api_key(), "k");
    }

    #[test]
    fn config_deserializes_with_defaults() {
        // is_isolated and side_effect default when omitted; testnet has no default and must be present.
        let config: BinanceMarginConfig =
            serde_json::from_str(r#"{"api_key":"k","secret_key":"s","testnet":false}"#)
                .expect("deserialize");
        assert!(!config.is_isolated);
        assert_eq!(config.side_effect, MarginSideEffect::AutoBorrowRepay);
    }

    #[test]
    fn config_deserializes_explicit_side_effect() {
        let config: BinanceMarginConfig = serde_json::from_str(
            r#"{"api_key":"k","secret_key":"s","testnet":false,"is_isolated":true,"side_effect":"no_borrow"}"#,
        )
        .expect("deserialize");
        assert!(config.is_isolated);
        assert_eq!(config.side_effect, MarginSideEffect::NoBorrow);
    }
}
