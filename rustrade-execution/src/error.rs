//! Error types for [`ExecutionClient`](super::client::ExecutionClient) operations.
//!
//! # Retry Semantics
//!
//! Use [`ClientError::is_transient`] to determine if an operation should be retried.
//! Transient errors (connectivity issues, rate limits) may succeed on retry with
//! appropriate backoff. Non-transient errors (invalid instrument, insufficient
//! balance) will fail identically on retry — the caller must change the request.
//!
//! The `is_transient()` method is the stable contract for retry decisions. Prefer
//! it over pattern matching on specific variants, as the internal taxonomy may
//! evolve while `is_transient()` semantics remain stable.

use rustrade_instrument::{
    asset::{AssetIndex, name::AssetNameExchange},
    exchange::ExchangeId,
    instrument::{InstrumentIndex, name::InstrumentNameExchange},
};
use rustrade_integration::error::SocketError;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Type alias for a [`ClientError`] that is keyed on [`AssetNameExchange`] and
/// [`InstrumentNameExchange`] (yet to be indexed).
pub type UnindexedClientError = ClientError<AssetNameExchange, InstrumentNameExchange>;

/// Type alias for a [`ApiError`] that is keyed on [`AssetNameExchange`] and
/// [`InstrumentNameExchange`] (yet to be indexed).
pub type UnindexedApiError = ApiError<AssetNameExchange, InstrumentNameExchange>;

/// Type alias for a [`OrderError`] that is keyed on [`AssetNameExchange`] and
/// [`InstrumentNameExchange`] (yet to be indexed).
pub type UnindexedOrderError = OrderError<AssetNameExchange, InstrumentNameExchange>;

/// Represents all errors produced by an [`ExecutionClient`](super::client::ExecutionClient).
#[non_exhaustive]
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize, Error)]
pub enum ClientError<AssetKey = AssetIndex, InstrumentKey = InstrumentIndex> {
    /// Connectivity based error.
    ///
    /// eg/ Timeout.
    #[error("Connectivity: {0}")]
    Connectivity(#[from] ConnectivityError),

    /// API based error.
    ///
    /// eg/ RateLimit.
    #[error("API: {0}")]
    Api(#[from] ApiError<AssetKey, InstrumentKey>),

    /// A background task panicked or was cancelled during an operation.
    ///
    /// This indicates a bug or unexpected runtime condition (e.g., a tokio
    /// `spawn_blocking` task panicked). The operation was not retried and
    /// the caller should treat this as non-recoverable, requiring operator
    /// attention.
    #[error("task failed: {0}")]
    TaskFailed(String),

    /// An opaque error from an upstream library that cannot be further classified.
    ///
    /// This is a catch-all for errors that don't fit into [`Self::Connectivity`] or
    /// [`Self::Api`] categories — typically because the upstream library (e.g., ibapi,
    /// binance-sdk) returns unstructured errors.
    ///
    /// Conservatively treated as non-transient. If you encounter this error
    /// frequently, consider filing an issue to improve error classification.
    #[error("internal error: {0}")]
    Internal(String),

    /// Activity pagination was truncated at the page limit.
    ///
    /// The returned data from the underlying call is a partial result. This error
    /// indicates that more activities exist beyond the safety limit, typically due
    /// to a very long outage (>5000 fills). Callers should alert operators and
    /// consider manual reconciliation.
    #[error("activity pagination truncated at {limit} pages — data may be incomplete")]
    Truncated {
        /// Maximum number of pages that were fetched before truncation.
        limit: usize,
    },

    /// Open orders snapshot was truncated at the API's row limit.
    ///
    /// Unlike [`Self::Truncated`] (which applies to paginated activity fetches), this
    /// error indicates a single-request endpoint hit its maximum row count.
    /// Alpaca's `/v2/orders` endpoint caps results at 500; accounts with more
    /// concurrent open orders will have an incomplete snapshot.
    ///
    /// Callers should alert operators — an incomplete order snapshot can cause
    /// duplicate submissions, missed cancellations, or incorrect position sizing.
    #[error("open orders snapshot truncated at {limit} results — data may be incomplete")]
    TruncatedSnapshot {
        /// Maximum number of rows returned by the single-request endpoint.
        limit: usize,
    },
}

impl<AssetKey, InstrumentKey> ClientError<AssetKey, InstrumentKey> {
    /// Returns `true` if this error is likely transient and the operation
    /// may succeed if retried after a suitable backoff.
    ///
    /// The caller is responsible for retry limits and backoff strategy.
    /// This method classifies the error only — it does not implement policy.
    ///
    /// # Transient errors
    /// - [`Connectivity`](Self::Connectivity) errors (timeout, socket, offline)
    /// - [`Api::RateLimit`](ApiError::RateLimit)
    ///
    /// # Non-transient errors
    /// - Other [`Api`](Self::Api) errors (invalid instrument, insufficient balance, etc.)
    /// - [`TaskFailed`](Self::TaskFailed) (indicates a bug)
    /// - [`Internal`](Self::Internal) (unknown — conservatively non-transient)
    /// - [`Truncated`](Self::Truncated) / [`TruncatedSnapshot`](Self::TruncatedSnapshot)
    pub fn is_transient(&self) -> bool {
        match self {
            Self::Connectivity(e) => e.is_transient(),
            Self::Api(ApiError::RateLimit) => true,
            Self::Api(_) => false,
            Self::TaskFailed(_) => false,
            Self::Internal(_) => false,
            Self::Truncated { .. } => false,
            Self::TruncatedSnapshot { .. } => false,
        }
    }
}

/// Represents all connectivity-centric errors.
///
/// Connectivity errors are generally intermittent / non-deterministic (eg/ Timeout).
/// All variants are transient — retry with exponential backoff (typically 1-30s).
#[non_exhaustive]
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize, Error)]
pub enum ConnectivityError {
    /// Exchange is offline, likely due to scheduled maintenance.
    ///
    /// Transient — retry with backoff. Maintenance windows typically last minutes
    /// to hours; consider longer backoff intervals (30s-5min) to avoid log spam.
    #[error("Exchange offline: {0}")]
    ExchangeOffline(ExchangeId),

    /// Request timed out before a response was received.
    ///
    /// Transient — retry with backoff. May indicate network congestion, server
    /// overload, or an overly aggressive timeout. Consider increasing timeout
    /// on subsequent attempts.
    #[error("ExecutionRequest timed out")]
    Timeout,

    /// Network-level socket error (connection refused, reset, DNS failure, etc.).
    ///
    /// Transient — retry with backoff. If persistent, may indicate firewall
    /// issues, incorrect endpoint configuration, or prolonged server outage.
    #[error("{0}")]
    Socket(String),
}

impl From<SocketError> for ConnectivityError {
    fn from(value: SocketError) -> Self {
        Self::Socket(value.to_string())
    }
}

impl ConnectivityError {
    /// Returns `true` if this connectivity error is transient.
    ///
    /// All connectivity errors are considered transient — they represent
    /// temporary network or server conditions that may resolve with retry.
    pub fn is_transient(&self) -> bool {
        match self {
            Self::ExchangeOffline(_) => true,
            Self::Timeout => true,
            Self::Socket(_) => true,
        }
    }
}

/// Represents all API errors generated by an exchange.
///
/// These typically indicate a request is invalid for some reason (eg/ BalanceInsufficient).
/// Most variants are **not transient** — the same request will fail identically on retry.
/// The exception is [`RateLimit`](Self::RateLimit), which is transient.
#[non_exhaustive]
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize, Error)]
pub enum ApiError<AssetKey = AssetIndex, InstrumentKey = InstrumentIndex> {
    /// Provided asset identifier is invalid or not supported.
    ///
    /// For example:
    /// - The [`AssetNameExchange`] was an invalid format.
    ///
    /// Not transient — do not retry. The asset identifier must be corrected.
    #[error("asset {0} invalid: {1}")]
    AssetInvalid(AssetKey, String),

    /// Provided instrument identifier is invalid or not supported.
    ///
    /// For example:
    /// - The exchange does not have a market for an instrument.
    /// - The [`InstrumentNameExchange`] was an invalid format.
    ///
    /// Not transient — do not retry. The instrument identifier must be corrected.
    #[error("instrument {0} invalid: {1}")]
    InstrumentInvalid(InstrumentKey, String),

    /// Request was rejected due to rate limiting.
    ///
    /// The exchange enforces request quotas and the caller has exceeded them.
    /// Some exchanges provide a `Retry-After` header or similar hint; the client
    /// may incorporate this into internal retry logic before surfacing this error.
    ///
    /// Transient — retry with backoff. Typical backoff is 10-60 seconds, but
    /// respect exchange-specific guidance if available.
    #[error("rate limit exceeded")]
    RateLimit,

    /// Authentication failed (invalid credentials, expired key, bad signature).
    ///
    /// Unlike other API errors which affect a single request, authentication
    /// failures indicate that **all** subsequent requests will fail until
    /// credentials are corrected. Callers should halt trading and alert operators.
    ///
    /// Not transient — do not retry. Fix credentials and restart.
    #[error("authentication failed: {0}")]
    Unauthenticated(String),

    /// Balance of an asset is insufficient to execute the requested operation.
    ///
    /// # Warning: `AssetKey` field may hold an instrument name, not an asset name
    ///
    /// Some `ExecutionClient` implementations (e.g. `BinanceSpot`) populate the
    /// `AssetKey` field with the **instrument name** (e.g. `"BTCUSDT"`) rather than
    /// the specific low-balance asset (e.g. `"BTC"` or `"USDT"`), because splitting
    /// a symbol into base/quote requires exchange symbol-info metadata not available
    /// at error-parse time. Do **not** pattern-match on the `AssetKey` value to
    /// identify the specific low-balance asset — use the `String` field for
    /// diagnostics only.
    ///
    /// Not transient — do not retry the same request. Reduce order size or
    /// deposit additional funds.
    #[error("asset {0} balance insufficient: {1}")]
    BalanceInsufficient(AssetKey, String),

    /// Order was rejected by the exchange for a business rule violation.
    ///
    /// Common causes include: price outside allowed range, quantity below
    /// minimum, post-only order would cross, reduce-only with no position.
    ///
    /// Not transient — do not retry the same request. Adjust order parameters.
    #[error("order rejected: {0}")]
    OrderRejected(String),

    /// Cancel request failed because the order was already cancelled.
    ///
    /// This is a state conflict, not an error per se — the desired end state
    /// (order cancelled) has already been achieved.
    ///
    /// Not transient — do not retry. The order is already in the cancelled state.
    #[error("order already cancelled")]
    OrderAlreadyCancelled,

    /// Cancel request failed because the order was already fully filled.
    ///
    /// This is a state conflict — the order completed before the cancel arrived.
    /// The caller should reconcile their local state with the fill.
    ///
    /// Not transient — do not retry. The order no longer exists to cancel.
    #[error("order already fully filled")]
    OrderAlreadyFullyFilled,
}

/// Represents all errors that can be generated when cancelling or opening orders.
///
/// This is a subset of [`ClientError`] for order-specific operations. Use
/// [`is_transient()`](Self::is_transient) to determine retry eligibility.
#[non_exhaustive]
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize, Error)]
pub enum OrderError<AssetKey = AssetIndex, InstrumentKey = InstrumentIndex> {
    /// Connectivity-based error (timeout, socket failure, exchange offline).
    ///
    /// Transient — retry with backoff. See [`ConnectivityError`] for details.
    #[error("connectivity: {0}")]
    Connectivity(#[from] ConnectivityError),

    /// API-based error (rate limit, invalid instrument, order rejected, etc.).
    ///
    /// Retry semantics depend on the specific [`ApiError`] variant. Only
    /// [`ApiError::RateLimit`] is transient; other variants are not.
    #[error("order rejected: {0}")]
    Rejected(#[from] ApiError<AssetKey, InstrumentKey>),

    /// The order type is not supported by this connector.
    ///
    /// Non-transient — the connector does not support this order type (e.g.,
    /// trailing stop orders on a connector that only supports market/limit).
    #[error("unsupported order type: {0}")]
    UnsupportedOrderType(String),
}

impl<AssetKey, InstrumentKey> OrderError<AssetKey, InstrumentKey> {
    /// Returns `true` if this error is likely transient and the operation
    /// may succeed if retried after a suitable backoff.
    ///
    /// # Transient errors
    /// - [`Connectivity`](Self::Connectivity) errors (timeout, socket, offline)
    /// - [`Rejected(ApiError::RateLimit)`](ApiError::RateLimit)
    ///
    /// # Non-transient errors
    /// - Other [`Rejected`](Self::Rejected) errors (invalid instrument, insufficient balance, etc.)
    pub fn is_transient(&self) -> bool {
        match self {
            Self::Connectivity(e) => e.is_transient(),
            Self::Rejected(ApiError::RateLimit) => true,
            Self::Rejected(_) => false,
            Self::UnsupportedOrderType(_) => false,
        }
    }
}

/// Represents errors related to exchange, asset and instrument identifier key lookups.
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize, Error)]
pub enum KeyError {
    /// Indicates an [`ExchangeId`] was encountered that was not indexed, so does not have a
    /// corresponding `ExchangeIndex`.
    #[error("ExchangeId: {0}")]
    ExchangeId(String),

    /// Indicates an [`AssetNameExchange`] was encountered that was not indexed, so does not have a
    /// corresponding [`AssetIndex`].
    #[error("AssetKey: {0}")]
    AssetKey(String),

    /// Indicates an [`InstrumentNameExchange`] was encountered that was no indexed, so does
    /// not have a corresponding [`InstrumentIndex`].
    #[error("InstrumentKey: {0}")]
    InstrumentKey(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_connectivity_error_is_transient() {
        assert!(ConnectivityError::Timeout.is_transient());
        assert!(ConnectivityError::Socket("connection refused".into()).is_transient());
        assert!(ConnectivityError::ExchangeOffline(ExchangeId::BinanceSpot).is_transient());
    }

    #[test]
    fn test_client_error_is_transient_connectivity() {
        let err: ClientError = ClientError::Connectivity(ConnectivityError::Timeout);
        assert!(err.is_transient());

        let err: ClientError = ClientError::Connectivity(ConnectivityError::Socket("err".into()));
        assert!(err.is_transient());
    }

    #[test]
    fn test_client_error_is_transient_rate_limit() {
        let err: ClientError = ClientError::Api(ApiError::RateLimit);
        assert!(err.is_transient());
    }

    #[test]
    fn test_client_error_not_transient_api_errors() {
        let err: ClientError =
            ClientError::Api(ApiError::AssetInvalid(AssetIndex(0), "bad".into()));
        assert!(!err.is_transient(), "expected non-transient for {:?}", err);

        let err: ClientError =
            ClientError::Api(ApiError::BalanceInsufficient(AssetIndex(0), "low".into()));
        assert!(!err.is_transient(), "expected non-transient for {:?}", err);

        let err: ClientError = ClientError::Api(ApiError::InstrumentInvalid(
            InstrumentIndex(0),
            "bad".into(),
        ));
        assert!(!err.is_transient(), "expected non-transient for {:?}", err);

        let err: ClientError = ClientError::Api(ApiError::OrderRejected("rejected".into()));
        assert!(!err.is_transient(), "expected non-transient for {:?}", err);

        let err: ClientError = ClientError::Api(ApiError::OrderAlreadyCancelled);
        assert!(!err.is_transient(), "expected non-transient for {:?}", err);

        let err: ClientError = ClientError::Api(ApiError::OrderAlreadyFullyFilled);
        assert!(!err.is_transient(), "expected non-transient for {:?}", err);

        let err: ClientError =
            ClientError::Api(ApiError::Unauthenticated("invalid signature".into()));
        assert!(!err.is_transient(), "expected non-transient for {:?}", err);
    }

    #[test]
    fn test_client_error_not_transient_task_failed() {
        let err: ClientError = ClientError::TaskFailed("task panicked".into());
        assert!(!err.is_transient());
    }

    #[test]
    fn test_client_error_not_transient_internal() {
        let err: ClientError = ClientError::Internal("unknown error".into());
        assert!(!err.is_transient());
    }

    #[test]
    fn test_client_error_not_transient_truncated() {
        let err: ClientError = ClientError::Truncated { limit: 100 };
        assert!(!err.is_transient(), "expected non-transient for {:?}", err);

        let err: ClientError = ClientError::TruncatedSnapshot { limit: 500 };
        assert!(!err.is_transient(), "expected non-transient for {:?}", err);
    }

    #[test]
    fn test_client_error_is_transient_exchange_offline() {
        let err: ClientError =
            ClientError::Connectivity(ConnectivityError::ExchangeOffline(ExchangeId::BinanceSpot));
        assert!(err.is_transient(), "expected transient for {:?}", err);
    }

    #[test]
    fn test_order_error_is_transient_connectivity() {
        let err: UnindexedOrderError = OrderError::Connectivity(ConnectivityError::Timeout);
        assert!(err.is_transient(), "expected transient for {:?}", err);

        let err: UnindexedOrderError =
            OrderError::Connectivity(ConnectivityError::Socket("connection reset".into()));
        assert!(err.is_transient(), "expected transient for {:?}", err);

        let err: UnindexedOrderError =
            OrderError::Connectivity(ConnectivityError::ExchangeOffline(ExchangeId::BinanceSpot));
        assert!(err.is_transient(), "expected transient for {:?}", err);
    }

    #[test]
    fn test_order_error_is_transient_rate_limit() {
        let err: UnindexedOrderError = OrderError::Rejected(ApiError::RateLimit);
        assert!(err.is_transient(), "expected transient for {:?}", err);
    }

    #[test]
    fn test_order_error_not_transient_api_errors() {
        let err: UnindexedOrderError =
            OrderError::Rejected(ApiError::OrderRejected("price out of range".into()));
        assert!(!err.is_transient(), "expected non-transient for {:?}", err);

        let err: UnindexedOrderError = OrderError::Rejected(ApiError::OrderAlreadyCancelled);
        assert!(!err.is_transient(), "expected non-transient for {:?}", err);

        let err: UnindexedOrderError = OrderError::Rejected(ApiError::BalanceInsufficient(
            AssetNameExchange::from("BTC"),
            "insufficient".into(),
        ));
        assert!(!err.is_transient(), "expected non-transient for {:?}", err);
    }
}
