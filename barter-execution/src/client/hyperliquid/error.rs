//! Error mapping for Hyperliquid SDK errors to barter-execution error types.

use crate::error::{
    ApiError, ConnectivityError, UnindexedApiError, UnindexedClientError, UnindexedOrderError,
};
use barter_instrument::instrument::name::InstrumentNameExchange;

/// Maps Hyperliquid SDK errors to `UnindexedClientError`.
///
/// The SDK uses its own error type internally, so we pattern-match on error messages
/// to determine transient vs permanent errors.
pub fn map_sdk_error(error: hyperliquid_rust_sdk::Error) -> UnindexedClientError {
    let msg = error.to_string();
    let msg_lower = msg.to_lowercase();

    // Check for connectivity/network errors (transient)
    if msg_lower.contains("connection")
        || msg_lower.contains("timeout")
        || msg_lower.contains("network")
        || msg_lower.contains("dns")
        || msg_lower.contains("tls")
        || msg_lower.contains("ssl")
    {
        return UnindexedClientError::Connectivity(ConnectivityError::Socket(msg));
    }

    // Check for rate limiting (transient)
    if msg_lower.contains("rate limit")
        || msg_lower.contains("too many requests")
        || msg_lower.contains("429")
    {
        return UnindexedClientError::Api(UnindexedApiError::RateLimit);
    }

    // Check for authentication errors (permanent)
    if msg_lower.contains("signature")
        || msg_lower.contains("unauthorized")
        || msg_lower.contains("invalid key")
        || msg_lower.contains("authentication")
    {
        return UnindexedClientError::Api(UnindexedApiError::OrderRejected(msg));
    }

    // Default to internal error (assumed non-transient)
    UnindexedClientError::Internal(msg)
}

/// Maps Hyperliquid order placement errors to `UnindexedOrderError`.
///
/// Hyperliquid returns specific error codes/messages for order rejections.
pub fn map_order_error(
    error: hyperliquid_rust_sdk::Error,
    instrument: &InstrumentNameExchange,
) -> UnindexedOrderError {
    let msg = error.to_string();
    let msg_lower = msg.to_lowercase();

    // Instrument not found / invalid
    if msg_lower.contains("unknown")
        || msg_lower.contains("not found")
        || msg_lower.contains("invalid symbol")
    {
        return UnindexedOrderError::Rejected(ApiError::InstrumentInvalid(instrument.clone(), msg));
    }

    // All other rejections (insufficient balance, post-only crossed, price precision, etc.)
    // Use OrderRejected since we can't cleanly extract the specific error type without
    // more structured error responses from the SDK.
    UnindexedOrderError::Rejected(ApiError::OrderRejected(msg))
}
