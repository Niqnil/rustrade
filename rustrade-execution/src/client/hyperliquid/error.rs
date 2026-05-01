//! Error mapping for Hyperliquid SDK errors to rustrade-execution error types.

use crate::error::{
    ApiError, ConnectivityError, UnindexedApiError, UnindexedClientError, UnindexedOrderError,
};
use rustrade_instrument::instrument::name::InstrumentNameExchange;

/// Maps Hyperliquid SDK errors to `UnindexedClientError`.
///
/// The SDK uses its own error type internally, so we pattern-match on error messages
/// to determine transient vs permanent errors.
pub fn map_sdk_error(error: hyperliquid_rust_sdk::Error) -> UnindexedClientError {
    let msg = error.to_string();
    let msg_lower = msg.to_lowercase();

    // Check for rate limiting first (transient) — avoid full msg allocation when possible
    if msg_lower.contains("rate limit")
        || msg_lower.contains("too many requests")
        || msg_lower.contains("429")
    {
        return UnindexedClientError::Api(UnindexedApiError::RateLimit);
    }

    // Check for connectivity/network errors (transient)
    // Includes HTTP 5xx server errors which are typically transient
    if msg_lower.contains("connection")
        || msg_lower.contains("timeout")
        || msg_lower.contains("network")
        || msg_lower.contains("dns")
        || msg_lower.contains("tls")
        || msg_lower.contains("ssl")
        || msg_lower.contains(" 500")
        || msg_lower.contains(" 502")
        || msg_lower.contains(" 503")
        || msg_lower.contains(" 504")
        || msg_lower.contains("bad gateway")
        || msg_lower.contains("service unavailable")
        || msg_lower.contains("gateway timeout")
    {
        return UnindexedClientError::Connectivity(ConnectivityError::Socket(msg));
    }

    // Check for authentication errors (permanent)
    // Includes "eip712" for SDK's Error::Eip712 variant (EIP-712 signing failures)
    if msg_lower.contains("signature")
        || msg_lower.contains("unauthorized")
        || msg_lower.contains("invalid key")
        || msg_lower.contains("authentication")
        || msg_lower.contains("eip712")
    {
        return UnindexedClientError::Api(UnindexedApiError::Unauthenticated(msg));
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

#[cfg(test)]
mod tests {
    use super::*;
    use hyperliquid_rust_sdk::Error;

    fn make_sdk_error(msg: &str) -> Error {
        Error::GenericReader(msg.to_string())
    }

    #[test]
    fn test_map_sdk_error_connectivity() {
        let err = map_sdk_error(make_sdk_error("connection refused"));
        assert!(matches!(err, UnindexedClientError::Connectivity(_)));

        let err = map_sdk_error(make_sdk_error("request timeout occurred"));
        assert!(matches!(err, UnindexedClientError::Connectivity(_)));

        let err = map_sdk_error(make_sdk_error("DNS lookup failed"));
        assert!(matches!(err, UnindexedClientError::Connectivity(_)));

        let err = map_sdk_error(make_sdk_error("TLS handshake error"));
        assert!(matches!(err, UnindexedClientError::Connectivity(_)));
    }

    #[test]
    fn test_map_sdk_error_5xx_transient() {
        // HTTP 5xx errors should be transient (server-side failures)
        let err = map_sdk_error(make_sdk_error("HTTP 500 Internal Server Error"));
        assert!(matches!(err, UnindexedClientError::Connectivity(_)));

        let err = map_sdk_error(make_sdk_error("502 Bad Gateway"));
        assert!(matches!(err, UnindexedClientError::Connectivity(_)));

        let err = map_sdk_error(make_sdk_error("503 Service Unavailable"));
        assert!(matches!(err, UnindexedClientError::Connectivity(_)));

        let err = map_sdk_error(make_sdk_error("504 Gateway Timeout"));
        assert!(matches!(err, UnindexedClientError::Connectivity(_)));
    }

    #[test]
    fn test_map_sdk_error_rate_limit() {
        let err = map_sdk_error(make_sdk_error("rate limit exceeded"));
        assert!(matches!(
            err,
            UnindexedClientError::Api(UnindexedApiError::RateLimit)
        ));

        let err = map_sdk_error(make_sdk_error("too many requests"));
        assert!(matches!(
            err,
            UnindexedClientError::Api(UnindexedApiError::RateLimit)
        ));

        let err = map_sdk_error(make_sdk_error("HTTP 429"));
        assert!(matches!(
            err,
            UnindexedClientError::Api(UnindexedApiError::RateLimit)
        ));
    }

    #[test]
    fn test_map_sdk_error_auth() {
        let err = map_sdk_error(make_sdk_error("invalid signature"));
        assert!(matches!(
            err,
            UnindexedClientError::Api(UnindexedApiError::Unauthenticated(_))
        ));

        let err = map_sdk_error(make_sdk_error("unauthorized access"));
        assert!(matches!(
            err,
            UnindexedClientError::Api(UnindexedApiError::Unauthenticated(_))
        ));

        // SDK's Error::Eip712 variant formats as "Error from Eip712 struct: ..."
        let err = map_sdk_error(make_sdk_error("Error from Eip712 struct: invalid key"));
        assert!(matches!(
            err,
            UnindexedClientError::Api(UnindexedApiError::Unauthenticated(_))
        ));
    }

    #[test]
    fn test_map_sdk_error_internal() {
        let err = map_sdk_error(make_sdk_error("some unknown error"));
        assert!(matches!(err, UnindexedClientError::Internal(_)));
    }

    #[test]
    fn test_map_order_error_instrument_invalid() {
        let instrument = InstrumentNameExchange::from("INVALID-USD-PERP");

        let err = map_order_error(make_sdk_error("unknown asset"), &instrument);
        assert!(matches!(
            err,
            UnindexedOrderError::Rejected(ApiError::InstrumentInvalid(_, _))
        ));

        let err = map_order_error(make_sdk_error("Asset not found"), &instrument);
        assert!(matches!(
            err,
            UnindexedOrderError::Rejected(ApiError::InstrumentInvalid(_, _))
        ));
    }

    #[test]
    fn test_map_order_error_rejected() {
        let instrument = InstrumentNameExchange::from("BTC-USD-PERP");

        let err = map_order_error(make_sdk_error("insufficient margin"), &instrument);
        assert!(matches!(
            err,
            UnindexedOrderError::Rejected(ApiError::OrderRejected(_))
        ));

        let err = map_order_error(make_sdk_error("price precision too high"), &instrument);
        assert!(matches!(
            err,
            UnindexedOrderError::Rejected(ApiError::OrderRejected(_))
        ));
    }
}
