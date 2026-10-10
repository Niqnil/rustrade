//! Error mapping for Hyperliquid SDK errors to rustrade-execution error types.

use crate::error::{
    ApiError, ConnectivityError, UnindexedApiError, UnindexedClientError, UnindexedOrderError,
};
use rustrade_instrument::instrument::name::InstrumentNameExchange;
use smol_str::SmolStr;

/// Why [`HyperliquidClient::connect`](super::HyperliquidClient::connect) failed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum HyperliquidConnectError {
    /// Hyperliquid could not be reached, or did not answer as it should. Transient: retry with
    /// backoff.
    #[error("{0}")]
    Connectivity(#[from] ConnectivityError),

    /// A DEX in [`HyperliquidConfig::dexes`](super::HyperliquidConfig::dexes) is not one
    /// Hyperliquid lists on the configured network. Retrying does not help: fix the config.
    #[error("Hyperliquid lists no perpetual DEX named {0:?} on this network")]
    UnknownDex(SmolStr),

    /// Hyperliquid's description of a configured DEX could not be read or makes no sense, such
    /// as a response that does not parse or a collateral token it does not list. Not transient:
    /// a retry is unlikely to read anything different.
    #[error("Hyperliquid perpetual DEX metadata: {0}")]
    Metadata(String),
}

impl From<UnindexedClientError> for HyperliquidConnectError {
    /// A connectivity error stays one, and a rate limit becomes one, since both pass; anything
    /// else means the response could not be used.
    fn from(error: UnindexedClientError) -> Self {
        match error {
            UnindexedClientError::Connectivity(error) => Self::Connectivity(error),
            UnindexedClientError::Api(UnindexedApiError::RateLimit) => {
                Self::Connectivity(ConnectivityError::Socket(error.to_string()))
            }
            other => Self::Metadata(other.to_string()),
        }
    }
}

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

/// Maps an SDK error from placing or cancelling an order to `UnindexedOrderError`.
///
/// An unknown instrument is [`ApiError::InstrumentInvalid`]; anything else is classified by its
/// text, as a refusal in a placement response is, so a shortfall of funds is
/// [`ApiError::BalanceInsufficient`].
/// A cancel never fails for want of funds, so it shares the classifier harmlessly.
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

    // Anything else is classified by its text like a refusal in a response: a shortfall of
    // funds is BalanceInsufficient, the rest OrderRejected.
    UnindexedOrderError::Rejected(order_rejection(msg))
}

/// Classify why Hyperliquid refused an order: the error of a placement response or of the SDK
/// call that sent it, or a status ending in `Rejected` from `orderUpdates` or `orderStatus`.
///
/// The account's funds running short is [`ApiError::BalanceInsufficient`], with no asset, since
/// Hyperliquid does not name the one that ran short:
/// - a perp order's "Insufficient margin to place order." and the `perpMarginRejected` status;
/// - a spot order's "Order has insufficient spot balance to trade" and the
///   `insufficientSpotBalanceRejected` status.
///
/// Anything else (minimum notional, reduce-only, open-interest caps, oracle, margin-tier limit,
/// ...) is [`ApiError::OrderRejected`]. Either way the venue's text is kept.
pub(super) fn order_rejection(reason: String) -> UnindexedApiError {
    let lower = reason.to_ascii_lowercase();
    let short_of_funds = matches!(
        reason.as_str(),
        "perpMarginRejected" | "insufficientSpotBalanceRejected"
    ) || lower.contains("insufficient margin")
        || lower.contains("insufficient spot balance");
    if short_of_funds {
        ApiError::BalanceInsufficient(None, reason)
    } else {
        ApiError::OrderRejected(reason)
    }
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
        let instrument = InstrumentNameExchange::from("INVALID-USDC-PERP");

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
    fn a_shortfall_of_funds_is_balance_insufficient_and_anything_else_a_rejection() {
        for reason in [
            "perpMarginRejected",
            "insufficientSpotBalanceRejected",
            "Insufficient margin to place order. asset=0",
            "Order has insufficient spot balance to trade",
            "INSUFFICIENT MARGIN TO PLACE ORDER.",
        ] {
            assert_eq!(
                order_rejection(reason.to_owned()),
                ApiError::BalanceInsufficient(None, reason.to_owned()),
                "{reason}"
            );
        }
        for reason in [
            "minTradeNtlRejected",
            "reduceOnlyRejected",
            "positionIncreaseAtOpenInterestCapRejected",
            "oracleRejected",
            "perpMaxPositionRejected",
            "marketOrderNoLiquidityRejected",
            "badAloPxRejected",
            "Order must have minimum value of $10.",
        ] {
            assert_eq!(
                order_rejection(reason.to_owned()),
                ApiError::OrderRejected(reason.to_owned()),
                "{reason}"
            );
        }
    }

    #[test]
    fn test_map_order_error_rejected() {
        let instrument = InstrumentNameExchange::from("BTC-USDC-PERP");

        let err = map_order_error(make_sdk_error("insufficient margin"), &instrument);
        assert!(matches!(
            err,
            UnindexedOrderError::Rejected(ApiError::BalanceInsufficient(None, _))
        ));

        let err = map_order_error(make_sdk_error("price precision too high"), &instrument);
        assert!(matches!(
            err,
            UnindexedOrderError::Rejected(ApiError::OrderRejected(_))
        ));
    }

    #[test]
    fn a_connect_error_is_transient_only_when_the_request_was() {
        let socket = ConnectivityError::Socket("reset".to_owned());
        assert_eq!(
            HyperliquidConnectError::from(UnindexedClientError::Connectivity(socket.clone())),
            HyperliquidConnectError::Connectivity(socket)
        );
        assert!(matches!(
            HyperliquidConnectError::from(UnindexedClientError::Api(UnindexedApiError::RateLimit)),
            HyperliquidConnectError::Connectivity(ConnectivityError::Socket(_))
        ));
        assert!(matches!(
            HyperliquidConnectError::from(UnindexedClientError::Internal("bad json".to_owned())),
            HyperliquidConnectError::Metadata(reason) if reason.contains("bad json")
        ));
    }
}
