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
            UnindexedClientError::Api(UnindexedApiError::RateLimit { .. }) => {
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
///
/// A rate limit carries no `retry_after`: the SDK keeps no response headers, and Hyperliquid
/// documents no wait.
pub fn map_sdk_error(error: hyperliquid_rust_sdk::Error) -> UnindexedClientError {
    // An HTTP 429 is a rate limit whatever its body says.
    if let hyperliquid_rust_sdk::Error::ClientRequest {
        status_code: 429, ..
    } = error
    {
        return UnindexedClientError::Api(UnindexedApiError::rate_limited(None));
    }

    let msg = error.to_string();
    let msg_lower = msg.to_lowercase();

    // Check for rate limiting first (transient). Matched on the words, not a bare "429", which
    // an order id or a price in the message can hold.
    if msg_lower.contains("rate limit") || msg_lower.contains("too many requests") {
        return UnindexedClientError::Api(UnindexedApiError::rate_limited(None));
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

/// Maps an SDK error from placing or cancelling an order to `UnindexedOrderError`, by whether
/// Hyperliquid may have acted on the request.
///
/// The SDK reports whatever goes wrong once the request is signed as one variant,
/// [`Error::JsonParse`](hyperliquid_rust_sdk::Error::JsonParse), holding the underlying error's
/// text: a failure to send the request or to read the answer, an HTTP 4xx or 5xx, and an answer
/// that does not parse. So its text decides:
/// - **An HTTP 4xx is a refusal.** The request was turned away: Hyperliquid answers an
///   `/exchange` request it acts on with 200, refusals included. This rests on the venue's
///   behaviour; a proxy in between that forwarded the request and then answered 4xx would be
///   misread. A 429 is [`ApiError::RateLimit`]; any other 4xx is classified as below.
/// - **Anything else in `JsonParse` is [`OrderError::Connectivity`]**: the request may have
///   reached the venue, so its outcome is unknown, and the engine looks an open up by its client
///   order id. `JsonParse` also covers encoding and building the request before the send, which
///   the requests this client builds do not fail; if one did, the lookups would find the order
///   never placed.
///
/// Every other variant fails before the request is sent (an asset the SDK does not list,
/// signing, a number it cannot encode, ...), so it is a refusal too.
///
/// A refusal naming an unknown instrument is [`ApiError::InstrumentInvalid`]. Any other is
/// classified by its text, as a refusal in a placement response is, so a shortfall of funds is
/// [`ApiError::BalanceInsufficient`]. A cancel never fails for want of funds, so it shares the
/// classifier harmlessly.
///
/// [`OrderError::Connectivity`]: crate::error::OrderError::Connectivity
pub fn map_order_error(
    error: hyperliquid_rust_sdk::Error,
    instrument: &InstrumentNameExchange,
) -> UnindexedOrderError {
    let hyperliquid_rust_sdk::Error::JsonParse(text) = error else {
        return refusal(error.to_string(), instrument);
    };
    match client_error_status(&text) {
        Some(429) => UnindexedOrderError::Rejected(ApiError::rate_limited(None)),
        Some(_) => refusal(text, instrument),
        None => UnindexedOrderError::Connectivity(ConnectivityError::Socket(text)),
    }
}

/// What the SDK's text for an HTTP 4xx answer (`Error::ClientRequest`'s `Display`) starts with,
/// followed by the status code and a comma.
///
/// The SDK's text is the contract here: if it changed, a 4xx would read as an unknown outcome.
/// The tests build their input from the SDK's own `Display`, so such a change fails them.
const CLIENT_ERROR_PREFIX: &str = "Client error: status code: ";

/// The HTTP status of a 4xx answer, read from the text the SDK's `/exchange` post wraps in
/// `JsonParse`. `None` for any other text.
fn client_error_status(text: &str) -> Option<u16> {
    let (status, _) = text.strip_prefix(CLIENT_ERROR_PREFIX)?.split_once(',')?;
    status
        .parse()
        .ok()
        .filter(|status| (400..500).contains(status))
}

/// A refusal of an order request, classified by its text `msg`.
fn refusal(msg: String, instrument: &InstrumentNameExchange) -> UnindexedOrderError {
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

/// An order answered `ok` with a bare `success` status or with none: Hyperliquid took the
/// request but did not say what became of the order, which may be live. Its outcome is unknown,
/// as [`map_order_error`] reads a failure after the send.
pub(super) fn order_outcome_unanswered() -> UnindexedOrderError {
    UnindexedOrderError::Connectivity(ConnectivityError::Socket(
        "Hyperliquid answered the order without its status".to_owned(),
    ))
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
        let rate_limited = UnindexedClientError::Api(UnindexedApiError::rate_limited(None));

        let err = map_sdk_error(make_sdk_error("rate limit exceeded"));
        assert_eq!(err, rate_limited);

        let err = map_sdk_error(make_sdk_error("too many requests"));
        assert_eq!(err, rate_limited);

        let err = map_sdk_error(Error::ClientRequest {
            status_code: 429,
            error_code: None,
            error_message: "slow down".to_string(),
            error_data: None,
        });
        assert_eq!(err, rate_limited);
    }

    #[test]
    fn a_message_merely_holding_429_is_not_a_rate_limit() {
        let err = map_sdk_error(Error::ClientRequest {
            status_code: 400,
            error_code: None,
            error_message: "Order 74291 has invalid price 4290".to_string(),
            error_data: None,
        });
        assert!(
            !matches!(
                err,
                UnindexedClientError::Api(UnindexedApiError::RateLimit { .. })
            ),
            "{err:?}"
        );
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

    /// The `/exchange` post wraps the text of what failed in `JsonParse`, as the SDK writes it.
    fn after_the_send(error: Error) -> Error {
        Error::JsonParse(error.to_string())
    }

    fn client_error(status_code: u16, error_message: &str) -> Error {
        after_the_send(Error::ClientRequest {
            status_code,
            error_code: None,
            error_message: error_message.to_owned(),
            error_data: None,
        })
    }

    /// An HTTP 4xx was refused before Hyperliquid acted on it, so it ends the order, classified
    /// as any refusal is. Read from the SDK's own text for one, so a change to it fails here.
    #[test]
    fn a_4xx_is_a_refusal_and_a_429_a_rate_limit() {
        let instrument = InstrumentNameExchange::from("BTC-USDC-PERP");
        for status in [400, 422, 499] {
            assert!(
                matches!(
                    map_order_error(client_error(status, "Failed to deserialize"), &instrument),
                    UnindexedOrderError::Rejected(ApiError::OrderRejected(reason))
                        if reason.starts_with(&format!("{CLIENT_ERROR_PREFIX}{status},"))
                ),
                "{status}"
            );
        }
        assert!(matches!(
            map_order_error(client_error(400, "Insufficient margin"), &instrument),
            UnindexedOrderError::Rejected(ApiError::BalanceInsufficient(None, _))
        ));
        assert_eq!(
            map_order_error(client_error(429, "too many"), &instrument),
            UnindexedOrderError::Rejected(ApiError::rate_limited(None))
        );
    }

    /// Anything after the send other than a 4xx may have reached the venue: a 5xx, a failure to
    /// send or to read, an answer that does not parse, and text that only looks like a 4xx.
    #[test]
    fn any_other_failure_after_the_send_has_an_unknown_outcome() {
        let instrument = InstrumentNameExchange::from("BTC-USDC-PERP");
        for text in [
            Error::ServerRequest {
                status_code: 502,
                error_message: "Bad Gateway".to_owned(),
            }
            .to_string(),
            Error::GenericRequest(
                "error sending request for url (https://api.hyperliquid.xyz/exchange)".to_owned(),
            )
            .to_string(),
            "expected value at line 1 column 1".to_owned(),
            format!("{CLIENT_ERROR_PREFIX}500, error code: None"),
            format!("{CLIENT_ERROR_PREFIX}abc, error code: None"),
            format!("{CLIENT_ERROR_PREFIX}399, error code: None"),
            format!("{CLIENT_ERROR_PREFIX}422 without a comma"),
        ] {
            let mapped = map_order_error(Error::JsonParse(text.clone()), &instrument);
            assert_eq!(
                mapped,
                UnindexedOrderError::Connectivity(ConnectivityError::Socket(text.clone())),
                "{text}"
            );
            assert!(mapped.is_outcome_unknown(), "{text}");
        }
    }

    /// Every variant other than `JsonParse` comes from before the send, so it ends the order.
    #[test]
    fn a_failure_before_the_send_is_a_refusal() {
        let instrument = InstrumentNameExchange::from("BTC-USDC-PERP");
        assert!(matches!(
            map_order_error(Error::AssetNotFound, &instrument),
            UnindexedOrderError::Rejected(ApiError::InstrumentInvalid(name, _)) if name == instrument
        ));
        for error in [
            Error::Eip712("bad domain".to_owned()),
            Error::SignatureFailure("bad key".to_owned()),
            Error::FloatStringParse,
            Error::NoCloid,
        ] {
            assert!(
                matches!(
                    map_order_error(error.clone(), &instrument),
                    UnindexedOrderError::Rejected(ApiError::OrderRejected(_))
                ),
                "{error}"
            );
        }
    }

    #[test]
    fn an_order_answered_without_its_status_has_an_unknown_outcome() {
        assert!(order_outcome_unanswered().is_outcome_unknown());
    }

    #[test]
    fn a_connect_error_is_transient_only_when_the_request_was() {
        let socket = ConnectivityError::Socket("reset".to_owned());
        assert_eq!(
            HyperliquidConnectError::from(UnindexedClientError::Connectivity(socket.clone())),
            HyperliquidConnectError::Connectivity(socket)
        );
        assert!(matches!(
            HyperliquidConnectError::from(UnindexedClientError::Api(
                UnindexedApiError::rate_limited(None)
            )),
            HyperliquidConnectError::Connectivity(ConnectivityError::Socket(_))
        ));
        assert!(matches!(
            HyperliquidConnectError::from(UnindexedClientError::Internal("bad json".to_owned())),
            HyperliquidConnectError::Metadata(reason) if reason.contains("bad json")
        ));
    }
}
