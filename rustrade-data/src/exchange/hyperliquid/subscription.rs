use rustrade_integration::{Validator, error::SocketError};
use serde::{Deserialize, Serialize};

/// Hyperliquid WebSocket subscription response.
///
/// ### Raw Payload Examples
///
/// #### Subscription Success
/// ```json
/// {
///     "channel": "subscriptionResponse",
///     "data": {
///         "method": "subscribe",
///         "subscription": {"type": "trades", "coin": "BTC"}
///     }
/// }
/// ```
///
/// #### Pong Response (out of sequence during subscription validation)
/// ```json
/// {"channel": "pong"}
/// ```
#[derive(Clone, Eq, PartialEq, Debug, Deserialize, Serialize)]
#[serde(tag = "channel", rename_all = "camelCase")]
pub enum HyperliquidSubResponse {
    /// Successful subscription confirmation.
    SubscriptionResponse { data: HyperliquidSubResponseData },
    /// Pong response to ping; not counted as a subscription confirmation.
    Pong,
    /// Error response.
    Error { data: String },
}

/// Data payload for subscription response.
#[derive(Clone, Eq, PartialEq, Debug, Deserialize, Serialize)]
pub struct HyperliquidSubResponseData {
    pub method: String,
    pub subscription: serde_json::Value,
}

impl Validator for HyperliquidSubResponse {
    type Error = SocketError;

    fn validate(self) -> Result<Self, SocketError>
    where
        Self: Sized,
    {
        match &self {
            HyperliquidSubResponse::SubscriptionResponse { .. } => Ok(self),
            // Pong is not a subscription confirmation; matches Bybit's pattern of returning
            // Err so the validator does not increment its success counter for keepalive replies.
            HyperliquidSubResponse::Pong => Err(SocketError::Subscribe(
                "received pong out of sequence during subscription validation".to_owned(),
            )),
            HyperliquidSubResponse::Error { data } => Err(SocketError::Subscribe(format!(
                "received failure subscription response: {data}",
            ))),
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;

    mod de {
        use super::*;

        #[test]
        fn test_hyperliquid_sub_response_success() {
            let input = r#"
            {
                "channel": "subscriptionResponse",
                "data": {
                    "method": "subscribe",
                    "subscription": {"type": "trades", "coin": "BTC"}
                }
            }
            "#;

            let response: HyperliquidSubResponse = serde_json::from_str(input).unwrap();
            assert!(matches!(
                response,
                HyperliquidSubResponse::SubscriptionResponse { .. }
            ));
            assert!(response.validate().is_ok());
        }

        #[test]
        fn test_hyperliquid_sub_response_pong() {
            let input = r#"{"channel": "pong"}"#;

            let response: HyperliquidSubResponse = serde_json::from_str(input).unwrap();
            assert!(matches!(response, HyperliquidSubResponse::Pong));
            assert!(response.validate().is_err());
        }

        #[test]
        fn test_hyperliquid_sub_response_error() {
            let input = r#"{"channel": "error", "data": "invalid subscription"}"#;

            let response: HyperliquidSubResponse = serde_json::from_str(input).unwrap();
            assert!(matches!(response, HyperliquidSubResponse::Error { .. }));
            assert!(response.validate().is_err());
        }
    }
}
