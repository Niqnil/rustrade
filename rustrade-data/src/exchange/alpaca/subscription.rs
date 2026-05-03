use rustrade_integration::{Validator, error::SocketError};
use serde::{Deserialize, Deserializer, Serialize};
use smol_str::SmolStr;

/// Alpaca WebSocket subscription response wrapper.
///
/// Alpaca sends all messages as JSON arrays: `[{"T":"subscription",...}]`.
/// This wrapper deserializes the array and validates each element.
#[derive(Clone, PartialEq, Debug, Serialize)]
pub struct AlpacaSubResponse(pub Vec<AlpacaSubResponseInner>);

impl<'de> Deserialize<'de> for AlpacaSubResponse {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Vec::<AlpacaSubResponseInner>::deserialize(deserializer).map(AlpacaSubResponse)
    }
}

impl Validator for AlpacaSubResponse {
    type Error = SocketError;

    fn validate(self) -> Result<Self, SocketError> {
        for inner in &self.0 {
            if let AlpacaSubResponseInner::Error { code, msg } = inner {
                return Err(SocketError::Subscribe(format!(
                    "Alpaca subscription error (code {:?}): {}",
                    code, msg
                )));
            }
        }
        Ok(self)
    }
}

/// Individual Alpaca WebSocket message.
///
/// ### Raw Payload Examples
///
/// #### Subscription Success
/// ```json
/// [{"T":"subscription","trades":["AAPL","SPY"],"quotes":["AAPL"]}]
/// ```
///
/// #### Error
/// ```json
/// [{"T":"error","code":400,"msg":"invalid syntax"}]
/// ```
#[derive(Clone, PartialEq, Debug, Deserialize, Serialize)]
#[serde(tag = "T", rename_all = "lowercase")]
pub enum AlpacaSubResponseInner {
    /// Successful subscription confirmation.
    Subscription {
        #[serde(default)]
        trades: Vec<SmolStr>,
        #[serde(default)]
        quotes: Vec<SmolStr>,
        #[serde(default)]
        bars: Vec<SmolStr>,
    },
    /// Error response.
    Error {
        #[serde(default)]
        code: Option<i32>,
        msg: SmolStr,
    },
    /// Success message (auth confirmation, handled separately).
    Success { msg: SmolStr },
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;

    #[test]
    fn test_de_subscription_success() {
        let input = r#"[{"T":"subscription","trades":["AAPL","SPY"],"quotes":["AAPL"]}]"#;
        let response: AlpacaSubResponse = serde_json::from_str(input).unwrap();

        match &response.0[0] {
            AlpacaSubResponseInner::Subscription { trades, quotes, .. } => {
                assert_eq!(trades, &vec!["AAPL", "SPY"]);
                assert_eq!(quotes, &vec!["AAPL"]);
            }
            _ => panic!("expected Subscription variant"),
        }
    }

    #[test]
    fn test_de_subscription_trades_only() {
        let input = r#"[{"T":"subscription","trades":["BTC/USD"]}]"#;
        let response: AlpacaSubResponse = serde_json::from_str(input).unwrap();

        match &response.0[0] {
            AlpacaSubResponseInner::Subscription { trades, quotes, .. } => {
                assert_eq!(trades, &vec!["BTC/USD"]);
                assert!(quotes.is_empty());
            }
            _ => panic!("expected Subscription variant"),
        }
    }

    #[test]
    fn test_de_error() {
        let input = r#"[{"T":"error","code":400,"msg":"invalid syntax"}]"#;
        let response: AlpacaSubResponse = serde_json::from_str(input).unwrap();

        match &response.0[0] {
            AlpacaSubResponseInner::Error { code, msg } => {
                assert_eq!(*code, Some(400));
                assert_eq!(msg, "invalid syntax");
            }
            _ => panic!("expected Error variant"),
        }
    }

    #[test]
    fn test_de_success() {
        let input = r#"[{"T":"success","msg":"authenticated"}]"#;
        let response: AlpacaSubResponse = serde_json::from_str(input).unwrap();

        match &response.0[0] {
            AlpacaSubResponseInner::Success { msg } => {
                assert_eq!(msg, "authenticated");
            }
            _ => panic!("expected Success variant"),
        }
    }

    #[test]
    fn test_validate_subscription() {
        let response = AlpacaSubResponse(vec![AlpacaSubResponseInner::Subscription {
            trades: vec!["AAPL".into()],
            quotes: vec![],
            bars: vec![],
        }]);
        assert!(response.validate().is_ok());
    }

    #[test]
    fn test_validate_error() {
        let response = AlpacaSubResponse(vec![AlpacaSubResponseInner::Error {
            code: Some(400),
            msg: "bad request".into(),
        }]);
        assert!(response.validate().is_err());
    }

    #[test]
    fn test_validate_success_passes() {
        // Success messages are treated as benign during subscription validation
        // (they may arrive if auth confirmation is re-sent)
        let response = AlpacaSubResponse(vec![AlpacaSubResponseInner::Success {
            msg: "authenticated".into(),
        }]);
        assert!(response.validate().is_ok());
    }
}
