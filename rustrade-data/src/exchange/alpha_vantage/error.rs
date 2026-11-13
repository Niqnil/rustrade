//! Errors from the Alpha Vantage integration.

/// Byte cap on a message carried in an error, so a long notice or page cannot flood a log line.
pub(crate) const MESSAGE_BYTES: usize = 512;

/// A failed Alpha Vantage request.
///
/// Alpha Vantage answers most failures with HTTP 200 and a notice in place of the data; the client
/// reads those notices into the variants below. No error carries the API key: the client removes
/// it from every message and URL.
///
/// The client does not retry. [`Self::is_transient`] marks the errors a retry may cure, and the
/// caller decides whether and when to retry.
///
/// `#[non_exhaustive]`: variants may be added without a major-version bump.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum AlphaVantageError {
    /// Alpha Vantage refused the request with a notice in place of the data: the key's request
    /// rate (one a second on a free key) or its daily allowance (25 a day on a free key) is used
    /// up, or the request needs a premium plan. The notice, when Alpha Vantage sends it whole,
    /// says which. Not transient: a daily allowance lifts only the next day, and the client's
    /// pace keeps it under the per-second rate unless other programs share the key.
    #[error("Alpha Vantage refused the request: {message}")]
    Refused { message: String },

    /// Alpha Vantage reports no earnings for the symbol: it does not know the symbol, or the
    /// symbol has no earnings (a fund, for example). Alpha Vantage gives the same empty answer
    /// for both.
    #[error("Alpha Vantage has no earnings for {symbol:?}")]
    NotFound { symbol: String },

    /// An error status, or HTTP 200 with Alpha Vantage's error message in place of the data
    /// (`status` 200), such as for an API key it does not accept. The server errors 500, 502,
    /// 503 and 504, and 429, are transient.
    #[error("Alpha Vantage API error (HTTP {status}): {message}")]
    Api { status: u16, message: String },

    /// The request failed in transport (connection, timeout, or a response cut off midway).
    /// Transient.
    #[error("Alpha Vantage HTTP error: {message}")]
    Http { message: String },

    /// A response could not be decoded: Alpha Vantage changed a format this client reads.
    #[error("Alpha Vantage response could not be decoded: {message}")]
    Decode { message: String },

    /// The caller's arguments were rejected before any request was sent.
    #[error("invalid Alpha Vantage request: {message}")]
    InvalidInput { message: String },
}

impl AlphaVantageError {
    /// Whether retrying the same request later may succeed: a transport failure, or an error
    /// status of 429, 500, 502, 503 or 504.
    pub fn is_transient(&self) -> bool {
        match self {
            Self::Http { .. } => true,
            Self::Api { status, .. } => matches!(status, 429 | 500 | 502..=504),
            Self::Refused { .. }
            | Self::NotFound { .. }
            | Self::Decode { .. }
            | Self::InvalidInput { .. } => false,
        }
    }

    pub(crate) fn decode(message: impl Into<String>) -> Self {
        Self::Decode {
            message: cap(&message.into()),
        }
    }

    pub(crate) fn invalid_input(message: impl Into<String>) -> Self {
        Self::InvalidInput {
            message: message.into(),
        }
    }
}

/// `text` trimmed and capped at [`MESSAGE_BYTES`].
pub(crate) fn cap(text: &str) -> String {
    crate::exchange::http::truncate_str(text.trim(), MESSAGE_BYTES)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_transport_and_server_or_rate_statuses_are_transient() {
        let api = |status| AlphaVantageError::Api {
            status,
            message: String::new(),
        };
        for status in [429, 500, 502, 503, 504] {
            assert!(api(status).is_transient(), "{status}");
        }
        for status in [200, 400, 401, 403, 404, 501] {
            assert!(!api(status).is_transient(), "{status}");
        }
        assert!(
            AlphaVantageError::Http {
                message: String::new()
            }
            .is_transient()
        );
        for error in [
            AlphaVantageError::Refused {
                message: String::new(),
            },
            AlphaVantageError::NotFound {
                symbol: String::new(),
            },
            AlphaVantageError::decode("x"),
            AlphaVantageError::invalid_input("x"),
        ] {
            assert!(!error.is_transient(), "{error:?}");
        }
    }

    #[test]
    fn a_long_message_is_capped() {
        let long = "é".repeat(MESSAGE_BYTES);
        let AlphaVantageError::Decode { message } = AlphaVantageError::decode(long) else {
            unreachable!()
        };
        assert!(message.len() <= MESSAGE_BYTES);
        assert!(message.starts_with('é'));
    }
}
