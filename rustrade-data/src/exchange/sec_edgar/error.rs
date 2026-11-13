//! Errors from the SEC EDGAR integration.

use super::Cik;

/// Byte cap on a response body or a decode snippet carried in an error. SEC answers an error with
/// an HTML page; the cap keeps it from flooding a log line.
pub(crate) const MESSAGE_BYTES: usize = 512;

/// A failed SEC EDGAR request.
///
/// The client does not retry. [`Self::is_transient`] marks the errors a retry may cure, and the
/// caller decides whether and when to retry.
///
/// `#[non_exhaustive]`: variants may be added without a major-version bump.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum SecEdgarError {
    /// SEC refused the request (HTTP 403). SEC answers this way both to a request whose
    /// User-Agent does not declare who sends it and to a client over its request rate, with the
    /// same page ("Request Rate Threshold Exceeded"), so the cause cannot be told apart here. A
    /// block for the rate lifts after a while; a User-Agent SEC does not accept is refused every
    /// time.
    #[error("SEC refused the request (HTTP 403): {message}")]
    Refused { message: String },

    /// Too many requests (HTTP 429). Transient.
    #[error("SEC throttled the request (HTTP 429): {message}")]
    Throttled { message: String },

    /// No such company or filing (HTTP 404): for a company, a CIK that SEC does not know.
    #[error("SEC has no such resource (HTTP 404): {url}")]
    NotFound { url: String },

    /// Any other error status. The server errors 500, 502, 503 and 504 are transient.
    #[error("SEC API error (HTTP {status}): {message}")]
    Api { status: u16, message: String },

    /// The request failed in transport (connection, timeout, or a response cut off midway).
    /// Transient.
    #[error("SEC EDGAR HTTP error: {message}")]
    Http { message: String },

    /// A response could not be decoded: SEC changed a format this client reads.
    #[error("SEC EDGAR response could not be decoded: {message}")]
    Decode { message: String },

    /// The caller's arguments were rejected before any request was sent.
    #[error("invalid SEC EDGAR request: {message}")]
    InvalidInput { message: String },

    /// The company files the annual and current reports of a foreign private issuer (20-F, 40-F,
    /// 6-K) rather than 10-Q, 10-K and 8-K. Those reports carry no item numbers and no quarterly
    /// reports to pair a release with, so its earnings releases cannot be read from EDGAR.
    #[error("CIK {cik} files foreign private issuer reports (20-F, 40-F, 6-K), not 10-Q/10-K/8-K")]
    ForeignPrivateIssuer { cik: Cik },
}

impl SecEdgarError {
    /// Whether retrying the same request later may succeed: throttled, a transport failure, or a
    /// server error (HTTP 500, 502, 503 or 504).
    ///
    /// A refusal is not: SEC gives the same answer to a User-Agent it does not accept, which no
    /// retry cures, as to a client over the rate, whose block lifts only after a pause far longer
    /// than a retry loop's.
    pub fn is_transient(&self) -> bool {
        match self {
            Self::Throttled { .. } | Self::Http { .. } => true,
            Self::Api { status, .. } => matches!(status, 500 | 502..=504),
            Self::Refused { .. }
            | Self::NotFound { .. }
            | Self::Decode { .. }
            | Self::InvalidInput { .. }
            | Self::ForeignPrivateIssuer { .. } => false,
        }
    }

    /// The error for an error status, the requested URL and the response body.
    pub(crate) fn from_status(status: u16, url: &str, body: &str) -> Self {
        let message = crate::exchange::http::truncate_str(body.trim(), MESSAGE_BYTES);
        match status {
            403 => Self::Refused { message },
            429 => Self::Throttled { message },
            404 => Self::NotFound {
                url: url.to_owned(),
            },
            status => Self::Api { status, message },
        }
    }

    pub(crate) fn decode(message: impl Into<String>) -> Self {
        Self::Decode {
            message: crate::exchange::http::truncate_str(&message.into(), MESSAGE_BYTES),
        }
    }

    pub(crate) fn invalid_input(message: impl Into<String>) -> Self {
        Self::InvalidInput {
            message: message.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn statuses_map_to_their_variants_and_transience() {
        let url = "https://data.sec.gov/submissions/CIK0000000001.json";
        let cases: [(u16, SecEdgarError, bool); 7] = [
            (
                403,
                SecEdgarError::Refused {
                    message: "page".into(),
                },
                false,
            ),
            (
                429,
                SecEdgarError::Throttled {
                    message: "page".into(),
                },
                true,
            ),
            (404, SecEdgarError::NotFound { url: url.into() }, false),
            (
                500,
                SecEdgarError::Api {
                    status: 500,
                    message: "page".into(),
                },
                true,
            ),
            (
                503,
                SecEdgarError::Api {
                    status: 503,
                    message: "page".into(),
                },
                true,
            ),
            (
                501,
                SecEdgarError::Api {
                    status: 501,
                    message: "page".into(),
                },
                false,
            ),
            (
                400,
                SecEdgarError::Api {
                    status: 400,
                    message: "page".into(),
                },
                false,
            ),
        ];
        for (status, expected, transient) in cases {
            let error = SecEdgarError::from_status(status, url, "  page\n");
            assert_eq!(error, expected, "status {status}");
            assert_eq!(error.is_transient(), transient, "status {status}");
        }
    }

    #[test]
    fn a_long_body_is_capped() {
        let body = "x".repeat(MESSAGE_BYTES * 2);
        let SecEdgarError::Refused { message } = SecEdgarError::from_status(403, "u", &body) else {
            panic!("expected Refused");
        };
        assert_eq!(message.len(), MESSAGE_BYTES);
    }

    #[test]
    fn the_rest_are_not_transient_except_transport() {
        assert!(
            SecEdgarError::Http {
                message: "reset".into()
            }
            .is_transient()
        );
        assert!(!SecEdgarError::decode("bad").is_transient());
        assert!(!SecEdgarError::invalid_input("bad").is_transient());
        assert!(!SecEdgarError::ForeignPrivateIssuer { cik: Cik::new(1) }.is_transient());
    }
}
