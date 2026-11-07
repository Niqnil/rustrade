//! Errors from the ThetaData integration.

/// Byte cap on a Terminal error body or a row snippet carried in an error.
///
/// The Terminal answers an error with a plain-text body, which is usually one short sentence; the
/// cap only bounds a pathological one so it cannot flood a log line.
pub(crate) const MESSAGE_BYTES: usize = 512;

/// A failed ThetaData request.
///
/// The client does not retry. [`Self::is_transient`] marks the errors a retry may cure, and the
/// caller decides whether and when to retry.
///
/// HTTP 472 ("no data for this request") is not an error: the request's stream ends without rows.
///
/// `#[non_exhaustive]`: variants may be added without a major-version bump.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum ThetaDataError {
    /// The Theta Terminal could not be reached: nothing listens at the base URL (the Terminal is
    /// not running, or runs on another host or port), or the connection timed out.
    #[error("Theta Terminal unreachable at {base_url}: {message}")]
    TerminalUnreachable { base_url: String, message: String },

    /// The account's subscription does not include this endpoint or interval (HTTP 403 or 471).
    ///
    /// A request for dates before the plan's history starts is answered differently, with HTTP
    /// 400: it arrives as [`Self::InvalidRequest`], with ThetaData's message naming the plan.
    #[error("ThetaData subscription does not permit this request (HTTP {status}): {message}")]
    PermissionDenied { status: u16, message: String },

    /// The Terminal rejected the request (HTTP 400, 404, 473 or 475): malformed, or, with HTTP
    /// 400, for dates before the account's plan starts its history. An outdated Terminal can also
    /// answer this way to a request it does not know.
    #[error("ThetaData rejected the request (HTTP {status}): {message}")]
    InvalidRequest { status: u16, message: String },

    /// The Terminal lost its connection to ThetaData's servers (HTTP 474). Transient.
    #[error("Theta Terminal disconnected from ThetaData (HTTP 474): {message}")]
    Disconnected { message: String },

    /// The Terminal's session is no longer valid (HTTP 478), typically because another Terminal or
    /// another client logged in to the same account. Restarting this Terminal, alone on the
    /// account, is the cure; a retry is not.
    #[error("Theta Terminal session invalid (HTTP 478): {message}")]
    SessionInvalid { message: String },

    /// Too many requests are queued (HTTP 429): the Terminal queues requests beyond the account's
    /// concurrency limit up to its queue size, then refuses. Transient.
    #[error("ThetaData request queue full (HTTP 429): {message}")]
    Throttled { message: String },

    /// ThetaData's server is restarting (HTTP 571). Transient.
    #[error("ThetaData server restarting (HTTP 571): {message}")]
    ServerRestarting { message: String },

    /// Any other error status, including 470 (general), 476 (requests from a different address
    /// than the first one; `localhost` and `127.0.0.1` count as different) and 572 (uncaught).
    #[error("ThetaData API error (HTTP {status}): {message}")]
    Api { status: u16, message: String },

    /// The request failed in transport after the connection was made, or reading the response
    /// failed midway. Transient.
    #[error("ThetaData HTTP error: {message}")]
    Http { message: String },

    /// A response row could not be decoded.
    #[error("ThetaData row could not be decoded ({message}): {row}")]
    Decode { message: String, row: String },

    /// The caller's arguments were rejected before any request was sent.
    #[error("invalid ThetaData request: {message}")]
    InvalidInput { message: String },
}

impl ThetaDataError {
    /// Whether retrying the same request later may succeed: the Terminal unreachable,
    /// disconnected from ThetaData, throttled, the server restarting, or a transport failure.
    ///
    /// A permission denial, an invalid request, an invalid session, a decode failure and invalid
    /// input are not: the same request fails the same way until something else changes.
    pub fn is_transient(&self) -> bool {
        match self {
            Self::TerminalUnreachable { .. }
            | Self::Disconnected { .. }
            | Self::Throttled { .. }
            | Self::ServerRestarting { .. }
            | Self::Http { .. } => true,
            Self::PermissionDenied { .. }
            | Self::InvalidRequest { .. }
            | Self::SessionInvalid { .. }
            | Self::Api { .. }
            | Self::Decode { .. }
            | Self::InvalidInput { .. } => false,
        }
    }

    /// The error for an error status and its body.
    pub(crate) fn from_status(status: u16, body: &str) -> Self {
        let message = crate::exchange::http::truncate_str(body.trim(), MESSAGE_BYTES);
        match status {
            403 | 471 => Self::PermissionDenied { status, message },
            400 | 404 | 473 | 475 => Self::InvalidRequest { status, message },
            474 => Self::Disconnected { message },
            478 => Self::SessionInvalid { message },
            429 => Self::Throttled { message },
            571 => Self::ServerRestarting { message },
            status => Self::Api { status, message },
        }
    }

    pub(crate) fn decode(message: impl std::fmt::Display, row: &str) -> Self {
        Self::Decode {
            message: message.to_string(),
            row: crate::exchange::http::truncate_str(row, MESSAGE_BYTES),
        }
    }

    pub(crate) fn invalid_input(message: impl Into<String>) -> Self {
        Self::InvalidInput {
            message: message.into(),
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;

    #[test]
    fn statuses_map_to_their_variants() {
        let cases: [(u16, fn(&ThetaDataError) -> bool); 14] = [
            (403, |e| {
                matches!(e, ThetaDataError::PermissionDenied { status: 403, .. })
            }),
            (471, |e| {
                matches!(e, ThetaDataError::PermissionDenied { status: 471, .. })
            }),
            (400, |e| {
                matches!(e, ThetaDataError::InvalidRequest { status: 400, .. })
            }),
            (404, |e| {
                matches!(e, ThetaDataError::InvalidRequest { status: 404, .. })
            }),
            (473, |e| {
                matches!(e, ThetaDataError::InvalidRequest { status: 473, .. })
            }),
            (475, |e| {
                matches!(e, ThetaDataError::InvalidRequest { status: 475, .. })
            }),
            (474, |e| matches!(e, ThetaDataError::Disconnected { .. })),
            (478, |e| matches!(e, ThetaDataError::SessionInvalid { .. })),
            (429, |e| matches!(e, ThetaDataError::Throttled { .. })),
            (571, |e| {
                matches!(e, ThetaDataError::ServerRestarting { .. })
            }),
            (470, |e| {
                matches!(e, ThetaDataError::Api { status: 470, .. })
            }),
            (476, |e| {
                matches!(e, ThetaDataError::Api { status: 476, .. })
            }),
            (572, |e| {
                matches!(e, ThetaDataError::Api { status: 572, .. })
            }),
            (500, |e| {
                matches!(e, ThetaDataError::Api { status: 500, .. })
            }),
        ];
        for (status, is_expected) in cases {
            let error = ThetaDataError::from_status(status, " body \n");
            assert!(is_expected(&error), "{status} -> {error:?}");
            assert!(
                error.to_string().contains("body"),
                "{status} keeps the body: {error}"
            );
        }
    }

    #[test]
    fn transient_errors_are_the_ones_a_retry_may_cure() {
        let transient = [
            ThetaDataError::TerminalUnreachable {
                base_url: String::new(),
                message: String::new(),
            },
            ThetaDataError::from_status(474, ""),
            ThetaDataError::from_status(429, ""),
            ThetaDataError::from_status(571, ""),
            ThetaDataError::Http {
                message: String::new(),
            },
        ];
        for error in transient {
            assert!(error.is_transient(), "{error:?}");
        }
        let permanent = [
            ThetaDataError::from_status(403, ""),
            ThetaDataError::from_status(473, ""),
            ThetaDataError::from_status(478, ""),
            ThetaDataError::from_status(470, ""),
            ThetaDataError::decode("bad", "{}"),
            ThetaDataError::invalid_input("bad"),
        ];
        for error in permanent {
            assert!(!error.is_transient(), "{error:?}");
        }
    }

    #[test]
    fn a_long_body_is_truncated() {
        let body = "x".repeat(MESSAGE_BYTES * 2);
        let ThetaDataError::Api { message, .. } = ThetaDataError::from_status(470, &body) else {
            panic!("expected Api");
        };
        assert!(message.len() <= MESSAGE_BYTES + 3, "{}", message.len());
    }
}
