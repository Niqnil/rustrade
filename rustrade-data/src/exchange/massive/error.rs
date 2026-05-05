//! Error types for Massive integration.

use crate::error::DataError;
use std::time::Duration;

/// Massive-specific errors.
///
/// The library returns these errors without automatic retry or reconnection.
/// Consumers decide how to handle rate limits, disconnections, and auth failures.
#[derive(Debug, Clone, PartialEq)]
pub enum MassiveError {
    /// Rate limited by the API. Contains optional retry-after duration.
    ///
    /// Returned on HTTP 429 responses. The consumer decides whether and when to retry.
    RateLimited { retry_after: Option<Duration> },

    /// WebSocket connection dropped or ping timeout exceeded.
    ///
    /// Returned when:
    /// - WebSocket connection closes unexpectedly
    /// - Pong not received within 19 seconds of ping
    ///
    /// The consumer owns reconnection policy (backoff, credential refresh, dedup).
    Disconnected { reason: String },

    /// Authentication failed.
    ///
    /// Returned when:
    /// - API key is invalid or expired
    /// - API key lacks permission for the requested resource
    Auth { message: String },

    /// API returned an error response.
    ///
    /// Covers non-auth, non-rate-limit API errors (invalid parameters, not found, etc.)
    Api { status: u16, message: String },

    /// Network or HTTP client error.
    Http { message: String },

    /// JSON deserialization failed.
    Deserialize { message: String, payload: String },

    /// Environment variable not set.
    EnvVar { var: &'static str },

    /// Client-side input validation failed.
    ///
    /// Returned when input parameters are invalid before making an API request
    /// (e.g., timestamp out of representable range).
    InvalidInput { message: String },
}

impl std::fmt::Display for MassiveError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MassiveError::RateLimited { retry_after } => {
                write!(f, "Massive rate limited")?;
                if let Some(duration) = retry_after {
                    write!(f, " (retry after {:?})", duration)?;
                }
                Ok(())
            }
            MassiveError::Disconnected { reason } => {
                write!(f, "Massive disconnected: {}", reason)
            }
            MassiveError::Auth { message } => {
                write!(f, "Massive auth failed: {}", message)
            }
            MassiveError::Api { status, message } => {
                write!(f, "Massive API error ({}): {}", status, message)
            }
            MassiveError::Http { message } => {
                write!(f, "Massive HTTP error: {}", message)
            }
            MassiveError::Deserialize { message, payload } => {
                let boundary = payload.floor_char_boundary(100);
                let truncated = &payload[..boundary];
                let ellipsis = if boundary < payload.len() { "..." } else { "" };
                write!(
                    f,
                    "Massive deserialize error: {} (payload: {truncated}{ellipsis})",
                    message
                )
            }
            MassiveError::EnvVar { var } => {
                write!(f, "Massive environment variable not set: {}", var)
            }
            MassiveError::InvalidInput { message } => {
                write!(f, "Massive invalid input: {}", message)
            }
        }
    }
}

impl std::error::Error for MassiveError {}

impl From<MassiveError> for DataError {
    fn from(err: MassiveError) -> Self {
        DataError::Socket(err.to_string())
    }
}

impl From<reqwest::Error> for MassiveError {
    fn from(err: reqwest::Error) -> Self {
        MassiveError::Http {
            message: err.to_string(),
        }
    }
}

impl From<tokio_tungstenite::tungstenite::Error> for MassiveError {
    fn from(err: tokio_tungstenite::tungstenite::Error) -> Self {
        MassiveError::Disconnected {
            reason: err.to_string(),
        }
    }
}
