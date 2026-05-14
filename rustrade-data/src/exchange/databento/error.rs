//! Error types and conversions for Databento integration.

use crate::error::DataError;
use serde::{Deserialize, Serialize};
use std::error::Error as StdError;
use std::fmt;

/// Categorized Databento error for programmatic handling.
///
/// Enables callers to implement proper retry logic:
/// - [`Authentication`](DatabentoErrorKind::Authentication): Don't retry, fix credentials
/// - [`RateLimit`](DatabentoErrorKind::RateLimit): Retry with exponential backoff
/// - [`Network`](DatabentoErrorKind::Network): Retry with backoff
/// - [`Decode`](DatabentoErrorKind::Decode): Skip record or abort depending on context
/// - [`Api`](DatabentoErrorKind::Api): Check message for details
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum DatabentoErrorKind {
    /// API key missing, invalid, or expired.
    Authentication,
    /// Request throttled due to rate limits.
    RateLimit,
    /// Connection or network-level failure.
    Network,
    /// Failed to decode DBN record or response.
    Decode,
    /// Other API error (check message for details).
    Api,
}

impl fmt::Display for DatabentoErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Authentication => write!(f, "authentication"),
            Self::RateLimit => write!(f, "rate limit"),
            Self::Network => write!(f, "network"),
            Self::Decode => write!(f, "decode"),
            Self::Api => write!(f, "API"),
        }
    }
}

/// Internal error wrapper that preserves the source error chain.
#[derive(Debug)]
pub(crate) struct DatabentoError {
    pub(crate) kind: DatabentoErrorKind,
    context: &'static str,
    source: Box<dyn StdError + Send + Sync + 'static>,
}

impl DatabentoError {
    pub(crate) fn new(
        context: &'static str,
        source: impl StdError + Send + Sync + 'static,
    ) -> Self {
        let kind = classify_error(&source);
        Self {
            kind,
            context,
            source: Box::new(source),
        }
    }
}

impl fmt::Display for DatabentoError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Databento {}: {}", self.context, self.source)
    }
}

impl StdError for DatabentoError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        Some(self.source.as_ref())
    }
}

impl From<DatabentoError> for DataError {
    fn from(err: DatabentoError) -> Self {
        DataError::Databento {
            kind: err.kind,
            context: err.context.to_string(),
            message: err.source.to_string(),
        }
    }
}

/// Classify an error into a [`DatabentoErrorKind`] based on error message heuristics.
fn classify_error(err: &(impl StdError + ?Sized)) -> DatabentoErrorKind {
    let mut msg = err.to_string();
    msg.make_ascii_lowercase();

    if msg.contains("api key")
        || msg.contains("apikey")
        || msg.contains("unauthorized")
        || msg.contains("authentication")
        || msg.contains("invalid key")
        || msg.contains("expired")
    {
        return DatabentoErrorKind::Authentication;
    }

    if msg.contains("rate limit") || msg.contains("too many requests") || msg.contains("throttl") {
        return DatabentoErrorKind::RateLimit;
    }

    if msg.contains("connect")
        || msg.contains("timeout")
        || msg.contains("network")
        || msg.contains("dns")
        || msg.contains("socket")
        || msg.contains("unexpected end of file")
    {
        return DatabentoErrorKind::Network;
    }

    if msg.contains("decode") || msg.contains("parse") || msg.contains("invalid record") {
        return DatabentoErrorKind::Decode;
    }

    DatabentoErrorKind::Api
}

/// Extension trait for adding context to databento errors. Crate-internal
/// to avoid shadowing similarly-named methods on traits like `anyhow::Context`.
pub(crate) trait DatabentoResultExt<T> {
    fn with_context(self, ctx: &'static str) -> Result<T, DatabentoError>;
}

impl<T, E: StdError + Send + Sync + 'static> DatabentoResultExt<T> for Result<T, E> {
    fn with_context(self, ctx: &'static str) -> Result<T, DatabentoError> {
        self.map_err(|e| DatabentoError::new(ctx, e))
    }
}

/// Create a decode error from a message (for use in iterator/stream contexts).
pub(crate) fn decode_error(message: impl Into<String>) -> DataError {
    DataError::Databento {
        kind: DatabentoErrorKind::Decode,
        context: "decoding DBN record".to_string(),
        message: message.into(),
    }
}
