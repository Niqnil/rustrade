//! Error types and conversions for Databento integration.

use crate::error::DataError;
use std::error::Error as StdError;
use std::fmt;

/// Databento-specific error wrapper.
///
/// Provides context for errors from the databento crate while preserving
/// the original error's source chain via [`std::error::Error::source`].
/// Convertible to the library's [`DataError`] type.
#[derive(Debug)]
pub(crate) struct DatabentoError {
    context: &'static str,
    source: Box<dyn StdError + Send + Sync + 'static>,
}

impl DatabentoError {
    pub(crate) fn new(
        context: &'static str,
        source: impl StdError + Send + Sync + 'static,
    ) -> Self {
        Self {
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
        DataError::Socket(err.to_string())
    }
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
