#[cfg(feature = "databento")]
use crate::exchange::databento::DatabentoErrorKind;
use crate::subscription::{SubKind, candle::CandleInterval};
use rustrade_instrument::{exchange::ExchangeId, index::error::IndexError};
use rustrade_integration::{error::SocketError, subscription::SubscriptionId};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// All errors generated in `rustrade-data`.
///
/// # Non-exhaustive
/// Variants are added as integrations land, and two of the existing ones (`Databento`, `Lse` —
/// not linked: each is behind its own feature, and this block is not) appear only under that
/// feature, so the variant set a downstream `match` sees already depends on the feature selection
/// it built
/// with. A wildcard arm is required either way; `#[non_exhaustive]` makes that a compile-time
/// contract rather than something a consumer discovers when a feature flag or a release moves.
/// Matches *within* this crate stay exhaustively checked.
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize, Error)]
#[non_exhaustive]
pub enum DataError {
    #[error("failed to index market data Subscriptions: {0}")]
    Index(#[from] IndexError),

    #[error("failed to initialise reconnecting MarketStream due to empty subscriptions")]
    SubscriptionsEmpty,

    #[error("unsupported DynamicStreams Subscription SubKind: {0}")]
    UnsupportedSubKind(SubKind),

    #[error("initial snapshot missing for: {0}")]
    InitialSnapshotMissing(SubscriptionId),

    #[error("initial snapshot invalid: {0}")]
    InitialSnapshotInvalid(String),

    #[error("SocketError: {0}")]
    Socket(String),

    /// Databento-specific error with categorized kind for programmatic handling.
    #[cfg(feature = "databento")]
    #[error("Databento {kind} error ({context}): {message}")]
    Databento {
        kind: DatabentoErrorKind,
        context: String,
        message: String,
    },

    /// A London Strategic Edge integration error, flattened into this crate's common error type.
    ///
    /// [`LseError`](crate::exchange::lse::error::LseError) carries a `reqwest::Error`, which is
    /// neither `Clone` nor serialisable, so it cannot be nested here structurally. Callers wanting
    /// the full cause should handle `LseError` at the call site; this variant exists so an
    /// LSE-sourced stream can compose with the crate's generic stream helpers. Same flattening as
    /// [`Socket`](Self::Socket) applies to `SocketError`.
    ///
    /// `kind` survives that flattening, so a consumer can still tell a resumable
    /// [`RateLimit`](crate::exchange::lse::error::LseErrorKind::RateLimit) from a terminal
    /// [`Decode`](crate::exchange::lse::error::LseErrorKind::Decode) without substring-matching
    /// `message` — which is the only thing a bare `String` left it. Same shape as the `Databento`
    /// variant (not linked: it is behind the `databento` feature, which this variant is not).
    #[cfg(feature = "lse")]
    #[error("London Strategic Edge {kind} error: {message}")]
    Lse {
        kind: crate::exchange::lse::error::LseErrorKind,
        message: String,
    },

    #[error("unsupported dynamic Subscription for exchange: {exchange}, kind: {sub_kind}")]
    Unsupported {
        exchange: ExchangeId,
        sub_kind: SubKind,
    },

    #[error("exchange {exchange} does not support candle interval: {interval}")]
    UnsupportedInterval {
        exchange: ExchangeId,
        interval: CandleInterval,
    },

    #[error(
        "\
        InvalidSequence: first_update_id {first_update_id} does not follow on from the \
        prev_last_update_id {prev_last_update_id} \
    "
    )]
    InvalidSequence {
        prev_last_update_id: u64,
        first_update_id: u64,
    },
}

impl DataError {
    /// Determine if an error requires a [`MarketStream`](super::MarketStream) to re-initialise.
    // Explicit `match` (not `matches!`) is kept so additional terminal variants can be classified
    // arm-by-arm as they are added; the lint would otherwise push this to a single `matches!`.
    #[allow(clippy::match_like_matches_macro)]
    pub fn is_terminal(&self) -> bool {
        match self {
            DataError::InvalidSequence { .. } => true,
            _ => false,
        }
    }
}

impl From<SocketError> for DataError {
    fn from(value: SocketError) -> Self {
        Self::Socket(value.to_string())
    }
}

#[cfg(feature = "lse")]
impl From<crate::exchange::lse::error::LseError> for DataError {
    fn from(value: crate::exchange::lse::error::LseError) -> Self {
        Self::Lse {
            kind: value.kind(),
            message: value.to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_data_error_is_terminal() {
        struct TestCase {
            input: DataError,
            expected: bool,
        }

        let tests = vec![
            TestCase {
                // TC0: is terminal w/ DataError::InvalidSequence
                input: DataError::InvalidSequence {
                    prev_last_update_id: 0,
                    first_update_id: 0,
                },
                expected: true,
            },
            TestCase {
                // TC1: is not terminal w/ DataError::Socket
                input: DataError::from(SocketError::Sink),
                expected: false,
            },
            TestCase {
                // TC2: not terminal w/ DataError::UnsupportedInterval — a caller
                // configuration error, not a stream condition warranting re-init.
                input: DataError::UnsupportedInterval {
                    exchange: ExchangeId::HyperliquidPerp,
                    interval: CandleInterval::Sec1,
                },
                expected: false,
            },
        ];

        for (index, test) in tests.into_iter().enumerate() {
            let actual = test.input.is_terminal();
            assert_eq!(actual, test.expected, "TC{} failed", index);
        }
    }
}
