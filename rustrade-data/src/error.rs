#[cfg(feature = "databento")]
use crate::exchange::databento::DatabentoErrorKind;
use crate::subscription::{SubKind, candle::CandleInterval};
use rustrade_instrument::{
    exchange::ExchangeId, index::error::IndexError, instrument::name::InstrumentNameExchange,
};
use rustrade_integration::{
    error::SocketError,
    subscription::{SubscriptionId, display_subscription_ids},
};
use serde::{Deserialize, Serialize};
use std::time::Duration;
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

    /// A dynamic subscription names a venue whose subscriber must be built by the caller — usually
    /// because it carries credentials — and none was supplied. See
    /// [`DynamicSubscribers`](crate::streams::builder::dynamic::DynamicSubscribers).
    #[error(
        "dynamic Subscription for exchange {exchange} needs a subscriber, and none was supplied \
         through DynamicSubscribers"
    )]
    SubscriberRequired { exchange: ExchangeId },

    /// The venue serves the subscription, but this build was compiled without the cargo feature
    /// that carries its integration.
    #[error(
        "exchange {exchange} needs the `{feature}` cargo feature, which this build does not enable"
    )]
    FeatureDisabled {
        exchange: ExchangeId,
        feature: String,
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

    /// Subscription validation ended before every subscription in a batch was acknowledged.
    ///
    /// `unacknowledged` holds the [`SubscriptionId`]s that received no acknowledgement, sorted.
    /// They are candidates, not proof of rejection: a venue that closes the connection on one bad
    /// subscription also leaves those sent after it unanswered. Venues whose acknowledgements
    /// cannot be matched to subscriptions (one acknowledgement per batch, or one that does not
    /// echo the subscription) report the whole batch.
    ///
    /// A subscription's identifier is `ExchangeSub::new(&subscription).id()` — see
    /// [`ExchangeSub`](crate::exchange::subscription::ExchangeSub).
    #[error(
        "subscriptions not acknowledged ({reason}): {}",
        display_subscription_ids(unacknowledged)
    )]
    SubscriptionsUnacknowledged {
        reason: String,
        unacknowledged: Vec<SubscriptionId>,
    },

    /// A started stream failed to re-initialise after it ended, and will retry after its backoff.
    ///
    /// Yielded once per failed attempt. `attempt` counts consecutive failures since the stream
    /// last initialised, starting at 1, so a caller can tell a slow reconnect from one that keeps
    /// failing — for example a subscription the venue no longer accepts, reported as
    /// [`SubscriptionsUnacknowledged`](Self::SubscriptionsUnacknowledged) in `error`. The stream
    /// keeps retrying; giving up is the caller's decision.
    #[error("stream re-initialisation attempt {attempt} failed: {error}")]
    ReinitFailed { attempt: u32, error: Box<DataError> },

    /// A started stream delivered no market event for `threshold`, so it was ended and will
    /// re-initialise.
    ///
    /// Yielded only by a stream given a silence timeout through
    /// [`MarketStreamOptions::with_silence_timeout`](crate::streams::consumer::MarketStreamOptions::with_silence_timeout),
    /// once per timeout, immediately before that stream's
    /// [`Reconnecting`](crate::streams::reconnect::Event::Reconnecting). It reports that the
    /// connection was open but silent, as opposed to closed, so a caller counting transport
    /// faults can count it separately. The stream re-initialises as after any other end; giving
    /// up on a feed that stays silent is the caller's decision.
    #[error("no market event received for {threshold:?}, so the stream is reconnecting")]
    SilenceTimeout { threshold: Duration },

    /// A connection to the venue was not established within `timeout`: the TCP connect, the TLS
    /// handshake and the WebSocket upgrade together. The attempt was abandoned.
    ///
    /// `timeout` is the integration's
    /// [`Connector::connect_timeout`](crate::exchange::Connector::connect_timeout). Returned by a
    /// stream's first initialisation, and inside [`ReinitFailed`](Self::ReinitFailed) when a
    /// re-initialisation's connect times out, which the stream then retries after its backoff.
    #[error("connection to the venue not established within {timeout:?}")]
    ConnectTimeout { timeout: Duration },

    /// A stream initialisation attempt (connecting, subscribing and fetching any initial
    /// snapshots) did not complete within `timeout`, so it was abandoned.
    ///
    /// `timeout` is the stream's
    /// [`MarketStreamOptions::init_timeout`](crate::streams::consumer::MarketStreamOptions::init_timeout).
    /// It is the backstop for a step with no bound of its own: steps that have one report their
    /// own error, such as [`ConnectTimeout`](Self::ConnectTimeout) or
    /// [`SubscriptionsUnacknowledged`](Self::SubscriptionsUnacknowledged), when it expires first.
    /// Returned by a stream's first initialisation, and inside
    /// [`ReinitFailed`](Self::ReinitFailed) for a re-initialisation, which the stream then retries
    /// after its backoff.
    #[error("stream initialisation did not complete within {timeout:?}")]
    InitTimeout { timeout: Duration },

    /// The stream's connection to the venue was lost, so every subscription on it ended, and the
    /// stream will re-initialise.
    ///
    /// Yielded once per loss, immediately before the stream's
    /// [`Reconnecting`](crate::streams::reconnect::Event::Reconnecting), by a stream that sees the
    /// loss itself rather than only its connection closing. `reason` says what was lost, in the
    /// venue's words where it gave any. On IBKR: the socket to TWS/Gateway, or TWS/Gateway's own
    /// link to IB's servers (codes 1100, 1101). The stream re-initialises as after any other end;
    /// giving up is the caller's decision.
    #[error("{exchange} connection lost, so the stream is reconnecting: {reason}")]
    ConnectionLost {
        exchange: ExchangeId,
        reason: String,
    },

    /// A one-shot request, named by `operation`, got no complete answer within the time set for
    /// it, so it was given up.
    ///
    /// `operation` is the name of the public method that made the request, such as
    /// `fetch_candles`.
    ///
    /// A configured request timeout produces it, such as the `request_timeout` of
    /// `IbkrHistoricalDataConfig` (`ibkr` feature). It says nothing about whether the venue would
    /// have answered later: retrying is the caller's decision.
    #[error("{operation} got no complete answer in time")]
    RequestTimeout { operation: String },

    /// The venue refused the subscription to `instrument` because a cap on concurrent
    /// subscriptions is reached.
    ///
    /// `code` and `message` are the venue's own. Only that subscription ends; the stream's other
    /// subscriptions continue. The cap frees only when another subscription is cancelled, possibly
    /// one held outside this stream: on IBKR, market data lines (code 101) and depth requests
    /// (code 309) are shared by every session and API connection of the username, and the venue
    /// offers no way to read how many are free. Freeing one and subscribing again is the caller's
    /// decision.
    #[error("{exchange} refused {instrument}: subscription limit reached ({code} {message})")]
    SubscriptionLimitReached {
        exchange: ExchangeId,
        instrument: InstrumentNameExchange,
        code: i32,
        message: String,
    },

    /// The venue will not send market data for `instrument` to this account: it lacks the
    /// subscription, or another session has precedence.
    ///
    /// `code` and `message` are the venue's own. Only that subscription ends; the stream's other
    /// subscriptions continue. On IBKR: 354 (not subscribed), 10089 (the subscription does not
    /// extend to the API), 10168 (not subscribed, and delayed data is not enabled), 10197 (a
    /// competing live session), and for a candle subscription 2188 (the last 15 minutes of history,
    /// and so live bars, need a market data subscription). An account without the live subscription may still get delayed
    /// data, by asking for it on the stream's configuration.
    #[error("{exchange} has no market data for {instrument} on this account ({code} {message})")]
    MarketDataNotAvailable {
        exchange: ExchangeId,
        instrument: InstrumentNameExchange,
        code: i32,
        message: String,
    },

    /// A subscription asks for something the stream cannot make, such as a parameter outside the
    /// venue's range. The stream refuses it before connecting; `reason` says what is wrong.
    #[error("{exchange} cannot subscribe to {instrument}: {reason}")]
    InvalidSubscription {
        exchange: ExchangeId,
        instrument: InstrumentNameExchange,
        reason: String,
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
        match value {
            SocketError::Unacknowledged {
                reason,
                unacknowledged,
            } => Self::SubscriptionsUnacknowledged {
                reason,
                unacknowledged,
            },
            SocketError::ConnectTimeout { timeout } => Self::ConnectTimeout { timeout },
            other => Self::Socket(other.to_string()),
        }
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
    fn a_connect_timeout_keeps_its_type_and_timeout_through_the_conversion() {
        let timeout = Duration::from_secs(10);

        let error = DataError::from(SocketError::ConnectTimeout { timeout });

        assert_eq!(error, DataError::ConnectTimeout { timeout });
    }

    #[test]
    fn an_unacknowledged_batch_keeps_its_subscriptions_through_the_conversion() {
        let error = DataError::from(SocketError::Unacknowledged {
            reason: "WebSocket stream terminated unexpectedly".to_owned(),
            unacknowledged: vec!["l2Book|XYZ:TSLA".into(), "trades|@107".into()],
        });

        assert_eq!(
            error,
            DataError::SubscriptionsUnacknowledged {
                reason: "WebSocket stream terminated unexpectedly".to_owned(),
                unacknowledged: vec!["l2Book|XYZ:TSLA".into(), "trades|@107".into()],
            }
        );
        assert_eq!(
            error.to_string(),
            "subscriptions not acknowledged (WebSocket stream terminated unexpectedly): \
             l2Book|XYZ:TSLA, trades|@107"
        );
    }

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
            TestCase {
                // TC3: not terminal w/ DataError::SilenceTimeout — the stream that yields it
                // has already ended itself, and re-initialises without being terminated.
                input: DataError::SilenceTimeout {
                    threshold: Duration::from_secs(5),
                },
                expected: false,
            },
            TestCase {
                // TC4: not terminal w/ DataError::ConnectionLost — as SilenceTimeout, the stream
                // that yields it ends itself after it.
                input: DataError::ConnectionLost {
                    exchange: ExchangeId::Ibkr,
                    reason: "socket closed".to_owned(),
                },
                expected: false,
            },
            TestCase {
                // TC5: not terminal w/ DataError::InitTimeout — an attempt that failed ended no
                // stream, and the next attempt follows after the backoff.
                input: DataError::InitTimeout {
                    timeout: Duration::from_secs(60),
                },
                expected: false,
            },
            TestCase {
                // TC6: not terminal w/ DataError::ConnectTimeout — as InitTimeout, the attempt
                // failed and the next one follows after the backoff.
                input: DataError::ConnectTimeout {
                    timeout: Duration::from_secs(10),
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
