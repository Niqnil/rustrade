use crate::streams::{consumer::StreamKey, reconnect::Event};
use derive_more::Constructor;
use futures::{Stream, future::Either, stream};
use futures_util::StreamExt;
use rustrade_integration::channel::Tx;
use serde::{Deserialize, Serialize};
use std::{convert, fmt::Debug, future, future::Future};
use tracing::{error, info, warn};

/// Utilities for handling a continually reconnecting [`Stream`] initialised via the
/// [`init_reconnecting_stream`] function.
pub trait ReconnectingStream
where
    Self: Stream + Sized,
{
    /// Add an exponential backoff policy to an initialised [`ReconnectingStream`] using the
    /// provided [`ReconnectionBackoffPolicy`].
    ///
    /// A failed re-initialisation is logged with `warn!` and discarded. Use
    /// [`with_reconnect_backoff_reporting`](Self::with_reconnect_backoff_reporting) to receive it.
    fn with_reconnect_backoff<St, InitError>(
        self,
        policy: ReconnectionBackoffPolicy,
        stream_key: StreamKey,
    ) -> impl Stream<Item = St>
    where
        Self: Stream<Item = Result<St, InitError>>,
        St: Stream,
        InitError: Debug,
    {
        self.with_reconnect_backoff_reporting(policy, stream_key)
            .filter_map(|result| future::ready(result.ok()))
    }

    /// Add an exponential backoff policy to an initialised [`ReconnectingStream`] using the
    /// provided [`ReconnectionBackoffPolicy`], yielding each failed re-initialisation as a
    /// [`ReinitFailure`].
    ///
    /// A failure is yielded as soon as it happens; the backoff then runs before the next attempt.
    /// [`ReinitFailure::attempt`] counts consecutive failures, and resets when an attempt succeeds.
    fn with_reconnect_backoff_reporting<St, InitError>(
        self,
        policy: ReconnectionBackoffPolicy,
        stream_key: StreamKey,
    ) -> impl Stream<Item = Result<St, ReinitFailure<InitError>>>
    where
        Self: Stream<Item = Result<St, InitError>>,
        St: Stream,
        InitError: Debug,
    {
        self.enumerate()
            .scan(
                ReconnectionState::from(policy),
                move |state, (attempt, result)| {
                    let next =
                        match result {
                            Ok(stream) => {
                                info!(attempt, ?stream_key, "successfully initialised Stream");
                                state.reset_backoff();
                                Either::Left(stream::once(future::ready(Ok(stream))))
                            }
                            Err(error) => {
                                let consecutive = state.record_failure();
                                warn!(
                                    attempt,
                                    consecutive,
                                    ?stream_key,
                                    ?error,
                                    "failed to re-initialise Stream"
                                );
                                let backoff = state.generate_sleep_future();
                                state.multiply_backoff();
                                let failure = ReinitFailure {
                                    attempt: consecutive,
                                    error,
                                };
                                // Yield the failure, then hold the next attempt back until the
                                // backoff has elapsed: `flatten` drains this before pulling it
                                Either::Right(stream::once(future::ready(Err(failure))).chain(
                                    stream::once(backoff).filter_map(|()| future::ready(None)),
                                ))
                            }
                        };
                    future::ready(Some(next))
                },
            )
            .flatten()
    }

    /// Terminates the inner [`Stream`] if the encountered error is determined to be unrecoverable
    /// by the provided closure. This will cause the [`ReconnectingStream`] to re-initialise the
    /// inner [`Stream`].
    fn with_termination_on_error<St, T, E, FnIsTerminal>(
        self,
        is_terminal: FnIsTerminal,
        stream_key: StreamKey,
    ) -> impl Stream<Item = impl Stream<Item = Result<T, E>>>
    where
        Self: Stream<Item = St>,
        St: Stream<Item = Result<T, E>>,
        FnIsTerminal: Fn(&E) -> bool + Copy,
    {
        self.map(move |stream| terminate_on_error(stream, is_terminal, stream_key))
    }

    /// Maps every [`ReconnectingStream`] `Stream::Item` into an [`reconnect::Event::Item`](Event),
    /// and chain a [`reconnect::Event::Reconnecting`](Event)
    fn with_reconnection_events<St, Origin>(
        self,
        origin: Origin,
    ) -> impl Stream<Item = Event<Origin, St::Item>>
    where
        Self: Stream<Item = St>,
        St: Stream,
        Origin: Clone + 'static,
    {
        self.map(move |stream| with_trailing_reconnecting(stream, origin.clone()))
            .flatten()
    }

    /// Maps the initialisation attempts yielded by
    /// [`with_reconnect_backoff_reporting`](Self::with_reconnect_backoff_reporting) into
    /// [`reconnect::Event`](Event)s.
    ///
    /// Each initialised stream's items become [`Event::Item`]s, followed by one
    /// [`Event::Reconnecting`] when it ends. Each failed attempt becomes the single
    /// [`Event::Item`] that `on_failure` makes of it. A failed attempt ends no stream, so it adds
    /// no further [`Event::Reconnecting`].
    fn with_reconnection_events_reporting<St, InitError, Origin, FnOnFailure>(
        self,
        origin: Origin,
        on_failure: FnOnFailure,
    ) -> impl Stream<Item = Event<Origin, St::Item>>
    where
        Self: Stream<Item = Result<St, ReinitFailure<InitError>>>,
        St: Stream,
        Origin: Clone + 'static,
        FnOnFailure: Fn(ReinitFailure<InitError>) -> St::Item,
    {
        self.map(move |initialised| match initialised {
            Ok(stream) => Either::Left(with_trailing_reconnecting(stream, origin.clone())),
            Err(failure) => Either::Right(stream::once(future::ready(Event::Item(on_failure(
                failure,
            ))))),
        })
        .flatten()
    }

    /// Handles all encountered errors with the provided closure before filtering them out,
    /// returning a [`Stream`] of the Ok values. Useful for logging recoverable errors before
    /// continuing.
    fn with_error_handler<FnOnErr, Origin, T, E>(
        self,
        op: FnOnErr,
    ) -> impl Stream<Item = Event<Origin, T>>
    where
        Self: Stream<Item = Event<Origin, Result<T, E>>>,
        FnOnErr: Fn(E) + 'static,
    {
        self.filter_map(move |event| {
            std::future::ready(match event {
                Event::Reconnecting(origin) => Some(Event::Reconnecting(origin)),
                Event::Item(Ok(item)) => Some(Event::Item(item)),
                Event::Item(Err(error)) => {
                    op(error);
                    None
                }
            })
        })
    }

    /// Future for forwarding items in [`Self`] to the provided channel [`Tx`].
    fn forward_to<Transmitter>(self, tx: Transmitter) -> impl Future<Output = ()> + Send
    where
        Self: Stream + Sized + Send,
        Self::Item: Into<Transmitter::Item>,
        Transmitter: Tx + Send + 'static,
    {
        tokio_stream::StreamExt::map_while(self, move |event| tx.send(event.into()).ok()).collect()
    }
}

impl<T> ReconnectingStream for T where T: Stream {}

/// A failed re-initialisation of a [`ReconnectingStream`], yielded by
/// [`ReconnectingStream::with_reconnect_backoff_reporting`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReinitFailure<InitError> {
    /// Consecutive failed attempts since the stream last initialised, starting at 1.
    pub attempt: u32,
    /// Why this attempt failed.
    pub error: InitError,
}

/// Ends `stream` at the first error `is_terminal` classifies as terminal, so the
/// [`ReconnectingStream`] re-initialises it.
pub(crate) fn terminate_on_error<St, T, E, FnIsTerminal>(
    stream: St,
    is_terminal: FnIsTerminal,
    stream_key: StreamKey,
) -> impl Stream<Item = Result<T, E>>
where
    St: Stream<Item = Result<T, E>>,
    FnIsTerminal: Fn(&E) -> bool,
{
    tokio_stream::StreamExt::map_while(stream, move |result| match result {
        Ok(item) => Some(Ok(item)),
        Err(error) if is_terminal(&error) => {
            error!(
                ?stream_key,
                "MarketStream encountered terminal error that requires reconnecting"
            );
            None
        }
        Err(error) => Some(Err(error)),
    })
}

/// Maps every item of one initialised `stream` into an [`Event::Item`], followed by an
/// [`Event::Reconnecting`] when it ends.
pub(crate) fn with_trailing_reconnecting<St, Origin>(
    stream: St,
    origin: Origin,
) -> impl Stream<Item = Event<Origin, St::Item>>
where
    St: Stream,
{
    stream
        .map(Event::Item)
        .chain(stream::once(future::ready(Event::Reconnecting(origin))))
}

/// Initialise a [`ReconnectingStream`] using the provided initialisation closure.
pub async fn init_reconnecting_stream<FnInit, St, FnInitError, FnInitFut>(
    init_stream: FnInit,
) -> Result<impl Stream<Item = Result<St, FnInitError>>, FnInitError>
where
    FnInit: Fn() -> FnInitFut,
    FnInitFut: Future<Output = Result<St, FnInitError>>,
{
    let initial = init_stream().await?;
    let reconnections = futures::stream::repeat_with(init_stream).then(convert::identity);

    Ok(futures::stream::once(future::ready(Ok(initial))).chain(reconnections))
}

/// Reconnection backoff policy for a [`ReconnectingStream::with_reconnect_backoff`].
#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize, Constructor,
)]
pub struct ReconnectionBackoffPolicy {
    /// Initial backoff millisecond duration after the first `Stream` disconnection.
    ///
    /// This value then scales with the `backoff_multiplier` in the case of repeated failed
    /// `Stream` reconnection attempts.
    pub backoff_ms_initial: u64,

    /// Scaling factor for the backoff duration in the case of repeated `Stream` reconnection
    /// attempts.
    pub backoff_multiplier: u8,

    /// Maximum possible backoff duration between reconnection attempts.
    pub backoff_ms_max: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize)]
struct ReconnectionState {
    policy: ReconnectionBackoffPolicy,
    backoff_ms_current: u64,
    consecutive_failures: u32,
}

impl From<ReconnectionBackoffPolicy> for ReconnectionState {
    fn from(policy: ReconnectionBackoffPolicy) -> Self {
        Self {
            backoff_ms_current: policy.backoff_ms_initial,
            policy,
            consecutive_failures: 0,
        }
    }
}

impl ReconnectionState {
    fn reset_backoff(&mut self) {
        self.backoff_ms_current = self.policy.backoff_ms_initial;
        self.consecutive_failures = 0;
    }

    /// Count one more failed attempt, returning the consecutive total.
    fn record_failure(&mut self) -> u32 {
        self.consecutive_failures = self.consecutive_failures.saturating_add(1);
        self.consecutive_failures
    }

    fn multiply_backoff(&mut self) {
        let next = self.backoff_ms_current * self.policy.backoff_multiplier as u64;
        let next_capped = std::cmp::min(next, self.policy.backoff_ms_max);
        self.backoff_ms_current = next_capped;
    }

    fn generate_sleep_future(&self) -> tokio::time::Sleep {
        let sleep_duration = std::time::Duration::from_millis(self.backoff_ms_current);
        tokio::time::sleep(sleep_duration)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use rustrade_instrument::exchange::ExchangeId;
    use std::time::Duration;
    use tokio::time::Instant;

    const POLICY: ReconnectionBackoffPolicy = ReconnectionBackoffPolicy {
        backoff_ms_initial: 100,
        backoff_multiplier: 2,
        backoff_ms_max: 1_000,
    };

    fn key() -> StreamKey {
        StreamKey::new_general("test", ExchangeId::Simulated)
    }

    type Attempt = Result<stream::Iter<std::vec::IntoIter<u8>>, &'static str>;

    fn ok(items: &[u8]) -> Attempt {
        Ok(stream::iter(items.to_vec()))
    }

    /// Each yielded attempt, as the failure's consecutive count or `0` for a success.
    fn attempts<St>(results: &[Result<St, ReinitFailure<&'static str>>]) -> Vec<u32> {
        results
            .iter()
            .map(|result| {
                result
                    .as_ref()
                    .map_or_else(|failure| failure.attempt, |_| 0)
            })
            .collect()
    }

    #[tokio::test(start_paused = true)]
    async fn failures_count_consecutively_and_reset_on_success() {
        let results = stream::iter([ok(&[1]), Err("a"), Err("b"), ok(&[2]), Err("c")])
            .with_reconnect_backoff_reporting(POLICY, key())
            .collect::<Vec<_>>()
            .await;

        assert_eq!(attempts(&results), [0, 1, 2, 0, 1]);
        let Err(failure) = &results[2] else {
            panic!("expected a failure")
        };
        assert_eq!(failure.error, "b");
    }

    #[tokio::test(start_paused = true)]
    async fn a_failure_is_yielded_at_once_and_the_backoff_delays_the_next_attempt() {
        let started = Instant::now();
        let mut results = Box::pin(
            stream::iter([Err("a"), Err("b"), ok(&[1])])
                .with_reconnect_backoff_reporting(POLICY, key()),
        );

        assert!(results.next().await.unwrap().is_err());
        assert_eq!(started.elapsed(), Duration::ZERO);
        assert!(results.next().await.unwrap().is_err());
        assert_eq!(started.elapsed(), Duration::from_millis(100));
        assert!(results.next().await.unwrap().is_ok());
        assert_eq!(started.elapsed(), Duration::from_millis(300));
    }

    #[tokio::test(start_paused = true)]
    async fn the_discarding_backoff_yields_only_initialised_streams() {
        let streams = stream::iter([ok(&[1]), Err("a"), ok(&[2])])
            .with_reconnect_backoff(POLICY, key())
            .flatten()
            .collect::<Vec<_>>()
            .await;

        assert_eq!(streams, [1, 2]);
    }
}
