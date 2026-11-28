use crate::streams::{consumer::StreamKey, reconnect::Event};
use derive_more::Constructor;
use futures::{Stream, future::Either, stream};
use futures_util::StreamExt;
use rustrade_integration::channel::Tx;
use serde::{Deserialize, Serialize};
use std::{convert, fmt::Debug, future, future::Future, time::Duration};
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
    ///
    /// To also honour a wait the source of a failure asked for, such as a venue's rate-limit
    /// hint, use [`with_reconnect_backoff_reporting_hinted`](Self::with_reconnect_backoff_reporting_hinted).
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
        self.with_reconnect_backoff_reporting_hinted(policy, stream_key, |_| None)
    }

    /// As [`with_reconnect_backoff_reporting`](Self::with_reconnect_backoff_reporting), but the
    /// next attempt after a failure is held back for at least the wait `retry_after` reads from
    /// it: the longer of that wait and the policy's wait, jitter included. A failure whose wait
    /// is `None` waits the policy's wait alone.
    ///
    /// The hint does not change the backoff, which grows with each consecutive failure as usual.
    /// It is a floor for one wait, so an attempt is never made before the source said it could
    /// succeed, nor sooner than the policy allows. It is not capped: a caller that wants a limit
    /// clamps the wait in `retry_after`.
    ///
    /// A hint longer than the policy's wait is waited exactly, with no jitter, so streams given
    /// the same hint retry together. A caller that wants them spread adds its own jitter to the
    /// wait in `retry_after`.
    fn with_reconnect_backoff_reporting_hinted<St, InitError, FnRetryAfter>(
        self,
        policy: ReconnectionBackoffPolicy,
        stream_key: StreamKey,
        retry_after: FnRetryAfter,
    ) -> impl Stream<Item = Result<St, ReinitFailure<InitError>>>
    where
        Self: Stream<Item = Result<St, InitError>>,
        St: Stream,
        InitError: Debug,
        FnRetryAfter: Fn(&InitError) -> Option<Duration>,
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
                                let hint = retry_after(&error);
                                warn!(
                                    attempt,
                                    consecutive,
                                    ?stream_key,
                                    ?error,
                                    ?hint,
                                    "failed to re-initialise Stream"
                                );
                                let backoff = state.generate_sleep_future(hint);
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
    ///
    /// Pairs with [`with_reconnect_backoff`](Self::with_reconnect_backoff), which discards failed
    /// re-initialisations. To report them as events, use
    /// [`with_reconnect_backoff_reporting`](Self::with_reconnect_backoff_reporting) with
    /// [`with_reconnection_events_reporting`](Self::with_reconnection_events_reporting).
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
        Origin: Clone,
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

/// Ends `stream` once it has yielded no `Ok` item for `threshold`, yielding `on_silence` first.
///
/// The timer starts when this is called and restarts at each `Ok` item. An `Err` item is passed
/// on without restarting it, so a stream yielding only errors is silent. The deadline runs on
/// tokio's clock, so a test can advance it with paused time.
pub(crate) fn end_on_silence<St, T, E>(
    stream: St,
    threshold: Duration,
    on_silence: E,
    stream_key: StreamKey,
) -> impl Stream<Item = Result<T, E>>
where
    St: Stream<Item = Result<T, E>> + Unpin,
{
    let deadline = tokio::time::Instant::now() + threshold;
    stream::unfold(
        Some((stream, deadline, on_silence)),
        move |state| async move {
            let (mut stream, deadline, on_silence) = state?;
            match tokio::time::timeout_at(deadline, stream.next()).await {
                Ok(Some(Ok(item))) => {
                    let deadline = tokio::time::Instant::now() + threshold;
                    Some((Ok(item), Some((stream, deadline, on_silence))))
                }
                Ok(Some(Err(error))) => Some((Err(error), Some((stream, deadline, on_silence)))),
                Ok(None) => None,
                Err(_elapsed) => {
                    warn!(
                        ?stream_key,
                        ?threshold,
                        "MarketStream delivered nothing within its silence timeout, reconnecting"
                    );
                    Some((Err(on_silence), None))
                }
            }
        },
    )
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
///
/// # The wait
///
/// After the `n`th failed attempt in a row (`n` from 1), the backoff is
/// `backoff_ms_initial × backoff_multiplier^(n − 1)`, capped at `backoff_ms_max`. The wait before
/// the next attempt is drawn at random from the top `backoff_jitter_percent` of that backoff: with
/// `d` the backoff and `p` the percent, from `d × (1 − p/100)` to `d`, both included. So:
///
/// - a wait is never longer than `backoff_ms_max`;
/// - `0` gives exactly the backoff, every time; `50` a wait between half the backoff and all of
///   it; `100` anything from no wait to all of it;
/// - streams that failed together, such as every stream to a venue that dropped them all at once,
///   spread their next attempts over that range instead of all retrying at the same instant.
///
/// A percent above 100 counts as 100. A `backoff_multiplier` of 1 keeps the backoff at
/// `backoff_ms_initial`, and 0 makes every wait after the first zero.
///
/// A configuration deserialized without `backoff_jitter_percent` gets
/// [`DEFAULT_BACKOFF_JITTER_PERCENT`].
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

    /// How much of the backoff each wait may be cut by at random, in percent: see
    /// [The wait](ReconnectionBackoffPolicy#the-wait).
    #[serde(default = "default_backoff_jitter_percent")]
    pub backoff_jitter_percent: u8,
}

/// The [`ReconnectionBackoffPolicy::backoff_jitter_percent`] of
/// [`STREAM_RECONNECTION_POLICY`](crate::streams::consumer::STREAM_RECONNECTION_POLICY), and of a
/// configuration deserialized without one: each wait falls anywhere from no wait to all of the
/// backoff, which spreads streams that failed together the most and gives the shortest waits on
/// average, half the backoff. A lower percent keeps a floor under each wait, at the cost of a
/// longer one.
pub const DEFAULT_BACKOFF_JITTER_PERCENT: u8 = 100;

fn default_backoff_jitter_percent() -> u8 {
    DEFAULT_BACKOFF_JITTER_PERCENT
}

impl ReconnectionBackoffPolicy {
    /// The wait after the `failures`th failed attempt in a row, drawn as
    /// [The wait](ReconnectionBackoffPolicy#the-wait) says. `failures` of 0 counts as 1.
    pub(crate) fn wait(&self, failures: u32) -> Duration {
        self.wait_cut_by(failures, |most| rand::random_range(0..=most))
    }

    /// The wait after the `failures`th failed attempt in a row: the backoff, less what `cut` picks
    /// given the most the wait may be cut by, in milliseconds. A pick above that most counts as
    /// that most.
    fn wait_cut_by(&self, failures: u32, cut: impl FnOnce(u64) -> u64) -> Duration {
        let backoff = self.backoff_ms(failures);
        let percent = u64::from(self.backoff_jitter_percent.min(100));
        // `backoff × percent / 100`, rounded down, split at the hundreds so no product overflows:
        // the first term is at most `backoff`, the second at most 99 × 100.
        let most = backoff / 100 * percent + backoff % 100 * percent / 100;
        Duration::from_millis(backoff - cut(most).min(most))
    }

    /// The backoff, in milliseconds, after the `failures`th failed attempt in a row, before any
    /// jitter: `backoff_ms_initial × backoff_multiplier^(failures − 1)`, capped at
    /// `backoff_ms_max`.
    fn backoff_ms(&self, failures: u32) -> u64 {
        // Saturating, so a power or product past `u64::MAX` lands on the cap instead of wrapping.
        let growth = u64::from(self.backoff_multiplier).saturating_pow(failures.saturating_sub(1));
        self.backoff_ms_initial
            .saturating_mul(growth)
            .min(self.backoff_ms_max)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct ReconnectionState {
    policy: ReconnectionBackoffPolicy,
    consecutive_failures: u32,
}

impl From<ReconnectionBackoffPolicy> for ReconnectionState {
    fn from(policy: ReconnectionBackoffPolicy) -> Self {
        Self {
            policy,
            consecutive_failures: 0,
        }
    }
}

impl ReconnectionState {
    fn reset_backoff(&mut self) {
        self.consecutive_failures = 0;
    }

    /// Count one more failed attempt, returning the consecutive total.
    fn record_failure(&mut self) -> u32 {
        self.consecutive_failures = self.consecutive_failures.saturating_add(1);
        self.consecutive_failures
    }

    /// The wait before the next attempt: the policy's wait after the failures so far, or
    /// `at_least` when that is longer.
    fn generate_sleep_future(&self, at_least: Option<Duration>) -> tokio::time::Sleep {
        let backoff = self.policy.wait(self.consecutive_failures);
        tokio::time::sleep(at_least.map_or(backoff, |hint| hint.max(backoff)))
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
        backoff_jitter_percent: 0,
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
    async fn a_hinted_wait_longer_than_the_backoff_holds_the_next_attempt_back() {
        // "wait" asks for 500 ms; "short" for 50 ms, under the backoff; "none" for nothing.
        let hint = |error: &&str| match *error {
            "wait" => Some(Duration::from_millis(500)),
            "short" => Some(Duration::from_millis(50)),
            _ => None,
        };
        let started = Instant::now();
        let mut results = Box::pin(
            stream::iter([Err("wait"), Err("none"), Err("short"), ok(&[1])])
                .with_reconnect_backoff_reporting_hinted(POLICY, key(), hint),
        );

        assert!(results.next().await.unwrap().is_err());
        assert_eq!(started.elapsed(), Duration::ZERO);
        // The hint outlasts the 100 ms backoff, so it sets the wait.
        assert!(results.next().await.unwrap().is_err());
        assert_eq!(started.elapsed(), Duration::from_millis(500));
        // The hint did not stop the backoff growing: 200 ms now.
        assert!(results.next().await.unwrap().is_err());
        assert_eq!(started.elapsed(), Duration::from_millis(700));
        // A hint shorter than the 400 ms backoff does not shorten it.
        assert!(results.next().await.unwrap().is_ok());
        assert_eq!(started.elapsed(), Duration::from_millis(1_100));
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

    #[tokio::test]
    async fn a_failure_becomes_one_item_and_only_an_ended_stream_adds_reconnecting() {
        let origin = ExchangeId::Simulated;
        let failure = |attempt| ReinitFailure {
            attempt,
            error: "refused",
        };
        let events = stream::iter([
            Ok(stream::iter(vec![1_u32])),
            Err(failure(1)),
            Err(failure(2)),
            Ok(stream::iter(vec![2])),
        ])
        .with_reconnection_events_reporting(origin, |failure| 100 + failure.attempt)
        .collect::<Vec<_>>()
        .await;

        assert_eq!(
            events,
            [
                Event::Item(1),
                Event::Reconnecting(origin),
                Event::Item(101),
                Event::Item(102),
                Event::Item(2),
                Event::Reconnecting(origin),
            ]
        );
    }

    /// `POLICY` with `percent` of jitter.
    fn jittered(percent: u8) -> ReconnectionBackoffPolicy {
        ReconnectionBackoffPolicy {
            backoff_jitter_percent: percent,
            ..POLICY
        }
    }

    #[test]
    fn the_backoff_grows_by_the_multiplier_up_to_its_cap() {
        let backoffs = (0..=6).map(|n| POLICY.backoff_ms(n)).collect::<Vec<_>>();

        // No failure yet counts as the first.
        assert_eq!(backoffs, [100, 100, 200, 400, 800, 1_000, 1_000]);
        assert_eq!(POLICY.backoff_ms(u32::MAX), 1_000);
    }

    #[test]
    fn the_backoff_never_passes_the_cap_nor_overflows() {
        let above = ReconnectionBackoffPolicy {
            backoff_ms_initial: 5_000,
            ..POLICY
        };
        assert_eq!(above.backoff_ms(1), 1_000);

        let huge = ReconnectionBackoffPolicy::new(u64::MAX / 2, u8::MAX, u64::MAX, 0);
        assert_eq!(huge.backoff_ms(3), u64::MAX);
    }

    #[test]
    fn a_multiplier_of_one_holds_the_backoff_and_zero_drops_it_after_the_first() {
        let flat = ReconnectionBackoffPolicy::new(100, 1, 1_000, 0);
        assert_eq!(flat.backoff_ms(u32::MAX), 100);

        let zero = ReconnectionBackoffPolicy::new(100, 0, 1_000, 0);
        assert_eq!([zero.backoff_ms(1), zero.backoff_ms(2)], [100, 0]);
    }

    #[test]
    fn the_jitter_cuts_up_to_its_percent_off_the_backoff() {
        // The third failure's backoff is 400 ms; 50 % may cut up to 200 ms of it.
        let half = jittered(50);
        let mut offered = None;
        let uncut = half.wait_cut_by(3, |most| {
            offered = Some(most);
            0
        });
        assert_eq!(offered, Some(200));
        assert_eq!(uncut, Duration::from_millis(400));
        assert_eq!(half.wait_cut_by(3, |most| most), Duration::from_millis(200));
        assert_eq!(half.wait_cut_by(3, |_| 150), Duration::from_millis(250));
        // A cut past the most counts as the most.
        assert_eq!(
            half.wait_cut_by(3, |_| u64::MAX),
            Duration::from_millis(200)
        );
    }

    #[test]
    fn no_jitter_waits_the_backoff_and_a_percent_past_100_counts_as_100() {
        assert_eq!(
            jittered(0).wait_cut_by(3, |most| most),
            Duration::from_millis(400)
        );
        assert_eq!(jittered(0).wait(3), Duration::from_millis(400));
        assert_eq!(jittered(100).wait_cut_by(3, |most| most), Duration::ZERO);
        assert_eq!(jittered(255).wait_cut_by(3, |most| most), Duration::ZERO);
    }

    #[test]
    fn the_jitter_cut_rounds_down_and_never_overflows() {
        // `u64::MAX` is 18,446,744,073,709,551,615; half of it, rounded down, is `i64::MAX`.
        let mut offered = None;
        ReconnectionBackoffPolicy::new(u64::MAX, 2, u64::MAX, 50).wait_cut_by(1, |most| {
            offered = Some(most);
            0
        });
        assert_eq!(offered, Some(i64::MAX.unsigned_abs()));

        // 50 % of a 99 ms backoff is 49.5 ms, so the most the cut may take is 49 ms.
        let odd = ReconnectionBackoffPolicy::new(99, 2, 1_000, 50);
        assert_eq!(odd.wait_cut_by(1, |most| most), Duration::from_millis(50));
    }

    #[tokio::test(start_paused = true)]
    async fn a_hint_longer_than_the_jittered_wait_is_waited_exactly() {
        let hint = |_: &&str| Some(Duration::from_millis(500));
        let started = Instant::now();
        let mut results = Box::pin(
            stream::iter([Err("a"), ok(&[1])]).with_reconnect_backoff_reporting_hinted(
                jittered(50),
                key(),
                hint,
            ),
        );

        assert!(results.next().await.unwrap().is_err());
        assert!(results.next().await.unwrap().is_ok());
        // The jittered wait is 50 to 100 ms, so the 500 ms hint sets it, unjittered.
        assert_eq!(started.elapsed(), Duration::from_millis(500));
    }

    #[test]
    fn a_drawn_wait_falls_in_the_jitter_range_and_varies() {
        let waits = (0..1_000)
            .map(|_| jittered(50).wait(3).as_millis())
            .collect::<Vec<_>>();

        assert!(
            waits.iter().all(|wait| (200..=400).contains(wait)),
            "{waits:?}"
        );
        // Each of the 201 values is equally likely, so 1,000 draws all at one value never happen.
        assert!(waits.iter().any(|wait| *wait != waits[0]), "{waits:?}");
    }

    #[test]
    fn a_configuration_without_a_jitter_percent_gets_the_default() {
        let policy: ReconnectionBackoffPolicy = serde_json::from_str(
            r#"{"backoff_ms_initial": 125, "backoff_multiplier": 2, "backoff_ms_max": 60000}"#,
        )
        .unwrap();
        assert_eq!(
            policy.backoff_jitter_percent,
            DEFAULT_BACKOFF_JITTER_PERCENT
        );

        let stated: ReconnectionBackoffPolicy = serde_json::from_str(
            r#"{"backoff_ms_initial": 125, "backoff_multiplier": 2, "backoff_ms_max": 60000,
                "backoff_jitter_percent": 0}"#,
        )
        .unwrap();
        assert_eq!(stated.backoff_jitter_percent, 0);
    }

    #[tokio::test(start_paused = true)]
    async fn a_jittered_backoff_holds_the_next_attempt_back_within_the_range() {
        let mut waits = Vec::new();
        for _ in 0..20 {
            let started = Instant::now();
            let mut results = Box::pin(
                stream::iter([Err("a"), ok(&[1])])
                    .with_reconnect_backoff_reporting(jittered(50), key()),
            );
            assert!(results.next().await.unwrap().is_err());
            assert!(results.next().await.unwrap().is_ok());
            waits.push(started.elapsed());
        }

        // The first failure's backoff is 100 ms; 50 % jitter waits 50 to 100 ms.
        let range = Duration::from_millis(50)..=Duration::from_millis(100);
        assert!(waits.iter().all(|wait| range.contains(wait)), "{waits:?}");
        // Each of the 51 whole milliseconds is equally likely, so 20 draws all at one never happen.
        assert!(waits.iter().any(|wait| *wait != waits[0]), "{waits:?}");
    }
}
