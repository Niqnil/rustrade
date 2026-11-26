use crate::{
    Identifier, MarketStream,
    error::DataError,
    event::MarketEvent,
    exchange::StreamSelector,
    instrument::InstrumentData,
    streams::{
        reconnect,
        reconnect::stream::{
            ReconnectingStream, ReconnectionBackoffPolicy, ReinitFailure, end_on_silence,
            init_reconnecting_stream, terminate_on_error,
        },
    },
    subscription::{Subscription, SubscriptionKind, display_subscriptions_without_exchange},
};
use derive_more::Constructor;
use futures::{Stream, StreamExt, future::Either, stream::BoxStream};
use rustrade_instrument::exchange::ExchangeId;
use serde::{Deserialize, Serialize};
use std::{fmt::Display, time::Duration};
use thiserror::Error;
use tracing::info;

/// Default [`ReconnectionBackoffPolicy`] for a [`reconnecting`](`ReconnectingStream`) [`MarketStream`].
pub const STREAM_RECONNECTION_POLICY: ReconnectionBackoffPolicy = ReconnectionBackoffPolicy {
    backoff_ms_initial: 125,
    backoff_multiplier: 2,
    backoff_ms_max: 60000,
};

/// Default [`MarketStreamOptions::init_timeout`]: how long one attempt to initialise a market
/// stream may take.
///
/// Longer than the steps bounded within an attempt take together by default (the connect, the
/// subscription acknowledgements and any initial snapshot, ten seconds each), so that a step with a
/// bound of its own reports its own error first.
pub const DEFAULT_INIT_TIMEOUT: Duration = Duration::from_secs(60);

/// How one market stream connection keeps going: how long each attempt to initialise it may take,
/// how it reconnects once it ends, and whether it is ended when it stays silent.
///
/// Set per connection, through [`init_market_stream`],
/// [`StreamBuilder::subscribe_with_options`](crate::streams::builder::StreamBuilder::subscribe_with_options)
/// or [`DynamicStreams::init_with_options`](crate::streams::builder::dynamic::DynamicStreams::init_with_options).
/// Built from [`Default`] and the `with_*` methods, so fields can be added without a breaking
/// change.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MarketStreamOptions {
    /// The backoff between attempts to re-initialise a stream that ended.
    pub reconnect: ReconnectionBackoffPolicy,
    // Private, unlike `reconnect`, so it can only be set through `with_silence_timeout`, which
    // refuses zero.
    silence_timeout: Option<Duration>,
    // Private for the same reason: set through `with_init_timeout`, which refuses zero.
    init_timeout: Option<Duration>,
}

impl Default for MarketStreamOptions {
    /// [`STREAM_RECONNECTION_POLICY`], an initialisation timeout of [`DEFAULT_INIT_TIMEOUT`], and
    /// no silence timeout.
    fn default() -> Self {
        Self {
            reconnect: STREAM_RECONNECTION_POLICY,
            silence_timeout: None,
            init_timeout: Some(DEFAULT_INIT_TIMEOUT),
        }
    }
}

impl MarketStreamOptions {
    /// Sets [`reconnect`](Self::reconnect).
    #[must_use]
    pub fn with_reconnect(self, reconnect: ReconnectionBackoffPolicy) -> Self {
        Self { reconnect, ..self }
    }

    /// Ends the connection's stream when it delivers no market event for `threshold`, so it
    /// re-initialises as it would after the venue closed it.
    ///
    /// This catches a connection that stays open but stops delivering: the venue may keep
    /// answering pings, or sending its own heartbeats, while the subscriptions send nothing.
    ///
    /// - **What counts:** only the market events the stream delivers. WebSocket pings and pongs,
    ///   a venue's heartbeat and acknowledgement messages, and any update the integration does
    ///   not deliver (such as a candle that has not closed yet) do not restart the timer. Nor do
    ///   the stream's errors.
    /// - **Choosing `threshold`:** it must be longer than the longest gap between events the
    ///   connection delivers when healthy, or a quiet market reconnects it. For a candle stream
    ///   that is at least the candle interval, since only closed candles are delivered. One
    ///   connection can carry several subscriptions, and the timer is for all of them together:
    ///   one busy subscription keeps a silent one on the same connection from being noticed.
    /// - **When it expires:** the stream yields [`DataError::SilenceTimeout`], then
    ///   [`reconnect::Event::Reconnecting`], and re-initialises with the backoff in
    ///   [`reconnect`](Self::reconnect). The timer starts again once the stream has initialised,
    ///   which includes the venue acknowledging its subscriptions. A successful initialisation
    ///   resets the backoff, so a connection that initialises but stays silent reconnects once
    ///   per `threshold` until the caller stops it.
    ///   An initialisation that stalls before the stream is up is not this timeout's: it is
    ///   abandoned after the [`init_timeout`](Self::init_timeout).
    /// - **A shared connection:** for a subscriber whose streams share one connection (see
    ///   [`Subscriber::Transport`](crate::subscriber::Subscriber::Transport)), the stream is a view
    ///   of that connection, and ending it does not close the socket while another stream is
    ///   attached. Its re-initialisation attaches to the same socket, and sends its subscriptions
    ///   to the venue again only if no other stream on the socket holds the same ones. So on a
    ///   shared connection the timeout still reports the silence, but reconnects the socket only
    ///   when no other stream is attached to it.
    ///
    /// Off by default.
    ///
    /// # Errors
    /// [`MarketStreamOptionsError::SilenceTimeoutZero`] if `threshold` is zero.
    pub fn with_silence_timeout(
        self,
        threshold: Duration,
    ) -> Result<Self, MarketStreamOptionsError> {
        if threshold.is_zero() {
            return Err(MarketStreamOptionsError::SilenceTimeoutZero);
        }
        Ok(Self {
            silence_timeout: Some(threshold),
            ..self
        })
    }

    /// The silence timeout set by [`with_silence_timeout`](Self::with_silence_timeout), if any.
    pub fn silence_timeout(&self) -> Option<Duration> {
        self.silence_timeout
    }

    /// Abandons an attempt to initialise the stream that has not completed within `timeout`.
    ///
    /// An attempt connects to the venue, subscribes, waits for the venue to acknowledge the
    /// subscriptions, and fetches any initial snapshots. An attempt that stalls would otherwise
    /// leave the stream waiting indefinitely, with nothing yielded and nothing retried.
    ///
    /// - **When it expires:** the first initialisation returns [`DataError::InitTimeout`]. A
    ///   re-initialisation yields it inside [`DataError::ReinitFailed`] and is retried with the
    ///   backoff in [`reconnect`](Self::reconnect), as any failed attempt is.
    /// - **A backstop:** the connect and the acknowledgements have bounds of their own,
    ///   [`Connector::connect_timeout`](crate::exchange::Connector::connect_timeout) and
    ///   [`Connector::subscription_timeout`](crate::exchange::Connector::subscription_timeout), which
    ///   report what stalled ([`DataError::ConnectTimeout`],
    ///   [`DataError::SubscriptionsUnacknowledged`]). Set `timeout` longer than those together with
    ///   any snapshot fetch, or this timeout expires first and reports only that the attempt as a
    ///   whole did not complete.
    /// - **Abandoning:** the attempt is dropped where it waits, which closes its connection. An
    ///   implementation of [`Subscriber`](crate::subscriber::Subscriber),
    ///   [`SnapshotFetcher`](crate::SnapshotFetcher) or [`MarketStream`] must
    ///   allow that at any `.await`.
    /// - **Shared connections:** where streams share one connection (Alpaca, LSE, Massive), its
    ///   task serves one attach at a time, so `timeout` also covers waiting behind other streams'
    ///   attaches. An attach whose stream is abandoned before its turn is skipped, but one already
    ///   under way runs to the end of its own bounds first. Allow for that when several streams
    ///   share a connection.
    ///
    /// - **IBKR:** `IbkrMarketStream` (`ibkr` feature) opens its connection on a blocking thread,
    ///   so an abandoned attempt stops at its next step rather than where it waits, and the
    ///   stream's next attempt waits for it to stop; see `IbkrMarketStream::init`.
    ///
    /// [`DEFAULT_INIT_TIMEOUT`] unless changed. Applies to streams built through
    /// [`init_market_stream`], and to `IbkrMarketStream`.
    ///
    /// # Errors
    /// [`MarketStreamOptionsError::InitTimeoutZero`] if `timeout` is zero.
    pub fn with_init_timeout(self, timeout: Duration) -> Result<Self, MarketStreamOptionsError> {
        if timeout.is_zero() {
            return Err(MarketStreamOptionsError::InitTimeoutZero);
        }
        Ok(Self {
            init_timeout: Some(timeout),
            ..self
        })
    }

    /// Lets an attempt to initialise the stream take as long as it takes.
    ///
    /// Steps with bounds of their own keep them (see
    /// [`with_init_timeout`](Self::with_init_timeout)); a step without one can then wait
    /// indefinitely.
    #[must_use]
    pub fn without_init_timeout(self) -> Self {
        Self {
            init_timeout: None,
            ..self
        }
    }

    /// The initialisation timeout: [`DEFAULT_INIT_TIMEOUT`], the one set by
    /// [`with_init_timeout`](Self::with_init_timeout), or `None` after
    /// [`without_init_timeout`](Self::without_init_timeout).
    pub fn init_timeout(&self) -> Option<Duration> {
        self.init_timeout
    }
}

/// A [`MarketStreamOptions`] setting that was refused.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Error)]
pub enum MarketStreamOptionsError {
    /// A zero silence timeout would end the stream as soon as it started.
    #[error("a market stream's silence timeout must be longer than zero")]
    SilenceTimeoutZero,

    /// A zero initialisation timeout would abandon every attempt before it could complete.
    #[error("a market stream's initialisation timeout must be longer than zero")]
    InitTimeoutZero,
}

/// Convenient type alias for a [`MarketEvent`] [`Result`] consumed via a
/// [`reconnecting`](`ReconnectingStream`) [`MarketStream`].
pub type MarketStreamResult<InstrumentKey, Kind> =
    reconnect::Event<ExchangeId, Result<MarketEvent<InstrumentKey, Kind>, DataError>>;

/// Convenient type alias for a [`MarketEvent`] consumed via a
/// [`reconnecting`](`ReconnectingStream`) [`MarketStream`].
pub type MarketStreamEvent<InstrumentKey, Kind> =
    reconnect::Event<ExchangeId, MarketEvent<InstrumentKey, Kind>>;

/// Initialises a [`reconnecting`](`ReconnectingStream`) [`MarketStream`] using a collection of
/// [`Subscription`]s.
///
/// `options` sets how long each initialisation attempt may take, how the stream reconnects, and
/// whether a silent stream is ended; see [`MarketStreamOptions`].
///
/// The `subscriber` is cloned into the reconnect closure, so authenticated subscribers
/// will have their credentials available on reconnection.
///
/// # Errors
/// Returns the first initialisation's error. A batch the venue did not fully acknowledge is
/// [`DataError::SubscriptionsUnacknowledged`], naming the subscriptions that received no
/// acknowledgement. A connection not established within the connector's
/// [`connect_timeout`](crate::exchange::Connector::connect_timeout) is
/// [`DataError::ConnectTimeout`], and an attempt not complete within the options'
/// [`init_timeout`](MarketStreamOptions::init_timeout) is [`DataError::InitTimeout`].
///
/// # Re-initialisation
/// When the stream ends it yields one [`reconnect::Event::Reconnecting`] and re-initialises with
/// exponential backoff. Each failed attempt yields an `Event::Item(Err(`[`DataError::ReinitFailed`]`))`
/// counting consecutive failures; retrying continues until an attempt succeeds. Deciding when to
/// give up, or to drop a subscription the venue no longer accepts, is the caller's.
///
/// With a silence timeout set, a stream that delivers no market event for that long yields
/// [`DataError::SilenceTimeout`] and is ended, then re-initialises as above.
pub async fn init_market_stream<Exchange, Instrument, Kind>(
    options: MarketStreamOptions,
    subscriber: Exchange::Subscriber,
    subscriptions: Vec<Subscription<Exchange, Instrument, Kind>>,
) -> Result<impl Stream<Item = MarketStreamResult<Instrument::Key, Kind::Event>>, DataError>
where
    Exchange: StreamSelector<Instrument, Kind> + Send + Sync + 'static,
    Instrument: InstrumentData + Display + 'static,
    Kind: SubscriptionKind + Display + Send + Sync + 'static,
    Subscription<Exchange, Instrument, Kind>:
        Identifier<Exchange::Channel> + Identifier<Exchange::Market>,
{
    // Determine ExchangeId associated with these Subscriptions
    let exchange = Exchange::ID;

    // Determine StreamKey for use in logging
    let stream_key = subscriptions
        .first()
        .map(|sub| StreamKey::new("market_stream", exchange, Some(sub.kind.as_str())))
        .ok_or(DataError::SubscriptionsEmpty)?;

    info!(
        %exchange,
        subscriptions = %display_subscriptions_without_exchange(&subscriptions),
        ?options,
        ?stream_key,
        "MarketStream with auto reconnect initialising"
    );

    // Both the exchange stream and the stream of attempts are boxed before the reconnect
    // combinators wrap them. Unboxed, every combinator's type name repeats the whole exchange
    // stream and init closure types, nested ten or more deep, once per exchange and subscription
    // kind. Those names are almost all of this crate's debug info: they took a full-debug build
    // of it to several GB in one rustc. Boxed, the combinators are instantiated once per event
    // type. The cost is one allocation per connection attempt and a virtual call per poll.
    let init_timeout = options.init_timeout;
    let attempts = init_reconnecting_stream(move || {
        let subscriber = subscriber.clone();
        let subscriptions = subscriptions.clone();
        within_init_timeout(init_timeout, async move {
            Exchange::Stream::init::<Exchange::SnapFetcher>(&subscriber, &subscriptions)
                .await
                .map(StreamExt::boxed)
        })
    })
    .await?
    .boxed();

    Ok(market_stream_events(
        attempts, options, stream_key, exchange,
    ))
}

/// Runs one initialisation `attempt`, abandoning it with [`DataError::InitTimeout`] if `timeout`
/// passes first. With no timeout the attempt runs to completion.
pub(crate) async fn within_init_timeout<T>(
    timeout: Option<Duration>,
    attempt: impl Future<Output = Result<T, DataError>>,
) -> Result<T, DataError> {
    match timeout {
        None => attempt.await,
        Some(timeout) => tokio::time::timeout(timeout, attempt)
            .await
            .unwrap_or(Err(DataError::InitTimeout { timeout })),
    }
}

/// Turns a market stream's initialisation attempts into the events its consumer receives.
///
/// Each initialised stream's items, ended early by a terminal error or by its silence timeout
/// (which yields `SilenceTimeout`), then one `Reconnecting`. A failed attempt ends no stream, so
/// it yields `ReinitFailed` and no further `Reconnecting`.
pub(crate) fn market_stream_events<T>(
    attempts: BoxStream<'static, Result<BoxStream<'static, Result<T, DataError>>, DataError>>,
    options: MarketStreamOptions,
    stream_key: StreamKey,
    exchange: ExchangeId,
) -> impl Stream<Item = reconnect::Event<ExchangeId, Result<T, DataError>>> {
    let MarketStreamOptions {
        reconnect,
        silence_timeout,
        // Applied to each attempt before it reaches here, by whoever makes the attempts.
        init_timeout: _,
    } = options;
    attempts
        .with_reconnect_backoff_reporting(reconnect, stream_key)
        .map(move |initialised| {
            initialised.map(|stream| {
                let stream = terminate_on_error(stream, DataError::is_terminal, stream_key);
                match silence_timeout {
                    None => Either::Left(stream),
                    Some(threshold) => Either::Right(end_on_silence(
                        stream,
                        threshold,
                        DataError::SilenceTimeout { threshold },
                        stream_key,
                    )),
                }
            })
        })
        .with_reconnection_events_reporting(exchange, |ReinitFailure { attempt, error }| {
            Err(DataError::ReinitFailed {
                attempt,
                error: Box::new(error),
            })
        })
}

#[derive(
    Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize, Constructor,
)]
pub struct StreamKey<Kind = &'static str> {
    pub stream: &'static str,
    pub exchange: ExchangeId,
    pub kind: Option<Kind>,
}

impl StreamKey {
    pub fn new_general(stream: &'static str, exchange: ExchangeId) -> Self {
        Self::new(stream, exchange, None)
    }
}

impl std::fmt::Debug for StreamKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.kind {
            None => write!(f, "{}-{}", self.stream, self.exchange),
            Some(kind) => write!(f, "{}-{}-{}", self.stream, self.exchange, kind),
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use crate::streams::reconnect::Event;
    use futures::stream;

    const POLICY: ReconnectionBackoffPolicy = ReconnectionBackoffPolicy {
        backoff_ms_initial: 1,
        backoff_multiplier: 2,
        backoff_ms_max: 10,
    };

    type Item = Result<u8, DataError>;

    fn initialised(items: Vec<Item>) -> Result<BoxStream<'static, Item>, DataError> {
        Ok(stream::iter(items).boxed())
    }

    fn unacknowledged() -> DataError {
        DataError::SubscriptionsUnacknowledged {
            reason: "WebSocket stream terminated unexpectedly".to_owned(),
            unacknowledged: vec!["l2Book|XYZ:TSLA".into()],
        }
    }

    #[tokio::test(start_paused = true)]
    async fn each_failed_reinit_is_yielded_and_one_reconnecting_marks_each_disconnect() {
        let exchange = ExchangeId::HyperliquidPerp;
        let attempts = stream::iter([
            initialised(vec![Ok(1)]),
            Err(unacknowledged()),
            Err(unacknowledged()),
            initialised(vec![Ok(2)]),
        ]);

        let events = market_stream_events(
            attempts.boxed(),
            MarketStreamOptions::default().with_reconnect(POLICY),
            StreamKey::new("market_stream", exchange, Some("l2")),
            exchange,
        )
        .collect::<Vec<_>>()
        .await;

        let reinit_failed = |attempt| {
            Event::Item(Err(DataError::ReinitFailed {
                attempt,
                error: Box::new(unacknowledged()),
            }))
        };
        assert_eq!(
            events,
            [
                Event::Item(Ok(1)),
                Event::Reconnecting(exchange),
                reinit_failed(1),
                reinit_failed(2),
                Event::Item(Ok(2)),
                Event::Reconnecting(exchange),
            ]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_terminal_error_ends_the_stream_before_its_remaining_items() {
        let exchange = ExchangeId::BinanceSpot;
        let terminal = DataError::InvalidSequence {
            prev_last_update_id: 1,
            first_update_id: 3,
        };
        let attempts = stream::iter([initialised(vec![Ok(1), Err(terminal), Ok(2)])]);

        let events = market_stream_events(
            attempts.boxed(),
            MarketStreamOptions::default().with_reconnect(POLICY),
            StreamKey::new("market_stream", exchange, Some("l2")),
            exchange,
        )
        .collect::<Vec<_>>()
        .await;

        assert_eq!(events, [Event::Item(Ok(1)), Event::Reconnecting(exchange)]);
    }

    const SILENCE: Duration = Duration::from_secs(5);

    fn silent() -> Result<BoxStream<'static, Item>, DataError> {
        Ok(stream::pending().boxed())
    }

    /// `items`, each yielded `after` the one before it (the first `after` initialisation), then
    /// nothing, without ending.
    fn spaced(after: Duration, items: Vec<Item>) -> Result<BoxStream<'static, Item>, DataError> {
        Ok(stream::iter(items)
            .then(move |item| async move {
                tokio::time::sleep(after).await;
                item
            })
            .chain(stream::pending())
            .boxed())
    }

    fn silence_timeout() -> Item {
        Err(DataError::SilenceTimeout { threshold: SILENCE })
    }

    /// The events of `attempts` with a silence timeout of [`SILENCE`], each with the time it
    /// arrived since the call.
    fn timed_events(
        attempts: BoxStream<'static, Result<BoxStream<'static, Item>, DataError>>,
        options: MarketStreamOptions,
    ) -> impl Stream<Item = (Duration, Event<ExchangeId, Item>)> {
        let started = tokio::time::Instant::now();
        let exchange = ExchangeId::BinanceFuturesUsd;
        market_stream_events(
            attempts,
            options,
            StreamKey::new("market_stream", exchange, Some("candles")),
            exchange,
        )
        .map(move |event| (started.elapsed(), event))
    }

    /// The first `count` events of `events`, failing the test if they have not all arrived
    /// within a day of tokio's (paused) clock, so a stream that is never ended fails rather than
    /// hangs.
    async fn first<St: Stream>(events: St, count: usize) -> Vec<St::Item> {
        tokio::time::timeout(
            Duration::from_secs(24 * 60 * 60),
            events.take(count).collect::<Vec<_>>(),
        )
        .await
        .expect("the stream yielded fewer events than expected")
    }

    fn silence_options() -> MarketStreamOptions {
        MarketStreamOptions::default()
            .with_reconnect(POLICY)
            .with_silence_timeout(SILENCE)
            .unwrap()
    }

    #[tokio::test(start_paused = true)]
    async fn a_silent_stream_yields_silence_timeout_then_reconnecting_and_reinitialises() {
        let exchange = ExchangeId::BinanceFuturesUsd;
        let attempts = stream::iter([silent(), silent()]);

        let events = first(timed_events(attempts.boxed(), silence_options()), 4).await;

        // A successful initialisation resets the backoff, so the second connection starts at once
        // and is silent for one more threshold
        assert_eq!(
            events,
            [
                (SILENCE, Event::Item(silence_timeout())),
                (SILENCE, Event::Reconnecting(exchange)),
                (SILENCE * 2, Event::Item(silence_timeout())),
                (SILENCE * 2, Event::Reconnecting(exchange)),
            ]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn each_market_event_restarts_the_silence_timer() {
        let exchange = ExchangeId::BinanceFuturesUsd;
        let gap = Duration::from_secs(3);
        let attempts = stream::iter([spaced(gap, vec![Ok(1), Ok(2)])]);

        let events = first(timed_events(attempts.boxed(), silence_options()), 4).await;

        assert_eq!(
            events,
            [
                (gap, Event::Item(Ok(1))),
                (gap * 2, Event::Item(Ok(2))),
                (gap * 2 + SILENCE, Event::Item(silence_timeout())),
                (gap * 2 + SILENCE, Event::Reconnecting(exchange)),
            ]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn an_error_does_not_restart_the_silence_timer() {
        let exchange = ExchangeId::BinanceFuturesUsd;
        let gap = Duration::from_secs(3);
        let error = DataError::Socket("frame failed to parse".to_owned());
        let attempts = stream::iter([spaced(gap, vec![Err(error.clone())])]);

        let events = first(timed_events(attempts.boxed(), silence_options()), 3).await;

        assert_eq!(
            events,
            [
                (gap, Event::Item(Err(error))),
                (SILENCE, Event::Item(silence_timeout())),
                (SILENCE, Event::Reconnecting(exchange)),
            ]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn the_silence_timer_starts_once_the_stream_has_initialised() {
        let exchange = ExchangeId::BinanceFuturesUsd;
        let init = Duration::from_secs(10);
        let attempts = stream::once(async move {
            tokio::time::sleep(init).await;
            silent()
        });

        let events = first(timed_events(attempts.boxed(), silence_options()), 2).await;

        assert_eq!(
            events,
            [
                (init + SILENCE, Event::Item(silence_timeout())),
                (init + SILENCE, Event::Reconnecting(exchange)),
            ]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_stream_that_ends_within_its_silence_timeout_only_reconnects() {
        let exchange = ExchangeId::BinanceFuturesUsd;
        let attempts = stream::iter([initialised(vec![Ok(1)])]);

        let events = timed_events(attempts.boxed(), silence_options())
            .collect::<Vec<_>>()
            .await;

        assert_eq!(
            events,
            [
                (Duration::ZERO, Event::Item(Ok(1))),
                (Duration::ZERO, Event::Reconnecting(exchange)),
            ]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_silent_stream_without_a_silence_timeout_is_never_ended() {
        let attempts = stream::iter([silent()]);
        let options = MarketStreamOptions::default().with_reconnect(POLICY);

        let mut events = Box::pin(timed_events(attempts.boxed(), options));
        let next = tokio::time::timeout(Duration::from_secs(24 * 60 * 60), events.next()).await;

        assert!(next.is_err(), "expected no event, got {next:?}");
    }

    #[test]
    fn a_zero_silence_timeout_is_refused() {
        assert_eq!(
            MarketStreamOptions::default().with_silence_timeout(Duration::ZERO),
            Err(MarketStreamOptionsError::SilenceTimeoutZero)
        );
    }

    #[test]
    fn options_default_to_the_stream_reconnection_policy_and_no_silence_timeout() {
        let options = MarketStreamOptions::default();
        assert_eq!(options.reconnect, STREAM_RECONNECTION_POLICY);
        assert_eq!(options.silence_timeout(), None);

        let options = options.with_silence_timeout(SILENCE).unwrap();
        assert_eq!(options.silence_timeout(), Some(SILENCE));
        assert_eq!(options.reconnect, STREAM_RECONNECTION_POLICY);

        let options = options.with_reconnect(POLICY);
        assert_eq!(options.reconnect, POLICY);
        assert_eq!(options.silence_timeout(), Some(SILENCE));
    }

    const INIT: Duration = Duration::from_secs(60);

    #[tokio::test(start_paused = true)]
    async fn an_attempt_not_complete_within_the_init_timeout_is_abandoned_at_the_timeout() {
        let started = tokio::time::Instant::now();

        let result =
            within_init_timeout(Some(INIT), std::future::pending::<Result<u8, DataError>>()).await;

        assert_eq!(result, Err(DataError::InitTimeout { timeout: INIT }));
        assert_eq!(started.elapsed(), INIT);
    }

    #[tokio::test(start_paused = true)]
    async fn an_attempt_complete_within_the_init_timeout_returns_its_own_result() {
        let attempt = async {
            tokio::time::sleep(INIT - Duration::from_millis(1)).await;
            Err::<u8, _>(unacknowledged())
        };

        assert_eq!(
            within_init_timeout(Some(INIT), attempt).await,
            Err(unacknowledged())
        );
    }

    #[tokio::test(start_paused = true)]
    async fn without_an_init_timeout_an_attempt_runs_to_completion() {
        let attempt = async {
            tokio::time::sleep(INIT * 10).await;
            Ok::<u8, DataError>(1)
        };

        assert_eq!(within_init_timeout(None, attempt).await, Ok(1));
    }

    /// Initialisation attempts that each run `plan`'s next step under an init timeout of
    /// [`INIT`]: `Some(items)` initialises at once with `items`, `None` never completes.
    async fn planned_attempts(
        plan: Vec<Option<Vec<Item>>>,
    ) -> Result<BoxStream<'static, Result<BoxStream<'static, Item>, DataError>>, DataError> {
        let plan = std::sync::Arc::new(std::sync::Mutex::new(plan.into_iter()));
        init_reconnecting_stream(move || {
            let step = plan.lock().unwrap().next().flatten();
            within_init_timeout(Some(INIT), async move {
                match step {
                    Some(items) => initialised(items),
                    None => std::future::pending().await,
                }
            })
        })
        .await
        .map(StreamExt::boxed)
    }

    #[tokio::test(start_paused = true)]
    async fn a_first_initialisation_that_stalls_returns_init_timeout() {
        let result = planned_attempts(vec![None]).await;

        assert!(
            matches!(result, Err(DataError::InitTimeout { timeout }) if timeout == INIT),
            "{:?}",
            result.err()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_reinitialisation_that_stalls_yields_init_timeout_and_is_retried() {
        let exchange = ExchangeId::BinanceFuturesUsd;
        let attempts = planned_attempts(vec![Some(vec![Ok(1)]), None, Some(vec![Ok(2)])])
            .await
            .unwrap();
        let options = MarketStreamOptions::default().with_reconnect(POLICY);

        let events = first(timed_events(attempts, options), 5).await;

        // The stalled attempt starts as soon as the first stream ends and is abandoned one
        // init timeout later; the next follows after the initial backoff.
        let backoff = Duration::from_millis(POLICY.backoff_ms_initial);
        assert_eq!(
            events,
            [
                (Duration::ZERO, Event::Item(Ok(1))),
                (Duration::ZERO, Event::Reconnecting(exchange)),
                (
                    INIT,
                    Event::Item(Err(DataError::ReinitFailed {
                        attempt: 1,
                        error: Box::new(DataError::InitTimeout { timeout: INIT }),
                    }))
                ),
                (INIT + backoff, Event::Item(Ok(2))),
                (INIT + backoff, Event::Reconnecting(exchange)),
            ]
        );
    }

    #[test]
    fn options_default_to_the_default_init_timeout() {
        let options = MarketStreamOptions::default();
        assert_eq!(options.init_timeout(), Some(DEFAULT_INIT_TIMEOUT));
        assert_eq!(DEFAULT_INIT_TIMEOUT, Duration::from_secs(60));

        let options = options.with_init_timeout(INIT * 2).unwrap();
        assert_eq!(options.init_timeout(), Some(INIT * 2));
        assert_eq!(options.reconnect, STREAM_RECONNECTION_POLICY);
        assert_eq!(options.silence_timeout(), None);

        let options = options.without_init_timeout();
        assert_eq!(options.init_timeout(), None);
    }

    #[test]
    fn a_zero_init_timeout_is_refused() {
        assert_eq!(
            MarketStreamOptions::default().with_init_timeout(Duration::ZERO),
            Err(MarketStreamOptionsError::InitTimeoutZero)
        );
    }

    mod through_init_market_stream {
        use super::*;
        use crate::test_support::stalling_bybit_trades;
        use rustrade_integration::protocol::websocket::DEFAULT_CONNECT_TIMEOUT;

        #[tokio::test(start_paused = true)]
        async fn an_init_timeout_shorter_than_the_connect_abandons_the_first_attempt() {
            let timeout = Duration::from_secs(1);
            let options = MarketStreamOptions::default()
                .with_init_timeout(timeout)
                .unwrap();
            let started = tokio::time::Instant::now();

            let result =
                init_market_stream(options, Default::default(), vec![stalling_bybit_trades()])
                    .await;

            assert!(
                matches!(result, Err(DataError::InitTimeout { timeout: t }) if t == timeout),
                "{:?}",
                result.err()
            );
            assert_eq!(started.elapsed(), timeout);
        }

        #[tokio::test(start_paused = true)]
        async fn with_the_default_init_timeout_a_stalled_connect_reports_its_own_timeout() {
            let result = init_market_stream(
                MarketStreamOptions::default(),
                Default::default(),
                vec![stalling_bybit_trades()],
            )
            .await;

            assert!(
                matches!(
                    result,
                    Err(DataError::ConnectTimeout { timeout }) if timeout == DEFAULT_CONNECT_TIMEOUT
                ),
                "{:?}",
                result.err()
            );
        }
    }
}
