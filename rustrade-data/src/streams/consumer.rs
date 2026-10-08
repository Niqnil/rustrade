use crate::{
    Identifier, MarketStream,
    error::DataError,
    event::MarketEvent,
    exchange::StreamSelector,
    instrument::InstrumentData,
    streams::{
        reconnect,
        reconnect::stream::{
            ReconnectingStream, ReconnectionBackoffPolicy, ReinitFailure, init_reconnecting_stream,
            terminate_on_error,
        },
    },
    subscription::{Subscription, SubscriptionKind, display_subscriptions_without_exchange},
};
use derive_more::Constructor;
use futures::{Stream, StreamExt};
use rustrade_instrument::exchange::ExchangeId;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use tracing::info;

/// Default [`ReconnectionBackoffPolicy`] for a [`reconnecting`](`ReconnectingStream`) [`MarketStream`].
pub const STREAM_RECONNECTION_POLICY: ReconnectionBackoffPolicy = ReconnectionBackoffPolicy {
    backoff_ms_initial: 125,
    backoff_multiplier: 2,
    backoff_ms_max: 60000,
};

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
/// The provided [`ReconnectionBackoffPolicy`] dictates how the exponential backoff scales
/// between reconnections.
///
/// The `subscriber` is cloned into the reconnect closure, so authenticated subscribers
/// will have their credentials available on reconnection.
///
/// # Errors
/// Returns the first initialisation's error. A batch the venue did not fully acknowledge is
/// [`DataError::SubscriptionsUnacknowledged`], naming the subscriptions that received no
/// acknowledgement.
///
/// # Re-initialisation
/// When the stream ends it yields one [`reconnect::Event::Reconnecting`] and re-initialises with
/// exponential backoff. Each failed attempt yields an `Event::Item(Err(`[`DataError::ReinitFailed`]`))`
/// counting consecutive failures; retrying continues until an attempt succeeds. Deciding when to
/// give up, or to drop a subscription the venue no longer accepts, is the caller's.
pub async fn init_market_stream<Exchange, Instrument, Kind>(
    policy: ReconnectionBackoffPolicy,
    subscriber: Exchange::Subscriber,
    subscriptions: Vec<Subscription<Exchange, Instrument, Kind>>,
) -> Result<impl Stream<Item = MarketStreamResult<Instrument::Key, Kind::Event>>, DataError>
where
    Exchange: StreamSelector<Instrument, Kind>,
    Instrument: InstrumentData + Display,
    Kind: SubscriptionKind + Display,
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
        ?policy,
        ?stream_key,
        "MarketStream with auto reconnect initialising"
    );

    let attempts =
        init_reconnecting_stream(move || {
            let subscriber = subscriber.clone();
            let subscriptions = subscriptions.clone();
            async move {
                Exchange::Stream::init::<Exchange::SnapFetcher>(&subscriber, &subscriptions).await
            }
        })
        .await?;

    Ok(market_stream_events(attempts, policy, stream_key, exchange))
}

/// Turns a market stream's initialisation attempts into the events its consumer receives.
///
/// Each initialised stream's items, ended early by a terminal error, then one `Reconnecting`. A
/// failed attempt ends no stream, so it yields `ReinitFailed` and no further `Reconnecting`.
fn market_stream_events<Attempts, St, T>(
    attempts: Attempts,
    policy: ReconnectionBackoffPolicy,
    stream_key: StreamKey,
    exchange: ExchangeId,
) -> impl Stream<Item = reconnect::Event<ExchangeId, Result<T, DataError>>>
where
    Attempts: Stream<Item = Result<St, DataError>>,
    St: Stream<Item = Result<T, DataError>>,
{
    attempts
        .with_reconnect_backoff_reporting(policy, stream_key)
        .map(move |initialised| {
            initialised.map(|stream| terminate_on_error(stream, DataError::is_terminal, stream_key))
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
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
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

    fn initialised(items: Vec<Item>) -> Result<stream::Iter<std::vec::IntoIter<Item>>, DataError> {
        Ok(stream::iter(items))
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
            attempts,
            POLICY,
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
            attempts,
            POLICY,
            StreamKey::new("market_stream", exchange, Some("l2")),
            exchange,
        )
        .collect::<Vec<_>>()
        .await;

        assert_eq!(events, [Event::Item(Ok(1)), Event::Reconnecting(exchange)]);
    }
}
