//! The London Strategic Edge market stream.
//!
//! Exists for one reason: to carry the caller's resume state from the subscriber into the
//! transformer. [`ExchangeTransformer::init`](crate::transformer::ExchangeTransformer::init) is a
//! static function with no access to the subscriber, so a stream that wants resumption has to
//! assemble the pieces itself.

use super::{live::LseSubscriber, transformer::LseTransformer};
use crate::{
    Identifier, MarketStream, SnapshotFetcher, distribute_messages_to_exchange,
    error::DataError,
    event::{MarketEvent, MarketIter},
    exchange::Connector,
    instrument::InstrumentData,
    process_buffered_events, schedule_pings_to_exchange,
    subscriber::{Subscribed, Subscriber},
    subscription::{Subscription, SubscriptionKind},
};
use futures::{Stream, StreamExt};
use rustrade_instrument::exchange::ExchangeId;
use rustrade_integration::{
    protocol::websocket::{WebSocketSerdeParser, WsStream},
    stream::ExchangeStream,
};
use std::{
    pin::Pin,
    task::{Context, Poll},
};
use tokio::sync::mpsc;

/// The market stream every London Strategic Edge subscription kind is served over.
///
/// Behaves exactly as the standard WebSocket stream does; the only difference is that its
/// initialisation hands the subscriber's resume state to the transformer, which the standard
/// initialisation has no way to do.
// The bounds are on the struct rather than only its impls because the inner `ExchangeStream`
// carries them on its own definition; there is no way to name the field type without them.
#[derive(Debug)]
pub struct LseStream<Exchange, InstrumentKey, Kind>
where
    Exchange: Connector,
    InstrumentKey: Clone,
    Kind: SubscriptionKind,
    MarketIter<InstrumentKey, Kind::Event>:
        From<(ExchangeId, InstrumentKey, super::tick::LseMessage)>,
{
    inner: ExchangeStream<
        WebSocketSerdeParser,
        WsStream,
        LseTransformer<Exchange, InstrumentKey, Kind>,
    >,
}

impl<Exchange, InstrumentKey, Kind> Stream for LseStream<Exchange, InstrumentKey, Kind>
where
    Exchange: Connector,
    InstrumentKey: Clone,
    Kind: SubscriptionKind,
    MarketIter<InstrumentKey, Kind::Event>:
        From<(ExchangeId, InstrumentKey, super::tick::LseMessage)>,
{
    type Item = Result<MarketEvent<InstrumentKey, Kind::Event>, DataError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // `WsStream` is `Unpin` and `MarketStream` requires `Unpin` regardless, so the inner
        // stream can be re-pinned without a projection.
        Pin::new(&mut self.get_mut().inner).poll_next(cx)
    }
}

impl<Exchange, Instrument, Kind> MarketStream<Exchange, Instrument, Kind>
    for LseStream<Exchange, Instrument::Key, Kind>
where
    Exchange: Connector<Subscriber = LseSubscriber> + Send + Sync,
    Instrument: InstrumentData,
    Kind: SubscriptionKind + Send + Sync,
    Kind::Event: Send + Sync,
    MarketIter<Instrument::Key, Kind::Event>:
        From<(ExchangeId, Instrument::Key, super::tick::LseMessage)>,
{
    /// Connect, subscribe, and assemble the stream around a resume-aware transformer.
    ///
    /// This mirrors the standard WebSocket initialisation and calls the same public pieces of it.
    /// It is not a copy for its own sake: the resume state has to reach the transformer **before**
    /// any buffered event is processed. Replay ticks can be sitting in that buffer already — they
    /// fail to deserialise as subscription responses while other symbols are still being
    /// confirmed, and land there — so attaching the state after the standard initialisation
    /// returned would let the first replayed ticks past the skip and straight into the stream.
    async fn init<SnapFetcher>(
        subscriber: &Exchange::Subscriber,
        subscriptions: &[Subscription<Exchange, Instrument, Kind>],
    ) -> Result<Self, DataError>
    where
        SnapFetcher: SnapshotFetcher<Exchange, Kind>,
        Subscription<Exchange, Instrument, Kind>:
            Identifier<Exchange::Channel> + Identifier<Exchange::Market>,
    {
        let Subscribed {
            websocket,
            map: instrument_map,
            buffered_websocket_events,
        } = subscriber.subscribe(subscriptions).await?;

        // Always empty for this provider -- its streams fetch no initial snapshots -- but taken
        // through the generic path so a future kind that needs one is not silently ignored.
        let initial_snapshots = SnapFetcher::fetch_snapshots(subscriptions).await?;

        let (ws_sink, ws_stream) = websocket.split();

        let (ws_sink_tx, ws_sink_rx) = mpsc::unbounded_channel();
        tokio::spawn(distribute_messages_to_exchange(
            Exchange::ID,
            ws_sink,
            ws_sink_rx,
        ));

        if let Some(ping_interval) = Exchange::ping_interval() {
            tokio::spawn(schedule_pings_to_exchange(
                Exchange::ID,
                ws_sink_tx.clone(),
                ping_interval,
            ));
        }

        // The resume state is partitioned by dataset and subscription kind -- see
        // [`LseResumeKey`](super::resume::LseResumeKey). The dataset is `Exchange::ID` and the
        // transformer reads it there; the kind has no type-level value to read, and every
        // subscription in one batch shares a `Kind`, so the first names it for all of them. An
        // empty batch has nothing to resume, which is what the `zip` yields.
        let resume = subscriber.resume_state().zip(
            subscriptions
                .first()
                .map(|subscription| subscription.kind.as_str()),
        );

        let mut transformer =
            LseTransformer::new(instrument_map, &initial_snapshots, ws_sink_tx, resume).await?;

        let mut processed = process_buffered_events::<WebSocketSerdeParser, _>(
            &mut transformer,
            buffered_websocket_events,
        );
        processed.extend(initial_snapshots.into_iter().map(Ok));

        Ok(Self {
            inner: ExchangeStream::new(ws_stream, transformer, processed),
        })
    }
}
