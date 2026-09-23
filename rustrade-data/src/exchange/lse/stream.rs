//! The London Strategic Edge market stream.
//!
//! Exists for two reasons. Every stream reads from the connection its subscriber shares — an
//! [`LseAttachment`] rather than a socket of its own, which the standard WebSocket initialisation
//! cannot read. And the caller's resume state has to reach the transformer:
//! [`ExchangeTransformer::init`](crate::transformer::ExchangeTransformer::init) is a static
//! function with no access to the subscriber, so a stream that wants resumption has to assemble
//! the pieces itself.

use super::{
    connection::LseAttachment,
    live::{LseSubscriber, subscribes_per_underlying},
    transformer::{LseTransformer, ResumeContext},
};
use crate::{
    Identifier, MarketStream, SnapshotFetcher,
    error::DataError,
    event::{MarketEvent, MarketIter},
    exchange::Connector,
    instrument::InstrumentData,
    process_buffered_events,
    subscriber::{Subscribed, Subscriber},
    subscription::{Subscription, SubscriptionKind},
};
use futures::Stream;
use rustrade_instrument::exchange::ExchangeId;
use rustrade_integration::{protocol::websocket::WebSocketSerdeParser, stream::ExchangeStream};
use std::{
    pin::Pin,
    task::{Context, Poll},
};
use tokio::sync::mpsc;
use tracing::warn;

/// The market stream every London Strategic Edge subscription kind is served over.
///
/// Parses and transforms exactly as the standard WebSocket stream does. It differs in what it reads
/// — its view of the subscriber's shared connection — and in its initialisation, which hands the
/// subscriber's resume state and the connection's replay windows to the transformer.
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
        LseAttachment,
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
        // `LseAttachment` is `Unpin` and `MarketStream` requires `Unpin` regardless, so the inner
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
    /// Attach to the subscriber's connection, and assemble the stream around a resume-aware
    /// transformer.
    ///
    /// The resume state has to reach the transformer **before** any frame is processed. The
    /// connection routes a batch's frames into its attachment from the moment the batch is
    /// registered, so replayed ticks can already be waiting there when the attach returns;
    /// attaching the state any later would let them past the skip and straight into the stream.
    ///
    /// Nothing is sent on the stream's behalf: this provider needs no pings, and the transformer
    /// sends nothing, so the transformer is handed a sender nobody reads.
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
            transport: mut attachment,
            map: instrument_map,
            buffered_websocket_events,
        } = subscriber.subscribe(subscriptions).await?;

        // Always empty for this provider -- its streams fetch no initial snapshots -- but taken
        // through the generic path so a future kind that needs one is not silently ignored.
        let initial_snapshots = SnapFetcher::fetch_snapshots(subscriptions).await?;

        let (ws_sink_tx, _unread) = mpsc::unbounded_channel();

        // The resume state is partitioned by dataset and subscription kind -- see
        // [`LseResumeKey`](super::resume::LseResumeKey). The dataset is `Exchange::ID` and the
        // transformer reads it there; the kind has no type-level value to read, and every
        // subscription in one batch shares a `Kind`, so the first names it for all of them. An
        // empty batch has nothing to resume, which is what the `zip` yields.
        //
        // Option contracts never resume -- see `LseOptions` -- so the state is withheld from their
        // transformer too. Held, it would skip live prints at the watermark's instant as though a
        // replay had re-sent them, when no replay was asked for.
        let starts = attachment.take_starts();
        let resume = if subscribes_per_underlying(Exchange::ID) {
            if subscriber.resume_state().is_some() {
                warn!(
                    exchange = %Exchange::ID,
                    "London Strategic Edge option contracts do not resume; this stream will not \
                     replay what a reconnect missed",
                );
            }
            None
        } else {
            subscriber
                .resume_state()
                .zip(
                    subscriptions
                        .first()
                        .map(|subscription| subscription.kind.as_str()),
                )
                .map(|(state, kind)| ResumeContext {
                    state,
                    kind,
                    starts,
                })
        };

        let mut transformer =
            LseTransformer::new(instrument_map, &initial_snapshots, ws_sink_tx, resume).await?;

        // Empty for this provider -- see `LseSubscriber::subscribe` -- but processed through the
        // generic path so the `Subscribed` contract holds whatever the subscriber hands back.
        let mut processed = process_buffered_events::<WebSocketSerdeParser, _>(
            &mut transformer,
            buffered_websocket_events,
        );
        processed.extend(initial_snapshots.into_iter().map(Ok));

        Ok(Self {
            inner: ExchangeStream::new(attachment, transformer, processed),
        })
    }
}
