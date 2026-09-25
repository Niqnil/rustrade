//! The Alpaca market stream.
//!
//! Exists because every stream reads from its feed's shared connection — an [`AlpacaAttachment`]
//! rather than a socket of its own, which the standard WebSocket initialisation cannot read.

use super::{AlpacaSubscriber, connection::AlpacaAttachment};
use crate::{
    Identifier, MarketStream, SnapshotFetcher,
    error::DataError,
    exchange::Connector,
    instrument::InstrumentData,
    process_buffered_events,
    subscriber::{Subscribed, Subscriber},
    subscription::{Subscription, SubscriptionKind},
    transformer::ExchangeTransformer,
};
use futures::Stream;
use rustrade_integration::{
    Transformer,
    protocol::{
        StreamParser,
        websocket::{WebSocketSerdeParser, WsError, WsMessage},
    },
    stream::ExchangeStream,
};
use std::{
    pin::Pin,
    task::{Context, Poll},
};
use tokio::sync::mpsc;

/// The market stream every Alpaca subscription kind is served over.
///
/// Parses and transforms exactly as the standard WebSocket stream does. It differs only in what it
/// reads: its view of the subscriber's shared connection to the feed.
// The bounds are on the struct rather than only its impls because the inner `ExchangeStream`
// carries them on its own definition; there is no way to name the field type without them.
pub struct AlpacaStream<StreamTransformer>
where
    StreamTransformer: Transformer,
    WebSocketSerdeParser: StreamParser<StreamTransformer::Input>,
{
    inner: ExchangeStream<WebSocketSerdeParser, AlpacaAttachment, StreamTransformer>,
}

// Written by hand: a derive would demand `Debug` of the transformer's output and error types
// through the buffered events, which says nothing useful about the stream.
impl<StreamTransformer> std::fmt::Debug for AlpacaStream<StreamTransformer>
where
    StreamTransformer: Transformer,
    WebSocketSerdeParser: StreamParser<StreamTransformer::Input>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AlpacaStream")
            .field("buffered", &self.inner.buffer.len())
            .finish_non_exhaustive()
    }
}

impl<StreamTransformer> Stream for AlpacaStream<StreamTransformer>
where
    StreamTransformer: Transformer,
    WebSocketSerdeParser: StreamParser<StreamTransformer::Input>,
    ExchangeStream<WebSocketSerdeParser, AlpacaAttachment, StreamTransformer>:
        Stream<Item = Result<StreamTransformer::Output, StreamTransformer::Error>> + Unpin,
{
    type Item = Result<StreamTransformer::Output, StreamTransformer::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // `AlpacaAttachment` is `Unpin` and `MarketStream` requires `Unpin` regardless, so the
        // inner stream can be re-pinned without a projection.
        Pin::new(&mut self.get_mut().inner).poll_next(cx)
    }
}

impl<Exchange, Instrument, Kind, StreamTransformer> MarketStream<Exchange, Instrument, Kind>
    for AlpacaStream<StreamTransformer>
where
    Exchange: Connector<Subscriber = AlpacaSubscriber> + Send + Sync,
    Instrument: InstrumentData,
    Kind: SubscriptionKind + Send + Sync,
    Kind::Event: Send + Sync,
    StreamTransformer: ExchangeTransformer<Exchange, Instrument::Key, Kind> + Send,
    WebSocketSerdeParser:
        StreamParser<StreamTransformer::Input, Message = WsMessage, Error = WsError>,
{
    /// Attach to the feed's shared connection, and assemble the stream around the kind's
    /// transformer.
    ///
    /// Nothing is sent on the stream's behalf: Alpaca needs no application-level pings, and its
    /// transformers send nothing, so the transformer is handed a sender nobody reads.
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
            transport: attachment,
            map: instrument_map,
            buffered_websocket_events,
        } = subscriber.subscribe(subscriptions).await?;

        // Always empty for this provider -- its streams fetch no initial snapshots -- but taken
        // through the generic path so a future kind that needs one is not silently ignored.
        let initial_snapshots = SnapFetcher::fetch_snapshots(subscriptions).await?;

        let (ws_sink_tx, _unread) = mpsc::unbounded_channel();

        let mut transformer =
            StreamTransformer::init(instrument_map, &initial_snapshots, ws_sink_tx).await?;

        // Empty for this provider -- see `AlpacaSubscriber::subscribe` -- but processed through
        // the generic path so the `Subscribed` contract holds whatever the subscriber hands back.
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
