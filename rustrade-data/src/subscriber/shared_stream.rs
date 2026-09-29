//! The market stream of every provider whose streams share one connection.
//!
//! Such a provider hands each stream a view of the shared connection rather than a socket of its
//! own — see [`Subscriber::Transport`] — which the standard WebSocket initialisation, built to
//! split and drive a socket, cannot read.
//! [`SharedStream`](crate::subscriber::shared_stream::SharedStream) reads that view instead, and
//! parses and transforms exactly as the standard stream does.

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

/// One stream's view of a connection it shares with other streams: the raw frames carrying its own
/// subscriptions, in the order the socket delivered them, and whatever the connection handed it on
/// attaching.
///
/// Implemented by each sharing provider's attachment, such as `AlpacaAttachment`. Sealed: only
/// this crate's providers share a connection.
pub trait SharedTransport:
    Stream<Item = Result<WsMessage, WsError>> + Unpin + Send + sealed::Sealed
{
    /// What the connection hands a stream on attaching, for its transformer: `()` for a provider
    /// with nothing to hand over.
    type Attached: Send;

    /// Take what the connection handed the stream on attaching. Called once, before any frame is
    /// read.
    fn take_attached(&mut self) -> Self::Attached;
}

pub(crate) mod sealed {
    /// Seals [`SharedTransport`](super::SharedTransport).
    pub trait Sealed {}
}

/// A transformer that takes what its stream's connection handed over on attaching.
///
/// Every transformer takes `()`, the nothing most providers hand over. A provider handing over
/// more implements it for its own transformer, which receives it before any frame is processed.
pub trait AttachedTransformer<Attached> {
    /// Take what the connection handed the stream on attaching.
    fn attached(&mut self, attached: Attached);
}

impl<T> AttachedTransformer<()> for T {
    fn attached(&mut self, (): ()) {}
}

/// The market stream every provider sharing a connection among its streams is served over.
///
/// Parses and transforms exactly as the standard WebSocket stream does. It differs in what it
/// reads — its [`SharedTransport`] view of the subscriber's shared connection — and in handing the
/// transformer what the connection handed the stream on attaching.
///
/// Nothing is sent on the stream's behalf: the connection alone writes to the shared socket, so
/// the transformer is handed a sender nobody reads.
// The bounds are on the struct rather than only its impls because the inner `ExchangeStream`
// carries them on its own definition; there is no way to name the field type without them.
pub struct SharedStream<Transport, StreamTransformer>
where
    Transport: SharedTransport,
    StreamTransformer: Transformer,
    WebSocketSerdeParser: StreamParser<StreamTransformer::Input>,
{
    inner: ExchangeStream<WebSocketSerdeParser, Transport, StreamTransformer>,
}

// Written by hand: a derive would demand `Debug` of the transformer's output and error types
// through the buffered events, which says nothing useful about the stream.
impl<Transport, StreamTransformer> std::fmt::Debug for SharedStream<Transport, StreamTransformer>
where
    Transport: SharedTransport,
    StreamTransformer: Transformer,
    WebSocketSerdeParser: StreamParser<StreamTransformer::Input>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharedStream")
            .field("buffered", &self.inner.buffer.len())
            .finish_non_exhaustive()
    }
}

impl<Transport, StreamTransformer> Stream for SharedStream<Transport, StreamTransformer>
where
    Transport: SharedTransport,
    StreamTransformer: Transformer,
    WebSocketSerdeParser: StreamParser<StreamTransformer::Input>,
    ExchangeStream<WebSocketSerdeParser, Transport, StreamTransformer>:
        Stream<Item = Result<StreamTransformer::Output, StreamTransformer::Error>> + Unpin,
{
    type Item = Result<StreamTransformer::Output, StreamTransformer::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // The transport is `Unpin` and `MarketStream` requires `Unpin` regardless, so the inner
        // stream can be re-pinned without a projection.
        Pin::new(&mut self.get_mut().inner).poll_next(cx)
    }
}

impl<Exchange, Instrument, Kind, Transport, StreamTransformer>
    MarketStream<Exchange, Instrument, Kind> for SharedStream<Transport, StreamTransformer>
where
    Exchange: Connector + Send + Sync,
    Exchange::Subscriber: Subscriber<Transport = Transport>,
    Transport: SharedTransport,
    Instrument: InstrumentData,
    Kind: SubscriptionKind + Send + Sync,
    Kind::Event: Send + Sync,
    StreamTransformer: ExchangeTransformer<Exchange, Instrument::Key, Kind>
        + AttachedTransformer<Transport::Attached>
        + Send,
    WebSocketSerdeParser:
        StreamParser<StreamTransformer::Input, Message = WsMessage, Error = WsError>,
{
    /// Attach to the subscriber's shared connection, and assemble the stream around the kind's
    /// transformer.
    ///
    /// What the connection handed the stream on attaching reaches the transformer **before** any
    /// frame is processed. The connection routes a batch's frames into its attachment from the
    /// moment the batch is registered, so frames can already be waiting there when the attach
    /// returns; handing it over any later would let them past it.
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

        // Taken through the generic path, though no sharing provider fetches initial snapshots
        // today, so a future kind that needs one is not silently ignored.
        let initial_snapshots = SnapFetcher::fetch_snapshots(subscriptions).await?;

        let (ws_sink_tx, _unread) = mpsc::unbounded_channel();

        let mut transformer =
            StreamTransformer::init(instrument_map, &initial_snapshots, ws_sink_tx).await?;
        transformer.attached(attachment.take_attached());

        // Empty for a sharing provider, whose connection routes frames into the attachment rather
        // than reading ahead of it, but processed through the generic path so the `Subscribed`
        // contract holds whatever the subscriber hands back.
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
