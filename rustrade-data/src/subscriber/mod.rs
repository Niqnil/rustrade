use self::{
    mapper::{SubscriptionMapper, WebSocketSubMapper},
    validator::SubscriptionValidator,
};
use crate::{
    Identifier,
    exchange::Connector,
    instrument::InstrumentData,
    subscription::{Map, Subscription, SubscriptionKind, SubscriptionMeta},
};
use futures::SinkExt;
use rustrade_integration::{
    error::SocketError,
    protocol::websocket::{WebSocket, WsMessage, connect},
};
use serde::{Deserialize, Serialize};
use std::{fmt::Debug, future::Future};
use tracing::debug;

/// [`SubscriptionMapper`] implementations defining how to map a
/// collection of Barter [`Subscription`]s into exchange specific [`SubscriptionMeta`].
pub mod mapper;

/// [`SubscriptionValidator`] implementations defining how to
/// validate actioned [`Subscription`]s were successful.
pub mod validator;

/// Defines how to connect to a socket and subscribe to market data streams.
///
/// Subscribers may carry state such as authentication credentials.
/// The trait requires `Clone` to support reconnection (subscribers are cloned
/// into the reconnect closure).
pub trait Subscriber: Clone + Send + Sync {
    type SubMapper: SubscriptionMapper;

    /// What a successful subscribe hands the stream to read from.
    ///
    /// A [`WebSocket`] of its own for almost every exchange, and the only transport the standard
    /// [`MarketStream`](crate::MarketStream) initialisation accepts. A subscriber whose streams
    /// share one connection instead hands out a per-stream view of it, and pairs with a
    /// [`MarketStream`](crate::MarketStream) of its own that knows how to read that view.
    ///
    /// `Send`, because a stream's transport is moved into the task that drives it.
    type Transport: Send;

    fn subscribe<Exchange, Instrument, Kind>(
        &self,
        subscriptions: &[Subscription<Exchange, Instrument, Kind>],
    ) -> impl Future<Output = Result<Subscribed<Instrument::Key, Self::Transport>, SocketError>> + Send
    where
        Exchange: Connector + Send + Sync,
        Kind: SubscriptionKind + Send + Sync,
        Instrument: InstrumentData,
        Subscription<Exchange, Instrument, Kind>:
            Identifier<Exchange::Channel> + Identifier<Exchange::Market>;
}

/// The outcome of a successful [`Subscriber::subscribe`].
#[derive(Debug)]
pub struct Subscribed<InstrumentKey, Transport = WebSocket> {
    /// The connection, or view of one, that the subscribed events arrive on.
    pub transport: Transport,
    /// Each confirmed subscription's identifier, mapped to its instrument key.
    pub map: Map<InstrumentKey>,
    /// Frames that arrived during validation without being a confirmation, in arrival order —
    /// typically the first events of an already-confirmed subscription. They are the stream's first
    /// input and must be processed before anything read from `transport`.
    ///
    /// Raw WebSocket frames whatever the transport: a shared connection relays the frames it reads,
    /// so every stream parses its input the same way.
    pub buffered_websocket_events: Vec<WsMessage>,
}

/// Standard [`Subscriber`] for [`WebSocket`]s suitable for most exchanges.
///
/// This is a stateless subscriber for unauthenticated market data streams.
#[derive(
    Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default, Deserialize, Serialize,
)]
pub struct WebSocketSubscriber;

impl Subscriber for WebSocketSubscriber {
    type SubMapper = WebSocketSubMapper;
    type Transport = WebSocket;

    async fn subscribe<Exchange, Instrument, Kind>(
        &self,
        subscriptions: &[Subscription<Exchange, Instrument, Kind>],
    ) -> Result<Subscribed<Instrument::Key, Self::Transport>, SocketError>
    where
        Exchange: Connector + Send + Sync,
        Kind: SubscriptionKind + Send + Sync,
        Instrument: InstrumentData,
        Subscription<Exchange, Instrument, Kind>:
            Identifier<Exchange::Channel> + Identifier<Exchange::Market>,
    {
        // Define variables for logging ergonomics
        let exchange = Exchange::ID;
        let url = Exchange::url()?;
        debug!(%exchange, %url, ?subscriptions, "subscribing to WebSocket");

        // Connect to exchange
        let mut websocket = connect(url).await?;
        debug!(%exchange, ?subscriptions, "connected to WebSocket");

        // Map &[Subscription<Exchange, Kind>] to SubscriptionMeta
        let SubscriptionMeta {
            instrument_map,
            ws_subscriptions,
        } = Self::SubMapper::map::<Exchange, Instrument, Kind>(subscriptions);

        // Send Subscriptions over WebSocket
        for subscription in ws_subscriptions {
            debug!(%exchange, payload = ?subscription, "sending exchange subscription");
            websocket
                .send(subscription)
                .await
                .map_err(|error| SocketError::WebSocket(Box::new(error)))?;
        }

        // Validate Subscription responses
        let (map, buffered_websocket_events) = Exchange::SubValidator::validate::<
            Exchange,
            Instrument::Key,
            Kind,
        >(instrument_map, &mut websocket)
        .await?;

        debug!(%exchange, "successfully initialised WebSocket stream with confirmed Subscriptions");
        Ok(Subscribed {
            transport: websocket,
            map,
            buffered_websocket_events,
        })
    }
}
