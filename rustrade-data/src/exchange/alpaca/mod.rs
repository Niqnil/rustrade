//! Alpaca market data connectors for US equities and crypto.
//!
//! Connects to Alpaca's market data WebSocket streams:
//! - IEX: `wss://stream.data.alpaca.markets/v2/iex` (free, US equities)
//! - SIP: `wss://stream.data.alpaca.markets/v2/sip` (paid, consolidated tape)
//! - Crypto: `wss://stream.data.alpaca.markets/v1beta3/crypto/us`
//!
//! # Authentication
//!
//! Requires `APCA_API_KEY_ID` and `APCA_API_SECRET_KEY` environment variables.
//! Auth message is sent immediately after WebSocket connection, before subscriptions.
//!
//! # Connectors
//!
//! - [`AlpacaIex`]: Free IEX feed for US equities
//! - [`AlpacaSip`]: Paid consolidated SIP feed (untested — requires subscription)
//! - [`AlpacaCrypto`]: Crypto market data
//!
//! # Supported Streams
//!
//! - [`PublicTrades`](crate::subscription::trade::PublicTrades): Real-time trades
//! - [`Quotes`](crate::subscription::quote::Quotes): Real-time quotes (NBBO for equities, bid/ask for crypto)

use self::{
    channel::AlpacaChannel, market::AlpacaMarket, quote::AlpacaQuote,
    subscription::AlpacaSubResponse, trade::AlpacaTrade,
};
use crate::{
    ExchangeWsStream, NoInitialSnapshots,
    exchange::{Connector, ExchangeServer, ExchangeSub, StreamSelector},
    instrument::InstrumentData,
    subscriber::{
        mapper::SubscriptionMapper,
        validator::{SubscriptionValidator, WebSocketSubValidator},
    },
    subscription::{quote::Quotes, trade::PublicTrades},
    transformer::stateless::StatelessTransformer,
};
use async_trait::async_trait;
use futures::{SinkExt, StreamExt};
use rustrade_instrument::exchange::ExchangeId;
use rustrade_integration::{
    error::SocketError,
    protocol::websocket::{WebSocket, WebSocketSerdeParser, WsMessage, connect},
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::{env, fmt::Debug, marker::PhantomData, time::Duration};
use tracing::debug;
use url::Url;

pub mod channel;
pub mod market;
pub mod quote;
pub mod subscription;
pub mod trade;

/// IEX WebSocket URL (free US equities feed).
pub const WEBSOCKET_URL_IEX: &str = "wss://stream.data.alpaca.markets/v2/iex";

/// SIP WebSocket URL (paid consolidated US equities feed).
///
/// **Note**: Requires paid Alpaca market data subscription. This connector is
/// implemented but untested — use at your own risk.
pub const WEBSOCKET_URL_SIP: &str = "wss://stream.data.alpaca.markets/v2/sip";

/// Crypto WebSocket URL.
pub const WEBSOCKET_URL_CRYPTO: &str = "wss://stream.data.alpaca.markets/v1beta3/crypto/us";

/// Type alias for Alpaca WebSocket stream.
pub type AlpacaWsStream<Transformer> = ExchangeWsStream<WebSocketSerdeParser, Transformer>;

/// Alpaca IEX equities connector (free feed).
pub type AlpacaIex = Alpaca<AlpacaServerIex>;

/// Alpaca SIP equities connector (paid feed, untested).
pub type AlpacaSip = Alpaca<AlpacaServerSip>;

/// Alpaca crypto connector.
pub type AlpacaCrypto = Alpaca<AlpacaServerCrypto>;

/// Generic Alpaca market data connector.
///
/// Use type aliases [`AlpacaIex`], [`AlpacaSip`], or [`AlpacaCrypto`] for specific servers.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default)]
pub struct Alpaca<Server>(PhantomData<Server>);

/// IEX server (free US equities feed).
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default)]
pub struct AlpacaServerIex;

/// SIP server (paid consolidated US equities feed).
///
/// **Warning**: Requires paid Alpaca market data subscription. Untested.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default)]
pub struct AlpacaServerSip;

/// Crypto server.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default)]
pub struct AlpacaServerCrypto;

impl ExchangeServer for AlpacaServerIex {
    const ID: ExchangeId = ExchangeId::AlpacaIex;
    fn websocket_url() -> &'static str {
        WEBSOCKET_URL_IEX
    }
}

impl ExchangeServer for AlpacaServerSip {
    const ID: ExchangeId = ExchangeId::AlpacaSip;
    fn websocket_url() -> &'static str {
        WEBSOCKET_URL_SIP
    }
}

impl ExchangeServer for AlpacaServerCrypto {
    const ID: ExchangeId = ExchangeId::AlpacaCrypto;
    fn websocket_url() -> &'static str {
        WEBSOCKET_URL_CRYPTO
    }
}

impl<Server> Connector for Alpaca<Server>
where
    Server: ExchangeServer,
{
    const ID: ExchangeId = Server::ID;
    type Channel = AlpacaChannel;
    type Market = AlpacaMarket;
    type Subscriber = AlpacaSubscriber;
    type SubValidator = WebSocketSubValidator;
    type SubResponse = AlpacaSubResponse;

    fn url() -> Result<Url, url::ParseError> {
        Url::parse(Server::websocket_url())
    }

    fn requests(exchange_subs: Vec<ExchangeSub<Self::Channel, Self::Market>>) -> Vec<WsMessage> {
        let mut trades: Vec<&str> = Vec::new();
        let mut quotes: Vec<&str> = Vec::new();

        for sub in &exchange_subs {
            match sub.channel {
                AlpacaChannel::Trades => trades.push(sub.market.as_ref()),
                AlpacaChannel::Quotes => quotes.push(sub.market.as_ref()),
            }
        }

        let mut payload = json!({"action": "subscribe"});
        if !trades.is_empty() {
            payload["trades"] = json!(trades);
        }
        if !quotes.is_empty() {
            payload["quotes"] = json!(quotes);
        }

        vec![WsMessage::text(payload.to_string())]
    }

    fn expected_responses<InstrumentKey>(_map: &crate::subscription::Map<InstrumentKey>) -> usize {
        // Alpaca sends a single subscription confirmation for all symbols in one subscribe message
        1
    }
}

impl<Instrument, Server> StreamSelector<Instrument, PublicTrades> for Alpaca<Server>
where
    Instrument: InstrumentData,
    Server: ExchangeServer + Debug + Send + Sync,
{
    type SnapFetcher = NoInitialSnapshots;
    type Stream =
        AlpacaWsStream<StatelessTransformer<Self, Instrument::Key, PublicTrades, AlpacaTrade>>;
}

impl<Instrument, Server> StreamSelector<Instrument, Quotes> for Alpaca<Server>
where
    Instrument: InstrumentData,
    Server: ExchangeServer + Debug + Send + Sync,
{
    type SnapFetcher = NoInitialSnapshots;
    type Stream = AlpacaWsStream<StatelessTransformer<Self, Instrument::Key, Quotes, AlpacaQuote>>;
}

impl<'de, Server> Deserialize<'de> for Alpaca<Server>
where
    Server: ExchangeServer,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        let input = <String as Deserialize>::deserialize(deserializer)?;
        if input.as_str() == Self::ID.as_str() {
            Ok(Self::default())
        } else {
            Err(serde::de::Error::invalid_value(
                serde::de::Unexpected::Str(input.as_str()),
                &Self::ID.as_str(),
            ))
        }
    }
}

impl<Server> Serialize for Alpaca<Server>
where
    Server: ExchangeServer,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer,
    {
        serializer.serialize_str(Self::ID.as_str())
    }
}

/// Alpaca WebSocket subscriber with authentication.
///
/// Handles the auth → subscribe flow required by Alpaca market data streams.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Deserialize, Serialize)]
pub struct AlpacaSubscriber;

#[async_trait]
impl crate::subscriber::Subscriber for AlpacaSubscriber {
    type SubMapper = crate::subscriber::mapper::WebSocketSubMapper;

    async fn subscribe<Exchange, Instrument, Kind>(
        subscriptions: &[crate::subscription::Subscription<Exchange, Instrument, Kind>],
    ) -> Result<crate::subscriber::Subscribed<Instrument::Key>, SocketError>
    where
        Exchange: Connector + Send + Sync,
        Kind: crate::subscription::SubscriptionKind + Send + Sync,
        Instrument: InstrumentData,
        crate::subscription::Subscription<Exchange, Instrument, Kind>:
            crate::Identifier<Exchange::Channel> + crate::Identifier<Exchange::Market>,
    {
        let exchange = Exchange::ID;
        let url = Exchange::url()?;
        debug!(%exchange, %url, ?subscriptions, "subscribing to Alpaca WebSocket");

        let mut websocket = connect(url).await?;
        debug!(%exchange, "connected to Alpaca WebSocket, sending auth");

        alpaca_authenticate(&mut websocket).await?;
        debug!(%exchange, "Alpaca auth successful");

        let crate::subscription::SubscriptionMeta {
            instrument_map,
            ws_subscriptions,
        } = Self::SubMapper::map::<Exchange, Instrument, Kind>(subscriptions);

        for subscription in ws_subscriptions {
            debug!(%exchange, payload = ?subscription, "sending Alpaca subscription");
            websocket
                .send(subscription)
                .await
                .map_err(|error| SocketError::WebSocket(Box::new(error)))?;
        }

        let (map, buffered_websocket_events) = Exchange::SubValidator::validate::<
            Exchange,
            Instrument::Key,
            Kind,
        >(instrument_map, &mut websocket)
        .await?;

        debug!(%exchange, "Alpaca subscriptions confirmed");
        Ok(crate::subscriber::Subscribed {
            websocket,
            map,
            buffered_websocket_events,
        })
    }
}

/// Authenticate to Alpaca WebSocket using env credentials.
///
/// # Note
/// Reads `APCA_API_KEY_ID` and `APCA_API_SECRET_KEY` from environment.
/// This is a Phase 2 expedient — other market data connectors use unauthenticated
/// public streams. A proper credential-injection mechanism should be designed
/// when another authenticated market data connector is added.
// FIXME: Design credential-injection mechanism for Subscriber trait instead of env::var
async fn alpaca_authenticate(ws: &mut WebSocket) -> Result<(), SocketError> {
    let api_key = env::var("APCA_API_KEY_ID")
        .map_err(|e| SocketError::Subscribe(format!("APCA_API_KEY_ID: {e}")))?;
    let api_secret = env::var("APCA_API_SECRET_KEY")
        .map_err(|e| SocketError::Subscribe(format!("APCA_API_SECRET_KEY: {e}")))?;

    let auth_msg = json!({
        "action": "auth",
        "key": api_key,
        "secret": api_secret,
    })
    .to_string();

    ws.send(WsMessage::text(auth_msg))
        .await
        .map_err(|e| SocketError::WebSocket(Box::new(e)))?;

    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            match ws.next().await {
                Some(Ok(WsMessage::Text(text))) => {
                    if let Some(result) = check_alpaca_auth_response(text.as_str()) {
                        return result;
                    }
                }
                Some(Ok(WsMessage::Binary(bytes))) => {
                    if let Ok(text) = std::str::from_utf8(&bytes)
                        && let Some(result) = check_alpaca_auth_response(text)
                    {
                        return result;
                    }
                }
                Some(Err(e)) => {
                    return Err(SocketError::WebSocket(Box::new(e)));
                }
                None => {
                    return Err(SocketError::Subscribe(
                        "WebSocket closed before auth response".to_owned(),
                    ));
                }
                Some(Ok(WsMessage::Close(frame))) => {
                    return Err(SocketError::Subscribe(format!(
                        "WebSocket closed during auth: {frame:?}"
                    )));
                }
                _ => {}
            }
        }
    })
    .await
    .map_err(|_| SocketError::Subscribe("Alpaca auth timeout (10s)".to_owned()))?
}

fn check_alpaca_auth_response(text: &str) -> Option<Result<(), SocketError>> {
    #[derive(Deserialize)]
    struct AuthMsg<'a> {
        #[serde(rename = "T")]
        msg_type: &'a str,
        #[serde(default)]
        msg: Option<&'a str>,
    }

    // Alpaca sends messages as JSON arrays: [{"T":"success",...}]
    let messages: Vec<AuthMsg<'_>> = serde_json::from_str(text).ok()?;

    for msg in &messages {
        match msg.msg_type {
            "success" => return Some(Ok(())),
            "error" => {
                return Some(Err(SocketError::Subscribe(format!(
                    "Alpaca auth failed: {}",
                    msg.msg.unwrap_or("unknown error")
                ))));
            }
            _ => {}
        }
    }
    None
}
