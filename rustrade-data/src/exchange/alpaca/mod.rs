//! Alpaca market data connectors for US equities and crypto.
//!
//! Connects to Alpaca's market data WebSocket streams:
//! - IEX: `wss://stream.data.alpaca.markets/v2/iex` (free, US equities)
//! - SIP: `wss://stream.data.alpaca.markets/v2/sip` (paid, consolidated tape)
//! - Crypto: `wss://stream.data.alpaca.markets/v1beta3/crypto/us`
//!
//! # Testing Status
//!
//! **Tested locally, CI planned (free tier — paper trading allowed):**
//! - Crypto streaming: trades and quotes (24/7)
//! - IEX equity streaming: trades and quotes (market hours only)
//!
//! **NOT tested (requires Algo Trader Plus subscription):**
//! - SIP equity streaming — implemented but unverified against real endpoints
//!
//! # Authentication
//!
//! Alpaca requires authentication via [`AlpacaCredentials`](crate::exchange::alpaca::AlpacaCredentials). Credentials can be:
//! - Loaded from environment variables via [`AlpacaCredentials::from_env()`](crate::exchange::alpaca::AlpacaCredentials::from_env)
//! - Provided explicitly via [`AlpacaCredentials::new()`](crate::exchange::alpaca::AlpacaCredentials::new)
//!
//! Auth message is sent immediately after WebSocket connection, before subscriptions.
//!
//! # One connection per feed
//!
//! Alpaca allows an account one market data connection per feed and refuses a second with
//! `connection limit exceeded`. Every stream an
//! [`AlpacaSubscriber`](crate::exchange::alpaca::AlpacaSubscriber) and its clones open on a feed
//! shares one socket to it, so trades and quotes — any number of symbols, over any number of
//! `subscribe` calls — can stream together. Pass clones of one subscriber to every stream on the
//! account; see [`AlpacaSubscriber`](crate::exchange::alpaca::AlpacaSubscriber) and
//! [`connection`](crate::exchange::alpaca::connection) for the details.
//!
//! # Example
//!
//! ```ignore
//! use rustrade_data::exchange::alpaca::{AlpacaCredentials, AlpacaSubscriber, AlpacaIex};
//! use rustrade_data::streams::Streams;
//! use rustrade_data::subscription::trade::PublicTrades;
//!
//! // Load credentials at construction time (fails fast if env vars missing)
//! let subscriber = AlpacaSubscriber::from_env()?;
//!
//! let streams = Streams::<PublicTrades>::builder()
//!     .subscribe(subscriber, [(AlpacaIex::default(), "AAPL", "USD", PublicTrades)])
//!     .init()
//!     .await?;
//! ```
//!
//! # Connectors
//!
//! - [`AlpacaIex`](crate::exchange::alpaca::AlpacaIex): Free IEX feed for US equities
//! - [`AlpacaSip`](crate::exchange::alpaca::AlpacaSip): Paid consolidated SIP feed (untested — requires subscription)
//! - [`AlpacaCrypto`](crate::exchange::alpaca::AlpacaCrypto): Crypto market data
//!
//! # Supported Streams
//!
//! - [`PublicTrades`](crate::subscription::trade::PublicTrades): Real-time trades
//! - [`Quotes`](crate::subscription::quote::Quotes): Real-time quotes (NBBO for equities, bid/ask for crypto)
//!
//! # Subscription confirmation
//!
//! Alpaca answers a subscribe with one frame naming every symbol the connection holds.
//! Initialisation does not return until **every** requested symbol has been named; if any is
//! still outstanding when the subscription timeout expires, the subscribe fails and names what was
//! missing. A partial subscription is therefore an error rather than a quietly reduced stream. The
//! [`AlpacaSubscriber`](crate::exchange::alpaca::AlpacaSubscriber) confirms each subscribe on the
//! connection it shares.
//!
//! **A confirmed symbol is not a promise of prompt data.** Alpaca's crypto feed publishes a quote
//! when top-of-book changes, so the delay before a given symbol first ticks is large and highly
//! variable -- 1s, 15s, 96s and 132s for four symbols confirmed on one connection in a single
//! 300s window. Callers must not infer a failed subscription from silence, and must not use the
//! first event as a readiness signal for any particular instrument.

use self::{
    channel::AlpacaChannel,
    connection::{AlpacaAttachment, AlpacaConnections, AttachRequest, Slot},
    market::AlpacaMarket,
    quote::AlpacaQuoteTransformer,
    stream::AlpacaStream,
    subscription::AlpacaSubResponse,
    trade::AlpacaTradeTransformer,
    validator::AlpacaWebSocketSubValidator,
};
use crate::{
    Identifier, NoInitialSnapshots,
    exchange::{Connector, ExchangeServer, ExchangeSub, StreamSelector},
    instrument::InstrumentData,
    subscriber::{Subscribed, Subscriber, mapper::SubscriptionMapper},
    subscription::{
        Subscription, SubscriptionKind, SubscriptionMeta, quote::Quotes, trade::PublicTrades,
    },
};
use fnv::FnvHashSet;
use futures::{SinkExt, StreamExt};
use rustrade_instrument::exchange::ExchangeId;
use rustrade_integration::{
    error::SocketError,
    protocol::websocket::{WebSocket, WsMessage},
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use smol_str::SmolStr;
use std::{env, fmt, fmt::Debug, marker::PhantomData, sync::Arc, time::Duration};
use tracing::debug;
use url::Url;

pub mod channel;
pub mod connection;
pub mod market;
pub mod options;
pub mod quote;
pub mod reference;
pub mod rest;
pub mod stream;
pub mod subscription;
pub mod trade;
pub mod validator;

// `StockSplitSource` adapter for `AlpacaRestClient` (no public items of its own — just the impl).
mod corporate_action;
// Shared page-cap + `page_token` cycle guard for the paginated REST fetches.
mod pagination;

pub use reference::{AlpacaStockSplit, CorporateActionsQuery};
pub use rest::{AlpacaRestClient, AlpacaRestError};

/// IEX WebSocket URL (free US equities feed).
pub const WEBSOCKET_URL_IEX: &str = "wss://stream.data.alpaca.markets/v2/iex";

/// SIP WebSocket URL (paid consolidated US equities feed).
///
/// **Note**: Requires paid Alpaca market data subscription. This connector is
/// implemented but untested — use at your own risk.
pub const WEBSOCKET_URL_SIP: &str = "wss://stream.data.alpaca.markets/v2/sip";

/// Crypto WebSocket URL.
pub const WEBSOCKET_URL_CRYPTO: &str = "wss://stream.data.alpaca.markets/v1beta3/crypto/us";

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
    type SubValidator = AlpacaWebSocketSubValidator;
    type SubResponse = AlpacaSubResponse;

    fn url() -> Result<Url, url::ParseError> {
        Url::parse(Server::websocket_url())
    }

    /// The payload a socket of its own would be subscribed with.
    ///
    /// [`AlpacaSubscriber`] subscribes through its shared connection instead, which alone knows
    /// what the socket already holds; both build their payloads with the same function, so they
    /// cannot disagree about its shape.
    fn requests(exchange_subs: Vec<ExchangeSub<Self::Channel, Self::Market>>) -> Vec<WsMessage> {
        vec![channel_message(
            "subscribe",
            exchange_subs
                .iter()
                .map(|sub| (sub.channel, sub.market.as_ref())),
        )]
    }

    // `expected_responses` is deliberately left at its default. `AlpacaSubscriber` confirms a
    // subscribe on coverage of the requested subscriptions rather than on a response count, so
    // no count it could return would be consulted.
}

impl<Instrument, Server> StreamSelector<Instrument, PublicTrades> for Alpaca<Server>
where
    Instrument: InstrumentData,
    Server: ExchangeServer + Debug + Send + Sync,
{
    type SnapFetcher = NoInitialSnapshots;
    type Stream = AlpacaStream<AlpacaTradeTransformer<Self, Instrument::Key>>;
}

impl<Instrument, Server> StreamSelector<Instrument, Quotes> for Alpaca<Server>
where
    Instrument: InstrumentData,
    Server: ExchangeServer + Debug + Send + Sync,
{
    type SnapFetcher = NoInitialSnapshots;
    type Stream = AlpacaStream<AlpacaQuoteTransformer<Self, Instrument::Key>>;
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

/// Credentials for authenticating to Alpaca market data WebSocket.
///
/// `Debug` is implemented manually to redact `api_secret`, preventing accidental
/// exposure of the secret in tracing or panic output.
#[derive(Clone)]
pub struct AlpacaCredentials {
    api_key: String,
    api_secret: String,
}

impl fmt::Debug for AlpacaCredentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AlpacaCredentials")
            .field("api_key", &self.api_key)
            .field("api_secret", &"[REDACTED]")
            .finish()
    }
}

impl AlpacaCredentials {
    /// Create credentials from explicit values.
    pub fn new(api_key: impl Into<String>, api_secret: impl Into<String>) -> Self {
        Self {
            api_key: api_key.into(),
            api_secret: api_secret.into(),
        }
    }

    /// Load credentials from environment variables.
    ///
    /// Reads `ALPACA_API_KEY` and `ALPACA_SECRET_KEY` from environment.
    ///
    /// # Errors
    ///
    /// Returns error if either environment variable is not set.
    pub fn from_env() -> Result<Self, SocketError> {
        let api_key = env::var("ALPACA_API_KEY")
            .map_err(|e| SocketError::Subscribe(format!("ALPACA_API_KEY: {e}")))?;
        let api_secret = env::var("ALPACA_SECRET_KEY")
            .map_err(|e| SocketError::Subscribe(format!("ALPACA_SECRET_KEY: {e}")))?;
        Ok(Self {
            api_key,
            api_secret,
        })
    }
}

/// Alpaca market data subscriber: authenticates, and shares one connection per feed among every
/// stream it and its clones open.
///
/// # One connection per feed, shared by clones
/// Alpaca allows an account one market data connection per feed — crypto, IEX and SIP count
/// separately — and refuses a second on the same feed with `connection limit exceeded`. So every
/// stream this subscriber opens on a feed attaches to one socket to it: trades and quotes, any
/// number of symbols, in as many `subscribe` calls as the caller likes. **Pass clones of one
/// subscriber** to every stream on the account. A subscriber built separately opens a connection
/// of its own, and Alpaca refuses it once another is open on that feed. Streams on different feeds
/// run side by side, one socket each.
///
/// Each `(channel, symbol)` pair is subscribed once however many streams hold it, and unsubscribed
/// when the last stream holding it is dropped; the socket closes once none remains. Alpaca caps the
/// pairs one connection holds — 30 on the free IEX plan, as last measured — and a subscribe that
/// would pass the cap fails naming it. The cap counts what every stream on the feed holds.
///
/// If the connection is lost, every stream on it ends together and reconnects through the usual
/// reconnect wrapper, sharing one new connection. Alpaca replays nothing, so what it published
/// while no socket was open is not recovered.
///
/// See [`connection`] for how frames reach each stream, and [`AlpacaAttachment`] for why a stream
/// must be kept drained.
///
/// # Example
///
/// ```ignore
/// use rustrade_data::exchange::alpaca::{AlpacaCredentials, AlpacaSubscriber};
///
/// // Load credentials at construction time (fails fast if env vars missing)
/// let subscriber = AlpacaSubscriber::from_env()?;
///
/// // Or with explicit credentials
/// let subscriber = AlpacaSubscriber::new(AlpacaCredentials::new("key", "secret"));
/// ```
#[derive(Clone, Debug)]
pub struct AlpacaSubscriber {
    connections: Arc<AlpacaConnections>,
}

impl AlpacaSubscriber {
    /// Create a new subscriber with the provided credentials.
    ///
    /// The subscriber opens no connection until a stream subscribes.
    pub fn new(credentials: AlpacaCredentials) -> Self {
        Self {
            connections: Arc::new(AlpacaConnections::new(credentials)),
        }
    }

    /// Create a new subscriber using credentials from environment variables.
    ///
    /// Equivalent to `AlpacaSubscriber::new(AlpacaCredentials::from_env()?)`.
    /// See [`AlpacaCredentials::from_env`](crate::exchange::alpaca::AlpacaCredentials::from_env) for the variables read and error conditions.
    pub fn from_env() -> Result<Self, SocketError> {
        Ok(Self::new(AlpacaCredentials::from_env()?))
    }
}

impl Subscriber for AlpacaSubscriber {
    type SubMapper = crate::subscriber::mapper::WebSocketSubMapper;
    type Transport = AlpacaAttachment;

    /// Attach the batch to its feed's shared connection, subscribing whatever the connection does
    /// not already hold.
    ///
    /// Returns once Alpaca reports holding every requested `(channel, symbol)` pair. A pair
    /// another stream already holds is not sent again, and needs no answer.
    ///
    /// # Errors
    /// Returns [`SocketError::Subscribe`] if the batch is empty, authentication is refused — by
    /// the credentials, or because another connection holds the feed — Alpaca refuses the
    /// subscribe (the connection's pair cap among the reasons), or the subscription timeout passes
    /// before every pair is confirmed.
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
        let exchange = Exchange::ID;
        let url = Exchange::url()?;
        debug!(%exchange, %url, ?subscriptions, "subscribing to Alpaca WebSocket");

        let slots = requested_slots(exchange, subscriptions)?;
        if slots.is_empty() {
            return Err(SocketError::Subscribe(format!(
                "no subscriptions were given to subscribe to on {exchange}"
            )));
        }

        // Only the instrument map is taken from the mapper. The subscribe payload is built by the
        // connection instead, because it alone knows what the socket already holds.
        let SubscriptionMeta {
            instrument_map,
            ws_subscriptions: _,
        } = Self::SubMapper::map::<Exchange, Instrument, Kind>(subscriptions);

        let transport = self
            .connections
            .attach(AttachRequest {
                exchange,
                url,
                slots,
                timeout: Exchange::subscription_timeout(),
            })
            .await?;

        debug!(%exchange, "attached to the Alpaca connection");
        Ok(Subscribed {
            transport,
            map: instrument_map,
            // The connection routes every frame for the batch into `transport` from the moment it
            // is registered, so nothing is read ahead of it.
            buffered_websocket_events: Vec::new(),
        })
    }
}

/// The distinct `(channel, symbol)` pairs a batch requests, in request order.
fn requested_slots<Exchange, Instrument, Kind>(
    exchange: ExchangeId,
    subscriptions: &[Subscription<Exchange, Instrument, Kind>],
) -> Result<Vec<Slot>, SocketError>
where
    Exchange: Connector,
    Subscription<Exchange, Instrument, Kind>:
        Identifier<Exchange::Channel> + Identifier<Exchange::Market>,
{
    let mut seen = FnvHashSet::default();
    let mut slots = Vec::with_capacity(subscriptions.len());

    for subscription in subscriptions {
        let sub = ExchangeSub::<Exchange::Channel, Exchange::Market>::new(subscription);
        let Some(channel) = AlpacaChannel::from_name(sub.channel.as_ref()) else {
            return Err(SocketError::Subscribe(format!(
                "{exchange} has no Alpaca channel named {}",
                sub.channel.as_ref()
            )));
        };

        let slot = Slot {
            channel,
            symbol: SmolStr::new(sub.market.as_ref()),
        };
        if seen.insert(slot.clone()) {
            slots.push(slot);
        }
    }

    Ok(slots)
}

/// Build a `subscribe` or `unsubscribe` payload for `pairs`, grouped by channel.
fn channel_message<'a>(
    action: &str,
    pairs: impl IntoIterator<Item = (AlpacaChannel, &'a str)>,
) -> WsMessage {
    let mut trades: Vec<&str> = Vec::new();
    let mut quotes: Vec<&str> = Vec::new();

    for (channel, symbol) in pairs {
        match channel {
            AlpacaChannel::Trades => trades.push(symbol),
            AlpacaChannel::Quotes => quotes.push(symbol),
        }
    }

    let mut payload = json!({"action": action});
    if !trades.is_empty() {
        payload["trades"] = json!(trades);
    }
    if !quotes.is_empty() {
        payload["quotes"] = json!(quotes);
    }

    WsMessage::text(payload.to_string())
}

/// Authenticate to Alpaca WebSocket using the provided credentials.
async fn alpaca_authenticate(
    ws: &mut WebSocket,
    credentials: &AlpacaCredentials,
) -> Result<(), SocketError> {
    let auth_msg = json!({
        "action": "auth",
        "key": credentials.api_key,
        "secret": credentials.api_secret,
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
    // On connect, Alpaca sends [{"T":"success","msg":"connected"}]
    // After auth, Alpaca sends [{"T":"success","msg":"authenticated"}]
    // We must wait for "authenticated", not just any "success"
    let messages: Vec<AuthMsg<'_>> = serde_json::from_str(text).ok()?;

    for msg in &messages {
        match (msg.msg_type, msg.msg) {
            ("success", Some("authenticated")) => return Some(Ok(())),
            ("error", _) => {
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
