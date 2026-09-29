//! Massive (formerly Polygon.io) market data connectors.
//!
//! Provides access to institutional-grade market data across all major asset classes:
//! stocks, options, indices, forex, crypto, and futures.
//!
//! # Data licensing — redistribution prohibited
//!
//! This crate's code is MIT-licensed; **the data it retrieves is not**. Massive's
//! [Market Data Terms of Service](https://massive.com/legal/market-data-terms-of-service)
//! (§5(c)) prohibit redistributing the data to third parties. Use it for your own purposes only,
//! and do not commit it, publish it, or serve it onward.
//!
//! # Testing Status
//!
//! **Partially integration-tested.** We have a currencies subscription (crypto + forex)
//! but not stocks, options, indices, or futures subscriptions. Verified endpoints:
//!
//! - ✅ REST aggregates (crypto, forex, stocks, options, futures via free tier)
//! - ✅ REST tick trades (crypto)
//! - ✅ REST quotes (forex)
//! - ✅ WebSocket streaming (crypto, forex)
//! - ✅ Reference data (tickers, exchanges, market status, holidays)
//! - ✅ Corporate actions (dividends, splits)
//! - ✅ Options contracts reference data (free tier)
//! - ❌ Options snapshots with Greeks — requires Options Starter subscription
//! - ❌ REST aggregates (indices) — requires indices subscription
//! - ❌ REST tick trades (stocks) — requires stocks subscription
//! - ❌ REST quotes (stocks) — requires stocks subscription
//! - ❌ WebSocket (stocks, options) — requires a stocks or options subscription
//!
//! Unit tests for JSON transformation live inline in `reference.rs`, `options.rs`,
//! `transformer.rs` and `message.rs`. Live API integration tests are in
//! `tests/massive_integration.rs`.
//!
//! # Architecture
//!
//! - [`MassiveRestClient`](crate::exchange::massive::MassiveRestClient): Historical and intraday
//!   data via REST API
//! - [`MassiveSubscriber`](crate::exchange::massive::MassiveSubscriber) with the
//!   [`MassiveStocks`](crate::exchange::massive::MassiveStocks),
//!   [`MassiveCrypto`](crate::exchange::massive::MassiveCrypto),
//!   [`MassiveForex`](crate::exchange::massive::MassiveForex) and
//!   [`MassiveOptions`](crate::exchange::massive::MassiveOptions) connectors: real-time streaming
//!   through [`Streams`](crate::streams::Streams), like any other exchange
//!
//! # Authentication
//!
//! Requires `MASSIVE_API_KEY` environment variable from an active Massive subscription.
//! Get your API key from: <https://massive.com/dashboard/api-keys>
//!
//! # One connection per cluster
//!
//! Massive allows a key a fixed number of WebSocket connections **per cluster** — stocks,
//! options, forex and crypto count separately — one on an individual plan. Past the limit it
//! closes the *older* connection. Every stream a
//! [`MassiveSubscriber`](crate::exchange::massive::MassiveSubscriber) and its clones open on a
//! cluster therefore shares one socket to it. Pass clones of one subscriber to every stream on
//! the key; see [`connection`](crate::exchange::massive::connection) for the details.
//!
//! # Symbol Conventions
//!
//! **Important**: REST API and WebSocket use different symbol formats!
//!
//! ## REST API Symbols
//!
//! - `X:BTCUSD` — Crypto
//! - `C:EURUSD` — Forex
//! - `O:AAPL251219C00150000` — Options
//! - `I:SPX` — Indices
//! - `AAPL` — Stocks
//!
//! ## WebSocket Symbols
//!
//! The connectors spell these from each instrument; see
//! [`MassiveSymbols`](crate::exchange::massive::market::MassiveSymbols).
//!
//! - `BTC-USD` — Crypto (hyphenated)
//! - `EUR/USD` — Forex (slashed: the cluster accepts `EUR-USD` too, but delivers its data as
//!   `EUR/USD`)
//! - `O:AAPL251219C00150000` — Options
//! - `AAPL` — Stocks
//!
//! # Supported Streams
//!
//! | Kind | Stocks / Options | Crypto | Forex |
//! |------|------------------|--------|-------|
//! | [`PublicTrades`](crate::subscription::trade::PublicTrades) | `T` | `XT` | — (no trades) |
//! | [`Quotes`](crate::subscription::quote::Quotes), [`OrderBooksL1`](crate::subscription::book::OrderBooksL1) | `Q` | `XQ` | `C` |
//! | [`Candles`](crate::subscription::candle::Candles) at `Sec1` | `A` | `XAS` | `CAS` |
//! | [`Candles`](crate::subscription::candle::Candles) at `Min1` | `AM` | `XA` | `CA` |
//!
//! Massive aggregates at no other interval, so a `Candles` subscription at any other is refused
//! before anything is sent. Forex quotes carry no sizes, so their amounts are zero, and forex
//! candles report no volume: Massive builds them from quote updates, not trades. Only crypto's
//! trade conditions say which side took liquidity, so a stock or option trade has no side.
//!
//! # Examples
//!
//! ## REST Client
//!
//! ```ignore
//! use rustrade_data::exchange::massive::MassiveRestClient;
//!
//! let client = MassiveRestClient::from_env()?;
//! let candles = client.fetch_aggregates("X:BTCUSD", 1, "minute", from, to).await?;
//! ```
//!
//! ## WebSocket Streams
//!
//! ```ignore
//! use rustrade_data::exchange::massive::{MassiveCrypto, MassiveSubscriber};
//! use rustrade_data::streams::Streams;
//! use rustrade_data::subscription::trade::PublicTrades;
//! use rustrade_instrument::instrument::market_data::kind::MarketDataInstrumentKind;
//!
//! // Load the key at construction time (fails fast if the variable is missing)
//! let subscriber = MassiveSubscriber::from_env()?;
//!
//! let streams = Streams::<PublicTrades>::builder()
//!     .subscribe(
//!         subscriber,
//!         [(MassiveCrypto::default(), "btc", "usd", MarketDataInstrumentKind::Spot, PublicTrades)],
//!     )
//!     .init()
//!     .await?;
//! ```

use self::{
    channel::MassiveChannel,
    connection::{AttachRequest, MassiveAttachment, MassiveConnections, Slot},
    market::{MassiveMarket, MassiveServer, MassiveSymbols, MassiveTradeServer},
    message::MassiveTransformer,
    stream::MassiveStream,
};
use crate::{
    Identifier, NoInitialSnapshots,
    exchange::{Connector, ExchangeServer, ExchangeSub, StreamSelector},
    instrument::InstrumentData,
    subscriber::{
        Subscribed, Subscriber, mapper::SubscriptionMapper, validator::WebSocketSubValidator,
    },
    subscription::{
        Subscription, SubscriptionKind, SubscriptionMeta,
        book::OrderBooksL1,
        candle::{CandleInterval, Candles},
        quote::Quotes,
        trade::PublicTrades,
    },
};
use fnv::FnvHashSet;
use rustrade_instrument::exchange::ExchangeId;
use rustrade_integration::{Validator, error::SocketError, protocol::websocket::WsMessage};
use serde::{Deserialize, Serialize};
use smol_str::SmolStr;
use std::{env, fmt, fmt::Debug, marker::PhantomData, sync::Arc};
use tracing::debug;
use url::Url;

pub mod channel;
pub mod connection;
pub mod market;
pub mod message;
pub mod stream;

// Side-effect-only module: provides `impl StockSplitSource for MassiveRestClient`. It exports no
// types, so it is private — `use` it nowhere; the impl is in scope wherever the client is.
mod corporate_action;
mod error;
pub(crate) mod options;
mod pagination;
pub(crate) mod reference;
pub(crate) mod rest;
pub(crate) mod transformer;

pub use error::MassiveError;
pub use options::{
    MassiveOptionContract, MassiveOptionSnapshot, OptionContractQuery, OptionDayBar, OptionQuote,
    OptionSnapshotQuery, OptionTrade, UnderlyingAsset,
};
pub use reference::{
    Address, CurrencyStatus, Dividend, DividendFrequency, DividendQuery, Exchange, MarketHoliday,
    MarketStatus, SortOrder, SplitQuery, StockSplit, Ticker, TickerDetails, TickerQuery,
};
pub use rest::MassiveRestClient;
pub use transformer::FairMarketValue;

/// Stocks WebSocket URL.
pub const WEBSOCKET_URL_STOCKS: &str = "wss://socket.massive.com/stocks";

/// Crypto WebSocket URL.
pub const WEBSOCKET_URL_CRYPTO: &str = "wss://socket.massive.com/crypto";

/// Forex WebSocket URL.
pub const WEBSOCKET_URL_FOREX: &str = "wss://socket.massive.com/forex";

/// Options WebSocket URL.
pub const WEBSOCKET_URL_OPTIONS: &str = "wss://socket.massive.com/options";

/// Massive stocks connector.
pub type MassiveStocks = Massive<MassiveServerStocks>;

/// Massive crypto connector.
pub type MassiveCrypto = Massive<MassiveServerCrypto>;

/// Massive forex connector. Forex publishes no trades, so it serves no [`PublicTrades`].
pub type MassiveForex = Massive<MassiveServerForex>;

/// Massive options connector (untested — requires an options subscription).
pub type MassiveOptions = Massive<MassiveServerOptions>;

/// Generic Massive WebSocket connector, one per cluster.
///
/// Use the aliases [`MassiveStocks`], [`MassiveCrypto`], [`MassiveForex`] or [`MassiveOptions`].
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default)]
pub struct Massive<Server>(PhantomData<Server>);

/// The stocks cluster.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default)]
pub struct MassiveServerStocks;

/// The crypto cluster.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default)]
pub struct MassiveServerCrypto;

/// The forex cluster.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default)]
pub struct MassiveServerForex;

/// The options cluster.
///
/// **Warning**: requires an options subscription. Untested.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default)]
pub struct MassiveServerOptions;

impl ExchangeServer for MassiveServerStocks {
    const ID: ExchangeId = ExchangeId::MassiveStocks;
    fn websocket_url() -> &'static str {
        WEBSOCKET_URL_STOCKS
    }
}

impl ExchangeServer for MassiveServerCrypto {
    const ID: ExchangeId = ExchangeId::MassiveCrypto;
    fn websocket_url() -> &'static str {
        WEBSOCKET_URL_CRYPTO
    }
}

impl ExchangeServer for MassiveServerForex {
    const ID: ExchangeId = ExchangeId::MassiveForex;
    fn websocket_url() -> &'static str {
        WEBSOCKET_URL_FOREX
    }
}

impl ExchangeServer for MassiveServerOptions {
    const ID: ExchangeId = ExchangeId::MassiveOptions;
    fn websocket_url() -> &'static str {
        WEBSOCKET_URL_OPTIONS
    }
}

impl MassiveServer for MassiveServerStocks {
    const SYMBOLS: MassiveSymbols = MassiveSymbols::Ticker;
    const QUOTES: &'static str = "Q";
    const SECOND_AGGREGATES: &'static str = "A";
    const MINUTE_AGGREGATES: &'static str = "AM";
}

impl MassiveTradeServer for MassiveServerStocks {
    const TRADES: &'static str = "T";
}

impl MassiveServer for MassiveServerCrypto {
    const SYMBOLS: MassiveSymbols = MassiveSymbols::CryptoPair;
    const QUOTES: &'static str = "XQ";
    const SECOND_AGGREGATES: &'static str = "XAS";
    const MINUTE_AGGREGATES: &'static str = "XA";
}

impl MassiveTradeServer for MassiveServerCrypto {
    const TRADES: &'static str = "XT";
}

impl MassiveServer for MassiveServerForex {
    const SYMBOLS: MassiveSymbols = MassiveSymbols::CurrencyPair;
    const QUOTES: &'static str = "C";
    const SECOND_AGGREGATES: &'static str = "CAS";
    const MINUTE_AGGREGATES: &'static str = "CA";
}

impl MassiveServer for MassiveServerOptions {
    const SYMBOLS: MassiveSymbols = MassiveSymbols::OptionContract;
    const QUOTES: &'static str = "Q";
    const SECOND_AGGREGATES: &'static str = "A";
    const MINUTE_AGGREGATES: &'static str = "AM";
}

impl MassiveTradeServer for MassiveServerOptions {
    const TRADES: &'static str = "T";
}

impl<Server> Connector for Massive<Server>
where
    Server: MassiveServer,
{
    const ID: ExchangeId = Server::ID;
    type Channel = MassiveChannel;
    type Market = MassiveMarket;
    type Subscriber = MassiveSubscriber;
    // Never called: `MassiveSubscriber` confirms each subscribe on the connection it shares.
    type SubValidator = WebSocketSubValidator;
    type SubResponse = MassiveSubResponse;

    fn url() -> Result<Url, url::ParseError> {
        Url::parse(Server::websocket_url())
    }

    /// The payload a socket of its own would be subscribed with.
    ///
    /// [`MassiveSubscriber`] subscribes through its shared connection instead, which alone knows
    /// what the socket already holds; both build their payloads with the same function, so they
    /// cannot disagree about its shape.
    fn requests(exchange_subs: Vec<ExchangeSub<Self::Channel, Self::Market>>) -> Vec<WsMessage> {
        let slots = exchange_subs.iter().map(Slot::from).collect::<Vec<_>>();
        vec![connection::action("subscribe", &slots)]
    }

    // `expected_responses` is deliberately left at its default. `MassiveSubscriber` confirms a
    // subscribe by the subscriptions Massive names, not by a response count.
}

impl<Instrument, Server> StreamSelector<Instrument, PublicTrades> for Massive<Server>
where
    Instrument: InstrumentData,
    Server: MassiveTradeServer + Debug + Send + Sync,
{
    type SnapFetcher = NoInitialSnapshots;
    type Stream = MassiveStream<MassiveTransformer<Self, Instrument::Key, PublicTrades>>;
}

impl<Instrument, Server> StreamSelector<Instrument, Quotes> for Massive<Server>
where
    Instrument: InstrumentData,
    Server: MassiveServer + Debug + Send + Sync,
{
    type SnapFetcher = NoInitialSnapshots;
    type Stream = MassiveStream<MassiveTransformer<Self, Instrument::Key, Quotes>>;
}

/// The quotes channel, as a top of book. See [`OrderBooksL1`]'s
/// [`MassiveKind`](message::MassiveKind) implementation for how a quote maps onto one.
impl<Instrument, Server> StreamSelector<Instrument, OrderBooksL1> for Massive<Server>
where
    Instrument: InstrumentData,
    Server: MassiveServer + Debug + Send + Sync,
{
    type SnapFetcher = NoInitialSnapshots;
    type Stream = MassiveStream<MassiveTransformer<Self, Instrument::Key, OrderBooksL1>>;
}

/// Per-second and per-minute aggregates. Any other interval is refused when subscribing.
impl<Instrument, Server> StreamSelector<Instrument, Candles> for Massive<Server>
where
    Instrument: InstrumentData,
    Server: MassiveServer + Debug + Send + Sync,
{
    type SnapFetcher = NoInitialSnapshots;
    type Stream = MassiveStream<MassiveTransformer<Self, Instrument::Key, Candles>>;
}

impl<'de, Server> Deserialize<'de> for Massive<Server>
where
    Server: MassiveServer,
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

impl<Server> Serialize for Massive<Server>
where
    Server: MassiveServer,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer,
    {
        serializer.serialize_str(Self::ID.as_str())
    }
}

/// A Massive frame of status messages, as a subscribe is answered.
///
/// [`MassiveSubscriber`] reads Massive's answers on the connection it shares, so this is
/// consulted only by a caller validating a socket of its own.
#[derive(Clone, PartialEq, Eq, Debug, Deserialize, Serialize)]
pub struct MassiveSubResponse(pub Vec<MassiveStatus>);

/// A Massive status message: `{"ev":"status","status":"success","message":"subscribed to: XT.BTC-USD"}`.
#[derive(Clone, PartialEq, Eq, Debug, Deserialize, Serialize)]
pub struct MassiveStatus {
    /// `success`, `error`, `max_connections`, `auth_failed` and so on.
    pub status: SmolStr,
    #[serde(default)]
    pub message: String,
}

impl Validator for MassiveSubResponse {
    type Error = SocketError;

    /// Fails on the first status that is not a success.
    fn validate(self) -> Result<Self, SocketError> {
        match self.0.iter().find(|status| {
            !matches!(
                status.status.as_str(),
                "success" | "connected" | "auth_success"
            )
        }) {
            Some(MassiveStatus { status, message }) => Err(SocketError::Subscribe(format!(
                "Massive refused the subscription: {status}: {message}"
            ))),
            None => Ok(self),
        }
    }
}

/// The API key authenticating to Massive's WebSocket.
///
/// `Debug` is implemented manually to redact the key, preventing accidental exposure of it in
/// tracing or panic output.
#[derive(Clone)]
pub struct MassiveCredentials {
    api_key: String,
}

impl fmt::Debug for MassiveCredentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MassiveCredentials")
            .field("api_key", &"[REDACTED]")
            .finish()
    }
}

impl MassiveCredentials {
    /// Create credentials from an explicit key.
    pub fn new(api_key: impl Into<String>) -> Self {
        Self {
            api_key: api_key.into(),
        }
    }

    /// Load the key from the `MASSIVE_API_KEY` environment variable.
    ///
    /// # Errors
    ///
    /// Returns [`SocketError::Subscribe`] if the variable is not set.
    pub fn from_env() -> Result<Self, SocketError> {
        env::var(rest::ENV_API_KEY)
            .map(Self::new)
            .map_err(|error| SocketError::Subscribe(format!("{}: {error}", rest::ENV_API_KEY)))
    }

    pub(super) fn api_key(&self) -> &str {
        &self.api_key
    }
}

/// Massive market data subscriber: authenticates, and shares one connection per cluster among
/// every stream it and its clones open.
///
/// # One connection per cluster, shared by clones
/// Massive allows a key a fixed number of WebSocket connections per cluster — one on an
/// individual plan — and past it closes the **older** connection. So every stream this subscriber
/// opens on a cluster attaches to one socket to it: trades, quotes and candles, any number of
/// markets, in as many `subscribe` calls as the caller likes. **Pass clones of one subscriber** to
/// every stream on the key. A subscriber built separately, or another process using the key,
/// opens a connection of its own and evicts this one. The eviction ends every stream on it, and
/// the warning logged for the lost connection quotes Massive's `max_connections` status. Streams
/// on different clusters run side by side, one socket each.
///
/// Each subscription — a channel and a market — is sent once however many streams hold it, and
/// unsubscribed when the last stream holding it is dropped; the socket closes once none remains.
///
/// If the connection is lost, every stream on it ends together and reconnects through the usual
/// reconnect wrapper, sharing one new connection. Massive replays nothing, so what it published
/// while no socket was open is not recovered.
///
/// See [`connection`] for how frames reach each stream, and [`MassiveAttachment`] for why a
/// stream must be kept drained.
///
/// # Example
///
/// ```ignore
/// use rustrade_data::exchange::massive::{MassiveCredentials, MassiveSubscriber};
///
/// // Load the key at construction time (fails fast if the variable is missing)
/// let subscriber = MassiveSubscriber::from_env()?;
///
/// // Or with an explicit key
/// let subscriber = MassiveSubscriber::new(MassiveCredentials::new("key"));
/// ```
#[derive(Clone, Debug)]
pub struct MassiveSubscriber {
    connections: Arc<MassiveConnections>,
}

impl MassiveSubscriber {
    /// Create a new subscriber with the provided credentials.
    ///
    /// The subscriber opens no connection until a stream subscribes.
    pub fn new(credentials: MassiveCredentials) -> Self {
        Self {
            connections: Arc::new(MassiveConnections::new(credentials)),
        }
    }

    /// Create a new subscriber using the key in the `MASSIVE_API_KEY` environment variable.
    ///
    /// Equivalent to `MassiveSubscriber::new(MassiveCredentials::from_env()?)`.
    pub fn from_env() -> Result<Self, SocketError> {
        Ok(Self::new(MassiveCredentials::from_env()?))
    }
}

impl Subscriber for MassiveSubscriber {
    type SubMapper = crate::subscriber::mapper::WebSocketSubMapper;
    type Transport = MassiveAttachment;

    /// Attach the batch to its cluster's shared connection, subscribing whatever the connection
    /// does not already hold.
    ///
    /// Returns once Massive confirms every requested subscription by name. One another stream
    /// already holds is not sent again, and needs no answer.
    ///
    /// # Errors
    /// Returns [`SocketError::Subscribe`] — before connecting — if the batch is empty, asks for
    /// candles at an interval other than one second or one minute, or on the options cluster
    /// names an instrument that is not an option contract. Once connected, it returns one if
    /// authentication is refused (a key without WebSocket access to the cluster among the
    /// reasons), Massive refuses a subscription, or the subscription timeout passes before every
    /// subscription is confirmed. Massive does not answer a subscription to a channel the cluster
    /// does not publish, so that surfaces as the timeout, naming what was never confirmed.
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
        debug!(%exchange, %url, ?subscriptions, "subscribing to Massive WebSocket");

        // Every subscription in a batch shares one `Kind`, and a batch has a slot per subscription.
        let Some(kind) = subscriptions
            .first()
            .map(|subscription| subscription.kind.as_str())
        else {
            return Err(SocketError::Subscribe(format!(
                "no subscriptions were given to subscribe to on {exchange}"
            )));
        };
        let slots = requested_slots(exchange, subscriptions)?;

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
                kind,
                slots,
                timeout: Exchange::subscription_timeout(),
            })
            .await?;

        debug!(%exchange, "attached to the Massive connection");
        Ok(Subscribed {
            transport,
            map: instrument_map,
            // The connection routes every frame for the batch into `transport` from the moment it
            // is registered, so nothing is read ahead of it.
            buffered_websocket_events: Vec::new(),
        })
    }
}

/// The distinct subscriptions a batch requests, in request order, or why the batch cannot be
/// subscribed.
fn requested_slots<Exchange, Instrument, Kind>(
    exchange: ExchangeId,
    subscriptions: &[Subscription<Exchange, Instrument, Kind>],
) -> Result<Vec<Slot>, SocketError>
where
    Exchange: Connector,
    Kind: SubscriptionKind,
    Subscription<Exchange, Instrument, Kind>:
        Identifier<Exchange::Channel> + Identifier<Exchange::Market>,
{
    let mut seen = FnvHashSet::default();
    let mut slots = Vec::with_capacity(subscriptions.len());

    for subscription in subscriptions {
        let sub = ExchangeSub::<Exchange::Channel, Exchange::Market>::new(subscription);

        // A candle interval Massive does not aggregate at spells no channel.
        if sub.channel.as_ref().is_empty() {
            return Err(SocketError::Subscribe(format!(
                "{exchange} has no channel for {:?}: Massive aggregates per second \
                 (CandleInterval::{:?}) and per minute (CandleInterval::{:?}) only",
                subscription.kind,
                CandleInterval::Sec1,
                CandleInterval::Min1,
            )));
        }

        if exchange == ExchangeId::MassiveOptions
            && !market::is_option_contract(sub.market.as_ref())
        {
            return Err(SocketError::Subscribe(format!(
                "{exchange} streams option contracts only, and {} is not one",
                sub.market.as_ref()
            )));
        }

        let slot = Slot::from(&sub);
        if seen.insert(slot.clone()) {
            slots.push(slot);
        }
    }

    Ok(slots)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use rustrade_instrument::instrument::market_data::{
        MarketDataInstrument, kind::MarketDataInstrumentKind,
    };

    fn subscription<Server, Kind>(
        base: &str,
        quote: &str,
        kind: Kind,
    ) -> Subscription<Massive<Server>, MarketDataInstrument, Kind>
    where
        Server: MassiveServer,
    {
        Subscription::new(
            Massive::default(),
            MarketDataInstrument::new(base, quote, MarketDataInstrumentKind::Spot),
            kind,
        )
    }

    #[test]
    fn each_kind_reads_its_cluster_channel() {
        let slots =
            |subscriptions: &[Subscription<MassiveCrypto, MarketDataInstrument, Candles>]| {
                requested_slots(ExchangeId::MassiveCrypto, subscriptions)
                    .unwrap()
                    .into_iter()
                    .map(|slot| slot.to_string())
                    .collect::<Vec<_>>()
            };

        assert_eq!(
            slots(&[
                subscription(
                    "btc",
                    "usd",
                    Candles {
                        interval: CandleInterval::Sec1
                    }
                ),
                subscription(
                    "btc",
                    "usd",
                    Candles {
                        interval: CandleInterval::Min1
                    }
                ),
                subscription(
                    "btc",
                    "usd",
                    Candles {
                        interval: CandleInterval::Sec1
                    }
                ),
            ]),
            ["XAS.BTC-USD", "XA.BTC-USD"],
        );

        let forex = requested_slots(
            ExchangeId::MassiveForex,
            &[subscription::<MassiveServerForex, _>("eur", "usd", Quotes)],
        )
        .unwrap();
        assert_eq!(forex[0].to_string(), "C.EUR/USD");
    }

    #[test]
    fn an_interval_massive_does_not_aggregate_at_is_refused_naming_it() {
        let error = requested_slots(
            ExchangeId::MassiveCrypto,
            &[subscription::<MassiveServerCrypto, _>(
                "btc",
                "usd",
                Candles {
                    interval: CandleInterval::Min5,
                },
            )],
        )
        .unwrap_err()
        .to_string();

        assert!(error.contains("Min5"), "{error}");
        assert!(error.contains("Sec1") && error.contains("Min1"), "{error}");
    }

    #[test]
    fn the_options_cluster_refuses_an_instrument_that_is_not_a_contract() {
        let error = requested_slots(
            ExchangeId::MassiveOptions,
            &[subscription::<MassiveServerOptions, _>(
                "aapl", "usd", Quotes,
            )],
        )
        .unwrap_err()
        .to_string();

        assert!(error.contains("option contracts only"), "{error}");
    }

    #[test]
    fn a_status_frame_validates_only_when_every_status_succeeded() {
        let response = |json: &str| serde_json::from_str::<MassiveSubResponse>(json).unwrap();

        assert!(
            response(
                r#"[{"ev":"status","status":"success","message":"subscribed to: XT.BTC-USD"}]"#
            )
            .validate()
            .is_ok()
        );

        let refused = response(
            r#"[{"ev":"status","status":"success","message":"subscribed to: XT.BTC-USD"},
                {"ev":"status","status":"error","message":"not authorized"}]"#,
        )
        .validate()
        .unwrap_err()
        .to_string();
        assert!(refused.contains("not authorized"), "{refused}");
    }

    #[test]
    fn credentials_debug_redacts_the_key() {
        let rendered = format!("{:?}", MassiveCredentials::new("secret-key"));
        assert!(!rendered.contains("secret-key"), "{rendered}");
    }

    #[test]
    fn a_connector_round_trips_through_its_exchange_id() {
        let json = serde_json::to_string(&MassiveForex::default()).unwrap();
        assert_eq!(json, r#""massive_forex""#);
        assert_eq!(
            serde_json::from_str::<MassiveForex>(&json).unwrap(),
            MassiveForex::default()
        );
        assert!(serde_json::from_str::<MassiveCrypto>(&json).is_err());
    }
}
