//! WebSocket live streaming client for Massive real-time market data.
//!
//! Provides real-time market data streaming via [`MassiveLive`].
//!
//! # Architecture
//!
//! - **One connection per market**: Each `MassiveLive` instance connects to one market
//!   (stocks, crypto, forex, options). For multiple markets, create separate instances.
//! - **Multiple symbols per connection**: Subscribe to multiple symbols before calling `start()`.
//! - **No auto-reconnect**: Returns [`MassiveError::Disconnected`] on connection drop; consumer owns
//!   reconnection policy (backoff, credential refresh, dedup/fill-recovery).
//!
//! # Connection Limits
//!
//! Massive limits concurrent WebSocket connections per subscription tier:
//! - **Individual plan**: 1 connection per purchased product
//! - **Business plan**: 3 connections per purchased product
//!
//! See <https://massive.com/knowledge-base/article/how-many-massive-websocket-connections-can-i-use-at-one-time>
//!
//! # Symbol Formats (WebSocket vs REST)
//!
//! **Important**: WebSocket uses different symbol formats than REST API:
//!
//! | Market | REST Format | WebSocket Format |
//! |--------|-------------|------------------|
//! | Crypto | `X:BTCUSD` | `BTC-USD` |
//! | Forex | `C:EURUSD` | `EUR-USD` |
//! | Stocks | `AAPL` | `AAPL` |
//! | Options | `O:AAPL251219C00150000` | `O:AAPL251219C00150000` |
//!
//! # Keepalive
//!
//! The client sends ping frames every 20 seconds and checks for pong responses at each ping tick.
//! If no pong has been received since the last ping, the connection is considered dead and
//! `MassiveError::Disconnected` is returned. Effective timeout is up to 40 seconds (two ping intervals).
//!
//! # Example
//!
//! ```ignore
//! use rustrade_data::exchange::massive::{MassiveLive, Market, ChannelType};
//! use rustrade_instrument::exchange::ExchangeId;
//! use std::collections::HashMap;
//! use futures::StreamExt;
//!
//! // Map WebSocket symbols to your instrument keys
//! let instruments: HashMap<String, String> = [
//!     ("BTC-USD".to_string(), "btc-usd".to_string()),
//!     ("ETH-USD".to_string(), "eth-usd".to_string()),
//! ].into_iter().collect();
//!
//! let mut client = MassiveLive::from_env(
//!     Market::Crypto,
//!     ExchangeId::Massive,
//!     instruments,
//! )?;
//!
//! // Subscribe to channels (can call multiple times before start)
//! client.subscribe(&["BTC-USD", "ETH-USD"], ChannelType::Trade);
//! client.subscribe(&["BTC-USD"], ChannelType::Quote);
//!
//! // Start streaming (consumes client)
//! let mut stream = client.start().await?;
//!
//! while let Some(event) = stream.next().await {
//!     match event {
//!         Ok(market_event) => println!("{:?}", market_event),
//!         Err(e) => {
//!             eprintln!("Error: {}", e);
//!             // Consumer decides: reconnect, backoff, or exit
//!             break;
//!         }
//!     }
//! }
//! ```

use super::error::MassiveError;
use super::transformer::{WsMessage, parse_ws_message};
use crate::event::{DataKind, MarketEvent};
use chrono::{DateTime, Utc};
use futures::{SinkExt, Stream, StreamExt};
use rustrade_instrument::exchange::ExchangeId;
use serde::Serialize;
use std::collections::HashMap;
use std::env;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::time::{Instant, interval_at};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message};
use tracing::{debug, trace, warn};

const ENV_API_KEY: &str = "MASSIVE_API_KEY";

const PING_INTERVAL: Duration = Duration::from_secs(20);
const PONG_TIMEOUT: Duration = Duration::from_secs(19);

/// Market type for WebSocket connection.
///
/// Each market requires a separate WebSocket connection to a different endpoint.
/// Massive allows one simultaneous connection per cluster (market type).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Market {
    /// US equities (NYSE, NASDAQ, etc.)
    Stocks,
    /// Cryptocurrency pairs (BTC-USD, ETH-USD, etc.)
    Crypto,
    /// Foreign exchange pairs (EUR-USD, GBP-USD, etc.)
    Forex,
    /// US equity options
    Options,
}

impl Market {
    /// Get the WebSocket endpoint URL for this market.
    ///
    /// Massive operates on Polygon.io infrastructure; endpoints are unchanged from Polygon.
    fn ws_url(&self) -> &'static str {
        match self {
            Market::Stocks => "wss://socket.polygon.io/stocks",
            Market::Crypto => "wss://socket.polygon.io/crypto",
            Market::Forex => "wss://socket.polygon.io/forex",
            Market::Options => "wss://socket.polygon.io/options",
        }
    }
}

/// Channel types for WebSocket subscriptions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ChannelType {
    /// Tick-level trades
    /// - Stocks: `T.AAPL`
    /// - Crypto: `XT.BTC-USD`
    /// - Forex: Not available (use Quote instead)
    Trade,
    /// Quote updates / BBO
    /// - Stocks: `Q.AAPL`
    /// - Crypto: `XQ.BTC-USD`
    /// - Forex: `C.EUR-USD`
    Quote,
    /// Per-second aggregates
    /// - Stocks: `A.AAPL`
    /// - Crypto: `XA.BTC-USD`
    /// - Forex: `CA.EUR-USD`
    AggregateSecond,
    /// Per-minute aggregates
    /// - Stocks: `AM.AAPL`
    /// - Crypto: `XAM.BTC-USD`
    /// - Forex: `CAM.EUR-USD`
    AggregateMinute,
}

impl ChannelType {
    /// Get the channel prefix for a given market.
    ///
    /// Returns `None` if the channel type is not supported for the market
    /// (e.g., Trade for Forex).
    fn prefix(self, market: Market) -> Option<&'static str> {
        match (self, market) {
            // Stocks
            (ChannelType::Trade, Market::Stocks) => Some("T"),
            (ChannelType::Quote, Market::Stocks) => Some("Q"),
            (ChannelType::AggregateSecond, Market::Stocks) => Some("A"),
            (ChannelType::AggregateMinute, Market::Stocks) => Some("AM"),
            // Crypto
            (ChannelType::Trade, Market::Crypto) => Some("XT"),
            (ChannelType::Quote, Market::Crypto) => Some("XQ"),
            (ChannelType::AggregateSecond, Market::Crypto) => Some("XA"),
            (ChannelType::AggregateMinute, Market::Crypto) => Some("XAM"),
            // Forex (no trades channel)
            (ChannelType::Trade, Market::Forex) => None,
            (ChannelType::Quote, Market::Forex) => Some("C"),
            (ChannelType::AggregateSecond, Market::Forex) => Some("CA"),
            (ChannelType::AggregateMinute, Market::Forex) => Some("CAM"),
            // Options
            (ChannelType::Trade, Market::Options) => Some("T"),
            (ChannelType::Quote, Market::Options) => Some("Q"),
            (ChannelType::AggregateSecond, Market::Options) => Some("A"),
            (ChannelType::AggregateMinute, Market::Options) => Some("AM"),
        }
    }

    /// Build channel string for a symbol.
    ///
    /// Returns `None` if this channel type is not supported for the market.
    fn channel_for(self, market: Market, symbol: &str) -> Option<String> {
        let prefix = self.prefix(market)?;
        Some(format!("{}.{}", prefix, symbol))
    }
}

/// WebSocket message for authentication.
#[derive(Serialize)]
struct AuthMessage<'a> {
    action: &'static str,
    params: &'a str,
}

/// WebSocket message for subscription.
#[derive(Serialize)]
struct SubscribeMessage {
    action: &'static str,
    params: String,
}

/// Live streaming client for Massive WebSocket API.
///
/// Connects to Massive's WebSocket endpoint for real-time market data.
/// Each instance connects to one market (stocks, crypto, forex, options).
///
/// # Connection Limits
///
/// See module docs and <https://massive.com/knowledge-base/article/how-many-massive-websocket-connections-can-i-use-at-one-time>
pub struct MassiveLive<K> {
    api_key: String,
    market: Market,
    instruments: HashMap<String, K>,
    exchange: ExchangeId,
    subscriptions: Vec<String>,
    ws_url: String,
}

impl<K: std::fmt::Debug> std::fmt::Debug for MassiveLive<K> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MassiveLive")
            .field("api_key", &"[REDACTED]")
            .field("market", &self.market)
            .field("instruments", &self.instruments)
            .field("exchange", &self.exchange)
            .field("subscriptions", &self.subscriptions)
            .field("ws_url", &self.ws_url)
            .finish()
    }
}

impl<K> MassiveLive<K> {
    /// Create a new live client with explicit API key.
    ///
    /// # Arguments
    ///
    /// * `api_key` - Massive API key from <https://massive.com/dashboard/api-keys>
    /// * `market` - Market type to connect to
    /// * `exchange` - ExchangeId to tag events with
    /// * `instruments` - Map from WebSocket symbol strings to user's instrument keys
    ///
    /// # Symbol Formats
    ///
    /// Use WebSocket-native formats (different from REST API):
    /// - Crypto: `BTC-USD`, `ETH-USD` (hyphenated)
    /// - Forex: `EUR-USD`, `GBP-USD` (hyphenated)
    /// - Stocks: `AAPL`, `MSFT` (plain)
    /// - Options: `O:AAPL251219C00150000`
    pub fn new(
        api_key: impl Into<String>,
        market: Market,
        exchange: ExchangeId,
        instruments: HashMap<String, K>,
    ) -> Self {
        Self {
            api_key: api_key.into(),
            market,
            instruments,
            exchange,
            subscriptions: Vec::new(),
            ws_url: market.ws_url().to_string(),
        }
    }

    /// Create a new live client from `MASSIVE_API_KEY` environment variable.
    ///
    /// # Errors
    ///
    /// Returns error if `MASSIVE_API_KEY` is not set.
    pub fn from_env(
        market: Market,
        exchange: ExchangeId,
        instruments: HashMap<String, K>,
    ) -> Result<Self, MassiveError> {
        let api_key =
            env::var(ENV_API_KEY).map_err(|_| MassiveError::EnvVar { var: ENV_API_KEY })?;
        Ok(Self::new(api_key, market, exchange, instruments))
    }

    /// Override the WebSocket URL (useful for testing).
    #[must_use]
    pub fn with_ws_url(mut self, url: impl Into<String>) -> Self {
        self.ws_url = url.into();
        self
    }

    /// Get the market type for this client.
    pub fn market(&self) -> Market {
        self.market
    }

    /// Subscribe to channels for the given symbols.
    ///
    /// Can be called multiple times before `start()` to add more subscriptions.
    /// Subscriptions are batched and sent after authentication.
    ///
    /// # Arguments
    ///
    /// * `symbols` - Symbol identifiers in WebSocket format:
    ///   - Crypto: `BTC-USD`, `ETH-USD` (hyphenated)
    ///   - Forex: `EUR-USD`, `GBP-USD` (hyphenated)
    ///   - Stocks: `AAPL`, `MSFT` (plain)
    ///   - Options: `O:AAPL251219C00150000`
    /// * `channel_type` - Type of data to subscribe to
    ///
    /// # Notes
    ///
    /// If `channel_type` is not supported for this market (e.g., `Trade` for `Forex`),
    /// a warning is logged and the subscription is skipped.
    pub fn subscribe(&mut self, symbols: &[&str], channel_type: ChannelType) {
        for symbol in symbols {
            if let Some(channel) = channel_type.channel_for(self.market, symbol) {
                debug!(channel = %channel, "Adding subscription");
                self.subscriptions.push(channel);
            } else {
                warn!(
                    symbol = %symbol,
                    channel_type = ?channel_type,
                    market = ?self.market,
                    "Channel type not supported for this market, skipping"
                );
            }
        }
    }

    /// Get the list of pending subscriptions.
    pub fn subscriptions(&self) -> &[String] {
        &self.subscriptions
    }
}

impl<K: Clone + Send + 'static> MassiveLive<K> {
    /// Connect, authenticate, subscribe, and start streaming.
    ///
    /// Consumes `self` because the WebSocket connection requires exclusive access.
    /// The stream will emit events until the connection closes or an error occurs.
    ///
    /// # Returns
    ///
    /// A stream of `MarketEvent<K, DataKind>` where `DataKind` depends on the
    /// subscribed channels (Trade, OrderBookL1, or Candle).
    ///
    /// # Errors
    ///
    /// Returns error if:
    /// - WebSocket connection fails
    /// - Authentication fails
    /// - Subscription fails
    ///
    /// Stream items may contain `MassiveError::Disconnected` if:
    /// - Connection drops unexpectedly
    /// - Pong not received within one ping interval (checked at each 20s tick)
    pub async fn start(
        self,
    ) -> Result<impl Stream<Item = Result<MarketEvent<K, DataKind>, MassiveError>>, MassiveError>
    {
        debug!(url = %self.ws_url, market = ?self.market, "Connecting to Massive WebSocket");

        // Connect
        let (ws_stream, _response) = connect_async(&self.ws_url).await?;
        let (mut write, mut read) = ws_stream.split();

        // Consume the initial "connected" status frame before sending auth
        Self::consume_connected_frame(&mut read).await?;

        // Authenticate
        debug!("Sending auth message");
        let auth_msg = AuthMessage {
            action: "auth",
            params: &self.api_key,
        };
        // AuthMessage contains only &'static str and &str — serialization cannot fail
        #[allow(clippy::expect_used)]
        let auth_json = serde_json::to_string(&auth_msg).expect("infallible");
        write.send(Message::Text(auth_json.into())).await?;

        // Wait for auth response
        let auth_response = tokio::time::timeout(Duration::from_secs(10), read.next())
            .await
            .map_err(|_| MassiveError::Auth {
                message: "Auth response timeout".into(),
            })?
            .ok_or_else(|| MassiveError::Disconnected {
                reason: "Connection closed before auth response".into(),
            })??;

        Self::verify_auth_response(&auth_response)?;
        debug!("Authentication successful");

        // Subscribe to channels
        if self.subscriptions.is_empty() {
            warn!("start() called with no subscriptions; stream will receive no market data");
        }
        if !self.subscriptions.is_empty() {
            let params = self.subscriptions.join(",");
            debug!(channels = %params, "Subscribing to channels");

            let sub_msg = SubscribeMessage {
                action: "subscribe",
                params,
            };
            // SubscribeMessage contains only &'static str and String — serialization cannot fail
            #[allow(clippy::expect_used)]
            let sub_json = serde_json::to_string(&sub_msg).expect("infallible");
            write.send(Message::Text(sub_json.into())).await?;

            // Wait for subscription confirmation
            let sub_response = tokio::time::timeout(Duration::from_secs(10), read.next())
                .await
                .map_err(|_| MassiveError::Disconnected {
                    reason: "Subscription response timeout".into(),
                })?
                .ok_or_else(|| MassiveError::Disconnected {
                    reason: "Connection closed before subscription response".into(),
                })??;

            Self::verify_subscription_response(&sub_response)?;
            debug!("Subscription successful");
        }

        // Reunite the stream for the read loop
        let ws_stream = write
            .reunite(read)
            .map_err(|e| MassiveError::Disconnected {
                reason: format!("Failed to reunite WebSocket stream: {}", e),
            })?;

        // Create the event stream with ping/pong handling
        Ok(Self::create_event_stream(
            ws_stream,
            self.instruments,
            self.exchange,
        ))
    }

    /// Consume the initial "connected" status frame sent by the server on connect.
    ///
    /// Polygon/Massive sends `[{"ev":"status","status":"connected",...}]` immediately
    /// when a WebSocket connection is established, before the client sends auth.
    async fn consume_connected_frame(
        read: &mut futures::stream::SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
    ) -> Result<(), MassiveError> {
        let msg = tokio::time::timeout(Duration::from_secs(10), read.next())
            .await
            .map_err(|_| MassiveError::Disconnected {
                reason: "Timeout waiting for connected frame".into(),
            })?
            .ok_or_else(|| MassiveError::Disconnected {
                reason: "Connection closed before connected frame".into(),
            })??;

        match &msg {
            Message::Text(text) => {
                // Verify it's a "connected" status message
                if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(text)
                    && let Some(arr) = parsed.as_array()
                {
                    for m in arr {
                        if m.get("ev").and_then(|v| v.as_str()) == Some("status")
                            && m.get("status").and_then(|v| v.as_str()) == Some("connected")
                        {
                            debug!("Received connected frame");
                            return Ok(());
                        }
                    }
                }
                // Not a connected frame - unexpected but continue anyway
                debug!(msg = %text, "Unexpected initial frame (expected connected status)");
                Ok(())
            }
            Message::Close(frame) => Err(MassiveError::Disconnected {
                reason: frame
                    .as_ref()
                    .map(|f| f.reason.to_string())
                    .unwrap_or_else(|| "Connection closed".into()),
            }),
            _ => {
                // Intentional: unexpected non-text/non-close frames (e.g., Ping) are ignored;
                // the server drove an unusual handshake but we continue to auth
                debug!(?msg, "Unexpected message type before auth");
                Ok(())
            }
        }
    }

    /// Verify the authentication response.
    fn verify_auth_response(msg: &Message) -> Result<(), MassiveError> {
        match msg {
            Message::Text(text) => {
                // Parse status message: [{"ev":"status","status":"auth_success",...}]
                let parsed: serde_json::Value =
                    serde_json::from_str(text).map_err(|e| MassiveError::Auth {
                        message: format!("Failed to parse auth response: {}", e),
                    })?;

                // Response is an array of status messages
                let messages = parsed.as_array().ok_or_else(|| MassiveError::Auth {
                    message: "Auth response is not an array".into(),
                })?;

                for msg in messages {
                    if msg.get("ev").and_then(|v| v.as_str()) == Some("status") {
                        let status = msg.get("status").and_then(|v| v.as_str());
                        match status {
                            Some("auth_success") => return Ok(()),
                            Some("auth_failed") => {
                                let message = msg
                                    .get("message")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("Authentication failed");
                                return Err(MassiveError::Auth {
                                    message: message.into(),
                                });
                            }
                            _ => {}
                        }
                    }
                }

                Err(MassiveError::Auth {
                    message: format!("Unexpected auth response: {}", text),
                })
            }
            Message::Close(frame) => Err(MassiveError::Disconnected {
                reason: frame
                    .as_ref()
                    .map(|f| f.reason.to_string())
                    .unwrap_or_else(|| "Connection closed".into()),
            }),
            _ => Err(MassiveError::Auth {
                message: format!("Unexpected message type during auth: {:?}", msg),
            }),
        }
    }

    /// Verify the subscription response.
    fn verify_subscription_response(msg: &Message) -> Result<(), MassiveError> {
        match msg {
            Message::Text(text) => {
                // Parse status message: [{"ev":"status","status":"success","message":"subscribed to: ..."}]
                let parsed: serde_json::Value =
                    serde_json::from_str(text).map_err(|e| MassiveError::Disconnected {
                        reason: format!("Failed to parse subscription response: {}", e),
                    })?;

                let messages = parsed
                    .as_array()
                    .ok_or_else(|| MassiveError::Disconnected {
                        reason: "Subscription response is not an array".into(),
                    })?;

                for msg in messages {
                    if msg.get("ev").and_then(|v| v.as_str()) == Some("status") {
                        let status = msg.get("status").and_then(|v| v.as_str());
                        if status == Some("success") {
                            return Ok(());
                        }
                    }
                }

                // No success status found — observable failure over silent one
                Err(MassiveError::Disconnected {
                    reason: format!("Subscription failed: {}", text),
                })
            }
            Message::Close(frame) => Err(MassiveError::Disconnected {
                reason: frame
                    .as_ref()
                    .map(|f| f.reason.to_string())
                    .unwrap_or_else(|| "Connection closed".into()),
            }),
            _ => {
                // Observable failure over silent one: unexpected message types should error
                Err(MassiveError::Disconnected {
                    reason: format!("Unexpected message type during subscription: {:?}", msg),
                })
            }
        }
    }

    /// Create the event stream with ping/pong handling.
    ///
    /// # Note
    ///
    /// If the internal streaming task panics, the stream ends without emitting an error item.
    /// Panics in the streaming task are not expected under normal operation.
    fn create_event_stream(
        ws_stream: WebSocketStream<MaybeTlsStream<TcpStream>>,
        instruments: HashMap<String, K>,
        exchange: ExchangeId,
    ) -> impl Stream<Item = Result<MarketEvent<K, DataKind>, MassiveError>> {
        let (mut write, mut read) = ws_stream.split();

        // Channel for passing events from the read task
        let (tx, rx) = mpsc::unbounded_channel();

        // Spawn the read/ping task
        tokio::spawn(async move {
            // Skip the immediate first tick; first ping sent after PING_INTERVAL
            let mut ping_interval = interval_at(Instant::now() + PING_INTERVAL, PING_INTERVAL);
            let mut ping_sent_at: Option<Instant> = None;

            loop {
                tokio::select! {
                    // Send ping at regular intervals
                    _ = ping_interval.tick() => {
                        // Check if we're still waiting for a pong from the last ping
                        if let Some(sent_at) = ping_sent_at
                            && sent_at.elapsed() > PONG_TIMEOUT
                        {
                            let _ = tx.send(Err(MassiveError::Disconnected {
                                reason: "Pong timeout".into(),
                            }));
                            break;
                        }

                        trace!("Sending ping");
                        if let Err(e) = write.send(Message::Ping(vec![].into())).await {
                            let _ = tx.send(Err(MassiveError::Disconnected {
                                reason: format!("Failed to send ping: {}", e),
                            }));
                            break;
                        }
                        ping_sent_at = Some(Instant::now());
                    }

                    // Read incoming messages
                    msg = read.next() => {
                        match msg {
                            Some(Ok(Message::Text(text))) => {
                                // Sample time once per frame for all events
                                let time_received = Utc::now();
                                // Parse and emit market events
                                match parse_ws_message(&text) {
                                    Ok(messages) => {
                                        for ws_msg in messages {
                                            if let Some(event) = Self::ws_message_to_event(
                                                ws_msg,
                                                &instruments,
                                                exchange,
                                                time_received,
                                            ) && tx.send(Ok(event)).is_err() {
                                                // Receiver dropped
                                                return;
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        warn!(error = %e, "Failed to parse WebSocket message");
                                    }
                                }
                            }
                            Some(Ok(Message::Pong(_))) => {
                                trace!("Received pong");
                                ping_sent_at = None;
                            }
                            Some(Ok(Message::Ping(data))) => {
                                trace!("Received ping, sending pong");
                                if let Err(e) = write.send(Message::Pong(data)).await {
                                    let _ = tx.send(Err(MassiveError::Disconnected {
                                        reason: format!("Failed to send pong: {}", e),
                                    }));
                                    break;
                                }
                            }
                            Some(Ok(Message::Close(frame))) => {
                                let reason = frame
                                    .as_ref()
                                    .map(|f| f.reason.to_string())
                                    .unwrap_or_else(|| "Connection closed".into());
                                let _ = tx.send(Err(MassiveError::Disconnected { reason }));
                                break;
                            }
                            Some(Ok(Message::Binary(_))) => {
                                // Unexpected binary message, ignore
                                trace!("Received unexpected binary message");
                            }
                            Some(Ok(Message::Frame(_))) => {
                                // Raw frame, ignore
                            }
                            Some(Err(e)) => {
                                let _ = tx.send(Err(MassiveError::Disconnected {
                                    reason: e.to_string(),
                                }));
                                break;
                            }
                            None => {
                                let _ = tx.send(Err(MassiveError::Disconnected {
                                    reason: "WebSocket stream ended".into(),
                                }));
                                break;
                            }
                        }
                    }
                }
            }
        });

        // Convert the channel receiver into a stream
        futures::stream::unfold(rx, |mut rx| async move {
            rx.recv().await.map(|item| (item, rx))
        })
    }

    /// Convert a WebSocket message to a MarketEvent.
    fn ws_message_to_event(
        msg: WsMessage,
        instruments: &HashMap<String, K>,
        exchange: ExchangeId,
        time_received: DateTime<Utc>,
    ) -> Option<MarketEvent<K, DataKind>> {
        match msg {
            WsMessage::TradeStocks(trade) | WsMessage::TradeCrypto(trade) => {
                let instrument = instruments.get(&trade.symbol)?.clone();
                let (time_exchange, public_trade) = trade.into_public_trade();

                Some(MarketEvent {
                    time_exchange,
                    time_received,
                    exchange,
                    instrument,
                    kind: DataKind::Trade(public_trade),
                })
            }
            WsMessage::QuoteStocks(quote)
            | WsMessage::QuoteCrypto(quote)
            | WsMessage::QuoteForex(quote) => {
                let instrument = instruments.get(&quote.symbol)?.clone();
                let (time_exchange, l1) = quote.into_order_book_l1();

                Some(MarketEvent {
                    time_exchange,
                    time_received,
                    exchange,
                    instrument,
                    kind: DataKind::OrderBookL1(l1),
                })
            }
            WsMessage::AggSecondStocks(agg)
            | WsMessage::AggMinuteStocks(agg)
            | WsMessage::AggSecondCrypto(agg)
            | WsMessage::AggMinuteCrypto(agg)
            | WsMessage::AggSecondForex(agg)
            | WsMessage::AggMinuteForex(agg) => {
                let instrument = instruments.get(&agg.symbol)?.clone();
                let (time_exchange, candle) = agg.into_candle();

                Some(MarketEvent {
                    time_exchange,
                    time_received,
                    exchange,
                    instrument,
                    kind: DataKind::Candle(candle),
                })
            }
            WsMessage::Status(_) => {
                // Status messages are handled during auth/subscribe, not emitted as events
                None
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_market_ws_url() {
        assert_eq!(Market::Stocks.ws_url(), "wss://socket.polygon.io/stocks");
        assert_eq!(Market::Crypto.ws_url(), "wss://socket.polygon.io/crypto");
        assert_eq!(Market::Forex.ws_url(), "wss://socket.polygon.io/forex");
        assert_eq!(Market::Options.ws_url(), "wss://socket.polygon.io/options");
    }

    #[test]
    fn test_channel_type_prefix_stocks() {
        assert_eq!(ChannelType::Trade.prefix(Market::Stocks), Some("T"));
        assert_eq!(ChannelType::Quote.prefix(Market::Stocks), Some("Q"));
        assert_eq!(
            ChannelType::AggregateSecond.prefix(Market::Stocks),
            Some("A")
        );
        assert_eq!(
            ChannelType::AggregateMinute.prefix(Market::Stocks),
            Some("AM")
        );
    }

    #[test]
    fn test_channel_type_prefix_crypto() {
        assert_eq!(ChannelType::Trade.prefix(Market::Crypto), Some("XT"));
        assert_eq!(ChannelType::Quote.prefix(Market::Crypto), Some("XQ"));
        assert_eq!(
            ChannelType::AggregateSecond.prefix(Market::Crypto),
            Some("XA")
        );
        assert_eq!(
            ChannelType::AggregateMinute.prefix(Market::Crypto),
            Some("XAM")
        );
    }

    #[test]
    fn test_channel_type_prefix_forex() {
        // Forex has no trades channel
        assert_eq!(ChannelType::Trade.prefix(Market::Forex), None);
        assert_eq!(ChannelType::Quote.prefix(Market::Forex), Some("C"));
        assert_eq!(
            ChannelType::AggregateSecond.prefix(Market::Forex),
            Some("CA")
        );
        assert_eq!(
            ChannelType::AggregateMinute.prefix(Market::Forex),
            Some("CAM")
        );
    }

    #[test]
    fn test_channel_for() {
        // Crypto
        assert_eq!(
            ChannelType::Trade.channel_for(Market::Crypto, "BTC-USD"),
            Some("XT.BTC-USD".to_string())
        );
        assert_eq!(
            ChannelType::Quote.channel_for(Market::Crypto, "BTC-USD"),
            Some("XQ.BTC-USD".to_string())
        );
        assert_eq!(
            ChannelType::AggregateMinute.channel_for(Market::Crypto, "BTC-USD"),
            Some("XAM.BTC-USD".to_string())
        );

        // Stocks
        assert_eq!(
            ChannelType::Trade.channel_for(Market::Stocks, "AAPL"),
            Some("T.AAPL".to_string())
        );
        assert_eq!(
            ChannelType::Quote.channel_for(Market::Stocks, "AAPL"),
            Some("Q.AAPL".to_string())
        );
        assert_eq!(
            ChannelType::AggregateMinute.channel_for(Market::Stocks, "AAPL"),
            Some("AM.AAPL".to_string())
        );

        // Forex (no trades)
        assert_eq!(
            ChannelType::Trade.channel_for(Market::Forex, "EUR-USD"),
            None
        );
        assert_eq!(
            ChannelType::Quote.channel_for(Market::Forex, "EUR-USD"),
            Some("C.EUR-USD".to_string())
        );
    }

    #[test]
    fn test_subscribe_accumulates() {
        let instruments: HashMap<String, String> = HashMap::new();
        let mut client =
            MassiveLive::new("test_key", Market::Crypto, ExchangeId::Massive, instruments);

        client.subscribe(&["BTC-USD", "ETH-USD"], ChannelType::Trade);
        client.subscribe(&["BTC-USD"], ChannelType::Quote);

        assert_eq!(client.subscriptions().len(), 3);
        assert!(client.subscriptions().contains(&"XT.BTC-USD".to_string()));
        assert!(client.subscriptions().contains(&"XT.ETH-USD".to_string()));
        assert!(client.subscriptions().contains(&"XQ.BTC-USD".to_string()));
    }

    #[test]
    fn test_subscribe_skips_unsupported() {
        let instruments: HashMap<String, String> = HashMap::new();
        let mut client =
            MassiveLive::new("test_key", Market::Forex, ExchangeId::Massive, instruments);

        // Forex doesn't support trades
        client.subscribe(&["EUR-USD"], ChannelType::Trade);
        client.subscribe(&["EUR-USD"], ChannelType::Quote);

        // Only quote subscription should be added
        assert_eq!(client.subscriptions().len(), 1);
        assert!(client.subscriptions().contains(&"C.EUR-USD".to_string()));
    }

    #[test]
    fn test_from_env_missing() {
        temp_env::with_var_unset(ENV_API_KEY, || {
            let result: Result<MassiveLive<String>, _> =
                MassiveLive::from_env(Market::Crypto, ExchangeId::Massive, HashMap::new());
            assert!(matches!(result, Err(MassiveError::EnvVar { .. })));
        });
    }

    #[test]
    fn test_with_ws_url_override() {
        let instruments: HashMap<String, String> = HashMap::new();
        let client = MassiveLive::new("test_key", Market::Crypto, ExchangeId::Massive, instruments)
            .with_ws_url("wss://test.example.com/crypto");

        assert_eq!(client.ws_url, "wss://test.example.com/crypto");
    }

    // ========================================================================
    // Auth/Subscription Response Tests
    // ========================================================================

    #[test]
    fn test_verify_auth_response_success() {
        let msg = Message::Text(
            r#"[{"ev":"status","status":"auth_success","message":"authenticated"}]"#.into(),
        );
        assert!(MassiveLive::<String>::verify_auth_response(&msg).is_ok());
    }

    #[test]
    fn test_verify_auth_response_failed() {
        let msg = Message::Text(
            r#"[{"ev":"status","status":"auth_failed","message":"invalid api key"}]"#.into(),
        );
        let result = MassiveLive::<String>::verify_auth_response(&msg);
        assert!(matches!(result, Err(MassiveError::Auth { .. })));
    }

    #[test]
    fn test_verify_auth_response_close_frame() {
        let msg = Message::Close(Some(tokio_tungstenite::tungstenite::protocol::CloseFrame {
            code: tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode::Normal,
            reason: "server shutdown".into(),
        }));
        let result = MassiveLive::<String>::verify_auth_response(&msg);
        assert!(matches!(result, Err(MassiveError::Disconnected { .. })));
    }

    #[test]
    fn test_verify_auth_response_connected_status_is_unexpected() {
        // "connected" status should be consumed before auth is sent,
        // so if we see it here, it's unexpected and should error
        let msg = Message::Text(
            r#"[{"ev":"status","status":"connected","message":"Connected Successfully"}]"#.into(),
        );
        let result = MassiveLive::<String>::verify_auth_response(&msg);
        assert!(matches!(result, Err(MassiveError::Auth { .. })));
    }

    #[test]
    fn test_verify_subscription_response_success() {
        let msg = Message::Text(
            r#"[{"ev":"status","status":"success","message":"subscribed to: XT.BTC-USD"}]"#.into(),
        );
        assert!(MassiveLive::<String>::verify_subscription_response(&msg).is_ok());
    }

    #[test]
    fn test_verify_subscription_response_failure() {
        // No "success" status in response should now return error
        let msg = Message::Text(
            r#"[{"ev":"status","status":"error","message":"invalid symbol"}]"#.into(),
        );
        let result = MassiveLive::<String>::verify_subscription_response(&msg);
        assert!(matches!(result, Err(MassiveError::Disconnected { .. })));
    }

    #[test]
    fn test_verify_subscription_response_close_frame() {
        let msg = Message::Close(None);
        let result = MassiveLive::<String>::verify_subscription_response(&msg);
        assert!(matches!(result, Err(MassiveError::Disconnected { .. })));
    }
}
