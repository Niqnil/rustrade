//! Hyperliquid perpetual futures market data connector.
//!
//! Uses raw WebSocket connection to `wss://api.hyperliquid.xyz/ws` for market data streams.
//! Reuses `hyperliquid_rust_sdk` types for deserialization.
//!
//! # Supported Streams
//! - [`PublicTrades`](crate::subscription::trade::PublicTrades): Real-time trade feed
//! - [`OrderBooksL2`](crate::subscription::book::OrderBooksL2): L2 order book snapshots
//!
//! # Notes
//! - Market data streams are unauthenticated (public data)
//! - Execution client uses SDK's WebSocket separately for user data

use self::{
    book::HyperliquidL2Book, channel::HyperliquidChannel, market::HyperliquidMarket,
    subscription::HyperliquidSubResponse, trade::HyperliquidTrade,
};
use crate::{
    ExchangeWsStream, NoInitialSnapshots,
    exchange::{Connector, ExchangeSub, PingInterval, StreamSelector},
    instrument::InstrumentData,
    subscriber::{WebSocketSubscriber, validator::WebSocketSubValidator},
    subscription::{book::OrderBooksL2, trade::PublicTrades},
    transformer::stateless::StatelessTransformer,
};
use barter_instrument::exchange::ExchangeId;
use barter_integration::protocol::websocket::{WebSocketSerdeParser, WsMessage};
use barter_macro::{DeExchange, SerExchange};
use derive_more::Display;
use serde_json::json;
use std::time::Duration;
use url::Url;

pub mod book;
pub mod channel;
pub mod historical;
pub mod market;
pub mod subscription;
pub mod trade;

/// Hyperliquid mainnet WebSocket URL.
pub const BASE_URL_HYPERLIQUID: &str = "wss://api.hyperliquid.xyz/ws";

/// Ping interval for Hyperliquid WebSocket (50 seconds, matching SDK).
const PING_INTERVAL_SECS: u64 = 50;

/// Type alias for Hyperliquid WebSocket stream.
pub type HyperliquidWsStream<Transformer> = ExchangeWsStream<WebSocketSerdeParser, Transformer>;

/// Hyperliquid perpetual futures exchange connector.
#[derive(
    Copy,
    Clone,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    Hash,
    Debug,
    Default,
    Display,
    DeExchange,
    SerExchange,
)]
pub struct Hyperliquid;

impl Connector for Hyperliquid {
    const ID: ExchangeId = ExchangeId::HyperliquidPerp;
    type Channel = HyperliquidChannel;
    type Market = HyperliquidMarket;
    type Subscriber = WebSocketSubscriber;
    type SubValidator = WebSocketSubValidator;
    type SubResponse = HyperliquidSubResponse;

    fn url() -> Result<Url, url::ParseError> {
        Url::parse(BASE_URL_HYPERLIQUID)
    }

    fn ping_interval() -> Option<PingInterval> {
        Some(PingInterval {
            interval: tokio::time::interval(Duration::from_secs(PING_INTERVAL_SECS)),
            ping: || WsMessage::text(r#"{"method":"ping"}"#),
        })
    }

    fn requests(exchange_subs: Vec<ExchangeSub<Self::Channel, Self::Market>>) -> Vec<WsMessage> {
        exchange_subs
            .into_iter()
            .map(|ExchangeSub { channel, market }| {
                WsMessage::text(
                    json!({
                        "method": "subscribe",
                        "subscription": channel.subscription_payload(market.as_ref())
                    })
                    .to_string(),
                )
            })
            .collect()
    }
}

impl<Instrument> StreamSelector<Instrument, PublicTrades> for Hyperliquid
where
    Instrument: InstrumentData,
{
    type SnapFetcher = NoInitialSnapshots;
    type Stream = HyperliquidWsStream<
        StatelessTransformer<Self, Instrument::Key, PublicTrades, HyperliquidTrade>,
    >;
}

impl<Instrument> StreamSelector<Instrument, OrderBooksL2> for Hyperliquid
where
    Instrument: InstrumentData,
{
    type SnapFetcher = NoInitialSnapshots;
    type Stream = HyperliquidWsStream<
        StatelessTransformer<Self, Instrument::Key, OrderBooksL2, HyperliquidL2Book>,
    >;
}
