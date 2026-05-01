//! Hyperliquid market data connectors for perpetual futures and spot trading.
//!
//! Uses raw WebSocket connection to `wss://api.hyperliquid.xyz/ws` for market data streams.
//! Reuses `hyperliquid_rust_sdk` types for deserialization.
//!
//! # Connectors
//!
//! - `Hyperliquid`: Perpetual futures market data
//! - `HyperliquidSpot`: Spot trading market data
//!
//! # Supported Streams
//! - [`PublicTrades`](crate::subscription::trade::PublicTrades): Real-time trade feed
//! - [`OrderBooksL2`](crate::subscription::book::OrderBooksL2): L2 order book snapshots
//!
//! # Spot Market Subscriptions
//!
//! Hyperliquid spot uses `@{index}` format for WebSocket subscriptions.
//! Use `SpotMetaResolver` to convert pair names to indices:
//!
//! ```ignore
//! use rustrade_data::exchange::hyperliquid::{HyperliquidSpot, resolve_spot_pair};
//!
//! // Resolve pair name to @index format
//! let coin = resolve_spot_pair("hype", "usdc").await?; // "@107"
//!
//! // Subscribe using resolved index
//! let streams = Streams::<PublicTrades>::builder()
//!     .subscribe([(HyperliquidSpot, &coin, "usdc", MarketDataInstrumentKind::Spot, PublicTrades)])
//!     .init()
//!     .await?;
//! ```
//!
//! Or use `@index` directly if you already know the spot pair index.
//!
//! # Notes
//! - Market data streams are unauthenticated (public data)
//! - Execution client uses SDK's WebSocket separately for user data
//! - Spot and perps use the same WebSocket endpoint and protocol

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
use derive_more::Display;
use rustrade_instrument::exchange::ExchangeId;
use rustrade_integration::protocol::websocket::{WebSocketSerdeParser, WsMessage};
use rustrade_macro::{DeExchange, SerExchange};
use serde_json::json;
use std::time::Duration;
use url::Url;

pub mod book;
pub mod channel;
pub mod historical;
pub mod market;
pub mod spot_meta;
pub mod subscription;
pub mod trade;

pub use spot_meta::{SpotMetaResolver, resolve_spot_pair};

/// Hyperliquid mainnet WebSocket URL.
pub const BASE_URL_HYPERLIQUID: &str = "wss://api.hyperliquid.xyz/ws";

/// Ping interval for Hyperliquid WebSocket (50 seconds, matching SDK).
const PING_INTERVAL_SECS: u64 = 50;

/// Type alias for Hyperliquid WebSocket stream.
pub type HyperliquidWsStream<Transformer> = ExchangeWsStream<WebSocketSerdeParser, Transformer>;

/// Build WebSocket subscription messages for Hyperliquid channels.
fn build_subscribe_messages(
    exchange_subs: Vec<ExchangeSub<HyperliquidChannel, HyperliquidMarket>>,
) -> Vec<WsMessage> {
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
        build_subscribe_messages(exchange_subs)
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

/// Hyperliquid spot trading exchange connector.
///
/// Uses the same WebSocket endpoint and protocol as perpetuals, but with a different
/// `ExchangeId` for routing.
///
/// # Market Format
///
/// Spot markets use `@{index}` format for WebSocket subscriptions (e.g., `"@107"` for HYPE).
/// Get the spot index from the `spotMeta` API. Exception: PURR uses `"PURR/USDC"` literally.
///
/// Perpetuals use symbol-only format: `"BTC"`, `"ETH"`
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
pub struct HyperliquidSpot;

impl Connector for HyperliquidSpot {
    const ID: ExchangeId = ExchangeId::HyperliquidSpot;
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
        build_subscribe_messages(exchange_subs)
    }
}

impl<Instrument> StreamSelector<Instrument, PublicTrades> for HyperliquidSpot
where
    Instrument: InstrumentData,
{
    type SnapFetcher = NoInitialSnapshots;
    type Stream = HyperliquidWsStream<
        StatelessTransformer<Self, Instrument::Key, PublicTrades, HyperliquidTrade>,
    >;
}

impl<Instrument> StreamSelector<Instrument, OrderBooksL2> for HyperliquidSpot
where
    Instrument: InstrumentData,
{
    type SnapFetcher = NoInitialSnapshots;
    type Stream = HyperliquidWsStream<
        StatelessTransformer<Self, Instrument::Key, OrderBooksL2, HyperliquidL2Book>,
    >;
}
