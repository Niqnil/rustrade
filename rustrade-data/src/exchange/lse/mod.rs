//! London Strategic Edge market data (behind the `lse` feature) — ⚠️ the retrieved data is **not
//! redistributable**.
//!
//! A free, no-account market-data provider covering FX, equities, ETFs, crypto, commodities,
//! indices, futures and options, plus reference and macroeconomic series.
//!
//! # ⚠️ Licensing — the data is NOT redistributable
//! This integration's **code** is MIT-licensed like the rest of this repository. **The data it
//! retrieves is not.** London Strategic Edge permits use for your own research, trading and model
//! training — including commercially — but **prohibits redistributing, reselling, or otherwise
//! making the data available to third parties**, in bulk or through any competing feed, download
//! service or interface. Their own client library being MIT-licensed covers *that client only* and
//! confers no rights in the data; the same split applies here.
//!
//! In practice: do not commit retrieved data to a public repository, do not publish it as fixtures
//! or example datasets, and do not re-serve it. Terms: <https://londonstrategicedge.com/terms>
//!
//! # Data characteristics
//! Properties that will silently mislead if assumed away:
//!
//! - **FX candles are BID candles — not mid, and not last.** Reconciling a day of `EUR/USD`
//!   one-minute bars against the tick tape for the same day, open/high/low/close matched the
//!   **bid** series on 1421 of 1421 minutes and matched the mid or the ask on none. So a backtest
//!   that fills at the candle close is filling at the bid — systematically favourable by a full
//!   spread on every buy, in the provider's deepest dataset. This cannot be corrected here without
//!   inventing a spread. Equity candles, by contrast, track the trade tape; the asymmetry is real
//!   and per-dataset.
//! - **Candle volume is not a dependable figure.** For FX the vault omits the field entirely, which
//!   is modelled as `None` rather than a zero — a zero would aggregate into a legitimate-looking
//!   total at every derived resolution. (The provider's other host reports a volume for the same
//!   bar; this integration uses the vault, which does not.) Where the field *is* published it is
//!   still unreliable: a majority of sampled one-minute equity bars reported `0` in minutes the
//!   tick tape shows real trades, and one daily series carried a contiguous band roughly 2,000×
//!   too large. A literal `0` is passed through as `Some(0)`; rewriting it to `None` would be this
//!   library inventing a fact, and `None` is reserved for a column the provider does not publish.
//!   Validate before trading on it.
//! - **Non-trading days are emitted as FLAT bars, not omitted — daily series are not sparse.**
//!   Every sampled Saturday and the US Independence Day observance returned a bar with
//!   `open == high == low == close`; Sundays are absent. A backtest therefore sees a tradeable
//!   price on a closed market, and the only signal is the flat OHLC. Intraday bars *are* sparse
//!   (no-trade minutes are absent rather than zero-filled), so the two resolutions differ.
//! - **These are CFD and aggregated-spot series, not exchange instruments.** `XAU/USD` is spot
//!   gold rather than a COMEX contract, `SPX500/USD` is a CFD rather than an index or its future,
//!   and `ES.F` is a continuous front-month proxy with **no contract chain, expiry or roll**.
//!   There is no venue attribution anywhere in the feed.
//! - **Crypto is an aggregated tape**: no funding rates, no liquidations, no venue. It is not a
//!   substitute for a native exchange connector.
//! - **The live tick is a QUOTE, not a print.** Its `price` equals its `bid` on every sample taken
//!   — 3,966 of 3,966 ticks across every dataset family. Both [`PublicTrades`] and [`OrderBooksL1`]
//!   are served from it, but a trade decoded this way is a bid-side quote wearing a trade's shape
//!   and is not evidence that a transaction occurred. See [`trade`] for the mapping and its
//!   reasoning.
//! - **Live tick `volume` is real on two venues and FABRICATED on two others**, with no in-band
//!   signal separating them. [`LseCrypto`] and [`LseEquities`] carry a genuine per-tick size, which
//!   reconciles exactly against the provider's own one-minute candles (ratio `1.000`).
//!   **[`LseFx`] and [`LseCfd`] carry a hard-coded `1.0`** on every tick of every symbol sampled —
//!   a placeholder that will aggregate into a legitimate-looking total at any resolution, so
//!   volume-weighted prices and size filters on those two venues are meaningless rather than merely
//!   imprecise. Note this differs from the REST vault, which *omits* FX volume entirely; the
//!   WebSocket invents a value instead.
//! - **Identical consecutive live ticks are genuine and are never de-duplicated.** Barely a third
//!   of a sampled run was unique on `(ts, price, bid, ask, volume)`, yet removing the repeats
//!   destroyed 3–10% of volume that otherwise reconciles exactly. Do not add a de-duplication
//!   filter; a test pins that both are emitted.
//! - **London (`.L`) listings are quoted in PENCE**, and the catalog reports no unit. They are
//!   quoted in GBX, an asset distinct from GBP; see
//!   [`market::quote_asset`].
//! - **Dataset slugs are not instrument identities** and do not uniquely identify a series; see
//!   [`market::slug`].

// A module carries an outer `///` here only when its own file has no `//!` documentation.
// Supplying both makes rustdoc resolve the file's inner links in THIS module's scope rather than
// the child's, so every `[`SomeType`]` written inside the child silently renders as dead text.

/// Replay historical candles for N instruments as one time-ordered market stream.
pub mod backtest;

/// The WebSocket channel a subscription maps to.
pub mod channel;

/// Errors produced by the London Strategic Edge integration.
pub mod error;

pub mod export;

pub mod historical;

pub mod live;

/// London Strategic Edge symbology: datasets, underlying assets, quote currencies and slugs.
pub mod market;

#[cfg(feature = "lse-parquet")]
pub mod parquet;

/// The shared streaming + export allowance, as the provider reports it.
pub mod quota;

pub mod quote;

pub mod resume;

pub mod stream;

pub mod subscription;

pub mod tick;

pub mod trade;

pub mod transformer;

pub mod vault;

use self::{
    channel::LseChannel,
    live::{LseSubscriber, subscribe_message},
    market::{LseMarket, LseServer, LseSymbolShape},
    stream::LseStream,
    subscription::LseSubResponse,
};
use crate::{
    NoInitialSnapshots,
    exchange::{Connector, ExchangeServer, ExchangeSub, StreamSelector},
    instrument::InstrumentData,
    subscriber::validator::WebSocketSubValidator,
    subscription::{book::OrderBooksL1, trade::PublicTrades},
};
use rustrade_instrument::exchange::ExchangeId;
use rustrade_integration::protocol::websocket::WsMessage;
use serde::{Deserialize, Serialize};
use std::{fmt::Debug, marker::PhantomData};
use url::Url;

/// The WebSocket endpoint.
///
/// One host serves every dataset. The per-dataset connector split below is about provenance in
/// `MarketEvent.exchange` and per-dataset support declarations, not about distinct endpoints — so
/// subscribing across datasets opens several connections to this one host. The provider served at
/// least eight concurrent authenticated connections on one key when measured; the binding
/// constraint is the per-connection subscription cap, not the connection count.
pub const WEBSOCKET_URL: &str = "wss://data-ws.londonstrategicedge.com";

/// The London Strategic Edge live market data connector.
///
/// Use the per-dataset aliases — [`LseFx`], [`LseCrypto`], [`LseEquities`], [`LseFutures`],
/// [`LseCfd`] — rather than naming the server type directly.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default)]
pub struct Lse<Server>(PhantomData<Server>);

/// Spot FX: `EUR/USD`, `GBP/USD`, …
pub type LseFx = Lse<LseServerFx>;

/// Aggregated spot crypto: `BTC/USD`, `ETH/USD`, …
pub type LseCrypto = Lse<LseServerCrypto>;

/// Equities and ETFs: `AAPL`, `SPY`, `BP.L`, …
pub type LseEquities = Lse<LseServerEquities>;

/// Continuous front-month futures proxies: `ES.F`, `FDAX`, …
pub type LseFutures = Lse<LseServerFutures>;

/// Indices, commodities, interest rates, currency indices and volatility, all as CFDs:
/// `SPX500/USD`, `XAU/USD`, `USB10Y/USD`, `DXY/USD`, `VIX/USD`.
pub type LseCfd = Lse<LseServerCfd>;

/// Spot FX server.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default)]
pub struct LseServerFx;

/// Aggregated spot crypto server.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default)]
pub struct LseServerCrypto;

/// Equities and ETFs server.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default)]
pub struct LseServerEquities;

/// Continuous futures-proxy server.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default)]
pub struct LseServerFutures;

/// CFD server — indices, commodities, interest rates, currency indices and volatility.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default)]
pub struct LseServerCfd;

macro_rules! impl_lse_server {
    ($server:ty, $id:expr, $shape:expr) => {
        impl ExchangeServer for $server {
            const ID: ExchangeId = $id;

            fn websocket_url() -> &'static str {
                WEBSOCKET_URL
            }
        }

        impl LseServer for $server {
            const SYMBOL_SHAPE: LseSymbolShape = $shape;
        }
    };
}

// The two traits are declared together per server because they answer one question each about the
// same dataset -- which venue its events are stamped with, and how it spells a symbol -- and a
// server that implements one without the other is not usable.
impl_lse_server!(LseServerFx, ExchangeId::LseFx, LseSymbolShape::Pair);
impl_lse_server!(LseServerCrypto, ExchangeId::LseCrypto, LseSymbolShape::Pair);
impl_lse_server!(
    LseServerEquities,
    ExchangeId::LseEquities,
    LseSymbolShape::Bare
);
impl_lse_server!(
    LseServerFutures,
    ExchangeId::LseFutures,
    LseSymbolShape::Bare
);
impl_lse_server!(LseServerCfd, ExchangeId::LseCfd, LseSymbolShape::Pair);

impl<Server> Connector for Lse<Server>
where
    Server: LseServer,
{
    const ID: ExchangeId = Server::ID;
    type Channel = LseChannel;
    type Market = LseMarket;
    type Subscriber = LseSubscriber;
    type SubValidator = WebSocketSubValidator;
    type SubResponse = LseSubResponse;

    fn url() -> Result<Url, url::ParseError> {
        Url::parse(Server::websocket_url())
    }

    /// One payload per symbol — the provider accepts no batched subscribe.
    ///
    /// # ⚠️ Nothing sends what this builds
    /// [`LseSubscriber`] does not call this: a subscription may carry a replay window and this
    /// function cannot see one, so the subscriber builds its own payloads and the mapper's output
    /// is discarded where it does so. This is required by the [`Connector`] contract and is
    /// implemented faithfully; it is not the route this integration subscribes over.
    ///
    /// It shares `subscribe_message` with the live route, so the two cannot disagree about a
    /// payload's *shape*. They do differ in what they are given: this receives one entry per
    /// subscription, while the subscriber sends one per *distinct* symbol, because a repeated
    /// symbol is one slot and one confirmation on this provider.
    fn requests(exchange_subs: Vec<ExchangeSub<Self::Channel, Self::Market>>) -> Vec<WsMessage> {
        exchange_subs
            .iter()
            .map(|exchange_sub| subscribe_message(exchange_sub.market.as_ref(), None))
            .collect()
    }
}

// Both subscription kinds decode the SAME frame -- the provider publishes one data frame and it
// carries a price, a bid, an ask and a size -- so the two selectors differ only in which decoder
// the transformer is instantiated with. Neither needs an initial snapshot: the feed is a tick
// stream with no book to synchronise against.
//
// One blanket impl per kind covers all five servers, rather than five impls each. The servers
// differ in venue and symbol shape, never in framing.

impl<Instrument, Server> StreamSelector<Instrument, PublicTrades> for Lse<Server>
where
    Instrument: InstrumentData,
    Server: LseServer + Debug + Send + Sync,
{
    type SnapFetcher = NoInitialSnapshots;
    type Stream = LseStream<Self, Instrument::Key, PublicTrades>;
}

impl<Instrument, Server> StreamSelector<Instrument, OrderBooksL1> for Lse<Server>
where
    Instrument: InstrumentData,
    Server: LseServer + Debug + Send + Sync,
{
    type SnapFetcher = NoInitialSnapshots;
    type Stream = LseStream<Self, Instrument::Key, OrderBooksL1>;
}

impl<'de, Server> Deserialize<'de> for Lse<Server>
where
    Server: ExchangeServer,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        let input = <String as Deserialize>::deserialize(deserializer)?;

        if input.as_str() == Server::ID.as_str() {
            Ok(Self::default())
        } else {
            Err(serde::de::Error::invalid_value(
                serde::de::Unexpected::Str(input.as_str()),
                &Server::ID.as_str(),
            ))
        }
    }
}

impl<Server> Serialize for Lse<Server>
where
    Server: ExchangeServer,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer,
    {
        serializer.serialize_str(Server::ID.as_str())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;

    /// The connector is hand-serialised rather than derived, so nothing else pins that the two
    /// halves agree — a `Subscription` carrying one would otherwise fail to round-trip through a
    /// config file with no compile-time signal.
    #[test]
    fn a_connector_round_trips_through_its_exchange_id() {
        let json = serde_json::to_string(&LseCrypto::default()).unwrap();
        assert_eq!(json, r#""lse_crypto""#);

        assert_eq!(
            serde_json::from_str::<LseCrypto>(&json).unwrap(),
            LseCrypto::default()
        );
    }

    /// Every dataset connector is a distinct type over one endpoint, so deserialising a config that
    /// names a *different* dataset must fail rather than silently produce this one and subscribe
    /// the wrong venue's symbology.
    #[test]
    fn a_connector_rejects_another_datasets_identifier() {
        let error = serde_json::from_str::<LseCrypto>(r#""lse_fx""#).unwrap_err();
        assert!(error.to_string().contains("lse_crypto"), "{error}");
    }
}
