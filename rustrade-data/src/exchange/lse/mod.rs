//! London Strategic Edge market data (behind the `lse` feature) — ⚠️ the retrieved data is **not
//! redistributable**, and use by or for an organisation needs the provider's licence.
//!
//! A market-data provider covering US, UK, Canadian and European stocks, ETFs, US options with
//! Greeks, forex, crypto, commodities, indices and futures, plus macroeconomic series. A free key
//! comes with a free account; exchange feeds (Nasdaq TotalView, CME Group) are paid plans.
//!
//! # Surfaces
//! - **History** comes from LSE Data 3.0 (`https://download.londonstrategicedge.com/v3`) through
//!   [`client::LseClient`]: the key's plan in [`usage`], the catalogue in [`catalogue`], candles in
//!   [`historical`], option prints, chains, candles and Greeks in [`options`], and a candle replay
//!   for backtests in [`backtest`].
//! - **Live** ticks stream over the provider's websocket through the `Lse*` connectors below.
//! - The pre-3.0 modules still here ([`vault`], [`data_api`], [`export`], [`bond_yield`],
//!   [`calendar`], [`quota`]) talk to hosts the provider has retired and are being replaced.
//!
//! # Licensing
//! This integration's **code** is MIT-licensed like the rest of this repository. **The data it
//! retrieves is not.** The provider's licence (<https://londonstrategicedge.com/licence>, version
//! 2026-11-10) allows an account's data for **its holder's own personal use**: their own research,
//! their own trading and training their own models, and sharing charts and results they produce.
//! Otherwise:
//! - **No distribution.** Supplying, showing, publishing or making the data available to anyone
//!   else, in any form, or running a product or service on it, needs a licence from the provider.
//!   Storing, resampling or aggregating the data does not make it yours.
//! - **Organisations need a licence.** Use by or for a company, a fund or any other organisation
//!   (a startup, a non-profit, paid work for clients) needs the provider's licence. The provider
//!   exempts one person trading only their own money through their own company.
//! - **Exchange data** (Nasdaq, CME Group) is also under that exchange's subscriber terms and must
//!   be deleted when the plan for it ends.
//! - **History can change**: the provider corrects errors and rebuilds history, so a stored copy
//!   can go stale.
//!
//! This library is a tool each user runs with their own key, which the licence allows. In
//! practice: do not commit retrieved data to a public repository, do not publish it as fixtures or
//! example datasets, and do not re-serve it. The provider's own client being MIT-licensed covers
//! that client only and confers no rights in the data; the same split applies here. Terms:
//! <https://londonstrategicedge.com/terms>.
//!
//! # Data characteristics
//! Properties that will silently mislead if assumed away. Those marked *pre-3.0* were measured on
//! the provider's previous API and have not been re-measured on LSE Data 3.0.
//!
//! - **History is aggregated on request.** The provider builds a candle from the ticks inside the
//!   window it is asked for, so a window cut mid-period returns a fragment that looks whole.
//!   [`LseClient::fetch_candles`](client::LseClient::fetch_candles) aligns its windows to whole
//!   periods for this reason.
//! - **Crypto is listed against Tether on REST** (`BTCUSDT`) and labelled `/USD` on the websocket
//!   (`BTC/USD`); see [`market::rest_symbol_for`].
//! - **These are the provider's own series, not exchange instruments.** The provider states that
//!   the prices in its own markets are produced with its own methodology and are not represented
//!   as matching any exchange's published price. `XAU/USD` is spot gold rather than a COMEX
//!   contract, and a futures symbol such as `SP500` is a continuous proxy with **no contract chain,
//!   expiry or roll**.
//! - **Crypto is an aggregated tape**: no funding rates, no liquidations, no venue. It is not a
//!   substitute for a native exchange connector.
//! - *Pre-3.0*: **FX candles were BID candles** — not mid, and not last: a day of `EUR/USD`
//!   one-minute bars matched the bid series on 1421 of 1421 minutes. A backtest filling at the
//!   candle close would fill at the bid. Check this on 3.0 data before relying on either reading.
//! - *Pre-3.0*: **candle volume was not dependable**: a majority of sampled one-minute equity bars
//!   reported `0` in minutes the tick tape showed trades, and a third-party report on the
//!   provider's issue tracker found ETF volume three to four orders of magnitude too large from 2026-04-27.
//!   Reconcile volume against a second source before sizing anything on it.
//! - **The live tick is a QUOTE, not a print.** Its `price` equalled its `bid` on every sample
//!   taken — 3,966 of 3,966 ticks across every dataset family. Both [`PublicTrades`] and
//!   [`OrderBooksL1`] are served from it, but a trade decoded this way is a bid-side quote wearing
//!   a trade's shape and is not evidence that a transaction occurred. See [`trade`].
//! - **Live tick `volume` was real on two venues and FABRICATED on two others**, with no in-band
//!   signal separating them. [`LseCrypto`] and [`LseEquities`] carried a genuine per-tick size;
//!   **[`LseFx`] and [`LseCfd`] carried a hard-coded `1.0`** on every tick sampled, so
//!   volume-weighted prices and size filters on those two are meaningless.
//! - **Identical consecutive live ticks are genuine and are never de-duplicated.** Removing the
//!   repeats destroyed 3–10% of volume that otherwise reconciled exactly; a test pins that both are
//!   emitted.
//! - **London (`.L`) listings are quoted in PENCE** (GBX, an asset distinct from GBP); see
//!   [`market::quote_asset`].
//! - **Option Greeks follow trades**: a contract that does not trade is not marked. See
//!   [`options`].

// A module carries an outer `///` here only when its own file has no `//!` documentation.
// Supplying both makes rustdoc resolve the file's inner links in THIS module's scope rather than
// the child's, so every `[`SomeType`]` written inside the child silently renders as dead text.

/// Replay historical candles for N instruments as one time-ordered market stream.
pub mod backtest;

pub mod bond_yield;

pub mod calendar;

pub mod catalogue;

/// The WebSocket channel a subscription maps to.
pub mod channel;

pub mod client;

pub mod connection;

pub mod data_api;

/// Errors produced by the London Strategic Edge integration.
pub mod error;

pub mod export;

pub mod historical;

pub mod live;

pub mod mapper;

pub mod options;

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

pub mod usage;

// HTTP plumbing shared by the REST clients; documented in its own file.
pub(crate) mod transport;

pub mod vault;

use self::{
    channel::LseChannel,
    live::{
        LseSubscriber, subscribe_message, subscribe_options_message, subscribes_per_underlying,
    },
    market::{LseMarket, LseQuoteServer, LseServer, LseSymbolShape},
    stream::LseStream,
    subscription::LseSubResponse,
};
use crate::exchange::osi;
use crate::{
    NoInitialSnapshots,
    exchange::{Connector, ExchangeServer, ExchangeSub, StreamSelector},
    instrument::InstrumentData,
    subscriber::validator::WebSocketSubValidator,
    subscription::{Map, book::OrderBooksL1, trade::PublicTrades},
};
use rustrade_instrument::exchange::ExchangeId;
use rustrade_integration::protocol::websocket::WsMessage;
use serde::{Deserialize, Serialize};
use smol_str::SmolStr;
use std::{fmt::Debug, marker::PhantomData};
use url::Url;

/// The WebSocket endpoint.
///
/// One host serves every dataset. The per-dataset connector split below is about provenance in
/// `MarketEvent.exchange` and per-dataset support declarations, not about distinct endpoints.
///
/// # ⚠️ One connection per key, shared by every stream a subscriber and its clones open
/// A free key — the `registered` tier, the only one measured — holds exactly one concurrent
/// connection: its handshake reports `max_connections: 1`, and a second is refused with
/// `TOO_MANY_CONNECTIONS`. That one connection serves every dataset, both subscription kinds and
/// option underlyings, so every stream opened by one [`LseSubscriber`] and its clones shares it —
/// across any number of `subscribe` calls. A subscriber built separately for the same key opens a
/// connection of its own and is refused. See [`connection`] for how the connection is shared.
///
/// That one connection holds 100 subscriptions when last measured (the handshake reports the live
/// figure), shared by every stream on it: a symbol per slot, or on [`LseOptions`] an underlying per
/// slot. A symbol held by several streams — [`OrderBooksL1`] alongside [`PublicTrades`], say —
/// costs one.
pub const WEBSOCKET_URL: &str = "wss://data-ws.londonstrategicedge.com";

/// Format of every naive timestamp the provider serves over REST.
///
/// Used by the candle rows' `ts` and by the catalog's `first_tick` / `last_tick`, which share one
/// spelling (`2024-01-02 09:09:00.000000`). `%.f` makes the fractional part optional, so a response
/// that drops the microseconds still parses. The value carries no timezone and is UTC.
pub(crate) const PROVIDER_TIMESTAMP_FORMAT: &str = "%Y-%m-%d %H:%M:%S%.f";

/// The London Strategic Edge live market data connector.
///
/// Use the per-dataset aliases — [`LseFx`], [`LseCrypto`], [`LseEquities`], [`LseFutures`],
/// [`LseCfd`], [`LseOptions`] — rather than naming the server type directly.
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

/// US equity and ETF option contracts, one [`Subscription`](crate::subscription::Subscription)
/// per contract: `SPY260930C00700000`.
///
/// Serves [`PublicTrades`] only — every option tick is a genuine print, and none carries a quote.
///
/// # How a batch is subscribed
/// The provider streams options **per underlying**, not per contract: one subscribe covers an
/// underlying's whole chain, thousands of contracts, on a single one of the connection's
/// subscription slots. Contracts sharing an underlying therefore collapse into one subscribe, and
/// the connection holds as many underlyings as it has slots — 100 when last measured — however
/// many contracts are registered.
///
/// # ⚠️ Only registered contracts are delivered; the rest of the chain is counted and dropped
/// Every contract on a subscribed underlying ticks, whether registered or not. Prints for contracts
/// nobody registered are dropped without an error each, and reported instead as a periodic
/// per-underlying `info` count. The set of contracts that trade does not level off over a session —
/// it was still growing after five minutes with no plateau in sight — so a contract that starts
/// trading mid-session is missed unless it was registered up front. The provider's REST print tape
/// in [`options`] carries every contract, greeks included.
///
/// # ⚠️ A contract the provider does not list is silent, not rejected
/// The provider confirms an underlying without listing its contracts, so a registered contract that
/// does not exist — a wrong expiry, a strike off the chain — is accepted and never ticks. An
/// underlying with no options at all *is* rejected, by name.
///
/// # One connection per key, shared
/// Option chains stream over the same connection as every other London Strategic Edge dataset, and
/// draw on its one subscription cap: each underlying takes a slot, as each plain symbol does. See
/// [`WEBSOCKET_URL`].
///
/// # No resumption
/// A reconnect does not replay the gap: the provider has no replay window on this channel. An
/// options subscribe that names a `start` is confirmed as usual and draws no error, yet no replay
/// opens and nothing older than the subscribe is served — measured in session, beside a plain
/// subscribe on the same connection that did replay. None is therefore requested, and a subscriber configured with [`LseSubscriber::with_resume`] resumes its other
/// datasets but not this one.
///
/// Expiry and strike conventions are those of [`LseSymbolShape::OptionContract`].
pub type LseOptions = Lse<LseServerOptions>;

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

/// Option contract server.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default)]
pub struct LseServerOptions;

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
impl_lse_server!(
    LseServerOptions,
    ExchangeId::LseOptions,
    LseSymbolShape::OptionContract
);

// Every server but options: the options channel publishes no quote. See `LseQuoteServer`.
impl LseQuoteServer for LseServerFx {}
impl LseQuoteServer for LseServerCrypto {}
impl LseQuoteServer for LseServerEquities {}
impl LseQuoteServer for LseServerFutures {}
impl LseQuoteServer for LseServerCfd {}

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
    ///
    /// On [`LseOptions`] it sends one payload per distinct underlying, as the subscriber does. A
    /// contract with no OSI symbol has no underlying to name and is left out; the live route
    /// rejects the batch over it instead, which this infallible signature cannot.
    fn requests(exchange_subs: Vec<ExchangeSub<Self::Channel, Self::Market>>) -> Vec<WsMessage> {
        if !subscribes_per_underlying(Self::ID) {
            return exchange_subs
                .iter()
                .map(|exchange_sub| subscribe_message(exchange_sub.market.as_ref(), None))
                .collect();
        }

        let mut underlyings = Vec::<&str>::new();
        for exchange_sub in &exchange_subs {
            if let Some(root) = osi::root(exchange_sub.market.as_ref())
                && !underlyings.contains(&root)
            {
                underlyings.push(root);
            }
        }

        underlyings
            .into_iter()
            .map(subscribe_options_message)
            .collect()
    }

    /// One confirmation per distinct symbol — or, on [`LseOptions`], per distinct underlying.
    ///
    /// The instrument map holds one entry per registered contract, but the provider confirms an
    /// underlying once however many of its contracts are registered, so counting entries would
    /// leave the validator waiting out its timeout for confirmations that never come.
    fn expected_responses<InstrumentKey>(map: &Map<InstrumentKey>) -> usize {
        if !subscribes_per_underlying(Self::ID) {
            return map.0.len();
        }

        let mut underlyings = Vec::<SmolStr>::new();
        for subscription_id in map.0.keys() {
            // A subscription identifier is the contract symbol -- see `mapper::subscription_id`.
            if let Some(root) = osi::root(subscription_id.as_ref())
                && !underlyings.iter().any(|underlying| underlying == root)
            {
                underlyings.push(SmolStr::new(root));
            }
        }

        underlyings.len()
    }
}

// Both subscription kinds decode the SAME frame -- the provider publishes one data frame and it
// carries a price, a bid, an ask and a size -- so the two selectors differ only in which decoder
// the transformer is instantiated with. Neither needs an initial snapshot: the feed is a tick
// stream with no book to synchronise against.
//
// One blanket impl per kind covers every server, rather than an impl per server each. The servers
// differ in venue and symbol shape, never in framing -- save that the options channel carries no
// quote, which is why the L1 impl is bounded on `LseQuoteServer`.

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
    Server: LseQuoteServer + Debug + Send + Sync,
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

    fn map(markets: &[&str]) -> Map<u8> {
        markets
            .iter()
            .zip(0_u8..)
            .map(|(market, key)| (mapper::subscription_id(market), key))
            .collect()
    }

    /// The provider confirms an underlying once however many of its contracts are registered.
    /// Counting contracts would leave the validator waiting out its timeout.
    #[test]
    fn options_expect_one_confirmation_per_underlying() {
        let contracts = map(&[
            "SPY260930C00700000",
            "SPY260930P00650000",
            "QQQ260930C00500000",
        ]);

        assert_eq!(LseOptions::expected_responses(&contracts), 2);
    }

    #[test]
    fn other_datasets_expect_one_confirmation_per_symbol() {
        let symbols = map(&["BTC/USD", "ETH/USD"]);
        assert_eq!(LseCrypto::expected_responses(&symbols), 2);
    }

    #[test]
    fn options_requests_name_each_underlying_once() {
        let exchange_subs = [
            "SPY260930C00700000",
            "SPY260930P00650000",
            "QQQ260930C00500000",
        ]
        .into_iter()
        .map(|market| ExchangeSub::from((LseChannel::Tick, LseMarket(market.into()))))
        .collect();

        let payloads: Vec<serde_json::Value> = LseOptions::requests(exchange_subs)
            .into_iter()
            .map(|message| match message {
                WsMessage::Text(text) => serde_json::from_str(text.as_str()).unwrap(),
                other => panic!("expected a text payload, not {other:?}"),
            })
            .collect();

        assert_eq!(
            payloads,
            [
                serde_json::json!({"action": "subscribe_options", "underlying": "SPY"}),
                serde_json::json!({"action": "subscribe_options", "underlying": "QQQ"}),
            ]
        );
    }

    #[test]
    fn the_options_server_spells_option_contracts() {
        assert_eq!(
            LseServerOptions::SYMBOL_SHAPE,
            LseSymbolShape::OptionContract
        );
        assert_eq!(LseServerOptions::ID, ExchangeId::LseOptions);
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
