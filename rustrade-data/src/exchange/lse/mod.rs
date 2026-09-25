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
//! - **⚠️ Candle volume has additionally been reported wrong by three to four orders of magnitude
//!   since 2026-04-27, in opposite directions on ETFs and on equities.** Unlike the rest of this
//!   list, which is measured here, this is a third-party report on the provider's public issue
//!   tracker — repeated across symbols and dates, and unanswered. **ETFs over-report**: a `QQQ`
//!   one-minute bar was published at 189,272,655,373 against a consolidated **daily** figure of
//!   33,118,600 for the same session — roughly 5,700× the whole day inside one minute — on
//!   `QQQ`, `SPY`, `IWM`, `SMH`, `XLE`, `XLF`, `TLT`, every trading day sampled over two months.
//!   **Equities under-report**: `AAPL`, `MSFT` and `NVDA` daily totals ran at 24–45% of the
//!   consolidated tape, against 65–80% before the same date, which is the ordinary
//!   primary-venue-versus-consolidated gap. Splits and liquidity stress were both ruled out by
//!   the reporter. This integration decodes those figures faithfully, and a shape check passes on
//!   them: the bars are structurally valid and semantically wrong, so nothing in band distinguishes
//!   them from correct ones. **Reconcile volume against a second source before sizing anything on
//!   it**, and treat a volume-derived signal — VWAP, a liquidity filter, a participation-rate
//!   model — as unusable on this feed until you have.
//! - **⚠️ History depth varies per dataset and is far shallower than the headline suggests on
//!   some of them.** `QQQ` in the `etf` dataset was reported carrying a first tick of 2026-04-27
//!   and 0.2 years of coverage — roughly three months of spot — while options on the same ticker
//!   reach back to 2014 and equities are stated to reach 2004. Depth is a per-symbol,
//!   per-dataset property, discoverable from the `first_tick` and `years` fields the provider's
//!   catalog publishes for each entry. **That catalog is on the discovery host and is not wrapped
//!   by this integration**, so nothing here can check a requested range against it: a backtest
//!   asking for years that a symbol does not have receives the bars that exist and no indication
//!   that the rest were never published. Check depth per symbol before choosing a range.
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
//! - **Option greeks are print-triggered, not continuous**: they arrive only with a trade, so a
//!   contract's greeks are as stale as its last print and an unheld, untraded contract is never
//!   marked. See [`options`].
//! - **Dataset slugs are not instrument identities** and do not uniquely identify a series; see
//!   [`market::slug`].

// A module carries an outer `///` here only when its own file has no `//!` documentation.
// Supplying both makes rustdoc resolve the file's inner links in THIS module's scope rather than
// the child's, so every `[`SomeType`]` written inside the child silently renders as dead text.

/// Replay historical candles for N instruments as one time-ordered market stream.
pub mod backtest;

pub mod bond_yield;

pub mod calendar;

/// The WebSocket channel a subscription maps to.
pub mod channel;

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

pub(crate) mod osi;

#[cfg(feature = "lse-parquet")]
pub mod parquet;

/// The shared streaming + export allowance, as the provider reports it.
pub mod quota;

pub mod quote;

pub mod reference;

pub mod resume;

pub mod stream;

pub mod subscription;

pub mod tick;

pub mod trade;

pub mod transformer;

/// HTTP plumbing shared by the provider's two hosts. Internal: the host clients
/// ([`vault::LseVaultClient`] and [`data_api::LseDataApiClient`]) are the public surface.
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
