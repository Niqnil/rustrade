use crate::{
    exchange::Connector, instrument::InstrumentData, subscription::candle::CandleInterval,
};
use derive_more::Display;
use fnv::FnvHashMap;
use rustrade_instrument::{
    Keyed,
    asset::name::AssetNameInternal,
    exchange::ExchangeId,
    instrument::market_data::{MarketDataInstrument, kind::MarketDataInstrumentKind},
};
use rustrade_integration::{
    Validator, error::SocketError, protocol::websocket::WsMessage, subscription::SubscriptionId,
};
use serde::{Deserialize, Serialize};
use smol_str::{ToSmolStr, format_smolstr};
use std::{borrow::Borrow, fmt::Debug, hash::Hash};

/// OrderBook [`SubscriptionKind`]s and the associated Barter output data models.
pub mod book;

/// Candle [`SubscriptionKind`] and the associated Barter output data model.
pub mod candle;

/// Option Greeks data model for options analytics.
pub mod greeks;

/// Liquidation [`SubscriptionKind`] and the associated Barter output data model.
pub mod liquidation;

/// Quote [`SubscriptionKind`] and the associated Barter output data model.
pub mod quote;

/// Public trade [`SubscriptionKind`] and the associated Barter output data model.
pub mod trade;

/// Defines kind of a [`Subscription`], and the output [`Self::Event`] that it yields.
pub trait SubscriptionKind
where
    Self: Debug + Clone,
{
    type Event: Debug;
    fn as_str(&self) -> &'static str;
}

/// Barter [`Subscription`] used to subscribe to a [`SubscriptionKind`] for a particular exchange
/// [`MarketDataInstrument`].
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize)]
pub struct Subscription<Exchange = ExchangeId, Inst = MarketDataInstrument, Kind = SubKind> {
    pub exchange: Exchange,
    #[serde(flatten)]
    pub instrument: Inst,
    #[serde(alias = "type")]
    pub kind: Kind,
}

pub fn display_subscriptions_without_exchange<Exchange, Instrument, Kind>(
    subscriptions: &[Subscription<Exchange, Instrument, Kind>],
) -> String
where
    Instrument: std::fmt::Display,
    Kind: std::fmt::Display,
{
    subscriptions
        .iter()
        .map(
            |Subscription {
                 exchange: _,
                 instrument,
                 kind,
             }| { format_smolstr!("({instrument}, {kind})") },
        )
        .collect::<Vec<_>>()
        .join(",")
}

impl<Exchange, Instrument, Kind> std::fmt::Display for Subscription<Exchange, Instrument, Kind>
where
    Exchange: std::fmt::Display,
    Instrument: std::fmt::Display,
    Kind: std::fmt::Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "({}|{}|{})", self.exchange, self.kind, self.instrument)
    }
}

#[derive(
    Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Display, Deserialize, Serialize,
)]
pub enum SubKind {
    PublicTrades,
    OrderBooksL1,
    OrderBooksL2,
    OrderBooksL3,
    Liquidations,
    /// Candle (kline) subscription at a specific [`CandleInterval`] resolution.
    ///
    /// Unlike the other [`SubKind`]s, a candle subscription is **not** fully specified
    /// without a resolution — there is no venue-agnostic "default" candle stream — so the
    /// [`interval`](CandleInterval) is carried in the variant. The `derive_more::Display`
    /// tag is the fixed `"candles"` (independent of the interval), matching the typed
    /// [`Candles`](candle::Candles) kind tag.
    #[display("candles")]
    Candles {
        interval: CandleInterval,
    },
    /// Real-time top-of-book quotes (best bid/ask). Generic subscription kind
    /// that may be supported by multiple exchanges providing quote data.
    Quotes,
}

impl<Exchange, S, Kind> From<(Exchange, S, S, MarketDataInstrumentKind, Kind)>
    for Subscription<Exchange, MarketDataInstrument, Kind>
where
    S: Into<AssetNameInternal>,
{
    fn from(
        (exchange, base, quote, instrument_kind, kind): (
            Exchange,
            S,
            S,
            MarketDataInstrumentKind,
            Kind,
        ),
    ) -> Self {
        Self::new(exchange, (base, quote, instrument_kind), kind)
    }
}

impl<InstrumentKey, Exchange, S, Kind>
    From<(
        InstrumentKey,
        Exchange,
        S,
        S,
        MarketDataInstrumentKind,
        Kind,
    )> for Subscription<Exchange, Keyed<InstrumentKey, MarketDataInstrument>, Kind>
where
    S: Into<AssetNameInternal>,
{
    fn from(
        (instrument_id, exchange, base, quote, instrument_kind, kind): (
            InstrumentKey,
            Exchange,
            S,
            S,
            MarketDataInstrumentKind,
            Kind,
        ),
    ) -> Self {
        let instrument = Keyed::new(instrument_id, (base, quote, instrument_kind).into());

        Self::new(exchange, instrument, kind)
    }
}

impl<Exchange, I, Instrument, Kind> From<(Exchange, I, Kind)>
    for Subscription<Exchange, Instrument, Kind>
where
    I: Into<Instrument>,
{
    fn from((exchange, instrument, kind): (Exchange, I, Kind)) -> Self {
        Self::new(exchange, instrument, kind)
    }
}

impl<Instrument, Exchange, Kind> Subscription<Exchange, Instrument, Kind> {
    /// Constructs a new [`Subscription`] using the provided configuration.
    pub fn new<I>(exchange: Exchange, instrument: I, kind: Kind) -> Self
    where
        I: Into<Instrument>,
    {
        Self {
            exchange,
            instrument: instrument.into(),
            kind,
        }
    }
}

impl<Exchange, Instrument, Kind> Validator for Subscription<Exchange, Instrument, Kind>
where
    Exchange: Connector,
    Instrument: InstrumentData,
{
    type Error = SocketError;

    fn validate(self) -> Result<Self, Self::Error>
    where
        Self: Sized,
    {
        // Validate the Exchange supports the Subscription InstrumentKind
        if exchange_supports_instrument_kind(Exchange::ID, self.instrument.kind()) {
            Ok(self)
        } else {
            Err(SocketError::Unsupported {
                entity: Exchange::ID.to_string(),
                item: self.instrument.kind().to_string(),
            })
        }
    }
}

/// Determines whether the [`Connector`] associated with this [`ExchangeId`] supports the
/// ingestion of market data for the provided [`MarketDataInstrumentKind`].
// The arms are grouped by `MarketDataInstrumentKind`, each group ending in a default whose
// open-vs-closed choice is documented in place (see the `Spot` note below). Collapsing to
// `matches!` would fold every group into one boolean expression, leaving nowhere to record which
// defaults are deliberate and which exchange lists were verified against what.
#[allow(clippy::match_like_matches_macro)]
pub fn exchange_supports_instrument_kind(
    exchange: ExchangeId,
    instrument_kind: &MarketDataInstrumentKind,
) -> bool {
    use rustrade_instrument::{
        exchange::ExchangeId::*, instrument::market_data::kind::MarketDataInstrumentKind::*,
    };

    match (exchange, instrument_kind) {
        // Spot
        (
            BinanceFuturesUsd | Bitmex | BybitPerpetualsUsd | GateioPerpetualsUsd
            | GateioPerpetualsBtc,
            Spot,
        ) => false,
        (LseFx | LseCrypto | LseEquities, Spot) => true,
        (LseCfd | LseFutures, Spot) => false,
        // NOTE: this default is OPEN -- an exchange that serves no spot market claims spot support
        // unless it is denied above, with no compile error to prompt the edit. It is left open
        // deliberately: closing it means writing an explicit arm for every existing variant and
        // re-verifying each one's spot coverage, which is tracked separately rather than smuggled
        // in here. Deny explicitly above when adding a non-spot exchange.
        (_, Spot) => true,

        // Future
        (GateioFuturesUsd | GateioFuturesBtc | Okx, Future { .. }) => true,
        (_, Future { .. }) => false,

        // Perpetual
        (
            BinanceFuturesUsd | Bitmex | Okx | BybitPerpetualsUsd | GateioPerpetualsUsd
            | GateioPerpetualsBtc | HyperliquidPerp,
            Perpetual,
        ) => true,
        (_, Perpetual) => false,

        // Option
        (GateioOptions | Okx, Option { .. }) => true,
        (_, Option { .. }) => false,

        // Cfd
        (LseCfd | LseFutures, Cfd) => true,
        (_, Cfd) => false,
    }
}

impl<Instrument> Validator for Subscription<ExchangeId, Instrument, SubKind>
where
    Instrument: InstrumentData,
{
    type Error = SocketError;

    fn validate(self) -> Result<Self, Self::Error>
    where
        Self: Sized,
    {
        // Validate the Exchange supports the Subscription InstrumentKind
        if exchange_supports_instrument_kind_sub_kind(
            &self.exchange,
            self.instrument.kind(),
            self.kind,
        ) {
            Ok(self)
        } else {
            Err(SocketError::Unsupported {
                entity: self.exchange.to_string(),
                item: format!("({}, {})", self.instrument.kind(), self.kind),
            })
        }
    }
}

/// Determines whether the [`Connector`] associated with this [`ExchangeId`] supports the
/// ingestion of market data for the provided [`MarketDataInstrumentKind`] and [`SubKind`] combination.
///
/// [`SubKind::Candles`] carries its resolution, so support is checked **per interval**:
/// [`CandleInterval`] is the venue-agnostic union of every resolution any connector
/// serves, and no venue serves all of it (Binance publishes no `5s`/`15s`/`30s` kline).
pub fn exchange_supports_instrument_kind_sub_kind(
    exchange_id: &ExchangeId,
    instrument_kind: &MarketDataInstrumentKind,
    sub_kind: SubKind,
) -> bool {
    use ExchangeId::*;
    use MarketDataInstrumentKind::*;
    use SubKind::*;

    match (exchange_id, instrument_kind, sub_kind) {
        (BinanceSpot, Spot, PublicTrades | OrderBooksL1 | OrderBooksL2) => true,
        (BinanceSpot, Spot, Candles { interval }) => {
            crate::exchange::binance::supports_candle_interval(interval)
        }
        (
            BinanceFuturesUsd,
            Perpetual,
            PublicTrades | OrderBooksL1 | OrderBooksL2 | Liquidations,
        ) => true,
        (BinanceFuturesUsd, Perpetual, Candles { interval }) => {
            crate::exchange::binance::supports_candle_interval(interval)
        }
        (Bitfinex, Spot, PublicTrades) => true,
        (Bitmex, Perpetual, PublicTrades) => true,
        (BybitSpot, Spot, PublicTrades | OrderBooksL1 | OrderBooksL2) => true,
        (BybitPerpetualsUsd, Perpetual, PublicTrades | OrderBooksL1 | OrderBooksL2) => true,
        (Coinbase, Spot, PublicTrades) => true,
        (GateioSpot, Spot, PublicTrades) => true,
        (GateioFuturesUsd, Future { .. }, PublicTrades) => true,
        (GateioFuturesBtc, Future { .. }, PublicTrades) => true,
        (GateioPerpetualsUsd, Perpetual, PublicTrades) => true,
        (GateioPerpetualsBtc, Perpetual, PublicTrades) => true,
        (GateioOptions, Option { .. }, PublicTrades) => true,
        (Kraken, Spot, PublicTrades | OrderBooksL1) => true,
        (Okx, Spot | Future { .. } | Perpetual | Option { .. }, PublicTrades) => true,
        (HyperliquidPerp, Perpetual, PublicTrades | OrderBooksL2) => true,

        // London Strategic Edge serves both kinds from ONE frame: its WebSocket publishes a single
        // tick carrying `price`, `bid`, `ask` and `volume` together, so a dataset that serves one
        // of these kinds necessarily serves the other. The two arms differ only in instrument kind,
        // which is fixed per dataset.
        //
        // ⚠️ A `PublicTrade` from this feed MAY NOT BE A PRINT. The tick's `price` equals its `bid`
        // on every sample taken -- 3,966 of 3,966 ticks spanning every dataset family -- so a trade
        // decoded from it is a bid-side quote wearing a trade's shape, and its arrival is not
        // evidence that a transaction occurred, at that price or at all.
        //
        // ⚠️ And its `amount` is genuine on two of these venues and FABRICATED on the other two,
        // with no in-band signal separating them. `LseCrypto` and `LseEquities` carry a real
        // per-tick size, which reconciles against the provider's own one-minute candles to the last
        // decimal. `LseFx` and `LseCfd` carry a hard-coded `1.0` on every tick of every symbol
        // sampled -- a placeholder that, being a plausible positive number, aggregates into a
        // legitimate-looking total at any resolution, leaving volume-weighted prices and size
        // filters on those two meaningless rather than merely imprecise.
        //
        // Support is declared regardless, and uniformly: the frame is identical across datasets, so
        // withholding the kind would hide the feed rather than the hazard. Saying so is the useful
        // answer. The full reasoning lives on the trade decoder in the `lse` module.
        (LseFx | LseCrypto | LseEquities, Spot, PublicTrades | OrderBooksL1) => true,
        (LseCfd | LseFutures, Cfd, PublicTrades | OrderBooksL1) => true,
        // No `Candles` arm exists for any London Strategic Edge venue, deliberately: the provider's
        // WebSocket carries no candle channel at all -- the tick above is its only data frame, so
        // there is nothing to subscribe to. Its candles are served exclusively over the REST vault,
        // by a path that never reaches this matrix. The absence is a decision, not an omission.
        (_, _, _) => false,
    }
}

/// Metadata generated from a collection of Barter [`Subscription`]s, including the exchange
/// specific subscription payloads that are sent to the exchange.
#[derive(Clone, Eq, PartialEq, Debug)]
pub struct SubscriptionMeta<InstrumentKey> {
    /// `HashMap` containing the mapping between a [`SubscriptionId`] and
    /// it's associated Barter [`MarketDataInstrument`].
    pub instrument_map: Map<InstrumentKey>,
    /// Collection of [`WsMessage`]s containing exchange specific subscription payloads to be sent.
    pub ws_subscriptions: Vec<WsMessage>,
}

/// New type`HashMap` that maps a [`SubscriptionId`] to some associated type `T`.
///
/// Used by [`ExchangeTransformer`](crate::transformer::ExchangeTransformer)s to identify the
/// Barter [`MarketDataInstrument`] associated with incoming exchange messages.
#[derive(Clone, Eq, PartialEq, Debug, Deserialize, Serialize)]
pub struct Map<T>(pub FnvHashMap<SubscriptionId, T>);

impl<T> FromIterator<(SubscriptionId, T)> for Map<T> {
    fn from_iter<Iter>(iter: Iter) -> Self
    where
        Iter: IntoIterator<Item = (SubscriptionId, T)>,
    {
        Self(iter.into_iter().collect::<FnvHashMap<SubscriptionId, T>>())
    }
}

impl<T> Map<T> {
    /// Find the `InstrumentKey` associated with the provided [`SubscriptionId`].
    pub fn find<SubId>(&self, id: &SubId) -> Result<&T, SocketError>
    where
        SubscriptionId: Borrow<SubId>,
        SubId: AsRef<str> + Hash + Eq + ?Sized,
    {
        self.0
            .get(id)
            .ok_or_else(|| SocketError::Unidentifiable(SubscriptionId(id.as_ref().to_smolstr())))
    }

    /// Find the mutable reference to `T` associated with the provided [`SubscriptionId`].
    pub fn find_mut<SubId>(&mut self, id: &SubId) -> Result<&mut T, SocketError>
    where
        SubscriptionId: Borrow<SubId>,
        SubId: AsRef<str> + Hash + Eq + ?Sized,
    {
        self.0
            .get_mut(id)
            .ok_or_else(|| SocketError::Unidentifiable(SubscriptionId(id.as_ref().to_smolstr())))
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;

    mod supports {
        use super::*;
        use rustrade_instrument::exchange::ExchangeId;

        /// The London Strategic Edge datasets, paired with the single
        /// [`MarketDataInstrumentKind`] each one serves.
        const LSE: [(ExchangeId, MarketDataInstrumentKind); 5] = [
            (ExchangeId::LseFx, MarketDataInstrumentKind::Spot),
            (ExchangeId::LseCrypto, MarketDataInstrumentKind::Spot),
            (ExchangeId::LseEquities, MarketDataInstrumentKind::Spot),
            (ExchangeId::LseFutures, MarketDataInstrumentKind::Cfd),
            (ExchangeId::LseCfd, MarketDataInstrumentKind::Cfd),
        ];

        #[test]
        fn test_lse_supports_exactly_its_own_kind() {
            // Each dataset serves one kind and must deny the others. The denials matter as much as
            // the grants: `exchange_supports_instrument_kind`'s `(_, Spot) => true` default means
            // a CFD-only dataset would otherwise claim spot support with no edit and no error.
            for (exchange, supported) in LSE {
                assert!(
                    exchange_supports_instrument_kind(exchange, &supported),
                    "{exchange:?} should support {supported:?}"
                );

                for denied in [
                    MarketDataInstrumentKind::Spot,
                    MarketDataInstrumentKind::Cfd,
                    MarketDataInstrumentKind::Perpetual,
                ] {
                    if denied == supported {
                        continue;
                    }
                    assert!(
                        !exchange_supports_instrument_kind(exchange, &denied),
                        "{exchange:?} should not support {denied:?}"
                    );
                }
            }
        }

        #[test]
        fn test_cfd_denied_for_non_cfd_exchanges() {
            for exchange in [
                ExchangeId::BinanceSpot,
                ExchangeId::Coinbase,
                ExchangeId::Okx,
                ExchangeId::GateioOptions,
                ExchangeId::Mock,
            ] {
                assert!(
                    !exchange_supports_instrument_kind(exchange, &MarketDataInstrumentKind::Cfd),
                    "{exchange:?} should not support Cfd"
                );
            }
        }

        #[test]
        fn test_lse_supports_both_tick_derived_sub_kinds_for_its_own_kind_only() {
            // One tick frame carries price, bid, ask and size together, so every dataset serves
            // both kinds decoded from it -- and serves neither under an instrument kind it does
            // not model.
            for (exchange, supported) in LSE {
                for sub_kind in [SubKind::PublicTrades, SubKind::OrderBooksL1] {
                    assert!(
                        exchange_supports_instrument_kind_sub_kind(&exchange, &supported, sub_kind),
                        "{exchange:?} should support ({supported:?}, {sub_kind})"
                    );

                    for denied in [
                        MarketDataInstrumentKind::Spot,
                        MarketDataInstrumentKind::Cfd,
                        MarketDataInstrumentKind::Perpetual,
                    ] {
                        if denied == supported {
                            continue;
                        }
                        assert!(
                            !exchange_supports_instrument_kind_sub_kind(
                                &exchange, &denied, sub_kind
                            ),
                            "{exchange:?} should not support ({denied:?}, {sub_kind})"
                        );
                    }
                }
            }
        }

        #[test]
        fn test_lse_serves_no_candle_stream_at_any_interval() {
            // Pins the absent `Candles` arm as a decision: the provider's WebSocket has no candle
            // channel, and its candles reach users over the REST vault, which never consults this
            // matrix. A future edit that "completes" the LSE rows fails here.
            for (exchange, kind) in LSE {
                for interval in CandleInterval::ALL {
                    assert!(
                        !exchange_supports_instrument_kind_sub_kind(
                            &exchange,
                            &kind,
                            SubKind::Candles { interval },
                        ),
                        "{exchange:?} serves no candle stream, including {interval:?}"
                    );
                }
            }
        }

        #[test]
        fn test_lse_denies_sub_kinds_its_tick_cannot_serve() {
            // The tick is top-of-book, so there is no depth to serve; the feed publishes no
            // liquidations at all. `Quotes` is denied here as it is for every other venue -- no
            // exchange declares it in this matrix.
            for (exchange, kind) in LSE {
                for sub_kind in [
                    SubKind::OrderBooksL2,
                    SubKind::OrderBooksL3,
                    SubKind::Liquidations,
                    SubKind::Quotes,
                ] {
                    assert!(
                        !exchange_supports_instrument_kind_sub_kind(&exchange, &kind, sub_kind),
                        "{exchange:?} should not support ({kind:?}, {sub_kind})"
                    );
                }
            }
        }
    }

    mod subscription {
        use super::*;
        use crate::{
            exchange::{coinbase::Coinbase, okx::Okx},
            subscription::trade::PublicTrades,
        };
        use rustrade_instrument::instrument::market_data::MarketDataInstrument;

        mod de {
            use super::*;
            use crate::{
                exchange::{
                    binance::{futures::BinanceFuturesUsd, spot::BinanceSpot},
                    gateio::perpetual::GateioPerpetualsUsd,
                    okx::Okx,
                },
                subscription::{book::OrderBooksL2, trade::PublicTrades},
            };
            use rustrade_instrument::instrument::market_data::MarketDataInstrument;

            #[test]
            fn test_subscription_okx_spot_public_trades() {
                let input = r#"
                {
                    "exchange": "okx",
                    "base": "btc",
                    "quote": "usdt",
                    "instrument_kind": "spot",
                    "kind": "public_trades"
                }
                "#;

                serde_json::from_str::<Subscription<Okx, MarketDataInstrument, PublicTrades>>(
                    input,
                )
                .unwrap();
            }

            #[test]
            fn test_subscription_binance_spot_public_trades() {
                let input = r#"
                {
                    "exchange": "binance_spot",
                    "base": "btc",
                    "quote": "usdt",
                    "instrument_kind": "spot",
                    "kind": "public_trades"
                }
                "#;

                serde_json::from_str::<Subscription<BinanceSpot, MarketDataInstrument, PublicTrades>>(input)
                    .unwrap();
            }

            #[test]
            fn test_subscription_binance_futures_usd_order_books_l2() {
                let input = r#"
                {
                    "exchange": "binance_futures_usd",
                    "base": "btc",
                    "quote": "usdt",
                    "instrument_kind": "perpetual",
                    "kind": "order_books_l2"
                }
                "#;

                serde_json::from_str::<
                    Subscription<BinanceFuturesUsd, MarketDataInstrument, OrderBooksL2>,
                >(input)
                .unwrap();
            }

            #[test]
            fn subscription_gateio_futures_usd_public_trades() {
                let input = r#"
                {
                    "exchange": "gateio_perpetuals_usd",
                    "base": "btc",
                    "quote": "usdt",
                    "instrument_kind": "perpetual",
                    "kind": "public_trades"
                }
                "#;

                serde_json::from_str::<
                    Subscription<GateioPerpetualsUsd, MarketDataInstrument, PublicTrades>,
                >(input)
                .unwrap();
            }
        }

        #[test]
        fn test_validate_bitfinex_public_trades() {
            struct TestCase {
                input: Subscription<Coinbase, MarketDataInstrument, PublicTrades>,
                expected:
                    Result<Subscription<Coinbase, MarketDataInstrument, PublicTrades>, SocketError>,
            }

            let tests = vec![
                TestCase {
                    // TC0: Valid Coinbase Spot PublicTrades subscription
                    input: Subscription::from((
                        Coinbase,
                        "base",
                        "quote",
                        MarketDataInstrumentKind::Spot,
                        PublicTrades,
                    )),
                    expected: Ok(Subscription::from((
                        Coinbase,
                        "base",
                        "quote",
                        MarketDataInstrumentKind::Spot,
                        PublicTrades,
                    ))),
                },
                TestCase {
                    // TC1: Invalid Coinbase FuturePerpetual PublicTrades subscription
                    input: Subscription::from((
                        Coinbase,
                        "base",
                        "quote",
                        MarketDataInstrumentKind::Perpetual,
                        PublicTrades,
                    )),
                    expected: Err(SocketError::Unsupported {
                        entity: "".to_string(),
                        item: "".to_string(),
                    }),
                },
            ];

            for (index, test) in tests.into_iter().enumerate() {
                let actual = test.input.validate();
                match (actual, test.expected) {
                    (Ok(actual), Ok(expected)) => {
                        assert_eq!(actual, expected, "TC{} failed", index)
                    }
                    (Err(_), Err(_)) => {
                        // Test passed
                    }
                    (actual, expected) => {
                        // Test failed
                        panic!(
                            "TC{index} failed because actual != expected. \nActual: {actual:?}\nExpected: {expected:?}\n"
                        );
                    }
                }
            }
        }

        #[test]
        fn test_validate_okx_public_trades() {
            struct TestCase {
                input: Subscription<Okx, MarketDataInstrument, PublicTrades>,
                expected:
                    Result<Subscription<Okx, MarketDataInstrument, PublicTrades>, SocketError>,
            }

            let tests = vec![
                TestCase {
                    // TC0: Valid Okx Spot PublicTrades subscription
                    input: Subscription::from((
                        Okx,
                        "base",
                        "quote",
                        MarketDataInstrumentKind::Spot,
                        PublicTrades,
                    )),
                    expected: Ok(Subscription::from((
                        Okx,
                        "base",
                        "quote",
                        MarketDataInstrumentKind::Spot,
                        PublicTrades,
                    ))),
                },
                TestCase {
                    // TC1: Valid Okx FuturePerpetual PublicTrades subscription
                    input: Subscription::from((
                        Okx,
                        "base",
                        "quote",
                        MarketDataInstrumentKind::Perpetual,
                        PublicTrades,
                    )),
                    expected: Ok(Subscription::from((
                        Okx,
                        "base",
                        "quote",
                        MarketDataInstrumentKind::Perpetual,
                        PublicTrades,
                    ))),
                },
            ];

            for (index, test) in tests.into_iter().enumerate() {
                let actual = test.input.validate();
                match (actual, test.expected) {
                    (Ok(actual), Ok(expected)) => {
                        assert_eq!(actual, expected, "TC{} failed", index)
                    }
                    (Err(_), Err(_)) => {
                        // Test passed
                    }
                    (actual, expected) => {
                        // Test failed
                        panic!(
                            "TC{index} failed because actual != expected. \nActual: {actual:?}\nExpected: {expected:?}\n"
                        );
                    }
                }
            }
        }
    }

    mod sub_kind {
        use super::*;

        #[test]
        fn candles_variant_serde_round_trips() {
            let kind = SubKind::Candles {
                interval: CandleInterval::Min1,
            };

            let json = serde_json::to_string(&kind).unwrap();
            let back = serde_json::from_str::<SubKind>(&json).unwrap();

            assert_eq!(kind, back, "SubKind::Candles must serde round-trip");
        }

        #[test]
        fn candles_variant_serde_shape_is_externally_tagged() {
            // `SubKind` is an externally-tagged enum (no `#[serde(tag = ...)]`), so the struct
            // variant serialises as `{"Candles":{"interval":"1m"}}` — NOT an internally-tagged
            // `{"type":"candles",...}` form. Pin the wire shape so a future tag/rename change is
            // caught here rather than silently shipped to config-driven consumers.
            let json = serde_json::to_string(&SubKind::Candles {
                interval: CandleInterval::Min1,
            })
            .unwrap();
            assert_eq!(json, r#"{"Candles":{"interval":"1m"}}"#);
        }

        #[test]
        fn candles_display_tag_is_interval_independent() {
            // The `derive_more::Display` tag is the fixed kind name, never the interval.
            assert_eq!(
                SubKind::Candles {
                    interval: CandleInterval::Sec1
                }
                .to_string(),
                "candles"
            );
            assert_eq!(
                SubKind::Candles {
                    interval: CandleInterval::Month1
                }
                .to_string(),
                "candles"
            );
        }

        #[test]
        fn candles_supported_per_interval_on_both_binance_venues() {
            // `CandleInterval` is a venue-agnostic union, so candle support is decided
            // per interval, not per `SubKind`: both Binance venues serve every interval
            // Binance publishes a kline for, and neither serves `5s`/`15s`/`30s`.
            for interval in CandleInterval::ALL {
                let expected = crate::exchange::binance::supports_candle_interval(interval);
                assert_eq!(
                    exchange_supports_instrument_kind_sub_kind(
                        &ExchangeId::BinanceSpot,
                        &MarketDataInstrumentKind::Spot,
                        SubKind::Candles { interval },
                    ),
                    expected,
                    "BinanceSpot/Spot Candles {interval:?}"
                );
                assert_eq!(
                    exchange_supports_instrument_kind_sub_kind(
                        &ExchangeId::BinanceFuturesUsd,
                        &MarketDataInstrumentKind::Perpetual,
                        SubKind::Candles { interval },
                    ),
                    expected,
                    "BinanceFuturesUsd/Perpetual Candles {interval:?}"
                );
            }
        }

        #[test]
        fn candles_rejected_for_unsupported_venue_kind_pairing() {
            // A supported venue with the wrong instrument kind is still rejected.
            assert!(
                !exchange_supports_instrument_kind_sub_kind(
                    &ExchangeId::BinanceSpot,
                    &MarketDataInstrumentKind::Perpetual,
                    SubKind::Candles {
                        interval: CandleInterval::Min1,
                    },
                ),
                "BinanceSpot does not serve Perpetual candles"
            );
        }
    }

    mod instrument_map {
        use super::*;
        use rustrade_instrument::instrument::market_data::MarketDataInstrument;

        #[test]
        fn test_find_instrument() {
            // Initialise SubscriptionId-InstrumentKey HashMap
            let ids = Map(FnvHashMap::from_iter([(
                SubscriptionId::from("present"),
                MarketDataInstrument::from(("base", "quote", MarketDataInstrumentKind::Spot)),
            )]));

            struct TestCase {
                input: SubscriptionId,
                expected: Result<MarketDataInstrument, SocketError>,
            }

            let cases = vec![
                TestCase {
                    // TC0: SubscriptionId (channel) is present in the HashMap
                    input: SubscriptionId::from("present"),
                    expected: Ok(MarketDataInstrument::from((
                        "base",
                        "quote",
                        MarketDataInstrumentKind::Spot,
                    ))),
                },
                TestCase {
                    // TC1: SubscriptionId (channel) is not present in the HashMap
                    input: SubscriptionId::from("not present"),
                    expected: Err(SocketError::Unidentifiable(SubscriptionId::from(
                        "not present",
                    ))),
                },
            ];

            for (index, test) in cases.into_iter().enumerate() {
                let actual = ids.find(&test.input);
                match (actual, test.expected) {
                    (Ok(actual), Ok(expected)) => {
                        assert_eq!(*actual, expected, "TC{} failed", index)
                    }
                    (Err(_), Err(_)) => {
                        // Test passed
                    }
                    (actual, expected) => {
                        // Test failed
                        panic!(
                            "TC{index} failed because actual != expected. \nActual: {actual:?}\nExpected: {expected:?}\n"
                        );
                    }
                }
            }
        }
    }
}
