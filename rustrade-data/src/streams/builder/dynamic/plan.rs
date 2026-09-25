//! Routing each `(ExchangeId, SubKind)` group of a dynamic batch to the connector that serves it.
//!
//! Routing is synchronous and separate from connecting: every group of every batch is routed before
//! any is polled, so a group nothing serves — or one missing the subscriber or the cargo feature it
//! needs — fails the whole initialisation with nothing opened.

use super::DynamicSubscribers;
#[cfg(feature = "hyperliquid")]
use crate::exchange::hyperliquid::Hyperliquid;
#[cfg(feature = "lse")]
use crate::exchange::lse::{
    LseCfd, LseCrypto, LseEquities, LseFutures, LseFx, LseOptions, live::LseSubscriber,
};
use crate::{
    Identifier,
    error::DataError,
    exchange::{
        StreamSelector,
        binance::{
            futures::{BinanceFuturesUsd, BinanceFuturesUsdMarket},
            market::BinanceMarket,
            spot::BinanceSpot,
        },
        bitfinex::{Bitfinex, market::BitfinexMarket},
        bitmex::{Bitmex, market::BitmexMarket},
        bybit::{futures::BybitPerpetualsUsd, market::BybitMarket, spot::BybitSpot},
        coinbase::{Coinbase, market::CoinbaseMarket},
        gateio::{
            future::{GateioFuturesBtc, GateioFuturesUsd},
            market::GateioMarket,
            option::GateioOptions,
            perpetual::{GateioPerpetualsBtc, GateioPerpetualsUsd},
            spot::GateioSpot,
        },
        kraken::{Kraken, market::KrakenMarket},
        okx::{Okx, market::OkxMarket},
    },
    instrument::InstrumentData,
    streams::{
        consumer::{MarketStreamResult, STREAM_RECONNECTION_POLICY, init_market_stream},
        reconnect::stream::ReconnectingStream,
    },
    subscriber::WebSocketSubscriber,
    subscription::{
        SubKind, Subscription, SubscriptionKind,
        book::{OrderBookEvent, OrderBookL1, OrderBooksL1, OrderBooksL2},
        candle::{Candle, Candles},
        liquidation::{Liquidation, Liquidations},
        trade::{PublicTrade, PublicTrades},
    },
};
use fnv::FnvHashMap;
use futures::future::BoxFuture;
use rustrade_instrument::exchange::ExchangeId;
use rustrade_integration::channel::UnboundedTx;
use std::fmt::{Debug, Display};
use tokio::task::JoinHandle;

/// Initialises one group's stream, then resolves to the task forwarding it into its channel.
pub type GroupFuture = BoxFuture<'static, Result<JoinHandle<()>, DataError>>;

/// The sender each `(ExchangeId, kind)` channel is fed through.
#[derive(Debug)]
pub struct Txs<InstrumentKey> {
    pub trades: FnvHashMap<ExchangeId, UnboundedTx<MarketStreamResult<InstrumentKey, PublicTrade>>>,
    pub l1s: FnvHashMap<ExchangeId, UnboundedTx<MarketStreamResult<InstrumentKey, OrderBookL1>>>,
    pub l2s: FnvHashMap<ExchangeId, UnboundedTx<MarketStreamResult<InstrumentKey, OrderBookEvent>>>,
    pub liquidations:
        FnvHashMap<ExchangeId, UnboundedTx<MarketStreamResult<InstrumentKey, Liquidation>>>,
    pub candles: FnvHashMap<ExchangeId, UnboundedTx<MarketStreamResult<InstrumentKey, Candle>>>,
}

impl<InstrumentKey> Default for Txs<InstrumentKey> {
    fn default() -> Self {
        Self {
            trades: Default::default(),
            l1s: Default::default(),
            l2s: Default::default(),
            liquidations: Default::default(),
            candles: Default::default(),
        }
    }
}

/// Implied by [`DynamicInstrument`](super::DynamicInstrument); the crate's only implementation is
/// the blanket one below, which states every connector's identifier bound in one place.
pub trait Route: InstrumentData + Ord + Display + Send + Sync + Sized + 'static {
    /// Route one group — every subscription in it shares `exchange` and `sub_kind` — to the
    /// connector serving it, returning the future that initialises its stream without polling it.
    ///
    /// # Errors
    /// - [`DataError::Unsupported`] if no connector serves the pair.
    /// - [`DataError::SubscriberRequired`] if its connector needs a subscriber `subscribers` lacks.
    /// - [`DataError::FeatureDisabled`] if its connector is behind a cargo feature this build lacks.
    fn route(
        exchange: ExchangeId,
        sub_kind: SubKind,
        subscriptions: Vec<Subscription<ExchangeId, Self, SubKind>>,
        txs: &Txs<Self::Key>,
        subscribers: &DynamicSubscribers,
    ) -> Result<GroupFuture, DataError>;
}

impl<Instrument> Route for Instrument
where
    Instrument: InstrumentData
        + Ord
        + Display
        + Send
        + Sync
        + 'static
        + super::DynamicLseInstrument
        + super::DynamicHyperliquidInstrument,
    Instrument::Key: Debug + Clone + PartialEq + Send + Sync + 'static,
    Subscription<BinanceSpot, Instrument, PublicTrades>: Identifier<BinanceMarket>,
    Subscription<BinanceSpot, Instrument, OrderBooksL1>: Identifier<BinanceMarket>,
    Subscription<BinanceSpot, Instrument, OrderBooksL2>: Identifier<BinanceMarket>,
    Subscription<BinanceFuturesUsd, Instrument, PublicTrades>: Identifier<BinanceMarket>,
    Subscription<BinanceFuturesUsd, Instrument, OrderBooksL1>: Identifier<BinanceMarket>,
    Subscription<BinanceFuturesUsd, Instrument, OrderBooksL2>: Identifier<BinanceMarket>,
    Subscription<BinanceFuturesUsdMarket, Instrument, Liquidations>: Identifier<BinanceMarket>,
    Subscription<BinanceSpot, Instrument, Candles>: Identifier<BinanceMarket>,
    Subscription<BinanceFuturesUsdMarket, Instrument, Candles>: Identifier<BinanceMarket>,
    Subscription<Bitfinex, Instrument, PublicTrades>: Identifier<BitfinexMarket>,
    Subscription<Bitmex, Instrument, PublicTrades>: Identifier<BitmexMarket>,
    Subscription<BybitSpot, Instrument, PublicTrades>: Identifier<BybitMarket>,
    Subscription<BybitSpot, Instrument, OrderBooksL1>: Identifier<BybitMarket>,
    Subscription<BybitSpot, Instrument, OrderBooksL2>: Identifier<BybitMarket>,
    Subscription<BybitPerpetualsUsd, Instrument, PublicTrades>: Identifier<BybitMarket>,
    Subscription<BybitPerpetualsUsd, Instrument, OrderBooksL1>: Identifier<BybitMarket>,
    Subscription<BybitPerpetualsUsd, Instrument, OrderBooksL2>: Identifier<BybitMarket>,
    Subscription<Coinbase, Instrument, PublicTrades>: Identifier<CoinbaseMarket>,
    Subscription<GateioSpot, Instrument, PublicTrades>: Identifier<GateioMarket>,
    Subscription<GateioFuturesUsd, Instrument, PublicTrades>: Identifier<GateioMarket>,
    Subscription<GateioFuturesBtc, Instrument, PublicTrades>: Identifier<GateioMarket>,
    Subscription<GateioPerpetualsUsd, Instrument, PublicTrades>: Identifier<GateioMarket>,
    Subscription<GateioPerpetualsBtc, Instrument, PublicTrades>: Identifier<GateioMarket>,
    Subscription<GateioOptions, Instrument, PublicTrades>: Identifier<GateioMarket>,
    Subscription<Kraken, Instrument, PublicTrades>: Identifier<KrakenMarket>,
    Subscription<Kraken, Instrument, OrderBooksL1>: Identifier<KrakenMarket>,
    Subscription<Okx, Instrument, PublicTrades>: Identifier<OkxMarket>,
{
    fn route(
        exchange: ExchangeId,
        sub_kind: SubKind,
        subscriptions: Vec<Subscription<ExchangeId, Self, SubKind>>,
        txs: &Txs<Self::Key>,
        subscribers: &DynamicSubscribers,
    ) -> Result<GroupFuture, DataError> {
        use SubKind::{OrderBooksL1 as L1, OrderBooksL2 as L2, PublicTrades as Trades};

        // Only the London Strategic Edge arms take a subscriber from the caller.
        #[cfg(not(feature = "lse"))]
        let _ = subscribers;

        let subs = subscriptions;
        let ws = WebSocketSubscriber;

        Ok(match (exchange, sub_kind) {
            (ExchangeId::BinanceSpot, Trades) => group(
                ws,
                BinanceSpot::default(),
                PublicTrades,
                subs,
                tx(&txs.trades, exchange),
            ),
            (ExchangeId::BinanceSpot, L1) => group(
                ws,
                BinanceSpot::default(),
                OrderBooksL1,
                subs,
                tx(&txs.l1s, exchange),
            ),
            (ExchangeId::BinanceSpot, L2) => group(
                ws,
                BinanceSpot::default(),
                OrderBooksL2,
                subs,
                tx(&txs.l2s, exchange),
            ),
            (ExchangeId::BinanceFuturesUsd, Trades) => group(
                ws,
                BinanceFuturesUsd::default(),
                PublicTrades,
                subs,
                tx(&txs.trades, exchange),
            ),
            (ExchangeId::BinanceFuturesUsd, L1) => group(
                ws,
                BinanceFuturesUsd::default(),
                OrderBooksL1,
                subs,
                tx(&txs.l1s, exchange),
            ),
            (ExchangeId::BinanceFuturesUsd, L2) => group(
                ws,
                BinanceFuturesUsd::default(),
                OrderBooksL2,
                subs,
                tx(&txs.l2s, exchange),
            ),
            // `@forceOrder` is a `/market`-tier stream, so the market-tier server type is used for
            // its `/market/ws` URL. Both server types share the `BinanceFuturesUsd` id, so the
            // output still reaches its channel.
            (ExchangeId::BinanceFuturesUsd, SubKind::Liquidations) => group(
                ws,
                BinanceFuturesUsdMarket::default(),
                Liquidations,
                subs,
                tx(&txs.liquidations, exchange),
            ),
            // A group shares one `SubKind`, so its `interval` is every subscription's interval.
            (ExchangeId::BinanceSpot, SubKind::Candles { interval }) => group(
                ws,
                BinanceSpot::default(),
                Candles { interval },
                subs,
                tx(&txs.candles, exchange),
            ),
            // Futures klines are `/market`-tier too; see the liquidations arm.
            (ExchangeId::BinanceFuturesUsd, SubKind::Candles { interval }) => group(
                ws,
                BinanceFuturesUsdMarket::default(),
                Candles { interval },
                subs,
                tx(&txs.candles, exchange),
            ),
            (ExchangeId::Bitfinex, Trades) => {
                group(ws, Bitfinex, PublicTrades, subs, tx(&txs.trades, exchange))
            }
            (ExchangeId::Bitmex, Trades) => {
                group(ws, Bitmex, PublicTrades, subs, tx(&txs.trades, exchange))
            }
            (ExchangeId::BybitSpot, Trades) => group(
                ws,
                BybitSpot::default(),
                PublicTrades,
                subs,
                tx(&txs.trades, exchange),
            ),
            (ExchangeId::BybitSpot, L1) => group(
                ws,
                BybitSpot::default(),
                OrderBooksL1,
                subs,
                tx(&txs.l1s, exchange),
            ),
            (ExchangeId::BybitSpot, L2) => group(
                ws,
                BybitSpot::default(),
                OrderBooksL2,
                subs,
                tx(&txs.l2s, exchange),
            ),
            (ExchangeId::BybitPerpetualsUsd, Trades) => group(
                ws,
                BybitPerpetualsUsd::default(),
                PublicTrades,
                subs,
                tx(&txs.trades, exchange),
            ),
            (ExchangeId::BybitPerpetualsUsd, L1) => group(
                ws,
                BybitPerpetualsUsd::default(),
                OrderBooksL1,
                subs,
                tx(&txs.l1s, exchange),
            ),
            (ExchangeId::BybitPerpetualsUsd, L2) => group(
                ws,
                BybitPerpetualsUsd::default(),
                OrderBooksL2,
                subs,
                tx(&txs.l2s, exchange),
            ),
            (ExchangeId::Coinbase, Trades) => {
                group(ws, Coinbase, PublicTrades, subs, tx(&txs.trades, exchange))
            }
            (ExchangeId::GateioSpot, Trades) => group(
                ws,
                GateioSpot::default(),
                PublicTrades,
                subs,
                tx(&txs.trades, exchange),
            ),
            (ExchangeId::GateioFuturesUsd, Trades) => group(
                ws,
                GateioFuturesUsd::default(),
                PublicTrades,
                subs,
                tx(&txs.trades, exchange),
            ),
            (ExchangeId::GateioFuturesBtc, Trades) => group(
                ws,
                GateioFuturesBtc::default(),
                PublicTrades,
                subs,
                tx(&txs.trades, exchange),
            ),
            (ExchangeId::GateioPerpetualsUsd, Trades) => group(
                ws,
                GateioPerpetualsUsd::default(),
                PublicTrades,
                subs,
                tx(&txs.trades, exchange),
            ),
            (ExchangeId::GateioPerpetualsBtc, Trades) => group(
                ws,
                GateioPerpetualsBtc::default(),
                PublicTrades,
                subs,
                tx(&txs.trades, exchange),
            ),
            (ExchangeId::GateioOptions, Trades) => group(
                ws,
                GateioOptions::default(),
                PublicTrades,
                subs,
                tx(&txs.trades, exchange),
            ),
            (ExchangeId::Kraken, Trades) => {
                group(ws, Kraken, PublicTrades, subs, tx(&txs.trades, exchange))
            }
            (ExchangeId::Kraken, L1) => {
                group(ws, Kraken, OrderBooksL1, subs, tx(&txs.l1s, exchange))
            }
            (ExchangeId::Okx, Trades) => {
                group(ws, Okx, PublicTrades, subs, tx(&txs.trades, exchange))
            }

            #[cfg(feature = "hyperliquid")]
            (ExchangeId::HyperliquidPerp, Trades) => group(
                ws,
                Hyperliquid,
                PublicTrades,
                subs,
                tx(&txs.trades, exchange),
            ),
            #[cfg(feature = "hyperliquid")]
            (ExchangeId::HyperliquidPerp, L2) => {
                group(ws, Hyperliquid, OrderBooksL2, subs, tx(&txs.l2s, exchange))
            }
            #[cfg(not(feature = "hyperliquid"))]
            (ExchangeId::HyperliquidPerp, Trades | L2) => {
                return Err(feature_disabled(exchange, "hyperliquid"));
            }

            // Every London Strategic Edge group takes a clone of the ONE subscriber supplied, so
            // they all share its connection: the provider allows a key a single socket.
            #[cfg(feature = "lse")]
            (ExchangeId::LseFx, Trades) => group(
                lse(subscribers, exchange)?,
                LseFx::default(),
                PublicTrades,
                subs,
                tx(&txs.trades, exchange),
            ),
            #[cfg(feature = "lse")]
            (ExchangeId::LseFx, L1) => group(
                lse(subscribers, exchange)?,
                LseFx::default(),
                OrderBooksL1,
                subs,
                tx(&txs.l1s, exchange),
            ),
            #[cfg(feature = "lse")]
            (ExchangeId::LseCrypto, Trades) => group(
                lse(subscribers, exchange)?,
                LseCrypto::default(),
                PublicTrades,
                subs,
                tx(&txs.trades, exchange),
            ),
            #[cfg(feature = "lse")]
            (ExchangeId::LseCrypto, L1) => group(
                lse(subscribers, exchange)?,
                LseCrypto::default(),
                OrderBooksL1,
                subs,
                tx(&txs.l1s, exchange),
            ),
            #[cfg(feature = "lse")]
            (ExchangeId::LseEquities, Trades) => group(
                lse(subscribers, exchange)?,
                LseEquities::default(),
                PublicTrades,
                subs,
                tx(&txs.trades, exchange),
            ),
            #[cfg(feature = "lse")]
            (ExchangeId::LseEquities, L1) => group(
                lse(subscribers, exchange)?,
                LseEquities::default(),
                OrderBooksL1,
                subs,
                tx(&txs.l1s, exchange),
            ),
            #[cfg(feature = "lse")]
            (ExchangeId::LseFutures, Trades) => group(
                lse(subscribers, exchange)?,
                LseFutures::default(),
                PublicTrades,
                subs,
                tx(&txs.trades, exchange),
            ),
            #[cfg(feature = "lse")]
            (ExchangeId::LseFutures, L1) => group(
                lse(subscribers, exchange)?,
                LseFutures::default(),
                OrderBooksL1,
                subs,
                tx(&txs.l1s, exchange),
            ),
            #[cfg(feature = "lse")]
            (ExchangeId::LseCfd, Trades) => group(
                lse(subscribers, exchange)?,
                LseCfd::default(),
                PublicTrades,
                subs,
                tx(&txs.trades, exchange),
            ),
            #[cfg(feature = "lse")]
            (ExchangeId::LseCfd, L1) => group(
                lse(subscribers, exchange)?,
                LseCfd::default(),
                OrderBooksL1,
                subs,
                tx(&txs.l1s, exchange),
            ),
            // Options publish no quote, so there is no L1 arm; the support matrix refuses one.
            #[cfg(feature = "lse")]
            (ExchangeId::LseOptions, Trades) => group(
                lse(subscribers, exchange)?,
                LseOptions::default(),
                PublicTrades,
                subs,
                tx(&txs.trades, exchange),
            ),
            #[cfg(not(feature = "lse"))]
            (
                ExchangeId::LseFx
                | ExchangeId::LseCrypto
                | ExchangeId::LseEquities
                | ExchangeId::LseFutures
                | ExchangeId::LseCfd,
                Trades | L1,
            )
            | (ExchangeId::LseOptions, Trades) => {
                return Err(feature_disabled(exchange, "lse"));
            }

            (exchange, sub_kind) => return Err(DataError::Unsupported { exchange, sub_kind }),
        })
    }
}

/// Build the future initialising one group's stream on `exchange` and forwarding it into `tx`.
fn group<Exchange, Instrument, Kind>(
    subscriber: Exchange::Subscriber,
    exchange: Exchange,
    kind: Kind,
    subscriptions: Vec<Subscription<ExchangeId, Instrument, SubKind>>,
    tx: UnboundedTx<MarketStreamResult<Instrument::Key, Kind::Event>>,
) -> GroupFuture
where
    Exchange: StreamSelector<Instrument, Kind> + Clone + Send + Sync + 'static,
    Instrument: InstrumentData + Display + Send + Sync + 'static,
    Instrument::Key: Send + 'static,
    Kind: SubscriptionKind + Copy + Display + Send + Sync + 'static,
    Kind::Event: Clone + Send + 'static,
    Subscription<Exchange, Instrument, Kind>:
        Identifier<Exchange::Channel> + Identifier<Exchange::Market>,
{
    let subscriptions = subscriptions
        .into_iter()
        .map(|sub| Subscription::new(exchange.clone(), sub.instrument, kind))
        .collect();

    Box::pin(async move {
        let stream =
            init_market_stream(STREAM_RECONNECTION_POLICY, subscriber, subscriptions).await?;
        Ok(tokio::spawn(stream.forward_to(tx)))
    })
}

/// The sender for `exchange`'s channel of one kind.
#[allow(clippy::unwrap_used)] // Invariant: `Channels::try_from` opens a channel for every (exchange, kind) in the batches, and groups are drawn from the same batches
fn tx<Tx: Clone>(txs: &FnvHashMap<ExchangeId, Tx>, exchange: ExchangeId) -> Tx {
    txs.get(&exchange).unwrap().clone()
}

/// A clone of the London Strategic Edge subscriber, sharing its connection.
#[cfg(feature = "lse")]
fn lse(subscribers: &DynamicSubscribers, exchange: ExchangeId) -> Result<LseSubscriber, DataError> {
    subscribers
        .lse
        .clone()
        .ok_or(DataError::SubscriberRequired { exchange })
}

#[cfg(any(not(feature = "lse"), not(feature = "hyperliquid")))]
fn feature_disabled(exchange: ExchangeId, feature: &str) -> DataError {
    DataError::FeatureDisabled {
        exchange,
        feature: feature.to_owned(),
    }
}
