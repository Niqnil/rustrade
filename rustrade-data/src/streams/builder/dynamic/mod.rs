#[cfg(feature = "hyperliquid")]
use crate::exchange::hyperliquid::market::HyperliquidInstrument;
#[cfg(feature = "lse")]
use crate::exchange::lse::{live::LseSubscriber, market::LseInstrument};
use crate::{
    error::DataError,
    instrument::InstrumentData,
    streams::consumer::MarketStreamResult,
    subscription::{
        SubKind, Subscription,
        book::{OrderBookEvent, OrderBookL1},
        candle::Candle,
        liquidation::Liquidation,
        trade::PublicTrade,
    },
};
use fnv::FnvHashMap;
use futures::{Stream, stream::SelectAll};
use futures_util::{StreamExt, future::join_all};
use itertools::Itertools;
use plan::{Route, Txs};
use rustrade_instrument::exchange::ExchangeId;
use rustrade_integration::{
    Validator,
    channel::{UnboundedRx, mpsc_unbounded},
    error::SocketError,
};
use std::fmt::Debug;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::UnboundedReceiverStream;
use vecmap::VecMap;

pub mod indexed;

mod plan;

#[derive(Debug)]
pub struct DynamicStreams<InstrumentKey> {
    pub trades:
        VecMap<ExchangeId, UnboundedReceiverStream<MarketStreamResult<InstrumentKey, PublicTrade>>>,
    pub l1s:
        VecMap<ExchangeId, UnboundedReceiverStream<MarketStreamResult<InstrumentKey, OrderBookL1>>>,
    pub l2s: VecMap<
        ExchangeId,
        UnboundedReceiverStream<MarketStreamResult<InstrumentKey, OrderBookEvent>>,
    >,
    pub liquidations:
        VecMap<ExchangeId, UnboundedReceiverStream<MarketStreamResult<InstrumentKey, Liquidation>>>,
    pub candles:
        VecMap<ExchangeId, UnboundedReceiverStream<MarketStreamResult<InstrumentKey, Candle>>>,
}

/// The subscribers [`DynamicStreams`] cannot build for itself.
///
/// Most venues are served by a stateless [`WebSocketSubscriber`](crate::subscriber::WebSocketSubscriber),
/// which [`DynamicStreams`] constructs on its own. A venue whose subscriber carries credentials —
/// or shares one connection among its streams — needs one built by the caller and supplied here,
/// through [`DynamicStreams::init_with`]. A subscription to such a venue with no subscriber
/// supplied fails with [`DataError::SubscriberRequired`].
///
/// Venues that need one:
/// - **London Strategic Edge** (`lse` feature): every `Lse*` dataset.
///
/// # Example
/// ```ignore
/// use rustrade_data::exchange::lse::live::LseSubscriber;
/// use rustrade_data::streams::builder::dynamic::{DynamicStreams, DynamicSubscribers};
/// use rustrade_data::subscription::SubKind;
/// use rustrade_instrument::{
///     exchange::ExchangeId, instrument::market_data::kind::MarketDataInstrumentKind,
/// };
///
/// let subscribers = DynamicSubscribers::default().with_lse(LseSubscriber::from_env()?);
///
/// // Two datasets and both kinds: four streams, one connection.
/// let streams = DynamicStreams::init_with(&subscribers, [[
///     (ExchangeId::LseCrypto, "btc", "usd", MarketDataInstrumentKind::Spot, SubKind::PublicTrades),
///     (ExchangeId::LseCrypto, "btc", "usd", MarketDataInstrumentKind::Spot, SubKind::OrderBooksL1),
///     (ExchangeId::LseEquities, "aapl", "usd", MarketDataInstrumentKind::Spot, SubKind::PublicTrades),
///     (ExchangeId::LseEquities, "aapl", "usd", MarketDataInstrumentKind::Spot, SubKind::OrderBooksL1),
/// ]])
/// .await?;
/// ```
#[derive(Clone, Debug, Default)]
pub struct DynamicSubscribers {
    #[cfg(feature = "lse")]
    lse: Option<LseSubscriber>,
}

impl DynamicSubscribers {
    /// Serve every London Strategic Edge subscription — any dataset, either kind — from clones of
    /// `subscriber`.
    ///
    /// Clones share the subscriber's one connection, which is the only way several streams can
    /// coexist on this provider: it allows a key a single WebSocket and refuses a second with
    /// `TOO_MANY_CONNECTIONS`. So every `Lse*` group of every batch attaches to the same socket,
    /// and so should any other stream the caller opens for the same key — pass it a clone of this
    /// subscriber rather than building another.
    ///
    /// # Resumption
    /// Configure it with [`LseSubscriber::with_resume`] *before* passing it here to resume every
    /// London Strategic Edge stream this call opens; the one state then serves them all. Streams
    /// are keyed by dataset, symbol and kind, and [`DynamicStreams::init_with`] removes duplicates
    /// only within a batch — so repeating one subscription across two batches opens two streams
    /// sharing one watermark, which [`LseSubscriber::with_resume`] warns against.
    ///
    /// # ⚠️ The data is not redistributable
    /// London Strategic Edge permits use for your own research, trading and model training, but
    /// prohibits redistributing the data it serves. See
    /// [`exchange::lse`](crate::exchange::lse) and <https://londonstrategicedge.com/terms>.
    #[cfg(feature = "lse")]
    #[must_use]
    pub fn with_lse(mut self, subscriber: LseSubscriber) -> Self {
        self.lse = Some(subscriber);
        self
    }
}

/// An instrument type every venue [`DynamicStreams`] routes to can subscribe with.
///
/// Implemented for [`MarketDataInstrument`](rustrade_instrument::instrument::market_data::MarketDataInstrument),
/// [`Keyed`](rustrade_instrument::Keyed) over it, and
/// [`MarketInstrumentData`](crate::instrument::MarketInstrumentData): the representations every
/// connector can name a market for. It stands for the whole set of per-connector identifier
/// bounds, so a caller generic over the instrument states this one bound rather than one per
/// connector and kind.
///
/// Which connectors it covers depends on the cargo features enabled: with `lse` it also requires
/// [`LseInstrument`], with `hyperliquid` [`HyperliquidInstrument`]. The three types above satisfy
/// every combination.
///
/// # Only this crate's instrument types implement it
/// It is sealed: its supertrait lives in a private module, so no crate but this one can name it
/// or implement it. This crate implements that supertrait once, as a blanket impl requiring an
/// `Identifier<ExchangeMarket>` impl for a `Subscription` over the type, per connector and kind.
/// A downstream crate cannot write those impls for a type of its own either: the coherence rules
/// leave them to the crate that owns `Identifier`, `Subscription` and the market types, which is
/// this one. So the set of qualifying types is exactly the three above, in every build, whichever
/// features any crate in the build enables. A custom instrument type streams through the typed
/// [`Streams`](crate::streams::Streams) builder instead, for a connector whose identifier it can
/// provide: [`LseInstrument`] is implementable for a downstream type, for example.
pub trait DynamicInstrument: Route {}

impl<Instrument> DynamicInstrument for Instrument where Instrument: Route {}

/// Requires [`LseInstrument`] when the `lse` feature is enabled, and nothing otherwise.
///
/// A where-clause bound cannot be `cfg`-gated on stable Rust, but a supertrait of a trait declared
/// twice can: this is how [`DynamicInstrument`] requires London Strategic Edge support only in
/// builds that have it.
#[cfg(feature = "lse")]
pub trait DynamicLseInstrument: LseInstrument {}

#[cfg(feature = "lse")]
impl<Instrument> DynamicLseInstrument for Instrument where Instrument: LseInstrument {}

/// Requires `LseInstrument` when the `lse` feature is enabled, and nothing otherwise.
#[cfg(not(feature = "lse"))]
pub trait DynamicLseInstrument {}

#[cfg(not(feature = "lse"))]
impl<Instrument> DynamicLseInstrument for Instrument {}

/// Requires [`HyperliquidInstrument`] when the `hyperliquid` feature is enabled, and nothing
/// otherwise. See [`DynamicLseInstrument`] for why this is a trait.
#[cfg(feature = "hyperliquid")]
pub trait DynamicHyperliquidInstrument: HyperliquidInstrument {}

#[cfg(feature = "hyperliquid")]
impl<Instrument> DynamicHyperliquidInstrument for Instrument where Instrument: HyperliquidInstrument {}

/// Requires `HyperliquidInstrument` when the `hyperliquid` feature is enabled, and nothing
/// otherwise.
#[cfg(not(feature = "hyperliquid"))]
pub trait DynamicHyperliquidInstrument {}

#[cfg(not(feature = "hyperliquid"))]
impl<Instrument> DynamicHyperliquidInstrument for Instrument {}

impl<InstrumentKey> DynamicStreams<InstrumentKey> {
    /// Initialise a set of `Streams` by providing one or more [`Subscription`] batches, for venues
    /// that need no subscriber from the caller.
    ///
    /// Equivalent to [`init_with`](Self::init_with) with no [`DynamicSubscribers`]: a subscription
    /// to a venue that needs one fails with [`DataError::SubscriberRequired`].
    ///
    /// ## Examples
    /// Please see rustrade-data-rs/examples/dynamic_multi_stream_multi_exchange.rs for a
    /// comprehensive example of how to use this market data stream initialiser.
    pub async fn init<SubBatchIter, SubIter, Sub, Instrument>(
        subscription_batches: SubBatchIter,
    ) -> Result<Self, DataError>
    where
        SubBatchIter: IntoIterator<Item = SubIter>,
        SubIter: IntoIterator<Item = Sub>,
        Sub: Into<Subscription<ExchangeId, Instrument, SubKind>>,
        Instrument: DynamicInstrument + InstrumentData<Key = InstrumentKey>,
        InstrumentKey: Debug + Clone + PartialEq + Send + Sync + 'static,
    {
        Self::init_with(&DynamicSubscribers::default(), subscription_batches).await
    }

    /// Initialise a set of `Streams` by providing one or more [`Subscription`] batches, and the
    /// subscribers for venues that cannot be served without one.
    ///
    /// Each batch (ie/ `impl Iterator<Item = Subscription>`) will initialise at-least-one
    /// `Stream` under the hood. If the batch contains more-than-one [`ExchangeId`] and/or
    /// [`SubKind`], it will be further split under the hood for compile-time reasons.
    ///
    /// # Streams sharing a connection
    /// A venue supplied through `subscribers` receives a clone of that subscriber for every group,
    /// whichever batch it came from. Where clones share a connection — London Strategic Edge —
    /// every group on that venue shares it, across datasets and kinds.
    ///
    /// # Errors
    /// Nothing is connected until every group has been routed, so these fail the call with no
    /// connection opened:
    /// - a subscription the support matrix refuses (see
    ///   [`exchange_supports_instrument_kind_sub_kind`](crate::subscription::exchange_supports_instrument_kind_sub_kind));
    /// - [`DataError::Unsupported`] for a pair the matrix accepts but no connector here serves;
    /// - [`DataError::SubscriberRequired`] for a venue `subscribers` has no subscriber for;
    /// - [`DataError::FeatureDisabled`] for a venue whose cargo feature this build lacks.
    ///
    /// Every group is then initialised concurrently. If any fails, the call waits for the rest to
    /// finish initialising, stops every stream that succeeded, and returns the first error — so a
    /// failed call leaves no stream running: each is dropped before the call returns.
    ///
    /// A dropped London Strategic Edge stream asks its connection to release its share, and the
    /// connection does so asynchronously, closing the socket once no stream remains. A retry
    /// through the same subscriber, or a clone of it, reuses that connection whatever state it is
    /// in. A retry through a subscriber built separately for the same key may still be refused
    /// with `TOO_MANY_CONNECTIONS` until the socket has closed.
    ///
    /// ## Examples
    /// Please see rustrade-data-rs/examples/dynamic_multi_stream_multi_exchange.rs for a
    /// comprehensive example of how to use this market data stream initialiser.
    pub async fn init_with<SubBatchIter, SubIter, Sub, Instrument>(
        subscribers: &DynamicSubscribers,
        subscription_batches: SubBatchIter,
    ) -> Result<Self, DataError>
    where
        SubBatchIter: IntoIterator<Item = SubIter>,
        SubIter: IntoIterator<Item = Sub>,
        Sub: Into<Subscription<ExchangeId, Instrument, SubKind>>,
        Instrument: DynamicInstrument + InstrumentData<Key = InstrumentKey>,
        InstrumentKey: Debug + Clone + PartialEq + Send + Sync + 'static,
    {
        // Validate & dedup Subscription batches
        let batches = validate_batches(subscription_batches)?;

        // Generate required Channels from Subscription batches
        let Channels { txs, rxs } = Channels::try_from(&batches)?;

        let groups = route_batches(batches, &txs, subscribers)?;

        settle(join_all(groups).await).await?;

        Ok(Self {
            trades: rxs
                .trades
                .into_iter()
                .map(|(exchange, rx)| (exchange, rx.into_stream()))
                .collect(),
            l1s: rxs
                .l1s
                .into_iter()
                .map(|(exchange, rx)| (exchange, rx.into_stream()))
                .collect(),
            l2s: rxs
                .l2s
                .into_iter()
                .map(|(exchange, rx)| (exchange, rx.into_stream()))
                .collect(),
            liquidations: rxs
                .liquidations
                .into_iter()
                .map(|(exchange, rx)| (exchange, rx.into_stream()))
                .collect(),
            candles: rxs
                .candles
                .into_iter()
                .map(|(exchange, rx)| (exchange, rx.into_stream()))
                .collect(),
        })
    }

    /// Remove an exchange [`PublicTrade`] `Stream` from the [`DynamicStreams`] collection.
    ///
    /// Note that calling this method will permanently remove this `Stream` from [`Self`].
    pub fn select_trades(
        &mut self,
        exchange: ExchangeId,
    ) -> Option<UnboundedReceiverStream<MarketStreamResult<InstrumentKey, PublicTrade>>> {
        self.trades.remove(&exchange)
    }

    /// Select and merge every exchange [`PublicTrade`] `Stream` using
    /// [`SelectAll`](futures_util::stream::select_all::select_all).
    pub fn select_all_trades(
        &mut self,
    ) -> SelectAll<UnboundedReceiverStream<MarketStreamResult<InstrumentKey, PublicTrade>>> {
        futures_util::stream::select_all::select_all(std::mem::take(&mut self.trades).into_values())
    }

    /// Remove an exchange [`OrderBookL1`] `Stream` from the [`DynamicStreams`] collection.
    ///
    /// Note that calling this method will permanently remove this `Stream` from [`Self`].
    pub fn select_l1s(
        &mut self,
        exchange: ExchangeId,
    ) -> Option<UnboundedReceiverStream<MarketStreamResult<InstrumentKey, OrderBookL1>>> {
        self.l1s.remove(&exchange)
    }

    /// Select and merge every exchange [`OrderBookL1`] `Stream` using
    /// [`SelectAll`](futures_util::stream::select_all::select_all).
    pub fn select_all_l1s(
        &mut self,
    ) -> SelectAll<UnboundedReceiverStream<MarketStreamResult<InstrumentKey, OrderBookL1>>> {
        futures_util::stream::select_all::select_all(std::mem::take(&mut self.l1s).into_values())
    }

    /// Remove an exchange [`OrderBookEvent`] `Stream` from the [`DynamicStreams`] collection.
    ///
    /// Note that calling this method will permanently remove this `Stream` from [`Self`].
    pub fn select_l2s(
        &mut self,
        exchange: ExchangeId,
    ) -> Option<UnboundedReceiverStream<MarketStreamResult<InstrumentKey, OrderBookEvent>>> {
        self.l2s.remove(&exchange)
    }

    /// Select and merge every exchange [`OrderBookEvent`] `Stream` using
    /// [`SelectAll`](futures_util::stream::select_all::select_all).
    pub fn select_all_l2s(
        &mut self,
    ) -> SelectAll<UnboundedReceiverStream<MarketStreamResult<InstrumentKey, OrderBookEvent>>> {
        futures_util::stream::select_all::select_all(std::mem::take(&mut self.l2s).into_values())
    }

    /// Remove an exchange [`Liquidation`] `Stream` from the [`DynamicStreams`] collection.
    ///
    /// Note that calling this method will permanently remove this `Stream` from [`Self`].
    pub fn select_liquidations(
        &mut self,
        exchange: ExchangeId,
    ) -> Option<UnboundedReceiverStream<MarketStreamResult<InstrumentKey, Liquidation>>> {
        self.liquidations.remove(&exchange)
    }

    /// Select and merge every exchange [`Liquidation`] `Stream` using
    /// [`SelectAll`](futures_util::stream::select_all::select_all).
    pub fn select_all_liquidations(
        &mut self,
    ) -> SelectAll<UnboundedReceiverStream<MarketStreamResult<InstrumentKey, Liquidation>>> {
        futures_util::stream::select_all::select_all(
            std::mem::take(&mut self.liquidations).into_values(),
        )
    }

    /// Remove an exchange [`Candle`] `Stream` from the [`DynamicStreams`] collection.
    ///
    /// Note that calling this method will permanently remove this `Stream` from [`Self`].
    ///
    /// # Mixed intervals
    ///
    /// `DynamicStreams` keys candle streams by [`ExchangeId`] only, so subscribing to
    /// multiple intervals on one exchange (e.g. `Candles { Min1 }` + `Candles { Hour1 }`
    /// on `BinanceSpot`) merges them into this single per-exchange stream. Each distinct
    /// interval is a separate upstream WebSocket subscription (mind venue per-connection
    /// stream limits at high symbol × interval fan-out). Because [`Candle`] carries no
    /// `interval` field, a consumer mixing intervals must recover it from `close_time`
    /// spacing or the originating subscription; if per-interval routing matters, use the
    /// typed [`Streams`](crate::streams::Streams) builder with one
    /// [`StreamSelector`](crate::exchange::StreamSelector) per interval.
    pub fn select_candles(
        &mut self,
        exchange: ExchangeId,
    ) -> Option<UnboundedReceiverStream<MarketStreamResult<InstrumentKey, Candle>>> {
        self.candles.remove(&exchange)
    }

    /// Select and merge every exchange [`Candle`] `Stream` using
    /// [`SelectAll`](futures_util::stream::select_all::select_all).
    ///
    /// See [`select_candles`](Self::select_candles) for how multiple intervals on one
    /// exchange are merged — the merged `Candle`s carry no `interval` to disambiguate them.
    pub fn select_all_candles(
        &mut self,
    ) -> SelectAll<UnboundedReceiverStream<MarketStreamResult<InstrumentKey, Candle>>> {
        futures_util::stream::select_all::select_all(
            std::mem::take(&mut self.candles).into_values(),
        )
    }

    /// Select and merge every exchange `Stream` for every data type using [`select_all`](futures_util::stream::select_all::select_all)
    ///
    /// Note that using [`MarketStreamResult<Instrument, DataKind>`] as the `Output` is suitable for most
    /// use cases.
    pub fn select_all<Output>(self) -> impl Stream<Item = Output>
    where
        InstrumentKey: Send + 'static,
        Output: 'static,
        MarketStreamResult<InstrumentKey, PublicTrade>: Into<Output>,
        MarketStreamResult<InstrumentKey, OrderBookL1>: Into<Output>,
        MarketStreamResult<InstrumentKey, OrderBookEvent>: Into<Output>,
        MarketStreamResult<InstrumentKey, Liquidation>: Into<Output>,
        MarketStreamResult<InstrumentKey, Candle>: Into<Output>,
    {
        let Self {
            trades,
            l1s,
            l2s,
            liquidations,
            candles,
        } = self;

        let trades = trades
            .into_values()
            .map(|stream| stream.map(MarketStreamResult::into).boxed());

        let l1s = l1s
            .into_values()
            .map(|stream| stream.map(MarketStreamResult::into).boxed());

        let l2s = l2s
            .into_values()
            .map(|stream| stream.map(MarketStreamResult::into).boxed());

        let liquidations = liquidations
            .into_values()
            .map(|stream| stream.map(MarketStreamResult::into).boxed());

        let candles = candles
            .into_values()
            .map(|stream| stream.map(MarketStreamResult::into).boxed());

        let all = trades
            .chain(l1s)
            .chain(l2s)
            .chain(liquidations)
            .chain(candles);

        futures_util::stream::select_all::select_all(all)
    }
}

pub fn validate_batches<SubBatchIter, SubIter, Sub, Instrument>(
    batches: SubBatchIter,
) -> Result<Vec<Vec<Subscription<ExchangeId, Instrument, SubKind>>>, DataError>
where
    SubBatchIter: IntoIterator<Item = SubIter>,
    SubIter: IntoIterator<Item = Sub>,
    Sub: Into<Subscription<ExchangeId, Instrument, SubKind>>,
    Instrument: InstrumentData + Ord,
{
    batches
        .into_iter()
        .map(validate_subscriptions::<SubIter, Sub, Instrument>)
        .collect()
}

pub fn validate_subscriptions<SubIter, Sub, Instrument>(
    batch: SubIter,
) -> Result<Vec<Subscription<ExchangeId, Instrument, SubKind>>, DataError>
where
    SubIter: IntoIterator<Item = Sub>,
    Sub: Into<Subscription<ExchangeId, Instrument, SubKind>>,
    Instrument: InstrumentData + Ord,
{
    // Validate Subscriptions
    let mut batch = batch
        .into_iter()
        .map(Sub::into)
        .map(Validator::validate)
        .collect::<Result<Vec<_>, SocketError>>()?;

    // Remove duplicate Subscriptions
    batch.sort();
    batch.dedup();

    Ok(batch)
}

/// Split each batch into its `(ExchangeId, SubKind)` groups and route every one, connecting none.
///
/// Routing all of them first is what lets a group nothing can serve fail the call before any
/// connection is opened.
fn route_batches<Instrument>(
    batches: Vec<Vec<Subscription<ExchangeId, Instrument, SubKind>>>,
    txs: &Txs<Instrument::Key>,
    subscribers: &DynamicSubscribers,
) -> Result<Vec<plan::GroupFuture>, DataError>
where
    Instrument: DynamicInstrument,
{
    batches
        .into_iter()
        .flat_map(|mut batch| {
            batch.sort_unstable_by_key(|sub| (sub.exchange, sub.kind));
            batch
                .into_iter()
                .chunk_by(|sub| (sub.exchange, sub.kind))
                .into_iter()
                .map(|(key, subs)| (key, subs.collect::<Vec<_>>()))
                .collect::<Vec<_>>()
        })
        .map(|((exchange, sub_kind), subs)| {
            Instrument::route(exchange, sub_kind, subs, txs, subscribers)
        })
        .collect()
}

/// Keep every group's forwarder if all of them initialised, or stop them all and return the first
/// error.
///
/// Stopping them is what makes a failed initialisation leave nothing running: a forwarder owns its
/// stream, and a stream on a shared connection holds its share of it for as long as it lives. Each
/// aborted forwarder is awaited, because an abort only schedules the cancellation: awaiting it is
/// what guarantees the stream has been dropped by the time this returns.
async fn settle(results: Vec<Result<JoinHandle<()>, DataError>>) -> Result<(), DataError> {
    let (forwarders, errors): (Vec<_>, Vec<_>) = results.into_iter().partition_result();

    match errors.into_iter().next() {
        None => Ok(()),
        Some(error) => {
            forwarders.iter().for_each(JoinHandle::abort);
            join_all(forwarders).await;
            Err(error)
        }
    }
}

struct Channels<InstrumentKey> {
    txs: Txs<InstrumentKey>,
    rxs: Rxs<InstrumentKey>,
}

impl<'a, Instrument> TryFrom<&'a Vec<Vec<Subscription<ExchangeId, Instrument, SubKind>>>>
    for Channels<Instrument::Key>
where
    Instrument: InstrumentData,
{
    type Error = DataError;

    fn try_from(
        value: &'a Vec<Vec<Subscription<ExchangeId, Instrument, SubKind>>>,
    ) -> Result<Self, Self::Error> {
        let mut txs = Txs::default();
        let mut rxs = Rxs::default();

        for sub in value.iter().flatten() {
            match sub.kind {
                SubKind::PublicTrades => {
                    if let (None, None) =
                        (txs.trades.get(&sub.exchange), rxs.trades.get(&sub.exchange))
                    {
                        let (tx, rx) = mpsc_unbounded();
                        txs.trades.insert(sub.exchange, tx);
                        rxs.trades.insert(sub.exchange, rx);
                    }
                }
                SubKind::OrderBooksL1 => {
                    if let (None, None) = (txs.l1s.get(&sub.exchange), rxs.l1s.get(&sub.exchange)) {
                        let (tx, rx) = mpsc_unbounded();
                        txs.l1s.insert(sub.exchange, tx);
                        rxs.l1s.insert(sub.exchange, rx);
                    }
                }
                SubKind::OrderBooksL2 => {
                    if let (None, None) = (txs.l2s.get(&sub.exchange), rxs.l2s.get(&sub.exchange)) {
                        let (tx, rx) = mpsc_unbounded();
                        txs.l2s.insert(sub.exchange, tx);
                        rxs.l2s.insert(sub.exchange, rx);
                    }
                }
                SubKind::Liquidations => {
                    if let (None, None) = (
                        txs.liquidations.get(&sub.exchange),
                        rxs.liquidations.get(&sub.exchange),
                    ) {
                        let (tx, rx) = mpsc_unbounded();
                        txs.liquidations.insert(sub.exchange, tx);
                        rxs.liquidations.insert(sub.exchange, rx);
                    }
                }
                SubKind::Candles { .. } => {
                    if let (None, None) = (
                        txs.candles.get(&sub.exchange),
                        rxs.candles.get(&sub.exchange),
                    ) {
                        let (tx, rx) = mpsc_unbounded();
                        txs.candles.insert(sub.exchange, tx);
                        rxs.candles.insert(sub.exchange, rx);
                    }
                }
                sub_kind @ (SubKind::OrderBooksL3 | SubKind::Quotes) => {
                    return Err(DataError::Unsupported {
                        exchange: sub.exchange,
                        sub_kind,
                    });
                }
            }
        }

        Ok(Channels { txs, rxs })
    }
}

struct Rxs<InstrumentKey> {
    trades: FnvHashMap<ExchangeId, UnboundedRx<MarketStreamResult<InstrumentKey, PublicTrade>>>,
    l1s: FnvHashMap<ExchangeId, UnboundedRx<MarketStreamResult<InstrumentKey, OrderBookL1>>>,
    l2s: FnvHashMap<ExchangeId, UnboundedRx<MarketStreamResult<InstrumentKey, OrderBookEvent>>>,
    liquidations:
        FnvHashMap<ExchangeId, UnboundedRx<MarketStreamResult<InstrumentKey, Liquidation>>>,
    candles: FnvHashMap<ExchangeId, UnboundedRx<MarketStreamResult<InstrumentKey, Candle>>>,
}

impl<InstrumentKey> Default for Rxs<InstrumentKey> {
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

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use crate::subscription::{candle::CandleInterval, exchange_supports_instrument_kind_sub_kind};
    use chrono::{TimeZone, Utc};
    use rust_decimal::Decimal;
    use rustrade_instrument::instrument::{
        kind::option::{OptionExercise, OptionKind},
        market_data::{
            MarketDataInstrument,
            kind::{MarketDataFutureContract, MarketDataInstrumentKind, MarketDataOptionContract},
        },
    };

    /// Every [`ExchangeId`] variant.
    fn every_exchange() -> Vec<ExchangeId> {
        use ExchangeId::*;

        let every = vec![
            Other,
            Simulated,
            Mock,
            BinanceFuturesCoin,
            BinanceFuturesUsd,
            BinanceMargin,
            BinanceOptions,
            BinancePortfolioMargin,
            BinanceSpot,
            BinanceUs,
            Bitazza,
            Bitfinex,
            Bitflyer,
            Bitget,
            Bitmart,
            BitmartFuturesUsd,
            Bitmex,
            Bitso,
            Bitstamp,
            Bitvavo,
            Bithumb,
            BybitPerpetualsUsd,
            BybitSpot,
            Cexio,
            Coinbase,
            CoinbaseInternational,
            Cryptocom,
            DatabentoDbeq,
            DatabentoGlbx,
            DatabentoOpra,
            DatabentoXnas,
            DatabentoXnys,
            Deribit,
            GateioFuturesBtc,
            GateioFuturesUsd,
            GateioOptions,
            GateioPerpetualsBtc,
            GateioPerpetualsUsd,
            GateioSpot,
            Gemini,
            Hitbtc,
            Htx,
            HyperliquidPerp,
            HyperliquidSpot,
            AlpacaBroker,
            AlpacaCrypto,
            AlpacaIex,
            AlpacaSip,
            Ibkr,
            Kraken,
            Kucoin,
            Liquid,
            Massive,
            Mexc,
            Okx,
            Poloniex,
            LseFx,
            LseCrypto,
            LseEquities,
            LseFutures,
            LseCfd,
            LseOptions,
        ];

        // Exhaustive, so a new variant fails to compile here until it is added to the list too.
        for exchange in &every {
            match exchange {
                Other
                | Simulated
                | Mock
                | BinanceFuturesCoin
                | BinanceFuturesUsd
                | BinanceMargin
                | BinanceOptions
                | BinancePortfolioMargin
                | BinanceSpot
                | BinanceUs
                | Bitazza
                | Bitfinex
                | Bitflyer
                | Bitget
                | Bitmart
                | BitmartFuturesUsd
                | Bitmex
                | Bitso
                | Bitstamp
                | Bitvavo
                | Bithumb
                | BybitPerpetualsUsd
                | BybitSpot
                | Cexio
                | Coinbase
                | CoinbaseInternational
                | Cryptocom
                | DatabentoDbeq
                | DatabentoGlbx
                | DatabentoOpra
                | DatabentoXnas
                | DatabentoXnys
                | Deribit
                | GateioFuturesBtc
                | GateioFuturesUsd
                | GateioOptions
                | GateioPerpetualsBtc
                | GateioPerpetualsUsd
                | GateioSpot
                | Gemini
                | Hitbtc
                | Htx
                | HyperliquidPerp
                | HyperliquidSpot
                | AlpacaBroker
                | AlpacaCrypto
                | AlpacaIex
                | AlpacaSip
                | Ibkr
                | Kraken
                | Kucoin
                | Liquid
                | Massive
                | Mexc
                | Okx
                | Poloniex
                | LseFx
                | LseCrypto
                | LseEquities
                | LseFutures
                | LseCfd
                | LseOptions => {}
            }
        }

        every
    }

    fn instrument_kinds() -> [MarketDataInstrumentKind; 5] {
        let expiry = Utc.with_ymd_and_hms(2030, 1, 1, 0, 0, 0).unwrap();
        [
            MarketDataInstrumentKind::Spot,
            MarketDataInstrumentKind::Perpetual,
            MarketDataInstrumentKind::Cfd,
            MarketDataInstrumentKind::Future(MarketDataFutureContract { expiry }),
            MarketDataInstrumentKind::Option(MarketDataOptionContract {
                kind: OptionKind::Call,
                exercise: OptionExercise::American,
                expiry,
                strike: Decimal::ONE_HUNDRED,
            }),
        ]
    }

    fn sub_kinds() -> Vec<SubKind> {
        [
            SubKind::PublicTrades,
            SubKind::OrderBooksL1,
            SubKind::OrderBooksL2,
            SubKind::OrderBooksL3,
            SubKind::Liquidations,
            SubKind::Quotes,
        ]
        .into_iter()
        .chain(
            CandleInterval::ALL
                .into_iter()
                .map(|interval| SubKind::Candles { interval }),
        )
        .collect()
    }

    fn subscription(
        exchange: ExchangeId,
        kind: MarketDataInstrumentKind,
        sub_kind: SubKind,
    ) -> Subscription<ExchangeId, MarketDataInstrument, SubKind> {
        Subscription::new(
            exchange,
            MarketDataInstrument::new("btc", "usd", kind),
            sub_kind,
        )
    }

    /// Route `batches` as `init_with` does, without connecting anything.
    fn route(
        batches: Vec<Vec<Subscription<ExchangeId, MarketDataInstrument, SubKind>>>,
        subscribers: &DynamicSubscribers,
    ) -> Result<Vec<plan::GroupFuture>, DataError> {
        let Channels { txs, rxs: _ } = Channels::try_from(&batches)?;
        route_batches(batches, &txs, subscribers)
    }

    /// Subscribers for every venue that needs one in this build.
    fn every_subscriber() -> DynamicSubscribers {
        #[cfg(feature = "lse")]
        return DynamicSubscribers::default().with_lse(LseSubscriber::new(
            crate::exchange::lse::live::LseCredentials::new("test-key"),
        ));

        #[cfg(not(feature = "lse"))]
        DynamicSubscribers::default()
    }

    fn feature_enabled(feature: &str) -> bool {
        match feature {
            "lse" => cfg!(feature = "lse"),
            "hyperliquid" => cfg!(feature = "hyperliquid"),
            other => panic!("no dynamic route is gated on the `{other}` feature"),
        }
    }

    #[test]
    fn every_pair_the_support_matrix_accepts_is_routed() {
        let subscribers = every_subscriber();
        let mut routed = 0;

        for exchange in every_exchange() {
            for kind in instrument_kinds() {
                for sub_kind in sub_kinds() {
                    if !exchange_supports_instrument_kind_sub_kind(&exchange, &kind, sub_kind) {
                        continue;
                    }

                    // The futures are dropped unpolled, so nothing connects.
                    match route(
                        vec![vec![subscription(exchange, kind.clone(), sub_kind)]],
                        &subscribers,
                    ) {
                        Ok(groups) => {
                            assert_eq!(groups.len(), 1);
                            routed += 1;
                        }
                        Err(DataError::FeatureDisabled { feature, .. }) => assert!(
                            !feature_enabled(&feature),
                            "{exchange} ({kind}, {sub_kind}) reported `{feature}` disabled in a \
                             build that enables it"
                        ),
                        Err(error) => panic!(
                            "the matrix accepts {exchange} ({kind}, {sub_kind}), but routing it \
                             failed: {error}"
                        ),
                    }
                }
            }
        }

        // Guards against the loop passing vacuously.
        assert!(routed > 30, "only {routed} pairs were routed");
    }

    #[test]
    fn a_pair_the_support_matrix_refuses_is_refused_before_routing() {
        // Options publish no quote, so an options L1 stream is refused rather than left silent.
        let expiry = Utc.with_ymd_and_hms(2030, 1, 1, 0, 0, 0).unwrap();
        let option = MarketDataInstrumentKind::Option(MarketDataOptionContract {
            kind: OptionKind::Call,
            exercise: OptionExercise::American,
            expiry,
            strike: Decimal::ONE_HUNDRED,
        });

        let error = validate_batches([[subscription(
            ExchangeId::LseOptions,
            option,
            SubKind::OrderBooksL1,
        )]])
        .unwrap_err();

        assert!(
            matches!(&error, DataError::Socket(message) if message.contains("lse_options")),
            "{error:?}"
        );
    }

    #[cfg(feature = "lse")]
    #[test]
    fn one_lse_batch_across_datasets_and_kinds_shares_one_subscriber() {
        let subscriber =
            LseSubscriber::new(crate::exchange::lse::live::LseCredentials::new("test-key"));
        let subscribers = DynamicSubscribers::default().with_lse(subscriber.clone());
        let before = subscriber.connection_handles();

        let groups = route(
            vec![vec![
                subscription(
                    ExchangeId::LseCrypto,
                    MarketDataInstrumentKind::Spot,
                    SubKind::PublicTrades,
                ),
                subscription(
                    ExchangeId::LseCrypto,
                    MarketDataInstrumentKind::Spot,
                    SubKind::OrderBooksL1,
                ),
                subscription(
                    ExchangeId::LseEquities,
                    MarketDataInstrumentKind::Spot,
                    SubKind::PublicTrades,
                ),
                subscription(
                    ExchangeId::LseEquities,
                    MarketDataInstrumentKind::Spot,
                    SubKind::OrderBooksL1,
                ),
            ]],
            &subscribers,
        )
        .unwrap();

        // One group per (dataset, kind), each holding a clone of the one subscriber: a group given
        // a subscriber of its own would open a second socket, which the provider refuses.
        assert_eq!(groups.len(), 4);
        assert_eq!(subscriber.connection_handles(), before + groups.len());

        drop(groups);
        assert_eq!(subscriber.connection_handles(), before);
    }

    #[cfg(feature = "lse")]
    #[test]
    fn an_lse_subscription_without_a_subscriber_fails_the_call_before_any_group_connects() {
        let error = route(
            vec![
                vec![subscription(
                    ExchangeId::BinanceSpot,
                    MarketDataInstrumentKind::Spot,
                    SubKind::PublicTrades,
                )],
                vec![subscription(
                    ExchangeId::LseFx,
                    MarketDataInstrumentKind::Spot,
                    SubKind::PublicTrades,
                )],
            ],
            &DynamicSubscribers::default(),
        )
        .err()
        .unwrap();

        assert_eq!(
            error,
            DataError::SubscriberRequired {
                exchange: ExchangeId::LseFx
            }
        );
    }

    #[cfg(not(feature = "lse"))]
    #[test]
    fn an_lse_subscription_without_the_feature_names_the_feature() {
        let error = route(
            vec![vec![subscription(
                ExchangeId::LseFx,
                MarketDataInstrumentKind::Spot,
                SubKind::PublicTrades,
            )]],
            &DynamicSubscribers::default(),
        )
        .err()
        .unwrap();

        assert_eq!(
            error,
            DataError::FeatureDisabled {
                exchange: ExchangeId::LseFx,
                feature: "lse".to_owned(),
            }
        );
    }

    #[tokio::test]
    async fn a_failed_group_stops_every_forwarder_that_started() {
        struct OnDrop(Option<tokio::sync::oneshot::Sender<()>>);
        impl Drop for OnDrop {
            fn drop(&mut self) {
                let _ = self.0.take().map(|tx| tx.send(()));
            }
        }

        // Built outside the task, so it is dropped with it whether or not the task ever ran.
        let (dropped_tx, mut dropped) = tokio::sync::oneshot::channel();
        let guard = OnDrop(Some(dropped_tx));
        let forwarder = tokio::spawn(async move {
            let _guard = guard;
            std::future::pending::<()>().await;
        });

        let error = settle(vec![
            Ok(forwarder),
            Err(DataError::SubscriberRequired {
                exchange: ExchangeId::LseFx,
            }),
        ])
        .await
        .unwrap_err();

        assert_eq!(
            error,
            DataError::SubscriberRequired {
                exchange: ExchangeId::LseFx
            }
        );
        // The forwarder's task, and with it the stream it owned, was dropped before `settle`
        // returned: nothing further needs to run for the drop to have happened.
        assert_eq!(dropped.try_recv(), Ok(()));
    }

    #[tokio::test]
    async fn forwarders_keep_running_when_every_group_initialised() {
        let forwarder = tokio::spawn(std::future::pending::<()>());
        let abort = forwarder.abort_handle();

        settle(vec![Ok(forwarder)]).await.unwrap();
        tokio::task::yield_now().await;

        assert!(!abort.is_finished());
        abort.abort();
    }
}
