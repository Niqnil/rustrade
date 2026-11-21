use crate::{
    error::DataError,
    streams::consumer::MarketStreamResult,
    subscription::{
        book::{OrderBookEvent, OrderBookL1},
        candle::Candle,
        greeks::OptionGreeks,
        liquidation::Liquidation,
        price_band::PriceBand,
        trade::PublicTrade,
        trading_status::TradingStatus,
    },
};
use chrono::{DateTime, Utc};
use derive_more::From;
use rustrade_instrument::{exchange::ExchangeId, instrument::market_data::MarketDataInstrument};
use serde::{Deserialize, Serialize};

/// Convenient new type containing a collection of [`MarketEvent<T>`](MarketEvent)s.
#[derive(Debug)]
pub struct MarketIter<InstrumentKey, T>(pub Vec<Result<MarketEvent<InstrumentKey, T>, DataError>>);

impl<InstrumentKey, T> FromIterator<Result<MarketEvent<InstrumentKey, T>, DataError>>
    for MarketIter<InstrumentKey, T>
{
    fn from_iter<Iter>(iter: Iter) -> Self
    where
        Iter: IntoIterator<Item = Result<MarketEvent<InstrumentKey, T>, DataError>>,
    {
        Self(iter.into_iter().collect())
    }
}

/// Normalised [`MarketEvent<T>`](Self) wrapping the `T` data variant in metadata.
///
/// Note: `T` can be an enum such as the [`DataKind`] if required.
///
/// See [`crate::subscription`] for all existing market event variants.
///
/// ### Examples
/// - [`MarketEvent<PublicTrade>`](PublicTrade)
/// - [`MarketEvent<OrderBookL1>`](OrderBookL1)
/// - [`MarketEvent<DataKind>`](DataKind)
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Deserialize, Serialize)]
pub struct MarketEvent<InstrumentKey = MarketDataInstrument, T = DataKind> {
    /// Exchange timestamp of the event — its position on the **engine timeline**.
    ///
    /// A consuming engine treats this as *the* time the event occurred: the
    /// historical/backtest clock derives "current time" from it and replays
    /// events in `time_exchange` order (see `rustrade`'s `EngineClock` /
    /// `TimeExchange`).
    ///
    /// # Contract for aggregated / windowed payloads (candles, OHLCV bars)
    ///
    /// For a point-in-time payload (e.g. [`PublicTrade`], [`OrderBookL1`]) this is
    /// simply the venue event time. For a payload that aggregates a **time window**
    /// (a [`Candle`]), `time_exchange` **must be the period END**, i.e. the
    /// candle's [`close_time`](crate::subscription::candle::Candle::close_time) —
    /// **never the period start**.
    ///
    /// Stamping the open instant makes a *completed* bar enter the engine timeline
    /// at the moment its period *began*, so a strategy would act on a fully-formed
    /// bar before it could possibly exist — silent lookahead / repaint bias. This
    /// applies to **any** windowed payload, including a custom `T` supplied by a
    /// consumer driving the engine without this crate's producers. The library's
    /// own candle producers already stamp `close_time` here; when wrapping a
    /// [`Candle`] into a `MarketEvent` yourself, use its `close_time`.
    pub time_exchange: DateTime<Utc>,
    /// Local timestamp at which this event was received/decoded. Diagnostic only —
    /// the engine clock and replay ordering use [`time_exchange`](Self::time_exchange).
    pub time_received: DateTime<Utc>,
    pub exchange: ExchangeId,
    pub instrument: InstrumentKey,
    pub kind: T,
}

impl<InstrumentKey, T> MarketEvent<InstrumentKey, T> {
    pub fn map_kind<F, O>(self, op: F) -> MarketEvent<InstrumentKey, O>
    where
        F: FnOnce(T) -> O,
    {
        MarketEvent {
            time_exchange: self.time_exchange,
            time_received: self.time_received,
            exchange: self.exchange,
            instrument: self.instrument,
            kind: op(self.kind),
        }
    }
}

impl<InstrumentKey> MarketEvent<InstrumentKey, DataKind> {
    pub fn as_public_trade(&self) -> Option<MarketEvent<&InstrumentKey, &PublicTrade>> {
        match &self.kind {
            DataKind::Trade(public_trade) => Some(self.as_event(public_trade)),
            _ => None,
        }
    }

    pub fn as_order_book_l1(&self) -> Option<MarketEvent<&InstrumentKey, &OrderBookL1>> {
        match &self.kind {
            DataKind::OrderBookL1(orderbook) => Some(self.as_event(orderbook)),
            _ => None,
        }
    }

    pub fn as_order_book(&self) -> Option<MarketEvent<&InstrumentKey, &OrderBookEvent>> {
        match &self.kind {
            DataKind::OrderBook(orderbook) => Some(self.as_event(orderbook)),
            _ => None,
        }
    }

    pub fn as_candle(&self) -> Option<MarketEvent<&InstrumentKey, &Candle>> {
        match &self.kind {
            DataKind::Candle(candle) => Some(self.as_event(candle)),
            _ => None,
        }
    }

    pub fn as_liquidation(&self) -> Option<MarketEvent<&InstrumentKey, &Liquidation>> {
        match &self.kind {
            DataKind::Liquidation(liquidation) => Some(self.as_event(liquidation)),
            _ => None,
        }
    }

    pub fn as_option_greeks(&self) -> Option<MarketEvent<&InstrumentKey, &OptionGreeks>> {
        match &self.kind {
            DataKind::OptionGreeks(greeks) => Some(self.as_event(greeks)),
            _ => None,
        }
    }

    /// This event with a reference to its [`TradingStatus`], if it is one.
    pub fn as_trading_status(&self) -> Option<MarketEvent<&InstrumentKey, &TradingStatus>> {
        match &self.kind {
            DataKind::TradingStatus(status) => Some(self.as_event(status)),
            _ => None,
        }
    }

    /// This event with a reference to its [`PriceBand`], if it is one.
    pub fn as_price_band(&self) -> Option<MarketEvent<&InstrumentKey, &PriceBand>> {
        match &self.kind {
            DataKind::PriceBand(band) => Some(self.as_event(band)),
            _ => None,
        }
    }

    fn as_event<'a, K>(&'a self, kind: &'a K) -> MarketEvent<&'a InstrumentKey, &'a K> {
        MarketEvent {
            time_exchange: self.time_exchange,
            time_received: self.time_received,
            exchange: self.exchange,
            instrument: &self.instrument,
            kind,
        }
    }
}

/// Available kinds of normalised [`MarketEvent<T>`](MarketEvent).
///
/// ### Notes
/// - [`Self`] is only used as the [`MarketEvent<DataKind>`](MarketEvent) `Output` when combining
///   several [`Streams<SubscriptionKind::Event>`](crate::streams::Streams) using the
///   [`MultiStreamBuilder<Output>`](crate::streams::builder::multi::MultiStreamBuilder), or via
///   the [`DynamicStreams::select_all`](crate::streams::builder::dynamic::DynamicStreams) method.
/// - [`Self`] is purposefully not supported in any
///   [`Subscription`](crate::subscription::Subscription)s directly, it is only used to
///   make ergonomic [`Streams`](crate::streams::Streams) containing many
///   [`MarketEvent<T>`](MarketEvent) kinds.
/// - New kinds are added as venues publish them, so a `match` outside this crate needs a wildcard
///   arm.
#[non_exhaustive]
#[derive(Clone, PartialEq, Debug, Deserialize, Serialize, From)]
pub enum DataKind {
    Trade(PublicTrade),
    OrderBookL1(OrderBookL1),
    OrderBook(OrderBookEvent),
    Candle(Candle),
    Liquidation(Liquidation),
    OptionGreeks(OptionGreeks),
    TradingStatus(TradingStatus),
    PriceBand(PriceBand),
}

impl DataKind {
    pub fn kind_name(&self) -> &str {
        match self {
            DataKind::Trade(_) => "public_trade",
            DataKind::OrderBookL1(_) => "l1",
            DataKind::OrderBook(_) => "l2",
            DataKind::Candle(_) => "candle",
            DataKind::Liquidation(_) => "liquidation",
            DataKind::OptionGreeks(_) => "option_greeks",
            DataKind::TradingStatus(_) => "trading_status",
            DataKind::PriceBand(_) => "price_band",
        }
    }
}

impl<InstrumentKey> From<MarketEvent<InstrumentKey, OptionGreeks>>
    for MarketEvent<InstrumentKey, DataKind>
{
    fn from(value: MarketEvent<InstrumentKey, OptionGreeks>) -> Self {
        value.map_kind(OptionGreeks::into)
    }
}

impl<InstrumentKey> From<MarketStreamResult<InstrumentKey, OptionGreeks>>
    for MarketStreamResult<InstrumentKey, DataKind>
{
    fn from(value: MarketStreamResult<InstrumentKey, OptionGreeks>) -> Self {
        value.map_ok(MarketEvent::from)
    }
}

impl<InstrumentKey> From<MarketStreamResult<InstrumentKey, PublicTrade>>
    for MarketStreamResult<InstrumentKey, DataKind>
{
    fn from(value: MarketStreamResult<InstrumentKey, PublicTrade>) -> Self {
        value.map_ok(MarketEvent::from)
    }
}

impl<InstrumentKey> From<MarketEvent<InstrumentKey, PublicTrade>>
    for MarketEvent<InstrumentKey, DataKind>
{
    fn from(value: MarketEvent<InstrumentKey, PublicTrade>) -> Self {
        value.map_kind(PublicTrade::into)
    }
}

impl<InstrumentKey> From<MarketStreamResult<InstrumentKey, OrderBookL1>>
    for MarketStreamResult<InstrumentKey, DataKind>
{
    fn from(value: MarketStreamResult<InstrumentKey, OrderBookL1>) -> Self {
        value.map_ok(MarketEvent::from)
    }
}

impl<InstrumentKey> From<MarketEvent<InstrumentKey, OrderBookL1>>
    for MarketEvent<InstrumentKey, DataKind>
{
    fn from(value: MarketEvent<InstrumentKey, OrderBookL1>) -> Self {
        value.map_kind(OrderBookL1::into)
    }
}

impl<InstrumentKey> From<MarketStreamResult<InstrumentKey, OrderBookEvent>>
    for MarketStreamResult<InstrumentKey, DataKind>
{
    fn from(value: MarketStreamResult<InstrumentKey, OrderBookEvent>) -> Self {
        value.map_ok(MarketEvent::from)
    }
}

impl<InstrumentKey> From<MarketEvent<InstrumentKey, OrderBookEvent>>
    for MarketEvent<InstrumentKey, DataKind>
{
    fn from(value: MarketEvent<InstrumentKey, OrderBookEvent>) -> Self {
        value.map_kind(OrderBookEvent::into)
    }
}

impl<InstrumentKey> From<MarketStreamResult<InstrumentKey, Candle>>
    for MarketStreamResult<InstrumentKey, DataKind>
{
    fn from(value: MarketStreamResult<InstrumentKey, Candle>) -> Self {
        value.map_ok(MarketEvent::from)
    }
}

impl<InstrumentKey> From<MarketEvent<InstrumentKey, Candle>>
    for MarketEvent<InstrumentKey, DataKind>
{
    fn from(value: MarketEvent<InstrumentKey, Candle>) -> Self {
        value.map_kind(Candle::into)
    }
}

impl<InstrumentKey> From<MarketStreamResult<InstrumentKey, Liquidation>>
    for MarketStreamResult<InstrumentKey, DataKind>
{
    fn from(value: MarketStreamResult<InstrumentKey, Liquidation>) -> Self {
        value.map_ok(MarketEvent::from)
    }
}

impl<InstrumentKey> From<MarketEvent<InstrumentKey, Liquidation>>
    for MarketEvent<InstrumentKey, DataKind>
{
    fn from(value: MarketEvent<InstrumentKey, Liquidation>) -> Self {
        value.map_kind(Liquidation::into)
    }
}

impl<InstrumentKey> From<MarketStreamResult<InstrumentKey, TradingStatus>>
    for MarketStreamResult<InstrumentKey, DataKind>
{
    fn from(value: MarketStreamResult<InstrumentKey, TradingStatus>) -> Self {
        value.map_ok(MarketEvent::from)
    }
}

impl<InstrumentKey> From<MarketEvent<InstrumentKey, TradingStatus>>
    for MarketEvent<InstrumentKey, DataKind>
{
    fn from(value: MarketEvent<InstrumentKey, TradingStatus>) -> Self {
        value.map_kind(TradingStatus::into)
    }
}

impl<InstrumentKey> From<MarketStreamResult<InstrumentKey, PriceBand>>
    for MarketStreamResult<InstrumentKey, DataKind>
{
    fn from(value: MarketStreamResult<InstrumentKey, PriceBand>) -> Self {
        value.map_ok(MarketEvent::from)
    }
}

impl<InstrumentKey> From<MarketEvent<InstrumentKey, PriceBand>>
    for MarketEvent<InstrumentKey, DataKind>
{
    fn from(value: MarketEvent<InstrumentKey, PriceBand>) -> Self {
        value.map_kind(PriceBand::into)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::subscription::trading_status::TradingState;

    fn event(kind: DataKind) -> MarketEvent<u8, DataKind> {
        MarketEvent {
            time_exchange: DateTime::<Utc>::MIN_UTC,
            time_received: DateTime::<Utc>::MIN_UTC,
            exchange: ExchangeId::Ibkr,
            instrument: 7,
            kind,
        }
    }

    #[test]
    fn a_trading_status_event_reads_as_one_and_nothing_else() {
        let status = TradingStatus::new(TradingState::Halted);
        let event = event(DataKind::TradingStatus(status.clone()));

        let read = event.as_trading_status();

        assert_eq!(
            read.map(|read| (read.instrument, read.kind)),
            Some((&7, &status))
        );
        assert!(event.as_option_greeks().is_none());
    }

    #[test]
    fn another_kind_is_not_a_trading_status() {
        let event = event(DataKind::OptionGreeks(OptionGreeks::default()));

        assert!(event.as_trading_status().is_none());
    }

    #[test]
    fn a_price_band_event_reads_as_one_and_nothing_else() {
        let band = PriceBand::new(Some(rust_decimal::Decimal::ONE), None);
        let event = event(DataKind::PriceBand(band.clone()));

        let read = event.as_price_band();

        assert_eq!(
            read.map(|read| (read.instrument, read.kind)),
            Some((&7, &band))
        );
        assert!(event.as_trading_status().is_none());
    }

    #[test]
    fn a_status_and_a_band_convert_into_their_data_kinds_with_the_event_intact() {
        let typed = MarketEvent {
            time_exchange: DateTime::<Utc>::MIN_UTC,
            time_received: DateTime::<Utc>::MAX_UTC,
            exchange: ExchangeId::AlpacaIex,
            instrument: 7_u8,
            kind: (),
        };
        let status = TradingStatus::new(TradingState::Halted);
        let band = PriceBand::new(None, Some(rust_decimal::Decimal::ONE));

        let status_item: MarketStreamResult<u8, TradingStatus> =
            crate::streams::reconnect::Event::Item(Ok(typed.clone().map_kind(|()| status.clone())));
        let band_item: MarketStreamResult<u8, PriceBand> =
            crate::streams::reconnect::Event::Item(Ok(typed.clone().map_kind(|()| band.clone())));

        for (converted, kind) in [
            (
                MarketStreamResult::<u8, DataKind>::from(status_item),
                DataKind::TradingStatus(status),
            ),
            (
                MarketStreamResult::<u8, DataKind>::from(band_item),
                DataKind::PriceBand(band),
            ),
        ] {
            let crate::streams::reconnect::Event::Item(Ok(converted)) = converted else {
                panic!("expected the converted item");
            };
            assert_eq!(converted, typed.clone().map_kind(|()| kind));
        }
    }

    #[test]
    fn kind_names() {
        let names = [
            DataKind::OptionGreeks(OptionGreeks::default()),
            DataKind::TradingStatus(TradingStatus::new(TradingState::Trading)),
            DataKind::PriceBand(PriceBand::new(None, None)),
        ]
        .map(|kind| kind.kind_name().to_owned());

        assert_eq!(names, ["option_greeks", "trading_status", "price_band"]);
    }
}
