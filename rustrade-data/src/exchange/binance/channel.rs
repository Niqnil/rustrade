use super::{Binance, futures::BinanceFuturesUsdMarket, spot::BinanceSpot};
use crate::{
    Identifier,
    subscription::{
        Subscription,
        book::{OrderBooksL1, OrderBooksL2},
        candle::{CandleInterval, Candles},
        liquidation::Liquidations,
        trade::PublicTrades,
    },
};
use serde::Serialize;

/// Type that defines how to translate a Barter [`Subscription`] into a [`Binance`]
/// channel to be subscribed to.
///
/// See docs: <https://binance-docs.github.io/apidocs/spot/en/#websocket-market-streams>
/// See docs: <https://binance-docs.github.io/apidocs/futures/en/#websocket-market-streams>
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Serialize)]
pub struct BinanceChannel(pub &'static str);

impl BinanceChannel {
    /// [`Binance`] real-time trades channel name.
    ///
    /// See docs: <https://binance-docs.github.io/apidocs/spot/en/#trade-streams>
    ///
    /// Note:
    /// For [`BinanceFuturesUsd`](super::futures::BinanceFuturesUsd) this real-time
    /// stream is undocumented.
    ///
    /// See discord: <https://discord.com/channels/910237311332151317/923160222711812126/975712874582388757>
    pub const TRADES: Self = Self("@trade");

    /// [`Binance`] real-time OrderBook Level1 (top of books) channel name.
    ///
    /// See docs:<https://binance-docs.github.io/apidocs/spot/en/#individual-symbol-book-ticker-streams>
    /// See docs:<https://binance-docs.github.io/apidocs/futures/en/#individual-symbol-book-ticker-streams>
    pub const ORDER_BOOK_L1: Self = Self("@bookTicker");

    /// [`Binance`] OrderBook Level2 channel name (100ms delta updates).
    ///
    /// See docs: <https://binance-docs.github.io/apidocs/spot/en/#diff-depth-stream>
    /// See docs: <https://binance-docs.github.io/apidocs/futures/en/#diff-book-depth-streams>
    pub const ORDER_BOOK_L2: Self = Self("@depth@100ms");

    /// [`BinanceFuturesUsd`](super::futures::BinanceFuturesUsd) liquidation orders channel name.
    ///
    /// Routed on Binance's `/market` WS tier (see
    /// [`BinanceFuturesUsdMarket`]).
    ///
    /// See docs: <https://binance-docs.github.io/apidocs/futures/en/#liquidation-order-streams>
    pub const LIQUIDATIONS: Self = Self("@forceOrder");

    /// [`BinanceSpot`] kline (candle) channel name for the given [`CandleInterval`],
    /// e.g. `@kline_1m`.
    ///
    /// The interval suffix is exactly [`CandleInterval::as_str`] — pinned by the
    /// `spot_candle_channel_suffix_matches_interval` drift-guard test.
    ///
    /// See docs: <https://binance-docs.github.io/apidocs/spot/en/#kline-candlestick-streams>
    #[must_use]
    pub const fn spot_candle(interval: CandleInterval) -> Self {
        match interval {
            CandleInterval::Sec1 => Self("@kline_1s"),
            CandleInterval::Min1 => Self("@kline_1m"),
            CandleInterval::Min3 => Self("@kline_3m"),
            CandleInterval::Min5 => Self("@kline_5m"),
            CandleInterval::Min15 => Self("@kline_15m"),
            CandleInterval::Min30 => Self("@kline_30m"),
            CandleInterval::Hour1 => Self("@kline_1h"),
            CandleInterval::Hour2 => Self("@kline_2h"),
            CandleInterval::Hour4 => Self("@kline_4h"),
            CandleInterval::Hour6 => Self("@kline_6h"),
            CandleInterval::Hour8 => Self("@kline_8h"),
            CandleInterval::Hour12 => Self("@kline_12h"),
            CandleInterval::Day1 => Self("@kline_1d"),
            CandleInterval::Day3 => Self("@kline_3d"),
            CandleInterval::Week1 => Self("@kline_1w"),
            CandleInterval::Month1 => Self("@kline_1M"),
        }
    }

    /// [`BinanceFuturesUsd`](super::futures::BinanceFuturesUsd) continuous-contract
    /// (perpetual) kline channel name for the given [`CandleInterval`], e.g.
    /// `_perpetual@continuousKline_1m`.
    ///
    /// Routed on Binance's `/market` WS tier (see
    /// [`BinanceFuturesUsdMarket`]). The
    /// `contractType` is fixed `PERPETUAL` because [`BinanceFuturesUsd`](super::futures::BinanceFuturesUsd)
    /// is perpetual-only. The interval suffix is exactly [`CandleInterval::as_str`] —
    /// pinned by the `futures_candle_channel_suffix_matches_interval` drift-guard test.
    ///
    /// See docs: <https://binance-docs.github.io/apidocs/futures/en/#continuous-contract-kline-candlestick-streams>
    #[must_use]
    pub const fn futures_candle(interval: CandleInterval) -> Self {
        match interval {
            CandleInterval::Sec1 => Self("_perpetual@continuousKline_1s"),
            CandleInterval::Min1 => Self("_perpetual@continuousKline_1m"),
            CandleInterval::Min3 => Self("_perpetual@continuousKline_3m"),
            CandleInterval::Min5 => Self("_perpetual@continuousKline_5m"),
            CandleInterval::Min15 => Self("_perpetual@continuousKline_15m"),
            CandleInterval::Min30 => Self("_perpetual@continuousKline_30m"),
            CandleInterval::Hour1 => Self("_perpetual@continuousKline_1h"),
            CandleInterval::Hour2 => Self("_perpetual@continuousKline_2h"),
            CandleInterval::Hour4 => Self("_perpetual@continuousKline_4h"),
            CandleInterval::Hour6 => Self("_perpetual@continuousKline_6h"),
            CandleInterval::Hour8 => Self("_perpetual@continuousKline_8h"),
            CandleInterval::Hour12 => Self("_perpetual@continuousKline_12h"),
            CandleInterval::Day1 => Self("_perpetual@continuousKline_1d"),
            CandleInterval::Day3 => Self("_perpetual@continuousKline_3d"),
            CandleInterval::Week1 => Self("_perpetual@continuousKline_1w"),
            CandleInterval::Month1 => Self("_perpetual@continuousKline_1M"),
        }
    }
}

impl<Server, Instrument> Identifier<BinanceChannel>
    for Subscription<Binance<Server>, Instrument, PublicTrades>
{
    fn id(&self) -> BinanceChannel {
        BinanceChannel::TRADES
    }
}

impl<Server, Instrument> Identifier<BinanceChannel>
    for Subscription<Binance<Server>, Instrument, OrderBooksL1>
{
    fn id(&self) -> BinanceChannel {
        BinanceChannel::ORDER_BOOK_L1
    }
}

impl<Server, Instrument> Identifier<BinanceChannel>
    for Subscription<Binance<Server>, Instrument, OrderBooksL2>
{
    fn id(&self) -> BinanceChannel {
        BinanceChannel::ORDER_BOOK_L2
    }
}

// `@forceOrder` is a Binance `/market`-tier stream, so liquidations route through
// `BinanceFuturesUsdMarket` (not `BinanceFuturesUsd`/`/public`). Restricting the impl to
// the market-tier server type makes a `/public`-tier liquidation subscription a compile error.
impl<Instrument> Identifier<BinanceChannel>
    for Subscription<BinanceFuturesUsdMarket, Instrument, Liquidations>
{
    fn id(&self) -> BinanceChannel {
        BinanceChannel::LIQUIDATIONS
    }
}

impl<Instrument> Identifier<BinanceChannel> for Subscription<BinanceSpot, Instrument, Candles> {
    fn id(&self) -> BinanceChannel {
        BinanceChannel::spot_candle(self.kind.interval)
    }
}

// Futures klines ride the `@continuousKline_` `/market`-tier stream, so the channel impl is
// specialised to `BinanceFuturesUsdMarket` — routing a kline sub to `/public` is a compile error.
impl<Instrument> Identifier<BinanceChannel>
    for Subscription<BinanceFuturesUsdMarket, Instrument, Candles>
{
    fn id(&self) -> BinanceChannel {
        BinanceChannel::futures_candle(self.kind.interval)
    }
}

impl AsRef<str> for BinanceChannel {
    fn as_ref(&self) -> &str {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The spot kline channel must be `@kline_<interval>` where `<interval>` is exactly
    /// [`CandleInterval::as_str`] for every variant — otherwise the channel string would
    /// drift from the live Binance stream name and silently fail to match frames.
    #[test]
    fn spot_candle_channel_suffix_matches_interval() {
        for interval in CandleInterval::ALL {
            let channel = BinanceChannel::spot_candle(interval).0;
            assert_eq!(
                channel,
                format!("@kline_{}", interval.as_str()),
                "spot channel drifted for {interval:?}"
            );
        }
    }

    /// The futures continuous kline channel must be `_perpetual@continuousKline_<interval>`
    /// where `<interval>` is exactly [`CandleInterval::as_str`] for every variant.
    #[test]
    fn futures_candle_channel_suffix_matches_interval() {
        for interval in CandleInterval::ALL {
            let channel = BinanceChannel::futures_candle(interval).0;
            assert_eq!(
                channel,
                format!("_perpetual@continuousKline_{}", interval.as_str()),
                "futures channel drifted for {interval:?}"
            );
        }
    }
}
