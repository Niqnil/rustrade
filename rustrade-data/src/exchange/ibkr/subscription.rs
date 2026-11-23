//! IBKR subscription types.

use super::candles::IbkrCandleRequest;
use chrono::{DateTime, Utc};
use rustrade_instrument::instrument::name::InstrumentNameExchange;
use serde::{Deserialize, Serialize};

/// Subscription configuration for IBKR market data.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IbkrSubscription<K> {
    /// Instrument key for MarketEvent output
    pub key: K,
    /// Instrument name (must be registered in ContractRegistry)
    pub instrument: InstrumentNameExchange,
    /// Type of market data to subscribe to
    pub kind: IbkrSubscriptionKind,
}

impl<K> IbkrSubscription<K> {
    /// Create a quotes subscription.
    pub fn quotes(key: K, instrument: InstrumentNameExchange) -> Self {
        Self {
            key,
            instrument,
            kind: IbkrSubscriptionKind::Quotes,
        }
    }

    /// Create a depth subscription.
    ///
    /// # Arguments
    ///
    /// * `rows` - Number of order book rows. Valid range: 1-20 (IB API limit).
    ///   Common values: 5 (shallow), 10 (medium), 20 (deep).
    ///
    /// # Panics
    ///
    /// Panics if `rows` is outside the valid range 1-20. A subscription built without this
    /// constructor, or deserialized, with such `rows` is refused by
    /// [`IbkrMarketStream::init`](super::IbkrMarketStream::init) instead.
    ///
    /// # Limitations
    ///
    /// IB allows max 3 concurrent depth subscriptions. Exceeding this limit
    /// causes error 309.
    pub fn depth(key: K, instrument: InstrumentNameExchange, rows: i32) -> Self {
        assert!(
            (1..=20).contains(&rows),
            "IB depth rows must be 1-20, got {rows}"
        );
        Self {
            key,
            instrument,
            kind: IbkrSubscriptionKind::Depth { rows },
        }
    }

    /// Create a trades subscription.
    pub fn trades(key: K, instrument: InstrumentNameExchange) -> Self {
        Self {
            key,
            instrument,
            kind: IbkrSubscriptionKind::Trades,
        }
    }

    /// Create an option Greeks subscription.
    ///
    /// Use for option contracts to receive real-time Greeks (delta, gamma, theta,
    /// vega, implied volatility) computed from live market prices.
    ///
    /// # Subscription Requirements
    ///
    /// Requires OPRA subscription (paid) for US options.
    pub fn option_greeks(key: K, instrument: InstrumentNameExchange) -> Self {
        Self {
            key,
            instrument,
            kind: IbkrSubscriptionKind::OptionGreeks,
        }
    }

    /// Create a candle subscription: completed bars, as `request` describes. See
    /// [`candles`](super::candles).
    pub fn candles(key: K, instrument: InstrumentNameExchange, request: IbkrCandleRequest) -> Self {
        Self {
            key,
            instrument,
            kind: IbkrSubscriptionKind::Candles(request),
        }
    }
}

/// Type of IBKR market data subscription.
///
/// More kinds may be added, so a `match` on it needs a wildcard arm.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum IbkrSubscriptionKind {
    /// Best bid/ask quotes via market_data().
    ///
    /// Also yields a [`TradingStatus`](crate::subscription::trading_status::TradingStatus) when
    /// IB's Halted tick changes. IB stamps that tick with no time, so its event's
    /// `time_exchange` is when it was received.
    Quotes,
    /// L2 order book via market_depth()
    Depth {
        /// Number of rows in the order book
        rows: i32,
    },
    /// Tick-by-tick trades via tick_by_tick().
    ///
    /// Halts are reported on [`Quotes`](Self::Quotes), not here: the tick IB sends in this data
    /// to mark a halt (zero price and size) is skipped.
    Trades,
    /// Real-time option Greeks via market_data() with OptionComputation ticks.
    ///
    /// Use this for option contracts to receive live Greeks (delta, gamma, theta,
    /// vega, IV) computed from market prices. Requires OPRA subscription for US
    /// options.
    OptionGreeks,
    /// Completed candles from IB's historical bars kept up to date; see
    /// [`candles`](super::candles).
    Candles(IbkrCandleRequest),
}

impl IbkrSubscriptionKind {
    /// Why a subscription of this kind cannot be made at `now`, if it cannot: the checks the
    /// constructors make, for a subscription built or deserialized without them.
    pub(super) fn refusal(&self, now: DateTime<Utc>) -> Option<String> {
        match self {
            Self::Depth { rows } if !(1..=20).contains(rows) => {
                Some(format!("IB depth rows must be 1-20, got {rows}"))
            }
            Self::Candles(request) => request.refusal(now),
            Self::Quotes | Self::Depth { .. } | Self::Trades | Self::OptionGreeks => None,
        }
    }
}

#[cfg(test)]
// Test code: a panic is the failure report.
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::exchange::ibkr::candles::{IbkrBarSource, IbkrLiveBarSize};
    use rustrade_instrument::instrument::name::InstrumentNameExchange;

    #[test]
    fn refusal_checks_depth_rows_and_candle_since() {
        let now = DateTime::parse_from_rfc3339("2026-11-23T14:30:00Z")
            .unwrap()
            .with_timezone(&Utc);
        for rows in [0, 21, -1] {
            let reason = IbkrSubscriptionKind::Depth { rows }.refusal(now).unwrap();
            assert!(reason.contains(&format!("got {rows}")), "{reason}");
        }
        for rows in [1, 20] {
            assert_eq!(IbkrSubscriptionKind::Depth { rows }.refusal(now), None);
        }
        for kind in [
            IbkrSubscriptionKind::Quotes,
            IbkrSubscriptionKind::Trades,
            IbkrSubscriptionKind::OptionGreeks,
        ] {
            assert_eq!(kind.refusal(now), None);
        }
        let request = IbkrCandleRequest::new(IbkrLiveBarSize::Min1, IbkrBarSource::Trades);
        assert_eq!(
            IbkrSubscriptionKind::Candles(request.clone()).refusal(now),
            None
        );
        let late = request.with_since(now + chrono::TimeDelta::minutes(5));
        assert!(IbkrSubscriptionKind::Candles(late).refusal(now).is_some());
    }

    #[test]
    fn candles_builds_a_candle_subscription() {
        let request = IbkrCandleRequest::new(IbkrLiveBarSize::Sec5, IbkrBarSource::MidPoint);
        let subscription =
            IbkrSubscription::candles(7, InstrumentNameExchange::new("AAPL"), request.clone());
        assert_eq!(subscription.key, 7);
        assert_eq!(subscription.kind, IbkrSubscriptionKind::Candles(request));
    }

    #[test]
    #[should_panic(expected = "IB depth rows must be 1-20")]
    fn depth_panics_on_zero_rows() {
        let instrument = InstrumentNameExchange::new("AAPL");
        IbkrSubscription::<()>::depth((), instrument, 0);
    }

    #[test]
    #[should_panic(expected = "IB depth rows must be 1-20")]
    fn depth_panics_on_rows_over_20() {
        let instrument = InstrumentNameExchange::new("AAPL");
        IbkrSubscription::<()>::depth((), instrument, 21);
    }
}
