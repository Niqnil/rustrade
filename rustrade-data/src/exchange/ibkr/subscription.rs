//! IBKR subscription types.

use rustrade_instrument::instrument::name::InstrumentNameExchange;
use serde::{Deserialize, Serialize};

/// Subscription configuration for IBKR market data.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IbkrSubscription<K> {
    /// Instrument key for MarketEvent output
    pub key: K,
    /// Barter instrument name (must be registered in ContractRegistry)
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
    /// Panics if `rows` is outside the valid range 1-20.
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
    /// Requires OPRA subscription ($1.50/mo) for US options.
    pub fn option_greeks(key: K, instrument: InstrumentNameExchange) -> Self {
        Self {
            key,
            instrument,
            kind: IbkrSubscriptionKind::OptionGreeks,
        }
    }
}

/// Type of IBKR market data subscription.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum IbkrSubscriptionKind {
    /// Best bid/ask quotes via market_data()
    Quotes,
    /// L2 order book via market_depth()
    Depth {
        /// Number of rows in the order book
        rows: i32,
    },
    /// Tick-by-tick trades via tick_by_tick()
    Trades,
    /// Real-time option Greeks via market_data() with OptionComputation ticks.
    ///
    /// Use this for option contracts to receive live Greeks (delta, gamma, theta,
    /// vega, IV) computed from market prices. Requires OPRA subscription for US
    /// options.
    OptionGreeks,
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustrade_instrument::instrument::name::InstrumentNameExchange;

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
