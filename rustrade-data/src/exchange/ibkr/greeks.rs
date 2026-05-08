//! Greeks aggregation for IB option tick data.
//!
//! IB sends option computation ticks containing real-time Greeks computed from
//! live market data. This aggregator collects these updates and emits
//! [`OptionGreeks`] values.
//!
//! Unlike the calculator APIs in [`IbkrHistoricalData`], these are real-time
//! values computed by IB from live option prices.
//!
//! [`IbkrHistoricalData`]: super::historical::IbkrHistoricalData

use super::options::OptionGreeks;
use ibapi::market_data::realtime::TickTypes;

/// Aggregates IB option computation ticks into OptionGreeks.
///
/// Emits an [`OptionGreeks`] on each `TickTypes::OptionComputation` update.
/// Does not aggregate across multiple ticks — each tick is a complete snapshot.
#[derive(Debug, Default)]
pub struct GreeksAggregator;

impl GreeksAggregator {
    /// Create a new aggregator.
    pub fn new() -> Self {
        Self
    }

    /// Process a tick and potentially emit OptionGreeks.
    ///
    /// Returns `Some(OptionGreeks)` when the tick contains option computation data.
    /// Returns `None` for other tick types.
    ///
    /// # Arguments
    ///
    /// * `tick` - The tick update from IB
    pub fn update(&self, tick: &TickTypes) -> Option<OptionGreeks> {
        match tick {
            TickTypes::OptionComputation(computation) => {
                let greeks = OptionGreeks::from_ib(computation);
                if greeks.has_any_greek() {
                    Some(greeks)
                } else {
                    None
                }
            }
            _ => None,
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use ibapi::contracts::tick_types::TickType;

    fn make_option_computation(
        delta: Option<f64>,
        gamma: Option<f64>,
        theta: Option<f64>,
        vega: Option<f64>,
        iv: Option<f64>,
        price: Option<f64>,
    ) -> TickTypes {
        use ibapi::contracts::OptionComputation;

        TickTypes::OptionComputation(OptionComputation {
            field: TickType::ModelOption,
            tick_attribute: Some(1),
            delta,
            gamma,
            theta,
            vega,
            implied_volatility: iv,
            option_price: price,
            underlying_price: Some(150.0),
            present_value_dividend: Some(0.0),
        })
    }

    #[test]
    fn aggregator_emits_greeks_on_option_computation() {
        let agg = GreeksAggregator::new();

        let tick = make_option_computation(
            Some(0.55),
            Some(0.02),
            Some(-0.05),
            Some(0.15),
            Some(0.25),
            Some(5.50),
        );

        let result = agg.update(&tick);
        assert!(result.is_some());

        let greeks = result.unwrap();
        assert_eq!(greeks.delta, Some(0.55));
        assert_eq!(greeks.gamma, Some(0.02));
        assert_eq!(greeks.theta, Some(-0.05));
        assert_eq!(greeks.vega, Some(0.15));
        assert_eq!(greeks.implied_volatility, Some(0.25));
        assert_eq!(greeks.theoretical_price, Some(5.50));
    }

    #[test]
    fn aggregator_returns_none_for_empty_computation() {
        let agg = GreeksAggregator::new();

        let tick = make_option_computation(None, None, None, None, None, None);

        let result = agg.update(&tick);
        assert!(result.is_none());
    }

    #[test]
    fn aggregator_returns_none_for_other_tick_types() {
        use ibapi::market_data::realtime::{TickAttribute, TickPrice};

        let agg = GreeksAggregator::new();

        let tick = TickTypes::Price(TickPrice {
            tick_type: TickType::Bid,
            price: 100.0,
            attributes: TickAttribute::default(),
        });

        let result = agg.update(&tick);
        assert!(result.is_none());
    }

    #[test]
    fn aggregator_emits_partial_greeks() {
        let agg = GreeksAggregator::new();

        let tick = make_option_computation(Some(0.55), None, None, None, Some(0.25), None);

        let result = agg.update(&tick);
        assert!(result.is_some());

        let greeks = result.unwrap();
        assert_eq!(greeks.delta, Some(0.55));
        assert!(greeks.gamma.is_none());
        assert_eq!(greeks.implied_volatility, Some(0.25));
    }
}
