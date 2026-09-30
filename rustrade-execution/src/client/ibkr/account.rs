use crate::{
    balance::{AssetBalance, Balance},
    error::UnindexedClientError,
    position::Position,
};
use chrono::{DateTime, Utc};
use fnv::FnvHashMap;
use ibapi::contracts::SecurityType;
use rust_decimal::Decimal;
use rustrade_instrument::{
    asset::name::AssetNameExchange, instrument::name::InstrumentNameExchange,
};
use smol_str::SmolStr;
use std::str::FromStr;
use tracing::warn;

/// Aggregated balance data per currency from AccountSummary events.
#[derive(Debug, Default)]
pub struct BalanceAggregator {
    // SmolStr avoids heap allocation for short currency codes (USD, EUR, etc.)
    balances: FnvHashMap<SmolStr, CurrencyBalance>,
}

#[derive(Debug, Default, Clone)]
struct CurrencyBalance {
    total_cash: Option<Decimal>,
    available_funds: Option<Decimal>,
}

impl BalanceAggregator {
    pub fn new() -> Self {
        Self::default()
    }

    /// Process an AccountSummary event.
    pub fn process(&mut self, summary: &ibapi::accounts::AccountSummary) {
        let currency = SmolStr::new(&summary.currency);
        let entry = self.balances.entry(currency).or_default();

        match summary.tag.as_str() {
            "TotalCashValue" => {
                // Parse directly to Decimal to preserve precision
                match Decimal::from_str(&summary.value) {
                    Ok(val) => entry.total_cash = Some(val),
                    Err(e) => {
                        warn!(tag = %summary.tag, value = %summary.value, error = %e, "Failed to parse balance")
                    }
                }
            }
            "AvailableFunds" => match Decimal::from_str(&summary.value) {
                Ok(val) => entry.available_funds = Some(val),
                Err(e) => {
                    warn!(tag = %summary.tag, value = %summary.value, error = %e, "Failed to parse balance")
                }
            },
            _ => {}
        }
    }

    /// Convert aggregated data to rustrade AssetBalance list.
    pub fn to_balances(&self) -> Vec<AssetBalance<AssetNameExchange>> {
        let now = Utc::now();
        self.balances
            .iter()
            .filter_map(|(currency, bal)| {
                let total = bal.total_cash?;
                let free = bal.available_funds.unwrap_or(total);

                Some(AssetBalance {
                    asset: AssetNameExchange::from(currency.as_str()),
                    balance: Balance::new(total, free),
                    time_exchange: now,
                })
            })
            .collect()
    }

    /// Clear all aggregated data.
    pub fn clear(&mut self) {
        self.balances.clear();
    }
}

/// Collects IB position reports into at most one [`Position`] per instrument.
///
/// IB reports each position per account. For each account and instrument, the latest report
/// wins: IB streams a new report whenever a position changes, and on `ibapi` 4.2.0 the read can
/// start with stale replies to earlier failed calls (see `POSITION_STREAM_TIMEOUT`).
///
/// When more than one account holds the same instrument, the first account to report a non-zero
/// quantity, in IB's reporting order, is kept and the others are dropped with a warning. Summing them would report a
/// position that no single account holds.
#[derive(Debug, Default)]
pub(crate) struct PositionAggregator {
    /// Latest report per account and instrument, in the order each pair was first reported.
    reports: Vec<(InstrumentNameExchange, ibapi::accounts::Position)>,
    index: FnvHashMap<(String, InstrumentNameExchange), usize>,
}

impl PositionAggregator {
    /// Record a report for `instrument`, replacing any earlier one from the same account.
    pub(crate) fn process(
        &mut self,
        instrument: InstrumentNameExchange,
        position: ibapi::accounts::Position,
    ) {
        let key = (position.account.clone(), instrument);
        match self.index.get(&key) {
            Some(&i) => self.reports[i].1 = position,
            None => {
                self.index.insert(key.clone(), self.reports.len());
                self.reports.push((key.1, position));
            }
        }
    }

    /// One entry per reported instrument, in the order instruments were first reported.
    ///
    /// The [`Position`] is `None` when every account reported a zero quantity.
    ///
    /// # Errors
    ///
    /// [`UnindexedClientError::Internal`] if a quantity does not convert to a [`Decimal`].
    pub(crate) fn into_positions(
        self,
        now: DateTime<Utc>,
    ) -> Result<Vec<(InstrumentNameExchange, Option<Position>)>, UnindexedClientError> {
        let mut positions: Vec<(InstrumentNameExchange, Option<Position>)> = Vec::new();
        let mut by_instrument = FnvHashMap::default();
        for (instrument, report) in self.reports {
            let position = convert_position(&report, now)?;
            match by_instrument.get(&instrument) {
                None => {
                    by_instrument.insert(instrument.clone(), positions.len());
                    positions.push((instrument, position));
                }
                Some(&i) => {
                    let kept = &mut positions[i].1;
                    match (kept.is_some(), position) {
                        // A zero quantity in another account adds nothing.
                        (_, None) => {}
                        (false, position) => *kept = position,
                        (true, Some(_)) => warn!(
                            instrument = %instrument,
                            account = %report.account,
                            "IB reports a position in this instrument in more than one account; \
                             keeping the first account's and dropping this one"
                        ),
                    }
                }
            }
        }
        Ok(positions)
    }
}

/// Convert one IB position report to a [`Position`], or `None` if its quantity is zero.
///
/// - `quantity`: IB's signed position, negative when short. `ibapi` 4.2.0 hands it over as an
///   `f64`, converted with `Decimal::try_from`, which rounds to the float's precision of about 15
///   significant digits (0.1 stays 0.1) rather than keeping its exact binary expansion.
/// - `entry_price`: see [`entry_price`].
/// - `time_exchange`: `now`, since IB does not timestamp positions.
fn convert_position(
    report: &ibapi::accounts::Position,
    now: DateTime<Utc>,
) -> Result<Option<Position>, UnindexedClientError> {
    let quantity = Decimal::try_from(report.position).map_err(|e| {
        UnindexedClientError::Internal(format!(
            "IB position quantity {} for contract {} is not a decimal: {e}",
            report.position, report.contract.contract_id
        ))
    })?;
    if quantity.is_zero() {
        return Ok(None);
    }
    Ok(Some(Position::new(
        quantity,
        entry_price(report),
        None,
        None,
        None,
        None,
        now,
    )))
}

/// IB's average cost, quoted as orders are priced.
///
/// IB's average cost is per contract, so for a future or an option it is the price multiplied by
/// the contract multiplier; it is divided back out here. It also includes commissions.
///
/// `None`, logged unless the cost is simply absent, when:
/// - the average cost is zero, which is how `ibapi` 4.2.0 decodes an unset one;
/// - it does not convert to a [`Decimal`];
/// - IB sends no positive multiplier for a contract other than a stock or a forex pair, whose cost
///   is already per unit.
fn entry_price(report: &ibapi::accounts::Position) -> Option<Decimal> {
    if report.average_cost == 0.0 {
        return None;
    }
    let contract = &report.contract;
    let average_cost = Decimal::try_from(report.average_cost)
        .inspect_err(|e| {
            warn!(
                contract_id = contract.contract_id,
                average_cost = report.average_cost,
                error = %e,
                "IB average cost is not a decimal; leaving the entry price unset"
            )
        })
        .ok()?;
    let multiplier = Decimal::from_str(&contract.multiplier)
        .ok()
        .filter(|m| m.is_sign_positive() && !m.is_zero());
    match (multiplier, &contract.security_type) {
        (Some(multiplier), _) => average_cost.checked_div(multiplier),
        (None, SecurityType::Stock | SecurityType::ForexPair) => Some(average_cost),
        (None, security_type) => {
            warn!(
                contract_id = contract.contract_id,
                %security_type,
                multiplier = %contract.multiplier,
                "IB sent no contract multiplier; leaving the entry price unset"
            );
            None
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics are the correct failure mode
mod tests {
    use super::*;
    use ibapi::accounts::AccountSummary;
    use rust_decimal_macros::dec;
    use std::str::FromStr;

    fn mock_account_summary(tag: &str, value: &str, currency: &str) -> AccountSummary {
        AccountSummary {
            account: "DU123456".to_string(),
            tag: tag.to_string(),
            value: value.to_string(),
            currency: currency.to_string(),
        }
    }

    #[test]
    fn test_balance_aggregator() {
        let mut agg = BalanceAggregator::new();

        agg.process(&mock_account_summary("TotalCashValue", "10000.50", "USD"));
        agg.process(&mock_account_summary("AvailableFunds", "8000.25", "USD"));
        agg.process(&mock_account_summary("TotalCashValue", "5000.00", "EUR"));

        let balances = agg.to_balances();
        assert_eq!(balances.len(), 2);

        let usd = balances.iter().find(|b| b.asset.as_ref() == "USD").unwrap();
        assert_eq!(usd.balance.total, Decimal::from_str("10000.50").unwrap());
        assert_eq!(usd.balance.free, Decimal::from_str("8000.25").unwrap());

        let eur = balances.iter().find(|b| b.asset.as_ref() == "EUR").unwrap();
        assert_eq!(eur.balance.total, Decimal::from_str("5000.00").unwrap());
        assert_eq!(eur.balance.free, Decimal::from_str("5000.00").unwrap());
    }

    fn report(
        account: &str,
        security_type: SecurityType,
        multiplier: &str,
        quantity: f64,
        average_cost: f64,
    ) -> ibapi::accounts::Position {
        ibapi::accounts::Position {
            account: account.to_string(),
            contract: ibapi::contracts::Contract {
                contract_id: 42,
                security_type,
                multiplier: multiplier.to_string(),
                ..Default::default()
            },
            position: quantity,
            average_cost,
        }
    }

    fn instrument(name: &str) -> InstrumentNameExchange {
        InstrumentNameExchange::from(name)
    }

    fn now() -> DateTime<Utc> {
        DateTime::from_timestamp(1_700_000_000, 0).unwrap()
    }

    #[test]
    fn test_convert_position_stock_long() {
        let position =
            convert_position(&report("DU1", SecurityType::Stock, "", 10.0, 150.25), now())
                .unwrap()
                .unwrap();
        assert_eq!(
            position,
            Position::new(dec!(10), Some(dec!(150.25)), None, None, None, None, now())
        );
    }

    #[test]
    fn test_convert_position_short_keeps_sign() {
        let position = convert_position(&report("DU1", SecurityType::Stock, "", -5.0, 20.0), now())
            .unwrap()
            .unwrap();
        assert_eq!(position.quantity, dec!(-5));
        assert_eq!(position.entry_price, Some(dec!(20)));
    }

    #[test]
    fn test_convert_position_fractional_quantity_is_rounded_to_float_precision() {
        // 0.3 is not exact in binary; the conversion must not keep the float's expansion.
        let position = convert_position(&report("DU1", SecurityType::Stock, "", 0.3, 100.0), now())
            .unwrap()
            .unwrap();
        assert_eq!(position.quantity, dec!(0.3));
    }

    #[test]
    fn test_convert_position_zero_quantity_is_none() {
        assert_eq!(
            convert_position(&report("DU1", SecurityType::Stock, "", 0.0, 150.0), now()).unwrap(),
            None
        );
    }

    #[test]
    fn test_convert_position_non_finite_quantity_is_internal_error() {
        let result = convert_position(
            &report("DU1", SecurityType::Stock, "", f64::NAN, 1.0),
            now(),
        );
        assert!(matches!(result, Err(UnindexedClientError::Internal(_))));
    }

    #[test]
    fn test_entry_price_divides_by_multiplier() {
        // Option: 2.35 premium x 100 multiplier.
        let option = report("DU1", SecurityType::Option, "100", -2.0, 235.0);
        assert_eq!(entry_price(&option), Some(dec!(2.35)));
        // Future: 5000.25 x 50 multiplier.
        let future = report("DU1", SecurityType::Future, "50", 1.0, 250_012.5);
        assert_eq!(entry_price(&future), Some(dec!(5000.25)));
    }

    #[test]
    fn test_entry_price_without_multiplier() {
        // A stock or forex pair's average cost is already per unit.
        assert_eq!(
            entry_price(&report("DU1", SecurityType::Stock, "", 1.0, 150.0)),
            Some(dec!(150))
        );
        assert_eq!(
            entry_price(&report("DU1", SecurityType::ForexPair, "0", 1.0, 1.085)),
            Some(dec!(1.085))
        );
        // A derivative's cannot be scaled without one.
        assert_eq!(
            entry_price(&report("DU1", SecurityType::Future, "", 1.0, 250_000.0)),
            None
        );
        assert_eq!(
            entry_price(&report("DU1", SecurityType::Option, "0", 1.0, 235.0)),
            None
        );
    }

    #[test]
    fn test_entry_price_unset_or_invalid_average_cost_is_none() {
        assert_eq!(
            entry_price(&report("DU1", SecurityType::Stock, "", 1.0, 0.0)),
            None
        );
        assert_eq!(
            entry_price(&report("DU1", SecurityType::Stock, "", 1.0, f64::MAX)),
            None
        );
        assert_eq!(
            entry_price(&report("DU1", SecurityType::Stock, "", 1.0, f64::NAN)),
            None
        );
    }

    #[test]
    fn test_position_aggregator_latest_report_per_account_wins() {
        let mut agg = PositionAggregator::default();
        agg.process(
            instrument("AAPL"),
            report("DU1", SecurityType::Stock, "", 10.0, 150.0),
        );
        agg.process(
            instrument("MSFT"),
            report("DU1", SecurityType::Stock, "", 3.0, 400.0),
        );
        agg.process(
            instrument("AAPL"),
            report("DU1", SecurityType::Stock, "", 12.0, 151.0),
        );

        let positions = agg.into_positions(now()).unwrap();
        assert_eq!(positions.len(), 2);
        assert_eq!(positions[0].0, instrument("AAPL"));
        assert_eq!(positions[0].1.as_ref().unwrap().quantity, dec!(12));
        assert_eq!(positions[1].0, instrument("MSFT"));
    }

    #[test]
    fn test_position_aggregator_closed_position_is_listed_without_position() {
        let mut agg = PositionAggregator::default();
        agg.process(
            instrument("AAPL"),
            report("DU1", SecurityType::Stock, "", 10.0, 150.0),
        );
        agg.process(
            instrument("AAPL"),
            report("DU1", SecurityType::Stock, "", 0.0, 0.0),
        );

        assert_eq!(
            agg.into_positions(now()).unwrap(),
            vec![(instrument("AAPL"), None)]
        );
    }

    #[test]
    fn test_position_aggregator_later_flat_account_and_update_keep_latest() {
        let mut agg = PositionAggregator::default();
        agg.process(
            instrument("AAPL"),
            report("DU2", SecurityType::Stock, "", 7.0, 150.0),
        );
        // A flat account after a held one changes nothing.
        agg.process(
            instrument("AAPL"),
            report("DU1", SecurityType::Stock, "", 0.0, 0.0),
        );
        // DU2's newer report replaces its earlier one.
        agg.process(
            instrument("AAPL"),
            report("DU2", SecurityType::Stock, "", 9.0, 151.0),
        );

        let positions = agg.into_positions(now()).unwrap();
        assert_eq!(positions.len(), 1);
        assert_eq!(positions[0].1.as_ref().unwrap().quantity, dec!(9));
    }

    #[test]
    fn test_position_aggregator_non_finite_quantity_is_internal_error() {
        let mut agg = PositionAggregator::default();
        agg.process(
            instrument("AAPL"),
            report("DU1", SecurityType::Stock, "", f64::NAN, 150.0),
        );

        assert!(matches!(
            agg.into_positions(now()),
            Err(UnindexedClientError::Internal(_))
        ));
    }

    #[test]
    fn test_position_aggregator_multi_account_keeps_first_non_zero() {
        let mut agg = PositionAggregator::default();
        // DU1 is flat, so it must not hide DU2's position.
        agg.process(
            instrument("AAPL"),
            report("DU1", SecurityType::Stock, "", 0.0, 0.0),
        );
        agg.process(
            instrument("AAPL"),
            report("DU2", SecurityType::Stock, "", 7.0, 150.0),
        );
        // A third account's position is dropped, not summed.
        agg.process(
            instrument("AAPL"),
            report("DU3", SecurityType::Stock, "", 4.0, 149.0),
        );

        let positions = agg.into_positions(now()).unwrap();
        assert_eq!(positions.len(), 1);
        assert_eq!(positions[0].1.as_ref().unwrap().quantity, dec!(7));
    }

    #[test]
    fn test_balance_aggregator_clear() {
        let mut agg = BalanceAggregator::new();
        agg.process(&mock_account_summary("TotalCashValue", "1000", "USD"));
        assert_eq!(agg.to_balances().len(), 1);

        agg.clear();
        assert!(agg.to_balances().is_empty());
    }
}
