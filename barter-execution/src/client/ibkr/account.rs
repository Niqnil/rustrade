use crate::balance::{AssetBalance, Balance};
use barter_instrument::asset::name::AssetNameExchange;
use chrono::Utc;
use fnv::FnvHashMap;
use rust_decimal::Decimal;
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

    /// Convert aggregated data to barter AssetBalance list.
    pub fn to_balances(&self) -> Vec<AssetBalance<AssetNameExchange>> {
        let now = Utc::now();
        self.balances
            .iter()
            .filter_map(|(currency, bal)| {
                let total = bal.total_cash?;
                let free = bal.available_funds.unwrap_or(total);

                Some(AssetBalance {
                    asset: AssetNameExchange::from(currency.as_str()),
                    balance: Balance { total, free },
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

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics are the correct failure mode
mod tests {
    use super::*;
    use ibapi::accounts::AccountSummary;
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

    #[test]
    fn test_balance_aggregator_clear() {
        let mut agg = BalanceAggregator::new();
        agg.process(&mock_account_summary("TotalCashValue", "1000", "USD"));
        assert_eq!(agg.to_balances().len(), 1);

        agg.clear();
        assert!(agg.to_balances().is_empty());
    }
}
