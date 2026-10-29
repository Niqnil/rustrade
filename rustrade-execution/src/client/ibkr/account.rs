use crate::{
    balance::{AssetBalance, Balance},
    error::UnindexedClientError,
    position::{Position, PositionReport},
};
use chrono::{DateTime, Utc};
use fnv::FnvHashMap;
use ibapi::{accounts::AccountSummaryTags, contracts::SecurityType};
use rust_decimal::Decimal;
use rustrade_instrument::{
    asset::name::AssetNameExchange, instrument::name::InstrumentNameExchange,
};
use smol_str::SmolStr;
use std::{
    collections::{BTreeMap, BTreeSet},
    str::FromStr,
};
use tracing::warn;

/// The account-summary tags [`BalanceAggregator`] reads: `AvailableFunds`, and IB's per-currency
/// ledger for every currency of every account in the group.
pub(crate) const BALANCE_SUMMARY_TAGS: &[&str] = &[
    AccountSummaryTags::AVAILABLE_FUNDS,
    AccountSummaryTags::LEDGER_ALL,
];

/// The ledger's row for its totals converted into base currency. It is a sum of the other rows,
/// not a currency anyone holds.
const LEDGER_BASE: &str = "BASE";

/// Per-currency balances from an account-summary listing of `AvailableFunds` and `$LEDGER:ALL`.
///
/// One balance per currency in IB's ledger (`$LEDGER:ALL`), summed over every account in the
/// group:
/// - `total` is the currency's `TotalCashBalance`, or its `CashBalance` when that is missing.
///   IB defines both, like the account-level `TotalCashValue`, as cash recognized at the time of
///   trade plus futures PnL.
/// - `accrued` is the currency's `AccruedCash`: interest accrued this month and not yet posted.
/// - `free` is `total`, capped at the account's `AvailableFunds` converted into the currency at
///   the account's ledger `ExchangeRate`. `AvailableFunds` is account-level and in base currency
///   only, so every currency of an account is capped by the same funds and the `free` values do
///   not add up. Without `AvailableFunds`, or without a rate for the currency, `free` is `total`.
///   `free` is negative when `total` is.
///
/// Each account is capped by its own funds and rates, since accounts can differ in base currency,
/// and the accounts are then summed per currency. The ledger's `BASE` row is skipped.
#[derive(Debug, Default)]
pub struct BalanceAggregator {
    /// Ledger rows by account, then currency. SmolStr avoids heap allocation for short codes.
    ledger: FnvHashMap<(SmolStr, SmolStr), LedgerCurrency>,
    /// `AvailableFunds` per account, in its account's base currency.
    available_funds: FnvHashMap<SmolStr, Decimal>,
}

#[derive(Debug, Default, Clone)]
struct LedgerCurrency {
    total_cash_balance: Option<Decimal>,
    cash_balance: Option<Decimal>,
    accrued_cash: Option<Decimal>,
    /// The account's base-currency units per unit of this currency.
    exchange_rate: Option<Decimal>,
}

impl BalanceAggregator {
    pub fn new() -> Self {
        Self::default()
    }

    /// Process an AccountSummary event.
    pub fn process(&mut self, summary: &ibapi::accounts::AccountSummary) {
        let field: fn(&mut LedgerCurrency) -> &mut Option<Decimal> = match summary.tag.as_str() {
            AccountSummaryTags::AVAILABLE_FUNDS => {
                if let Some(value) = parse_summary_value(summary) {
                    self.available_funds
                        .insert(SmolStr::new(&summary.account), value);
                }
                return;
            }
            "TotalCashBalance" => |c| &mut c.total_cash_balance,
            "CashBalance" => |c| &mut c.cash_balance,
            AccountSummaryTags::ACCRUED_CASH => |c| &mut c.accrued_cash,
            "ExchangeRate" => |c| &mut c.exchange_rate,
            _ => return,
        };
        if let Some(value) = parse_summary_value(summary) {
            let currency = self
                .ledger
                .entry((
                    SmolStr::new(&summary.account),
                    SmolStr::new(&summary.currency),
                ))
                .or_default();
            *field(currency) = Some(value);
        }
    }

    /// Convert aggregated data to rustrade AssetBalance list, ordered by currency, each stamped
    /// `time`.
    pub fn to_balances(&self, time: DateTime<Utc>) -> Vec<AssetBalance<AssetNameExchange>> {
        let mut balances = BTreeMap::<&str, Balance>::new();
        // Currencies whose `free` is left uncapped though their account reported `AvailableFunds`.
        let mut unconverted = BTreeSet::<&str>::new();

        for ((account, currency), ledger) in &self.ledger {
            if currency.as_str() == LEDGER_BASE {
                continue;
            }
            let Some(total) = ledger.total_cash_balance.or(ledger.cash_balance) else {
                warn!(%account, %currency, "IB's ledger lists the currency without a cash balance; skipped");
                continue;
            };
            let rate = ledger.exchange_rate.filter(|rate| *rate > Decimal::ZERO);
            let free = match (self.available_funds.get(account), rate) {
                (Some(funds), Some(rate)) => funds
                    .checked_div(rate)
                    .map_or(total, |funds| total.min(funds)),
                (Some(_), None) => {
                    unconverted.insert(currency);
                    total
                }
                (None, _) => total,
            };

            let sum = balances.entry(currency).or_default();
            sum.total += total;
            sum.free += free;
            sum.accrued = match (sum.accrued, ledger.accrued_cash) {
                (Some(sum), Some(accrued)) => Some(sum + accrued),
                (sum, accrued) => sum.or(accrued),
            };
        }

        if !unconverted.is_empty() {
            warn!(
                currencies = ?unconverted,
                "IB's ledger gives no exchange rate to convert AvailableFunds into these \
                 currencies; their free balance is not capped"
            );
        }

        balances
            .into_iter()
            .map(|(currency, balance)| AssetBalance {
                asset: AssetNameExchange::from(currency),
                balance,
                time_exchange: time,
            })
            .collect()
    }

    /// Clear all aggregated data.
    pub fn clear(&mut self) {
        self.ledger.clear();
        self.available_funds.clear();
    }
}

/// Parse an account-summary value, warning if it is not a number.
fn parse_summary_value(summary: &ibapi::accounts::AccountSummary) -> Option<Decimal> {
    // Parse directly to Decimal to preserve precision
    Decimal::from_str(&summary.value)
        .inspect_err(|e| {
            warn!(tag = %summary.tag, value = %summary.value, error = %e, "Failed to parse balance")
        })
        .ok()
}

/// Collects IB position reports into at most one [`Position`] per instrument.
///
/// IB reports each position per account. For each account and instrument, the latest report
/// wins.
///
/// When more than one account holds the same instrument, the first account to report a non-zero
/// quantity, in IB's reporting order, is kept and the others are dropped with a warning. Summing them would report a
/// position that no single account holds.
#[derive(Debug, Default)]
pub(crate) struct PositionAggregator {
    /// Latest report per account and instrument, in the order each pair was first reported.
    reports: Vec<(InstrumentNameExchange, ibapi::accounts::Position)>,
    index: FnvHashMap<(String, InstrumentNameExchange), usize>,
    /// Whether IB marked the end of its listing (`PositionEnd`), so every position it holds has
    /// been reported.
    listed_all: bool,
}

impl PositionAggregator {
    /// Note that IB marked the end of its listing (`PositionEnd`).
    pub(crate) fn listing_ended(&mut self) {
        self.listed_all = true;
    }

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

    /// The [`PositionReport`] of each instrument in an `account_snapshot`.
    ///
    /// Each reported instrument is [`PositionReport::Open`], or [`PositionReport::Flat`] when
    /// every account reported zero, as IB does for a position closed today. `requested` are the
    /// requested instruments that IB's reports can be attributed to. Those IB did not report are
    /// [`PositionReport::Flat`] when the read ended on a complete listing (see
    /// [`Self::listing_ended`]). Otherwise the listing may be incomplete, so they are left out,
    /// with a warning.
    ///
    /// # Errors
    ///
    /// As [`Self::into_positions`].
    pub(crate) fn into_reports<'a>(
        self,
        now: DateTime<Utc>,
        requested: impl IntoIterator<Item = &'a InstrumentNameExchange>,
    ) -> Result<Vec<(InstrumentNameExchange, PositionReport)>, UnindexedClientError> {
        let listed_all = self.listed_all;
        let mut reports: Vec<_> = self
            .into_positions(now)?
            .into_iter()
            .map(|(instrument, position)| {
                let report = position.map_or(PositionReport::Flat, PositionReport::Open);
                (instrument, report)
            })
            .collect();
        let unlisted: Vec<_> = requested
            .into_iter()
            .filter(|instrument| !reports.iter().any(|(listed, _)| listed == *instrument))
            .cloned()
            .collect();
        if listed_all {
            reports.extend(
                unlisted
                    .into_iter()
                    .map(|instrument| (instrument, PositionReport::Flat)),
            );
        } else if !unlisted.is_empty() {
            warn!(
                unlisted = unlisted.len(),
                "IB did not finish listing positions during the read; requested instruments it \
                 did not list are left out rather than reported flat"
            );
        }
        Ok(reports)
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
/// - `quantity`: IB's signed position, negative when short. `ibapi` hands it over as an
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
/// - the average cost is zero, which is how `ibapi` decodes an unset one;
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

    fn mock_account_summary(tag: &str, value: &str, currency: &str) -> AccountSummary {
        account_summary("DU123456", tag, value, currency)
    }

    fn account_summary(account: &str, tag: &str, value: &str, currency: &str) -> AccountSummary {
        AccountSummary {
            account: account.to_string(),
            tag: tag.to_string(),
            value: value.to_string(),
            currency: currency.to_string(),
        }
    }

    /// Feed `agg` one account's `AvailableFunds` and a ledger row per `(currency, cash,
    /// accrued, rate)`, as `$LEDGER:ALL` lists them.
    fn ledger(
        agg: &mut BalanceAggregator,
        available_funds: Option<(&str, &str)>,
        rows: &[(&str, &str, &str, &str)],
    ) {
        account_ledger(agg, "DU123456", available_funds, rows);
    }

    /// [`ledger`] for the account `account`.
    fn account_ledger(
        agg: &mut BalanceAggregator,
        account: &str,
        available_funds: Option<(&str, &str)>,
        rows: &[(&str, &str, &str, &str)],
    ) {
        if let Some((currency, value)) = available_funds {
            agg.process(&account_summary(account, "AvailableFunds", value, currency));
        }
        for &(currency, cash, accrued, rate) in rows {
            for (tag, value) in [
                ("Currency", currency),
                ("CashBalance", cash),
                ("TotalCashBalance", cash),
                ("AccruedCash", accrued),
                ("ExchangeRate", rate),
                ("RealCurrency", currency),
            ] {
                agg.process(&account_summary(account, tag, value, currency));
            }
        }
    }

    fn balance_of<'a>(
        balances: &'a [AssetBalance<AssetNameExchange>],
        currency: &str,
    ) -> &'a Balance {
        &balances
            .iter()
            .find(|b| b.asset.as_ref() == currency)
            .unwrap()
            .balance
    }

    /// The shape of a paper account's ledger: base SGD with some USD. Each currency is its own
    /// balance with its accrual, and the BASE row is not one.
    #[test]
    fn test_balance_aggregator_reads_the_ledger_per_currency() {
        let mut agg = BalanceAggregator::new();
        ledger(
            &mut agg,
            Some(("SGD", "1001324.57")),
            &[
                ("SGD", "1001190.04", "123.90", "1.00"),
                ("USD", "8.30", "0.00", "1.2801581"),
                ("BASE", "1001200.6653", "123.90", "1.00"),
            ],
        );

        let balances = agg.to_balances(Utc::now());
        let currencies = balances
            .iter()
            .map(|b| b.asset.as_ref())
            .collect::<Vec<_>>();
        assert_eq!(currencies, ["SGD", "USD"], "ordered by currency");
        let sgd = balance_of(&balances, "SGD");
        assert_eq!(sgd.total, dec!(1001190.04));
        assert_eq!(sgd.free, dec!(1001190.04), "free is capped at total");
        assert_eq!(sgd.accrued, Some(dec!(123.90)));
        let usd = balance_of(&balances, "USD");
        assert_eq!(usd.total, dec!(8.30));
        assert_eq!(usd.free, dec!(8.30));
        assert_eq!(usd.accrued, Some(dec!(0.00)));
    }

    /// `free` is the account's `AvailableFunds` in the row's currency when that is below its cash.
    #[test]
    fn test_balance_aggregator_caps_free_at_available_funds() {
        let mut agg = BalanceAggregator::new();
        ledger(
            &mut agg,
            Some(("SGD", "1280")),
            &[("SGD", "5000", "0", "1"), ("USD", "4000", "-2.5", "1.28")],
        );

        let balances = agg.to_balances(Utc::now());
        assert_eq!(balance_of(&balances, "SGD").free, dec!(1280));
        let usd = balance_of(&balances, "USD");
        assert_eq!(usd.free, dec!(1000), "1280 SGD at 1.28 SGD per USD");
        assert_eq!(usd.accrued, Some(dec!(-2.5)));
    }

    /// Accounts are summed per currency, each capped by its own `AvailableFunds` at its own rates:
    /// here one account is based in SGD and the other in USD.
    #[test]
    fn test_balance_aggregator_sums_accounts_each_capped_by_its_own_funds() {
        let mut agg = BalanceAggregator::new();
        account_ledger(
            &mut agg,
            "DU1",
            Some(("SGD", "1280")),
            &[("SGD", "5000", "0", "1"), ("USD", "4000", "-2.5", "1.28")],
        );
        account_ledger(
            &mut agg,
            "DU2",
            Some(("USD", "100")),
            &[("USD", "500", "0.5", "1"), ("BASE", "500", "0.5", "1")],
        );

        let balances = agg.to_balances(Utc::now());
        assert_eq!(balances.len(), 2, "{balances:?}");
        let sgd = balance_of(&balances, "SGD");
        assert_eq!((sgd.total, sgd.free), (dec!(5000), dec!(1280)));
        let usd = balance_of(&balances, "USD");
        assert_eq!(usd.total, dec!(4500));
        assert_eq!(usd.free, dec!(1100), "1280 SGD at 1.28, plus DU2's 100");
        assert_eq!(usd.accrued, Some(dec!(-2.0)));
    }

    /// An accrual is summed over the accounts that report one, and stays `None` if none does.
    #[test]
    fn test_balance_aggregator_sums_accruals_reported() {
        let mut agg = BalanceAggregator::new();
        agg.process(&account_summary("DU1", "CashBalance", "10", "EUR"));
        agg.process(&account_summary("DU2", "CashBalance", "20", "EUR"));
        agg.process(&account_summary("DU2", "AccruedCash", "-1", "EUR"));
        agg.process(&account_summary("DU1", "CashBalance", "5", "JPY"));

        let balances = agg.to_balances(Utc::now());
        let eur = balance_of(&balances, "EUR");
        assert_eq!((eur.total, eur.accrued), (dec!(30), Some(dec!(-1))));
        assert_eq!(balance_of(&balances, "JPY").accrued, None);
    }

    /// Without `AvailableFunds`, or a rate to convert it, `free` is `total`; an unparseable value
    /// is skipped.
    #[test]
    fn test_balance_aggregator_free_is_total_without_funds_or_rate() {
        let mut agg = BalanceAggregator::new();
        ledger(&mut agg, None, &[("USD", "-50", "-0.1", "1")]);
        let usd = *balance_of(&agg.to_balances(Utc::now()), "USD");
        assert_eq!((usd.total, usd.free), (dec!(-50), dec!(-50)));

        let mut agg = BalanceAggregator::new();
        ledger(&mut agg, Some(("SGD", "10")), &[("USD", "500", "0", "n/a")]);
        assert_eq!(
            balance_of(&agg.to_balances(Utc::now()), "USD").free,
            dec!(500)
        );
    }

    /// `CashBalance` stands in when `TotalCashBalance` is missing, and a currency with neither, or
    /// without `AccruedCash`, is handled.
    #[test]
    fn test_balance_aggregator_cash_fallbacks() {
        let mut agg = BalanceAggregator::new();
        agg.process(&mock_account_summary("CashBalance", "75", "EUR"));
        agg.process(&mock_account_summary("AccruedCash", "1", "JPY"));

        let balances = agg.to_balances(Utc::now());
        assert_eq!(balances.len(), 1, "JPY has no cash: {balances:?}");
        let eur = balance_of(&balances, "EUR");
        assert_eq!(
            (eur.total, eur.free, eur.accrued),
            (dec!(75), dec!(75), None)
        );
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
            Position::new(
                dec!(10),
                Some(dec!(150.25)),
                None,
                None,
                None,
                None,
                None,
                now()
            )
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
        ledger(&mut agg, Some(("USD", "900")), &[("USD", "1000", "0", "1")]);
        assert_eq!(agg.to_balances(Utc::now()).len(), 1);

        agg.clear();
        assert!(agg.to_balances(Utc::now()).is_empty());
    }

    #[test]
    fn reports_are_open_or_flat_and_add_unlisted_instruments_only_after_a_complete_listing() {
        let requested = [instrument("AAPL"), instrument("MSFT")];
        let reports = |events: &[&str]| {
            let mut agg = PositionAggregator::default();
            for event in events {
                match *event {
                    "end" => agg.listing_ended(),
                    name => {
                        let quantity = if name == "F" { 0.0 } else { 3.0 };
                        agg.process(
                            instrument(name),
                            report("DU1", SecurityType::Stock, "", quantity, 10.0),
                        );
                    }
                }
            }
            agg.into_reports(now(), &requested).unwrap()
        };
        let aapl = || {
            (
                instrument("AAPL"),
                PositionReport::from_position(
                    convert_position(&report("DU1", SecurityType::Stock, "", 3.0, 10.0), now())
                        .unwrap()
                        .unwrap(),
                ),
            )
        };

        assert_eq!(
            reports(&["AAPL", "F", "end"]),
            vec![
                aapl(),
                (instrument("F"), PositionReport::Flat),
                (instrument("MSFT"), PositionReport::Flat),
            ]
        );
        // No end marker: MSFT's absence establishes nothing.
        assert_eq!(
            reports(&["AAPL", "F"]),
            vec![aapl(), (instrument("F"), PositionReport::Flat)]
        );
    }
}
