//! Margin borrowing: the borrow and repay policy a margin account applies to its orders, the
//! simulated venue's spot margin configuration, and the interest it charges on its loans.
//!
//! [`MarginSideEffect`] is the policy a live margin client sends with each order, and the one the
//! simulated venue applies when [`SpotMarginConfig`] turns its spot margin on. A
//! [`MarginInterestModel`] prices those loans over time; [`PeriodicMarginInterest`] is the
//! built-in one.

use crate::holding_cost::{DayCount, RateSeries, next_whole_interval};
use chrono::{DateTime, TimeDelta, Utc};
use fnv::FnvHashMap;
use rust_decimal::Decimal;
use rustrade_instrument::asset::name::AssetNameExchange;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use thiserror::Error;

/// Whether a margin account borrows what an order lacks and repays its loans from what an order
/// receives, fixed once per client or simulated venue.
///
/// Only the two *intent-agnostic* modes are modelled. Whether an order opens or closes a position
/// decides whether it should borrow or repay, and that is position state the library deliberately
/// does not track, so the venue is left to infer it from the account's loans. The per-order modes
/// (Binance's `MARGIN_BUY` / `AUTO_REPAY`) are intentionally **not** modelled here; they belong to
/// a future generic per-order `RequestOpen::margin_effect` field that all margin adapters would
/// map, added only when a venue with genuine per-order borrow intent appears. Mixed borrow
/// appetite on a single venue today means running two clients (one per mode).
///
/// The `Serialize`/`Deserialize` wire form is the config-file value (`"auto_borrow_repay"` /
/// `"no_borrow"`), which is not any venue's API value.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum MarginSideEffect {
    /// Borrow what an order lacks when it is placed, and repay loans from what an order receives,
    /// as Binance's `AUTO_BORROW_REPAY` does.
    ///
    /// # Warning
    /// This is the default because it makes shorting work out of the box, but it means a
    /// **mis-sized order silently takes on debt**: an order larger than the free balance borrows
    /// the shortfall without any further opt-in (same footgun class as `RequestOpen::reduce_only`).
    /// Use [`MarginSideEffect::NoBorrow`] for a client that must never silently borrow.
    #[default]
    AutoBorrowRepay,
    /// Never borrow or repay; an order that would need to borrow is rejected, as Binance's
    /// `NO_SIDE_EFFECT` is. The conservative opt-out from the
    /// [`AutoBorrowRepay`](Self::AutoBorrowRepay) silent-borrow behaviour.
    NoBorrow,
}

/// Spot margin on the simulated venue: what it lends a spot order, how much, and what it repays.
///
/// Off by default: `MockExecutionConfig::spot_margin` is `None`, and the venue is a cash account
/// whose spot orders must be paid for with what it holds. With a configuration, it is a cross
/// margin account, with one pool of balances shared by every instrument, which lends a spot order
/// what it lacks and reports each balance's loan as Binance's margin account does.
///
/// # What it lends, and when
/// Under [`MarginSideEffect::AutoBorrowRepay`], an order the account cannot pay for with its free
/// balance borrows the shortfall, and only the shortfall, of the asset it pays with: base for a
/// sell, quote for a buy. It borrows when the venue accepts the order, as Binance does, so a
/// resting limit order or a held market order is lent its shortfall at once, and the venue holds
/// it against the order as it holds any other balance. An asset the account has never held is
/// opened with the loan. An order that is cancelled or expires before it fills repays what it
/// borrowed, interest first, and a held market order that fills for less than was held repays the
/// part of its loan it no longer needs. One that fills in part and rests the
/// remainder pays for the part that filled with the account's own balance first, so the loan is
/// held against the remainder and repaid if the remainder is cancelled.
///
/// Under [`MarginSideEffect::NoBorrow`], the venue lends nothing and repays nothing: an order it
/// cannot pay for is rejected as on a cash account, and a loan already in the venue's initial
/// state is left as it is. Balances still report their loans.
///
/// Only spot orders borrow. A CFD's margin is posted from the quote asset's free balance, as
/// without this configuration.
///
/// # How much it lends
/// A borrow is allowed while the account's debt, after it, is at most `max_leverage − 1` times
/// the account's equity. Both are valued in [`valuation_asset`](Self::valuation_asset):
/// - Each asset is valued at the mid of an instrument on the venue that pairs it with the
///   valuation asset, else that market's last price, in either direction (`BTC/USDT`, or the
///   inverse of `USDT/BTC`). The valuation asset is worth one. A market priced at zero values
///   nothing.
/// - **Equity** is the sum over assets of `total − borrowed − interest`, valued that way. An asset
///   the venue cannot value counts as no equity, which understates what the account can borrow
///   rather than overstating it.
/// - **Debt** is the sum over assets of `borrowed + interest`, valued the same way. The venue
///   lends nothing while it cannot value a debt, and never lends an asset it cannot value.
///
/// An order that needs more than that is rejected as unaffordable, with a reason that says how
/// much the venue would lend. A `max_leverage` of one lends nothing, but still reports each
/// balance's loan.
///
/// # What it repays
/// Under [`MarginSideEffect::AutoBorrowRepay`], whatever a fill delivers into an asset the account
/// owes repays that asset's loan, interest first, then principal, and only the rest becomes free
/// balance. What a buy delivers is its quantity less any fee charged in base, so a buy of exactly
/// what is owed, with a base fee, leaves a fee-sized loan. Gross the buy up by the fee to clear
/// it.
///
/// # What it does not model
/// - **Liquidation.** An account whose debt grows past its equity keeps trading; nothing closes
///   its positions.
/// - **Isolated margin.** One pool of balances backs every instrument.
/// - **Per-asset borrow limits and the venue's lending pool.** Only the leverage limit applies.
/// - **Interest**, by this configuration alone. A venue given a [`MarginInterestModel`] charges
///   it; see that trait.
///
/// [`new`](Self::new) refuses a `max_leverage` below one, and so does deserialising one, so a
/// configuration that exists is one the venue can apply. Serialised, `max_leverage` is a decimal
/// string (`"3"`), as the crate writes decimals elsewhere, so no precision is lost to a float.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize)]
#[serde(try_from = "SpotMarginRepr", into = "SpotMarginRepr")]
pub struct SpotMarginConfig(SpotMarginRepr);

/// The serialised form of a [`SpotMarginConfig`], before validation.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize)]
struct SpotMarginRepr {
    valuation_asset: AssetNameExchange,
    #[serde(with = "rust_decimal::serde::str")]
    max_leverage: Decimal,
    #[serde(default)]
    side_effect: MarginSideEffect,
}

impl SpotMarginConfig {
    /// Spot margin that borrows and repays automatically
    /// ([`MarginSideEffect::AutoBorrowRepay`]), up to `max_leverage` over the account's equity,
    /// valued in `valuation_asset`.
    ///
    /// # Errors
    /// Returns [`SpotMarginConfigError`] if `max_leverage` is below one.
    pub fn new(
        valuation_asset: AssetNameExchange,
        max_leverage: Decimal,
    ) -> Result<Self, SpotMarginConfigError> {
        Self::try_from(SpotMarginRepr {
            valuation_asset,
            max_leverage,
            side_effect: MarginSideEffect::AutoBorrowRepay,
        })
    }

    /// This configuration, applying `side_effect` instead.
    #[must_use]
    pub fn with_side_effect(mut self, side_effect: MarginSideEffect) -> Self {
        self.0.side_effect = side_effect;
        self
    }

    /// The asset equity and debt are valued in.
    pub fn valuation_asset(&self) -> &AssetNameExchange {
        &self.0.valuation_asset
    }

    /// The most the account may hold, as a multiple of its equity: its debt may reach
    /// `max_leverage − 1` times its equity.
    pub fn max_leverage(&self) -> Decimal {
        self.0.max_leverage
    }

    /// Whether the venue borrows and repays automatically.
    pub fn side_effect(&self) -> MarginSideEffect {
        self.0.side_effect
    }
}

impl TryFrom<SpotMarginRepr> for SpotMarginConfig {
    type Error = SpotMarginConfigError;

    fn try_from(repr: SpotMarginRepr) -> Result<Self, Self::Error> {
        if repr.max_leverage < Decimal::ONE {
            return Err(SpotMarginConfigError::LeverageBelowOne(repr.max_leverage));
        }
        Ok(Self(repr))
    }
}

impl From<SpotMarginConfig> for SpotMarginRepr {
    fn from(config: SpotMarginConfig) -> Self {
        config.0
    }
}

/// Why a [`SpotMarginConfig`] was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum SpotMarginConfigError {
    /// A `max_leverage` below one, which would allow less than the account's own equity.
    #[error("spot margin max_leverage {0} is below 1; a leverage of 1 lends nothing")]
    LeverageBelowOne(Decimal),
}

/// Charges interest on a simulated margin loan: once when the loan is taken, and at boundaries in
/// time while it stays open.
///
/// The simulated venue asks for a charge on each loan its spot margin takes
/// ([`InterestDue::Borrow`], on the amount just lent), and, through
/// [`next_boundary`](Self::next_boundary), at each boundary its clock reaches while the account
/// owes the asset ([`InterestDue::Boundary`], on the principal owed when it falls). Each charge is
/// added to the asset's loan as interest, which the account repays before principal, and is
/// reported as a [`CashFlowKind::MarginInterest`](crate::cash_flow::CashFlowKind::MarginInterest)
/// in the borrowed asset.
///
/// Interest is charged on principal only: interest already owed earns none.
///
/// # Contract
/// - `next_boundary` must return an instant strictly after `after`, or `None` when the model
///   never charges the asset at a boundary. A boundary at or before `after` would charge one
///   instant forever, so the venue **panics** on one.
/// - A charge's [`interest`](InterestCharge::interest) must not be negative: interest is owed,
///   never paid out. The venue **panics** on a negative one.
/// - Both methods must be pure functions of their arguments: the venue may ask for the same
///   boundary more than once, and charges it once.
///
/// In return, the venue asks [`charge`](Self::charge) only about a positive `principal`: an order
/// that lends nothing, and an asset that owes nothing, is never charged.
///
/// `Send + Sync` because a venue holds its model behind an `Arc`, so that one model, and the
/// rate history it carries, can be shared by every run of a backtest sweep without being copied.
pub trait MarginInterestModel: Debug + Send + Sync {
    /// The first boundary strictly after `after` at which this model charges a loan of `asset`,
    /// or `None` if it never does.
    fn next_boundary(
        &self,
        asset: &AssetNameExchange,
        after: DateTime<Utc>,
    ) -> Option<DateTime<Utc>>;

    /// What a loan of `principal` of `asset` is charged when `due`, or `None` for nothing.
    fn charge(
        &self,
        asset: &AssetNameExchange,
        principal: Decimal,
        due: InterestDue,
    ) -> Option<InterestCharge>;
}

/// When a [`MarginInterestModel`] is asked to charge a loan.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum InterestDue {
    /// The loan was taken at this instant, and the principal is the amount just lent.
    Borrow(DateTime<Utc>),
    /// A boundary the model named, and the principal is what the account owed when it fell.
    Boundary(DateTime<Utc>),
}

impl InterestDue {
    /// The instant the charge is due.
    pub fn time(self) -> DateTime<Utc> {
        match self {
            Self::Borrow(time) | Self::Boundary(time) => time,
        }
    }
}

/// One charge a [`MarginInterestModel`] makes on one loan.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InterestCharge {
    /// The interest owed, in the borrowed asset. Never negative; zero charges nothing.
    pub interest: Decimal,
    /// The rate it was charged at, reported as the cash flow's `rate` and used for nothing else,
    /// so a model may quote it annualised or per its own period. [`PeriodicMarginInterest`]
    /// reports the annualised rate.
    pub rate: Option<Decimal>,
}

impl InterestCharge {
    /// A charge of `interest`, in the borrowed asset, at `rate`.
    pub fn new(interest: Decimal, rate: Option<Decimal>) -> Self {
        Self { interest, rate }
    }
}

/// When a [`PeriodicMarginInterest`] charges a loan.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum InterestSchedule {
    /// One period when the loan is taken, then one at every boundary while it is open.
    #[default]
    AtBorrowAndEachBoundary,
    /// One period when the loan is taken, and nothing afterwards however long it is held.
    AtBorrowOnly,
}

/// Margin interest charged a period at a time, at an annualised rate per asset: a crypto margin
/// account's hourly interest.
///
/// - **When.** One period when a loan is taken, whenever in the period that is, then one at every
///   boundary while the loan is open, unless [`InterestSchedule::AtBorrowOnly`]. Boundaries fall
///   at whole multiples of the period since the Unix epoch, so an hourly period charges on the
///   hour, UTC. Each charge pays in advance for the period it falls in: the one a borrow is taken
///   in, or the one a boundary begins. So a loan repaid mid-period has already paid for that
///   period, and one repaid just before a boundary is not charged there.
/// - **How much.** `principal × rate × period / basis`, where the rate is annualised, read from
///   the asset's [`RateSeries`] at the instant charged, and the basis is the [`DayCount`]'s year.
///   Each charge is rounded **up** to the asset's unit, `1e-8` unless set by
///   [`with_unit`](Self::with_unit), so a small loan is charged at least one unit, as a venue
///   that books interest in whole units charges it.
/// - **Which assets.** Only those given rates. A loan of any other asset, or one charged before its
///   series begins, is charged nothing.
///
/// A venue that quotes a daily rate is reproduced by an annual rate of `daily × 365` on
/// [`DayCount::Act365`].
#[derive(Debug, Clone)]
pub struct PeriodicMarginInterest {
    period: TimeDelta,
    day_count: DayCount,
    schedule: InterestSchedule,
    rates: FnvHashMap<AssetNameExchange, RateSeries>,
    units: FnvHashMap<AssetNameExchange, Decimal>,
}

/// The unit a [`PeriodicMarginInterest`] rounds an asset's charge up to unless told otherwise: one
/// satoshi, the unit crypto venues book interest in.
const DEFAULT_INTEREST_UNIT: Decimal = Decimal::from_parts(1, 0, 0, false, 8);

impl PeriodicMarginInterest {
    /// Interest charged every `period` on the `day_count` basis, at borrow and each boundary, with
    /// no asset's rates yet.
    ///
    /// # Panics
    /// If `period` is not positive.
    pub fn new(period: TimeDelta, day_count: DayCount) -> Self {
        assert!(
            period > TimeDelta::zero(),
            "PeriodicMarginInterest period must be positive, got {period}"
        );
        Self {
            period,
            day_count,
            schedule: InterestSchedule::default(),
            rates: FnvHashMap::default(),
            units: FnvHashMap::default(),
        }
    }

    /// Charges a loan of `asset` at the annualised rates in `rates`.
    ///
    /// # Panics
    /// If any rate in `rates` is negative: a borrower is never paid interest, so a negative rate is
    /// a fault in the data, refused here rather than when the venue first charges it.
    #[must_use]
    pub fn with_rates(mut self, asset: AssetNameExchange, rates: RateSeries) -> Self {
        if let Some(rate) = rates.rates().find(|rate| *rate < Decimal::ZERO) {
            panic!("PeriodicMarginInterest rate for {asset} must not be negative, got {rate}");
        }
        self.rates.insert(asset, rates);
        self
    }

    /// Rounds each charge on `asset` up to a multiple of `unit` instead of `1e-8`.
    ///
    /// # Panics
    /// If `unit` is not positive.
    #[must_use]
    pub fn with_unit(mut self, asset: AssetNameExchange, unit: Decimal) -> Self {
        assert!(
            unit > Decimal::ZERO,
            "PeriodicMarginInterest unit for {asset} must be positive, got {unit}"
        );
        self.units.insert(asset, unit);
        self
    }

    /// Charges on `schedule` instead of at borrow and each boundary.
    #[must_use]
    pub fn with_schedule(mut self, schedule: InterestSchedule) -> Self {
        self.schedule = schedule;
        self
    }
}

impl MarginInterestModel for PeriodicMarginInterest {
    fn next_boundary(
        &self,
        asset: &AssetNameExchange,
        after: DateTime<Utc>,
    ) -> Option<DateTime<Utc>> {
        match self.schedule {
            InterestSchedule::AtBorrowOnly => None,
            InterestSchedule::AtBorrowAndEachBoundary if !self.rates.contains_key(asset) => None,
            InterestSchedule::AtBorrowAndEachBoundary => next_whole_interval(self.period, after),
        }
    }

    fn charge(
        &self,
        asset: &AssetNameExchange,
        principal: Decimal,
        due: InterestDue,
    ) -> Option<InterestCharge> {
        let rate = self.rates.get(asset)?.at(due.time())?;
        let unit = self
            .units
            .get(asset)
            .copied()
            .unwrap_or(DEFAULT_INTEREST_UNIT);
        let exact = self.day_count.accrue_span(principal * rate, self.period);

        Some(InterestCharge::new(
            (exact / unit).ceil() * unit,
            Some(rate),
        ))
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics are the correct failure mode
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn a_leverage_below_one_is_refused_by_the_constructor_and_by_deserialising() {
        assert_eq!(
            SpotMarginConfig::new(AssetNameExchange::new("usdt"), dec!(0.5)),
            Err(SpotMarginConfigError::LeverageBelowOne(dec!(0.5)))
        );

        let json = r#"{"valuation_asset":"usdt","max_leverage":"0.5"}"#;
        assert!(serde_json::from_str::<SpotMarginConfig>(json).is_err());
    }

    #[test]
    fn the_side_effect_defaults_to_auto_borrow_repay_when_omitted() {
        let json = r#"{"valuation_asset":"usdt","max_leverage":"3"}"#;
        let config: SpotMarginConfig = serde_json::from_str(json).unwrap();

        assert_eq!(config.side_effect(), MarginSideEffect::AutoBorrowRepay);
        assert_eq!(config.max_leverage(), dec!(3));
        assert_eq!(config.valuation_asset(), &AssetNameExchange::new("usdt"));
    }

    #[test]
    fn a_configuration_round_trips_through_its_serialised_form() {
        let config = SpotMarginConfig::new(AssetNameExchange::new("usdt"), dec!(3))
            .unwrap()
            .with_side_effect(MarginSideEffect::NoBorrow);

        let json = serde_json::to_string(&config).unwrap();
        assert_eq!(
            json,
            r#"{"valuation_asset":"usdt","max_leverage":"3","side_effect":"no_borrow"}"#
        );
        assert_eq!(
            serde_json::from_str::<SpotMarginConfig>(&json).unwrap(),
            config
        );
    }

    fn btc() -> AssetNameExchange {
        AssetNameExchange::new("btc")
    }

    fn usdt() -> AssetNameExchange {
        AssetNameExchange::new("usdt")
    }

    fn utc(text: &str) -> DateTime<Utc> {
        text.parse().unwrap()
    }

    /// Hourly interest on BTC at `annual`, on the Act365 basis.
    fn hourly_btc(annual: Decimal) -> PeriodicMarginInterest {
        PeriodicMarginInterest::new(TimeDelta::hours(1), DayCount::Act365)
            .with_rates(btc(), RateSeries::constant(annual))
    }

    #[test]
    fn a_small_loan_is_charged_one_unit_for_its_first_period() {
        // 0.00063 BTC at 0.39% a year is about 2.8e-10 an hour, rounded up to one satoshi.
        let model = hourly_btc(dec!(0.0039));

        let charge = model.charge(
            &btc(),
            dec!(0.00063),
            InterestDue::Borrow(utc("2026-10-15T01:02:33Z")),
        );

        assert_eq!(
            charge,
            Some(InterestCharge::new(dec!(0.00000001), Some(dec!(0.0039))))
        );
    }

    #[test]
    fn a_daily_rate_times_365_on_act365_charges_a_24th_of_it_an_hour_without_rounding_up() {
        // 0.01% a day on 24 BTC is 0.0024 a day, exactly 0.0001 an hour.
        let model = hourly_btc(dec!(0.0001) * dec!(365));

        let charge = model
            .charge(
                &btc(),
                dec!(24),
                InterestDue::Boundary(utc("2026-10-15T02:00:00Z")),
            )
            .unwrap();

        assert_eq!(charge.interest, dec!(0.0001));
    }

    #[test]
    fn a_charge_is_rounded_up_to_the_asset_s_own_unit() {
        let model = PeriodicMarginInterest::new(TimeDelta::hours(1), DayCount::Act365)
            .with_rates(usdt(), RateSeries::constant(dec!(0.0876)))
            .with_unit(usdt(), dec!(0.01));
        let at = InterestDue::Borrow(utc("2026-10-15T01:00:00Z"));

        // 8.76% a year is 0.001% an hour: 0.01 on 1,000, and 0.01001 on 1,001.
        let charged = |principal| model.charge(&usdt(), principal, at).unwrap().interest;
        assert_eq!(charged(dec!(1000)), dec!(0.01));
        assert_eq!(charged(dec!(1001)), dec!(0.02));
    }

    #[test]
    fn boundaries_fall_on_whole_periods_and_one_on_a_boundary_steps_to_the_next() {
        let model = hourly_btc(dec!(0.01));

        assert_eq!(
            model.next_boundary(&btc(), utc("2026-10-15T01:02:33Z")),
            Some(utc("2026-10-15T02:00:00Z"))
        );
        assert_eq!(
            model.next_boundary(&btc(), utc("2026-10-15T02:00:00Z")),
            Some(utc("2026-10-15T03:00:00Z"))
        );
    }

    #[test]
    fn at_borrow_only_has_no_boundaries_but_still_charges_the_borrow() {
        let model = hourly_btc(dec!(0.01)).with_schedule(InterestSchedule::AtBorrowOnly);
        let at = utc("2026-10-15T01:02:33Z");

        assert_eq!(model.next_boundary(&btc(), at), None);
        assert!(
            model
                .charge(&btc(), dec!(1), InterestDue::Borrow(at))
                .is_some()
        );
    }

    #[test]
    fn an_asset_without_rates_or_before_its_rates_begin_is_charged_nothing() {
        let start = utc("2026-10-15T00:00:00Z");
        let model = PeriodicMarginInterest::new(TimeDelta::hours(1), DayCount::Act365)
            .with_rates(btc(), RateSeries::new(vec![(start, dec!(0.01))]).unwrap());
        let before = InterestDue::Borrow(start - TimeDelta::seconds(1));

        assert_eq!(model.next_boundary(&usdt(), start), None);
        assert_eq!(
            model.charge(&usdt(), dec!(1), InterestDue::Borrow(start)),
            None
        );
        assert_eq!(model.charge(&btc(), dec!(1), before), None);
        assert!(
            model
                .charge(&btc(), dec!(1), InterestDue::Borrow(start))
                .is_some()
        );
    }

    #[test]
    #[should_panic(expected = "period must be positive")]
    fn a_period_that_is_not_positive_panics() {
        let _ = PeriodicMarginInterest::new(TimeDelta::zero(), DayCount::Act365);
    }

    #[test]
    #[should_panic(expected = "unit for btc must be positive")]
    fn a_unit_that_is_not_positive_panics() {
        let _ = hourly_btc(dec!(0.01)).with_unit(btc(), Decimal::ZERO);
    }

    #[test]
    #[should_panic(expected = "rate for btc must not be negative, got -0.01")]
    fn a_negative_rate_anywhere_in_the_series_panics() {
        let rates = RateSeries::new(vec![
            (utc("2026-10-01T00:00:00Z"), dec!(0.02)),
            (utc("2026-10-02T00:00:00Z"), dec!(-0.01)),
        ])
        .unwrap();
        let _ = PeriodicMarginInterest::new(TimeDelta::hours(1), DayCount::Act365)
            .with_rates(btc(), rates);
    }

    #[test]
    fn a_zero_rate_is_accepted_and_charges_nothing() {
        let model = hourly_btc(Decimal::ZERO);

        let charge = model.charge(
            &btc(),
            dec!(1),
            InterestDue::Borrow(utc("2026-10-15T01:00:00Z")),
        );

        assert_eq!(charge.map(|charge| charge.interest), Some(Decimal::ZERO));
    }
}
