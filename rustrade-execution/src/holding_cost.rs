//! What a simulated position costs to hold over time: perpetual funding, borrow fees and
//! overnight financing.
//!
//! A [`FeeModel`](crate::fee::FeeModel) prices a fill, at the instant it happens. A
//! [`HoldingCostModel`] prices holding the position afterwards, at boundaries in time: a funding
//! time, a daily cutoff. The simulated venue asks each configured model for its next boundary,
//! charges the position it holds across that boundary, and reports each charge as the same
//! [`CashFlow`](crate::cash_flow::CashFlow) a live venue sends, so a backtest and a live run reach
//! the engine's `Position::carry` by one path.
//!
//! The built-in models cover the three costs a backtest of a short or leveraged strategy most
//! often leaves out:
//! - [`FundingModel`]: a perpetual's funding, every fixed interval, from a replayed rate.
//! - [`BorrowFeeModel`]: the fee for borrowing what a short sold, daily, from an annualised rate.
//! - [`FinancingModel`]: a CFD's overnight financing, daily, at separate long and short rates.
//!
//! Every rate is a [`RateSeries`], a step function of time, so a constant rate and a replayed
//! history are one type. A daily charge is timed and sized by a [`DailyAccrual`]: the cutoff, its
//! time zone, which days have one ([`AccrualDays`]) and the year basis ([`DayCount`]).
//!
//! # Where the models run
//! Configured on a venue through `SimVenueOptions` in `rustrade`, which drives the venue to each
//! boundary as it falls. A [`SimulatedVenue`](crate::exchange::mock::SimulatedVenue) built
//! directly takes them through
//! [`with_holding_costs`](crate::exchange::mock::SimulatedVenue::with_holding_costs), and charges
//! a boundary when its driver next moves its clock past it. `MockExchange` charges none.

use crate::cash_flow::CashFlowKind;
use chrono::{DateTime, Datelike, NaiveDate, NaiveTime, TimeDelta, TimeZone, Utc, Weekday};
use fnv::FnvHashMap;
use rust_decimal::Decimal;
use rustrade_instrument::instrument::name::InstrumentNameExchange;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeSet, fmt::Debug, sync::Arc};

/// Charges a simulated position for holding it across boundaries in time.
///
/// The venue calls [`next_boundary`](Self::next_boundary) to learn when the model next charges an
/// instrument, and [`charge`](Self::charge) once for each boundary its clock reaches while the
/// account holds a position there. Each charge becomes one
/// [`CashFlow`](crate::cash_flow::CashFlow), debited from or credited to the instrument's quote
/// asset.
///
/// # Which position is charged
/// The position held **across** the boundary. A position closed at the boundary instant is still
/// charged for it, and one opened at that instant is not: at any instant the venue charges what
/// is due before it fills anything. A position opened and closed between two boundaries is never
/// charged, as a venue charges only what is held at its cutoff.
///
/// # Contract
/// - `next_boundary` must return an instant strictly after `after`, or `None` when the model never
///   charges the instrument. A boundary at or before `after` would charge one instant forever, so
///   the venue **panics** on one.
/// - Both methods must be pure functions of their arguments: the venue may ask for the same
///   boundary more than once, and charges it once.
///
/// `Send + Sync` because a venue holds its models behind an `Arc`, so that one model, and the
/// rate history it carries, can be shared by every run of a backtest sweep without being copied.
pub trait HoldingCostModel: Debug + Send + Sync {
    /// The first boundary strictly after `after` at which this model charges `instrument`, or
    /// `None` if it never does.
    fn next_boundary(
        &self,
        instrument: &InstrumentNameExchange,
        after: DateTime<Utc>,
    ) -> Option<DateTime<Utc>>;

    /// What `position`, held across `boundary`, is charged there, or `None` for nothing.
    fn charge(&self, position: &HeldPosition<'_>, boundary: DateTime<Utc>)
    -> Option<HoldingCharge>;
}

/// A position as a [`HoldingCostModel`] sees it at one boundary.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HeldPosition<'a> {
    /// The instrument held.
    pub instrument: &'a InstrumentNameExchange,
    /// Signed quantity in contracts: positive long, negative short. Never zero.
    pub quantity: Decimal,
    /// The multiplier from contracts to units of the underlying.
    pub contract_size: Decimal,
    /// The average price the position was entered at.
    pub entry_price: Decimal,
    /// The price the venue values the position at: the mid of its latest market, else the last
    /// price, else, on a venue with no market for the instrument, the entry price.
    pub price: Decimal,
}

impl<'a> HeldPosition<'a> {
    /// A position of signed `quantity` contracts in `instrument`, each of `contract_size` units of
    /// the underlying, entered at `entry_price` and valued at `price`.
    pub fn new(
        instrument: &'a InstrumentNameExchange,
        quantity: Decimal,
        contract_size: Decimal,
        entry_price: Decimal,
        price: Decimal,
    ) -> Self {
        Self {
            instrument,
            quantity,
            contract_size,
            entry_price,
            price,
        }
    }

    /// The position's value at [`price`](Self::price), unsigned:
    /// `|quantity| × contract_size × price`.
    pub fn notional(&self) -> Decimal {
        self.quantity.abs() * self.contract_size * self.price
    }

    /// Whether the position is short.
    pub fn is_short(&self) -> bool {
        self.quantity.is_sign_negative() && !self.quantity.is_zero()
    }
}

/// One charge a [`HoldingCostModel`] makes at one boundary.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HoldingCharge {
    /// What the charge is for, with the model's figures.
    pub kind: CashFlowKind,
    /// The amount in the instrument's quote asset, signed as a
    /// [`CashFlow`](crate::cash_flow::CashFlow) is: positive when the account receives it, negative
    /// when it pays. A zero amount is not charged.
    pub amount: Decimal,
}

impl HoldingCharge {
    /// A charge for `kind` of `amount`, signed positive when the account receives it.
    pub fn new(kind: CashFlowKind, amount: Decimal) -> Self {
        Self { kind, amount }
    }
}

/// A rate as a step function of time: each point's rate applies from its instant until the next
/// point's.
///
/// One type for a constant rate ([`constant`](Self::constant)) and a replayed history
/// ([`new`](Self::new)). Before its first point a series has no rate, and a model charges nothing
/// there rather than inventing one.
///
/// Serialised as its list of points, and deserialised through [`new`](Self::new), so a history
/// loaded from a file is refused out of order exactly as one built in code is.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(
    try_from = "Vec<(DateTime<Utc>, Decimal)>",
    into = "Vec<(DateTime<Utc>, Decimal)>"
)]
pub struct RateSeries {
    points: Vec<(DateTime<Utc>, Decimal)>,
}

impl TryFrom<Vec<(DateTime<Utc>, Decimal)>> for RateSeries {
    type Error = RateSeriesUnsorted;

    fn try_from(points: Vec<(DateTime<Utc>, Decimal)>) -> Result<Self, Self::Error> {
        Self::new(points)
    }
}

impl From<RateSeries> for Vec<(DateTime<Utc>, Decimal)> {
    fn from(series: RateSeries) -> Self {
        series.points
    }
}

impl RateSeries {
    /// A series from `points`, each a rate and the instant it takes effect.
    ///
    /// # Errors
    /// [`RateSeriesUnsorted`] if the instants are not strictly ascending. A replayed history out
    /// of order would apply each rate over the wrong interval, so it is refused rather than
    /// sorted: sorting would hide a fault in the data that produced it.
    pub fn new(points: Vec<(DateTime<Utc>, Decimal)>) -> Result<Self, RateSeriesUnsorted> {
        if let Some(index) = points.windows(2).position(|pair| pair[1].0 <= pair[0].0) {
            return Err(RateSeriesUnsorted {
                index: index + 1,
                previous: points[index].0,
                time: points[index + 1].0,
            });
        }

        Ok(Self { points })
    }

    /// A series with one rate at every instant.
    pub fn constant(rate: Decimal) -> Self {
        Self {
            points: vec![(DateTime::<Utc>::MIN_UTC, rate)],
        }
    }

    /// The rate in effect at `time`: that of the latest point at or before it, or `None` before
    /// the first point.
    pub fn at(&self, time: DateTime<Utc>) -> Option<Decimal> {
        let after = self.points.partition_point(|(start, _)| *start <= time);
        after.checked_sub(1).map(|index| self.points[index].1)
    }
}

/// A [`RateSeries`] was given instants that are not strictly ascending.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error(
    "rate series instants must be strictly ascending: point {index} at {time} follows {previous}"
)]
pub struct RateSeriesUnsorted {
    /// The index of the first point that does not follow its predecessor.
    pub index: usize,
    /// Its predecessor's instant.
    pub previous: DateTime<Utc>,
    /// Its own instant.
    pub time: DateTime<Utc>,
}

/// The year basis an annualised rate is divided by: the number of days a full year's rate is
/// charged over.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Deserialize, Serialize,
)]
pub enum DayCount {
    /// Actual days over 360: US equity borrow and most money-market rates.
    #[default]
    Act360,
    /// Actual days over 365: sterling and many CFD financing rates.
    Act365,
}

impl DayCount {
    /// What `days` days of `annual`, a whole year's amount, come to on this basis:
    /// `annual × days / basis`.
    ///
    /// Divided last, so an amount the basis divides exactly comes out exact rather than carrying
    /// the rounding of a non-terminating year fraction such as `1 / 360`.
    pub fn accrue(self, annual: Decimal, days: u32) -> Decimal {
        let basis = match self {
            Self::Act360 => Decimal::from(360),
            Self::Act365 => Decimal::from(365),
        };
        annual * Decimal::from(days) / basis
    }
}

/// Which dates a market settles on.
///
/// `Send + Sync` because an [`AccrualDays`] shares one across the models it is cloned into.
pub trait SettlementCalendar: Debug + Send + Sync {
    /// Whether the market settles on `date`.
    fn is_business_day(&self, date: NaiveDate) -> bool;
}

/// A calendar whose business days are Monday to Friday, with no holidays.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Deserialize, Serialize,
)]
pub struct WeekendsOnly;

impl SettlementCalendar for WeekendsOnly {
    fn is_business_day(&self, date: NaiveDate) -> bool {
        !matches!(date.weekday(), Weekday::Sat | Weekday::Sun)
    }
}

/// A calendar whose business days are Monday to Friday, less the holidays it is given.
///
/// The holidays are the caller's: this library ships no market's calendar, since a bundled one
/// goes stale with every year a venue publishes.
#[derive(Debug, Clone, PartialEq, Eq, Default, Deserialize, Serialize)]
pub struct HolidayCalendar {
    holidays: BTreeSet<NaiveDate>,
}

impl HolidayCalendar {
    /// A calendar closed on weekends and on each of `holidays`.
    pub fn new(holidays: impl IntoIterator<Item = NaiveDate>) -> Self {
        Self {
            holidays: holidays.into_iter().collect(),
        }
    }
}

impl SettlementCalendar for HolidayCalendar {
    fn is_business_day(&self, date: NaiveDate) -> bool {
        WeekendsOnly.is_business_day(date) && !self.holidays.contains(&date)
    }
}

/// Which dates a daily charge falls on, and how many days each one covers.
#[derive(Debug, Clone)]
pub enum AccrualDays {
    /// A cutoff on every calendar date, each charging one day. A weekend is charged on its own
    /// days.
    Calendar,
    /// A cutoff on each business day only, each charging the calendar days until the next one
    /// settles.
    ///
    /// A position held across the cutoff on date `d` is charged
    /// `settle(next(d)) − settle(d)` days, where `next(d)` is the next business day and
    /// `settle(x)` is the date `lag` business days after `x`. With `lag: 1` (US equities, T+1) a
    /// short held over Thursday's cutoff settles Friday, and the next day's trade settles
    /// Monday, so it pays 3 days; 4 across a Monday holiday. With `lag: 0` the 3 days fall on
    /// Friday instead.
    Settlement {
        /// The market's business days.
        calendar: Arc<dyn SettlementCalendar>,
        /// The settlement lag in business days.
        lag: u32,
    },
}

/// How far ahead a [`DailyAccrual`] looks for a cutoff date before deciding its calendar has none.
const CUTOFF_SEARCH_DAYS: u32 = 3660;

/// When a daily charge falls and what fraction of a year it covers.
///
/// A cutoff is a local time of day in `zone` (`Utc`, a `chrono::FixedOffset`, or a
/// `chrono_tz::Tz` such as `America/New_York`, which moves with daylight saving). A cutoff
/// that falls in a daylight-saving gap is taken at the first local time after the gap, and one
/// that falls twice is taken the first time.
#[derive(Debug, Clone)]
pub struct DailyAccrual<Tz: TimeZone = Utc> {
    cutoff: NaiveTime,
    zone: Tz,
    days: AccrualDays,
    day_count: DayCount,
}

impl<Tz: TimeZone> DailyAccrual<Tz> {
    /// Cutoffs at local time `cutoff` in `zone`, on the dates `days` names, each charging its days
    /// on the `day_count` basis.
    pub fn new(cutoff: NaiveTime, zone: Tz, days: AccrualDays, day_count: DayCount) -> Self {
        Self {
            cutoff,
            zone,
            days,
            day_count,
        }
    }

    /// The first cutoff strictly after `after`.
    ///
    /// # Panics
    /// If an [`AccrualDays::Settlement`] calendar has no business day within ten years of
    /// `after`: a calendar that never settles is a mis-specified fixture.
    pub fn next_boundary(&self, after: DateTime<Utc>) -> DateTime<Utc> {
        let mut date = after.with_timezone(&self.zone).date_naive();

        for _ in 0..CUTOFF_SEARCH_DAYS {
            if self.has_cutoff(date) {
                let boundary = self.cutoff_on(date);
                if boundary > after {
                    return boundary;
                }
            }
            date = next_date(date);
        }

        panic!(
            "DailyAccrual found no cutoff within {CUTOFF_SEARCH_DAYS} days of {after}: its \
             settlement calendar has no business day"
        )
    }

    /// What the charge at `boundary` comes to when a whole year's is `annual`: its
    /// [`days`](Self::days) on the [`DayCount`] basis.
    pub fn accrue(&self, annual: Decimal, boundary: DateTime<Utc>) -> Decimal {
        self.day_count.accrue(annual, self.days(boundary))
    }

    /// How many days the charge at `boundary`, one of this accrual's cutoffs, covers.
    pub fn days(&self, boundary: DateTime<Utc>) -> u32 {
        match &self.days {
            AccrualDays::Calendar => 1,
            AccrualDays::Settlement { calendar, lag } => {
                let date = boundary.with_timezone(&self.zone).date_naive();
                let settles = add_business_days(calendar.as_ref(), date, *lag);
                let next_settles = add_business_days(
                    calendar.as_ref(),
                    next_business_day(calendar.as_ref(), date),
                    *lag,
                );
                // Ascending by construction, and bounded by the search limit, so this fits.
                u32::try_from((next_settles - settles).num_days()).unwrap_or(u32::MAX)
            }
        }
    }

    fn has_cutoff(&self, date: NaiveDate) -> bool {
        match &self.days {
            AccrualDays::Calendar => true,
            AccrualDays::Settlement { calendar, .. } => calendar.is_business_day(date),
        }
    }

    /// The instant of the cutoff on local `date`.
    fn cutoff_on(&self, date: NaiveDate) -> DateTime<Utc> {
        let local = date.and_time(self.cutoff);

        // A daylight-saving gap is at most a few hours wide; step past it a minute at a time.
        (0..=24 * 60)
            .find_map(|minutes| {
                self.zone
                    .from_local_datetime(&(local + TimeDelta::minutes(minutes)))
                    .earliest()
            })
            .map(|instant| instant.with_timezone(&Utc))
            .unwrap_or_else(|| panic!("DailyAccrual cannot place a cutoff on {date} in its zone"))
    }
}

fn next_date(date: NaiveDate) -> NaiveDate {
    date.succ_opt()
        .unwrap_or_else(|| panic!("DailyAccrual ran past the last representable date"))
}

/// The first business day strictly after `date`.
fn next_business_day(calendar: &dyn SettlementCalendar, date: NaiveDate) -> NaiveDate {
    let mut next = next_date(date);
    for _ in 0..CUTOFF_SEARCH_DAYS {
        if calendar.is_business_day(next) {
            return next;
        }
        next = next_date(next);
    }
    panic!("SettlementCalendar has no business day within {CUTOFF_SEARCH_DAYS} days after {date}")
}

/// The date `days` business days after `date`, or `date` itself for zero.
fn add_business_days(calendar: &dyn SettlementCalendar, date: NaiveDate, days: u32) -> NaiveDate {
    (0..days).fold(date, |date, _| next_business_day(calendar, date))
}

/// A perpetual's funding: at every fixed interval, the position pays
/// `quantity × contract_size × price × rate`, signed so that a long pays a positive rate and a
/// short receives it.
///
/// Boundaries fall at whole multiples of `interval` since the Unix epoch, which puts an hourly
/// interval on the hour and an 8-hour one at 00:00, 08:00 and 16:00 UTC, as Hyperliquid and
/// Binance settle. The rate is per interval, as venues publish it, read from the instrument's
/// [`RateSeries`] at the boundary; an instrument with no series is not charged.
///
/// Charged as [`CashFlowKind::Funding`], with the rate and the signed position in contracts.
#[derive(Debug, Clone)]
pub struct FundingModel {
    interval: TimeDelta,
    rates: FnvHashMap<InstrumentNameExchange, RateSeries>,
}

impl FundingModel {
    /// A model charging funding every `interval`, with no instrument's rates yet.
    ///
    /// # Panics
    /// If `interval` is not positive.
    pub fn new(interval: TimeDelta) -> Self {
        assert!(
            interval > TimeDelta::zero(),
            "FundingModel interval must be positive, got {interval}"
        );
        Self {
            interval,
            rates: FnvHashMap::default(),
        }
    }

    /// Charges `instrument` at the funding rates in `rates`.
    #[must_use]
    pub fn with_rates(mut self, instrument: InstrumentNameExchange, rates: RateSeries) -> Self {
        self.rates.insert(instrument, rates);
        self
    }
}

impl HoldingCostModel for FundingModel {
    fn next_boundary(
        &self,
        instrument: &InstrumentNameExchange,
        after: DateTime<Utc>,
    ) -> Option<DateTime<Utc>> {
        if !self.rates.contains_key(instrument) {
            return None;
        }

        // Whole intervals since the epoch, so `after` on a boundary steps to the next one.
        let since_epoch = after - DateTime::UNIX_EPOCH;
        let interval_ns = self.interval.num_nanoseconds()?;
        let elapsed_ns = since_epoch.num_nanoseconds()?;
        let next = elapsed_ns.div_euclid(interval_ns).checked_add(1)?;
        DateTime::UNIX_EPOCH
            .checked_add_signed(TimeDelta::nanoseconds(next.checked_mul(interval_ns)?))
    }

    fn charge(
        &self,
        position: &HeldPosition<'_>,
        boundary: DateTime<Utc>,
    ) -> Option<HoldingCharge> {
        let rate = self.rates.get(position.instrument)?.at(boundary)?;
        let amount = -(position.quantity * position.contract_size * position.price * rate);

        Some(HoldingCharge::new(
            CashFlowKind::Funding {
                rate: Some(rate),
                position_quantity: Some(position.quantity),
            },
            amount,
        ))
    }
}

/// The fee for borrowing what a short sold: at each daily cutoff, a short pays
/// `notional × rate × days / basis`. A long is not charged.
///
/// The rate is annualised, as brokers quote borrow, read from the instrument's [`RateSeries`] at
/// the cutoff; an instrument with no series is not charged. The cutoff, its days and the basis
/// come from the [`DailyAccrual`].
///
/// Charged as [`CashFlowKind::BorrowFee`], with the rate and the quantity borrowed in units of the
/// underlying (`|quantity| × contract_size`).
///
/// Its boundaries do not depend on the position's side, so a long in an instrument it has rates
/// for is still woken for at each cutoff, and charged nothing there.
#[derive(Debug, Clone)]
pub struct BorrowFeeModel<Tz: TimeZone = Utc> {
    accrual: DailyAccrual<Tz>,
    rates: FnvHashMap<InstrumentNameExchange, RateSeries>,
}

impl<Tz: TimeZone> BorrowFeeModel<Tz> {
    /// A model charging at `accrual`'s cutoffs, with no instrument's rates yet.
    pub fn new(accrual: DailyAccrual<Tz>) -> Self {
        Self {
            accrual,
            rates: FnvHashMap::default(),
        }
    }

    /// Charges a short in `instrument` at the annualised borrow rates in `rates`.
    #[must_use]
    pub fn with_rates(mut self, instrument: InstrumentNameExchange, rates: RateSeries) -> Self {
        self.rates.insert(instrument, rates);
        self
    }
}

impl<Tz> HoldingCostModel for BorrowFeeModel<Tz>
where
    Tz: TimeZone + Debug + Send + Sync,
{
    fn next_boundary(
        &self,
        instrument: &InstrumentNameExchange,
        after: DateTime<Utc>,
    ) -> Option<DateTime<Utc>> {
        self.rates
            .contains_key(instrument)
            .then(|| self.accrual.next_boundary(after))
    }

    fn charge(
        &self,
        position: &HeldPosition<'_>,
        boundary: DateTime<Utc>,
    ) -> Option<HoldingCharge> {
        if !position.is_short() {
            return None;
        }

        let rate = self.rates.get(position.instrument)?.at(boundary)?;
        let amount = -self.accrual.accrue(position.notional() * rate, boundary);

        Some(HoldingCharge::new(
            CashFlowKind::BorrowFee {
                rate: Some(rate),
                quantity: Some(position.quantity.abs() * position.contract_size),
            },
            amount,
        ))
    }
}

/// The annualised rates a [`FinancingModel`] charges one instrument: one for a long, one for a
/// short.
///
/// Each is what the position **pays**; a negative rate is received. A CFD provider typically
/// charges a long a benchmark rate plus its markup, and a short the benchmark less its markup,
/// which a short receives while it is positive.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct FinancingRates {
    /// The annualised rate a long pays.
    pub long: RateSeries,
    /// The annualised rate a short pays; negative when it receives.
    pub short: RateSeries,
}

impl FinancingRates {
    /// The rates a long and a short pay.
    pub fn new(long: RateSeries, short: RateSeries) -> Self {
        Self { long, short }
    }
}

/// A CFD's overnight financing: at each daily cutoff, the position pays
/// `notional × rate × days / basis` at the rate for its side, and receives it when that rate is
/// negative.
///
/// Rates come from the instrument's [`FinancingRates`] at the cutoff; an instrument with none is
/// not charged. The cutoff, its days and the basis come from the [`DailyAccrual`].
///
/// Charged as [`CashFlowKind::Financing`], with the rate for the position's side and its notional.
#[derive(Debug, Clone)]
pub struct FinancingModel<Tz: TimeZone = Utc> {
    accrual: DailyAccrual<Tz>,
    rates: FnvHashMap<InstrumentNameExchange, FinancingRates>,
}

impl<Tz: TimeZone> FinancingModel<Tz> {
    /// A model charging at `accrual`'s cutoffs, with no instrument's rates yet.
    pub fn new(accrual: DailyAccrual<Tz>) -> Self {
        Self {
            accrual,
            rates: FnvHashMap::default(),
        }
    }

    /// Charges a position in `instrument` at `rates`.
    #[must_use]
    pub fn with_rates(mut self, instrument: InstrumentNameExchange, rates: FinancingRates) -> Self {
        self.rates.insert(instrument, rates);
        self
    }
}

impl<Tz> HoldingCostModel for FinancingModel<Tz>
where
    Tz: TimeZone + Debug + Send + Sync,
{
    fn next_boundary(
        &self,
        instrument: &InstrumentNameExchange,
        after: DateTime<Utc>,
    ) -> Option<DateTime<Utc>> {
        self.rates
            .contains_key(instrument)
            .then(|| self.accrual.next_boundary(after))
    }

    fn charge(
        &self,
        position: &HeldPosition<'_>,
        boundary: DateTime<Utc>,
    ) -> Option<HoldingCharge> {
        let rates = self.rates.get(position.instrument)?;
        let series = match position.is_short() {
            true => &rates.short,
            false => &rates.long,
        };
        let rate = series.at(boundary)?;
        let notional = position.notional();
        let amount = -self.accrual.accrue(notional * rate, boundary);

        Some(HoldingCharge::new(
            CashFlowKind::Financing {
                rate: Some(rate),
                notional: Some(notional),
            },
            amount,
        ))
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn utc(text: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(text).unwrap().to_utc()
    }

    fn date(text: &str) -> NaiveDate {
        NaiveDate::parse_from_str(text, "%Y-%m-%d").unwrap()
    }

    fn name() -> InstrumentNameExchange {
        InstrumentNameExchange::new("AAPL")
    }

    fn held(instrument: &InstrumentNameExchange, quantity: Decimal) -> HeldPosition<'_> {
        HeldPosition::new(instrument, quantity, Decimal::ONE, dec!(90), dec!(100))
    }

    fn cutoff(hour: u32) -> NaiveTime {
        NaiveTime::from_hms_opt(hour, 0, 0).unwrap()
    }

    fn settlement(lag: u32, holidays: &[&str]) -> AccrualDays {
        AccrualDays::Settlement {
            calendar: Arc::new(HolidayCalendar::new(holidays.iter().map(|d| date(d)))),
            lag,
        }
    }

    #[test]
    fn a_rate_series_steps_at_each_point_and_has_none_before_the_first() {
        let series = RateSeries::new(vec![
            (utc("2026-01-01T00:00:00Z"), dec!(0.01)),
            (utc("2026-01-03T00:00:00Z"), dec!(0.02)),
        ])
        .unwrap();

        assert_eq!(series.at(utc("2025-12-31T23:59:59Z")), None);
        assert_eq!(series.at(utc("2026-01-01T00:00:00Z")), Some(dec!(0.01)));
        assert_eq!(series.at(utc("2026-01-02T23:59:59Z")), Some(dec!(0.01)));
        assert_eq!(series.at(utc("2026-01-03T00:00:00Z")), Some(dec!(0.02)));
        assert_eq!(series.at(utc("2030-01-01T00:00:00Z")), Some(dec!(0.02)));
        assert_eq!(
            RateSeries::constant(dec!(0.05)).at(DateTime::<Utc>::MIN_UTC),
            Some(dec!(0.05))
        );
    }

    #[test]
    fn a_rate_series_out_of_order_or_repeating_an_instant_is_refused() {
        let first = utc("2026-01-02T00:00:00Z");
        let unsorted = RateSeries::new(vec![
            (first, dec!(0.01)),
            (utc("2026-01-01T00:00:00Z"), dec!(0.02)),
        ]);
        assert_eq!(
            unsorted,
            Err(RateSeriesUnsorted {
                index: 1,
                previous: first,
                time: utc("2026-01-01T00:00:00Z"),
            })
        );

        let repeated = RateSeries::new(vec![(first, dec!(0.01)), (first, dec!(0.02))]);
        assert!(repeated.is_err(), "two rates at one instant are ambiguous");
    }

    #[test]
    fn a_rate_series_loaded_out_of_order_is_refused_as_one_built_out_of_order_is() {
        let sorted = RateSeries::new(vec![
            (utc("2026-01-01T00:00:00Z"), dec!(0.01)),
            (utc("2026-01-02T00:00:00Z"), dec!(0.02)),
        ])
        .unwrap();
        let json = serde_json::to_string(&sorted).unwrap();
        assert_eq!(serde_json::from_str::<RateSeries>(&json).unwrap(), sorted);

        let unsorted = r#"[["2026-01-02T00:00:00Z","0.02"],["2026-01-01T00:00:00Z","0.01"]]"#;
        let error = serde_json::from_str::<RateSeries>(unsorted).unwrap_err();
        assert!(
            error.to_string().contains("strictly ascending"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn a_calendar_cutoff_falls_every_day_and_charges_one_day() {
        let accrual = DailyAccrual::new(cutoff(21), Utc, AccrualDays::Calendar, DayCount::Act360);

        // Friday 2026-10-09, then Saturday and Sunday each get their own cutoff.
        let friday = accrual.next_boundary(utc("2026-10-09T12:00:00Z"));
        assert_eq!(friday, utc("2026-10-09T21:00:00Z"));
        let saturday = accrual.next_boundary(friday);
        assert_eq!(saturday, utc("2026-10-10T21:00:00Z"));
        assert_eq!(accrual.next_boundary(saturday), utc("2026-10-11T21:00:00Z"));
        assert_eq!(accrual.days(friday), 1);
        assert_eq!(accrual.accrue(dec!(360), friday), Decimal::ONE);
    }

    #[test]
    fn a_settlement_cutoff_skips_non_business_days() {
        let accrual = DailyAccrual::new(cutoff(21), Utc, settlement(1, &[]), DayCount::Act360);

        let friday = utc("2026-10-09T21:00:00Z");
        assert_eq!(accrual.next_boundary(friday), utc("2026-10-12T21:00:00Z"));
    }

    /// The T+1 case: a short held over Thursday's cutoff settles Friday, and the next trade date
    /// settles Monday, so Thursday's charge covers three days and Friday's one.
    #[test]
    fn a_t_plus_1_short_held_over_thursday_pays_three_days() {
        let accrual = DailyAccrual::new(cutoff(21), Utc, settlement(1, &[]), DayCount::Act360);

        assert_eq!(accrual.days(utc("2026-10-07T21:00:00Z")), 1, "Wednesday");
        assert_eq!(accrual.days(utc("2026-10-08T21:00:00Z")), 3, "Thursday");
        assert_eq!(accrual.days(utc("2026-10-09T21:00:00Z")), 1, "Friday");
    }

    #[test]
    fn a_t_plus_1_short_held_over_thursday_before_a_monday_holiday_pays_four_days() {
        let accrual = DailyAccrual::new(
            cutoff(21),
            Utc,
            settlement(1, &["2026-10-12"]),
            DayCount::Act360,
        );

        assert_eq!(accrual.days(utc("2026-10-08T21:00:00Z")), 4, "Thursday");
        assert_eq!(accrual.days(utc("2026-10-09T21:00:00Z")), 1, "Friday");
    }

    #[test]
    fn with_no_settlement_lag_friday_pays_the_weekend() {
        let accrual = DailyAccrual::new(cutoff(21), Utc, settlement(0, &[]), DayCount::Act360);

        assert_eq!(accrual.days(utc("2026-10-08T21:00:00Z")), 1, "Thursday");
        assert_eq!(accrual.days(utc("2026-10-09T21:00:00Z")), 3, "Friday");
    }

    /// 17:00 in New York is 21:00 UTC in summer and 22:00 UTC in winter; the cutoff follows it.
    #[test]
    fn a_cutoff_in_a_zone_with_daylight_saving_follows_local_time() {
        let accrual = DailyAccrual::new(
            cutoff(17),
            chrono_tz::America::New_York,
            AccrualDays::Calendar,
            DayCount::Act360,
        );

        assert_eq!(
            accrual.next_boundary(utc("2026-10-09T12:00:00Z")),
            utc("2026-10-09T21:00:00Z")
        );
        assert_eq!(
            accrual.next_boundary(utc("2026-12-09T12:00:00Z")),
            utc("2026-12-09T22:00:00Z")
        );
    }

    /// 02:30 does not exist in New York on 2026-03-08; the cutoff falls when the clock resumes.
    #[test]
    fn a_cutoff_in_a_daylight_saving_gap_falls_after_the_gap() {
        let accrual = DailyAccrual::new(
            NaiveTime::from_hms_opt(2, 30, 0).unwrap(),
            chrono_tz::America::New_York,
            AccrualDays::Calendar,
            DayCount::Act360,
        );

        // 03:00 EDT, the first local time after the gap.
        assert_eq!(
            accrual.next_boundary(utc("2026-03-08T00:00:00Z")),
            utc("2026-03-08T07:00:00Z")
        );
    }

    /// 01:30 happens twice in New York on 2026-11-01; the cutoff is the first, in daylight time.
    #[test]
    fn a_cutoff_that_falls_twice_is_taken_the_first_time() {
        let accrual = DailyAccrual::new(
            NaiveTime::from_hms_opt(1, 30, 0).unwrap(),
            chrono_tz::America::New_York,
            AccrualDays::Calendar,
            DayCount::Act360,
        );

        // 01:30 EDT, not 01:30 EST an hour later.
        assert_eq!(
            accrual.next_boundary(utc("2026-11-01T00:00:00Z")),
            utc("2026-11-01T05:30:00Z")
        );
    }

    #[test]
    #[should_panic(expected = "no business day")]
    fn a_calendar_with_no_business_day_panics_rather_than_searching_forever() {
        #[derive(Debug)]
        struct Never;
        impl SettlementCalendar for Never {
            fn is_business_day(&self, _: NaiveDate) -> bool {
                false
            }
        }

        let days = AccrualDays::Settlement {
            calendar: Arc::new(Never),
            lag: 0,
        };
        let accrual = DailyAccrual::new(cutoff(21), Utc, days, DayCount::Act360);
        let _ = accrual.next_boundary(utc("2026-10-09T12:00:00Z"));
    }

    #[test]
    fn funding_falls_on_whole_intervals_since_the_epoch() {
        let instrument = name();
        let model = FundingModel::new(TimeDelta::hours(8))
            .with_rates(instrument.clone(), RateSeries::constant(dec!(0.0001)));

        assert_eq!(
            model.next_boundary(&instrument, utc("2026-10-09T05:00:00Z")),
            Some(utc("2026-10-09T08:00:00Z"))
        );
        assert_eq!(
            model.next_boundary(&instrument, utc("2026-10-09T08:00:00Z")),
            Some(utc("2026-10-09T16:00:00Z")),
            "a boundary steps strictly forward"
        );
        assert_eq!(
            model.next_boundary(
                &InstrumentNameExchange::new("MSFT"),
                utc("2026-10-09T05:00:00Z")
            ),
            None,
            "an instrument with no rates is never charged"
        );
    }

    #[test]
    fn funding_is_paid_by_a_long_and_received_by_a_short_at_a_positive_rate() {
        let instrument = name();
        let model = FundingModel::new(TimeDelta::hours(1))
            .with_rates(instrument.clone(), RateSeries::constant(dec!(0.0001)));
        let boundary = utc("2026-10-09T08:00:00Z");

        let long = model
            .charge(&held(&instrument, dec!(10)), boundary)
            .unwrap();
        assert_eq!(long.amount, dec!(-0.1), "10 × 100 × 0.0001 paid");
        assert_eq!(
            long.kind,
            CashFlowKind::Funding {
                rate: Some(dec!(0.0001)),
                position_quantity: Some(dec!(10)),
            }
        );

        let short = model
            .charge(&held(&instrument, dec!(-10)), boundary)
            .unwrap();
        assert_eq!(short.amount, dec!(0.1));
    }

    #[test]
    fn a_borrow_fee_charges_a_short_only() {
        let instrument = name();
        let accrual = DailyAccrual::new(cutoff(21), Utc, AccrualDays::Calendar, DayCount::Act360);
        let model = BorrowFeeModel::new(accrual)
            .with_rates(instrument.clone(), RateSeries::constant(dec!(0.36)));
        let boundary = utc("2026-10-09T21:00:00Z");

        assert_eq!(model.charge(&held(&instrument, dec!(10)), boundary), None);

        let short = model
            .charge(&held(&instrument, dec!(-10)), boundary)
            .unwrap();
        // 1000 notional × 36% / 360 for one day.
        assert_eq!(short.amount, dec!(-1));
        assert_eq!(
            short.kind,
            CashFlowKind::BorrowFee {
                rate: Some(dec!(0.36)),
                quantity: Some(dec!(10)),
            }
        );
    }

    #[test]
    fn a_borrow_fee_before_its_rates_begin_is_not_charged() {
        let instrument = name();
        let accrual = DailyAccrual::new(cutoff(21), Utc, AccrualDays::Calendar, DayCount::Act360);
        let rates = RateSeries::new(vec![(utc("2026-10-10T00:00:00Z"), dec!(0.36))]).unwrap();
        let model = BorrowFeeModel::new(accrual).with_rates(instrument.clone(), rates);

        let before = utc("2026-10-09T21:00:00Z");
        assert_eq!(model.charge(&held(&instrument, dec!(-10)), before), None);
    }

    #[test]
    fn financing_charges_each_side_at_its_own_rate_and_pays_a_negative_one() {
        let instrument = name();
        let accrual = DailyAccrual::new(cutoff(22), Utc, AccrualDays::Calendar, DayCount::Act365);
        let rates = FinancingRates::new(
            RateSeries::constant(dec!(0.073)),
            RateSeries::constant(dec!(-0.0365)),
        );
        let model = FinancingModel::new(accrual).with_rates(instrument.clone(), rates);
        let boundary = utc("2026-10-09T22:00:00Z");

        let long = model
            .charge(&held(&instrument, dec!(10)), boundary)
            .unwrap();
        // 1000 × 7.3% / 365.
        assert_eq!(long.amount, dec!(-0.2));
        assert_eq!(
            long.kind,
            CashFlowKind::Financing {
                rate: Some(dec!(0.073)),
                notional: Some(dec!(1000)),
            }
        );

        let short = model
            .charge(&held(&instrument, dec!(-10)), boundary)
            .unwrap();
        assert_eq!(short.amount, dec!(0.1), "a negative short rate is received");
    }
}
