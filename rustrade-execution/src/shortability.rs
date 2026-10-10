//! Whether an instrument can be sold short, and on what terms: what a venue says about lending it.
//!
//! [`Shortability`] is the one description of an instrument's borrow terms, whether it comes from
//! a live venue or from a backtest's own data. A simulated venue reads it from a
//! [`ShortabilityProvider`], which also says when a short-sale restriction is in effect, and
//! refuses a short the provider does not allow — see
//! [`SimulatedVenue::with_shortability`](crate::exchange::mock::SimulatedVenue::with_shortability).
//! [`ShortabilityTable`] is a provider built from per-instrument histories.

use chrono::{DateTime, Utc};
use fnv::FnvHashMap;
use rust_decimal::Decimal;
use rustrade_instrument::instrument::name::InstrumentNameExchange;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, fmt::Debug};

/// What a venue says about lending an instrument for a short sale.
///
/// Every field is optional, as no venue reports all of them: `None` means not known, never
/// "no". A consumer treats an unknown value as it sees fit; the simulated venue treats it as
/// allowing the short.
///
/// # Advisory
/// The values describe the venue's lending as it reported it, not a promise to lend. A venue can
/// still refuse a short its flags allow — Alpaca has refused "cannot be sold short" for an asset
/// it flagged both shortable and easy to borrow — so a short sale must still handle
/// [`ApiError::BorrowRejected`](crate::error::ApiError::BorrowRejected).
#[non_exhaustive]
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Shortability {
    /// Whether the venue allows the instrument to be sold short at all.
    pub shortable: Option<bool>,
    /// Whether the venue lists the instrument as easy to borrow, rather than hard to borrow.
    pub easy_to_borrow: Option<bool>,
    /// How much the venue has to lend, in the units an order on the instrument is sized in: its
    /// total, not what is left after this account's own shorts.
    pub available: Option<Decimal>,
    /// The annualised fee for borrowing the instrument, as a fraction: `0.05` is 5% a year.
    ///
    /// Simple, not compounded, and on the venue's own year basis, which differs between venues:
    /// Alpaca divides its borrow rate over a 360-day year, and Binance's hourly rate is annualised
    /// over 365 days. A simulated venue's holding costs apply their own
    /// [`DayCount`](crate::holding_cost::DayCount) to whatever rate they are given.
    pub fee_rate: Option<Decimal>,
}

impl Shortability {
    /// Nothing known.
    pub fn new() -> Self {
        Self::default()
    }

    /// With whether the instrument may be sold short at all.
    #[must_use]
    pub fn with_shortable(mut self, shortable: bool) -> Self {
        self.shortable = Some(shortable);
        self
    }

    /// With whether the instrument is easy to borrow.
    #[must_use]
    pub fn with_easy_to_borrow(mut self, easy_to_borrow: bool) -> Self {
        self.easy_to_borrow = Some(easy_to_borrow);
        self
    }

    /// With how much the venue has to lend in total.
    #[must_use]
    pub fn with_available(mut self, available: Decimal) -> Self {
        self.available = Some(available);
        self
    }

    /// With the annualised borrow fee, as a fraction.
    #[must_use]
    pub fn with_fee_rate(mut self, fee_rate: Decimal) -> Self {
        self.fee_rate = Some(fee_rate);
        self
    }
}

/// Says what a simulated venue may lend for a short sale, at any instant of a run.
///
/// The venue asks when an order arrives that would open or increase a short. Both methods must be
/// pure functions of their arguments, so a replay asks the same question and gets the same answer.
///
/// `Send + Sync` because a venue holds its provider behind an `Arc`, so that one provider, and the
/// history it carries, can be shared by every run of a backtest sweep without being copied.
pub trait ShortabilityProvider: Debug + Send + Sync {
    /// What the venue says about lending `instrument` at `time`.
    fn shortability(
        &self,
        instrument: &InstrumentNameExchange,
        time: DateTime<Utc>,
    ) -> Shortability;

    /// Whether a short-sale restriction is in effect on `instrument` at `time`, such as the SEC's
    /// Rule 201 circuit breaker after a 10% fall, under which a short sale may not be priced at or
    /// below the best bid.
    ///
    /// Separate from [`shortability`](Self::shortability) because it is a rule of the market, not
    /// a term of the loan. `false` unless implemented.
    fn short_sale_restricted(
        &self,
        _instrument: &InstrumentNameExchange,
        _time: DateTime<Utc>,
    ) -> bool {
        false
    }
}

/// A [`ShortabilityProvider`] from per-instrument histories: each instrument's [`Shortability`]
/// as a step function of time, and the windows in which a short-sale restriction is in effect.
///
/// Missing data allows the short: an instrument with no entry, or a time before its first one, is
/// [`Shortability::new`], with nothing known, and an instrument is unrestricted outside its
/// windows.
#[derive(Debug, Clone, Default)]
pub struct ShortabilityTable {
    shortability: FnvHashMap<InstrumentNameExchange, BTreeMap<DateTime<Utc>, Shortability>>,
    restrictions: FnvHashMap<InstrumentNameExchange, Vec<(DateTime<Utc>, DateTime<Utc>)>>,
}

impl ShortabilityTable {
    /// An empty table, which allows every short.
    pub fn new() -> Self {
        Self::default()
    }

    /// With `instrument` described by `shortability` from `from` until its next entry. An entry
    /// already at `from` is replaced. Entries may be added in any order.
    #[must_use]
    pub fn with_shortability(
        mut self,
        instrument: InstrumentNameExchange,
        from: DateTime<Utc>,
        shortability: Shortability,
    ) -> Self {
        self.shortability
            .entry(instrument)
            .or_default()
            .insert(from, shortability);
        self
    }

    /// With a short-sale restriction on `instrument` from `from`, inclusive, until `until`,
    /// exclusive. Windows may overlap.
    ///
    /// # Panics
    /// If `until` is not after `from`. Such a window would never be in effect, so it is refused
    /// rather than ignored: it means the data it came from has its instants reversed.
    #[must_use]
    pub fn with_short_sale_restriction(
        mut self,
        instrument: InstrumentNameExchange,
        from: DateTime<Utc>,
        until: DateTime<Utc>,
    ) -> Self {
        assert!(
            from < until,
            "a short-sale restriction on {instrument} must end after it starts: {from} to {until}"
        );
        self.restrictions
            .entry(instrument)
            .or_default()
            .push((from, until));
        self
    }
}

impl ShortabilityProvider for ShortabilityTable {
    fn shortability(
        &self,
        instrument: &InstrumentNameExchange,
        time: DateTime<Utc>,
    ) -> Shortability {
        self.shortability
            .get(instrument)
            .and_then(|series| series.range(..=time).next_back())
            .map(|(_, shortability)| shortability.clone())
            .unwrap_or_default()
    }

    fn short_sale_restricted(
        &self,
        instrument: &InstrumentNameExchange,
        time: DateTime<Utc>,
    ) -> bool {
        self.restrictions.get(instrument).is_some_and(|windows| {
            windows
                .iter()
                .any(|(from, until)| *from <= time && time < *until)
        })
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

    fn name() -> InstrumentNameExchange {
        InstrumentNameExchange::new("AAPL")
    }

    #[test]
    fn shortability_starts_unknown_and_each_setter_fills_one_field() {
        assert_eq!(
            Shortability::new(),
            Shortability {
                shortable: None,
                easy_to_borrow: None,
                available: None,
                fee_rate: None,
            }
        );

        let full = Shortability::new()
            .with_shortable(true)
            .with_easy_to_borrow(false)
            .with_available(dec!(1500))
            .with_fee_rate(dec!(0.12));
        assert_eq!(full.shortable, Some(true));
        assert_eq!(full.easy_to_borrow, Some(false));
        assert_eq!(full.available, Some(dec!(1500)));
        assert_eq!(full.fee_rate, Some(dec!(0.12)));
    }

    #[test]
    fn shortability_round_trips_through_serde_and_reads_missing_fields_as_unknown() {
        let full = Shortability::new()
            .with_shortable(true)
            .with_available(dec!(10));
        let json = serde_json::to_string(&full).unwrap();
        assert_eq!(serde_json::from_str::<Shortability>(&json).unwrap(), full);

        let sparse: Shortability = serde_json::from_str(r#"{"shortable":false}"#).unwrap();
        assert_eq!(sparse, Shortability::new().with_shortable(false));
    }

    #[test]
    fn a_table_steps_at_each_entry_and_knows_nothing_before_the_first() {
        let hard = Shortability::new()
            .with_shortable(true)
            .with_easy_to_borrow(false);
        let closed = Shortability::new().with_shortable(false);
        // Added out of order: the table orders its entries by instant.
        let table = ShortabilityTable::new()
            .with_shortability(name(), utc("2026-01-03T00:00:00Z"), closed.clone())
            .with_shortability(name(), utc("2026-01-01T00:00:00Z"), hard.clone());

        let at = |text| table.shortability(&name(), utc(text));
        assert_eq!(at("2025-12-31T23:59:59Z"), Shortability::new());
        assert_eq!(at("2026-01-01T00:00:00Z"), hard);
        assert_eq!(at("2026-01-02T23:59:59Z"), hard);
        assert_eq!(at("2026-01-03T00:00:00Z"), closed);
        assert_eq!(
            table.shortability(
                &InstrumentNameExchange::new("MSFT"),
                utc("2026-01-03T00:00:00Z")
            ),
            Shortability::new()
        );
    }

    #[test]
    fn a_later_entry_at_the_same_instant_replaces_the_earlier() {
        let instant = utc("2026-01-01T00:00:00Z");
        let table = ShortabilityTable::new()
            .with_shortability(name(), instant, Shortability::new().with_shortable(true))
            .with_shortability(name(), instant, Shortability::new().with_shortable(false));

        assert_eq!(table.shortability(&name(), instant).shortable, Some(false));
    }

    #[test]
    fn a_restriction_is_in_effect_from_its_start_until_just_before_its_end() {
        let table = ShortabilityTable::new()
            .with_short_sale_restriction(
                name(),
                utc("2026-01-01T15:00:00Z"),
                utc("2026-01-02T21:00:00Z"),
            )
            .with_short_sale_restriction(
                name(),
                utc("2026-01-05T15:00:00Z"),
                utc("2026-01-06T21:00:00Z"),
            );

        let restricted = |text| table.short_sale_restricted(&name(), utc(text));
        assert!(!restricted("2026-01-01T14:59:59Z"));
        assert!(restricted("2026-01-01T15:00:00Z"));
        assert!(restricted("2026-01-02T20:59:59Z"));
        assert!(!restricted("2026-01-02T21:00:00Z"));
        assert!(restricted("2026-01-06T00:00:00Z"));
        assert!(!table.short_sale_restricted(
            &InstrumentNameExchange::new("MSFT"),
            utc("2026-01-01T16:00:00Z")
        ));
    }

    #[test]
    #[should_panic(expected = "must end after it starts")]
    fn a_restriction_that_ends_before_it_starts_panics() {
        let _ = ShortabilityTable::new().with_short_sale_restriction(
            name(),
            utc("2026-01-02T00:00:00Z"),
            utc("2026-01-01T00:00:00Z"),
        );
    }

    #[test]
    fn a_provider_is_unrestricted_unless_it_says_otherwise() {
        #[derive(Debug)]
        struct Constant;
        impl ShortabilityProvider for Constant {
            fn shortability(&self, _: &InstrumentNameExchange, _: DateTime<Utc>) -> Shortability {
                Shortability::new()
            }
        }

        assert!(!Constant.short_sale_restricted(&name(), utc("2026-01-01T00:00:00Z")));
    }
}
