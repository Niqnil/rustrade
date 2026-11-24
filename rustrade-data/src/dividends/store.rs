//! [`DividendStore`]: cash dividends as they were known at a given time.

use super::Dividend;
use chrono::{DateTime, NaiveDate, Utc};
use rustrade_instrument::calendar::Tz;
use smol_str::SmolStr;
use std::{
    cmp::Ordering,
    collections::{BTreeMap, HashMap},
    ops::RangeBounds,
};

/// Cash dividends as they were known at any given time: the next and the previous dividend of a
/// symbol as of an instant, and every one known by then, from every record put in, whatever its
/// provider.
///
/// It keeps every record and answers from those already public at the instant asked about, so a
/// backtest asks it what a live system would have known at its clock, and a live system asks it
/// about now.
///
/// The store is plain data, [`Send`] and [`Sync`]: a caller sharing one between threads adds its
/// own lock.
///
/// # Contract
/// - **Visibility.** A query as of `as_of` uses only records whose
///   [`known_at`](Dividend::known_at) is at or before `as_of`.
/// - **Supersession.** Of a dividend's visible records (those with the same symbol and
///   ex-date), one stands for it: the one with the later [`known_at`](Dividend::known_at), then
///   the later [`observed_at`](super::DividendProvenance::observed_at), then the one inserted
///   later. Providers rank alike: a caller that prefers one provider puts in only its records.
/// - **Past or coming.** As of `as_of`, a dividend has gone ex from the start of its ex-date in
///   the store's [zone](Self::zone): on its ex-date it is [`previous`](Self::previous), since
///   the stock already trades without it. Before that it is [`next`](Self::next).
///
/// The store does not project dividends not yet declared, nor restate amounts for later splits:
/// both are the caller's (see the [module documentation](super#amounts)).
///
/// # Time zone
/// Every [`ex_date`](Dividend::ex_date) is in the local time of the issuer's listing exchange,
/// and the store compares them with `as_of`'s date in the one zone it is built with. Keep issuers
/// listed in different zones in separate stores.
///
/// # Known limitations
/// - **A withdrawn dividend stays.** A provider that drops a dividend from its answers leaves no
///   record of the drop, so the store keeps answering with the last record it was given.
/// - **One dividend per symbol and ex-date.** Two dividends going ex on the same day, such as a
///   regular and a special one, are one dividend here, and the later record stands for both. A
///   caller whose provider lists them apart sums them first.
/// - **A moved ex-date is a second dividend.** A record whose ex-date changed does not supersede
///   the record of the old date.
/// - Every record put in is kept, so memory grows with each fetch. Only a record equal in every
///   field to one already held is dropped; the same dividend fetched twice differs in
///   [`observed_at`](super::DividendProvenance::observed_at) and is kept twice.
///
/// ```
/// use chrono::{NaiveDate, TimeZone, Utc};
/// use rust_decimal_macros::dec;
/// use rustrade_data::dividends::{Dividend, DividendProvenance, DividendProvider, DividendStore};
/// use rustrade_instrument::calendar::Tz;
///
/// let day = |y, m, d| NaiveDate::from_ymd_opt(y, m, d).unwrap();
/// let fetched = Utc.with_ymd_and_hms(2026, 11, 24, 12, 0, 0).unwrap();
/// // Declared 17 November, so known from 00:00 New York on 18 November.
/// let declared = Utc.with_ymd_and_hms(2026, 11, 18, 5, 0, 0).unwrap();
/// let dividend = Dividend::new(
///     "XYZ",
///     day(2026, 12, 2),
///     dec!(0.25),
///     declared,
///     DividendProvenance::new(DividendProvider::AlphaVantage, fetched),
/// )
/// .with_declaration_date(day(2026, 11, 17));
///
/// let mut store = DividendStore::new("America/New_York".parse::<Tz>().unwrap());
/// store.insert(dividend.clone());
///
/// // Before its declaration, nothing is known; after, it is the next dividend.
/// assert_eq!(store.next("XYZ", declared - chrono::TimeDelta::seconds(1)), None);
/// assert_eq!(store.next("XYZ", declared), Some(&dividend));
/// // From 00:00 New York on its ex-date it is the previous one.
/// let ex = Utc.with_ymd_and_hms(2026, 12, 2, 5, 0, 0).unwrap();
/// assert_eq!(store.next("XYZ", ex), None);
/// assert_eq!(store.previous("XYZ", ex), Some(&dividend));
/// ```
#[derive(Debug, Clone)]
pub struct DividendStore {
    zone: Tz,
    /// Each symbol's dividends by ex-date, each dividend's records in supersession order, the
    /// strongest last.
    dividends: HashMap<SmolStr, BTreeMap<NaiveDate, Vec<Record>>>,
    records: usize,
}

/// A record as held, with the order it was inserted in for the last tie-break.
#[derive(Debug, Clone)]
struct Record {
    dividend: Dividend,
    sequence: usize,
}

impl Record {
    /// Supersession order: see the [`DividendStore`] contract.
    fn rank(&self, other: &Self) -> Ordering {
        let (a, b) = (&self.dividend, &other.dividend);
        a.known_at
            .cmp(&b.known_at)
            .then(a.source.observed_at.cmp(&b.source.observed_at))
            .then(self.sequence.cmp(&other.sequence))
    }
}

impl DividendStore {
    /// An empty store comparing ex-dates in `zone`, the time zone of the listing exchange.
    pub fn new(zone: Tz) -> Self {
        Self {
            zone,
            dividends: HashMap::new(),
            records: 0,
        }
    }

    /// The time zone the store reads ex-dates in.
    pub fn zone(&self) -> Tz {
        self.zone
    }

    /// Adds `dividend`, in any order relative to the records already held. Returns `false`, and
    /// keeps nothing, if a record equal in every field is already held.
    pub fn insert(&mut self, dividend: Dividend) -> bool {
        let records = self
            .dividends
            .entry(dividend.symbol.clone())
            .or_default()
            .entry(dividend.ex_date)
            .or_default();
        if records.iter().any(|held| held.dividend == dividend) {
            return false;
        }
        let record = Record {
            dividend,
            sequence: self.records,
        };
        // Later insertion ranks higher on a full tie, so the new record goes after any equal one.
        let at = records.partition_point(|held| held.rank(&record) != Ordering::Greater);
        records.insert(at, record);
        self.records += 1;
        true
    }

    /// The record that stands for `symbol`'s dividend going ex on `ex_date` as of `as_of`, by the
    /// [supersession](Self#contract) order, or `None` if none of its records was public by then.
    pub fn current(
        &self,
        symbol: &str,
        ex_date: NaiveDate,
        as_of: DateTime<Utc>,
    ) -> Option<&Dividend> {
        self.dividends
            .get(symbol)?
            .get(&ex_date)
            .and_then(|records| standing_record(records, as_of))
    }

    /// The dividend of `symbol` known as of `as_of` with the earliest ex-date after `as_of`'s
    /// date, or `None` if none is known by then. See the [contract](Self#contract).
    pub fn next(&self, symbol: &str, as_of: DateTime<Utc>) -> Option<&Dividend> {
        let today = self.today(as_of);
        self.known(symbol, as_of, today.succ_opt()?..).next()
    }

    /// The dividend of `symbol` known as of `as_of` with the latest ex-date on or before
    /// `as_of`'s date, or `None` if none is known by then. See the [contract](Self#contract).
    pub fn previous(&self, symbol: &str, as_of: DateTime<Utc>) -> Option<&Dividend> {
        self.known(symbol, as_of, ..=self.today(as_of)).next_back()
    }

    /// Every dividend of `symbol` known as of `as_of` whose ex-date is in `ex_dates`, by ex-date:
    /// the standing record of each. An option pricer valuing at `as_of` asks for the ex-dates
    /// after today up to the expiry. See the [contract](Self#contract).
    pub fn known<R: RangeBounds<NaiveDate>>(
        &self,
        symbol: &str,
        as_of: DateTime<Utc>,
        ex_dates: R,
    ) -> impl DoubleEndedIterator<Item = &Dividend> {
        self.dividends
            .get(symbol)
            .map(|by_date| by_date.range(ex_dates))
            .into_iter()
            .flatten()
            .filter_map(move |(_, records)| standing_record(records, as_of))
    }

    /// Every record held of `symbol`'s dividend going ex on `ex_date`, public or not, in
    /// supersession order with the strongest last: the last is the one standing once all of them
    /// are public.
    pub fn observations(
        &self,
        symbol: &str,
        ex_date: NaiveDate,
    ) -> impl Iterator<Item = &Dividend> {
        self.dividends
            .get(symbol)
            .and_then(|by_date| by_date.get(&ex_date))
            .into_iter()
            .flatten()
            .map(|record| &record.dividend)
    }

    /// The number of records held.
    pub fn len(&self) -> usize {
        self.records
    }

    /// Whether the store holds no records.
    pub fn is_empty(&self) -> bool {
        self.records == 0
    }

    /// `as_of`'s date in the store's zone.
    fn today(&self, as_of: DateTime<Utc>) -> NaiveDate {
        as_of.with_timezone(&self.zone).date_naive()
    }
}

impl Extend<Dividend> for DividendStore {
    fn extend<I: IntoIterator<Item = Dividend>>(&mut self, dividends: I) {
        for dividend in dividends {
            self.insert(dividend);
        }
    }
}

/// The strongest of `records` public by `as_of`: they are held in supersession order.
fn standing_record(records: &[Record], as_of: DateTime<Utc>) -> Option<&Dividend> {
    records
        .iter()
        .rev()
        .map(|record| &record.dividend)
        .find(|dividend| dividend.known_at <= as_of)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use crate::dividends::{DividendProvenance, DividendProvider};
    use chrono::{TimeDelta, TimeZone};
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    fn day(y: i32, m: u32, d: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(y, m, d).unwrap()
    }

    fn utc(y: i32, m: u32, d: u32, h: u32, min: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(y, m, d, h, min, 0).unwrap()
    }

    fn new_york() -> DividendStore {
        DividendStore::new("America/New_York".parse().unwrap())
    }

    /// XYZ's dividend of `amount` going ex on `ex_date`, known and fetched at `known_at`.
    fn record(ex_date: NaiveDate, amount: Decimal, known_at: DateTime<Utc>) -> Dividend {
        Dividend::new(
            "XYZ",
            ex_date,
            amount,
            known_at,
            DividendProvenance::new(DividendProvider::AlphaVantage, known_at),
        )
    }

    #[test]
    fn a_record_is_visible_from_its_known_at_inclusive() {
        let known_at = utc(2026, 1, 5, 12, 0);
        let row = record(day(2026, 2, 3), dec!(0.25), known_at);
        let mut store = new_york();
        assert!(store.is_empty());
        assert!(store.insert(row.clone()));
        assert_eq!((store.len(), store.is_empty()), (1, false));

        let before = known_at - TimeDelta::nanoseconds(1);
        assert_eq!(store.current("XYZ", row.ex_date, before), None);
        assert_eq!(store.next("XYZ", before), None);
        assert_eq!(store.known("XYZ", before, ..).count(), 0);
        assert_eq!(store.current("XYZ", row.ex_date, known_at), Some(&row));
        assert_eq!(store.next("XYZ", known_at), Some(&row));
        assert_eq!(store.previous("XYZ", known_at), None);
        assert_eq!(store.next("ABC", known_at), None);
        assert_eq!(store.previous("ABC", known_at), None);
        assert_eq!(store.current("ABC", row.ex_date, known_at), None);
        assert_eq!(store.current("XYZ", day(2026, 2, 4), known_at), None);
    }

    #[test]
    fn a_dividend_goes_ex_at_the_start_of_its_ex_date_in_the_stores_zone() {
        let row = record(day(2026, 2, 3), dec!(0.25), utc(2026, 1, 5, 0, 0));
        let mut store = new_york();
        store.insert(row.clone());

        // 2026-02-02 23:59 New York is already 2026-02-03 in UTC.
        let last_minute = utc(2026, 2, 3, 4, 59);
        assert_eq!(store.next("XYZ", last_minute), Some(&row));
        assert_eq!(store.previous("XYZ", last_minute), None);
        let midnight = utc(2026, 2, 3, 5, 0);
        assert_eq!(store.next("XYZ", midnight), None);
        assert_eq!(store.previous("XYZ", midnight), Some(&row));
    }

    #[test]
    fn a_later_record_supersedes_from_its_known_at() {
        let ex = day(2026, 2, 3);
        let first = record(ex, dec!(0.25), utc(2026, 1, 5, 12, 0));
        let corrected = record(ex, dec!(0.26), utc(2026, 1, 9, 12, 0));
        let mut store = new_york();
        // Inserted newest first: the store sorts.
        store.extend([corrected.clone(), first.clone()]);

        assert_eq!(store.next("XYZ", utc(2026, 1, 9, 11, 59)), Some(&first));
        assert_eq!(store.next("XYZ", utc(2026, 1, 9, 12, 0)), Some(&corrected));
        assert_eq!(
            store.previous("XYZ", utc(2026, 2, 4, 0, 0)),
            Some(&corrected)
        );
        assert_eq!(
            store.observations("XYZ", ex).collect::<Vec<_>>(),
            [&first, &corrected]
        );
        assert_eq!(store.observations("XYZ", day(2026, 2, 4)).count(), 0);
        assert_eq!(store.observations("ABC", ex).count(), 0);
    }

    #[test]
    fn a_later_known_at_outranks_a_later_fetch() {
        let ex = day(2026, 2, 3);
        let mut known_early_fetched_late = record(ex, dec!(0.25), utc(2026, 1, 5, 0, 0));
        known_early_fetched_late.source.observed_at = utc(2026, 1, 20, 0, 0);
        let known_late = record(ex, dec!(0.26), utc(2026, 1, 6, 0, 0));
        let mut store = new_york();
        store.extend([known_late.clone(), known_early_fetched_late.clone()]);
        assert_eq!(
            store.current("XYZ", ex, utc(2026, 1, 21, 0, 0)),
            Some(&known_late)
        );
    }

    #[test]
    fn ties_on_known_at_fall_to_observed_at_then_insertion() {
        let ex = day(2026, 2, 3);
        let known_at = utc(2026, 1, 5, 12, 0);
        let base = record(ex, dec!(0.25), known_at);
        let mut observed_later = record(ex, dec!(0.27), known_at);
        observed_later.source.observed_at = known_at + TimeDelta::seconds(1);

        let mut store = new_york();
        store.extend([observed_later.clone(), base.clone()]);
        assert_eq!(
            store.observations("XYZ", ex).collect::<Vec<_>>(),
            [&base, &observed_later]
        );
        assert_eq!(store.current("XYZ", ex, known_at), Some(&observed_later));

        // A full tie: the record inserted later stands, whichever it is.
        let other_amount = record(ex, dec!(0.30), known_at);
        for (first, second) in [(&base, &other_amount), (&other_amount, &base)] {
            let mut store = new_york();
            store.extend([first.clone(), second.clone()]);
            assert_eq!(store.current("XYZ", ex, known_at), Some(second));
        }
    }

    #[test]
    fn next_is_the_earliest_coming_previous_the_latest_past_and_known_the_range() {
        let known_at = utc(2025, 10, 1, 0, 0);
        let august = record(day(2025, 8, 8), dec!(0.24), known_at);
        let november = record(day(2025, 11, 7), dec!(0.25), known_at);
        let february = record(day(2026, 2, 6), dec!(0.25), known_at);
        let mut store = new_york();
        store.extend([february.clone(), august.clone(), november.clone()]);

        assert_eq!(store.next("XYZ", known_at), Some(&november));
        assert_eq!(store.previous("XYZ", known_at), Some(&august));
        assert_eq!(
            store.known("XYZ", known_at, ..).collect::<Vec<_>>(),
            [&august, &november, &february]
        );
        // The pricer's question: ex-dates after today up to an expiry in January.
        let expiry = day(2026, 1, 16);
        assert_eq!(
            store
                .known("XYZ", known_at, day(2025, 10, 1)..=expiry)
                .collect::<Vec<_>>(),
            [&november]
        );
        assert_eq!(
            store
                .known("XYZ", known_at, ..day(2025, 11, 7))
                .collect::<Vec<_>>(),
            [&august]
        );

        let after_november = utc(2025, 12, 1, 0, 0);
        assert_eq!(store.next("XYZ", after_november), Some(&february));
        assert_eq!(store.previous("XYZ", after_november), Some(&november));
    }

    #[test]
    fn known_skips_dividends_not_yet_public() {
        let early = record(day(2026, 2, 6), dec!(0.25), utc(2026, 1, 5, 0, 0));
        let late = record(day(2026, 5, 8), dec!(0.26), utc(2026, 4, 20, 0, 0));
        let mut store = new_york();
        store.extend([late.clone(), early.clone()]);
        let as_of = utc(2026, 1, 10, 0, 0);
        assert_eq!(store.known("XYZ", as_of, ..).collect::<Vec<_>>(), [&early]);
        assert_eq!(store.next("XYZ", as_of), Some(&early));
        assert_eq!(
            store
                .known("XYZ", utc(2026, 4, 20, 0, 0), ..)
                .collect::<Vec<_>>(),
            [&early, &late]
        );
    }

    #[test]
    fn a_dividend_known_only_from_its_ex_date_is_never_next() {
        // No declaration date: known from 00:00 New York on the ex-date, when it has gone ex.
        let ex = day(2026, 2, 3);
        let row = record(ex, dec!(0.25), utc(2026, 2, 3, 5, 0));
        let mut store = new_york();
        store.insert(row.clone());
        assert_eq!(store.next("XYZ", utc(2026, 2, 3, 4, 59)), None);
        assert_eq!(store.next("XYZ", utc(2026, 2, 3, 5, 0)), None);
        assert_eq!(store.previous("XYZ", utc(2026, 2, 3, 5, 0)), Some(&row));
    }

    #[test]
    fn an_identical_record_is_kept_once_and_a_refetch_twice() {
        let known_at = utc(2026, 1, 5, 0, 0);
        let row = record(day(2026, 2, 3), dec!(0.25), known_at);
        let mut refetch = row.clone();
        refetch.source.observed_at = known_at + TimeDelta::days(1);
        let mut store = new_york();
        assert!(store.insert(row.clone()));
        assert!(!store.insert(row.clone()));
        assert_eq!(store.len(), 1);
        assert!(store.insert(refetch.clone()));
        assert_eq!(store.len(), 2);
        assert_eq!(store.observations("XYZ", row.ex_date).count(), 2);
        assert_eq!(store.known("XYZ", known_at, ..).count(), 1);
        assert_eq!(store.current("XYZ", row.ex_date, known_at), Some(&refetch));
    }

    #[test]
    fn the_store_reads_dates_in_its_own_zone_and_is_send_and_sync() {
        fn send_sync<T: Send + Sync>() {}
        send_sync::<DividendStore>();

        let tokyo: Tz = "Asia/Tokyo".parse().unwrap();
        let mut store = DividendStore::new(tokyo);
        assert_eq!(store.zone(), tokyo);
        let row = record(day(2026, 2, 3), dec!(25), utc(2026, 1, 5, 0, 0));
        store.insert(row.clone());
        // 2026-02-02 15:00 UTC is already 2026-02-03 in Tokyo.
        assert_eq!(store.previous("XYZ", utc(2026, 2, 2, 15, 0)), Some(&row));
        assert_eq!(store.next("XYZ", utc(2026, 2, 2, 14, 59)), Some(&row));
    }
}
