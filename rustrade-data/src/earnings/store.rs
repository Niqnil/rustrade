//! [`EarningsStore`]: earnings releases as they were known at a given time.

use super::{EarningsKey, EarningsRelease, ReleaseStatus};
use chrono::{DateTime, NaiveDate, Utc};
use rustrade_instrument::calendar::Tz;
use smol_str::SmolStr;
use std::{
    cmp::Ordering,
    collections::{BTreeMap, HashMap},
};

/// Earnings releases as they were known at any given time: the next and the previous release of
/// a symbol as of an instant, from every record put in, whatever its provider.
///
/// It keeps every record and answers from those already public at the instant asked about, so a
/// backtest asks it what a live system would have known at its clock, and a live system asks it
/// about now.
///
/// # Contract
/// - **Visibility.** A query as of `as_of` uses only records whose
///   [`known_at`](EarningsRelease::known_at) is at or before `as_of`.
/// - **Supersession.** Of a release's visible records (those with the same
///   [`EarningsKey`]), one stands for it, in this order:
///   1. a [`Reported`](ReleaseStatus::Reported) record beats any other status, since a release
///      cannot un-happen: a calendar's estimate known later does not hide it;
///   2. then the later [`known_at`](EarningsRelease::known_at);
///   3. then the later [`observed_at`](super::ReleaseSource::observed_at);
///   4. then the later [`updated_at`](super::ReleaseSource::updated_at), a record without one
///      ranking lowest;
///   5. then the record inserted later.
///
///   Providers rank alike: a caller that prefers one provider puts in only its records.
/// - **Past or coming.** As of `as_of`, a release has happened if its standing record is
///   `Reported`, or is dated before `as_of`'s date in the store's [zone](Self::zone). Otherwise
///   it is still coming, all day on its own date if no `Reported` record is visible yet.
/// - **Statuses.** A [`Canceled`](ReleaseStatus::Canceled) release is neither next nor previous.
///   A [`Postponed`](ReleaseStatus::Postponed) release is next whatever its date, since it is
///   still owed and its date no longer holds, and never previous. A
///   [`Confirmed`](ReleaseStatus::Confirmed), [`Projected`](ReleaseStatus::Projected) or
///   [`Unknown`](ReleaseStatus::Unknown) release whose date has passed is previous: its status
///   says no record confirms it happened.
///
/// The store does not decide when a coming date is firm enough to trade on: a consumer that
/// estimates dates from past releases until shortly before the next one applies its own rule to
/// what [`next`](Self::next) returns.
///
/// # Time zone
/// Every [`date`](EarningsRelease::date) is in the local time of the issuer's listing exchange,
/// and the store compares them with `as_of`'s date in the one zone it is built with. Keep issuers
/// listed in different zones in separate stores.
///
/// # Known limitations
/// - A release on `as_of`'s date with no visible `Reported` record stays next until the day
///   ends, even after an after-close release has come out. The store puts no session times on
///   [`timing`](EarningsRelease::timing).
/// - Every record put in is kept, so memory grows with each fetch of a calendar. Only a record
///   equal in every field to one already held is dropped; the same calendar fetched twice differs
///   in [`observed_at`](super::ReleaseSource::observed_at) and is kept twice.
///
/// It is plain data: [`Send`] and [`Sync`], and a caller sharing one between threads adds its own
/// lock.
///
/// ```
/// use chrono::{NaiveDate, TimeZone, Utc};
/// use rustrade_data::earnings::{
///     EarningsProvider, EarningsRelease, EarningsStore, ReleaseSource, ReleaseStatus,
///     ReleaseTiming,
/// };
/// use rustrade_instrument::calendar::Tz;
///
/// let day = |y, m, d| NaiveDate::from_ymd_opt(y, m, d).unwrap();
/// let fetched = Utc.with_ymd_and_hms(2026, 1, 5, 12, 0, 0).unwrap();
/// let calendar_row = EarningsRelease::new(
///     "XYZ",
///     day(2025, 12, 31),
///     day(2026, 1, 28),
///     ReleaseTiming::AfterClose,
///     ReleaseStatus::Unknown,
///     fetched,
///     ReleaseSource::new(EarningsProvider::AlphaVantage, fetched),
/// );
///
/// let mut store = EarningsStore::new("America/New_York".parse::<Tz>().unwrap());
/// store.insert(calendar_row.clone());
///
/// // Before the row was fetched, nothing is known; after, it is the next release.
/// assert_eq!(store.next("XYZ", fetched - chrono::TimeDelta::seconds(1)), None);
/// assert_eq!(store.next("XYZ", fetched), Some(&calendar_row));
/// assert_eq!(store.previous("XYZ", fetched), None);
/// ```
#[derive(Debug, Clone)]
pub struct EarningsStore {
    zone: Tz,
    /// Each symbol's releases by period, each release's records in supersession order, the
    /// strongest last.
    releases: HashMap<SmolStr, BTreeMap<NaiveDate, Vec<Record>>>,
    records: usize,
}

/// A record as held, with the order it was inserted in for the last tie-break.
#[derive(Debug, Clone)]
struct Record {
    release: EarningsRelease,
    sequence: usize,
}

impl Record {
    /// Supersession order: see the [`EarningsStore`] contract.
    fn rank(&self, other: &Self) -> Ordering {
        let (a, b) = (&self.release, &other.release);
        (a.status == ReleaseStatus::Reported)
            .cmp(&(b.status == ReleaseStatus::Reported))
            .then(a.known_at.cmp(&b.known_at))
            .then(a.source.observed_at.cmp(&b.source.observed_at))
            .then(a.source.updated_at.cmp(&b.source.updated_at))
            .then(self.sequence.cmp(&other.sequence))
    }
}

/// Where a release stands as of a time, from its standing record.
enum Standing {
    Coming,
    Past,
    /// Neither next nor previous.
    Excluded,
}

impl EarningsStore {
    /// An empty store comparing release dates in `zone`, the time zone of the listing exchange.
    pub fn new(zone: Tz) -> Self {
        Self {
            zone,
            releases: HashMap::new(),
            records: 0,
        }
    }

    /// The time zone the store reads release dates in.
    pub fn zone(&self) -> Tz {
        self.zone
    }

    /// Adds `release`, in any order relative to the records already held. Returns `false`, and
    /// keeps nothing, if a record equal in every field is already held.
    pub fn insert(&mut self, release: EarningsRelease) -> bool {
        let key = release.key();
        let records = self
            .releases
            .entry(key.symbol().clone())
            .or_default()
            .entry(key.period())
            .or_default();
        if records.iter().any(|held| held.release == release) {
            return false;
        }
        let record = Record {
            release,
            sequence: self.records,
        };
        // Later insertion ranks higher on a full tie, so the new record goes after any equal one.
        let at = records.partition_point(|held| held.rank(&record) != Ordering::Greater);
        records.insert(at, record);
        self.records += 1;
        true
    }

    /// The record that stands for the release `key` as of `as_of`, by the
    /// [supersession](Self#contract) order, or `None` if none of its records was public by then.
    pub fn current(&self, key: &EarningsKey, as_of: DateTime<Utc>) -> Option<&EarningsRelease> {
        self.releases
            .get(key.symbol())?
            .get(&key.period())
            .and_then(|records| standing_record(records, as_of))
    }

    /// The coming release of `symbol` as of `as_of` with the earliest date (a
    /// [`Postponed`](ReleaseStatus::Postponed) one first, by its old date), or `None` if none is
    /// known by then. See the [contract](Self#contract).
    pub fn next(&self, symbol: &str, as_of: DateTime<Utc>) -> Option<&EarningsRelease> {
        self.standing(symbol, as_of)
            .filter(|(standing, _, _)| matches!(standing, Standing::Coming))
            .min_by_key(|(_, period, release)| (release.date, *period))
            .map(|(_, _, release)| release)
    }

    /// The release of `symbol` that has happened as of `as_of` with the latest date, or `None`
    /// if none is known by then. See the [contract](Self#contract).
    pub fn previous(&self, symbol: &str, as_of: DateTime<Utc>) -> Option<&EarningsRelease> {
        self.standing(symbol, as_of)
            .filter(|(standing, _, _)| matches!(standing, Standing::Past))
            .max_by_key(|(_, period, release)| (release.date, *period))
            .map(|(_, _, release)| release)
    }

    /// Every record held of the release `key`, public or not, in supersession order with the
    /// strongest last: the last is the one standing once all of them are public.
    pub fn observations(&self, key: &EarningsKey) -> impl Iterator<Item = &EarningsRelease> {
        self.releases
            .get(key.symbol())
            .and_then(|periods| periods.get(&key.period()))
            .into_iter()
            .flatten()
            .map(|record| &record.release)
    }

    /// The number of records held.
    pub fn len(&self) -> usize {
        self.records
    }

    /// Whether the store holds no records.
    pub fn is_empty(&self) -> bool {
        self.records == 0
    }

    /// Each release of `symbol` with a record public by `as_of`: where its standing record puts
    /// it, its key's period, and the record.
    fn standing<'a>(
        &'a self,
        symbol: &str,
        as_of: DateTime<Utc>,
    ) -> impl Iterator<Item = (Standing, NaiveDate, &'a EarningsRelease)> {
        let today = as_of.with_timezone(&self.zone).date_naive();
        self.releases
            .get(symbol)
            .into_iter()
            .flatten()
            .filter_map(move |(period, records)| {
                let release = standing_record(records, as_of)?;
                Some((standing(release, today), *period, release))
            })
    }
}

impl Extend<EarningsRelease> for EarningsStore {
    fn extend<I: IntoIterator<Item = EarningsRelease>>(&mut self, releases: I) {
        for release in releases {
            self.insert(release);
        }
    }
}

/// The strongest of `records` public by `as_of`: they are held in supersession order.
fn standing_record(records: &[Record], as_of: DateTime<Utc>) -> Option<&EarningsRelease> {
    records
        .iter()
        .rev()
        .map(|record| &record.release)
        .find(|release| release.known_at <= as_of)
}

/// Where `release`, a standing record, puts its release on the exchange's date `today`.
fn standing(release: &EarningsRelease, today: NaiveDate) -> Standing {
    match release.status {
        ReleaseStatus::Canceled => Standing::Excluded,
        ReleaseStatus::Postponed => Standing::Coming,
        ReleaseStatus::Reported => Standing::Past,
        ReleaseStatus::Confirmed | ReleaseStatus::Projected | ReleaseStatus::Unknown => {
            if release.date < today {
                Standing::Past
            } else {
                Standing::Coming
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use crate::earnings::{EarningsProvider, ReleaseSource, ReleaseTiming};
    use chrono::{TimeDelta, TimeZone};

    fn day(y: i32, m: u32, d: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(y, m, d).unwrap()
    }

    fn utc(y: i32, m: u32, d: u32, h: u32, min: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(y, m, d, h, min, 0).unwrap()
    }

    fn new_york() -> EarningsStore {
        EarningsStore::new("America/New_York".parse().unwrap())
    }

    const Q3: (i32, u32, u32) = (2025, 9, 30);
    const Q4: (i32, u32, u32) = (2025, 12, 31);
    const Q1: (i32, u32, u32) = (2026, 3, 31);

    /// A record of XYZ's release for the period ending `period`, dated `date`, known and fetched
    /// at `known_at`.
    fn record(
        period: (i32, u32, u32),
        date: NaiveDate,
        status: ReleaseStatus,
        known_at: DateTime<Utc>,
    ) -> EarningsRelease {
        EarningsRelease::new(
            "XYZ",
            day(period.0, period.1, period.2),
            date,
            ReleaseTiming::AfterClose,
            status,
            known_at,
            ReleaseSource::new(EarningsProvider::AlphaVantage, known_at),
        )
    }

    #[test]
    fn a_record_is_visible_from_its_known_at_inclusive() {
        let known_at = utc(2026, 1, 5, 12, 0);
        let row = record(Q4, day(2026, 1, 28), ReleaseStatus::Unknown, known_at);
        let mut store = new_york();
        assert!(store.is_empty());
        assert!(store.insert(row.clone()));
        assert_eq!((store.len(), store.is_empty()), (1, false));

        let key = row.key();
        let before = known_at - TimeDelta::nanoseconds(1);
        assert_eq!(store.current(&key, before), None);
        assert_eq!(store.next("XYZ", before), None);
        assert_eq!(store.current(&key, known_at), Some(&row));
        assert_eq!(store.next("XYZ", known_at), Some(&row));
        assert_eq!(store.previous("XYZ", known_at), None);
        assert_eq!(store.next("ABC", known_at), None);
        assert_eq!(
            store.current(&EarningsKey::new("XYZ", day(2026, 3, 31)), known_at),
            None
        );
    }

    #[test]
    fn a_moved_date_supersedes_the_old_one_from_its_known_at() {
        let first = record(
            Q4,
            day(2026, 1, 28),
            ReleaseStatus::Confirmed,
            utc(2026, 1, 5, 12, 0),
        );
        let moved = record(
            Q4,
            day(2026, 1, 29),
            ReleaseStatus::Confirmed,
            utc(2026, 1, 9, 12, 0),
        );
        let mut store = new_york();
        // Inserted newest first: the store sorts.
        store.extend([moved.clone(), first.clone()]);

        assert_eq!(store.next("XYZ", utc(2026, 1, 9, 11, 59)), Some(&first));
        assert_eq!(store.next("XYZ", utc(2026, 1, 9, 12, 0)), Some(&moved));
        assert_eq!(
            store.observations(&first.key()).collect::<Vec<_>>(),
            [&first, &moved]
        );
    }

    #[test]
    fn a_reported_record_beats_an_estimate_known_later() {
        // EDGAR accepted the release at 16:05 New York; a history fetch at 16:10, before its
        // session bound, could only say Unknown. EDGAR's exact period end keys with the month end.
        let reported = EarningsRelease::new(
            "XYZ",
            day(2025, 12, 27),
            day(2026, 1, 28),
            ReleaseTiming::AfterClose,
            ReleaseStatus::Reported,
            utc(2026, 1, 28, 21, 5),
            ReleaseSource::new(EarningsProvider::SecEdgar, utc(2026, 3, 1, 0, 0)),
        );
        let estimate = record(
            Q4,
            day(2026, 1, 28),
            ReleaseStatus::Unknown,
            utc(2026, 1, 28, 21, 10),
        );
        let key = reported.key();
        assert_eq!(key, estimate.key());

        for order in [
            [reported.clone(), estimate.clone()],
            [estimate.clone(), reported.clone()],
        ] {
            let mut store = new_york();
            store.extend(order);
            assert_eq!(store.current(&key, utc(2026, 1, 28, 21, 4)), None);
            assert_eq!(
                store.current(&key, utc(2026, 1, 28, 21, 5)),
                Some(&reported)
            );
            assert_eq!(
                store.current(&key, utc(2026, 1, 28, 21, 10)),
                Some(&reported)
            );
            assert_eq!(
                store.previous("XYZ", utc(2026, 1, 28, 21, 10)),
                Some(&reported)
            );
            assert_eq!(store.next("XYZ", utc(2026, 1, 28, 21, 10)), None);
            assert_eq!(
                store.observations(&key).collect::<Vec<_>>(),
                [&estimate, &reported]
            );
        }
    }

    #[test]
    fn a_later_known_at_outranks_a_later_fetch() {
        let known_early_fetched_late = EarningsRelease::new(
            "XYZ",
            day(2025, 12, 31),
            day(2026, 1, 28),
            ReleaseTiming::AfterClose,
            ReleaseStatus::Confirmed,
            utc(2026, 1, 5, 0, 0),
            ReleaseSource::new(EarningsProvider::AlphaVantage, utc(2026, 1, 20, 0, 0)),
        );
        let known_late = record(
            Q4,
            day(2026, 1, 29),
            ReleaseStatus::Confirmed,
            utc(2026, 1, 6, 0, 0),
        );
        let mut store = new_york();
        store.extend([known_late.clone(), known_early_fetched_late.clone()]);
        assert_eq!(
            store.current(&known_late.key(), utc(2026, 1, 21, 0, 0)),
            Some(&known_late)
        );
    }

    #[test]
    fn ties_on_known_at_fall_to_observed_at_then_updated_at_then_insertion() {
        let known_at = utc(2026, 1, 5, 12, 0);
        let later = known_at + TimeDelta::seconds(1);
        let base = record(Q4, day(2026, 1, 28), ReleaseStatus::Unknown, known_at);
        let with = |date: NaiveDate, source: ReleaseSource| {
            let mut release = base.clone();
            release.date = date;
            release.source = source;
            release
        };
        let source = ReleaseSource::new(EarningsProvider::AlphaVantage, known_at);
        let updated = with(day(2026, 1, 29), source.clone().with_updated_at(known_at));
        let updated_later = with(day(2026, 1, 30), source.clone().with_updated_at(later));
        let observed_later = with(
            day(2026, 2, 2),
            ReleaseSource::new(EarningsProvider::AlphaVantage, later),
        );
        let key = base.key();

        let mut store = new_york();
        store.extend([
            observed_later.clone(),
            updated_later.clone(),
            base.clone(),
            updated.clone(),
        ]);
        assert_eq!(
            store.observations(&key).collect::<Vec<_>>(),
            [&base, &updated, &updated_later, &observed_later]
        );
        assert_eq!(store.current(&key, known_at), Some(&observed_later));

        // A full tie: the record inserted later stands, whichever it is.
        let other_date = with(day(2026, 1, 27), source.clone());
        for (first, second) in [(&base, &other_date), (&other_date, &base)] {
            let mut store = new_york();
            store.extend([first.clone(), second.clone()]);
            assert_eq!(store.current(&key, known_at), Some(second));
        }
    }

    #[test]
    fn a_release_on_todays_date_stays_next_until_the_exchange_day_ends() {
        let row = record(
            Q4,
            day(2026, 1, 28),
            ReleaseStatus::Confirmed,
            utc(2026, 1, 5, 0, 0),
        );
        let mut store = new_york();
        store.insert(row.clone());

        // 2026-01-28 23:59 New York is already 2026-01-29 in UTC.
        let last_minute = utc(2026, 1, 29, 4, 59);
        assert_eq!(store.next("XYZ", last_minute), Some(&row));
        assert_eq!(store.previous("XYZ", last_minute), None);
        let midnight = utc(2026, 1, 29, 5, 0);
        assert_eq!(store.next("XYZ", midnight), None);
        assert_eq!(store.previous("XYZ", midnight), Some(&row));
    }

    #[test]
    fn next_is_the_earliest_coming_and_previous_the_latest_past() {
        let known_at = utc(2025, 10, 1, 0, 0);
        let q3 = record(Q3, day(2025, 10, 29), ReleaseStatus::Reported, known_at);
        let q4 = record(Q4, day(2026, 1, 28), ReleaseStatus::Projected, known_at);
        let q1 = record(Q1, day(2026, 4, 29), ReleaseStatus::Projected, known_at);
        let mut store = new_york();
        store.extend([q1.clone(), q3.clone(), q4.clone()]);

        assert_eq!(store.next("XYZ", known_at), Some(&q4));
        assert_eq!(store.previous("XYZ", known_at), Some(&q3));
        // Once Q4's date passes with no record that it happened, it is the previous release.
        let after_q4 = utc(2026, 2, 1, 0, 0);
        assert_eq!(store.next("XYZ", after_q4), Some(&q1));
        assert_eq!(store.previous("XYZ", after_q4), Some(&q4));
    }

    #[test]
    fn releases_on_the_same_date_order_by_period() {
        let known_at = utc(2026, 1, 5, 0, 0);
        let date = day(2026, 1, 28);
        let q3 = record(Q3, date, ReleaseStatus::Unknown, known_at);
        let q4 = record(Q4, date, ReleaseStatus::Unknown, known_at);
        let mut store = new_york();
        store.extend([q4.clone(), q3.clone()]);
        assert_eq!(store.next("XYZ", known_at), Some(&q3));
        assert_eq!(store.previous("XYZ", utc(2026, 2, 1, 0, 0)), Some(&q4));
    }

    #[test]
    fn a_canceled_release_is_neither_next_nor_previous() {
        let q4 = record(
            Q4,
            day(2026, 1, 28),
            ReleaseStatus::Confirmed,
            utc(2026, 1, 5, 0, 0),
        );
        let canceled = record(
            Q4,
            day(2026, 1, 28),
            ReleaseStatus::Canceled,
            utc(2026, 1, 10, 0, 0),
        );
        let q1 = record(
            Q1,
            day(2026, 4, 29),
            ReleaseStatus::Projected,
            utc(2026, 1, 5, 0, 0),
        );
        let mut store = new_york();
        store.extend([q4.clone(), canceled.clone(), q1.clone()]);

        assert_eq!(store.next("XYZ", utc(2026, 1, 9, 0, 0)), Some(&q4));
        assert_eq!(store.next("XYZ", utc(2026, 1, 10, 0, 0)), Some(&q1));
        assert_eq!(store.previous("XYZ", utc(2026, 2, 1, 0, 0)), None);
        assert_eq!(
            store.current(&q4.key(), utc(2026, 2, 1, 0, 0)),
            Some(&canceled)
        );
    }

    #[test]
    fn a_postponed_release_is_next_after_its_date_and_never_previous() {
        let postponed = record(
            Q4,
            day(2026, 1, 28),
            ReleaseStatus::Postponed,
            utc(2026, 1, 20, 0, 0),
        );
        let q1 = record(
            Q1,
            day(2026, 4, 29),
            ReleaseStatus::Projected,
            utc(2026, 1, 5, 0, 0),
        );
        let mut store = new_york();
        store.extend([postponed.clone(), q1.clone()]);

        let after_its_date = utc(2026, 2, 15, 0, 0);
        assert_eq!(store.next("XYZ", after_its_date), Some(&postponed));
        assert_eq!(store.previous("XYZ", after_its_date), None);
        assert_eq!(store.previous("XYZ", utc(2026, 5, 1, 0, 0)), Some(&q1));
    }

    #[test]
    fn an_identical_record_is_kept_once_and_a_refetch_twice() {
        let known_at = utc(2026, 1, 5, 0, 0);
        let row = record(Q4, day(2026, 1, 28), ReleaseStatus::Unknown, known_at);
        let refetch = record(
            Q4,
            day(2026, 1, 28),
            ReleaseStatus::Unknown,
            known_at + TimeDelta::days(1),
        );
        let mut store = new_york();
        assert!(store.insert(row.clone()));
        assert!(!store.insert(row.clone()));
        assert_eq!(store.len(), 1);
        assert!(store.insert(refetch));
        assert_eq!(store.len(), 2);
        assert_eq!(store.observations(&row.key()).count(), 2);
        assert_eq!(
            store
                .observations(&EarningsKey::new("ABC", day(2025, 12, 31)))
                .count(),
            0
        );
    }

    #[test]
    fn the_store_reads_dates_in_its_own_zone_and_is_send_and_sync() {
        fn send_sync<T: Send + Sync>() {}
        send_sync::<EarningsStore>();

        let tokyo: Tz = "Asia/Tokyo".parse().unwrap();
        let mut store = EarningsStore::new(tokyo);
        assert_eq!(store.zone(), tokyo);
        let row = record(
            Q4,
            day(2026, 1, 28),
            ReleaseStatus::Confirmed,
            utc(2026, 1, 5, 0, 0),
        );
        store.insert(row.clone());
        // 2026-01-28 15:00 UTC is already 2026-01-29 in Tokyo.
        assert_eq!(store.previous("XYZ", utc(2026, 1, 28, 15, 0)), Some(&row));
        assert_eq!(store.next("XYZ", utc(2026, 1, 28, 14, 59)), Some(&row));
    }
}
