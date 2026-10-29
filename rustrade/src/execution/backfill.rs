//! Reading the fills an account stream missed while it was down.
//!
//! When the execution manager reopens an account stream that ended, fills made in between never
//! reach the engine. [`backfill`] reads that span through
//! [`fetch_trades`](rustrade_execution::client::ExecutionClient::fetch_trades) so the manager can deliver those fills before the new
//! snapshot, and report what it could not read as a [`FillRecoveryGap`](rustrade_execution::fill_recovery::FillRecoveryGap).
//!
//! The span starts at a watermark: the last moment the manager knows it was in sync, less an
//! overlap, so a fill made right at the edge is read twice rather than missed. Reading a fill
//! twice is harmless because the manager drops duplicates with an
//! [`AccountEventDeduper`](rustrade_execution::client::dedup::AccountEventDeduper).
//!
//! The venue reads the span on its own clock, which can differ from this host's. So the span is
//! placed on the venue's clock, as a
//! [`VenueClock`](rustrade_execution::client::venue_clock::VenueClock) estimates it from the
//! times the venue stamped on the fills delivered, counted on from there on tokio's monotonic
//! clock. A step of this
//! host's wall clock, before the stream ended or while it was down, does not move the span.

use chrono::{DateTime, TimeDelta, Utc};
use rustrade_execution::{
    AccountEvent, AccountEventKind,
    client::venue_clock::VenueClock,
    error::UnindexedClientError,
    fill_recovery::{FillRecoveryFailure, FillRecoveryGap, FillRecoveryScope},
    trade::{Trade, TradesRead},
};
use rustrade_instrument::{
    asset::name::AssetNameExchange, instrument::name::InstrumentNameExchange,
};
use serde::{Deserialize, Serialize};
use std::{future::Future, time::Duration};
use tokio::time::Instant;

/// Reads of the fills missed across a re-opened account stream: what to read and how much effort
/// to spend. [`Default`] is the one in
/// [`AccountStreamOptions::default`](super::manager::AccountStreamOptions), which
/// [`ExecutionBuilder::add_live`](super::builder::ExecutionBuilder::add_live) uses.
///
/// Recovery is on whenever the manager reopens a stream. It never blocks the reopening: a read
/// that fails or runs out of budget is reported in-band as a
/// [`FillRecoveryGaveUp`](AccountEventKind::FillRecoveryGaveUp) and the stream still opens.
///
/// Built from [`Default`] and the `with_*` methods, so fields can be added without a breaking
/// change. Venues differ in how many fills one call returns and how long they keep them, so the
/// budget is set per venue.
///
/// A field missing from its serialised form takes its default, so a form written before a field
/// was added still loads. Each [`Duration`] is serialised as `secs` and `nanos`.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize, Serialize)]
#[serde(default)]
pub struct FillRecoveryConfig {
    /// How far before the watermark the read starts. Covers the delay between a fill and its
    /// delivery, so a fill delivered just before the stream ended is read again, not missed.
    pub overlap: Duration,
    /// The difference between this host's clock and the venue's that is allowed for without
    /// a warning, and the margin for it where it cannot be measured:
    /// - Once a fill has been delivered, the venue's clock is estimated from the time it stamped
    ///   on it, and the read is placed on that clock. A difference beyond this allowance is
    ///   logged as a warning at each reopening, and the read still covers it.
    /// - Before any fill, the venue's clock is unknown, and the read starts this much before the
    ///   local time the last event arrived. A host clock further ahead of the venue's than this
    ///   can then miss fills.
    /// - The read runs this much past the later of the local time and the venue's estimated
    ///   time, to cover a venue clock ahead of the estimate. A fill it reads that the new stream
    ///   also delivers is delivered once.
    pub max_clock_skew: Duration,
    /// The time the whole read may take, across all its calls. Measured on the tokio clock, not
    /// on the manager's [`Clock`](super::manager::Clock).
    pub read_timeout: Duration,
    /// The most calls to `fetch_trades` one read makes; at least one call is always made.
    pub max_pages: u32,
    /// The longest span one read covers. What lies further back is reported as a gap, not read.
    pub max_span: Duration,
}

impl Default for FillRecoveryConfig {
    fn default() -> Self {
        Self {
            overlap: Duration::from_secs(2),
            max_clock_skew: Duration::from_secs(30),
            read_timeout: Duration::from_secs(30),
            max_pages: 20,
            max_span: Duration::from_secs(24 * 60 * 60),
        }
    }
}

impl FillRecoveryConfig {
    /// Sets [`overlap`](Self::overlap).
    #[must_use]
    pub const fn with_overlap(self, overlap: Duration) -> Self {
        Self { overlap, ..self }
    }

    /// Sets [`max_clock_skew`](Self::max_clock_skew).
    #[must_use]
    pub const fn with_max_clock_skew(self, max_clock_skew: Duration) -> Self {
        Self {
            max_clock_skew,
            ..self
        }
    }

    /// Sets [`read_timeout`](Self::read_timeout).
    #[must_use]
    pub const fn with_read_timeout(self, read_timeout: Duration) -> Self {
        Self {
            read_timeout,
            ..self
        }
    }

    /// Sets [`max_pages`](Self::max_pages).
    #[must_use]
    pub const fn with_max_pages(self, max_pages: u32) -> Self {
        Self { max_pages, ..self }
    }

    /// Sets [`max_span`](Self::max_span).
    #[must_use]
    pub const fn with_max_span(self, max_span: Duration) -> Self {
        Self { max_span, ..self }
    }
}

/// The last moment the manager knows its account stream was in sync with the venue, from what it
/// has delivered.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Watermark {
    /// When this host last received an event, or opened the stream before any arrived, by the
    /// manager's clock.
    receipt: DateTime<Utc>,
    /// The same moment on tokio's monotonic clock.
    received: Instant,
    /// The venue's clock, from the times it stamped on the fills delivered.
    venue: VenueClock,
}

impl Watermark {
    /// A stream opened at `now`, read at `instant` on tokio's clock, with nothing delivered yet.
    pub(crate) fn opened(now: DateTime<Utc>, instant: Instant) -> Self {
        Self {
            receipt: now,
            received: instant,
            venue: VenueClock::new(),
        }
    }

    /// Records an event delivered at `now`, read at `instant` on tokio's clock.
    ///
    /// A [`StreamTerminated`](AccountEventKind::StreamTerminated) is not a sign of sync but the
    /// end of it: the stream can have lost events before it says so, as on a lagged buffer, so it
    /// leaves the watermark where the last event before it put it. A loss the stream delivers
    /// further events after, without ending, is not seen here.
    ///
    /// A fill's time is the only one taken for the venue's clock: every client stamps fills with
    /// the venue's time, but some stamp other events with this host's. A fill stamped `max_span`
    /// or more before `now` is not taken either: it is an old fill delivered late, such as one a
    /// venue replays on subscribing, and a read placed by it would start further back than any
    /// read covers.
    pub(crate) fn delivered<K, A, I>(
        &mut self,
        now: DateTime<Utc>,
        instant: Instant,
        event: &AccountEvent<K, A, I>,
        config: &FillRecoveryConfig,
    ) {
        match &event.kind {
            AccountEventKind::StreamTerminated(_) => return,
            AccountEventKind::Trade(trade) => {
                let recent = to_delta(config.max_span)
                    .is_none_or(|span| now.signed_duration_since(trade.time_exchange) < span);
                if recent {
                    self.venue.observe(trade.time_exchange, instant);
                }
            }
            _ => {}
        }
        self.receipt = now;
        self.received = instant;
    }

    /// Where a read to cover everything missed starts, less `overlap`.
    ///
    /// The venue's time when the last event arrived, as estimated from the fills delivered, or
    /// the local time it arrived if that is earlier, as on a local clock behind the venue's or a
    /// simulated one. With no fill delivered the venue's clock is unknown, and the read starts
    /// `max_clock_skew` before the local time instead.
    pub(crate) fn read_from(&self, config: &FillRecoveryConfig) -> DateTime<Utc> {
        let bound = match self.venue.earliest_at(self.received) {
            Some(venue) => venue.min(self.receipt),
            None => self.receipt - config.max_clock_skew,
        };
        bound - config.overlap
    }

    /// Where a read started at `now`, read at `instant` on tokio's clock, ends: `max_clock_skew`
    /// past the later of `now` and the venue's estimated time, so a local clock behind the
    /// venue's does not cut the read short.
    pub(crate) fn read_to(
        &self,
        now: DateTime<Utc>,
        instant: Instant,
        config: &FillRecoveryConfig,
    ) -> DateTime<Utc> {
        let venue = self.venue.earliest_at(instant).unwrap_or(now);
        venue.max(now) + config.max_clock_skew
    }

    /// How far this host's clock leads the venue's at `now`, read at `instant` on tokio's clock,
    /// when that is beyond `max_clock_skew` either way, or `None` within it or with no fill
    /// delivered.
    ///
    /// A lead is overstated by the delivery delay of the fill it was measured from, never
    /// understated, so a host clock behind the venue's is reported only when it is surely beyond
    /// the allowance.
    pub(crate) fn skew_beyond_allowance(
        &self,
        now: DateTime<Utc>,
        instant: Instant,
        config: &FillRecoveryConfig,
    ) -> Option<TimeDelta> {
        let lead = self.venue.host_lead(now, instant)?;
        let allowance = to_delta(config.max_clock_skew)?;
        (lead.abs() > allowance).then_some(lead)
    }
}

/// `duration` as a [`TimeDelta`], or `None` beyond its range.
fn to_delta(duration: Duration) -> Option<TimeDelta> {
    TimeDelta::from_std(duration).ok()
}

/// What one [`backfill`] read: the fills it got, oldest first, and the spans it gave up on.
#[derive(Debug)]
pub(crate) struct Backfill {
    pub(crate) trades: Vec<Trade<AssetNameExchange, InstrumentNameExchange>>,
    pub(crate) gaps: Vec<FillRecoveryGap<InstrumentNameExchange>>,
}

/// Reads `[start, end]` within `config`'s budget, one call of `fetch(from, to)` per page: the
/// client's [`fetch_trades`](rustrade_execution::client::ExecutionClient::fetch_trades) for
/// `instruments`. An empty `instruments` means every instrument.
///
/// Never fails: whatever it could not read is a gap in the result, so the caller reopens the
/// stream either way.
pub(crate) async fn backfill<Fetch, Fut>(
    mut fetch: Fetch,
    instruments: &[InstrumentNameExchange],
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    config: &FillRecoveryConfig,
) -> Backfill
where
    Fetch: FnMut(DateTime<Utc>, DateTime<Utc>) -> Fut,
    Fut: Future<
        Output = Result<
            TradesRead<AssetNameExchange, InstrumentNameExchange>,
            UnindexedClientError,
        >,
    >,
{
    let deadline = Instant::now() + config.read_timeout;
    let scope = || {
        if instruments.is_empty() {
            FillRecoveryScope::AllInstruments
        } else {
            FillRecoveryScope::Instruments(instruments.to_vec())
        }
    };
    let gap = |start, end, reason| FillRecoveryGap::new(scope(), start, end, 1, reason);

    let mut result = Backfill {
        trades: Vec::new(),
        gaps: Vec::new(),
    };

    let oldest = chrono::Duration::from_std(config.max_span)
        .ok()
        .and_then(|span| end.checked_sub_signed(span))
        .unwrap_or(start);
    let mut cursor = if start < oldest {
        result.gaps.push(gap(
            start,
            oldest - chrono::Duration::milliseconds(1),
            FillRecoveryFailure::SpanLimit {
                max_span_secs: config.max_span.as_secs(),
            },
        ));
        oldest
    } else {
        start
    };

    let mut pages = 0;
    loop {
        pages += 1;
        let read = match tokio::time::timeout_at(deadline, fetch(cursor, end)).await {
            Ok(Ok(read)) => read,
            Ok(Err(error)) => {
                result.gaps.push(gap(
                    cursor,
                    end,
                    FillRecoveryFailure::Request(error.to_string()),
                ));
                break;
            }
            Err(_) => {
                result.gaps.push(gap(
                    cursor,
                    end,
                    FillRecoveryFailure::TimedOut {
                        timeout_secs: config.read_timeout.as_secs(),
                    },
                ));
                break;
            }
        };

        if pages == 1
            && let Some(held_since) = read.held_since.filter(|held| *held > cursor)
        {
            result.gaps.push(gap(
                cursor,
                (held_since - chrono::Duration::milliseconds(1)).max(cursor),
                FillRecoveryFailure::VenueRetention { held_since },
            ));
        }

        result.trades.extend(read.trades);

        let Some(resume) = read.resume else { break };
        if resume <= cursor {
            // A call that read on from `cursor` and came back no further is not progress.
            result.gaps.push(gap(
                cursor,
                end,
                FillRecoveryFailure::Truncated {
                    fills_read: result.trades.len(),
                },
            ));
            break;
        }
        cursor = resume;
        if pages >= config.max_pages {
            result.gaps.push(gap(
                cursor,
                end,
                FillRecoveryFailure::PageLimit {
                    max_pages: config.max_pages,
                },
            ));
            break;
        }
    }

    result.trades.sort_by_key(|trade| trade.time_exchange);
    result
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use chrono::TimeZone;
    use rust_decimal::Decimal;
    use rustrade_execution::{
        order::id::{OrderId, StrategyId},
        trade::{AssetFees, TradeId},
    };
    use rustrade_instrument::Side;
    use std::{collections::VecDeque, sync::Mutex};

    fn at(secs: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(1_800_000_000 + secs, 0).unwrap()
    }

    fn trade(id: &str, secs: i64) -> Trade<AssetNameExchange, InstrumentNameExchange> {
        Trade::new(
            TradeId::new(id),
            OrderId::new("o"),
            InstrumentNameExchange::new("btc_usdt"),
            StrategyId::unknown(),
            at(secs),
            Side::Buy,
            Decimal::ONE,
            Decimal::ONE,
            None,
            AssetFees::new(AssetNameExchange::from("usdt"), Decimal::ZERO, None),
        )
    }

    /// When every read in these tests runs.
    const NOW: i64 = 100;

    type Read = Result<TradesRead<AssetNameExchange, InstrumentNameExchange>, UnindexedClientError>;

    /// Runs `backfill` against scripted answers, returning the result and each call's `from`.
    async fn run(
        answers: Vec<Read>,
        start: i64,
        config: &FillRecoveryConfig,
    ) -> (Backfill, Vec<DateTime<Utc>>) {
        let answers = Mutex::new(VecDeque::from(answers));
        let calls = Mutex::new(Vec::new());
        let result = backfill(
            |from, _| {
                calls.lock().unwrap().push(from);
                let answer = answers.lock().unwrap().pop_front();
                async move { answer.expect("a call beyond the scripted answers") }
            },
            &[InstrumentNameExchange::new("btc_usdt")],
            at(start),
            at(NOW),
            config,
        )
        .await;
        (result, calls.into_inner().unwrap())
    }

    #[tokio::test]
    async fn a_complete_read_returns_its_trades_oldest_first_and_no_gap() {
        let read = TradesRead::complete(vec![trade("b", 20), trade("a", 10)]);

        let (result, calls) = run(vec![Ok(read)], 0, &FillRecoveryConfig::default()).await;

        assert_eq!(calls, [at(0)]);
        let ids = result
            .trades
            .iter()
            .map(|t| t.id.0.as_str())
            .collect::<Vec<_>>();
        assert_eq!(ids, ["a", "b"]);
        assert!(result.gaps.is_empty());
    }

    #[tokio::test]
    async fn a_read_that_stops_at_its_bound_reads_on_from_resume() {
        let first = TradesRead::new(vec![trade("a", 10)], Some(at(10)));
        let second = TradesRead::complete(vec![trade("b", 30)]);

        let (result, calls) = run(
            vec![Ok(first), Ok(second)],
            0,
            &FillRecoveryConfig::default(),
        )
        .await;

        assert_eq!(calls, [at(0), at(10)]);
        assert_eq!(result.trades.len(), 2);
        assert!(result.gaps.is_empty());
    }

    #[tokio::test]
    async fn a_read_that_does_not_advance_is_a_truncated_gap() {
        let stuck = TradesRead::new(vec![trade("a", 0)], Some(at(0)));

        let (result, calls) = run(vec![Ok(stuck)], 0, &FillRecoveryConfig::default()).await;

        assert_eq!(calls.len(), 1);
        assert_eq!(result.trades.len(), 1);
        let [gap] = result.gaps.as_slice() else {
            panic!("one gap, got {:?}", result.gaps)
        };
        assert_eq!((gap.start, gap.end), (at(0), at(100)));
        assert_eq!(gap.reason, FillRecoveryFailure::Truncated { fills_read: 1 });
    }

    #[tokio::test]
    async fn the_page_limit_leaves_the_unread_rest_as_a_gap() {
        let page = |resume| Ok(TradesRead::new(vec![], Some(at(resume))));
        let config = FillRecoveryConfig {
            max_pages: 2,
            ..FillRecoveryConfig::default()
        };

        let (result, calls) = run(vec![page(10), page(20)], 0, &config).await;

        assert_eq!(calls, [at(0), at(10)]);
        let [gap] = result.gaps.as_slice() else {
            panic!("one gap, got {:?}", result.gaps)
        };
        assert_eq!((gap.start, gap.end), (at(20), at(100)));
        assert_eq!(gap.reason, FillRecoveryFailure::PageLimit { max_pages: 2 });
    }

    #[tokio::test]
    async fn a_failed_call_is_a_gap_from_where_it_read_and_keeps_what_was_read() {
        let first = Ok(TradesRead::new(vec![trade("a", 10)], Some(at(10))));
        let failed = Err(UnindexedClientError::Internal("boom".into()));

        let (result, _) = run(vec![first, failed], 0, &FillRecoveryConfig::default()).await;

        assert_eq!(result.trades.len(), 1);
        let [gap] = result.gaps.as_slice() else {
            panic!("one gap, got {:?}", result.gaps)
        };
        assert_eq!((gap.start, gap.end), (at(10), at(100)));
        assert!(matches!(&gap.reason, FillRecoveryFailure::Request(m) if m.contains("boom")));
        assert_eq!(gap.attempts, 1);
    }

    #[tokio::test]
    async fn a_span_older_than_the_maximum_is_a_gap_and_the_rest_is_read() {
        let config = FillRecoveryConfig {
            max_span: Duration::from_secs(60),
            ..FillRecoveryConfig::default()
        };

        let (result, calls) = run(vec![Ok(TradesRead::complete(vec![]))], 0, &config).await;

        assert_eq!(calls, [at(40)]);
        let [gap] = result.gaps.as_slice() else {
            panic!("one gap, got {:?}", result.gaps)
        };
        assert_eq!(
            (gap.start, gap.end),
            (at(0), at(40) - chrono::Duration::milliseconds(1))
        );
        assert_eq!(
            gap.reason,
            FillRecoveryFailure::SpanLimit { max_span_secs: 60 }
        );
    }

    #[tokio::test]
    async fn fills_the_venue_no_longer_holds_are_a_gap_before_held_since() {
        let read = TradesRead::complete(vec![trade("a", 50)]).with_held_since(Some(at(40)));

        let (result, _) = run(vec![Ok(read)], 0, &FillRecoveryConfig::default()).await;

        let [gap] = result.gaps.as_slice() else {
            panic!("one gap, got {:?}", result.gaps)
        };
        assert_eq!(
            (gap.start, gap.end),
            (at(0), at(40) - chrono::Duration::milliseconds(1))
        );
        assert_eq!(
            gap.reason,
            FillRecoveryFailure::VenueRetention { held_since: at(40) }
        );
        assert_eq!(result.trades.len(), 1);
    }

    #[tokio::test]
    async fn a_held_since_less_than_a_millisecond_after_the_start_is_a_gap_of_that_moment() {
        let held_since = at(0) + chrono::Duration::microseconds(500);
        let read = TradesRead::complete(vec![]).with_held_since(Some(held_since));

        let (result, _) = run(vec![Ok(read)], 0, &FillRecoveryConfig::default()).await;

        let [gap] = result.gaps.as_slice() else {
            panic!("one gap, got {:?}", result.gaps)
        };
        assert_eq!((gap.start, gap.end), (at(0), at(0)));
    }

    #[tokio::test]
    async fn a_held_since_at_or_before_the_start_is_no_gap() {
        let read = TradesRead::complete(vec![]).with_held_since(Some(at(0)));

        let (result, _) = run(vec![Ok(read)], 0, &FillRecoveryConfig::default()).await;

        assert!(result.gaps.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn a_read_that_outlasts_the_timeout_is_a_timed_out_gap() {
        let config = FillRecoveryConfig {
            read_timeout: Duration::from_secs(5),
            ..FillRecoveryConfig::default()
        };

        let result = backfill(
            |_, _| std::future::pending::<Read>(),
            &[],
            at(0),
            at(NOW),
            &config,
        )
        .await;

        let [gap] = result.gaps.as_slice() else {
            panic!("one gap, got {:?}", result.gaps)
        };
        assert_eq!(
            gap.reason,
            FillRecoveryFailure::TimedOut { timeout_secs: 5 }
        );
        assert_eq!(gap.scope, FillRecoveryScope::AllInstruments);
    }

    fn fill(secs: i64) -> AccountEvent<usize, usize, usize> {
        AccountEvent {
            exchange: 0usize,
            kind: AccountEventKind::Trade(Trade::new(
                TradeId::new("t"),
                OrderId::new("o"),
                0,
                StrategyId::unknown(),
                at(secs),
                Side::Buy,
                Decimal::ONE,
                Decimal::ONE,
                None,
                AssetFees::new(0, Decimal::ZERO, None),
            )),
        }
    }

    /// An event that carries no time of the venue's.
    fn snapshot() -> AccountEvent<usize, usize, usize> {
        AccountEvent {
            exchange: 0usize,
            kind: AccountEventKind::Snapshot(rustrade_execution::AccountSnapshot::new(
                0,
                vec![],
                vec![],
            )),
        }
    }

    fn secs(secs: u64) -> Duration {
        Duration::from_secs(secs)
    }

    /// The venue's time `secs` after the sample stamped `stamped`, less the 100 ppm drift
    /// allowance.
    fn counted_on(stamped: i64, secs: u64) -> DateTime<Utc> {
        let elapsed = Duration::from_secs(secs);
        at(stamped) + (elapsed - elapsed / 10_000)
    }

    #[test]
    fn with_no_fill_delivered_the_read_starts_the_skew_allowance_before_the_receipt_time() {
        let config = FillRecoveryConfig::default();

        let watermark = Watermark::opened(at(100), Instant::now());

        assert_eq!(watermark.read_from(&config), at(100 - 30 - 2));
    }

    /// The issue this watermark exists for: a host clock 190 s ahead of the venue's, far beyond
    /// the 30 s allowance, still places the read on the venue's clock.
    #[test]
    fn a_host_clock_ahead_of_the_venue_beyond_the_allowance_reads_from_the_venues_time() {
        let config = FillRecoveryConfig::default();
        let base = Instant::now();
        let mut watermark = Watermark::opened(at(200), base);

        watermark.delivered(at(200), base, &fill(10), &config);
        // The last event, 5 s later, carries no venue time.
        watermark.delivered(at(205), base + secs(5), &snapshot(), &config);

        assert_eq!(watermark.read_from(&config), counted_on(10, 5) - secs(2));
    }

    #[test]
    fn a_host_clock_behind_the_venue_reads_from_the_receipt_time() {
        let config = FillRecoveryConfig::default();
        let base = Instant::now();
        let mut watermark = Watermark::opened(at(0), base);

        watermark.delivered(at(0), base, &fill(500), &config);

        assert_eq!(watermark.read_from(&config), at(0) - secs(2));
    }

    #[test]
    fn a_host_clock_stepped_forward_after_the_last_event_does_not_move_the_start() {
        let config = FillRecoveryConfig::default();
        let base = Instant::now();
        let mut watermark = Watermark::opened(at(100), base);
        watermark.delivered(at(100), base, &fill(100), &config);

        // The read is placed after the host clock jumped an hour ahead.
        let start = watermark.read_from(&config);

        assert_eq!(start, at(98));
        assert_eq!(
            watermark.read_to(at(3_700), base + secs(10), &config),
            at(3_700) + secs(30)
        );
    }

    /// The issue's second case: the host clock steps back before the reconnect, so its "now" is
    /// before the outage ended. The read still runs past the venue's time.
    #[test]
    fn a_host_clock_stepped_back_before_the_reconnect_still_reads_to_the_venues_time() {
        let config = FillRecoveryConfig::default();
        let base = Instant::now();
        let mut watermark = Watermark::opened(at(100), base);
        watermark.delivered(at(100), base, &fill(100), &config);

        // 60 s later the host clock reads 100 s earlier than at the fill.
        let end = watermark.read_to(at(0), base + secs(60), &config);

        assert_eq!(end, counted_on(100, 60) + secs(30));
        assert!(end > at(160), "{end} ends before the venue's 160");
    }

    #[test]
    fn with_no_fill_delivered_the_read_ends_the_allowance_past_now() {
        let config = FillRecoveryConfig::default();
        let base = Instant::now();
        let watermark = Watermark::opened(at(100), base);

        assert_eq!(
            watermark.read_to(at(150), base + secs(50), &config),
            at(180)
        );
    }

    #[test]
    fn a_fill_stamped_further_back_than_the_span_is_not_taken_for_the_venues_clock() {
        let config = FillRecoveryConfig::default();
        let base = Instant::now();
        let mut watermark = Watermark::opened(at(100_000), base);

        // A day and a second old: a replayed fill, not a venue clock a day behind.
        watermark.delivered(at(100_000), base, &fill(100_000 - 86_401), &config);

        assert_eq!(watermark.read_from(&config), at(100_000 - 30 - 2));
    }

    #[test]
    fn a_fill_stamped_just_within_the_span_is_taken_for_the_venues_clock() {
        let config = FillRecoveryConfig::default();
        let base = Instant::now();
        let mut watermark = Watermark::opened(at(100_000), base);

        watermark.delivered(at(100_000), base, &fill(100_000 - 86_399), &config);

        assert_eq!(watermark.read_from(&config), at(100_000 - 86_399 - 2));
    }

    #[test]
    fn delivering_an_event_moves_the_receipt_time_on_both_clocks() {
        let config = FillRecoveryConfig::default();
        let base = Instant::now();
        let mut watermark = Watermark::opened(at(0), base);

        watermark.delivered(at(100), base + secs(1), &snapshot(), &config);

        assert_eq!(watermark.receipt, at(100));
        assert_eq!(watermark.received, base + secs(1));
    }

    #[test]
    fn a_stream_terminated_leaves_the_watermark_where_it_was() {
        let config = FillRecoveryConfig::default();
        let base = Instant::now();
        let mut watermark = Watermark::opened(at(0), base);
        let terminated = AccountEvent {
            exchange: 0usize,
            kind: AccountEventKind::<usize, usize, usize>::StreamTerminated(
                rustrade_execution::error::StreamTerminationReason::Error("lagged".to_owned()),
            ),
        };

        watermark.delivered(at(100), base + secs(1), &terminated, &config);

        assert_eq!(watermark.receipt, at(0));
        assert_eq!(watermark.received, base);
        assert_eq!(watermark.venue, VenueClock::new());
    }

    #[test]
    fn the_skew_warning_fires_above_the_allowance_and_not_at_or_below_it() {
        let config = FillRecoveryConfig::default();
        let base = Instant::now();
        let mut watermark = Watermark::opened(at(0), base);
        watermark.delivered(at(0), base, &fill(100), &config);
        let beyond = |host: i64| watermark.skew_beyond_allowance(at(host), base, &config);

        assert_eq!(beyond(131), Some(TimeDelta::seconds(31)));
        assert_eq!(beyond(130), None);
        assert_eq!(beyond(100), None);
        assert_eq!(beyond(70), None);
        assert_eq!(beyond(69), Some(TimeDelta::seconds(-31)));
    }

    #[test]
    fn with_no_fill_delivered_no_skew_is_reported() {
        let config = FillRecoveryConfig::default();
        let base = Instant::now();
        let watermark = Watermark::opened(at(0), base);

        assert_eq!(
            watermark.skew_beyond_allowance(at(10_000), base, &config),
            None
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_read_that_finishes_within_the_timeout_is_not_a_gap() {
        let config = FillRecoveryConfig {
            read_timeout: Duration::from_secs(5),
            ..FillRecoveryConfig::default()
        };

        let result = backfill(
            |_, _| async {
                tokio::time::sleep(Duration::from_secs(1)).await;
                Ok(TradesRead::complete(vec![trade("a", 10)]))
            },
            &[],
            at(0),
            at(NOW),
            &config,
        )
        .await;

        assert_eq!(result.trades.len(), 1);
        assert!(result.gaps.is_empty());
    }

    #[tokio::test]
    async fn a_span_exactly_the_maximum_long_is_read_whole_with_no_gap() {
        let config = FillRecoveryConfig {
            max_span: Duration::from_secs(100),
            ..FillRecoveryConfig::default()
        };

        let (result, calls) = run(vec![Ok(TradesRead::complete(vec![]))], 0, &config).await;

        assert_eq!(calls, [at(0)]);
        assert!(result.gaps.is_empty());
    }

    #[test]
    fn the_config_serialises_each_duration_as_secs_and_nanos_and_loads_a_partial_form() {
        let config = FillRecoveryConfig::default();
        let json = serde_json::to_value(config).unwrap();
        assert_eq!(
            json,
            serde_json::json!({
                "overlap": { "secs": 2, "nanos": 0 },
                "max_clock_skew": { "secs": 30, "nanos": 0 },
                "read_timeout": { "secs": 30, "nanos": 0 },
                "max_pages": 20,
                "max_span": { "secs": 86_400, "nanos": 0 },
            })
        );
        assert_eq!(
            serde_json::from_value::<FillRecoveryConfig>(json).unwrap(),
            config
        );

        let partial: FillRecoveryConfig =
            serde_json::from_value(serde_json::json!({ "max_pages": 3 })).unwrap();
        assert_eq!(partial, config.with_max_pages(3));
    }

    #[test]
    fn the_setters_set_only_their_field() {
        let second = Duration::from_secs(1);
        let base = FillRecoveryConfig::default();
        assert_eq!(
            base.with_overlap(second),
            FillRecoveryConfig {
                overlap: second,
                ..base
            }
        );
        assert_eq!(
            base.with_max_clock_skew(second),
            FillRecoveryConfig {
                max_clock_skew: second,
                ..base
            }
        );
        assert_eq!(
            base.with_read_timeout(second),
            FillRecoveryConfig {
                read_timeout: second,
                ..base
            }
        );
        assert_eq!(
            base.with_max_pages(3),
            FillRecoveryConfig {
                max_pages: 3,
                ..base
            }
        );
        assert_eq!(
            base.with_max_span(second),
            FillRecoveryConfig {
                max_span: second,
                ..base
            }
        );
    }
}
