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

use chrono::{DateTime, Utc};
use rustrade_execution::{
    AccountEvent, AccountEventKind,
    error::UnindexedClientError,
    fill_recovery::{FillRecoveryFailure, FillRecoveryGap, FillRecoveryScope},
    trade::{Trade, TradesRead},
};
use rustrade_instrument::{
    asset::name::AssetNameExchange, instrument::name::InstrumentNameExchange,
};
use std::{fmt, future::Future, sync::Arc, time::Duration};
use tokio::time::Instant;

/// Reads of the fills missed across a re-opened account stream: what to read and how much effort
/// to spend. [`Default`] is what [`ExecutionBuilder`](super::builder::ExecutionBuilder) uses.
///
/// Recovery is on whenever the manager reopens a stream. It never blocks the reopening: a read
/// that fails or runs out of budget is reported in-band as a
/// [`FillRecoveryGaveUp`](AccountEventKind::FillRecoveryGaveUp) and the stream still opens.
#[derive(Debug, Clone)]
pub struct FillRecoveryConfig {
    /// How far before the watermark the read starts. Covers the delay between a fill and its
    /// delivery, so a fill delivered just before the stream ended is read again, not missed.
    pub overlap: Duration,
    /// The most the venue's clock is assumed to lag this host's. The watermark can sit up to this
    /// much before the local time the last event arrived, to follow the exchange's clock when the
    /// last fill carries an earlier time than the local clock says. Beyond it the local time is
    /// trusted, so an old last fill does not stretch the read. The span still starts at the last
    /// event delivered, so a stream that was quiet for hours reads those hours (up to `max_span`).
    /// A venue clock ahead of this host's is not allowed for: a fill stamped after the read began
    /// is the new stream's to deliver.
    pub max_clock_skew: Duration,
    /// The time the whole read may take, across all its calls. Measured on the tokio clock, not
    /// on [`clock`](Self::clock).
    pub read_timeout: Duration,
    /// The most calls to `fetch_trades` one read makes; at least one call is always made.
    pub max_pages: u32,
    /// The longest span one read covers. What lies further back is reported as a gap, not read.
    pub max_span: Duration,
    /// Where "now" comes from, for the end of the span and for the watermark.
    pub clock: Clock,
}

impl Default for FillRecoveryConfig {
    fn default() -> Self {
        Self {
            overlap: Duration::from_secs(2),
            max_clock_skew: Duration::from_secs(30),
            read_timeout: Duration::from_secs(30),
            max_pages: 20,
            max_span: Duration::from_secs(24 * 60 * 60),
            clock: Clock::default(),
        }
    }
}

/// A source of the current time, injectable so a test controls it.
#[derive(Clone)]
pub struct Clock(Arc<dyn Fn() -> DateTime<Utc> + Send + Sync>);

impl Clock {
    /// A clock that calls `now`.
    pub fn new(now: impl Fn() -> DateTime<Utc> + Send + Sync + 'static) -> Self {
        Self(Arc::new(now))
    }

    /// The current time.
    pub fn now(&self) -> DateTime<Utc> {
        (self.0)()
    }
}

impl Default for Clock {
    /// The system clock.
    fn default() -> Self {
        Self::new(Utc::now)
    }
}

impl fmt::Debug for Clock {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Clock(..)")
    }
}

/// The last moment the manager knows its account stream was in sync with the venue, from what it
/// has delivered.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Watermark {
    /// When this host last received an event, or opened the stream before any arrived.
    receipt: DateTime<Utc>,
    /// The latest time the exchange stamped on a fill delivered, if any.
    exchange: Option<DateTime<Utc>>,
}

impl Watermark {
    /// A stream opened at `now`, with nothing delivered yet.
    pub(crate) fn opened(now: DateTime<Utc>) -> Self {
        Self {
            receipt: now,
            exchange: None,
        }
    }

    /// Records an event delivered at `now`.
    pub(crate) fn delivered<K, A, I>(&mut self, now: DateTime<Utc>, event: &AccountEvent<K, A, I>) {
        self.receipt = now;
        if let AccountEventKind::Trade(trade) = &event.kind {
            self.exchange = self.exchange.max(Some(trade.time_exchange));
        }
    }

    /// Where a read to cover everything missed starts.
    ///
    /// The earlier of the local receipt time and the exchange's time of the last fill, so a local
    /// clock ahead of the venue's does not push the start past fills the venue stamped earlier,
    /// less `overlap`. The exchange's time counts only within `max_clock_skew` of the receipt
    /// time: a fill from long ago says nothing of this host's clock.
    pub(crate) fn read_from(&self, config: &FillRecoveryConfig) -> DateTime<Utc> {
        let floor = self.receipt - config.max_clock_skew;
        let bound = match self.exchange {
            Some(exchange) if exchange < self.receipt => exchange.max(floor),
            _ => self.receipt,
        };
        bound - config.overlap
    }
}

/// What one [`backfill`] read: the fills it got, oldest first, and the spans it gave up on.
#[derive(Debug)]
pub(crate) struct Backfill {
    pub(crate) trades: Vec<Trade<AssetNameExchange, InstrumentNameExchange>>,
    pub(crate) gaps: Vec<FillRecoveryGap<InstrumentNameExchange>>,
}

/// Reads `[start, now]` within `config`'s budget, one call of `fetch(from, to)` per page: the
/// client's [`fetch_trades`](rustrade_execution::client::ExecutionClient::fetch_trades) for
/// `instruments`. An empty `instruments` means every instrument.
///
/// Never fails: whatever it could not read is a gap in the result, so the caller reopens the
/// stream either way.
pub(crate) async fn backfill<Fetch, Fut>(
    mut fetch: Fetch,
    instruments: &[InstrumentNameExchange],
    start: DateTime<Utc>,
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
    let now = config.clock.now();
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
        .and_then(|span| now.checked_sub_signed(span))
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
        let read = match tokio::time::timeout_at(deadline, fetch(cursor, now)).await {
            Ok(Ok(read)) => read,
            Ok(Err(error)) => {
                result.gaps.push(gap(
                    cursor,
                    now,
                    FillRecoveryFailure::Request(error.to_string()),
                ));
                break;
            }
            Err(_) => {
                result.gaps.push(gap(
                    cursor,
                    now,
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
                now,
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
                now,
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

    fn config(now: i64) -> FillRecoveryConfig {
        FillRecoveryConfig {
            clock: Clock::new(move || at(now)),
            ..FillRecoveryConfig::default()
        }
    }

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
            config,
        )
        .await;
        (result, calls.into_inner().unwrap())
    }

    #[tokio::test]
    async fn a_complete_read_returns_its_trades_oldest_first_and_no_gap() {
        let read = TradesRead::complete(vec![trade("b", 20), trade("a", 10)]);

        let (result, calls) = run(vec![Ok(read)], 0, &config(100)).await;

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

        let (result, calls) = run(vec![Ok(first), Ok(second)], 0, &config(100)).await;

        assert_eq!(calls, [at(0), at(10)]);
        assert_eq!(result.trades.len(), 2);
        assert!(result.gaps.is_empty());
    }

    #[tokio::test]
    async fn a_read_that_does_not_advance_is_a_truncated_gap() {
        let stuck = TradesRead::new(vec![trade("a", 0)], Some(at(0)));

        let (result, calls) = run(vec![Ok(stuck)], 0, &config(100)).await;

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
            ..config(100)
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

        let (result, _) = run(vec![first, failed], 0, &config(100)).await;

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
            ..config(100)
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

        let (result, _) = run(vec![Ok(read)], 0, &config(100)).await;

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

        let (result, _) = run(vec![Ok(read)], 0, &config(100)).await;

        let [gap] = result.gaps.as_slice() else {
            panic!("one gap, got {:?}", result.gaps)
        };
        assert_eq!((gap.start, gap.end), (at(0), at(0)));
    }

    #[tokio::test]
    async fn a_held_since_at_or_before_the_start_is_no_gap() {
        let read = TradesRead::complete(vec![]).with_held_since(Some(at(0)));

        let (result, _) = run(vec![Ok(read)], 0, &config(100)).await;

        assert!(result.gaps.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn a_read_that_outlasts_the_timeout_is_a_timed_out_gap() {
        let config = FillRecoveryConfig {
            read_timeout: Duration::from_secs(5),
            ..config(100)
        };

        let result = backfill(|_, _| std::future::pending::<Read>(), &[], at(0), &config).await;

        let [gap] = result.gaps.as_slice() else {
            panic!("one gap, got {:?}", result.gaps)
        };
        assert_eq!(
            gap.reason,
            FillRecoveryFailure::TimedOut { timeout_secs: 5 }
        );
        assert_eq!(gap.scope, FillRecoveryScope::AllInstruments);
    }

    #[test]
    fn the_watermark_reads_from_the_receipt_time_less_the_overlap_until_a_fill_is_delivered() {
        let config = config(0);

        let watermark = Watermark::opened(at(100));

        assert_eq!(watermark.read_from(&config), at(98));
    }

    #[test]
    fn a_fill_stamped_before_the_receipt_time_pulls_the_watermark_back_within_the_skew() {
        let config = config(0);
        let mut watermark = Watermark::opened(at(100));
        watermark.exchange = Some(at(90));

        // 10 s before the receipt time, inside the 30 s skew allowance.
        assert_eq!(watermark.read_from(&config), at(88));
    }

    #[test]
    fn a_fill_stamped_long_before_the_receipt_time_is_held_to_the_skew_allowance() {
        let config = config(0);
        let mut watermark = Watermark::opened(at(10_000));
        watermark.exchange = Some(at(0));

        assert_eq!(watermark.read_from(&config), at(10_000 - 30 - 2));
    }

    #[test]
    fn a_fill_stamped_after_the_receipt_time_does_not_push_the_watermark_forward() {
        let config = config(0);
        let mut watermark = Watermark::opened(at(100));
        watermark.exchange = Some(at(150));

        assert_eq!(watermark.read_from(&config), at(98));
    }

    #[test]
    fn delivering_a_trade_moves_the_receipt_time_and_keeps_the_latest_fill_time() {
        let mut watermark = Watermark::opened(at(0));
        let event = |secs| AccountEvent {
            exchange: 0usize,
            kind: AccountEventKind::<usize, usize, usize>::Trade(Trade::new(
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
        };

        watermark.delivered(at(100), &event(95));
        watermark.delivered(at(200), &event(80));

        assert_eq!(watermark.receipt, at(200));
        assert_eq!(watermark.exchange, Some(at(95)));
    }

    #[tokio::test(start_paused = true)]
    async fn a_read_that_finishes_within_the_timeout_is_not_a_gap() {
        let config = FillRecoveryConfig {
            read_timeout: Duration::from_secs(5),
            ..config(100)
        };

        let result = backfill(
            |_, _| async {
                tokio::time::sleep(Duration::from_secs(1)).await;
                Ok(TradesRead::complete(vec![trade("a", 10)]))
            },
            &[],
            at(0),
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
            ..config(100)
        };

        let (result, calls) = run(vec![Ok(TradesRead::complete(vec![]))], 0, &config).await;

        assert_eq!(calls, [at(0)]);
        assert!(result.gaps.is_empty());
    }

    #[test]
    fn the_clock_has_a_debug_form() {
        assert_eq!(format!("{:?}", Clock::default()), "Clock(..)");
    }
}
