use crate::{EngineEvent, engine::Processor, execution::AccountStreamEvent};
use chrono::{DateTime, Utc};
use rustrade_data::streams::consumer::MarketStreamEvent;
use rustrade_execution::AccountEventKind;
use seqlock::{RawInstant, SeqLockInstant};
use serde::{Deserialize, Serialize};
use std::{fmt::Debug, sync::Arc};
use tracing::{debug, error, warn};

mod seqlock;

/// The atomics [`seqlock`] is built from: the standard library's here, a model checker's in its
/// test.
mod sync {
    pub(super) use std::sync::atomic::{AtomicI64, AtomicU32, AtomicU64, Ordering, fence};

    pub(super) fn spin() {
        std::hint::spin_loop();
    }
}

/// Defines how an [`Engine`](super::Engine) will determine the current time.
///
/// Generally an `Engine` will use a:
/// * [`LiveClock`] for live-trading.
/// * [`HistoricalClock`] for back-testing.
///
/// A [`HistoricalClock`] derives "current time" — and the engine replays events in
/// that order — from each event's `time_exchange`. For aggregated payloads such as
/// candles, `time_exchange` must be the period **end** (`close_time`) to avoid
/// lookahead; see [`rustrade_data::event::MarketEvent::time_exchange`].
pub trait EngineClock {
    fn time(&self) -> DateTime<Utc>;

    /// Advance the clock's notion of "current time" to `time`, if `time` is later than the
    /// current time (**monotonic** — never regresses).
    ///
    /// Used for engine events that carry no timestamp in their payload but whose effective
    /// instant is derivable from engine state — e.g. [`EngineEvent::ContractExpiry`], whose
    /// handler resolves the expiring instrument's `expiry` and advances the clock to it so a
    /// backtest stamps the synthetic settlement fill at the expiry instant rather than the prior
    /// market tick. Events that already carry their instant on the payload (e.g.
    /// [`EngineEvent::CorporateAction`]'s `effective_time`) advance the clock via the normal
    /// [`TimeExchange`] path in [`Processor::process`] and do not use this method.
    ///
    /// The default implementation is a no-op, correct for clocks that derive time externally
    /// (e.g. [`LiveClock`], which reads `Utc::now()`). [`HistoricalClock`] overrides it.
    ///
    /// # Implementors
    /// If your [`time`](Self::time) is derived from the timestamps of processed events (like
    /// [`HistoricalClock`]) rather than a live wall-clock source, you **must** override this
    /// method. The default no-op would otherwise silently prevent payload-timeless events such as
    /// [`EngineEvent::ContractExpiry`] from advancing your clock to their derived instant —
    /// reproducing the stale-stamp bug this method exists to fix.
    fn advance_to(&self, _time: DateTime<Utc>) {}
}

/// Defines how to extract an "exchange timestamp" from an event.
///
/// Used by a [`HistoricalClock`] to assist deriving the "current" `Engine` time.
/// The returned instant is the event's position on the engine timeline — for
/// candles and other windowed data it is the period **end** (`close_time`); see
/// [`rustrade_data::event::MarketEvent::time_exchange`] for the full contract.
pub trait TimeExchange {
    fn time_exchange(&self) -> Option<DateTime<Utc>>;
}

/// Live `Clock` using `Utc::now()`.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize)]
pub struct LiveClock;

impl EngineClock for LiveClock {
    fn time(&self) -> DateTime<Utc> {
        Utc::now()
    }
}

impl<Event> Processor<&Event> for LiveClock {
    type Audit = ();

    fn process(&mut self, _: &Event) -> Self::Audit {}
}

/// Live `Clock` measured on tokio's clock: the wall-clock instant it was created at, plus the
/// [`tokio::time::Instant`] time elapsed since.
///
/// For tests of a live setup, such as paper trading on a
/// [`PaperMarketFeed`](crate::execution::paper::PaperMarketFeed), under tokio's paused time
/// (`#[tokio::test(start_paused = true)]`): it moves only as tokio's clock does, so a test that
/// advances time sees every timer and every stamp move together, where [`LiveClock`] would not move
/// at all.
///
/// It never moves backwards, but after it is created it follows tokio's monotonic clock rather
/// than the system clock, so it drifts from `Utc::now()` by however the system clock is adjusted
/// later. Live trading wants [`LiveClock`].
#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub struct TokioClock {
    anchor_time: DateTime<Utc>,
    anchor_instant: tokio::time::Instant,
}

impl TokioClock {
    /// A clock reading `time` now, and moving on with tokio's clock from here.
    pub fn starting_at(time: DateTime<Utc>) -> Self {
        Self {
            anchor_time: time,
            anchor_instant: tokio::time::Instant::now(),
        }
    }
}

impl EngineClock for TokioClock {
    /// Saturates at [`DateTime::<Utc>::MAX_UTC`] rather than overflowing, which would take some
    /// hundreds of thousands of years.
    fn time(&self) -> DateTime<Utc> {
        chrono::TimeDelta::from_std(self.anchor_instant.elapsed())
            .ok()
            .and_then(|elapsed| self.anchor_time.checked_add_signed(elapsed))
            .unwrap_or(DateTime::<Utc>::MAX_UTC)
    }
}

impl<Event> Processor<&Event> for TokioClock {
    type Audit = ();

    fn process(&mut self, _: &Event) -> Self::Audit {}
}

/// Historical `Clock` whose "now" is the `time_exchange` of the most recent event processed.
///
/// [`time`](EngineClock::time) is a pure function of the events replayed so far: it returns that
/// timestamp verbatim and never consults the wall clock. Two replays of the same data therefore
/// read the same time at the same point, which is what makes a backtest reproducible — the clock
/// stamps simulated fills, trades and balances through the simulated exchange, and seeds
/// `time_engine_start`/`time_engine_end`, the denominator of every annualised statistic.
///
/// The consequence to know: between events the clock does **not** advance. A strategy that reads
/// `time()` twice without an intervening event sees one instant, and a sparse feed leaves it
/// standing still for as long as the data does. That is the correct reading of simulated time — no
/// simulated time passes where no data does — but it differs from a wall clock, and code that
/// measures elapsed time by differencing `time()` will measure the data rather than itself.
///
/// Note that this cannot be initialised without a starting `last_exchange_timestamp`.
///
/// # Sharing between threads
///
/// Clones share one instant, so advancing any clone advances them all.
/// - [`time`](EngineClock::time) never takes a lock. A read that overlaps an advance retries, and
///   never blocks the advance. A read that finds an advance storing spins until it is done, which
///   takes a few stores, unless the advancing thread is descheduled in between.
/// - Advances ([`advance_to`](EngineClock::advance_to) and [`process`](Processor::process))
///   serialise among themselves: one waits, spinning, while another on a clone is storing. An
///   advance to an instant no later than the current one stores nothing and takes no lock: it
///   waits only as a read does.
/// - Monotonic: whatever order advances on several threads arrive in, the clock ends at the
///   latest instant any of them was given, and a reader never sees it move backwards.
/// - Every `DateTime<Utc>` is held exactly, from [`DateTime::<Utc>::MIN_UTC`] to
///   [`DateTime::<Utc>::MAX_UTC`], leap seconds and nanoseconds included.
#[derive(Clone)]
pub struct HistoricalClock {
    inner: Arc<SeqLockInstant>,
}

impl Debug for HistoricalClock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HistoricalClock")
            .field("time_exchange_last", &self.inner.load())
            .finish()
    }
}

impl HistoricalClock {
    /// Construct a new `HistoricalClock` using the provided `last_exchange_time` as a seed.
    pub fn new(last_exchange_time: DateTime<Utc>) -> Self {
        Self {
            inner: Arc::new(SeqLockInstant::new(last_exchange_time)),
        }
    }

    /// Whether `self` and `other` are clones of one clock, so advancing either advances both.
    pub(crate) fn is_shared_with(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }
}

impl EngineClock for HistoricalClock {
    fn time(&self) -> DateTime<Utc> {
        self.inner.load()
    }

    fn advance_to(&self, time: DateTime<Utc>) {
        // Monotonic: an earlier or equal `time` leaves the clock where it is.
        self.inner.advance(time.into());
    }
}

impl<Event> Processor<&Event> for HistoricalClock
where
    Event: Debug + TimeExchange,
{
    type Audit = ();

    fn process(&mut self, event: &Event) -> Self::Audit {
        let Some(time_event_exchange) = event.time_exchange() else {
            debug!(?event, "HistoricalClock found no timestamp in event");
            return;
        };

        // Decided and stored at once, so that another clone's concurrent advance cannot fall
        // between the comparison and the store. Logged after, outside the store. Compared as raw
        // instants: the held instant is rebuilt as a `DateTime` only to be logged.
        let time_event = RawInstant::from(time_event_exchange);
        let held = self.inner.advance(time_event);

        // Input event is as recent or more recent
        if time_event >= held {
            debug!(
                ?event,
                time_exchange_last_current = ?held.to_datetime(),
                time_update = ?time_event_exchange,
                "HistoricalClock updating based on input event time_exchange"
            );
            return;
        };
        let time_exchange_last = held.to_datetime();

        // Input event is older, so log at varying degrees of severity
        let time_diff_secs = time_event_exchange
            .signed_duration_since(time_exchange_last)
            .num_seconds()
            .abs();

        if time_diff_secs < 1 {
            debug!(
                ?event,
                time_exchange_last_current = ?time_exchange_last,
                time_update = ?time_event_exchange,
                time_diff_secs,
                "HistoricalClock received out-of-order events"
            );
        } else if time_diff_secs < 30 {
            warn!(
                ?event,
                time_exchange_last_current = ?time_exchange_last,
                time_update = ?time_event_exchange,
                time_diff_secs,
                "HistoricalClock received out-of-order events"
            );
        } else {
            error!(
                ?event,
                time_exchange_last_current = ?time_exchange_last,
                time_update = ?time_event_exchange,
                time_diff_secs,
                "HistoricalClock received out-of-order events"
            );
        }
    }
}

impl<MarketEventKind: Debug> TimeExchange for EngineEvent<MarketEventKind> {
    fn time_exchange(&self) -> Option<DateTime<Utc>> {
        match self {
            Self::Market(MarketStreamEvent::Item(event)) => Some(event.time_exchange),
            Self::Account(AccountStreamEvent::Item(event)) => match &event.kind {
                AccountEventKind::Snapshot(snapshot) => snapshot.time_most_recent(),
                AccountEventKind::BalanceSnapshot(balance) => Some(balance.0.time_exchange),
                AccountEventKind::BalanceStreamUpdate(update) => Some(update.0.time_exchange),
                AccountEventKind::InstrumentBalanceUpdate(update) => {
                    // Per-pair isolated balance update — advance on its event time, consistent with
                    // its asset-keyed sibling `BalanceStreamUpdate`.
                    Some(update.balance.time_exchange)
                }
                AccountEventKind::InstrumentBalanceSnapshot(snapshot) => {
                    Some(snapshot.balance.time_exchange)
                }
                AccountEventKind::OrderSnapshot(order) => order.0.state.time_exchange(),
                AccountEventKind::OrderCancelled(response) => response
                    .state
                    .as_ref()
                    .map(|cancelled| cancelled.time_exchange)
                    .ok(),
                AccountEventKind::Trade(trade) => Some(trade.time_exchange),
                AccountEventKind::CashFlow(flow) => Some(flow.time_exchange),
                AccountEventKind::Notice(notice) => Some(notice.time_exchange),
                AccountEventKind::MarginDebt(update) => Some(update.time_exchange),
                _ => None,
            },
            // The corporate action carries its own resolved effective instant, so the
            // `HistoricalClock` advances to it — the adjustment is ordered and stamped exactly
            // (no look-ahead onto the prior session's market events).
            Self::CorporateAction { effective_time, .. } => Some(*effective_time),
            _ => None,
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use crate::engine::state::position::SplitRoundingPolicy;
    use chrono::TimeDelta;
    use rust_decimal::Decimal;
    use rustrade_data::event::MarketEvent;
    use rustrade_instrument::{
        corporate_action::{CorporateActionKind, SplitRatio},
        exchange::ExchangeId,
        instrument::InstrumentIndex,
    };

    fn market_event(time_exchange: DateTime<Utc>) -> EngineEvent<()> {
        EngineEvent::Market(MarketStreamEvent::Item(MarketEvent {
            time_exchange,
            time_received: Default::default(),
            exchange: ExchangeId::BinanceSpot,
            instrument: InstrumentIndex::new(0),
            kind: (),
        }))
    }

    #[test]
    fn test_historical_clock_process() {
        #[derive(Debug)]
        struct TestCase {
            name: &'static str,
            time_initial: DateTime<Utc>,
            input_events: Vec<EngineEvent<()>>,
            expected_time_exchange_last: DateTime<Utc>,
        }

        // Create a fixed initial time to use as a base
        let time_base = DateTime::<Utc>::MIN_UTC;

        // Util for adding time
        let plus_ms = |ms: i64| {
            time_base
                .checked_add_signed(TimeDelta::milliseconds(ms))
                .unwrap()
        };

        let cases = [
            // TC0: Basic case - single event in order
            TestCase {
                name: "single event in order",
                time_initial: time_base,
                input_events: vec![market_event(plus_ms(1000))],
                expected_time_exchange_last: plus_ms(1000),
            },
            // TC1: Out of order event - earlier than current
            TestCase {
                name: "out of order event - earlier than current",
                time_initial: plus_ms(1000),
                input_events: vec![market_event(plus_ms(500))],
                expected_time_exchange_last: plus_ms(1000), // Should not update
            },
            // TC2: Equal timestamp event
            TestCase {
                name: "equal timestamp event",
                time_initial: plus_ms(1000),
                input_events: vec![market_event(plus_ms(1000))],
                expected_time_exchange_last: plus_ms(1000), // Should maintain current time
            },
            // TC3: Multiple events in order
            TestCase {
                name: "multiple events in order",
                time_initial: time_base,
                input_events: vec![
                    market_event(plus_ms(1000)),
                    market_event(plus_ms(2000)),
                    market_event(plus_ms(3000)),
                ],
                expected_time_exchange_last: plus_ms(3000),
            },
            // TC4: Multiple events out of order
            TestCase {
                name: "multiple events out of order",
                time_initial: time_base,
                input_events: vec![
                    market_event(plus_ms(3000)),
                    market_event(plus_ms(1000)),
                    market_event(plus_ms(2000)),
                ],
                expected_time_exchange_last: plus_ms(3000),
            },
            // TC5: Event with no timestamp
            TestCase {
                name: "event with no timestamp",
                time_initial: plus_ms(1000),
                input_events: vec![EngineEvent::Market(MarketStreamEvent::Reconnecting(
                    ExchangeId::BinanceSpot,
                ))],
                expected_time_exchange_last: plus_ms(1000), // Should not update
            },
            // TC6: Mixed events with and without timestamps
            TestCase {
                name: "mixed events with and without timestamps",
                time_initial: time_base,
                input_events: vec![
                    market_event(plus_ms(1000)),
                    EngineEvent::Market(MarketStreamEvent::Reconnecting(ExchangeId::BinanceSpot)),
                    market_event(plus_ms(2000)),
                ],
                expected_time_exchange_last: plus_ms(2000),
            },
        ];

        for (index, test) in cases.iter().enumerate() {
            // Setup clock with initial time
            let mut clock = HistoricalClock::new(test.time_initial);

            // Process all events
            for event in test.input_events.iter() {
                clock.process(event);
            }

            assert_eq!(
                clock.time(),
                test.expected_time_exchange_last,
                "TC{} ({}) failed - incorrect time_exchange_last",
                index,
                test.name
            );
        }
    }

    /// Simulated time advances with the data, never with the host.
    ///
    /// This is the property the whole clock exists for: it stamps simulated fills, trades and
    /// balances, so any wall-clock component would make two replays of one dataset disagree. The
    /// assertion is deliberately the exact inverse of what this test asserted while `time()`
    /// interpolated between events.
    #[test]
    fn test_historical_clock_time_does_not_advance_with_wall_clock() {
        let time_base = DateTime::<Utc>::MIN_UTC;
        let clock = HistoricalClock::new(time_base);

        let time_1 = clock.time();
        spin_sleep::sleep(std::time::Duration::from_millis(100));
        let time_2 = clock.time();

        assert_eq!(
            time_1, time_base,
            "a clock that has processed no event reads its seed"
        );
        assert_eq!(
            time_2, time_1,
            "100ms of wall time passed and no event did, so simulated time must not move"
        );

        // An event is the only thing that advances it.
        let mut clock = clock;
        clock.process(&market_event(
            time_base.checked_add_signed(TimeDelta::seconds(7)).unwrap(),
        ));

        assert_eq!(
            clock.time(),
            time_base.checked_add_signed(TimeDelta::seconds(7)).unwrap(),
            "processing an event advances the clock to that event's time_exchange"
        );
    }

    fn corporate_action_event(effective_time: DateTime<Utc>) -> EngineEvent<()> {
        EngineEvent::CorporateAction {
            id: "test-split".into(),
            instrument: InstrumentIndex::new(0),
            kind: CorporateActionKind::StockSplit {
                ratio: SplitRatio::new(Decimal::new(2, 0)).unwrap(),
            },
            policy: SplitRoundingPolicy::Fractional,
            effective_time,
        }
    }

    #[test]
    fn test_corporate_action_advances_clock_to_effective_time() {
        use chrono::{NaiveDate, NaiveTime};

        // Midnight UTC of the effective date, plus two intraday market events on that date.
        let effective = NaiveDate::from_ymd_opt(2026, 6, 22)
            .unwrap()
            .and_time(NaiveTime::MIN)
            .and_utc();
        let intraday_open = effective + TimeDelta::hours(13) + TimeDelta::minutes(30);
        let intraday_later = intraday_open + TimeDelta::hours(2);

        // The event reports its `effective_time` as its exchange time.
        assert_eq!(
            corporate_action_event(effective).time_exchange(),
            Some(effective),
            "CorporateAction must report effective_time as its exchange time"
        );

        // In-order: the midnight split precedes that day's intraday events — the clock
        // advances to the split, then monotonically through the intraday prints.
        let mut clock = HistoricalClock::new(effective - TimeDelta::days(1));
        clock.process(&corporate_action_event(effective));
        assert_eq!(clock.time(), effective);
        clock.process(&market_event(intraday_open));
        assert_eq!(clock.time(), intraday_open);
        clock.process(&market_event(intraday_later));
        assert_eq!(clock.time(), intraday_later);

        // Out-of-order safety: a midnight split arriving after that day's intraday events
        // must not regress the clock (same monotonic guard as any other event).
        let mut clock = HistoricalClock::new(intraday_later);
        clock.process(&corporate_action_event(effective));
        assert_eq!(
            clock.time(),
            intraday_later,
            "an earlier midnight split must not regress time_exchange_last"
        );
    }

    #[test]
    fn test_historical_clock_advance_to_is_monotonic() {
        let base = DateTime::<Utc>::MIN_UTC;
        let later = base + TimeDelta::days(365);

        // Advancing forward moves time_exchange_last to the target (the ContractExpiry use-case:
        // jump the clock to a far-future contract expiry before stamping the settlement fill).
        let clock = HistoricalClock::new(base);
        clock.advance_to(later);
        assert_eq!(clock.time(), later);

        // Advancing to an earlier — or equal — instant is a no-op: the clock never regresses.
        clock.advance_to(base);
        assert_eq!(clock.time(), later);
        clock.advance_to(later);
        assert_eq!(clock.time(), later);
    }

    /// Every `DateTime<Utc>` round-trips through the clock exactly: the extremes, which builders
    /// seed it with, and a leap second, whose nanoseconds exceed one second.
    #[test]
    fn historical_clock_holds_every_instant_exactly() {
        use chrono::NaiveDate;

        let leap_second = NaiveDate::from_ymd_opt(2016, 12, 31)
            .unwrap()
            .and_hms_nano_opt(23, 59, 59, 1_500_000_000)
            .unwrap()
            .and_utc();
        let sub_microsecond = DateTime::<Utc>::from_timestamp(-1, 999_999_999).unwrap();

        for time in [
            DateTime::<Utc>::MIN_UTC,
            DateTime::<Utc>::MAX_UTC,
            DateTime::<Utc>::UNIX_EPOCH,
            leap_second,
            sub_microsecond,
        ] {
            assert_eq!(
                HistoricalClock::new(time).time(),
                time,
                "seeded with {time:?}"
            );

            let clock = HistoricalClock::new(DateTime::<Utc>::MIN_UTC);
            clock.advance_to(time);
            assert_eq!(clock.time(), time, "advanced to {time:?}");

            let mut clock = HistoricalClock::new(DateTime::<Utc>::MIN_UTC);
            clock.process(&market_event(time));
            assert_eq!(clock.time(), time, "processed an event at {time:?}");
        }
    }

    /// The seqlock compares instants as seconds then nanoseconds, which must order them exactly as
    /// `DateTime<Utc>` does, around a leap second and at both ends of the range too.
    #[test]
    fn raw_instants_order_as_their_datetimes() {
        use chrono::NaiveDate;

        let at = |h, m, s, nanos| {
            NaiveDate::from_ymd_opt(2016, 12, 31)
                .unwrap()
                .and_hms_nano_opt(h, m, s, nanos)
                .unwrap()
                .and_utc()
        };
        let times = [
            DateTime::<Utc>::MIN_UTC,
            DateTime::<Utc>::MIN_UTC + TimeDelta::nanoseconds(1),
            DateTime::<Utc>::from_timestamp(-1, 999_999_999).unwrap(),
            DateTime::<Utc>::UNIX_EPOCH,
            at(23, 59, 58, 999_999_999),
            at(23, 59, 59, 0),
            at(23, 59, 59, 999_999_999),
            // The leap second.
            at(23, 59, 59, 1_000_000_000),
            at(23, 59, 59, 1_999_999_999),
            at(0, 0, 0, 0) + TimeDelta::days(1),
            DateTime::<Utc>::MAX_UTC - TimeDelta::nanoseconds(1),
            DateTime::<Utc>::MAX_UTC,
        ];

        for a in times {
            assert_eq!(RawInstant::from(a).to_datetime(), a, "{a:?} round trip");
            for b in times {
                assert_eq!(
                    RawInstant::from(a).cmp(&RawInstant::from(b)),
                    a.cmp(&b),
                    "{a:?} against {b:?}"
                );
            }
        }
    }

    /// Neither `advance_to` nor `process` moves the clock backwards, including from the largest
    /// instant, and an event at the current instant leaves it there.
    #[test]
    fn historical_clock_never_moves_backwards() {
        let max = DateTime::<Utc>::MAX_UTC;
        let earlier = max - TimeDelta::nanoseconds(1);

        let mut clock = HistoricalClock::new(max);
        clock.advance_to(earlier);
        assert_eq!(clock.time(), max);
        clock.process(&market_event(earlier));
        assert_eq!(clock.time(), max);
        clock.process(&market_event(DateTime::<Utc>::MIN_UTC));
        assert_eq!(clock.time(), max);

        // An event at the current instant is accepted, and changes nothing.
        clock.process(&market_event(max));
        assert_eq!(clock.time(), max);

        // One nanosecond later is an advance.
        let mut clock = HistoricalClock::new(earlier);
        clock.process(&market_event(max));
        assert_eq!(clock.time(), max);
    }

    /// `process` logs an event at the clock's instant or later at `DEBUG`, and an older one by how
    /// much older it is: `DEBUG` within a second, `WARN` within 30 seconds, `ERROR` beyond.
    #[test]
    fn historical_clock_logs_an_older_event_by_how_late_it_is() {
        use tracing::Level;

        let base = DateTime::<Utc>::UNIX_EPOCH + TimeDelta::days(1);
        let cases = [
            ("a minute later", TimeDelta::seconds(60), Level::DEBUG),
            ("at the same instant", TimeDelta::zero(), Level::DEBUG),
            (
                "half a second older",
                -TimeDelta::milliseconds(500),
                Level::DEBUG,
            ),
            ("10 seconds older", -TimeDelta::seconds(10), Level::WARN),
            ("a minute older", -TimeDelta::seconds(60), Level::ERROR),
        ];

        for (name, offset, expected) in cases {
            let mut clock = HistoricalClock::new(base);
            let levels = levels_logged(|| clock.process(&market_event(base + offset)));
            assert_eq!(levels, [expected], "an event {name}");
            assert_eq!(clock.time(), base.max(base + offset), "an event {name}");
        }
    }

    /// Runs `f` and returns the level of each `tracing` event it emitted on this thread.
    fn levels_logged(f: impl FnOnce()) -> Vec<tracing::Level> {
        use std::sync::{Arc, Mutex};
        use tracing_subscriber::layer::SubscriberExt;

        #[derive(Default)]
        struct Levels(Arc<Mutex<Vec<tracing::Level>>>);

        impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for Levels {
            fn on_event(
                &self,
                event: &tracing::Event<'_>,
                _: tracing_subscriber::layer::Context<'_, S>,
            ) {
                self.0.lock().unwrap().push(*event.metadata().level());
            }
        }

        let layer = Levels::default();
        let levels = Arc::clone(&layer.0);
        // Thread-local, so other tests running in parallel are not recorded.
        tracing::subscriber::with_default(tracing_subscriber::registry().with(layer), f);
        levels.lock().unwrap().clone()
    }

    /// `Debug` shows the instant held, as the clock's only state.
    #[test]
    fn historical_clock_debug_shows_its_instant() {
        let clock = HistoricalClock::new(DateTime::<Utc>::UNIX_EPOCH);
        assert_eq!(
            format!("{clock:?}"),
            "HistoricalClock { time_exchange_last: 1970-01-01T00:00:00Z }"
        );
    }

    /// Clones share one instant: advancing either, by `advance_to` or by `process`, advances both.
    /// A clock built separately with the same seed does not.
    #[test]
    fn historical_clock_clones_share_one_instant() {
        let base = DateTime::<Utc>::UNIX_EPOCH;
        let mut clock = HistoricalClock::new(base);
        let clone = clock.clone();
        let separate = HistoricalClock::new(base);

        assert!(clock.is_shared_with(&clone));
        assert!(clone.is_shared_with(&clock));
        assert!(!clock.is_shared_with(&separate));

        clone.advance_to(base + TimeDelta::seconds(1));
        assert_eq!(clock.time(), base + TimeDelta::seconds(1));

        clock.process(&market_event(base + TimeDelta::seconds(2)));
        assert_eq!(clone.time(), base + TimeDelta::seconds(2));

        assert_eq!(separate.time(), base);
    }

    /// Clones advanced from several threads end at the latest instant any of them was given, and a
    /// reader running alongside never sees the clock move backwards or read an instant no writer
    /// stored.
    #[test]
    fn historical_clock_advances_from_several_threads() {
        // Few iterations under Miri, which interprets every step.
        const STEPS: i64 = if cfg!(miri) { 20 } else { 2_000 };
        const WRITERS: i64 = 4;

        let base = DateTime::<Utc>::UNIX_EPOCH;
        // Writer `w` stores `base + (step * WRITERS + w)` seconds and as many nanoseconds, so a
        // read mixing two writes' seconds and nanoseconds does not have this shape.
        let instant = |n: i64| base + TimeDelta::seconds(n) + TimeDelta::nanoseconds(n);
        let clock = HistoricalClock::new(base);

        std::thread::scope(|scope| {
            for writer in 0..WRITERS {
                let mut clock = clock.clone();
                scope.spawn(move || {
                    for step in 0..STEPS {
                        let n = step * WRITERS + writer;
                        if n % 2 == 0 {
                            clock.advance_to(instant(n));
                        } else {
                            clock.process(&market_event(instant(n)));
                        }
                    }
                });
            }

            let reader = clock.clone();
            scope.spawn(move || {
                let mut previous = base;
                for _ in 0..STEPS * WRITERS {
                    let time = reader.time();
                    assert!(time >= previous, "{time:?} read after {previous:?}");
                    let elapsed = time - base;
                    assert_eq!(
                        elapsed.subsec_nanos() as i64,
                        elapsed.num_seconds(),
                        "{time:?} mixes two writes"
                    );
                    previous = time;
                }
            });
        });

        assert_eq!(clock.time(), instant(STEPS * WRITERS - 1));
    }

    /// A cash flow is stamped with the venue's time, so a historical clock advances on one as it
    /// does on a trade.
    #[test]
    fn a_cash_flow_reports_its_time_exchange() {
        use rustrade_execution::{
            AccountEvent, AccountEventKind,
            cash_flow::{CashFlow, CashFlowKind},
        };
        use rustrade_instrument::{asset::AssetIndex, exchange::ExchangeIndex};

        let time = DateTime::<Utc>::MIN_UTC + TimeDelta::hours(1);
        let event: EngineEvent<()> =
            EngineEvent::Account(AccountStreamEvent::Item(AccountEvent::new(
                ExchangeIndex(0),
                AccountEventKind::CashFlow(CashFlow::new(
                    CashFlowKind::BorrowFee {
                        rate: None,
                        quantity: None,
                    },
                    AssetIndex(0),
                    Decimal::NEGATIVE_ONE,
                    Some(InstrumentIndex::new(0)),
                    time,
                    None,
                )),
            )));

        assert_eq!(event.time_exchange(), Some(time));
    }

    /// A margin debt change is stamped with the venue's time, so a historical clock advances on
    /// one as it does on a balance update.
    #[test]
    fn a_margin_debt_update_reports_its_time_exchange() {
        use rustrade_execution::{
            AccountEvent, AccountEventKind,
            balance::MarginDetails,
            margin_debt::{MarginDebtKind, MarginDebtUpdate},
        };
        use rustrade_instrument::{asset::AssetIndex, exchange::ExchangeIndex};

        let time = DateTime::<Utc>::MIN_UTC + TimeDelta::hours(1);
        let event: EngineEvent<()> =
            EngineEvent::Account(AccountStreamEvent::Item(AccountEvent::new(
                ExchangeIndex(0),
                AccountEventKind::MarginDebt(MarginDebtUpdate::new(
                    AssetIndex(0),
                    None,
                    MarginDetails {
                        borrowed: Decimal::ONE,
                        interest: Decimal::ZERO,
                    },
                    MarginDebtKind::Borrow,
                    "BORROW".into(),
                    time,
                )),
            )));

        assert_eq!(event.time_exchange(), Some(time));
    }

    /// A `TokioClock` reads its start, then moves exactly as far as tokio's clock does.
    #[tokio::test(start_paused = true)]
    async fn a_tokio_clock_moves_with_tokios_clock() {
        let start = DateTime::<Utc>::from_timestamp(1_700_000_000, 0).unwrap();
        let clock = TokioClock::starting_at(start);
        assert_eq!(clock.time(), start);

        tokio::time::advance(std::time::Duration::from_millis(1_500)).await;
        assert_eq!(clock.time(), start + chrono::TimeDelta::milliseconds(1_500));
    }
}
