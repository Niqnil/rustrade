use crate::{EngineEvent, engine::Processor, execution::AccountStreamEvent};
use chrono::{DateTime, Utc};
use rustrade_data::streams::consumer::MarketStreamEvent;
use rustrade_execution::AccountEventKind;
use serde::{Deserialize, Serialize};
use std::{fmt::Debug, sync::Arc};
use tracing::{debug, error, warn};

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
#[derive(Debug, Clone)]
pub struct HistoricalClock {
    inner: Arc<parking_lot::RwLock<HistoricalClockInner>>,
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize)]
struct HistoricalClockInner {
    time_exchange_last: DateTime<Utc>,
}

impl HistoricalClock {
    /// Construct a new `HistoricalClock` using the provided `last_exchange_time` as a seed.
    pub fn new(last_exchange_time: DateTime<Utc>) -> Self {
        Self {
            inner: Arc::new(parking_lot::RwLock::new(HistoricalClockInner {
                time_exchange_last: last_exchange_time,
            })),
        }
    }
}

impl EngineClock for HistoricalClock {
    fn time(&self) -> DateTime<Utc> {
        self.inner.read().time_exchange_last
    }

    fn advance_to(&self, time: DateTime<Utc>) {
        let mut lock = self.inner.write();
        // Monotonic: only advance strictly forwards, so the clock never regresses. An earlier — or
        // equal — `time` is a no-op. This is stricter than `process`'s in-order branch (`>=`), but
        // equivalent in effect: assigning `time_exchange_last` a value it already holds changes
        // nothing now that no wall-clock anchor is re-based alongside it.
        if time > lock.time_exchange_last {
            lock.time_exchange_last = time;
        }
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

        // Obtain lock
        let mut lock = self.inner.write();

        // Input event is more recent
        if time_event_exchange >= lock.time_exchange_last {
            debug!(
                ?event,
                time_exchange_last_current = ?lock.time_exchange_last,
                time_update = ?time_event_exchange,
                "HistoricalClock updating based on input event time_exchange"
            );
            lock.time_exchange_last = time_event_exchange;
            return;
        };

        // Input event is older, so log at varying degrees of severity
        let time_diff_secs = time_event_exchange
            .signed_duration_since(lock.time_exchange_last)
            .num_seconds()
            .abs();

        if time_diff_secs < 1 {
            debug!(
                ?event,
                time_exchange_last_current = ?lock.time_exchange_last,
                time_update = ?time_event_exchange,
                time_diff_secs,
                "HistoricalClock received out-of-order events"
            );
        } else if time_diff_secs < 30 {
            warn!(
                ?event,
                time_exchange_last_current = ?lock.time_exchange_last,
                time_update = ?time_event_exchange,
                time_diff_secs,
                "HistoricalClock received out-of-order events"
            );
        } else {
            error!(
                ?event,
                time_exchange_last_current = ?lock.time_exchange_last,
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
                    // its asset-keyed sibling `BalanceStreamUpdate` (base/quote share the frame's time).
                    Some(update.base.time_exchange)
                }
                AccountEventKind::OrderSnapshot(order) => order.0.state.time_exchange(),
                AccountEventKind::OrderCancelled(response) => response
                    .state
                    .as_ref()
                    .map(|cancelled| cancelled.time_exchange)
                    .ok(),
                AccountEventKind::Trade(trade) => Some(trade.time_exchange),
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
                clock.inner.read().time_exchange_last,
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
        assert_eq!(clock.inner.read().time_exchange_last, effective);
        clock.process(&market_event(intraday_open));
        assert_eq!(clock.inner.read().time_exchange_last, intraday_open);
        clock.process(&market_event(intraday_later));
        assert_eq!(clock.inner.read().time_exchange_last, intraday_later);

        // Out-of-order safety: a midnight split arriving after that day's intraday events
        // must not regress the clock (same monotonic guard as any other event).
        let mut clock = HistoricalClock::new(intraday_later);
        clock.process(&corporate_action_event(effective));
        assert_eq!(
            clock.inner.read().time_exchange_last,
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
        assert_eq!(clock.inner.read().time_exchange_last, later);

        // Advancing to an earlier — or equal — instant is a no-op: the clock never regresses.
        clock.advance_to(base);
        assert_eq!(clock.inner.read().time_exchange_last, later);
        clock.advance_to(later);
        assert_eq!(clock.inner.read().time_exchange_last, later);
    }
}
