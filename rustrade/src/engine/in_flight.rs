//! Flagging orders whose request has stayed in flight past a deadline.
//!
//! Every open and cancel request the [`Engine`](crate::engine::Engine) sends leaves its order
//! [`OpenInFlight`](rustrade_execution::order::state::OpenInFlight) or
//! [`CancelInFlight`](rustrade_execution::order::state::CancelInFlight) until the venue's answer
//! arrives. The `ExecutionManager` answers every request within its `request_timeout`, so an order
//! still in flight well after that was stranded some other way: the manager task died, an answer
//! was ignored by a state transition, or a bug. While it stays, `has_requests_in_flight` stays
//! true, and in Hedging mode fills that match no order are held back.
//!
//! The engine **flags** such an order and does nothing else. Settling it would be a guess: the
//! venue may hold the order live, and a retired order's later reports are ignored, so a wrong
//! guess would hide a live order. Settling and reconciling are the caller's decision.

use crate::engine::state::{instrument::InstrumentState, order::manager::OrderManager};
use chrono::{DateTime, TimeDelta, Utc};
use rustrade_execution::order::{OrderKey, state::ActiveOrderState};
use rustrade_instrument::exchange::ExchangeId;
use rustrade_instrument::{exchange::ExchangeIndex, instrument::InstrumentIndex};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use thiserror::Error;
use tracing::error;

/// How long an order's request may stay in flight on each exchange before the
/// [`Engine`](crate::engine::Engine) flags it with an [`InFlightOverdue`] output.
///
/// An exchange with no deadline is never checked. [`InFlightDeadlines::default`] has none, which
/// turns the check off.
///
/// Keyed by [`ExchangeIndex`], the position of the exchange in the
/// [`IndexedInstruments`](rustrade_instrument::index::IndexedInstruments) the engine was built
/// from. To key a deadline by [`ExchangeId`], look its index up there first, with
/// `IndexedInstruments::find_exchange_index`.
///
/// # Choosing a deadline
/// A deadline must exceed the exchange's `ExecutionManager` `request_timeout`, by which the manager
/// answers every request, plus the time that answer takes to reach the engine. Otherwise orders
/// awaiting an ordinary answer are flagged. [`ExecutionBuilder`](crate::execution::builder::ExecutionBuilder)
/// derives one per live exchange with [`Self::deadline_for_request_timeout`], and exposes them on
/// [`ExecutionBuild`](crate::execution::builder::ExecutionBuild) and
/// [`Execution`](crate::execution::Execution).
/// [`SystemBuilder::in_flight_deadline`](crate::system::builder::SystemBuilder::in_flight_deadline)
/// overrides one exchange's deadline by [`ExchangeId`].
///
/// Deadlines are measured on the engine's `EngineClock`. In a backtest that is simulated time, so
/// a deadline is only meaningful where the simulated venue answers within a bounded simulated
/// time. That is why the builders give mock venues none.
#[derive(Debug, Clone, Default, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct InFlightDeadlines(
    /// Indexed by [`ExchangeIndex`].
    Vec<Option<Duration>>,
);

impl InFlightDeadlines {
    /// What [`Self::deadline_for_request_timeout`] adds to an
    /// exchange's `request_timeout`, for the manager's answer to reach the engine.
    pub const REQUEST_TIMEOUT_MARGIN: Duration = Duration::from_secs(5);

    /// The deadline for an exchange whose `ExecutionManager` has this `request_timeout`:
    /// `request_timeout` plus [`Self::REQUEST_TIMEOUT_MARGIN`].
    pub fn deadline_for_request_timeout(request_timeout: Duration) -> Duration {
        request_timeout.saturating_add(Self::REQUEST_TIMEOUT_MARGIN)
    }

    /// Set the deadline for `exchange`, replacing any it had.
    ///
    /// # Panics
    /// If `deadline` is zero. Such a request would fall due at the moment it is sent, which a
    /// check made at that same moment has already passed, so it would never be flagged.
    pub fn with(mut self, exchange: ExchangeIndex, deadline: Duration) -> Self {
        self.insert(exchange, deadline);
        self
    }

    /// Set the deadline for `exchange`, replacing any it had.
    ///
    /// # Panics
    /// If `deadline` is zero; see [`Self::with`].
    pub fn insert(&mut self, exchange: ExchangeIndex, deadline: Duration) {
        assert!(
            !deadline.is_zero(),
            "InFlightDeadlines: the deadline for {exchange} must be non-zero"
        );
        let index = exchange.index();
        if self.0.len() <= index {
            self.0.resize(index + 1, None);
        }
        self.0[index] = Some(deadline);
    }

    /// Remove the deadline for `exchange`, so it is never checked, returning the one it had.
    pub fn remove(&mut self, exchange: ExchangeIndex) -> Option<Duration> {
        let removed = self.0.get_mut(exchange.index()).and_then(Option::take);
        // Drop trailing empty slots, so equal deadlines compare and hash equal however built.
        while matches!(self.0.last(), Some(None)) {
            self.0.pop();
        }
        removed
    }

    /// The deadline for `exchange`, if it has one.
    pub fn get(&self, exchange: ExchangeIndex) -> Option<Duration> {
        self.0.get(exchange.index()).copied().flatten()
    }

    /// Whether no exchange has a deadline, so nothing is ever checked.
    pub fn is_empty(&self) -> bool {
        self.0.iter().all(Option::is_none)
    }

    fn shortest(&self) -> Option<Duration> {
        self.0.iter().flatten().min().copied()
    }
}

impl FromIterator<(ExchangeIndex, Duration)> for InFlightDeadlines {
    fn from_iter<Iter: IntoIterator<Item = (ExchangeIndex, Duration)>>(iter: Iter) -> Self {
        iter.into_iter()
            .fold(Self::default(), |deadlines, (exchange, deadline)| {
                deadlines.with(exchange, deadline)
            })
    }
}

/// Why an in-flight deadline override was rejected.
///
/// Returned by [`SystemBuilder::build`](crate::system::builder::SystemBuilder::build) for an
/// override set with
/// [`SystemBuilder::in_flight_deadline`](crate::system::builder::SystemBuilder::in_flight_deadline).
/// Either would leave the override silently doing nothing.
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize, Error)]
pub enum InFlightDeadlineError {
    /// The exchange has no execution client in the system, so no order request is ever sent to it.
    #[error("{0} has no execution client, so an in-flight deadline for it would never apply")]
    NoExecution(ExchangeId),
    /// The deadline is zero, so it could never be passed; see [`InFlightDeadlines::with`].
    #[error("the in-flight deadline for {0} is zero, so an order could never be flagged")]
    Zero(ExchangeId),
}

/// Which request an [`InFlightOverdue`] order is awaiting an answer to.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize)]
pub enum InFlightRequest {
    /// The open request: the order is
    /// [`OpenInFlight`](rustrade_execution::order::state::OpenInFlight).
    Open,
    /// A cancel request: the order is
    /// [`CancelInFlight`](rustrade_execution::order::state::CancelInFlight).
    Cancel,
}

/// An order whose request has been in flight longer than its exchange's deadline.
///
/// Emitted as [`EngineOutput::InFlightOverdue`](crate::engine::EngineOutput::InFlightOverdue), once per
/// request: an order flagged while `OpenInFlight` is flagged again only for a later cancel, with
/// that cancel's own deadline. A cancel resent while one is in flight keeps the first one's send
/// time. An open sent under a client order id the engine already tracks replaces that order,
/// starting its time afresh. A restarted engine flags again, once, an order it restores already
/// overdue. The engine leaves the order as it is; see the [module docs](self).
///
/// `#[non_exhaustive]` so that fields can be added without a breaking change.
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize)]
#[non_exhaustive]
pub struct InFlightOverdue<ExchangeKey = ExchangeIndex, InstrumentKey = InstrumentIndex> {
    /// The order the request is for.
    pub key: OrderKey<ExchangeKey, InstrumentKey>,
    /// Which request is awaiting an answer.
    pub request: InFlightRequest,
    /// When the engine sent the request, by its `EngineClock`.
    pub time_sent: DateTime<Utc>,
    /// The exchange's deadline the request exceeded.
    pub deadline: Duration,
    /// How long the request had been in flight when the engine flagged it.
    pub elapsed: Duration,
}

/// Decides, after each event the engine processes, which in-flight orders have newly passed their
/// deadline.
///
/// The engine skips the check on a `Shutdown` event, and on a `Command` whose action hit an
/// unrecoverable error; it checks again on the next event it processes.
///
/// # Once per request, without state on the order
/// A check flags the orders whose deadline falls after the previous check and at or before now.
/// One whose deadline fell at or before the previous check was flagged by it, so none is flagged
/// twice, and nothing is stored on the order. The watch lives on the `Engine`, not in
/// `EngineState`, so a restored engine starts with no previous check, which is what flags an
/// already-overdue order once more.
///
/// The cost is one assumption: the clock does not step back by more than a deadline. Live, it is
/// the wall clock. An order sent during a backward step larger than its deadline can have a
/// deadline at or before the previous check, and is then never flagged. A backtest's clock is
/// monotonic.
///
/// # Cheap when nothing is due
/// The watch keeps the earliest time an in-flight order can fall due, and scans the orders only
/// once the clock reaches it. With nothing in flight it does not read the clock at all. A newly
/// sent request lowers it by the shortest deadline of any exchange, which may be early for its own
/// exchange; the scan then finds nothing and recomputes it exactly.
///
/// This relies on every in-flight order having been sent by the engine, which calls
/// [`Self::on_sent`], or being in the state the engine started from, which the first check scans.
/// An order that becomes in flight any other way, such as a snapshot carrying an in-flight state,
/// is found only by a scan some other order prompts, so with nothing else in flight it may never
/// be flagged.
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub(crate) struct InFlightWatch {
    deadlines: InFlightDeadlines,
    /// The shortest of `deadlines`, or `None` if there are none.
    shortest: Option<Duration>,
    /// When the last scan ran.
    last_scan: Option<DateTime<Utc>>,
    /// The earliest time a scan can find an order newly overdue, or `None` if none can be.
    next_scan: Option<DateTime<Utc>>,
}

impl InFlightWatch {
    pub(crate) fn new(deadlines: InFlightDeadlines) -> Self {
        // Scan on the first event: the engine may start from a state that has orders in flight.
        let shortest = deadlines.shortest();
        let next_scan = shortest.map(|_| DateTime::<Utc>::MIN_UTC);
        Self {
            deadlines,
            shortest,
            last_scan: None,
            next_scan,
        }
    }

    /// The deadlines this watch checks against.
    pub(crate) fn deadlines(&self) -> &InFlightDeadlines {
        &self.deadlines
    }

    /// Note that requests were sent at `time_sent`.
    pub(crate) fn on_sent(&mut self, time_sent: DateTime<Utc>) {
        let Some(shortest) = self.shortest else {
            return;
        };
        let due = add(time_sent, shortest);
        self.next_scan = Some(self.next_scan.map_or(due, |next| next.min(due)));
    }

    /// Flag the in-flight orders whose deadline has passed since the last scan, logging an
    /// `error!` for each.
    ///
    /// `now` is called only if an order may be due, so an engine with nothing in flight never
    /// reads its clock here.
    ///
    /// Overdue orders are returned in instrument order, then by send time and client order id, so
    /// a backtest's audit is deterministic.
    pub(crate) fn check<'a, InstrumentData: 'a>(
        &mut self,
        now: impl FnOnce() -> DateTime<Utc>,
        instruments: impl Iterator<Item = &'a InstrumentState<InstrumentData>>,
    ) -> Vec<InFlightOverdue> {
        let Some(next_scan) = self.next_scan else {
            return Vec::new();
        };
        let now = now();
        if now < next_scan {
            return Vec::new();
        }

        let last_scan = self.last_scan;
        let mut next_scan = None::<DateTime<Utc>>;
        let mut overdue = Vec::new();

        for instrument in instruments {
            let first = overdue.len();
            for order in instrument.orders.orders() {
                let request = match &order.state {
                    ActiveOrderState::OpenInFlight(_) => InFlightRequest::Open,
                    ActiveOrderState::CancelInFlight(_) => InFlightRequest::Cancel,
                    ActiveOrderState::Open(_) => continue,
                };
                let (Some(time_sent), Some(deadline)) = (
                    order.state.time_sent(),
                    self.deadlines.get(order.key.exchange),
                ) else {
                    continue;
                };

                let due = add(time_sent, deadline);
                if due > now {
                    next_scan = Some(next_scan.map_or(due, |next| next.min(due)));
                } else if last_scan.is_none_or(|last| due > last) {
                    overdue.push(InFlightOverdue {
                        key: order.key.clone(),
                        request,
                        time_sent,
                        deadline,
                        elapsed: (now - time_sent).to_std().unwrap_or_default(),
                    });
                }
            }
            overdue[first..]
                .sort_unstable_by(|a, b| (a.time_sent, &a.key.cid).cmp(&(b.time_sent, &b.key.cid)));
        }

        self.last_scan = Some(last_scan.map_or(now, |last| last.max(now)));
        self.next_scan = next_scan;

        for order in &overdue {
            error!(
                exchange = %order.key.exchange,
                instrument = %order.key.instrument,
                strategy = %order.key.strategy,
                cid = %order.key.cid,
                request = ?order.request,
                time_sent = %order.time_sent,
                deadline = ?order.deadline,
                elapsed = ?order.elapsed,
                "Engine flagged an order whose request has been in flight past its deadline - leaving it in flight"
            );
        }

        overdue
    }
}

fn add(time: DateTime<Utc>, duration: Duration) -> DateTime<Utc> {
    TimeDelta::from_std(duration)
        .ok()
        .and_then(|delta| time.checked_add_signed(delta))
        .unwrap_or(DateTime::<Utc>::MAX_UTC)
}

#[cfg(test)]
#[allow(clippy::expect_used)] // test code: panics acceptable
mod tests {
    use super::*;
    use crate::engine::state::EngineState;
    use rust_decimal_macros::dec;
    use rustrade_execution::order::{
        Order, OrderKind, TimeInForce,
        id::{ClientOrderId, StrategyId},
        state::{CancelInFlight, OpenInFlight},
    };
    use rustrade_instrument::{
        Side, exchange::ExchangeId, index::IndexedInstruments,
        test_utils::instrument as test_instrument,
    };

    const T: DateTime<Utc> = DateTime::<Utc>::UNIX_EPOCH;

    fn at(secs: i64) -> DateTime<Utc> {
        T + TimeDelta::seconds(secs)
    }

    /// Instrument 0 on exchange 0, whose deadline is 10 s, and instrument 1 on exchange 1, whose
    /// deadline is 60 s.
    fn two_exchanges() -> (EngineState<(), ()>, InFlightDeadlines) {
        let instruments = IndexedInstruments::new([
            test_instrument(ExchangeId::BinanceSpot, "btc", "usdt"),
            test_instrument(ExchangeId::Kraken, "eth", "usd"),
        ]);
        assert_eq!(
            instruments.find_exchange_index(ExchangeId::Kraken),
            Ok(ExchangeIndex(1))
        );
        let state = EngineState::builder(&instruments, (), |_| ())
            .time_engine_start(T)
            .build();
        let deadlines = InFlightDeadlines::default()
            .with(ExchangeIndex(0), Duration::from_secs(10))
            .with(ExchangeIndex(1), Duration::from_secs(60));
        (state, deadlines)
    }

    /// Record an order on `instrument` (which trades on the exchange of the same index) as the
    /// engine would, telling the watch of its send.
    fn send(
        state: &mut EngineState<(), ()>,
        watch: &mut InFlightWatch,
        instrument: usize,
        cid: &str,
        in_flight: ActiveOrderState,
    ) {
        let time_sent = in_flight.time_sent().expect("an in-flight state");
        let cid = ClientOrderId::new(cid);
        let order = Order {
            key: OrderKey {
                exchange: ExchangeIndex(instrument),
                instrument: InstrumentIndex(instrument),
                strategy: StrategyId::new("strategy"),
                cid: cid.clone(),
            },
            side: Side::Buy,
            price: Some(dec!(1)),
            quantity: dec!(1),
            kind: OrderKind::Limit,
            time_in_force: TimeInForce::GoodUntilCancelled { post_only: false },
            state: in_flight,
        };
        state
            .instruments
            .0
            .get_index_mut(instrument)
            .expect("instrument exists")
            .1
            .orders
            .0
            .insert(cid, order);
        watch.on_sent(time_sent);
    }

    fn open_sent(secs: i64) -> ActiveOrderState {
        ActiveOrderState::OpenInFlight(OpenInFlight::new(at(secs)))
    }

    fn check(
        watch: &mut InFlightWatch,
        state: &EngineState<(), ()>,
        now: DateTime<Utc>,
    ) -> Vec<(String, InFlightRequest)> {
        watch
            .check(|| now, state.instruments.0.values())
            .into_iter()
            .map(|overdue| (overdue.key.cid.0.to_string(), overdue.request))
            .collect()
    }

    #[test]
    fn deadlines_are_set_and_read_per_exchange() {
        let deadlines = InFlightDeadlines::from_iter([
            (ExchangeIndex(2), Duration::from_secs(7)),
            (ExchangeIndex(0), Duration::from_secs(3)),
        ]);

        assert_eq!(
            deadlines.get(ExchangeIndex(0)),
            Some(Duration::from_secs(3))
        );
        assert_eq!(deadlines.get(ExchangeIndex(1)), None);
        assert_eq!(
            deadlines.get(ExchangeIndex(2)),
            Some(Duration::from_secs(7))
        );
        assert_eq!(deadlines.get(ExchangeIndex(9)), None);
        assert_eq!(deadlines.shortest(), Some(Duration::from_secs(3)));
        assert!(!deadlines.is_empty());
        assert!(InFlightDeadlines::default().is_empty());

        let mut replaced = deadlines.with(ExchangeIndex(2), Duration::from_secs(1));
        assert_eq!(replaced.get(ExchangeIndex(2)), Some(Duration::from_secs(1)));

        assert_eq!(
            replaced.remove(ExchangeIndex(2)),
            Some(Duration::from_secs(1))
        );
        assert_eq!(replaced.get(ExchangeIndex(2)), None);
        assert_eq!(replaced.remove(ExchangeIndex(2)), None);
        assert_eq!(replaced.remove(ExchangeIndex(9)), None, "beyond the end");
        assert_eq!(replaced.shortest(), Some(Duration::from_secs(3)));
        assert_eq!(
            replaced,
            InFlightDeadlines::default().with(ExchangeIndex(0), Duration::from_secs(3)),
            "equal to the same deadlines built directly"
        );
    }

    #[test]
    fn deadline_for_request_timeout_adds_the_margin() {
        assert_eq!(
            InFlightDeadlines::deadline_for_request_timeout(Duration::from_secs(10)),
            Duration::from_secs(10) + InFlightDeadlines::REQUEST_TIMEOUT_MARGIN
        );
        assert_eq!(
            InFlightDeadlines::deadline_for_request_timeout(Duration::MAX),
            Duration::MAX
        );
    }

    #[test]
    fn a_watch_without_deadlines_never_scans() {
        let watch = InFlightWatch::new(InFlightDeadlines::default());
        assert_eq!(watch.next_scan, None);

        let mut watch = watch;
        watch.on_sent(DateTime::<Utc>::MIN_UTC);
        assert_eq!(watch.next_scan, None);
    }

    #[test]
    fn an_order_due_exactly_now_is_flagged_and_not_again_at_the_same_time() {
        let (mut state, deadlines) = two_exchanges();
        let mut watch = InFlightWatch::new(deadlines);
        send(&mut state, &mut watch, 0, "a", open_sent(0));

        assert!(check(&mut watch, &state, at(9)).is_empty());
        assert_eq!(
            check(&mut watch, &state, at(10)),
            [("a".to_owned(), InFlightRequest::Open)]
        );
        assert!(
            check(&mut watch, &state, at(10)).is_empty(),
            "due at the last scan, so that scan flagged it"
        );
        assert!(check(&mut watch, &state, at(100)).is_empty());
        assert_eq!(watch.next_scan, None, "nothing left that can fall due");
    }

    #[test]
    fn each_exchange_uses_its_own_deadline() {
        let (mut state, deadlines) = two_exchanges();
        let mut watch = InFlightWatch::new(deadlines);
        send(&mut state, &mut watch, 0, "fast", open_sent(0));
        send(
            &mut state,
            &mut watch,
            1,
            "slow",
            ActiveOrderState::CancelInFlight(CancelInFlight {
                order: None,
                time_sent: at(0),
            }),
        );

        assert_eq!(
            check(&mut watch, &state, at(10)),
            [("fast".to_owned(), InFlightRequest::Open)]
        );
        assert_eq!(watch.next_scan, Some(at(60)));
        assert!(check(&mut watch, &state, at(59)).is_empty());
        assert_eq!(
            check(&mut watch, &state, at(60)),
            [("slow".to_owned(), InFlightRequest::Cancel)]
        );
    }

    #[test]
    fn overdue_orders_come_by_instrument_then_time_sent_then_client_order_id() {
        let (mut state, deadlines) = two_exchanges();
        let mut watch = InFlightWatch::new(deadlines);
        send(&mut state, &mut watch, 1, "a", open_sent(0));
        for cid in ["e", "c", "d"] {
            send(&mut state, &mut watch, 0, cid, open_sent(5));
        }
        send(&mut state, &mut watch, 0, "z", open_sent(1));

        let overdue = check(&mut watch, &state, at(100));
        let cids: Vec<&str> = overdue.iter().map(|(cid, _)| cid.as_str()).collect();
        assert_eq!(cids, ["z", "c", "d", "e", "a"]);
    }

    /// The documented cost of firing once by time window: an order sent during a backward clock
    /// step falls due at or before the last scan only if the step exceeds its deadline.
    #[test]
    fn a_backward_clock_step_misses_only_an_order_it_moves_past_the_last_scan() {
        let (mut state, deadlines) = two_exchanges();
        let mut watch = InFlightWatch::new(deadlines);
        send(&mut state, &mut watch, 0, "before", open_sent(100));
        assert!(check(&mut watch, &state, at(100)).is_empty());

        // Stepped back 50 s, more than the 10 s deadline: due at 60, before the scan at 100.
        send(&mut state, &mut watch, 0, "missed", open_sent(50));
        // Stepped back 5 s, less than the deadline: due at 105, after the scan at 100.
        send(&mut state, &mut watch, 0, "caught", open_sent(95));

        assert!(check(&mut watch, &state, at(70)).is_empty());
        assert_eq!(
            check(&mut watch, &state, at(105)),
            [("caught".to_owned(), InFlightRequest::Open)]
        );
        assert_eq!(
            check(&mut watch, &state, at(110)),
            [("before".to_owned(), InFlightRequest::Open)]
        );
        assert!(check(&mut watch, &state, at(1_000)).is_empty());
    }

    #[test]
    fn a_watch_with_nothing_due_does_not_read_the_clock() {
        let (state, deadlines) = two_exchanges();
        let mut watch = InFlightWatch::new(deadlines);
        assert!(check(&mut watch, &state, at(0)).is_empty(), "first scan");

        let overdue = watch.check(
            || -> DateTime<Utc> { panic!("the clock was read with nothing in flight") },
            state.instruments.0.values(),
        );
        assert!(overdue.is_empty());
    }

    #[test]
    #[should_panic(expected = "must be non-zero")]
    fn a_zero_deadline_is_rejected() {
        let _ = InFlightDeadlines::default().with(ExchangeIndex(0), Duration::ZERO);
    }
}
