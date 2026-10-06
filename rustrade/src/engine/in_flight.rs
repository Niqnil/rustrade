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
use rustrade_instrument::{exchange::ExchangeIndex, instrument::InstrumentIndex};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tracing::error;

/// How long an order's request may stay in flight on each exchange before the
/// [`Engine`](crate::engine::Engine) flags it with an [`InFlightOverdue`] output.
///
/// An exchange with no deadline is never checked. [`InFlightDeadlines::default`] has none, which
/// turns the check off.
///
/// # Choosing a deadline
/// A deadline must exceed the exchange's `ExecutionManager` `request_timeout`, by which the manager
/// answers every request, plus the time that answer takes to reach the engine. Otherwise orders
/// awaiting an ordinary answer are flagged. [`ExecutionBuilder`](crate::execution::builder::ExecutionBuilder)
/// derives one per live exchange with [`Self::deadline_for_request_timeout`], and exposes them on
/// [`ExecutionBuild`](crate::execution::builder::ExecutionBuild) and
/// [`Execution`](crate::execution::Execution).
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
    pub fn with(mut self, exchange: ExchangeIndex, deadline: Duration) -> Self {
        self.insert(exchange, deadline);
        self
    }

    /// Set the deadline for `exchange`, replacing any it had.
    pub fn insert(&mut self, exchange: ExchangeIndex, deadline: Duration) {
        let index = exchange.index();
        if self.0.len() <= index {
            self.0.resize(index + 1, None);
        }
        self.0[index] = Some(deadline);
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

/// Which request an [`InFlightOverdue`] order is awaiting an answer to.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize)]
pub enum InFlightRequest {
    Open,
    Cancel,
}

/// An order whose request has been in flight longer than its exchange's deadline.
///
/// Emitted as [`EngineOutput::InFlightOverdue`](crate::engine::EngineOutput::InFlightOverdue), once per
/// request: an order flagged while `OpenInFlight` is flagged again only for a later cancel, with
/// that cancel's own deadline. A restarted engine flags again, once, an order it restores already
/// overdue. The engine leaves the order as it is; see the [module docs](self).
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize)]
pub struct InFlightOverdue<ExchangeKey = ExchangeIndex, InstrumentKey = InstrumentIndex> {
    pub key: OrderKey<ExchangeKey, InstrumentKey>,
    pub request: InFlightRequest,
    /// When the engine sent the request, by its `EngineClock`.
    pub time_sent: DateTime<Utc>,
    /// The exchange's deadline the request exceeded.
    pub deadline: Duration,
    /// How long the request had been in flight when the engine flagged it.
    pub elapsed: Duration,
}

/// Decides, on each processed event, which in-flight orders have newly passed their deadline.
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
/// once the clock reaches it. A newly sent request lowers it by the shortest deadline of any
/// exchange, which may be early for its own exchange; the scan then finds nothing and recomputes
/// it exactly.
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub(crate) struct InFlightWatch {
    deadlines: InFlightDeadlines,
    /// When the last scan ran.
    last_scan: Option<DateTime<Utc>>,
    /// The earliest time a scan can find an order newly overdue, or `None` if none can be.
    next_scan: Option<DateTime<Utc>>,
}

impl InFlightWatch {
    pub(crate) fn new(deadlines: InFlightDeadlines) -> Self {
        // Scan on the first event: the engine may start from a state that has orders in flight.
        let next_scan = (!deadlines.is_empty()).then_some(DateTime::<Utc>::MIN_UTC);
        Self {
            deadlines,
            last_scan: None,
            next_scan,
        }
    }

    /// Note that requests were sent at `time_sent`.
    pub(crate) fn on_sent(&mut self, time_sent: DateTime<Utc>) {
        let Some(shortest) = self.deadlines.shortest() else {
            return;
        };
        let due = add(time_sent, shortest);
        self.next_scan = Some(self.next_scan.map_or(due, |next| next.min(due)));
    }

    /// Flag the in-flight orders whose deadline has passed since the last scan, logging an
    /// `error!` for each.
    ///
    /// Overdue orders are returned in instrument order, then by send time and client order id, so
    /// a backtest's audit is deterministic.
    pub(crate) fn check<'a, InstrumentData: 'a>(
        &mut self,
        now: DateTime<Utc>,
        instruments: impl Iterator<Item = &'a InstrumentState<InstrumentData>>,
    ) -> Vec<InFlightOverdue> {
        if self.next_scan.is_none_or(|next| now < next) {
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
                .sort_by(|a, b| (a.time_sent, &a.key.cid).cmp(&(b.time_sent, &b.key.cid)));
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
mod tests {
    use super::*;

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

        let replaced = deadlines.with(ExchangeIndex(2), Duration::from_secs(1));
        assert_eq!(replaced.get(ExchangeIndex(2)), Some(Duration::from_secs(1)));
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
}
