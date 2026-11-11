//! The [`ExecutionManager`](crate::execution::manager::ExecutionManager)'s timers for the orders it sent with
//! a [`cancel_after`](rustrade_execution::order::request::RequestOpen::cancel_after).

use fnv::FnvHashMap;
use rustrade_execution::order::{OrderKey, request::OrderRequestOpen};
use rustrade_instrument::{exchange::ExchangeIndex, instrument::InstrumentIndex};
use std::collections::BTreeMap;
use tokio::time::Instant;

/// When each order sent with a `cancel_after` falls due, soonest first.
///
/// Timed on tokio's monotonic clock, as the manager's request timeouts are, so a step of the host's
/// wall clock does not move a deadline, and a test with paused time drives it.
#[derive(Debug, Default)]
pub(crate) struct OrderDeadlines {
    /// Each armed order by when it falls due, and the order it was armed in to break a tie.
    due: BTreeMap<(Instant, u64), OrderKey<ExchangeIndex, InstrumentIndex>>,
    /// Where each armed order sits in `due`, to disarm it.
    armed: FnvHashMap<OrderKey<ExchangeIndex, InstrumentIndex>, (Instant, u64)>,
    /// How many have been armed, for the tie-break.
    sequence: u64,
    /// Set by [`close`](Self::close): nothing more is armed.
    closed: bool,
}

impl OrderDeadlines {
    /// Arm `request`'s `cancel_after`, if it has one, counted from `sent`. Arming an order again
    /// replaces its deadline. A deadline too far off for the clock to hold is never reached, so it
    /// is not armed, and nothing is armed once [`close`](Self::close)d.
    pub(crate) fn arm(
        &mut self,
        request: &OrderRequestOpen<ExchangeIndex, InstrumentIndex>,
        sent: Instant,
    ) {
        if self.closed {
            return;
        }
        let Some(due) = request
            .state
            .cancel_after
            .and_then(|after| sent.checked_add(after))
        else {
            return;
        };
        self.disarm(&request.key);
        let slot = (due, self.sequence);
        self.sequence += 1;
        self.due.insert(slot, request.key.clone());
        self.armed.insert(request.key.clone(), slot);
    }

    /// Stop timing `key`'s order: it ended, so there is nothing left to cancel.
    pub(crate) fn disarm(&mut self, key: &OrderKey<ExchangeIndex, InstrumentIndex>) {
        if let Some(slot) = self.armed.remove(key) {
            self.due.remove(&slot);
        }
    }

    /// When the soonest deadline falls due, or `None` when none is armed.
    pub(crate) fn next(&self) -> Option<Instant> {
        self.due.first_key_value().map(|((due, _), _)| *due)
    }

    /// Take every order whose deadline is at or before `now`, soonest first.
    pub(crate) fn take_due(
        &mut self,
        now: Instant,
    ) -> Vec<OrderKey<ExchangeIndex, InstrumentIndex>> {
        let mut taken = Vec::new();
        while let Some(entry) = self.due.first_entry() {
            if entry.key().0 > now {
                break;
            }
            let key = entry.remove();
            self.armed.remove(&key);
            taken.push(key);
        }
        taken
    }

    /// Drop every deadline and arm no more. The manager is draining: it reads no more requests,
    /// so the engine's cancel could not be sent, and a deadline must not hold the drain open.
    pub(crate) fn close(&mut self) {
        self.closed = true;
        self.due.clear();
        self.armed.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;
    use rustrade_execution::order::{
        OrderKind, TimeInForce,
        id::{ClientOrderId, StrategyId},
        request::RequestOpen,
    };
    use rustrade_instrument::Side;
    use std::time::Duration;

    fn open(
        cid: &str,
        cancel_after: Option<u64>,
    ) -> OrderRequestOpen<ExchangeIndex, InstrumentIndex> {
        let request = RequestOpen::new(
            Side::Buy,
            Some(dec!(1)),
            dec!(1),
            OrderKind::Limit,
            TimeInForce::GoodUntilEndOfDay,
        );
        OrderRequestOpen {
            key: OrderKey::new(
                ExchangeIndex(0),
                InstrumentIndex(0),
                StrategyId::unknown(),
                ClientOrderId::new(cid),
            ),
            state: match cancel_after {
                Some(secs) => request.with_cancel_after(Duration::from_secs(secs)),
                None => request,
            },
        }
    }

    #[test]
    fn deadlines_fall_due_soonest_first_and_only_once() {
        let start = Instant::now();
        let mut deadlines = OrderDeadlines::default();
        deadlines.arm(&open("late", Some(5)), start);
        deadlines.arm(&open("early", Some(2)), start);
        deadlines.arm(&open("none", None), start);

        assert_eq!(deadlines.next(), Some(start + Duration::from_secs(2)));
        assert!(
            deadlines
                .take_due(start + Duration::from_secs(1))
                .is_empty()
        );
        let due = deadlines.take_due(start + Duration::from_secs(5));
        let cids: Vec<_> = due.iter().map(|key| key.cid.0.as_str()).collect();
        assert_eq!(cids, ["early", "late"]);
        assert_eq!(deadlines.next(), None);
        assert!(
            deadlines
                .take_due(start + Duration::from_secs(9))
                .is_empty()
        );
    }

    #[test]
    fn a_disarmed_rearmed_or_closed_order_falls_due_at_most_once() {
        let start = Instant::now();
        let mut deadlines = OrderDeadlines::default();
        deadlines.arm(&open("ended", Some(1)), start);
        deadlines.arm(&open("rearmed", Some(1)), start);
        deadlines.arm(&open("rearmed", Some(3)), start);
        deadlines.disarm(&open("ended", None).key);

        assert_eq!(deadlines.next(), Some(start + Duration::from_secs(3)));
        assert_eq!(deadlines.take_due(start + Duration::from_secs(3)).len(), 1);

        deadlines.arm(&open("closed", Some(1)), start);
        deadlines.close();
        assert_eq!(deadlines.next(), None);
        deadlines.arm(&open("after", Some(1)), start);
        assert_eq!(deadlines.next(), None);
    }

    #[test]
    fn a_deadline_past_what_the_clock_holds_is_not_armed() {
        let mut deadlines = OrderDeadlines::default();
        let mut request = open("forever", None);
        request.state.cancel_after = Some(Duration::MAX);

        deadlines.arm(&request, Instant::now());

        assert_eq!(deadlines.next(), None);
    }
}
