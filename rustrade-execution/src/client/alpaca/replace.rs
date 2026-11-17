//! What the Alpaca account stream remembers of the orders Alpaca replaces, so that it reports a
//! replace as one order going on under new ids.
//!
//! Alpaca carries out a replace (`PATCH /v2/orders/{id}`) as a new order, with a new order id and
//! a new client order id. How its `trade_updates` events report it depends on the asset class.
//! Observed for a crypto order, three events, in this order:
//!
//! 1. `pending_replace` on the order replaced, naming its replacement (`replaced_by`);
//! 2. the replacement's first event (`accepted`), naming the order it replaces (`replaces`);
//! 3. `replaced` on the order replaced, ending it.
//!
//! Observed for an equity order, in regular hours, only the third: `replaced` on the order
//! replaced. The replacement sends nothing until its own next change, such as a fill or a cancel.
//!
//! The stream links each report of a live replacement to the order it replaces
//! ([`OrderReport::replaces`](crate::order::report::OrderReport::replaces)), naming its order id,
//! and its client order id when a `pending_replace` or a `replaced` gave it. [`ReplaceTracker`]
//! supplies that client order id, and makes sure the end of the order replaced is not reported
//! before its replacement, as the report's producer obligations require, whatever order Alpaca
//! sends them in. When a `replaced` arrives before any event of its replacement, the tracker asks
//! its owner to read the replacement over REST ([`ReplaceTracker::take_reads`]), and reports what
//! that read finds as the replacement's first report
//! ([`ReplaceTracker::process_read`]).

use crate::{
    AccountEventKind, UnindexedAccountEvent,
    order::{
        id::{ClientOrderId, OrderId},
        report::{Replaces, UnindexedOrderReport},
        state::{ActiveOrderState, OrderState},
    },
};
use lru::LruCache;
use rust_decimal::Decimal;
use smol_str::SmolStr;
use std::{collections::VecDeque, num::NonZeroUsize, time::Duration};
use tokio::time::Instant;
use tracing::{debug, warn};

/// How long the end of an order replaced is held for its replacement's first report, before it
/// is reported anyway.
///
/// Observed for crypto, the replacement's event arrives first, about a millisecond before, so
/// nothing is held. For an equity, the end is held while the replacement is read over REST. Once
/// the bound passes, because that read failed or was slow, or no read was asked for, the end is
/// reported with a warning, as the order replaced has ended whether or not its replacement was
/// seen.
pub(super) const REPLACED_HOLD: Duration = Duration::from_secs(5);

/// How many replaces [`ReplaceTracker`] remembers each of its facts for, before it forgets the
/// least recently used, and how many ends it holds at once.
///
/// A fact is needed only between a replace's events, which arrive within milliseconds of each
/// other, so the bound is far above what a stream needs at once. Each entry is a few ids.
const TRACKED_REPLACES: NonZeroUsize = match NonZeroUsize::new(1_024) {
    Some(capacity) => capacity,
    None => panic!("a non-zero literal"),
};

/// What a `trade_updates` event says about a replace, read before it is converted.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct ReplaceFacts {
    /// Which of a replace's events this is, if any.
    pub(super) event: ReplaceEvent,
    /// The order id of the event's order.
    pub(super) id: SmolStr,
    /// The client order id the event's order is reported under.
    pub(super) cid: ClientOrderId,
    /// The order id of the order the event's order replaces, if it replaces one.
    pub(super) replaces: Option<SmolStr>,
    /// The order id of the order replacing the event's order, if one is.
    pub(super) replaced_by: Option<SmolStr>,
    /// What the event's order has filled, if it says.
    pub(super) filled: Option<Decimal>,
}

/// The events of a replace that [`ReplaceTracker`] acts on, beside every event of a replacement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ReplaceEvent {
    /// `pending_replace`: the event's order is being replaced.
    PendingReplace,
    /// `replaced`: the event's order was replaced, and has ended.
    Replaced,
    /// Any other event.
    Other,
}

/// What [`ReplaceTracker`] learned from a `pending_replace`, or from a `replaced` that came before
/// any event of its replacement, by the replacement's order id.
#[derive(Debug)]
struct Pending {
    /// The order id of the order replaced.
    id: SmolStr,
    /// The client order id of the order replaced.
    cid: ClientOrderId,
    /// What the order replaced had filled, if the event said.
    filled: Option<Decimal>,
    /// Whether a report of the replacement has been checked against `filled`.
    checked: bool,
}

/// The end of an order replaced, held for its replacement's first event.
#[derive(Debug)]
struct Held {
    /// The order id of the order replaced.
    id: SmolStr,
    /// The order id of its replacement.
    replaced_by: SmolStr,
    /// The events reporting the end.
    events: Vec<UnindexedAccountEvent>,
    /// When it is reported anyway.
    deadline: Instant,
}

/// What one Alpaca account stream remembers of the replaces it has seen, so that it reports
/// them as the module docs describe. Owned by the stream's task, and told the time by its caller.
///
/// - **Old client order id.** A `pending_replace` names the replacement's order id, and so does a
///   `replaced`. A live report of that replacement is linked to the order replaced by its client
///   order id too, which spares a reader the search by order id, and finds an order whose order id
///   it has not learned yet.
/// - **Reads.** A `replaced` that arrives before any report of its replacement, as Alpaca sends
///   an equity's replace, asks for the replacement to be read over REST: its owner takes the order
///   ids to read ([`take_reads`](Self::take_reads)), and hands each read back
///   ([`process_read`](Self::process_read)). A live replacement read is reported as its first
///   report, which releases the end held for it. A read that arrives after a report of the
///   replacement from the stream is dropped, as it may be older. A replacement replaced again
///   before its read returns may not be reported: a read that finds it ended reports nothing.
/// - **Fills.** The replacement is reported with the fill Alpaca states for it. Alpaca is taken to
///   count the fills of the order replaced in it, which was not observed for a partly filled
///   order: a replacement first reported with less filled than its `pending_replace` stated for
///   the order replaced is logged at `warn`, naming both.
/// - **The end of the order replaced** is reported once the replacement's first report, of any
///   kind, from the stream or a read, has been. Until then it is held, for at most
///   [`REPLACED_HOLD`]. An end held when the stream disconnects is dropped, and so are the reads
///   not yet taken ([`drop_held`](Self::drop_held)): the order is then still held
///   as live, so the reconnect's check of the orders that ended while disconnected reports it.
///   The hold orders the stream's own events only: that check runs beside the stream, so it can
///   report an order whose end is held as ended before its replacement's first event.
///
/// Each fact is remembered for the last [`TRACKED_REPLACES`] replaces, and as many ends are held
/// at once; past that, the end held longest is reported at once, with a warning.
///
/// # Caller obligations
///
/// The `now` passed to [`process`](Self::process) and [`expire`](Self::expire) must not go
/// backwards: pass a monotonic clock's reading. Ends are held in deadline order, and only
/// the earliest is checked, so an earlier `now` would hold an end behind a later deadline.
#[derive(Debug)]
pub(super) struct ReplaceTracker {
    /// What each `pending_replace` said, by the replacement's order id.
    pending: LruCache<SmolStr, Pending>,
    /// The order ids of the replacements whose events have been reported.
    reported: LruCache<SmolStr, ()>,
    /// Ends held, oldest first, which is also their deadlines' order.
    held: VecDeque<Held>,
    /// The order ids of replacements to read over REST, not yet taken by the owner.
    reads: Vec<SmolStr>,
}

impl Default for ReplaceTracker {
    fn default() -> Self {
        Self {
            pending: LruCache::new(TRACKED_REPLACES),
            reported: LruCache::new(TRACKED_REPLACES),
            held: VecDeque::new(),
            reads: Vec::new(),
        }
    }
}

impl ReplaceTracker {
    /// The events to report now for one `trade_updates` event, which `facts` describes and
    /// converted to `events`, in the order to report them. At `now`.
    ///
    /// The event's own events come first, unless they are an end held for a replacement not yet
    /// reported. Ends held for the event's order, as the replacement they wait for, follow.
    pub(super) fn process(
        &mut self,
        facts: ReplaceFacts,
        mut events: Vec<UnindexedAccountEvent>,
        now: Instant,
    ) -> Vec<UnindexedAccountEvent> {
        let ReplaceFacts {
            event,
            id,
            cid,
            replaces,
            replaced_by,
            filled,
        } = facts;
        if event == ReplaceEvent::PendingReplace
            && let Some(replaced_by) = replaced_by.clone()
        {
            self.pending.put(
                replaced_by,
                Pending {
                    id: id.clone(),
                    cid: cid.clone(),
                    filled,
                    checked: false,
                },
            );
        }
        if let Some(replaced) = &replaces {
            // Whether the fill restarted is logged; the report goes out as Alpaca stated it.
            let _restarted = self.link(&id, &cid, replaced, filled, &mut events);
        }
        let mut out = match (event, replaced_by) {
            (ReplaceEvent::Replaced, Some(replaced_by))
                if !events.is_empty() && !self.reported.contains(&replaced_by) =>
            {
                // No `pending_replace` came first, as for an equity: the `replaced` gives the old
                // client order id instead.
                if !self.pending.contains(&replaced_by) {
                    self.pending.put(
                        replaced_by.clone(),
                        Pending {
                            id: id.clone(),
                            cid,
                            filled,
                            checked: false,
                        },
                    );
                }
                // A second `replaced` for an end already held has its read asked for already.
                if !self.held.iter().any(|held| held.replaced_by == replaced_by) {
                    self.reads.push(replaced_by.clone());
                }
                self.hold(id.clone(), replaced_by, events, now)
            }
            _ => events,
        };
        if replaces.is_some() {
            self.reported.put(id.clone(), ());
            self.release(&id, &mut out);
        }
        out
    }

    /// The events to report now for a REST read of a replacement asked for by
    /// [`take_reads`](Self::take_reads), which `facts` describes and converted to `events`: as
    /// [`process`](Self::process) reports them, or nothing when the stream has reported the
    /// replacement already, as the read may be older than that report.
    ///
    /// A read that arrives after the end it was asked for was reported without it, because its
    /// hold ran out, is still reported, so the replacement is not lost.
    ///
    /// A read that does not name the order it replaces is linked to the order whose `replaced`
    /// asked for it, which the tracker remembers, so its end is released all the same.
    pub(super) fn process_read(
        &mut self,
        mut facts: ReplaceFacts,
        mut events: Vec<UnindexedAccountEvent>,
        now: Instant,
    ) -> Vec<UnindexedAccountEvent> {
        if self.reported.contains(&facts.id) {
            debug!(
                id = %facts.id,
                "Alpaca WS: the stream reported a replacement before its REST read returned - dropping the read"
            );
            return Vec::new();
        }
        if facts.replaces.is_none()
            && let Some(pending) = self.pending.peek(&facts.id)
        {
            let replaced = pending.id.clone();
            debug!(
                id = %facts.id,
                replaced_id = %replaced,
                "Alpaca WS: a replacement's REST read names no order it replaces - linking it to the one whose replace asked for the read"
            );
            for report in live_reports(&mut events) {
                report
                    .replaces
                    .get_or_insert_with(|| Replaces::new(OrderId(replaced.clone())));
            }
            facts.replaces = Some(replaced);
        }
        self.process(facts, events, now)
    }

    /// The order ids of the replacements to read over REST, each once per end held for it, and
    /// each to be handed back to [`process_read`](Self::process_read). Asked for by a `replaced` that arrived before any
    /// report of its replacement.
    pub(super) fn take_reads(&mut self) -> Vec<SmolStr> {
        std::mem::take(&mut self.reads)
    }

    /// Ends held whose deadline is at or before `now`, oldest first, logged at `warn` as reported
    /// without their replacement.
    pub(super) fn expire(&mut self, now: Instant) -> Vec<UnindexedAccountEvent> {
        let mut out = Vec::new();
        while self.held.front().is_some_and(|held| held.deadline <= now) {
            if let Some(held) = self.held.pop_front() {
                warn!(
                    replaced_id = %held.id,
                    replaced_by = %held.replaced_by,
                    hold = ?REPLACED_HOLD,
                    "Alpaca WS: no event of the order replacing this one arrived in time - reporting the replaced order's end without it"
                );
                out.extend(held.events);
            }
        }
        out
    }

    /// When the end held longest is due, if any is held.
    pub(super) fn next_deadline(&self) -> Option<Instant> {
        self.held.front().map(|held| held.deadline)
    }

    /// Drop every end held, and every read not yet taken, for a stream that disconnected,
    /// returning how many ends were held.
    pub(super) fn drop_held(&mut self) -> usize {
        let dropped = self.held.len();
        self.held.clear();
        self.reads.clear();
        dropped
    }

    /// Link the live reports among `events`, of the replacement `id` under `cid`, to the order
    /// they replace by its client order id too, when a `pending_replace` of `replaced` gave it,
    /// and check the first against what that order had filled. Whether that check found the
    /// replacement less filled ([`fill_restarted`]), which it logs.
    fn link(
        &mut self,
        id: &SmolStr,
        cid: &ClientOrderId,
        replaced: &SmolStr,
        filled: Option<Decimal>,
        events: &mut [UnindexedAccountEvent],
    ) -> bool {
        let Some(pending) = self.pending.get_mut(id) else {
            return false;
        };
        if &pending.id != replaced {
            return false;
        }
        let mut linked = false;
        for report in live_reports(events) {
            let Some(link) = report.replaces.take() else {
                continue;
            };
            linked = true;
            report.replaces = Some(if &pending.cid == cid {
                link
            } else {
                link.with_cid(pending.cid.clone())
            });
        }
        if !linked || pending.checked {
            return false;
        }
        pending.checked = true;
        let restarted = fill_restarted(pending.filled, filled);
        if restarted {
            warn!(
                id = %id,
                replaced_id = %replaced,
                replaced_filled = ?pending.filled,
                filled = ?filled,
                "Alpaca WS: a replacement reports less filled than the order it replaces had - Alpaca may not count the replaced order's fills in it, so its filled quantity understates the order's"
            );
        }
        restarted
    }

    /// Hold `events`, the end of `id`, for `replaced_by`'s first event, returning what to report
    /// at once instead: nothing, or the end held longest when the bound is reached.
    fn hold(
        &mut self,
        id: SmolStr,
        replaced_by: SmolStr,
        events: Vec<UnindexedAccountEvent>,
        now: Instant,
    ) -> Vec<UnindexedAccountEvent> {
        let deadline = now + REPLACED_HOLD;
        debug_assert!(
            self.held
                .back()
                .is_none_or(|last| last.deadline <= deadline),
            "the time told a ReplaceTracker went backwards"
        );
        let out = if self.held.len() >= TRACKED_REPLACES.get()
            && let Some(oldest) = self.held.pop_front()
        {
            warn!(
                replaced_id = %oldest.id,
                replaced_by = %oldest.replaced_by,
                held = TRACKED_REPLACES.get(),
                "Alpaca WS: too many replaced orders' ends held for their replacements - reporting the oldest without it"
            );
            oldest.events
        } else {
            Vec::new()
        };
        self.held.push_back(Held {
            id,
            replaced_by,
            events,
            deadline,
        });
        out
    }

    /// Append to `out` every end held for the replacement `id`, oldest first.
    fn release(&mut self, id: &SmolStr, out: &mut Vec<UnindexedAccountEvent>) {
        if !self.held.iter().any(|held| &held.replaced_by == id) {
            return;
        }
        let (due, kept): (VecDeque<_>, VecDeque<_>) = std::mem::take(&mut self.held)
            .into_iter()
            .partition(|held| &held.replaced_by == id);
        self.held = kept;
        out.extend(due.into_iter().flat_map(|held| held.events));
    }
}

/// The reports among `events` that report an order live: the ones a replacement's link goes on.
fn live_reports(
    events: &mut [UnindexedAccountEvent],
) -> impl Iterator<Item = &mut UnindexedOrderReport> {
    events.iter_mut().filter_map(|event| match &mut event.kind {
        AccountEventKind::OrderSnapshot(snapshot)
            if matches!(
                snapshot.0.order.state,
                OrderState::Active(ActiveOrderState::Open(_))
            ) =>
        {
            Some(&mut snapshot.0)
        }
        _ => None,
    })
}

/// Whether a replacement reporting `after` filled counts less than the order it replaces had,
/// `before`: what a venue that starts a replacement's fill afresh shows, and one counting the fills
/// of the order replaced in it cannot, as fills only grow. `false` when either is unknown.
fn fill_restarted(before: Option<Decimal>, after: Option<Decimal>) -> bool {
    matches!((before, after), (Some(before), Some(after)) if after < before)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use crate::order::{
        Order, OrderKey, OrderKind, TimeInForce,
        id::{OrderId, StrategyId, VenueOrderId},
        report::{OrderReport, Replaces},
        state::{Cancelled, Open},
    };
    use rust_decimal_macros::dec;
    use rustrade_instrument::{
        Side, exchange::ExchangeId, instrument::name::InstrumentNameExchange,
    };
    use rustrade_integration::collection::snapshot::Snapshot;
    use smol_str::format_smolstr;

    fn order_event(
        cid: &str,
        state: OrderState<
            rustrade_instrument::asset::name::AssetNameExchange,
            InstrumentNameExchange,
        >,
        replaces: Option<&str>,
    ) -> UnindexedAccountEvent {
        let order = Order {
            key: OrderKey::new(
                ExchangeId::AlpacaBroker,
                InstrumentNameExchange::new("BTC/USD"),
                StrategyId::unknown(),
                ClientOrderId::new(cid),
            ),
            side: Side::Buy,
            price: Some(dec!(1000)),
            quantity: dec!(2),
            kind: OrderKind::Limit,
            time_in_force: TimeInForce::GoodUntilCancelled { post_only: false },
            state,
        };
        let report = OrderReport::state_only(order);
        let report = match replaces {
            Some(id) => report.with_replaces(Replaces::new(OrderId::new(id))),
            None => report,
        };
        UnindexedAccountEvent::new(
            ExchangeId::AlpacaBroker,
            AccountEventKind::OrderSnapshot(Snapshot(report)),
        )
    }

    fn open(cid: &str, id: &str, filled: Decimal, replaces: Option<&str>) -> UnindexedAccountEvent {
        let state = OrderState::active(Open::new(
            VenueOrderId::Assigned(OrderId::new(id)),
            chrono::DateTime::UNIX_EPOCH,
            filled,
        ));
        order_event(cid, state, replaces)
    }

    fn ended(cid: &str, id: &str) -> UnindexedAccountEvent {
        let state = OrderState::inactive(Cancelled::new(
            OrderId::new(id),
            chrono::DateTime::UNIX_EPOCH,
            Some(Decimal::ZERO),
        ));
        order_event(cid, state, None)
    }

    fn facts(
        event: ReplaceEvent,
        id: &str,
        cid: &str,
        replaces: Option<&str>,
        replaced_by: Option<&str>,
        filled: Decimal,
    ) -> ReplaceFacts {
        ReplaceFacts {
            event,
            id: SmolStr::new(id),
            cid: ClientOrderId::new(cid),
            replaces: replaces.map(SmolStr::new),
            replaced_by: replaced_by.map(SmolStr::new),
            filled: Some(filled),
        }
    }

    fn pending_replace(filled: Decimal) -> ReplaceFacts {
        facts(
            ReplaceEvent::PendingReplace,
            "old",
            "c-old",
            None,
            Some("new"),
            filled,
        )
    }

    fn replaced() -> ReplaceFacts {
        facts(
            ReplaceEvent::Replaced,
            "old",
            "c-old",
            None,
            Some("new"),
            Decimal::ZERO,
        )
    }

    fn replacement(filled: Decimal) -> ReplaceFacts {
        facts(
            ReplaceEvent::Other,
            "new",
            "c-new",
            Some("old"),
            None,
            filled,
        )
    }

    fn link_of(event: &UnindexedAccountEvent) -> Option<Replaces> {
        let AccountEventKind::OrderSnapshot(snapshot) = &event.kind else {
            panic!("an order snapshot: {event:?}");
        };
        snapshot.0.replaces.clone()
    }

    /// A replace in the order Alpaca was seen to send it: nothing for `pending_replace`, the
    /// replacement linked by the old order's id and client order id, then the old order's end
    /// at once.
    #[test]
    fn a_replace_in_the_order_seen_is_reported_as_it_arrives() {
        let mut tracker = ReplaceTracker::default();
        let now = Instant::now();

        assert!(
            tracker
                .process(pending_replace(Decimal::ZERO), Vec::new(), now)
                .is_empty()
        );
        let out = tracker.process(
            replacement(Decimal::ZERO),
            vec![open("c-new", "new", Decimal::ZERO, Some("old"))],
            now,
        );
        assert_eq!(out.len(), 1);
        assert_eq!(
            link_of(&out[0]),
            Some(Replaces::new(OrderId::new("old")).with_cid(ClientOrderId::new("c-old")))
        );
        let out = tracker.process(replaced(), vec![ended("c-old", "old")], now);
        assert_eq!(out, vec![ended("c-old", "old")]);
        assert_eq!(tracker.next_deadline(), None);
        assert!(tracker.take_reads().is_empty(), "nothing to read");
    }

    /// An end arriving before its replacement's first event is held, and goes out right after
    /// that event's own.
    #[test]
    fn an_end_before_its_replacement_is_held_until_the_replacement_is_reported() {
        let mut tracker = ReplaceTracker::default();
        let now = Instant::now();

        tracker.process(pending_replace(Decimal::ZERO), Vec::new(), now);
        assert!(
            tracker
                .process(replaced(), vec![ended("c-old", "old")], now)
                .is_empty()
        );
        assert_eq!(tracker.next_deadline(), Some(now + REPLACED_HOLD));

        let out = tracker.process(
            replacement(Decimal::ZERO),
            vec![open("c-new", "new", Decimal::ZERO, Some("old"))],
            now,
        );
        assert_eq!(out.len(), 2);
        assert!(link_of(&out[0]).is_some());
        assert_eq!(out[1], ended("c-old", "old"));
        assert_eq!(tracker.next_deadline(), None);
    }

    /// Any event of the replacement releases the end, an ended one included, such as Alpaca's
    /// rejection of a replacement whose order filled first. An ended report is not linked.
    #[test]
    fn an_ended_replacement_releases_the_end_held_and_is_not_linked() {
        let mut tracker = ReplaceTracker::default();
        let now = Instant::now();

        tracker.process(pending_replace(Decimal::ZERO), Vec::new(), now);
        tracker.process(replaced(), vec![ended("c-old", "old")], now);
        let out = tracker.process(replacement(Decimal::ZERO), vec![ended("c-new", "new")], now);

        assert_eq!(out, vec![ended("c-new", "new"), ended("c-old", "old")]);
    }

    /// A replacement whose `pending_replace` was not seen, or named another order, is linked by
    /// the old order's id alone; one under the old client order id is linked without naming it.
    #[test]
    fn a_replacement_is_linked_by_client_order_id_only_when_the_pending_replace_gave_another() {
        let now = Instant::now();
        let linked = |tracker: &mut ReplaceTracker, cid: &str| {
            let mut facts = replacement(Decimal::ZERO);
            facts.cid = ClientOrderId::new(cid);
            let out = tracker.process(
                facts,
                vec![open(cid, "new", Decimal::ZERO, Some("old"))],
                now,
            );
            link_of(&out[0])
        };

        let by_id = Some(Replaces::new(OrderId::new("old")));
        assert_eq!(linked(&mut ReplaceTracker::default(), "c-new"), by_id);

        let mut other = ReplaceTracker::default();
        let mut pending = pending_replace(Decimal::ZERO);
        pending.id = SmolStr::new("another");
        other.process(pending, Vec::new(), now);
        assert_eq!(linked(&mut other, "c-new"), by_id);

        let mut same_cid = ReplaceTracker::default();
        let mut pending = pending_replace(Decimal::ZERO);
        pending.cid = ClientOrderId::new("c-same");
        same_cid.process(pending, Vec::new(), now);
        assert_eq!(linked(&mut same_cid, "c-same"), by_id);
    }

    /// The first linked report of a replacement is checked against what the order it replaces had
    /// filled, once; an ended report is neither linked nor checked.
    #[test]
    fn the_first_live_report_of_a_replacement_is_checked_against_the_old_fill_once() {
        let mut tracker = ReplaceTracker::default();
        let now = Instant::now();
        tracker.process(pending_replace(dec!(1)), Vec::new(), now);
        let (new, cid, old) = (
            SmolStr::new("new"),
            ClientOrderId::new("c-new"),
            SmolStr::new("old"),
        );

        let mut ended_events = vec![ended("c-new", "new")];
        assert!(!tracker.link(&new, &cid, &old, Some(Decimal::ZERO), &mut ended_events));
        assert_eq!(link_of(&ended_events[0]), None);

        let mut live = vec![open("c-new", "new", Decimal::ZERO, Some("old"))];
        assert!(tracker.link(&new, &cid, &old, Some(Decimal::ZERO), &mut live));
        let mut again = vec![open("c-new", "new", Decimal::ZERO, Some("old"))];
        assert!(!tracker.link(&new, &cid, &old, Some(Decimal::ZERO), &mut again));
        assert!(link_of(&again[0]).is_some_and(|link| link.cid.is_some()));
    }

    #[test]
    fn a_fill_restarted_only_when_the_replacement_counts_less() {
        assert!(fill_restarted(Some(dec!(1)), Some(Decimal::ZERO)));
        assert!(!fill_restarted(Some(dec!(1)), Some(dec!(1))));
        assert!(!fill_restarted(Some(dec!(1)), Some(dec!(1.5))));
        assert!(!fill_restarted(None, Some(Decimal::ZERO)));
        assert!(!fill_restarted(Some(dec!(1)), None));
    }

    /// An end held past its deadline goes out without its replacement; one not yet due stays.
    #[test]
    fn an_end_held_past_its_deadline_is_reported_without_its_replacement() {
        let mut tracker = ReplaceTracker::default();
        let now = Instant::now();
        tracker.process(replaced(), vec![ended("c-old", "old")], now);
        let mut later = replaced();
        later.id = SmolStr::new("old-2");
        later.replaced_by = Some(SmolStr::new("new-2"));
        tracker.process(
            later,
            vec![ended("c-old-2", "old-2")],
            now + Duration::from_secs(1),
        );

        assert!(
            tracker
                .expire(now + REPLACED_HOLD - Duration::from_millis(1))
                .is_empty()
        );
        assert_eq!(
            tracker.expire(now + REPLACED_HOLD),
            vec![ended("c-old", "old")]
        );
        assert_eq!(
            tracker.next_deadline(),
            Some(now + Duration::from_secs(1) + REPLACED_HOLD)
        );
    }

    /// A disconnect drops what is held, so a reconnect's check reports it instead.
    #[test]
    fn a_disconnect_drops_every_end_held() {
        let mut tracker = ReplaceTracker::default();
        let now = Instant::now();
        tracker.process(replaced(), vec![ended("c-old", "old")], now);

        assert_eq!(tracker.drop_held(), 1);
        assert_eq!(tracker.next_deadline(), None);
        assert!(tracker.expire(now + REPLACED_HOLD).is_empty());
        assert_eq!(tracker.drop_held(), 0);
        assert!(
            tracker.take_reads().is_empty(),
            "the read asked for is dropped too"
        );
    }

    /// An end whose replacement was already reported, or that names none, goes out at once.
    #[test]
    fn an_end_is_not_held_when_its_replacement_was_reported_or_is_unnamed() {
        let mut tracker = ReplaceTracker::default();
        let now = Instant::now();
        tracker.process(
            replacement(Decimal::ZERO),
            vec![open("c-new", "new", Decimal::ZERO, Some("old"))],
            now,
        );
        assert_eq!(
            tracker.process(replaced(), vec![ended("c-old", "old")], now),
            vec![ended("c-old", "old")]
        );

        let mut unnamed = replaced();
        unnamed.replaced_by = None;
        assert_eq!(
            tracker.process(unnamed, vec![ended("c-old", "old")], now),
            vec![ended("c-old", "old")]
        );
        assert_eq!(tracker.next_deadline(), None);
    }

    /// Past the bound, the end held longest goes out at once.
    #[test]
    fn past_the_bound_the_end_held_longest_is_reported_at_once() {
        let mut tracker = ReplaceTracker::default();
        let now = Instant::now();
        let end = |index: usize| {
            let mut facts = replaced();
            facts.id = format_smolstr!("old-{index}");
            facts.replaced_by = Some(format_smolstr!("new-{index}"));
            (
                facts,
                vec![ended(&format!("c-old-{index}"), &format!("old-{index}"))],
            )
        };
        for index in 0..TRACKED_REPLACES.get() {
            let (facts, events) = end(index);
            assert!(tracker.process(facts, events, now).is_empty());
        }
        let (facts, events) = end(TRACKED_REPLACES.get());

        assert_eq!(
            tracker.process(facts, events, now),
            vec![ended("c-old-0", "old-0")]
        );
        assert_eq!(tracker.drop_held(), TRACKED_REPLACES.get());
    }

    /// A `replaced` before any event of its replacement, as Alpaca sends an equity's replace,
    /// asks once for the replacement to be read. The read is reported linked by the old order's id
    /// and the client order id the `replaced` gave, then the end held for it.
    #[test]
    fn a_replaced_before_its_replacement_asks_for_a_read_reported_before_the_end() {
        let mut tracker = ReplaceTracker::default();
        let now = Instant::now();

        assert!(
            tracker
                .process(replaced(), vec![ended("c-old", "old")], now)
                .is_empty()
        );
        assert_eq!(tracker.take_reads(), vec![SmolStr::new("new")]);
        assert!(
            tracker.take_reads().is_empty(),
            "each read is asked for once"
        );

        let out = tracker.process_read(
            replacement(Decimal::ZERO),
            vec![open("c-new", "new", Decimal::ZERO, Some("old"))],
            now,
        );
        assert_eq!(out.len(), 2);
        assert_eq!(
            link_of(&out[0]),
            Some(Replaces::new(OrderId::new("old")).with_cid(ClientOrderId::new("c-old")))
        );
        assert_eq!(out[1], ended("c-old", "old"));
        assert_eq!(tracker.next_deadline(), None);
    }

    /// A read returning after the stream reported the replacement is dropped, as it may be older.
    #[test]
    fn a_read_after_the_stream_reported_the_replacement_is_dropped() {
        let mut tracker = ReplaceTracker::default();
        let now = Instant::now();
        tracker.process(replaced(), vec![ended("c-old", "old")], now);
        let out = tracker.process(
            replacement(Decimal::ZERO),
            vec![open("c-new", "new", Decimal::ZERO, Some("old"))],
            now,
        );
        assert_eq!(out.len(), 2, "the replacement, then the end released");

        assert!(
            tracker
                .process_read(
                    replacement(Decimal::ZERO),
                    vec![open("c-new", "new", Decimal::ZERO, Some("old"))],
                    now,
                )
                .is_empty()
        );
    }

    /// A read returning after its end was reported without it, its hold run out, still reports
    /// the replacement, linked, and nothing else.
    #[test]
    fn a_read_after_the_hold_ran_out_still_reports_the_replacement() {
        let mut tracker = ReplaceTracker::default();
        let now = Instant::now();
        tracker.process(replaced(), vec![ended("c-old", "old")], now);
        assert_eq!(
            tracker.expire(now + REPLACED_HOLD),
            vec![ended("c-old", "old")]
        );

        let out = tracker.process_read(
            replacement(Decimal::ZERO),
            vec![open("c-new", "new", Decimal::ZERO, Some("old"))],
            now + REPLACED_HOLD,
        );
        assert_eq!(out.len(), 1);
        assert_eq!(
            link_of(&out[0]),
            Some(Replaces::new(OrderId::new("old")).with_cid(ClientOrderId::new("c-old")))
        );
    }

    /// A `pending_replace` seen first keeps its client order id and fill: the `replaced` after it
    /// does not overwrite them, though it still asks for a read.
    #[test]
    fn a_replaced_after_a_pending_replace_keeps_what_the_pending_replace_gave() {
        let mut tracker = ReplaceTracker::default();
        let now = Instant::now();
        tracker.process(pending_replace(dec!(1)), Vec::new(), now);
        let mut end = replaced();
        end.cid = ClientOrderId::new("c-other");
        end.filled = Some(Decimal::ZERO);
        tracker.process(end, vec![ended("c-old", "old")], now);
        assert_eq!(tracker.take_reads(), vec![SmolStr::new("new")]);

        let out = tracker.process_read(
            replacement(dec!(1)),
            vec![open("c-new", "new", dec!(1), Some("old"))],
            now,
        );
        assert_eq!(
            link_of(&out[0]),
            Some(Replaces::new(OrderId::new("old")).with_cid(ClientOrderId::new("c-old")))
        );
    }

    /// A read that names no order it replaces is linked to the order whose `replaced` asked for
    /// it, by its id and client order id, and releases that order's end.
    #[test]
    fn a_read_naming_no_replaced_order_is_linked_to_the_one_that_asked() {
        let mut tracker = ReplaceTracker::default();
        let now = Instant::now();
        tracker.process(replaced(), vec![ended("c-old", "old")], now);
        let mut facts = replacement(Decimal::ZERO);
        facts.replaces = None;

        let out = tracker.process_read(facts, vec![open("c-new", "new", Decimal::ZERO, None)], now);
        assert_eq!(out.len(), 2);
        assert_eq!(
            link_of(&out[0]),
            Some(Replaces::new(OrderId::new("old")).with_cid(ClientOrderId::new("c-old")))
        );
        assert_eq!(out[1], ended("c-old", "old"));
    }

    /// A second `replaced` for an end already held asks for no second read.
    #[test]
    fn a_repeated_replaced_asks_for_one_read() {
        let mut tracker = ReplaceTracker::default();
        let now = Instant::now();
        tracker.process(replaced(), vec![ended("c-old", "old")], now);
        tracker.process(replaced(), vec![ended("c-old", "old")], now);

        assert_eq!(tracker.take_reads(), vec![SmolStr::new("new")]);
    }

    /// The old client order id goes on live reports of a replacement only: an ended one keeps the
    /// link it came with.
    #[test]
    fn an_ended_report_of_a_replacement_gets_no_client_order_id() {
        let mut tracker = ReplaceTracker::default();
        let now = Instant::now();
        tracker.process(pending_replace(Decimal::ZERO), Vec::new(), now);
        let state = OrderState::inactive(Cancelled::new(
            OrderId::new("new"),
            chrono::DateTime::UNIX_EPOCH,
            Some(Decimal::ZERO),
        ));

        let out = tracker.process(
            replacement(Decimal::ZERO),
            vec![order_event("c-new", state, Some("old"))],
            now,
        );
        assert_eq!(link_of(&out[0]), Some(Replaces::new(OrderId::new("old"))));
    }
}
