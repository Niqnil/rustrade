use crate::engine::state::order::{
    applied::{
        OrderAdopted, OrderNotAdopted, OrderNotAdoptedReason, OrderReplaced, OrderReportsApplied,
    },
    in_flight_recorder::InFlightRequestRecorder,
    manager::OrderManager,
    terms::{OrderTerms, OrderTermsChanged, OrderTermsDiverged, Reconciled, reconcile},
};
use chrono::{DateTime, Utc};
use fnv::FnvHashMap;
use rust_decimal::Decimal;
use rustrade_execution::{
    error::{ApiError, OrderError},
    order::{
        Order, OrderKey,
        amend::AmendPatch,
        id::{ClientOrderId, OrderId, VenueOrderId},
        report::{OrderReport, Replaces, ReportedTerms},
        request::{
            OrderRequestAmend, OrderRequestCancel, OrderRequestOpen, OrderResponseAmend,
            OrderResponseCancel,
        },
        state::{
            ActiveOrderState, AmendInFlight, CancelInFlight, InactiveOrderState, Open,
            OpenInFlight, OrderState,
        },
    },
};
use rustrade_instrument::{exchange::ExchangeIndex, instrument::InstrumentIndex};
use rustrade_integration::collection::snapshot::Snapshot;
use serde::{Deserialize, Serialize};
use std::{
    collections::{
        VecDeque,
        hash_map::{Entry, VacantEntry},
    },
    fmt::Debug,
};
use tracing::{debug, error, info, warn};

pub mod applied;
pub mod in_flight_recorder;
pub mod manager;
pub mod terms;

/// What applying one order snapshot did to the order it describes.
///
/// Returned by [`OrderManager::update_from_order_snapshot`].
///
/// `#[non_exhaustive]`, so a field can be added without breaking a caller.
#[non_exhaustive]
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SnapshotApplied<ExchangeKey, InstrumentKey> {
    /// The snapshot ended the order, and it is no longer tracked: the caller owes the routing
    /// prune of any retirement.
    pub retired: bool,
    /// What the snapshot did to the order: adopted it or refused it when it was untracked, or
    /// changed its terms or left stated terms diverged.
    pub orders: OrderReportsApplied<ExchangeKey, InstrumentKey>,
}

impl<ExchangeKey, InstrumentKey> Default for SnapshotApplied<ExchangeKey, InstrumentKey> {
    fn default() -> Self {
        Self::working(OrderReportsApplied::default())
    }
}

impl<ExchangeKey, InstrumentKey> SnapshotApplied<ExchangeKey, InstrumentKey> {
    fn working(orders: OrderReportsApplied<ExchangeKey, InstrumentKey>) -> Self {
        Self {
            retired: false,
            orders,
        }
    }

    fn retired(orders: OrderReportsApplied<ExchangeKey, InstrumentKey>) -> Self {
        Self {
            retired: true,
            orders,
        }
    }
}

/// Synchronous order manager that tracks the lifecycle of active exchange orders.
///
/// The `Orders` struct maintains a `FnvHashMap` of orders keyed by their [`ClientOrderId`].
///
/// Implements the [`OrderManager`] and [`InFlightRequestRecorder`] traits.
///
/// A distinct instance of `Orders` is used in the engine
/// [`InstrumentState`](super::instrument::InstrumentState) to track the active orders for
/// each instrument, however it could be used to track global orders if [`ClientOrderId`]
/// is globally unique.
///
/// # State Transitions
/// Orders tend to progress through the following states:
/// 1. OpenInFlight - Initial order request sent to exchange
/// 2. Open - Order confirmed as open on exchange
/// 3. CancelInFlight - Cancellation request sent to exchange
/// 4. Cancelled/Expired/FullyFilled - Terminal states, once achieved order is no longer tracked.
///
/// A venue need not use a distinct state to report completion: an `Open` snapshot with no quantity
/// remaining is terminal too, and is untracked on the same rule. Every arm that accepts an `Open`
/// update applies it, so an order cannot be retained as active once it has nothing left to fill.
///
/// An open whose outcome is unknown
/// ([`OrderState::is_outcome_unknown`](rustrade_execution::order::state::OrderState::is_outcome_unknown))
/// is not terminal: it changes nothing, and an `OpenInFlight` order stays in flight until the
/// venue's report of it, or the `ExecutionManager`'s lookup, arrives. A report that the order was
/// never placed ([`OrderError::NotPlaced`]) ends it only while the venue has not reported it
/// working: a lookup can conclude the order unknown just as the venue's report of it arrives.
///
/// # Retired orders stay retired
///
/// The account stream and the response to an open request travel on different connections, so
/// the stream can report how an order ended before the response, which carries the order's state
/// *at placement*, arrives. A stale listing can likewise still show an order that has just
/// ended. Applied as it stands, such a late active snapshot would find the order untracked and
/// track it again: a live order the venue no longer has.
///
/// So `Orders` remembers the [`ClientOrderId`]s of the last [`MAX_RECENTLY_RETIRED_ORDERS`]
/// orders that ended, oldest forgotten first, and ignores an active snapshot for one of them. An
/// order counts as ended when `Orders` retires it, when [`Orders::clear`] drops it, or when a
/// snapshot reports it ended while it is not tracked, which includes an order this engine never
/// placed: each takes a place in the same window. This relies on what the engine already assumes,
/// that a client order id names one order for good. Recording a new open request under a
/// remembered id forgets it, since that request is a new order by the engine's own hand.
///
/// The memory is bookkeeping, not order state: `Orders` serialises as its tracked orders alone,
/// and two `Orders` that track the same orders are equal whatever each remembers.
///
/// # Orders replaced at the venue
///
/// A venue may carry out a change to a working order by replacing it, under a new venue order id,
/// a new client order id, or both. A report linking the replacement to the order replaced
/// ([`OrderReport::replaces`]) keeps it one tracked order:
/// - **A new venue order id** under the same client order id is followed when the link names the
///   id held. The id held becomes one the order is known to have been replaced from (see
///   [`TrackedOrder::superseded_ids`]).
/// - **A new client order id**: the order replaced, found by the client order id the link names,
///   or else by its venue order id, moves to the new one, keeping its strategy, its fills and
///   its terms. Its old client order id is remembered as retired, so a late report under it is
///   ignored. A cancel or amend in flight under the old id is dropped: the venue answers it under
///   that id, which `Orders` no longer tracks. The order keeps the state the request was sent
///   from, and the report is then applied to it.
/// - **Late reports of the order replaced**, naming a venue order id the order was replaced from,
///   are ignored: a working report, and an ended one, such as the cancel of the old order a venue
///   reports alongside its replacement. A fill naming such an id still counts towards the order
///   ([`Orders::update_from_fill`]).
/// - **A link is ignored** on a report naming no venue order id, and on a report under a client
///   order id already retired, which is a replay of a replace already followed: nothing moves
///   back to an id the order has left.
///
/// Each replace followed is returned as an [`OrderReplaced`]. An unlinked report naming a
/// different venue order id from the one held is still refused, as one client order id covering
/// two venue orders. The engine's own amends need no link: an answer naming the venue order id it
/// replaced ([`Amended::previous_id`](rustrade_execution::order::amend::Amended::previous_id))
/// moves the order to its new id in the same way.
///
/// **Known limitations:**
/// - Only the last [`MAX_SUPERSEDED_IDS`] venue order ids an order was replaced from are kept. A
///   late ended report naming an older one ends the order, as an ended report naming an id never
///   held does.
/// - The audit replica sees no open requests, only the venue's reports, so it cannot tell an id
///   reused within the window from a late report. It ignores the new order where the engine
///   tracks it, until the id has left the window.
/// - A deserialised `Orders`, such as a restored engine's or a replica seeded from a snapshot,
///   remembers nothing. A late report of an order that retired before the snapshot is tracked
///   again, as before this memory existed.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(transparent)]
pub struct Orders<ExchangeKey = ExchangeIndex, InstrumentKey = InstrumentIndex>(
    pub FnvHashMap<ClientOrderId, TrackedOrder<ExchangeKey, InstrumentKey>>,
    #[serde(skip)] RecentlyRetired,
);

/// An order [`Orders`] tracks, with when the venue last stated each of its terms.
///
/// # Terms times
///
/// A report's terms are ordered by their own time, not by the order's state. The state an order
/// holds may come from a report that states no terms, such as a listing or a placement answer
/// stamped on another path. Ordered against that state, a report of a change made at the venue,
/// stamped a little earlier, would be dropped, and the change missed until the venue reported it
/// again. So [`terms_times`](Self::terms_times) records, term by term, when the venue last stated
/// the value held, and a report's stated term is read only when the report is stamped no earlier
/// than that, whether or not its state supersedes the one held. Term by term, because a report
/// may state some terms and not others: a report stating only the price says nothing of when the
/// quantity last changed.
///
/// Serialized as the order's own fields, with `terms_times` beside them once any is set: an order
/// serialized without it reads back with none. That flattened layout needs a self-describing
/// format, such as JSON, to read back, and it is why [`Order`] has no field named `terms_times`.
#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize)]
pub struct TrackedOrder<ExchangeKey = ExchangeIndex, InstrumentKey = InstrumentIndex> {
    /// The order as tracked.
    #[serde(flatten)]
    pub order: Order<ExchangeKey, InstrumentKey, ActiveOrderState>,
    #[serde(default, skip_serializing_if = "TermsTimes::is_none")]
    terms_times: TermsTimes,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    superseded_ids: Vec<OrderId>,
}

impl<ExchangeKey, InstrumentKey> TrackedOrder<ExchangeKey, InstrumentKey> {
    /// Track `order` on the terms it holds, which no venue report has yet stated.
    pub fn new(order: Order<ExchangeKey, InstrumentKey, ActiveOrderState>) -> Self {
        Self {
            order,
            terms_times: TermsTimes::default(),
            superseded_ids: Vec::new(),
        }
    }

    /// When the venue last stated each of this order's terms (see "Terms times").
    pub fn terms_times(&self) -> TermsTimes {
        self.terms_times
    }

    /// The venue order ids this order was replaced from, oldest first: the last
    /// [`MAX_SUPERSEDED_IDS`] (see "Orders replaced at the venue" on [`Orders`]).
    ///
    /// A late report naming one of them is of the order before a replace: a working or ended
    /// report is ignored, and a fill counts towards the order. Serialized beside the order's
    /// fields once any is set, as [`terms_times`](Self::terms_times) is.
    pub fn superseded_ids(&self) -> &[OrderId] {
        &self.superseded_ids
    }

    /// Whether `id` is a venue order id this order was replaced from.
    fn is_superseded(&self, id: &OrderId) -> bool {
        self.superseded_ids.contains(id)
    }

    /// Whether `id` names a venue order id this order was replaced from.
    fn names_superseded(&self, id: &VenueOrderId) -> bool {
        id.assigned().is_some_and(|id| self.is_superseded(id))
    }

    /// The venue order id the order is addressed by now, if the venue has named it.
    fn current_id(&self) -> Option<&OrderId> {
        self.order
            .state
            .open_meta()
            .and_then(|open| open.id.assigned())
    }

    /// Record that the order was replaced from the venue order id `id`, forgetting the oldest
    /// past [`MAX_SUPERSEDED_IDS`]. Nothing for the id the order is addressed by now, or one
    /// already recorded.
    fn supersede(&mut self, id: OrderId) {
        if self.current_id() == Some(&id) || self.is_superseded(&id) {
            return;
        }
        if self.superseded_ids.len() == MAX_SUPERSEDED_IDS {
            self.superseded_ids.remove(0);
        }
        self.superseded_ids.push(id);
    }
}

/// How many venue order ids a [`TrackedOrder`] keeps of those it was replaced from, so that a
/// late report of the order before a replace is known for one (see "Orders replaced at the venue"
/// on [`Orders`]).
///
/// A late report of a replaced order arrives within moments of its replacement, so the bound
/// needs to span the replaces made in that time: a few, even for an order a strategy reprices
/// on every tick. It costs at most this many venue order ids per tracked order.
pub const MAX_SUPERSEDED_IDS: usize = 16;

/// When the venue last stated each of an order's terms: the state timestamp of the latest report
/// stating it, or of the venue's answer to an amend that set it. `None` for a term no report has
/// stated, which the order holds as it was placed or first tracked.
///
/// A term's time records that the venue spoke to it, not that its value was taken: it advances
/// even when the stated value is not adopted, such as a stand-in kind or time in force the
/// report carries, or a price on a kind with no fixed limit. A report older than that still
/// speaks for an earlier moment, so it is not read either.
///
/// `#[non_exhaustive]`, so a term can be added without breaking a caller.
#[derive(Debug, Copy, Clone, Default, Eq, PartialEq, Hash, Deserialize, Serialize)]
#[serde(default)]
#[non_exhaustive]
pub struct TermsTimes {
    /// When the venue last stated the order's total quantity.
    pub quantity: Option<DateTime<Utc>>,
    /// When the venue last stated the order's limit price.
    pub price: Option<DateTime<Utc>>,
    /// When the venue last stated the order's kind.
    pub kind: Option<DateTime<Utc>>,
    /// When the venue last stated the order's time in force.
    pub time_in_force: Option<DateTime<Utc>>,
}

impl TermsTimes {
    /// Whether no term has been stated.
    pub fn is_none(&self) -> bool {
        *self == Self::default()
    }

    /// The terms of `stated` that a report stamped `time` speaks to: those last stated no later
    /// than `time`.
    fn in_sequence(&self, stated: ReportedTerms, time: DateTime<Utc>) -> ReportedTerms {
        let current = |held: Option<DateTime<Utc>>| held.is_none_or(|held| time >= held);
        ReportedTerms::NONE
            .with_quantity(stated.quantity && current(self.quantity))
            .with_price(stated.price && current(self.price))
            .with_kind(stated.kind && current(self.kind))
            .with_time_in_force(stated.time_in_force && current(self.time_in_force))
    }

    /// Record that the venue stated the terms of `stated` at `time`, never moving a time back.
    fn stated_at(&mut self, stated: ReportedTerms, time: DateTime<Utc>) {
        let advance = |held: &mut Option<DateTime<Utc>>, stated: bool| {
            if stated {
                *held = Some(held.map_or(time, |held| held.max(time)));
            }
        };
        advance(&mut self.quantity, stated.quantity);
        advance(&mut self.price, stated.price);
        advance(&mut self.kind, stated.kind);
        advance(&mut self.time_in_force, stated.time_in_force);
    }
}

/// How many ended orders' [`ClientOrderId`]s an [`Orders`] remembers, so that a late active
/// snapshot cannot track one again (see "Retired orders stay retired" on [`Orders`]).
///
/// The bound has to span the orders that retire between an order's end and the last stale report
/// of it: a placement response the account stream outran, or a listing fetched just before the
/// order ended. That is a few orders in practice; the bound matches
/// [`MAX_RETIRED_ROUTING`](crate::engine::state::instrument::MAX_RETIRED_ROUTING), which spans
/// the same kind of gap for a late fill. It costs at most this many client order ids per
/// instrument, allocated only as orders retire.
pub const MAX_RECENTLY_RETIRED_ORDERS: usize = 64;

/// The [`ClientOrderId`]s of the orders an [`Orders`] most recently saw end, at most
/// [`MAX_RECENTLY_RETIRED_ORDERS`], oldest forgotten first.
///
/// A plain list: it is searched only for a snapshot of an untracked order, and a scan of so few
/// ids costs about what hashing one would.
#[derive(Debug, Clone, Default)]
struct RecentlyRetired(VecDeque<ClientOrderId>);

impl RecentlyRetired {
    fn insert(&mut self, cid: ClientOrderId) {
        if self.contains(&cid) {
            return;
        }
        if self.0.len() == MAX_RECENTLY_RETIRED_ORDERS {
            self.0.pop_front();
        }
        self.0.push_back(cid);
    }

    fn contains(&self, cid: &ClientOrderId) -> bool {
        self.0.contains(cid)
    }

    fn forget(&mut self, cid: &ClientOrderId) {
        self.0.retain(|retired| retired != cid);
    }
}

impl<ExchangeKey, InstrumentKey> Default for Orders<ExchangeKey, InstrumentKey> {
    fn default() -> Self {
        Self::new(FnvHashMap::default())
    }
}

/// Compares the tracked orders only; see [Retired orders stay retired](Orders#retired-orders-stay-retired).
impl<ExchangeKey, InstrumentKey> PartialEq for Orders<ExchangeKey, InstrumentKey>
where
    ExchangeKey: PartialEq,
    InstrumentKey: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl<ExchangeKey, InstrumentKey> Orders<ExchangeKey, InstrumentKey> {
    /// Track `orders`, on the terms each holds, remembering no retired ones.
    pub fn new(
        orders: FnvHashMap<ClientOrderId, Order<ExchangeKey, InstrumentKey, ActiveOrderState>>,
    ) -> Self {
        Self(
            orders
                .into_iter()
                .map(|(cid, order)| (cid, TrackedOrder::new(order)))
                .collect(),
            RecentlyRetired::default(),
        )
    }

    /// The order tracked under `cid`, with when the venue last stated its terms.
    pub fn tracked(
        &self,
        cid: &ClientOrderId,
    ) -> Option<&TrackedOrder<ExchangeKey, InstrumentKey>> {
        self.0.get(cid)
    }

    /// The order tracked under `cid`.
    pub fn get(
        &self,
        cid: &ClientOrderId,
    ) -> Option<&Order<ExchangeKey, InstrumentKey, ActiveOrderState>> {
        self.0.get(cid).map(|tracked| &tracked.order)
    }

    /// The tracked orders, by client order id, in no particular order.
    pub fn iter(
        &self,
    ) -> impl Iterator<
        Item = (
            &ClientOrderId,
            &Order<ExchangeKey, InstrumentKey, ActiveOrderState>,
        ),
    > {
        self.0.iter().map(|(cid, tracked)| (cid, &tracked.order))
    }

    /// The tracked orders, in no particular order.
    pub fn values(
        &self,
    ) -> impl Iterator<Item = &Order<ExchangeKey, InstrumentKey, ActiveOrderState>> {
        self.0.values().map(|tracked| &tracked.order)
    }

    /// Remove all tracked orders, discarding any pending state, and remember each as retired.
    ///
    /// Used during contract expiry cleanup where the exchange silently voids all
    /// open and cancel-in-flight orders at expiry. The normal `cleanup_routing_tables`
    /// path cannot remove `CancelInFlight` entries because their cancel acks may
    /// never arrive after expiry, causing unbounded accumulation in a long-running engine.
    /// Remembering them keeps a late report of a voided order from tracking it again.
    pub fn clear(&mut self) {
        for (cid, _) in self.0.drain() {
            self.1.insert(cid);
        }
    }

    /// Whether any tracked order is awaiting a response from the exchange.
    ///
    /// True for [`ActiveOrderState::OpenInFlight`], [`ActiveOrderState::CancelInFlight`] and
    /// [`ActiveOrderState::AmendInFlight`] — a request has been sent and its outcome is not yet
    /// known. An order merely *resting* at the
    /// exchange ([`ActiveOrderState::Open`]) is **not** in flight: nothing is owed in reply, and a
    /// limit order that never fills must not hold up a drain.
    pub fn has_request_in_flight(&self) -> bool {
        self.0.values().any(|tracked| {
            matches!(
                tracked.order.state,
                ActiveOrderState::OpenInFlight(_)
                    | ActiveOrderState::CancelInFlight(_)
                    | ActiveOrderState::AmendInFlight(_)
            )
        })
    }

    /// Whether `snapshot` reports that its order was never placed
    /// ([`OrderError::NotPlaced`]) while the order is tracked as one the venue has reported
    /// working: `Open` or `AmendInFlight`, or `CancelInFlight` after it was. The venue's own report outweighs the
    /// conclusion, so [`update_from_order_snapshot`](OrderManager::update_from_order_snapshot)
    /// ignores such a snapshot.
    pub(crate) fn is_contradicted_never_placed<AssetKey>(
        &self,
        snapshot: &Order<ExchangeKey, InstrumentKey, OrderState<AssetKey, InstrumentKey>>,
    ) -> bool {
        let OrderState::Inactive(InactiveOrderState::OpenFailed(OrderError::NotPlaced(_))) =
            &snapshot.state
        else {
            return false;
        };
        self.0
            .get(&snapshot.key.cid)
            .is_some_and(|tracked| match &tracked.order.state {
                ActiveOrderState::OpenInFlight(_) => false,
                ActiveOrderState::Open(_) | ActiveOrderState::AmendInFlight(_) => true,
                ActiveOrderState::CancelInFlight(cancel) => cancel.order.is_some(),
            })
    }

    /// Whether `snapshot` is a failed open that `update_from_order_snapshot` leaves unapplied,
    /// logging why.
    fn is_failed_open_to_ignore<AssetKey>(
        &self,
        snapshot: &Order<ExchangeKey, InstrumentKey, OrderState<AssetKey, InstrumentKey>>,
    ) -> bool
    where
        ExchangeKey: Debug,
        AssetKey: Debug,
        InstrumentKey: Debug,
    {
        // An open whose outcome is unknown says nothing about the order: the venue may hold it
        // live. So it neither ends a tracked order nor retires the id, and the venue's later
        // report of the order, or the `ExecutionManager`'s lookup of it, settles it.
        if snapshot.state.is_outcome_unknown() {
            debug!(
                exchange = ?snapshot.key.exchange,
                instrument = ?snapshot.key.instrument,
                strategy = %snapshot.key.strategy,
                cid = %snapshot.key.cid,
                update = ?snapshot,
                "OrderManager received an open whose outcome is unknown - leaving the order as it is"
            );
            return true;
        }

        // A report that the order was never placed is contradicted once the venue has reported
        // the order working, so it does not end it: the venue's own reports settle the order.
        if self.is_contradicted_never_placed(snapshot) {
            warn!(
                exchange = ?snapshot.key.exchange,
                instrument = ?snapshot.key.instrument,
                strategy = %snapshot.key.strategy,
                cid = %snapshot.key.cid,
                update = ?snapshot,
                "OrderManager received a never-placed report for an order the venue reported working - ignoring"
            );
            return true;
        }
        false
    }

    /// Stop tracking the order under `cid` if it is working as the venue order `id`, `Open` or
    /// with an amend in flight, returning it.
    ///
    /// Kept, with `None` returned, when:
    /// - its open or cancel is in flight, because that request is still being answered;
    /// - it cannot be shown to be the venue order `id` ([`VenueOrderId::is_same_order_as`]). The
    ///   client id may have been reused for a new order that `id` does not describe, and without
    ///   a venue identifier on both sides nothing tells the two apart, so an order the venue never
    ///   named is always kept.
    ///
    /// The caller owes the same routing prune as for any other retirement.
    pub fn remove_open(
        &mut self,
        cid: &ClientOrderId,
        id: &VenueOrderId,
    ) -> Option<Order<ExchangeKey, InstrumentKey, ActiveOrderState>> {
        match &self.0.get(cid)?.order.state {
            ActiveOrderState::Open(open)
            | ActiveOrderState::AmendInFlight(AmendInFlight { order: open, .. })
                if open.id.is_same_order_as(id) =>
            {
                self.1.insert(cid.clone());
                self.0.remove(cid).map(|tracked| tracked.order)
            }
            _ => None,
        }
    }

    /// Advance a tracked order's cumulative filled quantity to what one of its fills reported,
    /// untracking the order once nothing is left to fill.
    ///
    /// Returns `true` if the order was untracked, so the caller can prune the routing that
    /// referred to it -- the same obligation a terminal
    /// [`OrderManager::update_from_order_snapshot`] carries.
    ///
    /// # Why a cumulative rather than an increment
    ///
    /// `filled_quantity` is advanced to `max(current, reported)`, never incremented. A venue can
    /// re-deliver a fill, and two fills can arrive out of order; adding each execution's size to a
    /// running total double-counts under either. Taking the greater of the two is unconditionally
    /// idempotent, which is what lets this be driven from the fill stream at all.
    ///
    /// # What it will not do
    ///
    /// Nothing happens unless the order is already tracked and working, `Open` or with an amend
    /// in flight, under this exact venue [`OrderId`], or one it was replaced from (see "Orders
    /// replaced at the venue" on [`Orders`]). A fill for an untracked order cannot insert one, so this cannot resurrect an
    /// order that has retired -- unlike an order snapshot, which reaches a vacant-entry arm that
    /// inserts. A fill arriving after its order retired is therefore safe to apply here, and is
    /// simply ignored.
    ///
    /// An order the venue has not named -- `VenueOrderId::ClientAssigned` -- is never advanced
    /// here, because a fill is reported against a venue id and that order has none to match.
    /// Such an order is advanced by its order snapshots instead.
    pub fn update_from_fill(
        &mut self,
        cid: &ClientOrderId,
        order_id: &OrderId,
        filled_quantity: Decimal,
    ) -> bool
    where
        InstrumentKey: Debug,
    {
        let Some(TrackedOrder {
            order,
            superseded_ids,
            ..
        }) = self.0.get_mut(cid)
        else {
            return false;
        };
        // While an amend raising the quantity is in flight, the venue may already have made it and
        // filled beyond the old total: such a fill retires the order only past the new total. A
        // venue that refused the raise cannot have filled beyond the old one.
        let (open, quantity) = match &mut order.state {
            ActiveOrderState::Open(open) => (open, order.quantity),
            ActiveOrderState::AmendInFlight(AmendInFlight {
                order: open, patch, ..
            }) => (
                open,
                patch
                    .quantity
                    .map_or(order.quantity, |amended| amended.max(order.quantity)),
            ),
            _ => return false,
        };
        // A `ClientOrderId` identifies the order this engine sent; the venue's own id identifies
        // what it filled. Requiring both to agree keeps a fill off an order that merely shares the
        // slot -- a replaced order, or a reused client id.
        //
        // An order the venue never named has no id to agree with, so it cannot be advanced from
        // the fill stream at all. Reading two absent ids as a match is precisely the confusion
        // this comparison exists to prevent, so the absence is compared, not skipped.
        //
        // A late fill of the order before a replace names an id it was replaced from, and counts:
        // its cumulative is at most the order's, whether the venue carried the fills over or the
        // producer added them back up, so taking the greater of the two never undercounts.
        if open.id.assigned() != Some(order_id) && !superseded_ids.contains(order_id) {
            return false;
        }
        if filled_quantity <= open.filled_quantity {
            // Stale or re-delivered: the order already reflects at least this much.
            return false;
        }

        open.filled_quantity = filled_quantity;

        // `<= 0` rather than `is_zero`: a venue that reports a cumulative above the quantity this
        // engine recorded -- an over-fill, or an order amended at the venue -- has still finished
        // with it, and a negative remainder must retire the order rather than leave it resting
        // forever.
        if open.quantity_remaining(quantity) <= Decimal::ZERO {
            debug!(
                instrument = ?order.key.instrument,
                strategy = %order.key.strategy,
                cid = %cid,
                %order_id,
                "OrderManager removing an Open order a fill reports as fully filled"
            );
            self.0.remove(cid);
            self.1.insert(cid.clone());
            return true;
        }

        false
    }
}

#[cfg(test)]
impl<ExchangeKey, InstrumentKey> Orders<ExchangeKey, InstrumentKey>
where
    ExchangeKey: Debug + Clone,
    InstrumentKey: Debug + Clone,
{
    /// [`OrderManager::update_from_order_snapshot`] for a snapshot stating none of its terms, as
    /// most producers send: only its state is applied.
    pub(crate) fn update_from_state_only_snapshot<AssetKey>(
        &mut self,
        snapshot: Snapshot<&Order<ExchangeKey, InstrumentKey, OrderState<AssetKey, InstrumentKey>>>,
    ) -> SnapshotApplied<ExchangeKey, InstrumentKey>
    where
        AssetKey: Debug + Clone,
    {
        self.update_from_order_snapshot(Snapshot(&OrderReport::state_only(snapshot.0.clone())))
    }
}

impl<ExchangeKey, InstrumentKey> OrderManager<ExchangeKey, InstrumentKey>
    for Orders<ExchangeKey, InstrumentKey>
where
    ExchangeKey: Debug + Clone,
    InstrumentKey: Debug + Clone,
{
    fn orders<'a>(
        &'a self,
    ) -> impl Iterator<Item = &'a Order<ExchangeKey, InstrumentKey, ActiveOrderState>>
    where
        ExchangeKey: 'a,
        InstrumentKey: 'a,
    {
        self.0.values().map(|tracked| &tracked.order)
    }

    fn update_from_order_snapshot<AssetKey>(
        &mut self,
        snapshot: Snapshot<&OrderReport<ExchangeKey, AssetKey, InstrumentKey>>,
    ) -> SnapshotApplied<ExchangeKey, InstrumentKey>
    where
        AssetKey: Debug + Clone,
    {
        let Snapshot(report) = snapshot;

        if self.is_failed_open_to_ignore(&report.order) {
            return SnapshotApplied::default();
        }

        // A replace at the venue moves the order to the ids the report names before the report
        // is applied, so it is applied as a report of the order it replaces (see "Orders replaced
        // at the venue").
        let rekeyed = self.rekey_replaced(report);
        let replaced = self.follow_replaced(report, rekeyed);
        let mut applied = self.apply_report(report);
        if let Some(replaced) = replaced {
            applied.orders.replaced.insert(0, replaced);
        }
        applied
    }

    fn update_from_cancel_response<AssetKey>(
        &mut self,
        response: &OrderResponseCancel<ExchangeKey, AssetKey, InstrumentKey>,
    ) where
        AssetKey: Debug + Clone,
    {
        let Entry::Occupied(mut order) = self.0.entry(response.key.cid.clone()) else {
            warn!(
                exchange = ?response.key.exchange,
                instrument = ?response.key.instrument,
                strategy = %response.key.strategy,
                cid = %response.key.cid,
                update = ?response,
                "OrderManager received an OrderResponseCancel for untracked order - ignoring"
            );
            return;
        };

        match (&order.get().order.state, &response.state) {
            (
                ActiveOrderState::OpenInFlight(_)
                | ActiveOrderState::Open(_)
                | ActiveOrderState::AmendInFlight(_),
                Ok(_),
            ) => {
                warn!(
                    exchange = ?response.key.exchange,
                    instrument = ?response.key.instrument,
                    strategy = %response.key.strategy,
                    cid = %response.key.cid,
                    update = ?response,
                    "OrderManager received Ok(Cancelled) for tracked order not CancelInFlight - removing"
                );
                self.1.insert(order.remove().order.key.cid);
            }
            (ActiveOrderState::CancelInFlight(_), Ok(_)) => {
                debug!(
                    exchange = ?response.key.exchange,
                    instrument = ?response.key.instrument,
                    strategy = %response.key.strategy,
                    cid = %response.key.cid,
                    update = ?response,
                    "OrderManager received Ok(Cancelled) for tracked order CancelInFlight - removing"
                );
                self.1.insert(order.remove().order.key.cid);
            }
            (
                ActiveOrderState::OpenInFlight(_)
                | ActiveOrderState::Open(_)
                | ActiveOrderState::AmendInFlight(_),
                Err(error),
            ) => {
                warn!(
                    exchange = ?response.key.exchange,
                    instrument = ?response.key.instrument,
                    strategy = %response.key.strategy,
                    cid = %response.key.cid,
                    update = ?response,
                    ?error,
                    "OrderManager received Err(Cancelled) for tracked order not CancelInFlight - ignoring"
                );
            }
            (ActiveOrderState::CancelInFlight(in_flight_cancel), Err(error)) => {
                // Expected, keep move to Open
                if let Some(open) = &in_flight_cancel.order {
                    // Terms adopted behind the cancel may have left the order nothing to fill,
                    // which the cancel's answer was to settle. Refused, the order has ended.
                    if nothing_left(open, order.get().order.quantity, &response.key) {
                        debug!(
                            exchange = ?response.key.exchange,
                            instrument = ?response.key.instrument,
                            strategy = %response.key.strategy,
                            cid = %response.key.cid,
                            ?error,
                            "OrderManager received Err(Cancelled) for an order with nothing left to fill - removing"
                        );
                        self.1.insert(order.remove().order.key.cid);
                        return;
                    }
                    debug!(
                        exchange = ?response.key.exchange,
                        instrument = ?response.key.instrument,
                        strategy = %response.key.strategy,
                        cid = %response.key.cid,
                        update = ?response,
                        ?error,
                        "OrderManager received Err(Cancelled) for previously Open order - setting Open"
                    );
                    order.get_mut().order.state = ActiveOrderState::Open(open.clone())
                } else {
                    debug!(
                        exchange = ?response.key.exchange,
                        instrument = ?response.key.instrument,
                        strategy = %response.key.strategy,
                        cid = %response.key.cid,
                        update = ?response,
                        ?error,
                        "OrderManager received Err(Cancelled) for previously non-Open order - removing"
                    );
                    // Likely previously OpenInFlight, and attempted cancel before Open snapshot
                    // -> it's expected that an Order snapshot is inbound. Not retired, so that
                    // snapshot is still applied when it arrives.
                    order.remove();
                }
            }
        }
    }

    fn update_from_amend_response<AssetKey>(
        &mut self,
        response: &OrderResponseAmend<ExchangeKey, AssetKey, InstrumentKey>,
    ) where
        AssetKey: Debug + Clone,
    {
        let Entry::Occupied(mut entry) = self.0.entry(response.key.cid.clone()) else {
            debug!(
                exchange = ?response.key.exchange,
                instrument = ?response.key.instrument,
                strategy = %response.key.strategy,
                cid = %response.key.cid,
                update = ?response,
                "OrderManager received an OrderResponseAmend for an untracked order - ignoring"
            );
            return;
        };

        let amended = match &response.state {
            Ok(amended) => amended,
            // Unknown: the amend may yet be made. The `ExecutionManager` looks the order up and
            // answers again with what the venue lists.
            Err(error) if error.is_outcome_unknown() => {
                debug!(
                    exchange = ?response.key.exchange,
                    instrument = ?response.key.instrument,
                    strategy = %response.key.strategy,
                    cid = %response.key.cid,
                    ?error,
                    "OrderManager received an amend whose outcome is unknown - leaving it in flight"
                );
                return;
            }
            Err(error) => {
                if let ActiveOrderState::AmendInFlight(amend) = &entry.get().order.state {
                    debug!(
                        exchange = ?response.key.exchange,
                        instrument = ?response.key.instrument,
                        strategy = %response.key.strategy,
                        cid = %response.key.cid,
                        ?error,
                        "OrderManager received a refused amend - the order is unchanged"
                    );
                    let open = amend.order.clone();
                    // A fill can reach the old total while a raise is in flight: refused, the
                    // raise leaves the order with nothing to fill.
                    if open.quantity_remaining(entry.get().order.quantity) <= Decimal::ZERO {
                        debug!(
                            exchange = ?response.key.exchange,
                            instrument = ?response.key.instrument,
                            strategy = %response.key.strategy,
                            cid = %response.key.cid,
                            "OrderManager removing an order filled while its refused amend was in flight"
                        );
                        self.1.insert(entry.remove().order.key.cid);
                        return;
                    }
                    entry.get_mut().order.state = ActiveOrderState::Open(open);
                } else {
                    debug!(
                        exchange = ?response.key.exchange,
                        instrument = ?response.key.instrument,
                        strategy = %response.key.strategy,
                        cid = %response.key.cid,
                        ?error,
                        state = ?entry.get().order.state,
                        "OrderManager received a refused amend for an order not AmendInFlight - ignoring"
                    );
                }
                return;
            }
        };

        let order = &mut entry.get_mut().order;
        let held = match &mut order.state {
            ActiveOrderState::OpenInFlight(_) => {
                warn!(
                    exchange = ?response.key.exchange,
                    instrument = ?response.key.instrument,
                    strategy = %response.key.strategy,
                    cid = %response.key.cid,
                    update = ?response,
                    "OrderManager received an amend for an OpenInFlight order - ignoring"
                );
                return;
            }
            // No amend was recorded: a duplicate or stale answer, whose values may be older than
            // what the order has since been reported as.
            ActiveOrderState::Open(_) => {
                warn!(
                    exchange = ?response.key.exchange,
                    instrument = ?response.key.instrument,
                    strategy = %response.key.strategy,
                    cid = %response.key.cid,
                    update = ?response,
                    "OrderManager received an amend for an order with no amend in flight - ignoring"
                );
                return;
            }
            ActiveOrderState::AmendInFlight(AmendInFlight { order: open, .. }) => {
                Some(open.clone())
            }
            ActiveOrderState::CancelInFlight(cancel) => cancel.order.clone(),
        };

        // The answer must be for the venue order amended: under its previous identifier when the
        // venue issued a new one. Only two different assigned identifiers say it is not, as for
        // snapshots: an order the venue had not named may be named in the answer. A report linked
        // to the order before the amend may already have moved it to the new identifier, which
        // the answer then names as the order's.
        if let Some(held) = &held {
            let amended_id = amended.previous_id.as_ref().unwrap_or(&amended.open.id);
            if held.id.contradicts(amended_id) && !held.id.is_same_order_as(&amended.open.id) {
                warn!(
                    exchange = ?response.key.exchange,
                    instrument = ?response.key.instrument,
                    strategy = %response.key.strategy,
                    cid = %response.key.cid,
                    tracked_id = ?held.id,
                    update = ?response,
                    "OrderManager received an amend of another venue order - ignoring"
                );
                return;
            }
        }

        // The values the venue confirmed, behind a cancel too: a late answer still says what the
        // order was while it rested.
        order.price = amended.price;
        order.quantity = amended.quantity;
        order.kind = amended.kind;

        // The venue's state after the amend, unless a later report already advanced the held one.
        // The identifier is the amend's either way: the venue may have issued a new one.
        let open = match held {
            Some(held) if !held.is_superseded_by(&amended.open) => Open {
                id: amended.open.id.clone(),
                ..held
            },
            _ => amended.open.clone(),
        };

        if open.quantity_remaining(order.quantity) <= Decimal::ZERO {
            debug!(
                exchange = ?response.key.exchange,
                instrument = ?response.key.instrument,
                strategy = %response.key.strategy,
                cid = %response.key.cid,
                update = ?response,
                "OrderManager removing an order its amend left with nothing to fill"
            );
            self.1.insert(entry.remove().order.key.cid);
            return;
        }

        debug!(
            exchange = ?response.key.exchange,
            instrument = ?response.key.instrument,
            strategy = %response.key.strategy,
            cid = %response.key.cid,
            update = ?response,
            "OrderManager applied an amend"
        );
        let time_exchange = open.time_exchange;
        match &mut order.state {
            ActiveOrderState::CancelInFlight(cancel) => cancel.order = Some(open),
            state => *state = ActiveOrderState::Open(open),
        }
        // A late report of the order before the amend names the identifier it replaced.
        if let Some(previous) = amended
            .previous_id
            .as_ref()
            .and_then(VenueOrderId::assigned)
        {
            entry.get_mut().supersede(previous.clone());
        }
        // The venue's answer states the terms just applied, at the time of the state it gives:
        // the whole order except its time in force, which an amend does not change.
        entry
            .get_mut()
            .terms_times
            .stated_at(ReportedTerms::ALL.with_time_in_force(false), time_exchange);
    }
}

impl<ExchangeKey, InstrumentKey> Orders<ExchangeKey, InstrumentKey>
where
    ExchangeKey: Debug + Clone,
    InstrumentKey: Debug + Clone,
{
    /// Move the order `report` replaces to the report's client order id, when the report links
    /// one tracked under another (see "Orders replaced at the venue"), returning the client order
    /// id it was tracked under.
    ///
    /// The order replaced is the one under the client order id the link names, or, when it names
    /// none, the one addressed by its venue order id, now or before a replace. Nothing moves when
    /// the report's own client order id is tracked or retired, when the link names no order
    /// tracked, when the order under the client order id named is addressed by another venue
    /// order id, or when the report names no venue order id.
    fn rekey_replaced<AssetKey>(
        &mut self,
        report: &OrderReport<ExchangeKey, AssetKey, InstrumentKey>,
    ) -> Option<ClientOrderId>
    where
        AssetKey: Debug,
    {
        let replaces = report.replaces.as_ref()?;
        let key = &report.order.key;
        // A report under a client order id already retired is late: a replay of a replace already
        // followed, perhaps one the order has since been moved on from, and moves nothing back.
        if self.0.contains_key(&key.cid)
            || self.1.contains(&key.cid)
            || reported_assigned_id(&report.order.state).is_none()
        {
            return None;
        }
        let names_replaced = |tracked: &TrackedOrder<ExchangeKey, InstrumentKey>| {
            tracked.current_id() == Some(&replaces.id) || tracked.is_superseded(&replaces.id)
        };
        let old = match &replaces.cid {
            // A link naming the report's own client order id finds nothing here: that id is not
            // tracked (checked above).
            Some(old) => {
                let tracked = self.0.get(old)?;
                // An order the venue has named is the one replaced only under the id named.
                if tracked.current_id().is_some() && !names_replaced(tracked) {
                    warn!(
                        exchange = ?key.exchange,
                        instrument = ?key.instrument,
                        cid = %key.cid,
                        replaced_cid = %old,
                        replaced_id = %replaces.id,
                        tracked = ?tracked.order,
                        "OrderManager received a report replacing an order it tracks under another venue order id - not moving it"
                    );
                    return None;
                }
                old.clone()
            }
            None => self
                .0
                .iter()
                .find(|(_, tracked)| names_replaced(tracked))
                .map(|(cid, _)| cid.clone())?,
        };

        let mut tracked = self.0.remove(&old)?;
        // A late report under the old id is ignored, as for any order retired.
        self.1.insert(old.clone());
        let dropped = matches!(
            tracked.order.state,
            ActiveOrderState::AmendInFlight(_) | ActiveOrderState::CancelInFlight(_)
        );
        // The venue answers a request in flight under the old id, which is no longer tracked, so
        // the order goes back to the state the request was sent from.
        tracked.order.state = match tracked.order.state {
            ActiveOrderState::Open(open)
            | ActiveOrderState::AmendInFlight(AmendInFlight { order: open, .. })
            | ActiveOrderState::CancelInFlight(CancelInFlight {
                order: Some(open), ..
            }) => ActiveOrderState::Open(open),
            ActiveOrderState::CancelInFlight(CancelInFlight {
                order: None,
                time_sent,
            }) => ActiveOrderState::OpenInFlight(OpenInFlight::new(time_sent)),
            in_flight @ ActiveOrderState::OpenInFlight(_) => in_flight,
        };
        tracked.order.key.cid = key.cid.clone();
        if dropped {
            warn!(
                exchange = ?key.exchange,
                instrument = ?key.instrument,
                strategy = %tracked.order.key.strategy,
                cid = %key.cid,
                replaced_cid = %old,
                "OrderManager moved an order replaced at the venue to its new ClientOrderId, dropping the request in flight under the old one"
            );
        } else {
            info!(
                exchange = ?key.exchange,
                instrument = ?key.instrument,
                strategy = %tracked.order.key.strategy,
                cid = %key.cid,
                replaced_cid = %old,
                "OrderManager moved an order replaced at the venue to its new ClientOrderId"
            );
        }
        self.0.insert(key.cid.clone(), tracked);
        Some(old)
    }

    /// Move the order tracked under `report`'s client order id to the venue order id the report
    /// names, when the report links the one it holds (see "Orders replaced at the venue"),
    /// recording the id replaced. Returns the replace followed, or the move to a new client order
    /// id `rekeyed` made, if any.
    ///
    /// A report naming an id the order was already replaced from is late, and moves nothing. A
    /// link naming an id the order is not addressed by moves nothing either, unless the venue has
    /// not named the order, when the id is recorded as one it was replaced from.
    fn follow_replaced<AssetKey>(
        &mut self,
        report: &OrderReport<ExchangeKey, AssetKey, InstrumentKey>,
        rekeyed: Option<ClientOrderId>,
    ) -> Option<OrderReplaced<ExchangeKey, InstrumentKey>> {
        let replaces = report.replaces.as_ref()?;
        let tracked = self.0.get_mut(&report.order.key.cid)?;
        // A link on a report that names no venue order id is ignored: the order would be left
        // addressed by none (see the producer obligations on `OrderReport`).
        let reported = reported_assigned_id(&report.order.state)?;
        let late = tracked.is_superseded(&reported);
        let held = tracked.current_id().cloned();
        let moved = !late
            && reported != replaces.id
            && held.as_ref().is_none_or(|held| held == &replaces.id);
        if moved {
            if let Some(open) = tracked.order.state.open_meta_mut() {
                open.id = VenueOrderId::Assigned(reported.clone());
            }
            tracked.supersede(replaces.id.clone());
        }
        let followed = moved && held.is_some();
        if !followed && rekeyed.is_none() {
            return None;
        }
        let key = &tracked.order.key;
        info!(
            exchange = ?key.exchange,
            instrument = ?key.instrument,
            strategy = %key.strategy,
            cid = %key.cid,
            id = %reported,
            replaced_id = %replaces.id,
            replaced_cid = ?rekeyed,
            "OrderManager followed an order replaced at the venue"
        );
        let mut replaced = Replaces::new(replaces.id.clone());
        if let Some(rekeyed) = rekeyed {
            replaced = replaced.with_cid(rekeyed);
        }
        Some(OrderReplaced {
            key: key.clone(),
            id: reported,
            replaced,
        })
    }

    /// [`OrderManager::update_from_order_snapshot`] once a replace the report links is followed.
    // One match over the tracked and the incoming state, an arm per pair. The lint counts each
    // arm's log macro expansion, and splitting the match would scatter the transition table.
    #[allow(clippy::cognitive_complexity)]
    fn apply_report<AssetKey>(
        &mut self,
        report: &OrderReport<ExchangeKey, AssetKey, InstrumentKey>,
    ) -> SnapshotApplied<ExchangeKey, InstrumentKey>
    where
        AssetKey: Debug + Clone,
    {
        let snapshot = &report.order;

        let (mut current_entry, update) = match (
            self.0.entry(snapshot.key.cid.clone()),
            snapshot.to_active(),
        ) {
            // Order untracked: track it, or ignore it
            (Entry::Vacant(entry), update) => {
                return SnapshotApplied::working(update_untracked(
                    entry,
                    &mut self.1,
                    report,
                    update,
                ));
            }

            // A refusal of a request under a client order id already in use says nothing about
            // the order holding it, so it does not end that order. Unless the order tracked is
            // that request's own, still in flight, which the refusal does end.
            (Entry::Occupied(entry), None)
                if is_duplicate_cid_refusal(&snapshot.state)
                    && !matches!(entry.get().order.state, ActiveOrderState::OpenInFlight(_)) =>
            {
                warn!(
                    exchange = ?snapshot.key.exchange,
                    instrument = ?snapshot.key.instrument,
                    strategy = %snapshot.key.strategy,
                    cid = %snapshot.key.cid,
                    update = ?snapshot,
                    "OrderManager received a duplicate ClientOrderId refusal for a tracked order it did not send - ignoring"
                );
                return SnapshotApplied::default();
            }

            // An ended report of the order before a replace, such as the cancel of the old order
            // a venue reports alongside its replacement, says nothing of the order now
            (Entry::Occupied(entry), None)
                if !entry.get().superseded_ids.is_empty()
                    && reported_id(&snapshot.state)
                        .is_some_and(|id| entry.get().names_superseded(&id)) =>
            {
                debug!(
                    exchange = ?snapshot.key.exchange,
                    instrument = ?snapshot.key.instrument,
                    strategy = %snapshot.key.strategy,
                    cid = %snapshot.key.cid,
                    update = ?snapshot,
                    "OrderManager received an ended report naming a venue order id the order was replaced from - ignoring"
                );
                return SnapshotApplied::default();
            }

            // Order tracked, input Snapshot is InactiveOrderState (ie/ finished), so remove
            (Entry::Occupied(entry), None) => {
                debug!(
                    exchange = ?snapshot.key.exchange,
                    instrument = ?snapshot.key.instrument,
                    strategy = %snapshot.key.strategy,
                    cid = %snapshot.key.cid,
                    update = ?snapshot,
                    "OrderManager received inactive order snapshot for tracked order - removing"
                );
                self.1.insert(entry.remove().order.key.cid);
                return SnapshotApplied::retired(OrderReportsApplied::default());
            }

            // Order tracked, input Snapshot is ActiveOrderState, so forward for further processing
            (Entry::Occupied(entry), Some(update)) => (entry, update),
        };

        match (&current_entry.get().order.state, update.state) {
            (ActiveOrderState::OpenInFlight(_), ActiveOrderState::OpenInFlight(_)) => {
                warn!(
                    exchange = ?snapshot.key.exchange,
                    instrument = ?snapshot.key.instrument,
                    strategy = %snapshot.key.strategy,
                    cid = %snapshot.key.cid,
                    update = ?snapshot,
                    "OrderManager received a duplicate OpenInFlight recording - ignoring"
                );
                SnapshotApplied::default()
            }
            (ActiveOrderState::OpenInFlight(_), ActiveOrderState::Open(open)) => {
                debug!(
                    exchange = ?snapshot.key.exchange,
                    instrument = ?snapshot.key.instrument,
                    strategy = %snapshot.key.strategy,
                    cid = %snapshot.key.cid,
                    update = ?snapshot,
                    "OrderManager transitioned an OpenInFlight order to Open"
                );
                // The venue's first report of the order: what it says of the terms is the first
                // word the venue has given on them, so there is no earlier stamp to order against.
                let terms = reconcile_terms(current_entry.get_mut(), report, &open, None);
                if nothing_left(&open, current_entry.get().order.quantity, &snapshot.key) {
                    self.1.insert(current_entry.remove().order.key.cid);
                    return SnapshotApplied::retired(terms);
                }
                current_entry.get_mut().order.state = ActiveOrderState::Open(open);
                SnapshotApplied::working(terms)
            }
            (ActiveOrderState::OpenInFlight(_), ActiveOrderState::CancelInFlight(update)) => {
                debug!(
                    exchange = ?snapshot.key.exchange,
                    instrument = ?snapshot.key.instrument,
                    strategy = %snapshot.key.strategy,
                    cid = %snapshot.key.cid,
                    update = ?snapshot,
                    "OrderManager transitioned an OpenInFlight order to CancelInFlight"
                );
                current_entry.get_mut().order.state = ActiveOrderState::CancelInFlight(update);
                SnapshotApplied::default()
            }
            (ActiveOrderState::Open(_), ActiveOrderState::OpenInFlight(_)) => {
                warn!(
                    exchange = ?snapshot.key.exchange,
                    instrument = ?snapshot.key.instrument,
                    strategy = %snapshot.key.strategy,
                    cid = %snapshot.key.cid,
                    update = ?snapshot,
                    "OrderManager received an OpenInFlight recording for an Open order - ignoring"
                );
                SnapshotApplied::default()
            }
            (ActiveOrderState::Open(current), ActiveOrderState::Open(open)) => {
                // This order was reached by `ClientOrderId` alone. If both states name a venue
                // order and the two disagree, one client id is covering two distinct exchange
                // orders, and applying the update would overwrite a live order's price, quantity
                // and cumulative fill -- or retire it outright -- on the strength of a different
                // order's state.
                //
                // `contradicts` rejects only on positive evidence. An order the venue has not
                // named carries nothing to compare, and two such orders are not shown to be the
                // same order by both lacking an identifier.
                if current.id.contradicts(&open.id) {
                    if !is_late_replaced(current_entry.get(), &open.id, snapshot) {
                        error!(
                            exchange = ?snapshot.key.exchange,
                            instrument = ?snapshot.key.instrument,
                            strategy = %snapshot.key.strategy,
                            cid = %snapshot.key.cid,
                            tracked_id = %current.id,
                            update_id = %open.id,
                            "OrderManager received an Open snapshot naming a different exchange order than the one tracked under this ClientOrderId - ignoring"
                        );
                    }
                    return SnapshotApplied::default();
                }

                // Terms are ordered by their own time, whether or not the state supersedes the one
                // held (see "Terms times" on `TrackedOrder`).
                let superseded = current.is_superseded_by(&open);
                let terms = reconcile_terms(current_entry.get_mut(), report, &open, None);

                if superseded {
                    // A venue may report a completed fill as an Open snapshot with nothing left to
                    // fill, rather than as a distinct terminal state. That order is finished, so
                    // it stops being tracked -- exactly as the OpenInFlight -> Open arm above
                    // already does. Retaining it would leave the strategy reading a resting order
                    // that no longer exists on the exchange.
                    //
                    // Nested inside the ordering gate deliberately, so a snapshot that does not
                    // supersede the tracked state cannot retire it. Note that an earlier-stamped
                    // snapshot claiming a full fill does reach here: it reports a cumulative above
                    // a live order's, which is exactly the evidence the fill key tests for, and
                    // an order the venue has once reported complete cannot become live again.
                    if nothing_left(&open, current_entry.get().order.quantity, &snapshot.key) {
                        debug!(
                            exchange = ?snapshot.key.exchange,
                            instrument = ?snapshot.key.instrument,
                            strategy = %snapshot.key.strategy,
                            cid = %snapshot.key.cid,
                            update = ?snapshot,
                            "OrderManager removing an Open order a more recent snapshot reports as fully filled"
                        );
                        self.1.insert(current_entry.remove().order.key.cid);
                        return SnapshotApplied::retired(terms);
                    }

                    debug!(
                        exchange = ?snapshot.key.exchange,
                        instrument = ?snapshot.key.instrument,
                        strategy = %snapshot.key.strategy,
                        cid = %snapshot.key.cid,
                        update = ?snapshot,
                        "OrderManager updating an Open order from a more recent snapshot"
                    );
                    current_entry.get_mut().order.state = ActiveOrderState::Open(open);
                    SnapshotApplied::working(terms)
                } else {
                    debug!(
                        exchange = ?snapshot.key.exchange,
                        instrument = ?snapshot.key.instrument,
                        strategy = %snapshot.key.strategy,
                        cid = %snapshot.key.cid,
                        update = ?snapshot,
                        "OrderManager received an Open snapshot that does not supersede the state held - keeping the state, reading its terms"
                    );
                    // A quantity cut to what the state held has filled leaves nothing to fill.
                    let tracked = &current_entry.get().order;
                    let finished = !terms.changed.is_empty()
                        && matches!(
                            &tracked.state,
                            ActiveOrderState::Open(held)
                                if nothing_left(held, tracked.quantity, &snapshot.key)
                        );
                    if finished {
                        debug!(
                            exchange = ?snapshot.key.exchange,
                            instrument = ?snapshot.key.instrument,
                            strategy = %snapshot.key.strategy,
                            cid = %snapshot.key.cid,
                            update = ?snapshot,
                            "OrderManager removing an Open order whose adopted terms leave nothing to fill"
                        );
                        self.1.insert(current_entry.remove().order.key.cid);
                        return SnapshotApplied::retired(terms);
                    }
                    SnapshotApplied::working(terms)
                }
            }
            (ActiveOrderState::Open(current), ActiveOrderState::CancelInFlight(mut update)) => {
                debug!(
                    exchange = ?snapshot.key.exchange,
                    instrument = ?snapshot.key.instrument,
                    strategy = %snapshot.key.strategy,
                    cid = %snapshot.key.cid,
                    update = ?snapshot,
                    "OrderManager transitioned an Open order to CancelInFlight"
                );

                // Ensure next CancelInFlight.Open is populated and the most recent.
                //
                // A carried `Open` naming a different venue order is dropped rather than adopted,
                // on the reasoning in the `Open` -> `Open` arm. The transition itself still
                // happens: a cancel is in flight either way, and falling back to the tracked
                // `Open` keeps this order's own state rather than a foreign one's.
                let latest_open = update
                    .order
                    .take()
                    .filter(|update| {
                        !current.id.contradicts(&update.id) && current.is_superseded_by(update)
                    })
                    .unwrap_or_else(|| current.clone());

                current_entry.get_mut().order.state =
                    ActiveOrderState::CancelInFlight(CancelInFlight {
                        order: Some(latest_open),
                        time_sent: update.time_sent,
                    });
                SnapshotApplied::default()
            }
            (ActiveOrderState::CancelInFlight(_), ActiveOrderState::OpenInFlight(_)) => {
                error!(
                    exchange = ?snapshot.key.exchange,
                    instrument = ?snapshot.key.instrument,
                    strategy = %snapshot.key.strategy,
                    cid = %snapshot.key.cid,
                    update = ?snapshot,
                    "OrderManager received an OpenInFlight recording for a CancelInFlight - ignoring"
                );
                SnapshotApplied::default()
            }
            (ActiveOrderState::CancelInFlight(current), ActiveOrderState::Open(update)) => {
                // Reached by `ClientOrderId` alone; see the `Open` -> `Open` arm.
                if current
                    .order
                    .as_ref()
                    .is_some_and(|held| held.id.contradicts(&update.id))
                {
                    if !is_late_replaced(current_entry.get(), &update.id, snapshot) {
                        error!(
                            exchange = ?snapshot.key.exchange,
                            instrument = ?snapshot.key.instrument,
                            strategy = %snapshot.key.strategy,
                            cid = %snapshot.key.cid,
                            update_id = %update.id,
                            "OrderManager received an Open snapshot naming a different exchange order than the tracked CancelInFlight - ignoring"
                        );
                    }
                    return SnapshotApplied::default();
                }

                debug!(
                    exchange = ?snapshot.key.exchange,
                    instrument = ?snapshot.key.instrument,
                    strategy = %snapshot.key.strategy,
                    cid = %snapshot.key.cid,
                    update = ?snapshot,
                    "OrderManager received an Open order snapshot for a CancelInFlight - updating CancelInFlight.Open"
                );

                // Check if the update Open supersedes the one the cancel carries.
                let update_open_is_latest = current
                    .order
                    .as_ref()
                    .is_none_or(|current| current.is_superseded_by(&update));
                let time_sent = current.time_sent;

                // Terms are adopted as for an `Open` order, but the order is not retired whatever
                // they leave it: the cancel's answer ends it.
                let terms = reconcile_terms(current_entry.get_mut(), report, &update, None);
                if update_open_is_latest {
                    current_entry.get_mut().order.state =
                        ActiveOrderState::CancelInFlight(CancelInFlight {
                            order: Some(update),
                            time_sent,
                        });
                }
                SnapshotApplied::working(terms)
            }
            (ActiveOrderState::CancelInFlight(_), ActiveOrderState::CancelInFlight(_)) => {
                warn!(
                    exchange = ?snapshot.key.exchange,
                    instrument = ?snapshot.key.instrument,
                    strategy = %snapshot.key.strategy,
                    cid = %snapshot.key.cid,
                    update = ?snapshot,
                    "OrderManager received a duplicate CancelInFlight recording - ignoring"
                );
                SnapshotApplied::default()
            }
            (ActiveOrderState::AmendInFlight(_), ActiveOrderState::OpenInFlight(_)) => {
                warn!(
                    exchange = ?snapshot.key.exchange,
                    instrument = ?snapshot.key.instrument,
                    strategy = %snapshot.key.strategy,
                    cid = %snapshot.key.cid,
                    update = ?snapshot,
                    "OrderManager received an OpenInFlight recording for an AmendInFlight order - ignoring"
                );
                SnapshotApplied::default()
            }
            // The order keeps working while its amend is in flight: a later report of it advances
            // the state the amend carries, and only the amend's own answer ends the amend.
            (ActiveOrderState::AmendInFlight(current), ActiveOrderState::Open(open)) => {
                if current.order.id.contradicts(&open.id) {
                    if !is_late_replaced(current_entry.get(), &open.id, snapshot) {
                        error!(
                            exchange = ?snapshot.key.exchange,
                            instrument = ?snapshot.key.instrument,
                            strategy = %snapshot.key.strategy,
                            cid = %snapshot.key.cid,
                            tracked_id = %current.order.id,
                            update_id = %open.id,
                            "OrderManager received an Open snapshot naming a different exchange order than the tracked AmendInFlight - ignoring"
                        );
                    }
                    return SnapshotApplied::default();
                }
                let superseded = current.order.is_superseded_by(&open);
                // Nothing is adopted while the amend is in flight: its answer settles the terms. A
                // report matching neither the order before the amend nor after it is reported.
                let patch = current.patch;
                let terms = reconcile_terms(current_entry.get_mut(), report, &open, Some(&patch));
                if !superseded {
                    debug!(
                        exchange = ?snapshot.key.exchange,
                        instrument = ?snapshot.key.instrument,
                        strategy = %snapshot.key.strategy,
                        cid = %snapshot.key.cid,
                        update = ?snapshot,
                        "OrderManager received an Open snapshot that does not supersede the AmendInFlight state held - keeping the state, comparing its terms"
                    );
                    return SnapshotApplied::working(terms);
                }
                // The venue may already have made a raise and filled beyond the old total, as in
                // `update_from_fill`.
                let tracked = current_entry.get().order.quantity;
                let quantity = patch
                    .quantity
                    .map_or(tracked, |amended| amended.max(tracked));
                if nothing_left(&open, quantity, &snapshot.key) {
                    debug!(
                        exchange = ?snapshot.key.exchange,
                        instrument = ?snapshot.key.instrument,
                        strategy = %snapshot.key.strategy,
                        cid = %snapshot.key.cid,
                        update = ?snapshot,
                        "OrderManager removing an AmendInFlight order a more recent snapshot reports as fully filled"
                    );
                    self.1.insert(current_entry.remove().order.key.cid);
                    return SnapshotApplied::retired(terms);
                }
                if let ActiveOrderState::AmendInFlight(amend) =
                    &mut current_entry.get_mut().order.state
                {
                    amend.order = open;
                }
                SnapshotApplied::working(terms)
            }
            // A cancel supersedes the amend: the order is cancelled whatever the amend's answer.
            (
                ActiveOrderState::AmendInFlight(current),
                ActiveOrderState::CancelInFlight(mut update),
            ) => {
                debug!(
                    exchange = ?snapshot.key.exchange,
                    instrument = ?snapshot.key.instrument,
                    strategy = %snapshot.key.strategy,
                    cid = %snapshot.key.cid,
                    update = ?snapshot,
                    "OrderManager transitioned an AmendInFlight order to CancelInFlight"
                );
                let current = &current.order;
                let latest_open = update
                    .order
                    .take()
                    .filter(|update| {
                        !current.id.contradicts(&update.id) && current.is_superseded_by(update)
                    })
                    .unwrap_or_else(|| current.clone());

                current_entry.get_mut().order.state =
                    ActiveOrderState::CancelInFlight(CancelInFlight {
                        order: Some(latest_open),
                        time_sent: update.time_sent,
                    });
                SnapshotApplied::default()
            }
            // An amend is recorded only by `record_in_flight_amend`, never by a snapshot.
            (_, ActiveOrderState::AmendInFlight(_)) => {
                warn!(
                    exchange = ?snapshot.key.exchange,
                    instrument = ?snapshot.key.instrument,
                    strategy = %snapshot.key.strategy,
                    cid = %snapshot.key.cid,
                    update = ?snapshot,
                    "OrderManager received an AmendInFlight snapshot - ignoring"
                );
                SnapshotApplied::default()
            }
        }
    }
}

/// The venue order id `state` names: a working order's, unless it carries no working state, or an
/// ended order's. `None` for a failed open, and for an order in flight with no state reported.
fn reported_id<AssetKey, InstrumentKey>(
    state: &OrderState<AssetKey, InstrumentKey>,
) -> Option<VenueOrderId> {
    match state {
        OrderState::Active(active) => active.open_meta().map(|open| open.id.clone()),
        OrderState::Inactive(InactiveOrderState::Cancelled(cancelled)) => {
            Some(VenueOrderId::Assigned(cancelled.id.clone()))
        }
        OrderState::Inactive(InactiveOrderState::FullyFilled(filled)) => {
            Some(VenueOrderId::Assigned(filled.id.clone()))
        }
        OrderState::Inactive(InactiveOrderState::Expired(expired)) => {
            Some(VenueOrderId::Assigned(expired.id.clone()))
        }
        OrderState::Inactive(InactiveOrderState::OpenFailed(_)) => None,
    }
}

/// The venue order id `state` names, when the venue has assigned one (see [`reported_id`]).
fn reported_assigned_id<AssetKey, InstrumentKey>(
    state: &OrderState<AssetKey, InstrumentKey>,
) -> Option<OrderId> {
    match reported_id(state)? {
        VenueOrderId::Assigned(id) => Some(id),
        VenueOrderId::ClientAssigned => None,
    }
}

/// Whether `id`, which a working report names in place of the id `tracked` holds, is one the
/// order was replaced from: the report is of the order before a replace, and is ignored as late,
/// which this logs. Otherwise the caller refuses it as a report of another order.
fn is_late_replaced<ExchangeKey, AssetKey, InstrumentKey>(
    tracked: &TrackedOrder<ExchangeKey, InstrumentKey>,
    id: &VenueOrderId,
    snapshot: &Order<ExchangeKey, InstrumentKey, OrderState<AssetKey, InstrumentKey>>,
) -> bool
where
    ExchangeKey: Debug,
    AssetKey: Debug,
    InstrumentKey: Debug,
{
    let late = tracked.names_superseded(id);
    if late {
        debug!(
            exchange = ?snapshot.key.exchange,
            instrument = ?snapshot.key.instrument,
            strategy = %snapshot.key.strategy,
            cid = %snapshot.key.cid,
            update = ?snapshot,
            "OrderManager received a working report naming a venue order id the order was replaced from - ignoring"
        );
    }
    late
}

/// Reconcile `tracked_order`'s terms with those `report` states (see [`terms`]), writing what is
/// adopted onto its order. `open` is the report's state, which the caller has not yet applied.
///
/// A stated term is not read when the report is stamped earlier than the venue last stated that
/// term (see "Terms times" on [`TrackedOrder`]). No term is read when the report reports less
/// filled than the state held: fills only grow, so such a report predates that state whatever its
/// stamp says. A term read with no amend in flight records the report's time, even when it
/// changes nothing: it says the value held was still the venue's then.
///
/// With `amend` set, an amend is in flight: nothing is adopted, and the report is compared with
/// the order before and after the amend. The terms times are left as they are, since the amend's
/// answer settles the terms.
fn reconcile_terms<ExchangeKey, AssetKey, InstrumentKey>(
    tracked_order: &mut TrackedOrder<ExchangeKey, InstrumentKey>,
    report: &OrderReport<ExchangeKey, AssetKey, InstrumentKey>,
    open: &Open,
    amend: Option<&AmendPatch>,
) -> OrderReportsApplied<ExchangeKey, InstrumentKey>
where
    ExchangeKey: Debug + Clone,
    InstrumentKey: Debug + Clone,
{
    let mut applied = OrderReportsApplied::default();
    if report.reported.is_none() {
        return applied;
    }
    let behind_held = tracked_order
        .order
        .state
        .open_meta()
        .is_some_and(|held| open.filled_quantity < held.filled_quantity);
    let stated = if behind_held {
        ReportedTerms::NONE
    } else {
        tracked_order
            .terms_times
            .in_sequence(report.reported, open.time_exchange)
    };
    if stated != report.reported {
        let key = &tracked_order.order.key;
        debug!(
            exchange = ?key.exchange,
            instrument = ?key.instrument,
            strategy = %key.strategy,
            cid = %key.cid,
            reported = ?report.reported,
            read = ?stated,
            behind_held,
            "OrderManager received terms older than those it holds for an order - not reading them"
        );
    }
    if stated.is_none() {
        return applied;
    }
    if amend.is_none() {
        tracked_order
            .terms_times
            .stated_at(stated, open.time_exchange);
    }
    let order = &mut tracked_order.order;
    let held_terms = OrderTerms::of(order);
    let reported = OrderTerms::of(&report.order);
    let Reconciled { terms, diverged } = reconcile(&held_terms, &reported, stated, amend);

    if terms != held_terms {
        terms.apply_to(order);
        info!(
            exchange = ?order.key.exchange,
            instrument = ?order.key.instrument,
            strategy = %order.key.strategy,
            cid = %order.key.cid,
            from = ?held_terms,
            to = ?terms,
            "OrderManager adopted the terms a venue reported for an order it tracks"
        );
        applied.changed.push(OrderTermsChanged {
            key: order.key.clone(),
            from: held_terms,
            to: terms,
        });
    }

    if !diverged.is_none() {
        warn!(
            exchange = ?order.key.exchange,
            instrument = ?order.key.instrument,
            strategy = %order.key.strategy,
            cid = %order.key.cid,
            tracked = ?terms,
            ?reported,
            terms = ?diverged,
            amend_in_flight = amend.is_some(),
            "OrderManager received terms for an order it tracks that it does not adopt"
        );
        applied.diverged.push(OrderTermsDiverged {
            key: order.key.clone(),
            tracked: terms,
            reported,
            terms: diverged,
            amend_in_flight: amend.is_some(),
        });
    }

    applied
}

/// Whether `open` leaves nothing to fill of `quantity`, the order's total.
///
/// `<= 0` rather than `is_zero`: an order the venue reports filled beyond its total has finished
/// too. That is anomalous, so it is warned of: a venue resizing an order to exactly what filled
/// completes it, but no venue fills more than an order holds.
fn nothing_left<ExchangeKey, InstrumentKey>(
    open: &Open,
    quantity: Decimal,
    key: &OrderKey<ExchangeKey, InstrumentKey>,
) -> bool
where
    ExchangeKey: Debug,
    InstrumentKey: Debug,
{
    let remaining = open.quantity_remaining(quantity);
    if remaining < Decimal::ZERO {
        warn!(
            exchange = ?key.exchange,
            instrument = ?key.instrument,
            strategy = %key.strategy,
            cid = %key.cid,
            %quantity,
            filled = %open.filled_quantity,
            "OrderManager received a report of an order filled beyond its quantity - retiring it"
        );
    }
    remaining <= Decimal::ZERO
}

/// Whether `state` is a venue's refusal of an open request because its client order id is in use.
fn is_duplicate_cid_refusal<AssetKey, InstrumentKey>(
    state: &OrderState<AssetKey, InstrumentKey>,
) -> bool {
    matches!(
        state,
        OrderState::Inactive(InactiveOrderState::OpenFailed(OrderError::Rejected(
            ApiError::DuplicateClientOrderId(_)
        )))
    )
}

/// Apply `snapshot` (`update` being its active state, if it has one) to an order `Orders` does
/// not track: track it, unless it has ended, `retired` remembers it, or its quantity is not
/// positive.
///
/// `OpenInFlight` is only ever the engine's own record of an open request, never a venue's report,
/// so a remembered id is forgotten and tracked again, as [`Orders`]'s `record_in_flight_open`
/// does. Any other active state for a remembered id is a late report and is ignored. A snapshot
/// saying the order has ended is remembered, so a late report of it cannot track it either.
///
/// Returns the order adopted, when a venue's report of a working order is tracked, or refused,
/// when its quantity is zero or negative. The terms it states are not compared with anything:
/// the order is tracked on them as they stand.
fn update_untracked<ExchangeKey, AssetKey, InstrumentKey>(
    entry: VacantEntry<'_, ClientOrderId, TrackedOrder<ExchangeKey, InstrumentKey>>,
    retired: &mut RecentlyRetired,
    report: &OrderReport<ExchangeKey, AssetKey, InstrumentKey>,
    update: Option<Order<ExchangeKey, InstrumentKey, ActiveOrderState>>,
) -> OrderReportsApplied<ExchangeKey, InstrumentKey>
where
    ExchangeKey: Debug + Clone,
    AssetKey: Debug,
    InstrumentKey: Debug + Clone,
{
    let mut applied = OrderReportsApplied::default();
    let snapshot = &report.order;
    let cid = &snapshot.key.cid;
    match update {
        // Input Snapshot is InactiveOrderState (ie/ finished), so ignore
        None if retired.contains(cid) => {
            // Expected: a venue that reports an order's end in the open response and on the
            // account stream reports it twice.
            debug!(
                exchange = ?snapshot.key.exchange,
                instrument = ?snapshot.key.instrument,
                strategy = %snapshot.key.strategy,
                %cid,
                update = ?snapshot,
                "OrderManager received inactive order snapshot for an order already retired - ignoring"
            );
        }
        None => {
            warn!(
                exchange = ?snapshot.key.exchange,
                instrument = ?snapshot.key.instrument,
                strategy = %snapshot.key.strategy,
                %cid,
                update = ?snapshot,
                "OrderManager received inactive order snapshot for untracked order - ignoring"
            );
            retired.insert(cid.clone());
        }

        // Input Snapshot is a venue's report of an order already retired, so ignore: it is late
        // (see "Retired orders stay retired" on `Orders`)
        Some(update)
            if !matches!(update.state, ActiveOrderState::OpenInFlight(_))
                && retired.contains(cid) =>
        {
            debug!(
                exchange = ?snapshot.key.exchange,
                instrument = ?snapshot.key.instrument,
                strategy = %snapshot.key.strategy,
                %cid,
                update = ?snapshot,
                "OrderManager received active order snapshot for an order already retired - ignoring"
            );
        }

        // No working order has nothing to fill in total: a placeholder, or a value the producer
        // could not read. Tracked, it would read as finished while the order may be working, so
        // it is refused, observably, and not remembered: a later report that can be tracked is.
        Some(update)
            if update.quantity <= Decimal::ZERO
                && !matches!(update.state, ActiveOrderState::OpenInFlight(_)) =>
        {
            warn!(
                exchange = ?snapshot.key.exchange,
                instrument = ?snapshot.key.instrument,
                strategy = %snapshot.key.strategy,
                %cid,
                update = ?snapshot,
                "OrderManager received a working order snapshot for an untracked order with no \
                 positive quantity - not tracking it"
            );
            applied.not_adopted.push(OrderNotAdopted {
                order: update,
                reason: OrderNotAdoptedReason::QuantityNotPositive,
            });
        }

        // Input Snapshot is ActiveOrderState, so insert
        Some(update) => match &update.state {
            ActiveOrderState::Open(open)
                if open.quantity_remaining(update.quantity) <= Decimal::ZERO =>
            {
                debug!(
                    exchange = ?snapshot.key.exchange,
                    instrument = ?snapshot.key.instrument,
                    strategy = %snapshot.key.strategy,
                    %cid,
                    update = ?snapshot,
                    "OrderManager ignoring new Open order which is actually FulledFilled"
                );
                retired.insert(cid.clone());
            }
            _active_order => {
                debug!(
                    exchange = ?snapshot.key.exchange,
                    instrument = ?snapshot.key.instrument,
                    strategy = %snapshot.key.strategy,
                    %cid,
                    update = ?snapshot,
                    "OrderManager tracking new order"
                );
                let in_flight = matches!(update.state, ActiveOrderState::OpenInFlight(_));
                if in_flight {
                    retired.forget(cid);
                } else {
                    // A venue's report of an order this engine has no request for.
                    applied.adopted.push(OrderAdopted {
                        order: update.clone(),
                        reported: report.reported,
                    });
                }
                // A venue's report stating terms is the first word on them, at its own time.
                let stated_at = update.state.open_meta().map(|open| open.time_exchange);
                let mut tracked = TrackedOrder::new(update);
                if let Some(time) = stated_at {
                    tracked.terms_times.stated_at(report.reported, time);
                }
                entry.insert(tracked);
            }
        },
    }
    applied
}

impl<ExchangeKey, InstrumentKey> InFlightRequestRecorder<ExchangeKey, InstrumentKey>
    for Orders<ExchangeKey, InstrumentKey>
where
    ExchangeKey: Debug + Clone,
    InstrumentKey: Debug + Clone,
{
    fn record_in_flight_cancel(
        &mut self,
        request: &OrderRequestCancel<ExchangeKey, InstrumentKey>,
        time_sent: DateTime<Utc>,
    ) {
        let Some(TrackedOrder { order, .. }) = self.0.get_mut(&request.key.cid) else {
            error!(
                cid = %request.key.cid,
                event = ?request,
                "OrderManager cannot mark CancelInFlight for untracked Order - ignoring"
            );
            return;
        };

        // A cancel resent while one is in flight keeps the first one's send time, so a strategy
        // that re-cancels on every event cannot keep the order from ever passing its in-flight
        // deadline.
        if matches!(order.state, ActiveOrderState::CancelInFlight(_)) {
            debug!(
                cid = %request.key.cid,
                event = ?request,
                "OrderManager recorded a cancel resent while one is in flight - keeping the first"
            );
            return;
        }

        order.state = ActiveOrderState::CancelInFlight(CancelInFlight {
            order: order.state.open_meta().cloned(),
            time_sent,
        });
    }

    fn record_in_flight_open(
        &mut self,
        request: &OrderRequestOpen<ExchangeKey, InstrumentKey>,
        time_sent: DateTime<Utc>,
    ) {
        match self.0.entry(request.key.cid.clone()) {
            // The order already under the id is the one a venue keeps, refusing this request as a
            // duplicate, so it is the one kept here. The `Engine` never sends such a request: see
            // `TracksOrder`.
            Entry::Occupied(tracked) => {
                error!(
                    cid = %request.key.cid,
                    tracked = ?tracked.get(),
                    event = ?request,
                    "OrderManager received an OpenInFlight under the ClientOrderId of a tracked order - keeping the tracked order"
                );
            }
            Entry::Vacant(entry) => {
                // A new order by the engine's own hand, so its snapshots apply even under a
                // client id that named a retired order.
                self.1.forget(&request.key.cid);
                entry.insert(TrackedOrder::new(Order::open_in_flight(request, time_sent)));
            }
        }
    }

    fn record_in_flight_amend(
        &mut self,
        request: &OrderRequestAmend<ExchangeKey, InstrumentKey>,
        time_sent: DateTime<Utc>,
    ) {
        let Some(TrackedOrder { order, .. }) = self.0.get_mut(&request.key.cid) else {
            error!(
                cid = %request.key.cid,
                event = ?request,
                "OrderManager cannot mark AmendInFlight for an untracked order - ignoring"
            );
            return;
        };

        // The `Engine` sends an amend only for an `Open` order, so anything else is a caller
        // recording one it should have refused.
        let ActiveOrderState::Open(open) = &order.state else {
            error!(
                cid = %request.key.cid,
                state = ?order.state,
                event = ?request,
                "OrderManager cannot mark AmendInFlight for an order not Open - ignoring"
            );
            return;
        };

        order.state = ActiveOrderState::AmendInFlight(AmendInFlight {
            order: open.clone(),
            patch: request.state.patch,
            time_sent,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{engine::state::order::Orders, test_utils::time_plus_secs};
    use rust_decimal_macros::dec;
    use rustrade_execution::{
        error::{ConnectivityError, NotPlacedReason, OrderError},
        order::{
            Order, OrderKey, OrderKind, TimeInForce,
            id::{ClientOrderId, OrderId, StrategyId, VenueOrderId},
            request::{RequestCancel, RequestOpen},
            state::{
                ActiveOrderState, CancelInFlight, Cancelled, Expired, Filled, Open, OpenInFlight,
            },
        },
    };
    use rustrade_instrument::{Side, exchange::ExchangeId};
    use smol_str::SmolStr;

    /// When a test's in-flight request was sent. Distinct from every `Open::time_exchange` the
    /// tests use (around `MIN_UTC`), so a transition that took the wrong time would fail.
    const TIME_SENT: DateTime<Utc> = DateTime::<Utc>::UNIX_EPOCH;

    fn orders(
        orders: impl IntoIterator<Item = Order<ExchangeId, u64, ActiveOrderState>>,
    ) -> Orders<ExchangeId, u64> {
        Orders::new(
            orders
                .into_iter()
                .map(|order| (order.key.cid.clone(), order))
                .collect(),
        )
    }

    fn order<State>(cid: ClientOrderId, state: State) -> Order<ExchangeId, u64, State> {
        Order {
            key: OrderKey {
                exchange: ExchangeId::Simulated,
                instrument: 1,
                strategy: StrategyId::unknown(),
                cid,
            },
            side: Side::Buy,
            price: Some(dec!(1)),
            quantity: dec!(1),
            kind: OrderKind::Limit,
            time_in_force: TimeInForce::GoodUntilCancelled { post_only: false },
            state,
        }
    }

    #[test]
    fn remove_open_retires_only_an_open_order_that_is_the_venue_order_named() {
        let assigned = |id: &str| VenueOrderId::Assigned(OrderId::new(id));
        let open_as = |cid: &str, id: VenueOrderId| {
            order(
                ClientOrderId::new(cid),
                ActiveOrderState::Open(Open::new(id, DateTime::<Utc>::MIN_UTC, Decimal::ZERO)),
            )
        };
        let open = |cid: &str, id: &str| open_as(cid, assigned(id));
        let mut manager = orders([
            open("open", "oid-open"),
            open("reused", "oid-new"),
            open_as("unnamed", VenueOrderId::ClientAssigned),
            open("named-now", "oid-now"),
            order(
                ClientOrderId::new("opening"),
                ActiveOrderState::OpenInFlight(OpenInFlight::new(TIME_SENT)),
            ),
            order_cancel_in_flight(ClientOrderId::new("cancelling")),
        ]);

        assert!(
            manager
                .remove_open(&ClientOrderId::new("open"), &assigned("oid-open"))
                .is_some()
        );
        assert!(
            manager
                .remove_open(&ClientOrderId::new("reused"), &assigned("oid-old"))
                .is_none(),
            "the client id now names a different venue order"
        );
        for (cid, id) in [
            ("unnamed", VenueOrderId::ClientAssigned),
            ("unnamed", assigned("oid-old")),
            ("named-now", VenueOrderId::ClientAssigned),
        ] {
            assert!(
                manager.remove_open(&ClientOrderId::new(cid), &id).is_none(),
                "{cid} vs {id}: nothing proves the tracked order is the one named"
            );
        }
        for in_flight in ["opening", "cancelling"] {
            assert!(
                manager
                    .remove_open(&ClientOrderId::new(in_flight), &assigned("oid"))
                    .is_none(),
                "{in_flight}"
            );
        }
        assert!(
            manager
                .remove_open(&ClientOrderId::new("untracked"), &assigned("oid"))
                .is_none()
        );

        let mut remaining = manager.0.into_keys().map(|cid| cid.0).collect::<Vec<_>>();
        remaining.sort();
        assert_eq!(
            remaining,
            ["cancelling", "named-now", "opening", "reused", "unnamed"]
        );
    }

    fn order_cancel_in_flight(cid: ClientOrderId) -> Order<ExchangeId, u64, ActiveOrderState> {
        order(
            cid,
            ActiveOrderState::CancelInFlight(CancelInFlight {
                order: None,
                time_sent: TIME_SENT,
            }),
        )
    }

    fn order_snapshot_cancelled(
        cid: ClientOrderId,
    ) -> Snapshot<Order<ExchangeId, u64, OrderState<u64, u64>>> {
        Snapshot(Order {
            key: OrderKey {
                exchange: ExchangeId::Simulated,
                instrument: 1,
                strategy: StrategyId::unknown(),
                cid,
            },
            side: Side::Buy,
            price: Default::default(),
            quantity: Default::default(),
            kind: OrderKind::Market,
            time_in_force: TimeInForce::GoodUntilEndOfDay,
            state: OrderState::inactive(Cancelled {
                id: OrderId(SmolStr::default()),
                time_exchange: Default::default(),
                filled_quantity: Default::default(),
            }),
        })
    }

    fn order_snapshot_fully_filled(
        cid: ClientOrderId,
    ) -> Snapshot<Order<ExchangeId, u64, OrderState<u64, u64>>> {
        Snapshot(Order {
            key: OrderKey {
                exchange: ExchangeId::Simulated,
                instrument: 1,
                strategy: StrategyId::unknown(),
                cid,
            },
            side: Side::Buy,
            price: Default::default(),
            quantity: Default::default(),
            kind: OrderKind::Market,
            time_in_force: TimeInForce::GoodUntilEndOfDay,
            state: OrderState::fully_filled(Filled::new(
                OrderId(SmolStr::default()),
                DateTime::<Utc>::MIN_UTC,
                Default::default(),
                None,
            )),
        })
    }

    fn order_snapshot_failed(
        cid: ClientOrderId,
    ) -> Snapshot<Order<ExchangeId, u64, OrderState<u64, u64>>> {
        let Snapshot(mut order) = order_snapshot_outcome_unknown(cid);
        order.state = OrderState::inactive(OrderError::Rejected(ApiError::OrderRejected(
            "refused".into(),
        )));
        Snapshot(order)
    }

    /// What `ExecutionManager::process_open_timeout` sends when no answer came in time.
    fn order_snapshot_outcome_unknown(
        cid: ClientOrderId,
    ) -> Snapshot<Order<ExchangeId, u64, OrderState<u64, u64>>> {
        order_snapshot_open_failed(cid, OrderError::Connectivity(ConnectivityError::Timeout))
    }

    fn order_snapshot_open_failed(
        cid: ClientOrderId,
        error: OrderError<u64, u64>,
    ) -> Snapshot<Order<ExchangeId, u64, OrderState<u64, u64>>> {
        Snapshot(Order {
            key: OrderKey {
                exchange: ExchangeId::Simulated,
                instrument: 1,
                strategy: StrategyId::unknown(),
                cid,
            },
            side: Side::Buy,
            price: Default::default(),
            quantity: Default::default(),
            kind: OrderKind::Market,
            time_in_force: TimeInForce::GoodUntilEndOfDay,
            state: OrderState::inactive(error),
        })
    }

    fn order_snapshot_expired(
        cid: ClientOrderId,
    ) -> Snapshot<Order<ExchangeId, u64, OrderState<u64, u64>>> {
        Snapshot(Order {
            key: OrderKey {
                exchange: ExchangeId::Simulated,
                instrument: 1,
                strategy: StrategyId::unknown(),
                cid,
            },
            side: Side::Buy,
            price: Default::default(),
            quantity: Default::default(),
            kind: OrderKind::Market,
            time_in_force: TimeInForce::GoodUntilEndOfDay,
            state: OrderState::expired(Expired::new(
                OrderId(SmolStr::default()),
                DateTime::<Utc>::MIN_UTC,
                Default::default(),
            )),
        })
    }

    fn order_snapshot_open(
        cid: ClientOrderId,
        time_exchange: DateTime<Utc>,
    ) -> Snapshot<Order<ExchangeId, u64, OrderState<u64, u64>>> {
        Snapshot(Order {
            key: OrderKey {
                exchange: ExchangeId::Simulated,
                instrument: 1,
                strategy: StrategyId::unknown(),
                cid,
            },
            side: Side::Buy,
            price: Some(dec!(1)),
            quantity: dec!(1),
            kind: OrderKind::Limit,
            time_in_force: TimeInForce::GoodUntilCancelled { post_only: false },
            state: OrderState::active(open(time_exchange)),
        })
    }

    fn open(time_exchange: DateTime<Utc>) -> Open {
        Open {
            id: VenueOrderId::Assigned(OrderId(SmolStr::default())),
            time_exchange,
            filled_quantity: Default::default(),
        }
    }

    /// An `Open` whose cumulative fill is the point rather than incidental.
    fn open_filled(time_exchange: DateTime<Utc>, filled_quantity: Decimal) -> Open {
        Open {
            id: VenueOrderId::Assigned(OrderId(SmolStr::default())),
            time_exchange,
            filled_quantity,
        }
    }

    /// An `Open` the venue has named, for the cases where *which* order it names is the point.
    fn open_assigned(id: &str, time_exchange: DateTime<Utc>) -> Open {
        Open {
            id: VenueOrderId::Assigned(OrderId::new(id)),
            time_exchange,
            filled_quantity: Default::default(),
        }
    }

    /// An `Open` the venue accepted without naming -- addressable only by its client id.
    fn open_client_assigned(time_exchange: DateTime<Utc>) -> Open {
        Open {
            id: VenueOrderId::ClientAssigned,
            time_exchange,
            filled_quantity: Default::default(),
        }
    }

    fn request_cancel(cid: ClientOrderId) -> OrderRequestCancel<ExchangeId, u64> {
        OrderRequestCancel {
            key: OrderKey {
                exchange: ExchangeId::Simulated,
                instrument: 1,
                strategy: StrategyId::unknown(),
                cid,
            },
            state: RequestCancel::default(),
        }
    }

    fn request_opens(
        orders: impl IntoIterator<Item = OrderRequestOpen<ExchangeId, u64>>,
    ) -> FnvHashMap<ClientOrderId, Order<ExchangeId, u64, ActiveOrderState>> {
        orders
            .into_iter()
            .map(|order| {
                (
                    order.key.cid.clone(),
                    Order::open_in_flight(&order, TIME_SENT),
                )
            })
            .collect()
    }

    fn request_open(cid: ClientOrderId) -> OrderRequestOpen<ExchangeId, u64> {
        OrderRequestOpen {
            key: OrderKey {
                exchange: ExchangeId::Simulated,
                instrument: 1,
                strategy: StrategyId::unknown(),
                cid,
            },
            state: RequestOpen::new(
                Side::Buy,
                Some(dec!(1)),
                dec!(1),
                OrderKind::Limit,
                TimeInForce::GoodUntilEndOfDay,
            ),
        }
    }

    fn response_cancel_ok(cid: ClientOrderId) -> OrderResponseCancel<ExchangeId, u64, u64> {
        OrderResponseCancel {
            key: OrderKey {
                exchange: ExchangeId::Simulated,
                instrument: 1,
                strategy: StrategyId::unknown(),
                cid,
            },
            state: Ok(Cancelled {
                id: OrderId(SmolStr::default()),
                time_exchange: DateTime::<Utc>::MIN_UTC,
                filled_quantity: Default::default(),
            }),
        }
    }

    fn response_cancel_err(cid: ClientOrderId) -> OrderResponseCancel<ExchangeId, u64, u64> {
        OrderResponseCancel {
            key: OrderKey {
                exchange: ExchangeId::Simulated,
                instrument: 1,
                strategy: StrategyId::unknown(),
                cid,
            },
            state: Err(OrderError::Connectivity(ConnectivityError::Timeout)),
        }
    }

    #[test]
    fn test_update_from_state_only_snapshot() {
        struct TestCase {
            name: &'static str,
            state: Orders<ExchangeId, u64>,
            input: Snapshot<Order<ExchangeId, u64, OrderState<u64, u64>>>,
            expected: Orders<ExchangeId, u64>,
        }

        let time_base = DateTime::<Utc>::MIN_UTC;
        let cid = ClientOrderId::default();

        let cases = vec![
            TestCase {
                name: "untracked, Snapshot is inactive, so ignore",
                state: Orders::default(),
                input: order_snapshot_expired(cid.clone()),
                expected: Orders::default(),
            },
            TestCase {
                name: "untracked, Snapshot is active, so insert",
                state: Orders::default(),
                input: order_snapshot_open(cid.clone(), time_base),
                expected: orders([order(cid.clone(), ActiveOrderState::from(open(time_base)))]),
            },
            TestCase {
                name: "untracked, Snapshot is active Open but fully filled, so ignore",
                state: Orders::default(),
                input: Snapshot(order(
                    cid.clone(),
                    OrderState::active(open_filled(time_base, dec!(1))),
                )),
                expected: Orders::default(),
            },
            TestCase {
                name: "tracked OpenInFlight, Snapshot is inactive cancelled, so remove",
                state: orders([order(
                    cid.clone(),
                    ActiveOrderState::OpenInFlight(OpenInFlight::new(TIME_SENT)),
                )]),
                input: order_snapshot_cancelled(cid.clone()),
                expected: Orders::default(),
            },
            TestCase {
                name: "tracked OpenInFlight, Snapshot is inactive fully filled, so remove",
                state: orders([order(
                    cid.clone(),
                    ActiveOrderState::OpenInFlight(OpenInFlight::new(TIME_SENT)),
                )]),
                input: order_snapshot_fully_filled(cid.clone()),
                expected: Orders::default(),
            },
            TestCase {
                name: "tracked OpenInFlight, Snapshot is inactive failed, so remove",
                state: orders([order(
                    cid.clone(),
                    ActiveOrderState::OpenInFlight(OpenInFlight::new(TIME_SENT)),
                )]),
                input: order_snapshot_failed(cid.clone()),
                expected: Orders::default(),
            },
            TestCase {
                name: "tracked OpenInFlight, Snapshot is inactive expired, so remove",
                state: orders([order(
                    cid.clone(),
                    ActiveOrderState::OpenInFlight(OpenInFlight::new(TIME_SENT)),
                )]),
                input: order_snapshot_expired(cid.clone()),
                expected: Orders::default(),
            },
            TestCase {
                name: "tracked Open, Snapshot is inactive cancelled, so remove",
                state: orders([order(cid.clone(), ActiveOrderState::Open(open(time_base)))]),
                input: order_snapshot_cancelled(cid.clone()),
                expected: Orders::default(),
            },
            TestCase {
                name: "tracked Open, Snapshot is inactive fully filled, so remove",
                state: orders([order(cid.clone(), ActiveOrderState::Open(open(time_base)))]),
                input: order_snapshot_fully_filled(cid.clone()),
                expected: Orders::default(),
            },
            TestCase {
                name: "tracked Open, Snapshot is inactive failed, so remove",
                state: orders([order(cid.clone(), ActiveOrderState::Open(open(time_base)))]),
                input: order_snapshot_failed(cid.clone()),
                expected: Orders::default(),
            },
            TestCase {
                name: "tracked Open, Snapshot is inactive expired, so remove",
                state: orders([order(cid.clone(), ActiveOrderState::Open(open(time_base)))]),
                input: order_snapshot_expired(cid.clone()),
                expected: Orders::default(),
            },
            TestCase {
                name: "tracked CancelInFlight, Snapshot is inactive cancelled, so remove",
                state: orders([order(
                    cid.clone(),
                    ActiveOrderState::CancelInFlight(CancelInFlight {
                        order: None,
                        time_sent: TIME_SENT,
                    }),
                )]),
                input: order_snapshot_cancelled(cid.clone()),
                expected: Orders::default(),
            },
            TestCase {
                name: "tracked CancelInFlight, Snapshot is inactive fully filled, so remove",
                state: orders([order(
                    cid.clone(),
                    ActiveOrderState::CancelInFlight(CancelInFlight {
                        order: None,
                        time_sent: TIME_SENT,
                    }),
                )]),
                input: order_snapshot_fully_filled(cid.clone()),
                expected: Orders::default(),
            },
            TestCase {
                name: "tracked CancelInFlight, Snapshot is inactive failed, so remove",
                state: orders([order(
                    cid.clone(),
                    ActiveOrderState::CancelInFlight(CancelInFlight {
                        order: None,
                        time_sent: TIME_SENT,
                    }),
                )]),
                input: order_snapshot_failed(cid.clone()),
                expected: Orders::default(),
            },
            TestCase {
                name: "tracked CancelInFlight, Snapshot is inactive expired, so remove",
                state: orders([order(
                    cid.clone(),
                    ActiveOrderState::CancelInFlight(CancelInFlight {
                        order: None,
                        time_sent: TIME_SENT,
                    }),
                )]),
                input: order_snapshot_expired(cid.clone()),
                expected: Orders::default(),
            },
            TestCase {
                name: "tracked OpenInFlight, Snapshot is active OpenInFlight, so ignore duplicate",
                state: orders([order(
                    cid.clone(),
                    ActiveOrderState::OpenInFlight(OpenInFlight::new(TIME_SENT)),
                )]),
                input: Snapshot(order(
                    cid.clone(),
                    OrderState::active(OpenInFlight {
                        time_sent: TIME_SENT,
                    }),
                )),
                expected: orders([order(
                    cid.clone(),
                    ActiveOrderState::OpenInFlight(OpenInFlight::new(TIME_SENT)),
                )]),
            },
            TestCase {
                name: "tracked OpenInFlight, Snapshot is active Open but fully filled, so remove",
                state: orders([order(
                    cid.clone(),
                    ActiveOrderState::OpenInFlight(OpenInFlight::new(TIME_SENT)),
                )]),
                input: Snapshot(order(
                    cid.clone(),
                    OrderState::active(open_filled(time_base, dec!(1))),
                )),
                expected: Orders::default(),
            },
            TestCase {
                name: "tracked OpenInFlight, Snapshot is active Open, so update",
                state: orders([order(
                    cid.clone(),
                    ActiveOrderState::OpenInFlight(OpenInFlight::new(TIME_SENT)),
                )]),
                input: order_snapshot_open(cid.clone(), time_base),
                expected: orders([order(cid.clone(), ActiveOrderState::Open(open(time_base)))]),
            },
            TestCase {
                name: "tracked OpenInFlight, Snapshot is active CancelInFlight, so update",
                state: orders([order(
                    cid.clone(),
                    ActiveOrderState::OpenInFlight(OpenInFlight::new(TIME_SENT)),
                )]),
                input: Snapshot(order(
                    cid.clone(),
                    OrderState::active(CancelInFlight {
                        order: None,
                        time_sent: time_plus_secs(TIME_SENT, 60),
                    }),
                )),
                expected: orders([order(
                    cid.clone(),
                    ActiveOrderState::CancelInFlight(CancelInFlight {
                        order: None,
                        time_sent: time_plus_secs(TIME_SENT, 60),
                    }),
                )]),
            },
            TestCase {
                name: "tracked Open, Snapshot is active OpenInFlight, so ignore",
                state: orders([order(cid.clone(), ActiveOrderState::Open(open(time_base)))]),
                input: Snapshot(order(
                    cid.clone(),
                    OrderState::active(OpenInFlight {
                        time_sent: TIME_SENT,
                    }),
                )),
                expected: orders([order(cid.clone(), ActiveOrderState::Open(open(time_base)))]),
            },
            TestCase {
                name: "tracked Open, Snapshot is active Open with newer time, so update",
                state: orders([order(cid.clone(), ActiveOrderState::Open(open(time_base)))]),
                input: Snapshot(order(
                    cid.clone(),
                    OrderState::active(ActiveOrderState::Open(open(time_plus_secs(time_base, 1)))),
                )),
                expected: orders([order(
                    cid.clone(),
                    ActiveOrderState::Open(open(time_plus_secs(time_base, 1))),
                )]),
            },
            TestCase {
                // The shape this arm used to retain as active. `order` builds `quantity: dec!(1)`,
                // so `filled_quantity: dec!(1)` leaves nothing remaining.
                name: "tracked Open, Snapshot is active Open but fully filled, so remove",
                state: orders([order(cid.clone(), ActiveOrderState::Open(open(time_base)))]),
                input: Snapshot(order(
                    cid.clone(),
                    OrderState::active(open_filled(time_plus_secs(time_base, 1), dec!(1))),
                )),
                expected: Orders::default(),
            },
            TestCase {
                // The cumulative-fill key outranks the stamp, and here that retires the order.
                //
                // The snapshot is stamped earlier than the tracked state, but reports the order
                // fully filled while the tracked state has nothing filled at all. Cumulative fill
                // is append-only for one venue order, so a snapshot reporting it complete is
                // reporting something that cannot later become untrue -- the order is finished
                // whatever its stamp says, and retaining it would leave the strategy reading a
                // resting order the exchange no longer holds.
                //
                // Note this is reachable for *every* older full-fill claim against a live order:
                // "fully filled" means the cumulative reached `quantity`, and a live order's is
                // below it, so the fill key always admits such a snapshot.
                name: "tracked Open, Snapshot is active Open fully filled and older, so remove",
                state: orders([order(
                    cid.clone(),
                    ActiveOrderState::Open(open(time_plus_secs(time_base, 1))),
                )]),
                input: Snapshot(order(
                    cid.clone(),
                    OrderState::active(open_filled(time_base, dec!(1))),
                )),
                expected: Orders::default(),
            },
            TestCase {
                name: "tracked Open, Snapshot is active Open with older time, so ignore",
                state: orders([order(
                    cid.clone(),
                    ActiveOrderState::Open(open(time_plus_secs(time_base, 1))),
                )]),
                input: Snapshot(order(
                    cid.clone(),
                    OrderState::active(ActiveOrderState::Open(open(time_base))),
                )),
                expected: orders([order(
                    cid.clone(),
                    ActiveOrderState::Open(open(time_plus_secs(time_base, 1))),
                )]),
            },
            TestCase {
                name: "tracked Open, Snapshot is active CancelInFlight w/ newer Open, update accordingly",
                state: orders([order(
                    cid.clone(),
                    ActiveOrderState::Open(open(time_plus_secs(time_base, 1))),
                )]),
                input: Snapshot(order(
                    cid.clone(),
                    OrderState::active(CancelInFlight {
                        order: Some(open(time_plus_secs(time_base, 2))),
                        time_sent: TIME_SENT,
                    }),
                )),
                expected: orders([order(
                    cid.clone(),
                    ActiveOrderState::CancelInFlight(CancelInFlight {
                        order: Some(open(time_plus_secs(time_base, 2))),
                        time_sent: TIME_SENT,
                    }),
                )]),
            },
            TestCase {
                name: "tracked Open, Snapshot is active CancelInFlight w/ older Open, update accordingly",
                state: orders([order(
                    cid.clone(),
                    ActiveOrderState::Open(open(time_plus_secs(time_base, 2))),
                )]),
                input: Snapshot(order(
                    cid.clone(),
                    OrderState::active(CancelInFlight {
                        order: Some(open(time_plus_secs(time_base, 1))),
                        time_sent: TIME_SENT,
                    }),
                )),
                expected: orders([order(
                    cid.clone(),
                    ActiveOrderState::CancelInFlight(CancelInFlight {
                        order: Some(open(time_plus_secs(time_base, 2))),
                        time_sent: TIME_SENT,
                    }),
                )]),
            },
            TestCase {
                name: "tracked Open, Snapshot is active CancelInFlight w/ None Open, update accordingly",
                state: orders([order(
                    cid.clone(),
                    ActiveOrderState::Open(open(time_plus_secs(time_base, 1))),
                )]),
                input: Snapshot(order(
                    cid.clone(),
                    OrderState::active(CancelInFlight {
                        order: None,
                        time_sent: TIME_SENT,
                    }),
                )),
                expected: orders([order(
                    cid.clone(),
                    ActiveOrderState::CancelInFlight(CancelInFlight {
                        order: Some(open(time_plus_secs(time_base, 1))),
                        time_sent: TIME_SENT,
                    }),
                )]),
            },
            TestCase {
                name: "tracked CancelInFlight, Snapshot is active OpenInFlight, so ignore",
                state: orders([order(
                    cid.clone(),
                    ActiveOrderState::CancelInFlight(CancelInFlight {
                        order: None,
                        time_sent: TIME_SENT,
                    }),
                )]),
                input: Snapshot(order(
                    cid.clone(),
                    OrderState::active(OpenInFlight {
                        time_sent: TIME_SENT,
                    }),
                )),
                expected: orders([order(
                    cid.clone(),
                    ActiveOrderState::CancelInFlight(CancelInFlight {
                        order: None,
                        time_sent: TIME_SENT,
                    }),
                )]),
            },
            TestCase {
                name: "tracked CancelInFlight w/ None Open, Snapshot is active Open, update accordingly",
                state: orders([order(
                    cid.clone(),
                    ActiveOrderState::CancelInFlight(CancelInFlight {
                        order: None,
                        time_sent: TIME_SENT,
                    }),
                )]),
                input: order_snapshot_open(cid.clone(), time_base),
                expected: orders([order(
                    cid.clone(),
                    ActiveOrderState::CancelInFlight(CancelInFlight {
                        order: Some(open(time_plus_secs(time_base, 0))),
                        time_sent: TIME_SENT,
                    }),
                )]),
            },
            TestCase {
                name: "tracked CancelInFlight, Snapshot is active Open w/ older time, so ignore",
                state: orders([order(
                    cid.clone(),
                    ActiveOrderState::CancelInFlight(CancelInFlight {
                        order: Some(open(time_plus_secs(time_base, 2))),
                        time_sent: TIME_SENT,
                    }),
                )]),
                input: order_snapshot_open(cid.clone(), time_plus_secs(time_base, 1)),
                expected: orders([order(
                    cid.clone(),
                    ActiveOrderState::CancelInFlight(CancelInFlight {
                        order: Some(open(time_plus_secs(time_base, 2))),
                        time_sent: TIME_SENT,
                    }),
                )]),
            },
            TestCase {
                name: "tracked CancelInFlight, Snapshot is active Open w/ newer time, so update accordingly",
                state: orders([order(
                    cid.clone(),
                    ActiveOrderState::CancelInFlight(CancelInFlight {
                        order: Some(open(time_plus_secs(time_base, 1))),
                        time_sent: TIME_SENT,
                    }),
                )]),
                input: order_snapshot_open(cid.clone(), time_plus_secs(time_base, 2)),
                expected: orders([order(
                    cid.clone(),
                    ActiveOrderState::CancelInFlight(CancelInFlight {
                        order: Some(open(time_plus_secs(time_base, 2))),
                        time_sent: TIME_SENT,
                    }),
                )]),
            },
            TestCase {
                name: "tracked CancelInFlight, Snapshot is active CancelInFlight, so ignore duplicate",
                state: orders([order(
                    cid.clone(),
                    ActiveOrderState::CancelInFlight(CancelInFlight {
                        order: None,
                        time_sent: TIME_SENT,
                    }),
                )]),
                input: Snapshot(order(
                    cid.clone(),
                    OrderState::active(CancelInFlight {
                        order: None,
                        time_sent: time_plus_secs(TIME_SENT, 60),
                    }),
                )),
                expected: orders([order(
                    cid.clone(),
                    ActiveOrderState::CancelInFlight(CancelInFlight {
                        order: None,
                        time_sent: TIME_SENT,
                    }),
                )]),
            },
        ];

        for mut test in cases.into_iter() {
            test.state
                .update_from_state_only_snapshot(test.input.as_ref());
            assert_eq!(test.state, test.expected, "TC failed: {}", test.name)
        }
    }

    /// What a producer's creation-stamped `Open::time_exchange` costs a consumer, and how far the
    /// cumulative-fill ordering key repairs it.
    ///
    /// `Open`'s producer contract requires the venue's last-update field, because creation time is
    /// identical across every snapshot of one order. Ordering on the stamp alone therefore cannot
    /// place such a snapshot after the states that followed it, and the cumulative fill it carries
    /// is lost with it -- exactly where a reconciliation fetch is meant to help, a partially filled
    /// order whose discarded snapshot held the only accurate cumulative.
    ///
    /// `Open::is_superseded_by`'s second key recovers this case: the reconciliation snapshot
    /// reports strictly more filled, which is evidence of a later state whatever the stamps say,
    /// so it lands despite being stamped earlier.
    ///
    /// Note that the fill has to reach the order *as a snapshot* for this to arise.
    /// `Orders::update_from_fill` writes `filled_quantity` alone and never `time_exchange`, so it
    /// cannot move an order past its own creation stamp.
    #[test]
    fn a_creation_time_stamped_snapshot_lands_once_it_reports_more_filled() {
        let time_created = DateTime::<Utc>::MIN_UTC;
        let time_filled = time_plus_secs(time_created, 1);
        let cid = ClientOrderId::default();

        // `order` builds `quantity: dec!(1)`. Both cumulatives stay under it, so the order remains
        // live throughout and the full-fill collapse stays out of what is being measured.
        let filled_on_stream = dec!(0.3);
        let filled_at_venue = dec!(0.6);

        // The order rests at the exchange, acknowledged at the moment it was created.
        let mut state = orders([order(
            cid.clone(),
            ActiveOrderState::Open(open(time_created)),
        )]);

        // A partial fill arrives on the stream paired with a snapshot, stamped when the exchange
        // matched it. The order now sits later than its own creation time.
        let fill: Snapshot<Order<ExchangeId, u64, OrderState<u64, u64>>> = Snapshot(order(
            cid.clone(),
            OrderState::active(open_filled(time_filled, filled_on_stream)),
        ));
        state.update_from_state_only_snapshot(fill.as_ref());

        // A reconciliation fetch returns the same order with the venue's true cumulative, stamped
        // with creation time -- older than what the stream already applied.
        let recovered: Snapshot<Order<ExchangeId, u64, OrderState<u64, u64>>> = Snapshot(order(
            cid.clone(),
            OrderState::active(open_filled(time_created, filled_at_venue)),
        ));
        state.update_from_state_only_snapshot(recovered.as_ref());

        // The venue's cumulative wins. The adopted state carries the reconciliation snapshot's own
        // stamp, which is the earlier of the two -- the documented consequence of taking a venue's
        // reported state as a unit rather than splicing the newer stamp onto it.
        assert_eq!(
            state,
            orders([order(
                cid,
                ActiveOrderState::Open(open_filled(time_created, filled_at_venue)),
            )]),
            "a snapshot reporting more filled was discarded for being stamped earlier"
        );
    }

    /// The limit of that repair, and why the producer obligation on `Open::time_exchange` still
    /// stands.
    ///
    /// The cumulative-fill key is evidence only where the cumulative actually moves. Two snapshots
    /// reporting the same fill are ordered on the stamp alone, so a creation-stamped one is still
    /// discarded -- and an order resting unfilled reports the same cumulative on every snapshot,
    /// which is the whole of its life before the first execution.
    #[test]
    fn a_creation_time_stamped_snapshot_is_still_discarded_when_it_reports_no_more_filled() {
        let time_created = DateTime::<Utc>::MIN_UTC;
        let time_filled = time_plus_secs(time_created, 1);
        let cid = ClientOrderId::default();
        let filled = dec!(0.3);

        let mut state = orders([order(
            cid.clone(),
            ActiveOrderState::Open(open(time_created)),
        )]);

        let fill: Snapshot<Order<ExchangeId, u64, OrderState<u64, u64>>> = Snapshot(order(
            cid.clone(),
            OrderState::active(open_filled(time_filled, filled)),
        ));
        state.update_from_state_only_snapshot(fill.as_ref());

        // Same cumulative, creation stamp. Neither key admits it, so it is discarded -- exactly as
        // before the fill key existed.
        let stale: Snapshot<Order<ExchangeId, u64, OrderState<u64, u64>>> = Snapshot(order(
            cid.clone(),
            OrderState::active(open_filled(time_created, filled)),
        ));
        state.update_from_state_only_snapshot(stale.as_ref());

        assert_eq!(
            state,
            orders([order(
                cid,
                ActiveOrderState::Open(open_filled(time_filled, filled)),
            )]),
            "a creation-stamped snapshot reporting no more filled was applied over a later one"
        );
    }

    /// The shape a venue takes on when it reports no timestamp of its own.
    ///
    /// IBKR's `orderStatus` callback carries no timestamp field, so the client stamps
    /// `Utc::now()` as it processes each one. Those stamps record *arrival*, not the venue's
    /// sequence, and they rise monotonically -- which means ordering on the stamp alone admits
    /// every snapshot and leaves the venue with last-writer-wins rather than ordering.
    ///
    /// So a snapshot that overtook a newer one in flight arrives carrying the later stamp and the
    /// earlier state, and the stamp cannot tell it apart from genuine progress. Cumulative fill
    /// can: it is append-only for one venue order, so a snapshot reporting less of it than the
    /// tracked state is out of sequence no matter how it is stamped.
    ///
    /// Without that, the second snapshot here would rewind the order's cumulative from the
    /// venue's 0.6 to a stale 0.3.
    #[test]
    fn a_receipt_stamped_snapshot_cannot_rewind_the_cumulative_it_arrives_after() {
        let time_base = DateTime::<Utc>::MIN_UTC;
        let cid = ClientOrderId::default();

        let mut state = orders([order(cid.clone(), ActiveOrderState::Open(open(time_base)))]);

        // Processed first, so stamped first. Carries the venue's true cumulative.
        let arrived_first: Snapshot<Order<ExchangeId, u64, OrderState<u64, u64>>> =
            Snapshot(order(
                cid.clone(),
                OrderState::active(open_filled(time_plus_secs(time_base, 1), dec!(0.6))),
            ));
        state.update_from_state_only_snapshot(arrived_first.as_ref());

        // Processed second, so stamped later -- but it describes an earlier state of the order.
        // Only the cumulative gives it away.
        let overtaken: Snapshot<Order<ExchangeId, u64, OrderState<u64, u64>>> = Snapshot(order(
            cid.clone(),
            OrderState::active(open_filled(time_plus_secs(time_base, 2), dec!(0.3))),
        ));
        state.update_from_state_only_snapshot(overtaken.as_ref());

        assert_eq!(
            state,
            orders([order(
                cid,
                ActiveOrderState::Open(open_filled(time_plus_secs(time_base, 1), dec!(0.6))),
            )]),
            "an out-of-sequence snapshot rewound the order's cumulative fill"
        );
    }

    /// A snapshot resolves to a tracked order by `ClientOrderId` alone, so two exchange orders
    /// sharing one client id land in the same slot. Where both name a venue order and the two
    /// disagree, applying the update would write one order's price, quantity and cumulative fill
    /// over another's -- or retire an order that is still resting at the venue.
    #[test]
    fn a_snapshot_naming_a_different_exchange_order_is_refused() {
        let time_base = DateTime::<Utc>::MIN_UTC;
        let cid = ClientOrderId::default();

        let mut state = orders([order(
            cid.clone(),
            ActiveOrderState::Open(open_assigned("A", time_base)),
        )]);

        // Newer, so the staleness gate would admit it. It is refused on identity alone.
        let foreign: Snapshot<Order<ExchangeId, u64, OrderState<u64, u64>>> = Snapshot(order(
            cid.clone(),
            OrderState::active(open_assigned("B", time_plus_secs(time_base, 1))),
        ));
        state.update_from_state_only_snapshot(foreign.as_ref());

        assert_eq!(
            state,
            orders([order(
                cid,
                ActiveOrderState::Open(open_assigned("A", time_base)),
            )]),
            "a snapshot for exchange order B was applied to tracked order A"
        );
    }

    /// The other half of that guard, and the reason it tests for contradiction rather than
    /// inequality.
    ///
    /// A venue may accept an order without assigning an identifier -- Hyperliquid does this for one
    /// that is resting but has not yet triggered -- and name it only later. Refusing that later
    /// snapshot would leave the order on its placeholder for the rest of its life, never learning
    /// the venue's identifier and never learning its fills: a worse failure than the one the guard
    /// exists to prevent.
    #[test]
    fn an_order_the_venue_has_not_named_adopts_the_id_a_later_snapshot_brings() {
        let time_base = DateTime::<Utc>::MIN_UTC;
        let time_named = time_plus_secs(time_base, 1);
        let cid = ClientOrderId::default();

        let mut state = orders([order(
            cid.clone(),
            ActiveOrderState::Open(open_client_assigned(time_base)),
        )]);

        let named: Snapshot<Order<ExchangeId, u64, OrderState<u64, u64>>> = Snapshot(order(
            cid.clone(),
            OrderState::active(open_assigned("A", time_named)),
        ));
        state.update_from_state_only_snapshot(named.as_ref());

        assert_eq!(
            state,
            orders([order(
                cid,
                ActiveOrderState::Open(open_assigned("A", time_named)),
            )]),
            "an order the venue had not yet named failed to adopt the id a later snapshot brought"
        );
    }

    /// A `CancelInFlight` reached either way round refuses a foreign `Open` too, but not
    /// identically: arriving *as* a cancel the transition still happens, because a cancel is in
    /// flight whatever the carried `Open` says, and the tracked `Open` is kept instead.
    #[test]
    fn a_cancel_in_flight_does_not_adopt_an_open_naming_a_different_exchange_order() {
        let time_base = DateTime::<Utc>::MIN_UTC;
        let cid = ClientOrderId::default();

        let mut state = orders([order(
            cid.clone(),
            ActiveOrderState::Open(open_assigned("A", time_base)),
        )]);

        let cancelling: Snapshot<Order<ExchangeId, u64, OrderState<u64, u64>>> = Snapshot(order(
            cid.clone(),
            OrderState::active(CancelInFlight {
                order: Some(open_assigned("B", time_plus_secs(time_base, 1))),
                time_sent: TIME_SENT,
            }),
        ));
        state.update_from_state_only_snapshot(cancelling.as_ref());

        assert_eq!(
            state,
            orders([order(
                cid.clone(),
                ActiveOrderState::CancelInFlight(CancelInFlight {
                    order: Some(open_assigned("A", time_base)),
                    time_sent: TIME_SENT,
                }),
            )]),
            "a CancelInFlight transition adopted an Open naming a different exchange order"
        );

        // Arriving as an `Open` for an existing `CancelInFlight`, adoption is the arm's only
        // effect, so the snapshot is refused outright.
        let foreign: Snapshot<Order<ExchangeId, u64, OrderState<u64, u64>>> = Snapshot(order(
            cid.clone(),
            OrderState::active(open_assigned("B", time_plus_secs(time_base, 2))),
        ));
        state.update_from_state_only_snapshot(foreign.as_ref());

        assert_eq!(
            state,
            orders([order(
                cid,
                ActiveOrderState::CancelInFlight(CancelInFlight {
                    order: Some(open_assigned("A", time_base)),
                    time_sent: TIME_SENT,
                }),
            )]),
            "a tracked CancelInFlight adopted an Open naming a different exchange order"
        );
    }

    /// `update_from_fill` matches on the venue's identifier as well as the client id, so a fill
    /// reaches only the order the venue named in it.
    #[test]
    fn a_fill_only_advances_the_order_the_venue_named_in_it() {
        let time_base = DateTime::<Utc>::MIN_UTC;
        let cid = ClientOrderId::default();

        // `order` builds `quantity: dec!(1)`.
        let mut state = orders([order(
            cid.clone(),
            ActiveOrderState::Open(open_assigned("A", time_base)),
        )]);

        // A fill for a different venue order leaves this one untouched.
        assert!(!state.update_from_fill(&cid, &OrderId::new("B"), dec!(0.5)));
        assert_eq!(
            state,
            orders([order(
                cid.clone(),
                ActiveOrderState::Open(open_assigned("A", time_base)),
            )])
        );

        // The matching one advances the cumulative, and reports `false`: the order is still live,
        // so the caller has no routing to prune.
        assert!(!state.update_from_fill(&cid, &OrderId::new("A"), dec!(0.5)));
        assert_eq!(
            state,
            orders([order(
                cid.clone(),
                ActiveOrderState::Open(Open {
                    id: VenueOrderId::Assigned(OrderId::new("A")),
                    time_exchange: time_base,
                    filled_quantity: dec!(0.5),
                }),
            )])
        );

        // Nothing left to fill: the order is untracked and the caller is told so.
        assert!(state.update_from_fill(&cid, &OrderId::new("A"), dec!(1)));
        assert_eq!(state, Orders::default());
    }

    /// A fill is reported against a venue identifier, so an order that has none cannot be matched
    /// to one. Reading two absent identifiers as agreement would attach any fill under this client
    /// id to this order.
    #[test]
    fn a_fill_cannot_advance_an_order_the_venue_has_not_named() {
        let time_base = DateTime::<Utc>::MIN_UTC;
        let cid = ClientOrderId::default();

        let mut state = orders([order(
            cid.clone(),
            ActiveOrderState::Open(open_client_assigned(time_base)),
        )]);

        assert!(!state.update_from_fill(&cid, &OrderId::new("A"), dec!(0.5)));
        assert_eq!(
            state,
            orders([order(
                cid,
                ActiveOrderState::Open(open_client_assigned(time_base)),
            )]),
            "a fill advanced an order the venue has not named"
        );
    }

    #[test]
    fn test_update_from_cancel_response() {
        struct TestCase {
            name: &'static str,
            state: Orders<ExchangeId, u64>,
            input: OrderResponseCancel<ExchangeId, u64, u64>,
            expected: Orders<ExchangeId, u64>,
        }

        let cid = ClientOrderId::default();
        let time_base = DateTime::<Utc>::MIN_UTC;

        let cases = vec![
            TestCase {
                name: "untracked, so ignore",
                state: Orders::default(),
                input: response_cancel_ok(cid.clone()),
                expected: Orders::default(),
            },
            TestCase {
                name: "tracked OpenInFlight, response Ok, so remove",
                state: orders([order(
                    cid.clone(),
                    ActiveOrderState::from(OpenInFlight::new(TIME_SENT)),
                )]),
                input: response_cancel_ok(cid.clone()),
                expected: Orders::default(),
            },
            TestCase {
                name: "tracked Open, response Ok, so remove",
                state: orders([order(cid.clone(), ActiveOrderState::from(open(time_base)))]),
                input: response_cancel_ok(cid.clone()),
                expected: Orders::default(),
            },
            TestCase {
                name: "tracked CancelInFlight, response Ok, so remove",
                state: orders([order(
                    cid.clone(),
                    ActiveOrderState::from(CancelInFlight {
                        order: None,
                        time_sent: TIME_SENT,
                    }),
                )]),
                input: response_cancel_ok(cid.clone()),
                expected: Orders::default(),
            },
            TestCase {
                name: "tracked OpenInFlight, response Err, so ignore",
                state: orders([order(
                    cid.clone(),
                    ActiveOrderState::from(OpenInFlight::new(TIME_SENT)),
                )]),
                input: response_cancel_err(cid.clone()),
                expected: orders([order(
                    cid.clone(),
                    ActiveOrderState::from(OpenInFlight::new(TIME_SENT)),
                )]),
            },
            TestCase {
                name: "tracked Open, response Err, so ignore",
                state: orders([order(cid.clone(), ActiveOrderState::from(open(time_base)))]),
                input: response_cancel_err(cid.clone()),
                expected: orders([order(cid.clone(), ActiveOrderState::from(open(time_base)))]),
            },
            TestCase {
                name: "tracked CancelInFlight w/ Some(Open), response Err, so set Open",
                state: orders([order(
                    cid.clone(),
                    ActiveOrderState::from(CancelInFlight {
                        order: Some(open(time_base)),
                        time_sent: TIME_SENT,
                    }),
                )]),
                input: response_cancel_err(cid.clone()),
                expected: orders([order(cid.clone(), ActiveOrderState::from(open(time_base)))]),
            },
            TestCase {
                name: "tracked CancelInFlight w/ None Open, response Err, so remove",
                state: orders([order(
                    cid.clone(),
                    ActiveOrderState::from(CancelInFlight {
                        order: None,
                        time_sent: TIME_SENT,
                    }),
                )]),
                input: response_cancel_err(cid),
                expected: Orders::default(),
            },
        ];

        for mut test in cases.into_iter() {
            test.state.update_from_cancel_response(&test.input);
            assert_eq!(test.state, test.expected, "TC failed: {}", test.name);
        }
    }

    #[test]
    fn test_record_in_flight_cancel() {
        struct TestCase {
            state: Orders<ExchangeId, u64>,
            input: Vec<OrderRequestCancel<ExchangeId, u64>>,
            expected: Orders<ExchangeId, u64>,
        }

        let cid_1 = ClientOrderId::default();
        let cid_2 = ClientOrderId::default();

        let cases = vec![
            TestCase {
                // TC0: Ignore untracked InFlight
                state: Orders::default(),
                input: vec![request_cancel(cid_1.clone())],
                expected: Orders::default(),
            },
            TestCase {
                // TC1: Insert InFlight that is already tracked
                state: orders([order_cancel_in_flight(cid_1.clone())]),
                input: vec![request_cancel(cid_1.clone())],
                expected: orders([order_cancel_in_flight(cid_1.clone())]),
            },
            TestCase {
                // TC2: Ignore one untracked InFlight, and ignore one already tracked
                state: orders([order_cancel_in_flight(cid_1.clone())]),
                input: vec![request_cancel(cid_1.clone()), request_cancel(cid_2.clone())],
                expected: orders([order_cancel_in_flight(cid_1)]),
            },
        ];

        for (index, mut test) in cases.into_iter().enumerate() {
            for in_flight in test.input {
                test.state.record_in_flight_cancel(&in_flight, TIME_SENT);
            }
            assert_eq!(test.state, test.expected, "TC{index} failed")
        }
    }

    /// A strategy that re-cancels on every event must not keep restarting the order's in-flight
    /// clock, or it would never pass its deadline.
    #[test]
    fn a_resent_cancel_keeps_the_first_cancels_send_time_and_order() {
        let cid = ClientOrderId::new("resent");
        let held_open = Open::new(
            VenueOrderId::Assigned(OrderId::new("venue")),
            DateTime::<Utc>::MIN_UTC,
            Decimal::ZERO,
        );
        let mut state = orders([order(
            cid.clone(),
            ActiveOrderState::Open(held_open.clone()),
        )]);

        state.record_in_flight_cancel(&request_cancel(cid.clone()), TIME_SENT);
        state.record_in_flight_cancel(&request_cancel(cid.clone()), time_plus_secs(TIME_SENT, 60));

        assert_eq!(
            state,
            orders([order(
                cid,
                ActiveOrderState::CancelInFlight(CancelInFlight {
                    order: Some(held_open),
                    time_sent: TIME_SENT,
                }),
            )])
        );
    }

    #[test]
    fn test_record_in_flight_open() {
        struct TestCase {
            state: Orders<ExchangeId, u64>,
            input: Vec<OrderRequestOpen<ExchangeId, u64>>,
            expected: Orders<ExchangeId, u64>,
        }

        let cid_1 = ClientOrderId::default();
        let cid_2 = ClientOrderId::default();

        let cases = vec![
            TestCase {
                // TC0: Insert unseen InFlight
                state: Orders::default(),
                input: vec![request_open(cid_1.clone())],
                expected: Orders::new(request_opens([request_open(cid_1.clone())])),
            },
            TestCase {
                // TC1: Insert InFlight that is already tracked
                state: Orders::new(request_opens([request_open(cid_1.clone())])),
                input: vec![request_open(cid_1.clone())],
                expected: Orders::new(request_opens([request_open(cid_1.clone())])),
            },
            TestCase {
                // TC2: Insert one untracked InFlight, and one already tracked
                state: Orders::new(request_opens([request_open(cid_1.clone())])),
                input: vec![request_open(cid_1.clone()), request_open(cid_2.clone())],
                expected: Orders::new(request_opens([request_open(cid_1), request_open(cid_2)])),
            },
        ];

        for (index, mut test) in cases.into_iter().enumerate() {
            for in_flight in test.input {
                test.state.record_in_flight_open(&in_flight, TIME_SENT);
            }
            assert_eq!(test.state, test.expected, "TC{index} failed")
        }
    }

    /// The race the memory exists for: the account stream reports how an order ended before the
    /// response to placing it arrives, carrying the order's state at placement.
    #[test]
    fn a_late_open_response_does_not_track_an_order_the_stream_retired() {
        let time = DateTime::<Utc>::MIN_UTC;
        let cid = ClientOrderId::new("raced");
        let mut state = Orders::<ExchangeId, u64>::default();
        state.record_in_flight_open(&request_open(cid.clone()), TIME_SENT);

        state.update_from_state_only_snapshot(order_snapshot_open(cid.clone(), time).as_ref());
        state.update_from_state_only_snapshot(order_snapshot_fully_filled(cid.clone()).as_ref());
        assert!(!state.0.contains_key(&cid), "precondition: retired");

        state.update_from_state_only_snapshot(order_snapshot_open(cid.clone(), time).as_ref());
        assert!(
            !state.0.contains_key(&cid),
            "the late response leaves it retired"
        );
        state.update_from_state_only_snapshot(order_snapshot_fully_filled(cid.clone()).as_ref());
        assert!(!state.0.contains_key(&cid));
    }

    /// An open the `ExecutionManager` timed out may still have reached the venue: the timeout
    /// says only that no answer came in time. The venue's later report of the order must track it.
    #[test]
    fn an_open_that_timed_out_is_tracked_when_the_venue_reports_it_open() {
        let time = DateTime::<Utc>::MIN_UTC;
        let cid = ClientOrderId::new("timed-out");
        let mut state = Orders::<ExchangeId, u64>::default();
        state.record_in_flight_open(&request_open(cid.clone()), TIME_SENT);

        state.update_from_state_only_snapshot(order_snapshot_outcome_unknown(cid.clone()).as_ref());
        assert!(
            matches!(
                state.get(&cid).map(|order| &order.state),
                Some(ActiveOrderState::OpenInFlight(_))
            ),
            "an unknown outcome leaves the order in flight"
        );

        state.update_from_state_only_snapshot(order_snapshot_open(cid.clone(), time).as_ref());
        assert!(
            matches!(
                state.get(&cid).map(|order| &order.state),
                Some(ActiveOrderState::Open(_))
            ),
            "the venue holds the order live, so the engine must track it"
        );
    }

    /// An unknown outcome changes nothing, whatever state the order is in, and does not retire
    /// an id the engine does not track.
    #[test]
    fn an_unknown_outcome_changes_no_order() {
        let time = DateTime::<Utc>::MIN_UTC;
        let cancel_in_flight = ActiveOrderState::CancelInFlight(CancelInFlight {
            order: Some(open(time)),
            time_sent: TIME_SENT,
        });
        for tracked in [ActiveOrderState::Open(open(time)), cancel_in_flight] {
            let cid = ClientOrderId::new("tracked");
            let mut state = orders([order(cid.clone(), tracked.clone())]);
            state.update_from_state_only_snapshot(
                order_snapshot_outcome_unknown(cid.clone()).as_ref(),
            );
            assert_eq!(state.get(&cid).map(|order| &order.state), Some(&tracked));
        }

        let cid = ClientOrderId::new("untracked");
        let mut state = Orders::<ExchangeId, u64>::default();
        state.update_from_state_only_snapshot(order_snapshot_outcome_unknown(cid.clone()).as_ref());
        state.update_from_state_only_snapshot(order_snapshot_open(cid.clone(), time).as_ref());
        assert!(
            state.0.contains_key(&cid),
            "the unknown outcome did not retire the id, so the venue's report tracks the order"
        );
    }

    /// A report that an order was never placed ends it only while the venue has not reported it.
    /// Once the venue has reported it working, as when its report beats a lookup that concluded
    /// otherwise, the order stays tracked, so its later fills and its cancel still reach it.
    #[test]
    fn a_never_placed_report_ends_only_an_order_the_venue_has_not_reported() {
        let time = DateTime::<Utc>::MIN_UTC;
        let not_placed = [
            OrderError::NotPlaced(NotPlacedReason::UnknownToVenue),
            OrderError::NotPlaced(NotPlacedReason::NotSent(ConnectivityError::Timeout)),
        ];
        let reported = [
            ActiveOrderState::Open(open(time)),
            ActiveOrderState::CancelInFlight(CancelInFlight {
                order: Some(open(time)),
                time_sent: TIME_SENT,
            }),
        ];
        let unreported = [
            ActiveOrderState::OpenInFlight(OpenInFlight::new(TIME_SENT)),
            ActiveOrderState::CancelInFlight(CancelInFlight {
                order: None,
                time_sent: TIME_SENT,
            }),
        ];
        for error in not_placed {
            for tracked in reported.clone() {
                let cid = ClientOrderId::new("reported");
                let mut state = orders([order(cid.clone(), tracked.clone())]);
                state.update_from_state_only_snapshot(
                    order_snapshot_open_failed(cid.clone(), error.clone()).as_ref(),
                );
                assert_eq!(
                    state.get(&cid).map(|order| &order.state),
                    Some(&tracked),
                    "{error:?} must not end {tracked:?}"
                );
            }
            for tracked in unreported.clone() {
                let cid = ClientOrderId::new("unreported");
                let mut state = orders([order(cid.clone(), tracked.clone())]);
                state.update_from_state_only_snapshot(
                    order_snapshot_open_failed(cid.clone(), error.clone()).as_ref(),
                );
                assert!(
                    !state.0.contains_key(&cid),
                    "{error:?} must end {tracked:?}"
                );
                assert!(state.1.contains(&cid), "and retire its id");
            }
        }
    }

    /// Every way `Orders` retires an order is remembered, so a stale listing that still shows
    /// the order cannot track it again.
    #[test]
    fn a_stale_snapshot_does_not_track_an_order_retired_any_way() {
        let time = DateTime::<Utc>::MIN_UTC;
        let open_order = |cid: &ClientOrderId| {
            order(
                cid.clone(),
                ActiveOrderState::Open(open_assigned("A", time)),
            )
        };
        let cids = [
            "snapshot",
            "cancelled",
            "cancel-in-flight",
            "filled",
            "absent",
            "acked-filled",
            "reported-filled",
        ]
        .map(ClientOrderId::new);
        let [
            snapshot,
            cancelled,
            cancel_in_flight,
            filled,
            absent,
            acked_filled,
            reported_filled,
        ] = &cids;
        let mut state = orders(cids.iter().map(open_order));
        state.0.insert(
            acked_filled.clone(),
            TrackedOrder::new(order(
                acked_filled.clone(),
                ActiveOrderState::OpenInFlight(OpenInFlight::new(TIME_SENT)),
            )),
        );

        // An `Open` with nothing left to fill, acknowledging an open request and updating an open
        // order.
        let complete = |cid: &ClientOrderId| {
            let mut snapshot = order_snapshot_open(cid.clone(), time_plus_secs(time, 1));
            snapshot.0.state = OrderState::active(Open {
                id: VenueOrderId::Assigned(OrderId::new("A")),
                ..open_filled(time_plus_secs(time, 1), dec!(1))
            });
            snapshot
        };
        state.update_from_state_only_snapshot(complete(acked_filled).as_ref());
        state.update_from_state_only_snapshot(complete(reported_filled).as_ref());

        state.update_from_state_only_snapshot(order_snapshot_expired(snapshot.clone()).as_ref());
        state.update_from_cancel_response(&response_cancel_ok(cancelled.clone()));
        state.record_in_flight_cancel(&request_cancel(cancel_in_flight.clone()), TIME_SENT);
        state.update_from_cancel_response(&response_cancel_ok(cancel_in_flight.clone()));
        assert!(state.update_from_fill(filled, &OrderId::new("A"), dec!(1)));
        assert!(
            state
                .remove_open(absent, &VenueOrderId::Assigned(OrderId::new("A")))
                .is_some()
        );
        assert!(state.0.is_empty(), "precondition: every order retired");

        for cid in &cids {
            state.update_from_state_only_snapshot(order_snapshot_open(cid.clone(), time).as_ref());
            assert!(!state.0.contains_key(cid), "{cid}: stays retired");
        }
    }

    /// An order removed only because a cancel failed before its open was answered is not
    /// retired: the open response it still waits for must track it.
    #[test]
    fn an_order_whose_cancel_failed_before_its_open_was_answered_is_not_retired() {
        let cid = ClientOrderId::new("unanswered");
        let mut state = Orders::<ExchangeId, u64>::default();
        state.record_in_flight_open(&request_open(cid.clone()), TIME_SENT);
        state.record_in_flight_cancel(&request_cancel(cid.clone()), TIME_SENT);
        state.update_from_cancel_response(&response_cancel_err(cid.clone()));
        assert!(!state.0.contains_key(&cid), "precondition: untracked");

        state.update_from_state_only_snapshot(
            order_snapshot_open(cid.clone(), DateTime::<Utc>::MIN_UTC).as_ref(),
        );
        assert!(state.0.contains_key(&cid));
    }

    /// An open recorded under the id of an order already tracked keeps that order, as the venue
    /// refusing the request does, rather than replacing it with an order the venue never held.
    #[test]
    fn an_open_recorded_under_a_tracked_id_keeps_the_tracked_order() {
        let cid = ClientOrderId::new("tracked");
        let mut state = Orders::<ExchangeId, u64>::default();
        state.record_in_flight_open(&request_open(cid.clone()), TIME_SENT);
        state.update_from_state_only_snapshot(
            order_snapshot_open(cid.clone(), DateTime::<Utc>::MIN_UTC).as_ref(),
        );

        state.record_in_flight_open(&request_open(cid.clone()), TIME_SENT);

        assert!(
            matches!(
                state.get(&cid).map(|order| &order.state),
                Some(ActiveOrderState::Open(_))
            ),
            "the tracked order is still the open one"
        );
    }

    /// A refusal of an open as a duplicate client order id ends only the request it refused: it
    /// retires an order still in flight under the id, which is that request's own, and leaves an
    /// order the venue already holds working.
    #[test]
    fn a_duplicate_cid_refusal_ends_only_the_request_it_refused() {
        let refusal = |cid: ClientOrderId| {
            let Snapshot(mut order) = order_snapshot_failed(cid);
            order.state = OrderState::inactive(OrderError::Rejected(
                ApiError::DuplicateClientOrderId("in use".into()),
            ));
            Snapshot(order)
        };
        let mut state = Orders::<ExchangeId, u64>::default();
        let (working, in_flight) = (
            ClientOrderId::new("working"),
            ClientOrderId::new("in_flight"),
        );
        state.record_in_flight_open(&request_open(working.clone()), TIME_SENT);
        state.update_from_state_only_snapshot(
            order_snapshot_open(working.clone(), DateTime::<Utc>::MIN_UTC).as_ref(),
        );
        state.record_in_flight_open(&request_open(in_flight.clone()), TIME_SENT);

        state.update_from_state_only_snapshot(refusal(working.clone()).as_ref());
        state.update_from_state_only_snapshot(refusal(in_flight.clone()).as_ref());

        assert!(
            matches!(
                state.get(&working).map(|order| &order.state),
                Some(ActiveOrderState::Open(_))
            ),
            "the order the venue holds keeps working"
        );
        assert!(
            !state.0.contains_key(&in_flight),
            "the refused request's own order is retired"
        );
    }

    #[test]
    fn a_retired_order_is_forgotten_after_the_window_or_a_new_open_under_its_id() {
        let time = DateTime::<Utc>::MIN_UTC;
        let retire = |state: &mut Orders<ExchangeId, u64>, cid: &ClientOrderId| {
            state.record_in_flight_open(&request_open(cid.clone()), TIME_SENT);
            state.update_from_state_only_snapshot(order_snapshot_cancelled(cid.clone()).as_ref());
        };
        let mut state = Orders::<ExchangeId, u64>::default();

        let reused = ClientOrderId::new("reused");
        retire(&mut state, &reused);
        state.record_in_flight_open(&request_open(reused.clone()), TIME_SENT);
        state.update_from_state_only_snapshot(order_snapshot_open(reused.clone(), time).as_ref());
        assert!(
            matches!(
                state.get(&reused).map(|order| &order.state),
                Some(ActiveOrderState::Open(_))
            ),
            "a new open request under the id is a new order"
        );

        let oldest = ClientOrderId::new("oldest");
        retire(&mut state, &oldest);
        for index in 0..MAX_RECENTLY_RETIRED_ORDERS {
            retire(&mut state, &ClientOrderId::new(format!("later-{index}")));
        }
        assert_eq!(state.1.0.len(), MAX_RECENTLY_RETIRED_ORDERS);

        state.update_from_state_only_snapshot(order_snapshot_open(oldest.clone(), time).as_ref());
        assert!(state.0.contains_key(&oldest), "forgotten beyond the window");
        let newest = ClientOrderId::new(format!("later-{}", MAX_RECENTLY_RETIRED_ORDERS - 1));
        state.update_from_state_only_snapshot(order_snapshot_open(newest.clone(), time).as_ref());
        assert!(!state.0.contains_key(&newest), "still remembered");
    }

    #[test]
    fn the_retired_memory_is_neither_compared_nor_serialised() {
        let retired = ClientOrderId::new("retired");
        let live = ClientOrderId::new("live");
        let mut state = orders([order(
            live.clone(),
            ActiveOrderState::Open(open(DateTime::<Utc>::MIN_UTC)),
        )]);
        state.record_in_flight_open(&request_open(retired.clone()), TIME_SENT);
        state.update_from_state_only_snapshot(order_snapshot_cancelled(retired.clone()).as_ref());

        assert!(state.1.contains(&retired));
        assert_eq!(state, orders(state.values().cloned()));

        let (Ok(json), Ok(map_json)) =
            (serde_json::to_value(&state), serde_json::to_value(&state.0))
        else {
            panic!("serialising failed")
        };
        assert_eq!(json, map_json, "serialised as the tracked orders alone");
        let Ok(restored) = serde_json::from_value::<Orders<ExchangeId, u64>>(map_json) else {
            panic!("a bare map of orders did not deserialise")
        };
        assert_eq!(restored, state);
        assert!(!restored.1.contains(&retired));
    }

    /// Contract expiry voids every order at once; a late report of one must not track it again.
    #[test]
    fn clearing_remembers_every_order_it_drops() {
        let time = DateTime::<Utc>::MIN_UTC;
        let cids = ["open", "cancelling"].map(ClientOrderId::new);
        let [open_cid, cancelling] = &cids;
        let mut state = orders([
            order(open_cid.clone(), ActiveOrderState::Open(open(time))),
            order_cancel_in_flight(cancelling.clone()),
        ]);

        state.clear();
        assert!(state.0.is_empty());
        for cid in &cids {
            state.update_from_state_only_snapshot(order_snapshot_open(cid.clone(), time).as_ref());
            assert!(!state.0.contains_key(cid), "{cid}: stays retired");
        }
    }

    /// An order the engine no longer tracks can still be reported ended, as when a cancel failed
    /// before its open was answered and the stream then reports the end; and a venue can report a
    /// new order already complete as an `Open` with nothing left. Either is remembered, so the
    /// open response that follows cannot track the order.
    #[test]
    fn an_untracked_order_reported_ended_is_remembered() {
        let time = DateTime::<Utc>::MIN_UTC;
        let mut state = Orders::<ExchangeId, u64>::default();

        let cancel_failed = ClientOrderId::new("cancel-failed");
        state.record_in_flight_open(&request_open(cancel_failed.clone()), TIME_SENT);
        state.record_in_flight_cancel(&request_cancel(cancel_failed.clone()), TIME_SENT);
        state.update_from_cancel_response(&response_cancel_err(cancel_failed.clone()));
        state.update_from_state_only_snapshot(
            order_snapshot_cancelled(cancel_failed.clone()).as_ref(),
        );

        let complete = ClientOrderId::new("complete");
        let mut reported_complete = order_snapshot_open(complete.clone(), time);
        reported_complete.0.state = OrderState::active(open_filled(time, dec!(1)));
        state.update_from_state_only_snapshot(reported_complete.as_ref());

        for cid in [&cancel_failed, &complete] {
            state.update_from_state_only_snapshot(order_snapshot_open(cid.clone(), time).as_ref());
            assert!(!state.0.contains_key(cid), "{cid}: stays retired");
        }
    }

    #[allow(clippy::unwrap_used, clippy::expect_used)] // Test code: panics on bad input are acceptable
    mod amend {
        use super::*;
        use rustrade_execution::{
            error::{AmendRefusal, ConnectivityError, OrderError},
            order::{
                amend::{AmendPatch, AmendTarget, Amended, RequestAmend},
                request::{OrderRequestAmend, OrderResponseAmend, ReduceOnly},
                state::AmendInFlight,
            },
        };

        fn t(secs: i64) -> DateTime<Utc> {
            time_plus_secs(DateTime::<Utc>::MIN_UTC, secs)
        }

        fn cid() -> ClientOrderId {
            ClientOrderId::new("amended")
        }

        fn tracked(state: ActiveOrderState) -> Orders<ExchangeId, u64> {
            orders([order(cid(), state)])
        }

        fn resting() -> Orders<ExchangeId, u64> {
            tracked(ActiveOrderState::Open(open_assigned("1", t(0))))
        }

        fn request(patch: AmendPatch) -> OrderRequestAmend<ExchangeId, u64> {
            let order = order(cid(), ());
            OrderRequestAmend {
                key: order.key,
                state: RequestAmend::new(
                    VenueOrderId::Assigned(OrderId::new("1")),
                    patch,
                    AmendTarget {
                        side: order.side,
                        price: order.price,
                        quantity: order.quantity,
                        kind: order.kind,
                        time_in_force: order.time_in_force,
                        reduce_only: Some(ReduceOnly::No),
                    },
                    Decimal::ZERO,
                ),
            }
        }

        fn in_flight() -> Orders<ExchangeId, u64> {
            let mut orders = resting();
            orders.record_in_flight_amend(
                &request(AmendPatch::default().with_price(dec!(2))),
                TIME_SENT,
            );
            orders
        }

        fn answer(
            state: Result<Amended, OrderError<u64, u64>>,
        ) -> OrderResponseAmend<ExchangeId, u64, u64> {
            OrderResponseAmend {
                key: order(cid(), ()).key,
                state,
            }
        }

        fn amended(open: Open, price: Decimal, quantity: Decimal) -> Amended {
            Amended::new(open, Some(price), quantity, OrderKind::Limit, None)
        }

        fn tracked_order(
            orders: &Orders<ExchangeId, u64>,
        ) -> &Order<ExchangeId, u64, ActiveOrderState> {
            orders.get(&cid()).expect("tracked")
        }

        #[test]
        fn an_amend_is_recorded_only_for_an_open_order() {
            let orders = in_flight();
            assert_eq!(
                tracked_order(&orders).state,
                ActiveOrderState::AmendInFlight(AmendInFlight {
                    order: open_assigned("1", t(0)),
                    patch: AmendPatch::default().with_price(dec!(2)),
                    time_sent: TIME_SENT,
                })
            );
            assert!(orders.has_request_in_flight());

            for state in [
                ActiveOrderState::OpenInFlight(OpenInFlight::new(TIME_SENT)),
                ActiveOrderState::CancelInFlight(CancelInFlight::new(None, TIME_SENT)),
            ] {
                let mut orders = tracked(state.clone());
                orders.record_in_flight_amend(
                    &request(AmendPatch::default().with_price(dec!(2))),
                    TIME_SENT,
                );
                assert_eq!(tracked_order(&orders).state, state);
            }
        }

        #[test]
        fn an_amend_the_venue_made_commits_its_values() {
            let mut orders = in_flight();
            orders.update_from_amend_response(&answer(Ok(amended(
                open_assigned("1", t(1)),
                dec!(2),
                dec!(3),
            ))));
            let order = tracked_order(&orders);
            assert_eq!(order.price, Some(dec!(2)));
            assert_eq!(order.quantity, dec!(3));
            assert_eq!(
                order.state,
                ActiveOrderState::Open(open_assigned("1", t(1)))
            );
            assert!(!orders.has_request_in_flight());
        }

        #[test]
        fn an_amend_keeps_a_later_fill_and_takes_the_new_venue_id() {
            let mut orders = in_flight();
            // A fill reported after the venue answered, but applied first.
            assert!(!orders.update_from_fill(&cid(), &OrderId::new("1"), dec!(0.4)));
            orders.update_from_amend_response(&answer(Ok(Amended::new(
                open_assigned("2", t(0)),
                Some(dec!(2)),
                dec!(1),
                OrderKind::Limit,
                Some(VenueOrderId::Assigned(OrderId::new("1"))),
            ))));
            let ActiveOrderState::Open(open) = &tracked_order(&orders).state else {
                panic!("Open");
            };
            assert_eq!(open.id, VenueOrderId::Assigned(OrderId::new("2")));
            assert_eq!(open.filled_quantity, dec!(0.4));
        }

        #[test]
        fn a_refused_amend_leaves_the_order_as_it_was() {
            for error in [
                OrderError::AmendRefused(AmendRefusal::NoChange),
                OrderError::Rejected(ApiError::OrderRejected("no".into())),
            ] {
                let mut orders = in_flight();
                orders.update_from_amend_response(&answer(Err(error)));
                let order = tracked_order(&orders);
                assert_eq!(order.price, Some(dec!(1)));
                assert_eq!(
                    order.state,
                    ActiveOrderState::Open(open_assigned("1", t(0)))
                );
            }
        }

        #[test]
        fn an_unknown_outcome_leaves_the_amend_in_flight() {
            let mut orders = in_flight();
            let before = tracked_order(&orders).clone();
            orders.update_from_amend_response(&answer(Err(OrderError::Connectivity(
                ConnectivityError::Timeout,
            ))));
            assert_eq!(*tracked_order(&orders), before);
        }

        #[test]
        fn the_order_keeps_working_while_its_amend_is_in_flight() {
            let mut orders = in_flight();
            orders.update_from_state_only_snapshot(Snapshot(&order(
                cid(),
                OrderState::<u64, u64>::active(open_assigned("1", t(2))),
            )));
            let ActiveOrderState::AmendInFlight(amend) = &tracked_order(&orders).state else {
                panic!("still AmendInFlight");
            };
            assert_eq!(amend.order.time_exchange, t(2));

            assert!(orders.update_from_fill(&cid(), &OrderId::new("1"), dec!(1)));
            assert!(orders.0.is_empty(), "a fill of the whole order retires it");
        }

        #[test]
        fn a_cancel_supersedes_the_amend_and_a_late_amend_still_commits_its_values() {
            let mut orders = in_flight();
            orders.record_in_flight_cancel(&request_cancel(cid()), t(3));
            assert_eq!(
                tracked_order(&orders).state,
                ActiveOrderState::CancelInFlight(CancelInFlight::new(
                    Some(open_assigned("1", t(0))),
                    t(3),
                ))
            );

            orders.update_from_amend_response(&answer(Ok(amended(
                open_assigned("1", t(1)),
                dec!(2),
                dec!(1),
            ))));
            let order = tracked_order(&orders);
            assert_eq!(order.price, Some(dec!(2)));
            assert!(matches!(
                order.state,
                ActiveOrderState::CancelInFlight(CancelInFlight { order: Some(_), .. })
            ));
        }

        /// A cancel's snapshot carrying the order replaces the amend's order only with a later
        /// report of the same venue order: one of another venue order, or an older one, leaves
        /// the order as last reported.
        #[test]
        fn a_cancel_snapshot_takes_only_a_later_report_of_the_amended_order() {
            let cancelled_with = |reported: Open| {
                let mut orders = tracked(ActiveOrderState::Open(open_assigned("1", t(2))));
                orders.record_in_flight_amend(
                    &request(AmendPatch::default().with_price(dec!(2))),
                    TIME_SENT,
                );
                orders.update_from_state_only_snapshot(Snapshot(&order(
                    cid(),
                    OrderState::<u64, u64>::Active(ActiveOrderState::CancelInFlight(
                        CancelInFlight::new(Some(reported), t(5)),
                    )),
                )));
                tracked_order(&orders).state.clone()
            };
            let cancel = |order: Open| {
                ActiveOrderState::CancelInFlight(CancelInFlight::new(Some(order), t(5)))
            };

            assert_eq!(
                cancelled_with(open_assigned("1", t(3))),
                cancel(open_assigned("1", t(3)))
            );
            assert_eq!(
                cancelled_with(open_assigned("2", t(3))),
                cancel(open_assigned("1", t(2))),
                "another venue order"
            );
            assert_eq!(
                cancelled_with(open_assigned("1", t(1))),
                cancel(open_assigned("1", t(2))),
                "an older report"
            );
        }

        #[test]
        fn an_amend_leaving_nothing_to_fill_retires_the_order() {
            let mut part_filled = open_assigned("1", t(1));
            part_filled.filled_quantity = dec!(0.5);
            let mut orders = tracked(ActiveOrderState::Open(part_filled));
            orders.record_in_flight_amend(
                &request(AmendPatch::default().with_quantity(dec!(0.6))),
                TIME_SENT,
            );
            let mut filled = open_assigned("1", t(2));
            filled.filled_quantity = dec!(0.5);
            orders.update_from_amend_response(&answer(Ok(amended(filled, dec!(1), dec!(0.5)))));
            assert!(orders.0.is_empty());
        }

        /// A fill beyond the old total while an amend raising the quantity is in flight means the
        /// venue made the raise: the order retires only past the new total. A lowering amend in
        /// flight does not lower the total a fill is measured against.
        #[test]
        fn a_fill_beyond_the_old_total_waits_for_a_raise_in_flight() {
            let amending = |patch: AmendPatch| {
                let mut orders = resting();
                orders.record_in_flight_amend(&request(patch), TIME_SENT);
                orders
            };

            let mut raising = amending(AmendPatch::default().with_quantity(dec!(3)));
            assert!(!raising.update_from_fill(&cid(), &OrderId::new("1"), dec!(2)));
            let ActiveOrderState::AmendInFlight(amend) = &tracked_order(&raising).state else {
                panic!("still AmendInFlight");
            };
            assert_eq!(amend.order.filled_quantity, dec!(2));
            assert!(raising.update_from_fill(&cid(), &OrderId::new("1"), dec!(3)));
            assert!(raising.0.is_empty());

            let mut lowering = amending(AmendPatch::default().with_quantity(dec!(0.5)));
            assert!(!lowering.update_from_fill(&cid(), &OrderId::new("1"), dec!(0.6)));
            assert!(lowering.update_from_fill(&cid(), &OrderId::new("1"), dec!(1)));
        }

        /// An answer is applied only to the amend it answers: not to an order with no amend in
        /// flight, nor to an amend of another venue order.
        #[test]
        fn an_amend_answer_is_ignored_without_its_amend() {
            let made = || answer(Ok(amended(open_assigned("1", t(1)), dec!(2), dec!(3))));

            let mut orders = resting();
            let before = tracked_order(&orders).clone();
            orders.update_from_amend_response(&made());
            assert_eq!(*tracked_order(&orders), before, "no amend in flight");

            for previous_id in [None, Some(VenueOrderId::Assigned(OrderId::new("9")))] {
                let mut orders = in_flight();
                let before = tracked_order(&orders).clone();
                orders.update_from_amend_response(&answer(Ok(Amended::new(
                    open_assigned("9", t(1)),
                    Some(dec!(2)),
                    dec!(3),
                    OrderKind::Limit,
                    previous_id,
                ))));
                assert_eq!(*tracked_order(&orders), before, "another venue order");
            }

            let mut cancelling = in_flight();
            cancelling.record_in_flight_cancel(&request_cancel(cid()), t(3));
            let before = tracked_order(&cancelling).clone();
            cancelling.update_from_amend_response(&answer(Ok(amended(
                open_assigned("9", t(1)),
                dec!(2),
                dec!(3),
            ))));
            assert_eq!(
                *tracked_order(&cancelling),
                before,
                "another venue order, behind a cancel"
            );
        }

        /// An order the venue had not named when the amend was sent may be named in the answer.
        #[test]
        fn an_amend_answer_may_name_an_order_the_venue_had_not() {
            let mut orders = tracked(ActiveOrderState::Open(open_client_assigned(t(0))));
            orders.record_in_flight_amend(
                &request(AmendPatch::default().with_price(dec!(2))),
                TIME_SENT,
            );
            orders.update_from_amend_response(&answer(Ok(amended(
                open_assigned("7", t(1)),
                dec!(2),
                dec!(1),
            ))));
            let order = tracked_order(&orders);
            assert_eq!(order.price, Some(dec!(2)));
            assert_eq!(
                order.state,
                ActiveOrderState::Open(open_assigned("7", t(1)))
            );
        }

        /// A fill that reached the old total while a raise was in flight leaves nothing to fill
        /// once the raise is refused.
        #[test]
        fn a_refused_raise_retires_an_order_filled_meanwhile() {
            let mut orders = resting();
            orders.record_in_flight_amend(
                &request(AmendPatch::default().with_quantity(dec!(3))),
                TIME_SENT,
            );
            assert!(!orders.update_from_fill(&cid(), &OrderId::new("1"), dec!(1)));
            orders.update_from_amend_response(&answer(Err(OrderError::AmendRefused(
                AmendRefusal::Venue("filled".into()),
            ))));
            assert!(orders.0.is_empty());
        }
    }

    #[allow(clippy::unwrap_used, clippy::expect_used)] // Test code: panics on bad input are acceptable
    mod terms {
        use super::*;
        use rustrade_execution::order::{
            amend::{AmendPatch, AmendTarget, RequestAmend},
            report::ReportedTerms,
            request::{OrderRequestAmend, ReduceOnly},
            state::AmendInFlight,
        };

        fn t(secs: i64) -> DateTime<Utc> {
            time_plus_secs(DateTime::<Utc>::MIN_UTC, secs)
        }

        fn cid() -> ClientOrderId {
            ClientOrderId::new("changed")
        }

        fn open_at(secs: i64, filled: Decimal) -> Open {
            Open {
                filled_quantity: filled,
                ..open_assigned("1", t(secs))
            }
        }

        /// The tracked order, `quantity` in total, in `state`.
        fn tracked(state: ActiveOrderState, quantity: Decimal) -> Orders<ExchangeId, u64> {
            orders([Order {
                quantity,
                ..order(cid(), state)
            }])
        }

        /// A report of the order working as `open`, with `price` and `quantity`, stating `stated`.
        fn report(
            open: Open,
            price: Decimal,
            quantity: Decimal,
            stated: ReportedTerms,
        ) -> OrderReport<ExchangeId, u64, u64> {
            OrderReport::new(
                Order {
                    price: Some(price),
                    quantity,
                    ..order(cid(), OrderState::active(open))
                },
                stated,
            )
        }

        fn apply(
            orders: &mut Orders<ExchangeId, u64>,
            report: &OrderReport<ExchangeId, u64, u64>,
        ) -> SnapshotApplied<ExchangeId, u64> {
            orders.update_from_order_snapshot(Snapshot(report))
        }

        fn limit(price: Decimal, quantity: Decimal) -> OrderTerms {
            OrderTerms {
                price: Some(price),
                quantity,
                kind: OrderKind::Limit,
                time_in_force: TimeInForce::GoodUntilCancelled { post_only: false },
            }
        }

        fn changed(from: OrderTerms, to: OrderTerms) -> Option<OrderTermsChanged<ExchangeId, u64>> {
            Some(OrderTermsChanged {
                key: order(cid(), ()).key,
                from,
                to,
            })
        }

        fn tracked_order(
            orders: &Orders<ExchangeId, u64>,
        ) -> &Order<ExchangeId, u64, ActiveOrderState> {
            orders.get(&cid()).expect("tracked")
        }

        fn times(orders: &Orders<ExchangeId, u64>) -> TermsTimes {
            orders.tracked(&cid()).expect("tracked").terms_times()
        }

        fn all_at(secs: i64) -> TermsTimes {
            TermsTimes {
                quantity: Some(t(secs)),
                price: Some(t(secs)),
                kind: Some(t(secs)),
                time_in_force: Some(t(secs)),
            }
        }

        fn terms_of(
            changed: Option<OrderTermsChanged<ExchangeId, u64>>,
            diverged: Option<OrderTermsDiverged<ExchangeId, u64>>,
        ) -> OrderReportsApplied<ExchangeId, u64> {
            OrderReportsApplied {
                changed: changed.into_iter().collect(),
                diverged: diverged.into_iter().collect(),
                ..OrderReportsApplied::default()
            }
        }

        /// A later report stating the order's terms changes them, says so, and the order's fills
        /// are then counted against the new total.
        #[test]
        fn a_later_report_stating_terms_changes_them_and_fills_count_against_them() {
            let mut orders = tracked(ActiveOrderState::Open(open_at(0, dec!(0))), dec!(1));

            let applied = apply(
                &mut orders,
                &report(open_at(1, dec!(0)), dec!(2), dec!(3), ReportedTerms::ALL),
            );
            assert_eq!(
                applied,
                SnapshotApplied::working(terms_of(
                    changed(limit(dec!(1), dec!(1)), limit(dec!(2), dec!(3))),
                    None
                ))
            );
            let order = tracked_order(&orders);
            assert_eq!((order.price, order.quantity), (Some(dec!(2)), dec!(3)));
            assert_eq!(order.state, ActiveOrderState::Open(open_at(1, dec!(0))));

            assert!(!orders.update_from_fill(&cid(), &OrderId::new("1"), dec!(2)));
            assert!(orders.update_from_fill(&cid(), &OrderId::new("1"), dec!(3)));
        }

        /// A report stating no term is not read for one: its quantity may be an echo of the
        /// placement, so the order is retired against the quantity tracked.
        #[test]
        fn a_report_stating_no_terms_changes_none_and_retires_against_the_tracked_quantity() {
            let mut orders = tracked(ActiveOrderState::Open(open_at(0, dec!(0))), dec!(2));

            let applied = apply(
                &mut orders,
                &report(open_at(1, dec!(1)), dec!(5), dec!(1), ReportedTerms::NONE),
            );
            assert_eq!(applied, SnapshotApplied::default());
            let order = tracked_order(&orders);
            assert_eq!((order.price, order.quantity), (Some(dec!(1)), dec!(2)));
            assert_eq!(order.state, ActiveOrderState::Open(open_at(1, dec!(1))));
        }

        /// A report stamped earlier than the terms held is admitted for its higher fill, but its
        /// terms may predate those, so none is taken from it. One reporting less filled than the
        /// state held predates that state whatever its stamp, so its terms are not taken either.
        #[test]
        fn an_earlier_or_out_of_sequence_report_changes_no_terms() {
            let mut orders = tracked(ActiveOrderState::Open(open_at(0, dec!(0))), dec!(1));
            // The venue states the terms held, at 5.
            let applied = apply(
                &mut orders,
                &report(open_at(5, dec!(0)), dec!(1), dec!(1), ReportedTerms::ALL),
            );
            assert_eq!(applied, SnapshotApplied::default());
            assert_eq!(times(&orders), all_at(5));

            let applied = apply(
                &mut orders,
                &report(open_at(1, dec!(0.5)), dec!(2), dec!(3), ReportedTerms::ALL),
            );
            assert_eq!(applied, SnapshotApplied::default());
            let order = tracked_order(&orders);
            assert_eq!((order.price, order.quantity), (Some(dec!(1)), dec!(1)));
            assert_eq!(order.state, ActiveOrderState::Open(open_at(1, dec!(0.5))));
            assert_eq!(times(&orders), all_at(5));

            let applied = apply(
                &mut orders,
                &report(open_at(6, dec!(0.2)), dec!(2), dec!(3), ReportedTerms::ALL),
            );
            assert_eq!(applied, SnapshotApplied::default());
            assert_eq!(tracked_order(&orders).quantity, dec!(1));
            assert_eq!(times(&orders), all_at(5));
        }

        /// A report stating no terms does not hide a change. A venue's report of new terms,
        /// stamped earlier than a state-only report already applied (one from another path,
        /// stamped later), still changes them, while the state held stays. An older report of the
        /// old terms then does not undo the change.
        #[test]
        fn a_state_only_report_does_not_hide_an_earlier_stamped_change() {
            let mut orders = tracked(ActiveOrderState::Open(open_at(0, dec!(0))), dec!(1));
            apply(
                &mut orders,
                &report(open_at(5, dec!(0)), dec!(1), dec!(1), ReportedTerms::NONE),
            );

            let applied = apply(
                &mut orders,
                &report(open_at(3, dec!(0)), dec!(2), dec!(3), ReportedTerms::ALL),
            );
            assert_eq!(
                applied,
                SnapshotApplied::working(terms_of(
                    changed(limit(dec!(1), dec!(1)), limit(dec!(2), dec!(3))),
                    None
                ))
            );
            let order = tracked_order(&orders);
            assert_eq!((order.price, order.quantity), (Some(dec!(2)), dec!(3)));
            assert_eq!(order.state, ActiveOrderState::Open(open_at(5, dec!(0))));
            assert_eq!(times(&orders), all_at(3));

            let applied = apply(
                &mut orders,
                &report(open_at(2, dec!(0)), dec!(1), dec!(1), ReportedTerms::ALL),
            );
            assert_eq!(applied, SnapshotApplied::default());
            let order = tracked_order(&orders);
            assert_eq!((order.price, order.quantity), (Some(dec!(2)), dec!(3)));
        }

        /// A change from a report whose state does not supersede the one held still retires the
        /// order when it cuts the quantity to what the state held has filled.
        #[test]
        fn an_out_of_sequence_cut_to_what_filled_retires_the_order() {
            let mut orders = tracked(ActiveOrderState::Open(open_at(5, dec!(0.5))), dec!(1));
            let applied = apply(
                &mut orders,
                &report(
                    open_at(3, dec!(0.5)),
                    dec!(1),
                    dec!(0.5),
                    ReportedTerms::ALL,
                ),
            );
            assert_eq!(
                applied,
                SnapshotApplied::retired(terms_of(
                    changed(limit(dec!(1), dec!(1)), limit(dec!(1), dec!(0.5))),
                    None
                ))
            );
            assert!(orders.0.is_empty());

            // One left something to fill keeps the order and the state held.
            let mut orders = tracked(ActiveOrderState::Open(open_at(5, dec!(0.5))), dec!(1));
            let applied = apply(
                &mut orders,
                &report(open_at(3, dec!(0.5)), dec!(1), dec!(2), ReportedTerms::ALL),
            );
            assert!(!applied.retired);
            assert_eq!(
                tracked_order(&orders).state,
                ActiveOrderState::Open(open_at(5, dec!(0.5)))
            );
            assert_eq!(tracked_order(&orders).quantity, dec!(2));
        }

        /// A report stating no terms leaves the terms times as they are, and one stating a term
        /// moves that term's time to its own, even when it changes nothing.
        #[test]
        fn only_a_report_stating_a_term_records_its_time() {
            let mut orders = tracked(ActiveOrderState::Open(open_at(0, dec!(0))), dec!(1));
            assert_eq!(times(&orders), TermsTimes::default());
            apply(
                &mut orders,
                &report(open_at(1, dec!(0)), dec!(1), dec!(1), ReportedTerms::NONE),
            );
            assert_eq!(times(&orders), TermsTimes::default());
            apply(
                &mut orders,
                &report(
                    open_at(2, dec!(0)),
                    dec!(1),
                    dec!(1),
                    ReportedTerms::NONE.with_price(true),
                ),
            );
            assert_eq!(
                times(&orders),
                TermsTimes {
                    price: Some(t(2)),
                    ..TermsTimes::default()
                }
            );
        }

        /// An untracked order tracked from a report stating its terms holds them from that
        /// report's time; one tracked from a report stating none holds no terms time.
        #[test]
        fn an_order_tracked_from_a_report_stating_terms_holds_their_time() {
            let mut orders = Orders::<ExchangeId, u64>::default();
            apply(
                &mut orders,
                &report(open_at(4, dec!(0)), dec!(2), dec!(3), ReportedTerms::ALL),
            );
            assert_eq!(times(&orders), all_at(4));
            let applied = apply(
                &mut orders,
                &report(open_at(2, dec!(0)), dec!(1), dec!(1), ReportedTerms::ALL),
            );
            assert_eq!(applied, SnapshotApplied::default());
            assert_eq!(tracked_order(&orders).quantity, dec!(3));

            let mut orders = Orders::<ExchangeId, u64>::default();
            apply(
                &mut orders,
                &report(open_at(4, dec!(0)), dec!(2), dec!(3), ReportedTerms::NONE),
            );
            assert_eq!(times(&orders), TermsTimes::default());
        }

        /// The venue's answer to an amend states the terms it commits, at the time of the state it
        /// gives: a report stamped earlier, of the terms before the amend, does not undo them.
        #[test]
        fn an_amend_answer_records_the_time_of_its_terms() {
            use rustrade_execution::order::{amend::Amended, request::OrderResponseAmend};

            let mut orders = tracked(ActiveOrderState::Open(open_at(0, dec!(0))), dec!(1));
            let patch = AmendPatch::default().with_price(dec!(2));
            let current = order(cid(), ());
            orders.record_in_flight_amend(
                &OrderRequestAmend {
                    key: current.key.clone(),
                    state: RequestAmend::new(
                        VenueOrderId::Assigned(OrderId::new("1")),
                        patch,
                        AmendTarget {
                            side: current.side,
                            price: current.price,
                            quantity: current.quantity,
                            kind: current.kind,
                            time_in_force: current.time_in_force,
                            reduce_only: Some(ReduceOnly::No),
                        },
                        Decimal::ZERO,
                    ),
                },
                TIME_SENT,
            );
            // A report while the amend is in flight does not move the terms time.
            apply(
                &mut orders,
                &report(open_at(3, dec!(0)), dec!(1), dec!(1), ReportedTerms::ALL),
            );
            assert_eq!(times(&orders), TermsTimes::default());

            orders.update_from_amend_response(&OrderResponseAmend::<ExchangeId, u64, u64> {
                key: current.key,
                state: Ok(Amended::new(
                    open_at(6, dec!(0)),
                    Some(dec!(2)),
                    dec!(1),
                    OrderKind::Limit,
                    None,
                )),
            });
            assert_eq!(
                times(&orders),
                TermsTimes {
                    time_in_force: None,
                    ..all_at(6)
                }
            );

            let applied = apply(
                &mut orders,
                &report(open_at(4, dec!(0)), dec!(1), dec!(1), ReportedTerms::ALL),
            );
            assert_eq!(applied, SnapshotApplied::default());
            assert_eq!(tracked_order(&orders).price, Some(dec!(2)));
        }

        /// A tracked order serializes as the order's own fields, with its terms times beside them
        /// only once one is set, so an order serialized before the field existed reads back with
        /// none. An `Orders` holding one round-trips whole, as a restored engine reads it.
        #[test]
        fn a_tracked_order_serializes_as_its_order_with_its_terms_times_beside_it() {
            let order = Order {
                quantity: dec!(1),
                ..order(cid(), ActiveOrderState::Open(open_at(0, dec!(0))))
            };
            let order_json = serde_json::to_value(&order).unwrap();
            // The flattened layout relies on the order having no field of the same name.
            assert!(order_json.get("terms_times").is_none(), "{order_json}");

            let untimed = TrackedOrder::new(order.clone());
            let json = serde_json::to_value(&untimed).unwrap();
            assert_eq!(json, order_json);
            assert_eq!(
                serde_json::from_value::<TrackedOrder<ExchangeId, u64>>(json).unwrap(),
                untimed
            );

            let mut timed = untimed;
            timed
                .terms_times
                .stated_at(ReportedTerms::NONE.with_price(true), t(7));
            let json = serde_json::to_value(&timed).unwrap();
            assert_eq!(
                json["terms_times"]["price"],
                serde_json::to_value(t(7)).unwrap()
            );
            assert_eq!(
                serde_json::from_value::<TrackedOrder<ExchangeId, u64>>(json).unwrap(),
                timed
            );

            let mut orders = Orders::<ExchangeId, u64>::default();
            orders.0.insert(cid(), timed.clone());
            let restored: Orders<ExchangeId, u64> =
                serde_json::from_str(&serde_json::to_string(&orders).unwrap()).unwrap();
            assert_eq!(restored, orders);
            assert_eq!(times(&restored), timed.terms_times());

            // A time is never moved back.
            timed
                .terms_times
                .stated_at(ReportedTerms::NONE.with_price(true), t(3));
            assert_eq!(timed.terms_times().price, Some(t(7)));
        }

        /// A report stating only some terms says nothing of when the others last changed: a later
        /// report stating only the price does not hide an earlier-stamped report of a new
        /// quantity, while that report's older price is not read.
        #[test]
        fn a_report_stating_some_terms_does_not_hide_an_earlier_change_to_others() {
            let mut orders = tracked(ActiveOrderState::Open(open_at(0, dec!(0))), dec!(1));
            apply(
                &mut orders,
                &report(
                    open_at(5, dec!(0)),
                    dec!(1),
                    dec!(1),
                    ReportedTerms::NONE.with_price(true),
                ),
            );

            let applied = apply(
                &mut orders,
                &report(open_at(3, dec!(0)), dec!(2), dec!(3), ReportedTerms::ALL),
            );
            assert_eq!(
                applied,
                SnapshotApplied::working(terms_of(
                    changed(limit(dec!(1), dec!(1)), limit(dec!(1), dec!(3))),
                    None
                ))
            );
            let order = tracked_order(&orders);
            assert_eq!((order.price, order.quantity), (Some(dec!(1)), dec!(3)));
            assert_eq!(
                times(&orders),
                TermsTimes {
                    price: Some(t(5)),
                    ..all_at(3)
                }
            );
        }

        /// Behind a cancel in flight, a report whose state does not supersede the one the cancel
        /// carries still changes the terms, and the state carried stays.
        #[test]
        fn an_out_of_sequence_report_behind_a_cancel_changes_the_terms_and_keeps_the_state() {
            let held = ActiveOrderState::CancelInFlight(CancelInFlight::new(
                Some(open_at(5, dec!(0))),
                TIME_SENT,
            ));
            let mut orders = tracked(held.clone(), dec!(1));
            let applied = apply(
                &mut orders,
                &report(open_at(3, dec!(0)), dec!(1), dec!(2), ReportedTerms::ALL),
            );
            assert_eq!(
                applied,
                SnapshotApplied::working(terms_of(
                    changed(limit(dec!(1), dec!(1)), limit(dec!(1), dec!(2))),
                    None
                ))
            );
            let order = tracked_order(&orders);
            assert_eq!(order.quantity, dec!(2));
            assert_eq!(order.state, held);
        }

        /// While an amend is in flight, a report whose state does not supersede the one held is
        /// still compared with the order before and after the amend: terms matching neither are
        /// reported, nothing is adopted, and the state held stays.
        #[test]
        fn an_out_of_sequence_report_during_an_amend_is_compared_and_keeps_the_state() {
            let patch = AmendPatch::default().with_quantity(dec!(3));
            let held = ActiveOrderState::AmendInFlight(AmendInFlight {
                order: open_at(5, dec!(0)),
                patch,
                time_sent: TIME_SENT,
            });
            let mut orders = tracked(held.clone(), dec!(1));
            let applied = apply(
                &mut orders,
                &report(open_at(3, dec!(0)), dec!(1), dec!(4), ReportedTerms::ALL),
            );
            assert_eq!(
                applied,
                SnapshotApplied::working(terms_of(
                    None,
                    Some(OrderTermsDiverged {
                        key: order(cid(), ()).key,
                        tracked: limit(dec!(1), dec!(1)),
                        reported: limit(dec!(1), dec!(4)),
                        terms: ReportedTerms::NONE.with_quantity(true),
                        amend_in_flight: true,
                    })
                ))
            );
            let order = tracked_order(&orders);
            assert_eq!(order.quantity, dec!(1));
            assert_eq!(order.state, held);
            assert_eq!(times(&orders), TermsTimes::default());
        }

        /// A venue resizing an order to what filled completes it, and one reporting more filled
        /// than the order holds ends it too.
        #[test]
        fn a_resize_to_or_below_what_filled_retires_the_order_and_reports_the_change() {
            for quantity in [dec!(1), dec!(0.5)] {
                let mut orders = tracked(ActiveOrderState::Open(open_at(0, dec!(1))), dec!(2));
                let applied = apply(
                    &mut orders,
                    &report(open_at(1, dec!(1)), dec!(1), quantity, ReportedTerms::ALL),
                );
                assert_eq!(
                    applied,
                    SnapshotApplied::retired(terms_of(
                        changed(limit(dec!(1), dec!(2)), limit(dec!(1), quantity)),
                        None
                    )),
                    "{quantity}"
                );
                assert!(orders.0.is_empty(), "{quantity}");
                assert!(
                    orders.1.contains(&cid()),
                    "{quantity}: remembered as retired"
                );
            }
        }

        /// The venue's first report of an order in flight may state terms other than the request's.
        #[test]
        fn the_first_report_of_an_order_in_flight_changes_its_terms() {
            let mut orders = tracked(
                ActiveOrderState::OpenInFlight(OpenInFlight::new(TIME_SENT)),
                dec!(1),
            );
            let applied = apply(
                &mut orders,
                &report(open_at(1, dec!(0)), dec!(0.9), dec!(1), ReportedTerms::ALL),
            );
            assert_eq!(
                applied,
                SnapshotApplied::working(terms_of(
                    changed(limit(dec!(1), dec!(1)), limit(dec!(0.9), dec!(1))),
                    None
                ))
            );
            assert_eq!(
                tracked_order(&orders).state,
                ActiveOrderState::Open(open_at(1, dec!(0)))
            );
        }

        /// Behind a cancel in flight, terms are adopted, but the order is left for the cancel's
        /// answer to end, even when they leave it nothing to fill.
        #[test]
        fn terms_are_adopted_behind_a_cancel_but_the_order_is_not_retired() {
            for held in [Some(open_at(0, dec!(0.5))), None] {
                let mut orders = tracked(
                    ActiveOrderState::CancelInFlight(CancelInFlight::new(held.clone(), TIME_SENT)),
                    dec!(1),
                );
                let applied = apply(
                    &mut orders,
                    &report(
                        open_at(1, dec!(0.5)),
                        dec!(1),
                        dec!(0.5),
                        ReportedTerms::ALL,
                    ),
                );
                assert_eq!(
                    applied,
                    SnapshotApplied::working(terms_of(
                        changed(limit(dec!(1), dec!(1)), limit(dec!(1), dec!(0.5))),
                        None
                    )),
                    "{held:?}"
                );
                assert_eq!(
                    tracked_order(&orders).state,
                    ActiveOrderState::CancelInFlight(CancelInFlight::new(
                        Some(open_at(1, dec!(0.5))),
                        TIME_SENT
                    )),
                    "{held:?}"
                );
            }

            // A report stamped earlier than the terms held, behind a cancel: its fill is taken,
            // its terms are not.
            let mut orders = tracked(
                ActiveOrderState::CancelInFlight(CancelInFlight::new(
                    Some(open_at(5, dec!(0))),
                    TIME_SENT,
                )),
                dec!(1),
            );
            apply(
                &mut orders,
                &report(open_at(5, dec!(0)), dec!(1), dec!(1), ReportedTerms::ALL),
            );
            let applied = apply(
                &mut orders,
                &report(open_at(1, dec!(0.5)), dec!(1), dec!(3), ReportedTerms::ALL),
            );
            assert_eq!(applied, SnapshotApplied::default());
            assert_eq!(tracked_order(&orders).quantity, dec!(1));
        }

        /// A cancel refused after terms adopted behind it left the order nothing to fill ends the
        /// order, as a refused amend does, rather than restoring it as working.
        #[test]
        fn a_refused_cancel_retires_an_order_its_adopted_terms_left_nothing_to_fill() {
            let mut orders = tracked(
                ActiveOrderState::CancelInFlight(CancelInFlight::new(
                    Some(open_at(0, dec!(0.5))),
                    TIME_SENT,
                )),
                dec!(1),
            );
            apply(
                &mut orders,
                &report(
                    open_at(1, dec!(0.5)),
                    dec!(1),
                    dec!(0.5),
                    ReportedTerms::ALL,
                ),
            );
            assert_eq!(tracked_order(&orders).quantity, dec!(0.5));

            orders.update_from_cancel_response(&response_cancel_err(cid()));
            assert!(orders.0.is_empty());
            assert!(orders.1.contains(&cid()));
        }

        /// While an amend is in flight nothing is adopted: a report of the order before or after
        /// the amend is expected, and one of neither is reported. The order is retired only once
        /// nothing is left of the greater of the two quantities.
        #[test]
        fn nothing_is_adopted_while_an_amend_is_in_flight() {
            let mut orders = tracked(ActiveOrderState::Open(open_at(0, dec!(0))), dec!(1));
            let patch = AmendPatch::default().with_quantity(dec!(3));
            let current = order(cid(), ());
            orders.record_in_flight_amend(
                &OrderRequestAmend {
                    key: current.key,
                    state: RequestAmend::new(
                        VenueOrderId::Assigned(OrderId::new("1")),
                        patch,
                        AmendTarget {
                            side: current.side,
                            price: current.price,
                            quantity: current.quantity,
                            kind: current.kind,
                            time_in_force: current.time_in_force,
                            reduce_only: Some(ReduceOnly::No),
                        },
                        Decimal::ZERO,
                    ),
                },
                TIME_SENT,
            );

            let applied = apply(
                &mut orders,
                &report(open_at(1, dec!(0)), dec!(1), dec!(3), ReportedTerms::ALL),
            );
            assert_eq!(applied, SnapshotApplied::default());
            assert_eq!(tracked_order(&orders).quantity, dec!(1));

            let applied = apply(
                &mut orders,
                &report(open_at(2, dec!(0)), dec!(1), dec!(4), ReportedTerms::ALL),
            );
            assert_eq!(
                applied,
                SnapshotApplied::working(terms_of(
                    None,
                    Some(OrderTermsDiverged {
                        key: order(cid(), ()).key,
                        tracked: limit(dec!(1), dec!(1)),
                        reported: limit(dec!(1), dec!(4)),
                        terms: ReportedTerms::NONE.with_quantity(true),
                        amend_in_flight: true,
                    })
                ))
            );

            let applied = apply(
                &mut orders,
                &report(open_at(3, dec!(1)), dec!(1), dec!(1), ReportedTerms::NONE),
            );
            assert!(!applied.retired, "the raise may have been made");
            assert_eq!(
                tracked_order(&orders).state,
                ActiveOrderState::AmendInFlight(AmendInFlight {
                    order: open_at(3, dec!(1)),
                    patch,
                    time_sent: TIME_SENT,
                })
            );
            let applied = apply(
                &mut orders,
                &report(open_at(4, dec!(3)), dec!(1), dec!(1), ReportedTerms::NONE),
            );
            assert!(applied.retired);
            assert!(orders.0.is_empty());
        }

        /// A change of kind is not adopted but reported, while the quantity the same report
        /// states is adopted.
        #[test]
        fn a_change_of_kind_is_reported_and_the_rest_of_the_report_adopted() {
            let mut orders = tracked(ActiveOrderState::Open(open_at(0, dec!(0))), dec!(1));
            let mut reported = report(open_at(1, dec!(0)), dec!(1), dec!(2), ReportedTerms::ALL);
            reported.order.kind = OrderKind::StopLimit {
                trigger_price: dec!(0.5),
            };

            let applied = apply(&mut orders, &reported);
            assert_eq!(
                applied,
                SnapshotApplied::working(terms_of(
                    changed(limit(dec!(1), dec!(1)), limit(dec!(1), dec!(2))),
                    Some(OrderTermsDiverged {
                        key: order(cid(), ()).key,
                        tracked: limit(dec!(1), dec!(2)),
                        reported: OrderTerms::of(&reported.order),
                        terms: ReportedTerms::NONE.with_kind(true),
                        amend_in_flight: false,
                    })
                ))
            );
            assert_eq!(tracked_order(&orders).kind, OrderKind::Limit);
        }

        /// An untracked order reported filled beyond its quantity has ended, like one filled to it.
        #[test]
        fn an_untracked_order_reported_filled_beyond_its_quantity_is_not_tracked() {
            let mut orders = Orders::<ExchangeId, u64>::default();
            let applied = apply(
                &mut orders,
                &report(open_at(1, dec!(2)), dec!(1), dec!(1), ReportedTerms::NONE),
            );
            assert_eq!(applied, SnapshotApplied::default());
            assert!(orders.0.is_empty());
            assert!(orders.1.contains(&cid()));
        }
    }

    #[allow(clippy::unwrap_used, clippy::expect_used)] // Test code: panics on bad input are acceptable
    mod untracked {
        use super::*;
        use rustrade_execution::order::{report::ReportedTerms, state::OpenInFlight};

        fn cid() -> ClientOrderId {
            ClientOrderId::new("untracked")
        }

        /// A report of the untracked order in `state`, `quantity` in total, stating `stated`.
        fn report(
            state: OrderState<u64, u64>,
            quantity: Decimal,
            stated: ReportedTerms,
        ) -> OrderReport<ExchangeId, u64, u64> {
            OrderReport::new(
                Order {
                    quantity,
                    ..order(cid(), state)
                },
                stated,
            )
        }

        fn working(filled: Decimal) -> OrderState<u64, u64> {
            OrderState::active(Open {
                filled_quantity: filled,
                ..open_assigned("venue", DateTime::<Utc>::MIN_UTC)
            })
        }

        fn apply(
            orders: &mut Orders<ExchangeId, u64>,
            report: &OrderReport<ExchangeId, u64, u64>,
        ) -> SnapshotApplied<ExchangeId, u64> {
            orders.update_from_order_snapshot(Snapshot(report))
        }

        /// A venue's report of a working order the engine does not track is tracked on its terms
        /// as they stand, and returned as adopted with the terms it stated.
        #[test]
        fn a_venues_report_of_an_untracked_working_order_is_adopted() {
            for stated in [ReportedTerms::NONE, ReportedTerms::ALL] {
                let mut orders = orders([]);
                let report = report(working(dec!(0.5)), dec!(2), stated);

                let applied = apply(&mut orders, &report);

                let tracked = orders.get(&cid()).expect("tracked").clone();
                assert_eq!(tracked.quantity, dec!(2));
                assert_eq!(Some(tracked.clone()), report.order.to_active());
                assert!(!applied.retired);
                assert_eq!(
                    applied.orders.adopted,
                    vec![OrderAdopted {
                        order: tracked,
                        reported: stated,
                    }],
                    "stated {stated:?}"
                );
                assert!(applied.orders.not_adopted.is_empty());
                assert!(applied.orders.changed.is_empty() && applied.orders.diverged.is_empty());
            }
        }

        /// A report of an untracked working order with no positive quantity is refused, since
        /// tracked it would read as finished, and not remembered: a later report of the same
        /// order that can be tracked is adopted.
        #[test]
        fn an_untracked_working_order_with_no_positive_quantity_is_refused_until_reported_with_one()
        {
            // A filled part leaves the order nothing to fill of such a quantity too: it is still
            // refused, not taken as finished.
            for (quantity, filled) in [(dec!(0), dec!(0)), (dec!(-1), dec!(0)), (dec!(0), dec!(1))]
            {
                let mut orders = orders([]);
                let refused = report(working(filled), quantity, ReportedTerms::NONE);

                let applied = apply(&mut orders, &refused);

                assert!(orders.get(&cid()).is_none(), "quantity {quantity}");
                assert!(
                    !orders.1.contains(&cid()),
                    "remembered at quantity {quantity}"
                );
                assert!(!applied.retired);
                assert!(applied.orders.adopted.is_empty());
                assert_eq!(
                    applied.orders.not_adopted,
                    vec![OrderNotAdopted {
                        order: refused.order.to_active().expect("working"),
                        reason: OrderNotAdoptedReason::QuantityNotPositive,
                    }]
                );

                let later = report(working(dec!(0)), dec!(1), ReportedTerms::NONE);
                let applied = apply(&mut orders, &later);
                assert_eq!(
                    orders.get(&cid()).map(|order| order.quantity),
                    Some(dec!(1))
                );
                assert_eq!(applied.orders.adopted.len(), 1);
                assert!(applied.orders.not_adopted.is_empty());
            }
        }

        /// The engine's own record of an open request is tracked, not adopted, whatever its
        /// quantity: it is no venue's report, and the request answers for it.
        #[test]
        fn the_engines_own_open_in_flight_record_is_tracked_without_an_adoption() {
            for quantity in [dec!(1), dec!(0)] {
                let mut orders = orders([]);
                let record = report(
                    OrderState::active(OpenInFlight {
                        time_sent: TIME_SENT,
                    }),
                    quantity,
                    ReportedTerms::NONE,
                );

                let applied = apply(&mut orders, &record);

                assert!(orders.get(&cid()).is_some(), "quantity {quantity}");
                assert!(applied.orders.is_empty(), "{applied:?}");
            }
        }

        /// Neither a late report of a retired order nor a report of an untracked order with
        /// nothing left to fill adopts anything.
        #[test]
        fn a_retired_or_finished_untracked_order_is_not_adopted() {
            let mut orders = orders([]);
            let finished = report(working(dec!(1)), dec!(1), ReportedTerms::ALL);

            let applied = apply(&mut orders, &finished);
            assert!(orders.get(&cid()).is_none());
            assert!(orders.1.contains(&cid()));
            assert!(applied.orders.is_empty(), "{applied:?}");

            let late = report(working(dec!(0)), dec!(1), ReportedTerms::ALL);
            let applied = apply(&mut orders, &late);
            assert!(orders.get(&cid()).is_none());
            assert!(applied.orders.is_empty(), "{applied:?}");
        }
    }

    #[allow(clippy::unwrap_used, clippy::expect_used)] // Test code: panics on bad input are acceptable
    mod replaced {
        use super::*;
        use rustrade_execution::order::{
            amend::{AmendPatch, AmendTarget, Amended, RequestAmend},
            report::{Replaces, ReportedTerms},
            request::{OrderRequestAmend, OrderResponseAmend, ReduceOnly},
            state::{AmendInFlight, Cancelled, OpenInFlight},
        };

        fn t(secs: i64) -> DateTime<Utc> {
            time_plus_secs(DateTime::<Utc>::MIN_UTC, secs)
        }

        fn old_cid() -> ClientOrderId {
            ClientOrderId::new("old")
        }

        fn new_cid() -> ClientOrderId {
            ClientOrderId::new("new")
        }

        fn id(id: &str) -> OrderId {
            OrderId::new(id)
        }

        fn working(venue_id: &str, secs: i64, filled: Decimal) -> Open {
            Open {
                filled_quantity: filled,
                ..open_assigned(venue_id, t(secs))
            }
        }

        /// An order tracked under `cid` in `state`, 2 in total, for strategy "strategy".
        fn tracked_as(cid: ClientOrderId, state: ActiveOrderState) -> Orders<ExchangeId, u64> {
            let mut order = order(cid, state);
            order.quantity = dec!(2);
            order.key.strategy = StrategyId::new("strategy");
            orders([order])
        }

        /// A report under `cid` in `state`, 2 in total at price 3, stating every term, linked by
        /// `replaces` if set. Its strategy is unknown, as a venue's report of an order not placed
        /// under that id has it.
        fn report(
            cid: ClientOrderId,
            state: OrderState<u64, u64>,
            replaces: Option<Replaces>,
        ) -> OrderReport<ExchangeId, u64, u64> {
            let mut order = order(cid, state);
            order.quantity = dec!(2);
            order.price = Some(dec!(3));
            let report = OrderReport::new(order, ReportedTerms::ALL);
            match replaces {
                Some(replaces) => report.with_replaces(replaces),
                None => report,
            }
        }

        fn apply(
            orders: &mut Orders<ExchangeId, u64>,
            report: &OrderReport<ExchangeId, u64, u64>,
        ) -> SnapshotApplied<ExchangeId, u64> {
            orders.update_from_order_snapshot(Snapshot(report))
        }

        fn cancelled(venue_id: &str, secs: i64) -> OrderState<u64, u64> {
            OrderState::inactive(Cancelled {
                id: id(venue_id),
                time_exchange: t(secs),
                filled_quantity: Some(dec!(0.5)),
            })
        }

        fn state_of(orders: &Orders<ExchangeId, u64>, cid: &ClientOrderId) -> ActiveOrderState {
            orders.get(cid).expect("tracked").state.clone()
        }

        /// The order `orders` tracks under the replacement's cid, followed from "1" to "2".
        fn followed() -> Orders<ExchangeId, u64> {
            let mut orders = tracked_as(
                old_cid(),
                ActiveOrderState::Open(working("1", 0, dec!(0.5))),
            );
            let linked = report(
                old_cid(),
                OrderState::active(working("2", 1, dec!(0.5))),
                Some(Replaces::new(id("1"))),
            );
            apply(&mut orders, &linked);
            orders
        }

        /// A report linking a new venue order id to the one held moves the order to it, keeping
        /// it one order: the old id is recorded, the report's terms adopted, and the replace
        /// returned ahead of the change of terms.
        #[test]
        fn a_new_venue_order_id_linked_to_the_one_held_is_followed() {
            let mut orders = tracked_as(
                old_cid(),
                ActiveOrderState::Open(working("1", 0, dec!(0.5))),
            );
            let linked = report(
                old_cid(),
                OrderState::active(working("2", 1, dec!(0.5))),
                Some(Replaces::new(id("1"))),
            );

            let applied = apply(&mut orders, &linked);

            assert!(!applied.retired);
            let tracked = orders.tracked(&old_cid()).expect("tracked");
            assert_eq!(
                tracked.order.state,
                ActiveOrderState::Open(working("2", 1, dec!(0.5)))
            );
            assert_eq!(tracked.order.price, Some(dec!(3)));
            assert_eq!(tracked.order.key.strategy, StrategyId::new("strategy"));
            assert_eq!(tracked.superseded_ids(), [id("1")]);
            assert_eq!(
                applied.orders.replaced,
                vec![OrderReplaced {
                    key: tracked.order.key.clone(),
                    id: id("2"),
                    replaced: Replaces::new(id("1")),
                }]
            );
            assert_eq!(applied.orders.changed.len(), 1, "{applied:?}");

            // A later report repeating the link is of the order already followed.
            let repeated = report(
                old_cid(),
                OrderState::active(working("2", 2, dec!(0.5))),
                Some(Replaces::new(id("1"))),
            );
            let applied = apply(&mut orders, &repeated);
            assert!(applied.orders.replaced.is_empty(), "{applied:?}");
            assert_eq!(
                orders.tracked(&old_cid()).unwrap().superseded_ids(),
                [id("1")]
            );
        }

        /// Followed, the order ignores late reports of itself under the id replaced, working or
        /// ended, and is ended by a report of its new id.
        #[test]
        fn late_reports_naming_the_id_replaced_are_ignored() {
            let mut orders = followed();
            let held = state_of(&orders, &old_cid());

            for late in [
                OrderState::active(working("1", 5, dec!(1))),
                cancelled("1", 5),
                OrderState::fully_filled(Filled::new(id("1"), t(5), dec!(2), None)),
            ] {
                let applied = apply(&mut orders, &report(old_cid(), late.clone(), None));
                assert_eq!(state_of(&orders, &old_cid()), held, "{late:?}");
                assert_eq!(applied, SnapshotApplied::default(), "{late:?}");
            }

            // With an amend or a cancel in flight too.
            for in_flight in [
                ActiveOrderState::AmendInFlight(AmendInFlight {
                    order: working("2", 1, dec!(0.5)),
                    patch: AmendPatch::default().with_price(dec!(4)),
                    time_sent: TIME_SENT,
                }),
                ActiveOrderState::CancelInFlight(CancelInFlight {
                    order: Some(working("2", 1, dec!(0.5))),
                    time_sent: TIME_SENT,
                }),
            ] {
                let mut orders = followed();
                orders.0.get_mut(&old_cid()).unwrap().order.state = in_flight.clone();
                let late = report(
                    old_cid(),
                    OrderState::active(working("1", 5, dec!(1))),
                    None,
                );
                apply(&mut orders, &late);
                assert_eq!(state_of(&orders, &old_cid()), in_flight);
            }

            let applied = apply(&mut orders, &report(old_cid(), cancelled("2", 6), None));
            assert!(applied.retired);
            assert!(orders.get(&old_cid()).is_none());
        }

        /// Only a link naming the id held is followed: an unlinked report naming another venue
        /// order, or one linked to an id the order is not addressed by, is refused as before.
        #[test]
        fn a_new_venue_order_id_not_linked_to_the_one_held_is_refused() {
            for replaces in [None, Some(Replaces::new(id("9")))] {
                let mut orders = tracked_as(
                    old_cid(),
                    ActiveOrderState::Open(working("1", 0, dec!(0.5))),
                );
                let held = state_of(&orders, &old_cid());
                let other = report(
                    old_cid(),
                    OrderState::active(working("2", 1, dec!(0.5))),
                    replaces.clone(),
                );

                let applied = apply(&mut orders, &other);

                assert_eq!(state_of(&orders, &old_cid()), held, "{replaces:?}");
                assert!(applied.orders.is_empty(), "{replaces:?}");
                assert!(
                    orders
                        .tracked(&old_cid())
                        .unwrap()
                        .superseded_ids()
                        .is_empty()
                );
            }
        }

        /// An order the venue has not named yet takes the replacement's id as the venue's first,
        /// and records the id linked as one replaced, so the old order's cancel cannot end it.
        /// No replace is returned: no id of the order's was known to move from.
        #[test]
        fn an_order_the_venue_has_not_named_records_the_id_its_first_report_replaces() {
            let mut orders = tracked_as(
                old_cid(),
                ActiveOrderState::OpenInFlight(OpenInFlight {
                    time_sent: TIME_SENT,
                }),
            );
            let linked = report(
                old_cid(),
                OrderState::active(working("2", 1, dec!(0))),
                Some(Replaces::new(id("1"))),
            );

            let applied = apply(&mut orders, &linked);

            assert!(applied.orders.replaced.is_empty(), "{applied:?}");
            let tracked = orders.tracked(&old_cid()).unwrap();
            assert_eq!(
                tracked.order.state,
                ActiveOrderState::Open(working("2", 1, dec!(0)))
            );
            assert_eq!(tracked.superseded_ids(), [id("1")]);
            apply(&mut orders, &report(old_cid(), cancelled("1", 2), None));
            assert!(orders.get(&old_cid()).is_some());
        }

        /// A late fill naming the id replaced counts towards the order, never moving it back;
        /// one naming a venue order it was never known by does not.
        #[test]
        fn a_fill_naming_the_id_replaced_counts_towards_the_order() {
            let mut orders = followed();

            assert!(!orders.update_from_fill(&old_cid(), &id("1"), dec!(0.4)));
            assert!(!orders.update_from_fill(&old_cid(), &id("9"), dec!(1.5)));
            assert_eq!(
                state_of(&orders, &old_cid()),
                ActiveOrderState::Open(working("2", 1, dec!(0.5)))
            );

            assert!(!orders.update_from_fill(&old_cid(), &id("1"), dec!(1.5)));
            assert_eq!(
                state_of(&orders, &old_cid()),
                ActiveOrderState::Open(working("2", 1, dec!(1.5)))
            );
            assert!(orders.update_from_fill(&old_cid(), &id("1"), dec!(2)));
            assert!(orders.get(&old_cid()).is_none());
        }

        /// A report under a new client order id linked to the order replaced moves that order to
        /// it, found by the client order id named or, failing one, by its venue order id: one
        /// order, with its strategy and fills, the old client order id retired so a late report
        /// under it is ignored.
        #[test]
        fn a_new_client_order_id_linked_to_a_tracked_order_moves_it() {
            // The venue order id changed too, kept, or the link named no client order id.
            for (venue_id, replaces) in [
                ("2", Replaces::new(id("1")).with_cid(old_cid())),
                ("1", Replaces::new(id("1")).with_cid(old_cid())),
                ("2", Replaces::new(id("1"))),
            ] {
                let mut orders = tracked_as(
                    old_cid(),
                    ActiveOrderState::Open(working("1", 0, dec!(0.5))),
                );
                let linked = report(
                    new_cid(),
                    OrderState::active(working(venue_id, 1, dec!(0.5))),
                    Some(replaces.clone()),
                );

                let applied = apply(&mut orders, &linked);

                let case = format!("{venue_id} {replaces:?}");
                assert!(orders.get(&old_cid()).is_none(), "{case}");
                assert!(orders.1.contains(&old_cid()), "{case}");
                let tracked = orders.tracked(&new_cid()).expect("moved");
                assert_eq!(tracked.order.key.cid, new_cid());
                assert_eq!(tracked.order.key.strategy, StrategyId::new("strategy"));
                assert_eq!(
                    tracked.order.state,
                    ActiveOrderState::Open(working(venue_id, 1, dec!(0.5))),
                    "{case}"
                );
                let superseded: &[OrderId] = if venue_id == "1" { &[] } else { &[id("1")] };
                assert_eq!(tracked.superseded_ids(), superseded, "{case}");
                assert_eq!(
                    applied.orders.replaced,
                    vec![OrderReplaced {
                        key: tracked.order.key.clone(),
                        id: id(venue_id),
                        replaced: Replaces::new(id("1")).with_cid(old_cid()),
                    }],
                    "{case}"
                );
                assert!(applied.orders.adopted.is_empty(), "{case}");

                // The old order's end, reported under its old client order id, ends nothing.
                let applied = apply(&mut orders, &report(old_cid(), cancelled("1", 2), None));
                assert!(orders.get(&new_cid()).is_some(), "{case}");
                assert!(orders.get(&old_cid()).is_none(), "{case}");
                assert!(!applied.retired, "{case}");
            }
        }

        /// Moved to a new client order id, an order drops a cancel or amend in flight under the
        /// old one, whose answer is then ignored, and takes the report's state.
        #[test]
        fn a_move_to_a_new_client_order_id_drops_the_request_in_flight() {
            let before = working("1", 0, dec!(0.5));
            for in_flight in [
                ActiveOrderState::AmendInFlight(AmendInFlight {
                    order: before.clone(),
                    patch: AmendPatch::default().with_price(dec!(4)),
                    time_sent: TIME_SENT,
                }),
                ActiveOrderState::CancelInFlight(CancelInFlight {
                    order: Some(before.clone()),
                    time_sent: TIME_SENT,
                }),
            ] {
                let mut orders = tracked_as(old_cid(), in_flight.clone());
                let linked = report(
                    new_cid(),
                    OrderState::active(working("2", 1, dec!(0.5))),
                    Some(Replaces::new(id("1")).with_cid(old_cid())),
                );

                apply(&mut orders, &linked);

                assert_eq!(
                    state_of(&orders, &new_cid()),
                    ActiveOrderState::Open(working("2", 1, dec!(0.5))),
                    "{in_flight:?}"
                );
                assert!(!orders.has_request_in_flight(), "{in_flight:?}");

                // The answer under the old id finds no order.
                let order = order(old_cid(), ());
                orders.update_from_amend_response(&OrderResponseAmend::<ExchangeId, u64, u64> {
                    key: order.key,
                    state: Ok(Amended::new(
                        working("1", 2, dec!(0.5)),
                        Some(dec!(4)),
                        dec!(2),
                        OrderKind::Limit,
                        None,
                    )),
                });
                assert_eq!(
                    state_of(&orders, &new_cid()),
                    ActiveOrderState::Open(working("2", 1, dec!(0.5)))
                );
                assert_eq!(orders.get(&new_cid()).unwrap().price, Some(dec!(3)));
            }

            // An order whose cancel went before the venue reported it waits for the report.
            let mut orders = tracked_as(
                old_cid(),
                ActiveOrderState::CancelInFlight(CancelInFlight {
                    order: None,
                    time_sent: TIME_SENT,
                }),
            );
            let linked = report(
                new_cid(),
                OrderState::active(working("2", 1, dec!(0))),
                Some(Replaces::new(id("1")).with_cid(old_cid())),
            );
            apply(&mut orders, &linked);
            assert_eq!(
                state_of(&orders, &new_cid()),
                ActiveOrderState::Open(working("2", 1, dec!(0)))
            );
        }

        /// No order moves when the client order id named holds an order the venue addresses by
        /// another id, or when the link names no order tracked: the report is then of an order
        /// the engine does not track, and is adopted as one.
        #[test]
        fn a_link_to_no_order_tracked_moves_nothing() {
            for replaces in [
                Replaces::new(id("1")).with_cid(old_cid()),
                Replaces::new(id("7")),
                Replaces::new(id("7")).with_cid(ClientOrderId::new("absent")),
            ] {
                let mut orders = tracked_as(
                    old_cid(),
                    ActiveOrderState::Open(working("5", 0, dec!(0.5))),
                );
                let linked = report(
                    new_cid(),
                    OrderState::active(working("2", 1, dec!(0))),
                    Some(replaces.clone()),
                );

                let applied = apply(&mut orders, &linked);

                assert_eq!(
                    state_of(&orders, &old_cid()),
                    ActiveOrderState::Open(working("5", 0, dec!(0.5))),
                    "{replaces:?}"
                );
                assert!(orders.get(&new_cid()).is_some(), "{replaces:?}");
                assert!(applied.orders.replaced.is_empty(), "{replaces:?}");
                assert_eq!(applied.orders.adopted.len(), 1, "{replaces:?}");
            }
        }

        /// A replay of a replace already followed, under a client order id the order has since
        /// moved on from, moves nothing back: that id is retired.
        #[test]
        fn a_replayed_replace_under_a_retired_cid_moves_nothing() {
            let mut orders = tracked_as(
                old_cid(),
                ActiveOrderState::Open(working("1", 0, dec!(0.5))),
            );
            // A new venue order id under the same client order id, then a new client order id.
            let first = report(
                old_cid(),
                OrderState::active(working("2", 1, dec!(0.5))),
                Some(Replaces::new(id("1"))),
            );
            apply(&mut orders, &first);
            let second = report(
                new_cid(),
                OrderState::active(working("3", 2, dec!(0.5))),
                Some(Replaces::new(id("2")).with_cid(old_cid())),
            );
            apply(&mut orders, &second);
            assert_eq!(
                orders.tracked(&new_cid()).unwrap().superseded_ids(),
                [id("1"), id("2")]
            );

            let applied = apply(&mut orders, &first);

            assert!(orders.get(&old_cid()).is_none());
            assert_eq!(
                state_of(&orders, &new_cid()),
                ActiveOrderState::Open(working("3", 2, dec!(0.5)))
            );
            assert!(!orders.1.contains(&new_cid()));
            assert!(applied.orders.is_empty(), "{applied:?}");
        }

        /// A link on a report naming no venue order id records no replace: under a new client
        /// order id, the order is not moved from the id it had.
        #[test]
        fn a_link_on_a_report_naming_no_venue_order_id_is_ignored() {
            let unnamed = Open {
                id: VenueOrderId::ClientAssigned,
                ..working("2", 1, dec!(0.5))
            };
            for cid in [old_cid(), new_cid()] {
                let mut orders = tracked_as(
                    old_cid(),
                    ActiveOrderState::Open(working("1", 0, dec!(0.5))),
                );
                let linked = report(
                    cid.clone(),
                    OrderState::active(unnamed.clone()),
                    Some(Replaces::new(id("1")).with_cid(old_cid())),
                );

                let applied = apply(&mut orders, &linked);

                let tracked = orders.tracked(&old_cid()).expect("not moved");
                assert!(tracked.superseded_ids().is_empty(), "{cid}");
                // Under the same client order id the unlinked report is then applied as any
                // other, whose state replaces the one held, id included, so only a move is checked.
                if cid == new_cid() {
                    assert_eq!(
                        tracked.order.state.open_meta().map(|open| &open.id),
                        Some(&VenueOrderId::Assigned(id("1")))
                    );
                }
                assert!(applied.orders.replaced.is_empty(), "{cid}");
            }
        }

        /// A replacement first reported ended still moves the order, which it then ends.
        #[test]
        fn a_replacement_reported_ended_moves_and_ends_the_order() {
            let mut orders = tracked_as(
                old_cid(),
                ActiveOrderState::Open(working("1", 0, dec!(0.5))),
            );
            let filled = report(
                new_cid(),
                OrderState::fully_filled(Filled::new(id("2"), t(1), dec!(2), None)),
                Some(Replaces::new(id("1")).with_cid(old_cid())),
            );

            let applied = apply(&mut orders, &filled);

            assert!(applied.retired);
            assert!(orders.0.is_empty());
            assert!(orders.1.contains(&old_cid()) && orders.1.contains(&new_cid()));
            assert_eq!(applied.orders.replaced.len(), 1, "{applied:?}");
            assert_eq!(applied.orders.replaced[0].key.cid, new_cid());
        }

        fn amend_request() -> OrderRequestAmend<ExchangeId, u64> {
            let order = order(old_cid(), ());
            OrderRequestAmend {
                key: order.key,
                state: RequestAmend::new(
                    VenueOrderId::Assigned(id("1")),
                    AmendPatch::default().with_price(dec!(4)),
                    AmendTarget {
                        side: order.side,
                        price: order.price,
                        quantity: dec!(2),
                        kind: order.kind,
                        time_in_force: order.time_in_force,
                        reduce_only: Some(ReduceOnly::No),
                    },
                    dec!(0.5),
                ),
            }
        }

        fn amend_answer(previous: &str, now: &str) -> OrderResponseAmend<ExchangeId, u64, u64> {
            OrderResponseAmend {
                key: order(old_cid(), ()).key,
                state: Ok(Amended::new(
                    working(now, 2, dec!(0.5)),
                    Some(dec!(4)),
                    dec!(2),
                    OrderKind::Limit,
                    Some(VenueOrderId::Assigned(id(previous))),
                )),
            }
        }

        /// The engine's own amend answered under a new venue order id records the one it
        /// replaced, whose late reports are then ignored, and is applied when a linked report
        /// has already moved the order to the new id.
        #[test]
        fn an_amend_answered_under_a_new_venue_order_id_records_the_one_it_replaced() {
            for linked_first in [false, true] {
                let mut orders = tracked_as(
                    old_cid(),
                    ActiveOrderState::Open(working("1", 0, dec!(0.5))),
                );
                orders.record_in_flight_amend(&amend_request(), TIME_SENT);
                if linked_first {
                    let linked = report(
                        old_cid(),
                        OrderState::active(working("2", 1, dec!(0.5))),
                        Some(Replaces::new(id("1"))),
                    );
                    apply(&mut orders, &linked);
                    assert!(
                        matches!(
                            state_of(&orders, &old_cid()),
                            ActiveOrderState::AmendInFlight(AmendInFlight { order, .. })
                                if order.id == VenueOrderId::Assigned(id("2"))
                        ),
                        "the stream moves the order before the answer"
                    );
                }

                orders.update_from_amend_response(&amend_answer("1", "2"));

                let tracked = orders.tracked(&old_cid()).unwrap();
                assert_eq!(tracked.order.price, Some(dec!(4)), "{linked_first}");
                assert_eq!(
                    tracked.order.state,
                    ActiveOrderState::Open(working("2", 2, dec!(0.5))),
                    "{linked_first}"
                );
                assert_eq!(tracked.superseded_ids(), [id("1")], "{linked_first}");
                apply(&mut orders, &report(old_cid(), cancelled("1", 3), None));
                assert!(orders.get(&old_cid()).is_some(), "{linked_first}");
            }

            // An answer naming neither the order's id nor one it moved from is of another order.
            let mut orders = followed();
            orders.0.get_mut(&old_cid()).unwrap().order.state =
                ActiveOrderState::AmendInFlight(AmendInFlight {
                    order: working("2", 1, dec!(0.5)),
                    patch: AmendPatch::default().with_price(dec!(4)),
                    time_sent: TIME_SENT,
                });
            orders.update_from_amend_response(&amend_answer("8", "9"));
            assert_eq!(orders.get(&old_cid()).unwrap().price, Some(dec!(3)));
        }

        /// The ids replaced are kept to a bound, oldest forgotten first, and the id the order is
        /// addressed by is never among them.
        #[test]
        fn the_ids_replaced_are_bounded() {
            let mut orders =
                tracked_as(old_cid(), ActiveOrderState::Open(working("0", 0, dec!(0))));
            for n in 1..=MAX_SUPERSEDED_IDS + 1 {
                let linked = report(
                    old_cid(),
                    OrderState::active(working(&n.to_string(), n as i64, dec!(0))),
                    Some(Replaces::new(id(&(n - 1).to_string()))),
                );
                apply(&mut orders, &linked);
            }
            let tracked = orders.0.get_mut(&old_cid()).unwrap();
            let expected: Vec<OrderId> = (1..=MAX_SUPERSEDED_IDS)
                .map(|n| id(&n.to_string()))
                .collect();
            assert_eq!(tracked.superseded_ids(), expected);

            let current = tracked.current_id().cloned().unwrap();
            tracked.supersede(current);
            tracked.supersede(id("5"));
            assert_eq!(tracked.superseded_ids(), expected);
        }

        /// The ids replaced are serialized beside the order once any is recorded, and read back.
        #[test]
        fn the_ids_replaced_round_trip_and_are_absent_until_one_is_recorded() {
            let plain = tracked_as(old_cid(), ActiveOrderState::Open(working("1", 0, dec!(0))));
            let json = serde_json::to_value(&plain).unwrap();
            assert!(json["old"].get("superseded_ids").is_none(), "{json}");

            let orders = followed();
            let json = serde_json::to_value(&orders).unwrap();
            assert_eq!(json["old"]["superseded_ids"], serde_json::json!(["1"]));
            let back: Orders<ExchangeId, u64> = serde_json::from_value(json).unwrap();
            assert_eq!(
                back.tracked(&old_cid()).unwrap().superseded_ids(),
                [id("1")]
            );
        }
    }
}
