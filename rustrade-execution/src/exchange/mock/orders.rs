//! Resting orders, held in the order a venue would match them.

use crate::order::{
    Order, TimeInForce, UnindexedOrder,
    id::ClientOrderId,
    state::{ActiveOrderState, Open},
};
use chrono::{DateTime, Utc};
use fnv::FnvHashMap;
use rust_decimal::Decimal;
use rustrade_instrument::{
    Side, asset::name::AssetNameExchange, exchange::ExchangeId,
    instrument::name::InstrumentNameExchange,
};
use std::collections::{BTreeMap, BTreeSet, hash_map::Entry};
use thiserror::Error;

/// An open order held by a simulated venue.
pub type OpenOrder = Order<ExchangeId, InstrumentNameExchange, Open>;

/// The balance a venue is holding against one resting order.
///
/// Recorded so the venue settles on a fill, and releases on a cancel, **exactly** what it took when
/// the order rested — rather than recomputing an amount that a changed fee model or contract size
/// could make disagree with the one the client was told about. A CFD fill is the exception: what
/// it costs depends on the position it trades against, which can move while the order rests, so
/// its hold is released and the fill costed afresh (see `SimulatedVenue`'s reserved balances).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reservation {
    /// The asset the order pays with: quote for a buy or a CFD, base for a spot sell.
    pub asset: AssetNameExchange,
    /// How much of it is held, inclusive of the fee the fill will charge. For a CFD, net of what
    /// the fill will pay back by closing a position, so an order that only reduces one holds
    /// nothing.
    ///
    /// Held against the order's **unfilled** quantity alone — its whole quantity for an order that
    /// rested without trading, and the remainder alone for one the book filled in part before it
    /// rested, or one a configured `initial_state` seeded already part-filled.
    pub amount: Decimal,
}

/// One resting order, and whatever the venue is holding against it.
///
/// The two are one value rather than two collections because they have exactly one lifetime: an
/// order that leaves the book takes its reservation with it, and a reservation that outlives its
/// order is held against nothing.
#[derive(Debug, Clone)]
pub struct RestingOrder {
    pub order: OpenOrder,
    /// What the venue holds against [`order`](Self::order), or `None` if it holds nothing.
    ///
    /// `None` for an order that arrived through a configured `initial_state`: the venue did not
    /// take that balance and has nothing of its own to give back. See [`OpenOrders`].
    pub reservation: Option<Reservation>,
}

/// An order [`OpenOrders::insert`] refused, because another is already resting under its
/// [`ClientOrderId`].
///
/// Holds the refused order and its reservation, untouched. Boxed because an order is large and the
/// refusal is the rare path.
#[derive(Debug, Clone, Error)]
#[error("an order is already resting under {}", .0.order.key.cid)]
pub struct AlreadyResting(pub Box<RestingOrder>);

/// One order's place in one side of one instrument's queue.
///
/// Ordered so that a [`BTreeMap`]'s ascending iteration is **best first** for the side it belongs
/// to. Derived field order is the priority rule: price, then arrival, then insertion.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct QueuePosition {
    /// The order's price, ranked for its side: negated on the buy side, so the **highest** bid and
    /// the **lowest** offer both sort first.
    price_rank: Decimal,
    /// When the venue accepted the order. Earlier is matched first, at equal price.
    time_exchange: DateTime<Utc>,
    /// Insertion sequence, which breaks the tie between two orders accepted at the same instant.
    ///
    /// Without it two such orders compare equal and a `BTreeMap` keeps only one of them.
    seq: u64,
}

impl QueuePosition {
    fn new(side: Side, price: Decimal, time_exchange: DateTime<Utc>, seq: u64) -> Self {
        Self {
            price_rank: match side {
                Side::Buy => -price,
                Side::Sell => price,
            },
            time_exchange,
            seq,
        }
    }
}

/// A venue's open orders, indexed for lookup by id **and** for matching in price-time order.
///
/// # Why two indices
///
/// The two accesses want opposite things. Cancelling an order, and answering a snapshot, address
/// one order by [`ClientOrderId`]. Matching a tick walks an instrument's orders from the best price
/// down, and must stop as soon as the price stops crossing.
///
/// Holding only the map makes matching a sort of every open order on every tick. Holding only the
/// queues makes a cancel a scan. The map owns the orders, the queues hold ids.
///
/// # Why price-time and not "whatever the map yields"
///
/// A tick that crosses two resting orders with only enough balance to fill one fills whichever
/// comes first. Read off a hash map, that is an arbitrary choice which a rebuild or a different
/// `ClientOrderId` can silently reverse, so two runs of one backtest need not agree. Price-time
/// priority is both the reproducible rule and the one real venues use.
///
/// # Orders with no price
///
/// An order resting without a limit price cannot be ranked against one, and is not a thing a venue
/// holds: an order with no price to wait at has nothing to wait for. Such an order is kept in the
/// map — a snapshot must report everything the account holds — but is absent from the queues, so
/// matching can never reach it. It can only arrive through a configured `initial_state`.
///
/// # Why a third index for deadlines
///
/// A [`TimeInForce::GoodTillDate`] order retires at a stated instant, and the venue sweeps for such
/// orders on every clock advance *and* every market tick. Neither of the other two indices can
/// answer "which orders are past their deadline": a deadline is unrelated to price, so the
/// price-time queues cannot be walked with a `take_while` the way matching walks them, and the map
/// is unordered. Answering from either would mean scanning every open order on every tick.
///
/// A third index therefore holds **only** the orders that have a deadline, keyed by it. A sweep is
/// a range query that stops at the first order still working, so it costs nothing when nothing can
/// expire — which is every venue whose orders are all `GoodUntilCancelled`, including every
/// market-order-only backtest.
///
/// # Reservations, and the ones that are not there
///
/// An order the venue booked itself carries the [`Reservation`] the venue took for it, so a fill
/// settles and a cancel releases the same amount that was held — see [`RestingOrder`].
///
/// An order seeded by a configured `initial_state` carries `None`, because the venue never took
/// anything for it. Cancelling such an order therefore releases nothing and restates no balance.
/// That is the honest answer rather than a convenient one: the snapshot says an order is resting
/// and says what the balances are, but not which order any held portion belongs to, so a venue
/// that credited a release would be inventing the amount.
#[derive(Debug, Default)]
pub struct OpenOrders {
    by_id: FnvHashMap<ClientOrderId, RestingOrder>,
    /// Per instrument and side, best first. Values are keys into [`by_id`](Self::by_id).
    queues: FnvHashMap<(InstrumentNameExchange, Side), BTreeMap<QueuePosition, ClientOrderId>>,
    /// Every order carrying a deadline, earliest first — see the type's note on deadlines.
    ///
    /// Keyed on `(deadline, cid)` rather than on the deadline alone because two orders can share
    /// one instant. No further tie-break is needed: a [`ClientOrderId`] is unique across open
    /// orders, since [`by_id`](Self::by_id) is keyed by it, so the pair is already total — and
    /// therefore a sweep's order is reproducible between runs.
    expiries: BTreeSet<(DateTime<Utc>, ClientOrderId)>,
    /// Monotone across every instrument, so insertion order is total even across queues.
    seq: u64,
}

/// The instant `order` retires at of its own accord, or `None` if it works until cancelled.
fn deadline_of(order: &OpenOrder) -> Option<DateTime<Utc>> {
    match order.time_in_force {
        TimeInForce::GoodTillDate { expiry } => Some(expiry),
        _ => None,
    }
}

impl OpenOrders {
    /// Inserts `order` holding `reservation` against it, unless an order is already resting under
    /// its [`ClientOrderId`].
    ///
    /// # Errors
    /// [`AlreadyResting`], carrying `order` and `reservation` back untouched, if an order is already
    /// held under its id. The book is left exactly as it was: the order already resting keeps its
    /// queue position, its deadline and its own reservation. `reservation` is handed back rather
    /// than dropped because it may already be held against the account, and only the caller knows
    /// the ledger to give it back to.
    ///
    /// `reservation` is `None` only for an order the venue did not book itself — see the type's
    /// note on reservations. It is a parameter rather than a later setter so that an order cannot
    /// reach the book in a state where what is held against it has not yet been decided.
    pub fn insert(
        &mut self,
        order: OpenOrder,
        reservation: Option<Reservation>,
    ) -> Result<(), AlreadyResting> {
        let cid = order.key.cid.clone();
        let vacant = match self.by_id.entry(cid.clone()) {
            Entry::Occupied(_) => {
                return Err(AlreadyResting(Box::new(RestingOrder {
                    order,
                    reservation,
                })));
            }
            Entry::Vacant(vacant) => vacant,
        };

        if let Some(expiry) = deadline_of(&order) {
            self.expiries.insert((expiry, cid.clone()));
        }

        if let Some(price) = order.price {
            self.seq += 1;
            self.queues
                .entry((order.key.instrument.clone(), order.side))
                .or_default()
                .insert(
                    QueuePosition::new(order.side, price, order.state.time_exchange, self.seq),
                    cid,
                );
        }

        vacant.insert(RestingOrder { order, reservation });
        Ok(())
    }

    /// Whether an order is resting under `cid`.
    pub fn contains(&self, cid: &ClientOrderId) -> bool {
        self.by_id.contains_key(cid)
    }

    /// Removes and returns the order held under `cid`, with whatever is held against it.
    pub fn remove(&mut self, cid: &ClientOrderId) -> Option<RestingOrder> {
        let resting = self.by_id.remove(cid)?;
        self.unindex(&resting.order);
        Some(resting)
    }

    /// Every order whose deadline has been reached by `now`, earliest deadline first.
    ///
    /// "Reached" is inclusive: an order with a deadline of exactly `now` is returned, because a
    /// [`TimeInForce::GoodTillDate`] order stops working *at* its stated instant rather than after
    /// it. The venue relies on this to make a deadline an unconditional cutoff — see
    /// `SimulatedVenue::advance_time`.
    ///
    /// Returned rather than iterated because the caller retires each one, which borrows `self`
    /// mutably. The walk stops at the first order still working, so it costs a single comparison
    /// when nothing is due, whatever the size of the book.
    pub fn expired_as_of(&self, now: DateTime<Utc>) -> Vec<ClientOrderId> {
        self.expiries
            .iter()
            .take_while(|(expiry, _)| *expiry <= now)
            .map(|(_, cid)| cid.clone())
            .collect()
    }

    /// Takes `order` out of every index that ranks it, leaving [`by_id`](Self::by_id) to its caller.
    fn unindex(&mut self, order: &OpenOrder) {
        self.dequeue(order);

        if let Some(expiry) = deadline_of(order) {
            self.expiries.remove(&(expiry, order.key.cid.clone()));
        }
    }

    pub fn get(&self, cid: &ClientOrderId) -> Option<&OpenOrder> {
        self.by_id.get(cid).map(|resting| &resting.order)
    }

    pub fn len(&self) -> usize {
        self.by_id.len()
    }

    pub fn is_empty(&self) -> bool {
        self.by_id.is_empty()
    }

    /// Every open order, in no particular order.
    ///
    /// Callers that need a reproducible sequence must impose one — [`resting`](Self::resting) for
    /// matching, or an explicit sort for a snapshot.
    pub fn iter(&self) -> impl Iterator<Item = &OpenOrder> + '_ {
        self.by_id.values().map(|resting| &resting.order)
    }

    /// One instrument's resting orders on one side, **best price first** and earliest first within
    /// a price.
    ///
    /// This is the order a venue matches in, so a caller filling against a crossing tick takes them
    /// as they come and stops at the first that does not cross.
    pub fn resting(
        &self,
        instrument: &InstrumentNameExchange,
        side: Side,
    ) -> impl Iterator<Item = &OpenOrder> + '_ {
        self.queues
            .get(&(instrument.clone(), side))
            .into_iter()
            .flat_map(|queue| queue.values())
            // `by_id` and `queues` are written only together, so a queued id is always present.
            .filter_map(|cid| self.by_id.get(cid))
            .map(|resting| &resting.order)
    }

    fn dequeue(&mut self, order: &OpenOrder) {
        let Some(price) = order.price else {
            // Never queued — see the type's note on orders with no price.
            return;
        };

        let key = (order.key.instrument.clone(), order.side);
        let Some(queue) = self.queues.get_mut(&key) else {
            return;
        };

        // The position's `seq` is not known from the order, so this finds it by value. Queues are
        // per instrument and side, and an order appears in exactly one, so this is a scan of one
        // instrument's book rather than of every open order.
        let position = queue
            .range(QueuePosition::new(order.side, price, order.state.time_exchange, 0)..)
            .find(|(_, queued)| *queued == &order.key.cid)
            .map(|(position, _)| *position);

        // A priced order is queued by `insert` and dequeued only from here, so failing to find one
        // means its `price`, `side` or `state.time_exchange` changed while it was on the book: the
        // range above no longer covers where it was filed. That silently leaves a queued id with no
        // order behind it, which `resting` then silently filters out — invisible in both
        // directions, and it would corrupt price-time priority for every later match. Asserted
        // rather than tolerated, because there is no correct way to carry on from it.
        debug_assert!(
            position.is_some(),
            "OpenOrders lost the queue position of {} ({} {price} at {}): its ranking fields were \
             mutated while it was on the book",
            order.key.cid,
            order.side,
            order.state.time_exchange
        );

        if let Some(position) = position {
            queue.remove(&position);
        }

        if queue.is_empty() {
            self.queues.remove(&key);
        }
    }
}

/// Narrows an order's state to `Open`, or `None` if it is in any other state.
pub(super) fn as_open(order: UnindexedOrder) -> Option<OpenOrder> {
    match order.state {
        crate::order::state::OrderState::Active(ActiveOrderState::Open(open)) => Some(Order {
            key: order.key,
            side: order.side,
            price: order.price,
            quantity: order.quantity,
            kind: order.kind,
            time_in_force: order.time_in_force,
            state: open,
        }),
        _ => None,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)] // Test code: panics acceptable
mod tests {
    use super::*;
    use crate::order::{
        OrderKey, OrderKind, TimeInForce,
        id::{OrderId, StrategyId, VenueOrderId},
    };
    use rust_decimal_macros::dec;

    fn at(millis: i64) -> DateTime<Utc> {
        DateTime::from_timestamp_millis(millis).unwrap()
    }

    fn instrument() -> InstrumentNameExchange {
        InstrumentNameExchange::new("btc_usdt")
    }

    fn gtd(cid: &str, expiry_millis: i64) -> OpenOrder {
        let mut order = order(cid, Side::Buy, Some(dec!(100)), 1_000);
        order.time_in_force = TimeInForce::GoodTillDate {
            expiry: at(expiry_millis),
        };
        order
    }

    fn order(cid: &str, side: Side, price: Option<Decimal>, millis: i64) -> OpenOrder {
        Order {
            key: OrderKey {
                exchange: ExchangeId::BinanceSpot,
                instrument: instrument(),
                strategy: StrategyId::new("test"),
                cid: ClientOrderId::new(cid),
            },
            side,
            price,
            quantity: dec!(1),
            kind: OrderKind::Limit,
            time_in_force: TimeInForce::GoodUntilCancelled { post_only: false },
            state: Open {
                id: VenueOrderId::Assigned(OrderId::new(cid)),
                time_exchange: at(millis),
                filled_quantity: Decimal::ZERO,
            },
        }
    }

    /// A book holding `orders`, none of them with a reservation.
    fn book(orders: impl IntoIterator<Item = OpenOrder>) -> OpenOrders {
        let mut open = OpenOrders::default();
        for order in orders {
            open.insert(order, None).unwrap();
        }
        open
    }

    fn cids(orders: impl Iterator<Item = &'static OpenOrder>) -> Vec<String> {
        orders.map(|order| order.key.cid.0.to_string()).collect()
    }

    fn resting_cids(open: &OpenOrders, side: Side) -> Vec<String> {
        open.resting(&instrument(), side)
            .map(|order| order.key.cid.0.to_string())
            .collect()
    }

    /// The best bid is the highest one, and the earliest of those at the same price.
    #[test]
    fn buys_rest_highest_price_then_earliest_first() {
        let open = book([
            order("low", Side::Buy, Some(dec!(99)), 1_000),
            order("high_late", Side::Buy, Some(dec!(101)), 3_000),
            order("high_early", Side::Buy, Some(dec!(101)), 2_000),
            order("mid", Side::Buy, Some(dec!(100)), 500),
        ]);

        assert_eq!(
            resting_cids(&open, Side::Buy),
            ["high_early", "high_late", "mid", "low"],
            "price first, then arrival — not insertion order, and not the map's order"
        );
    }

    /// The best offer is the lowest one. Same rule, opposite price direction.
    #[test]
    fn sells_rest_lowest_price_then_earliest_first() {
        let open = book([
            order("high", Side::Sell, Some(dec!(101)), 1_000),
            order("low_late", Side::Sell, Some(dec!(99)), 3_000),
            order("low_early", Side::Sell, Some(dec!(99)), 2_000),
        ]);

        assert_eq!(
            resting_cids(&open, Side::Sell),
            ["low_early", "low_late", "high"]
        );
    }

    /// Two orders at one price and one instant must both survive.
    ///
    /// Without the insertion sequence in the key they compare equal, and a `BTreeMap` keeps one —
    /// an order silently vanishing from the book rather than merely being mis-ranked.
    #[test]
    fn orders_at_the_same_price_and_instant_both_rest() {
        let open = book([
            order("first", Side::Buy, Some(dec!(100)), 1_000),
            order("second", Side::Buy, Some(dec!(100)), 1_000),
        ]);

        assert_eq!(open.len(), 2);
        assert_eq!(
            resting_cids(&open, Side::Buy),
            ["first", "second"],
            "the tie is broken by insertion, which is deterministic"
        );
    }

    #[test]
    fn the_two_sides_are_separate_queues() {
        let open = book([
            order("bid", Side::Buy, Some(dec!(99)), 1_000),
            order("ask", Side::Sell, Some(dec!(101)), 1_000),
        ]);

        assert_eq!(resting_cids(&open, Side::Buy), ["bid"]);
        assert_eq!(resting_cids(&open, Side::Sell), ["ask"]);
    }

    #[test]
    fn a_removed_order_leaves_the_queue() {
        let mut open = book([
            order("a", Side::Buy, Some(dec!(100)), 1_000),
            order("b", Side::Buy, Some(dec!(99)), 1_000),
        ]);

        let removed = open.remove(&ClientOrderId::new("a")).expect("a is open");
        assert_eq!(removed.order.key.cid, ClientOrderId::new("a"));

        assert_eq!(resting_cids(&open, Side::Buy), ["b"]);
        assert_eq!(open.len(), 1);
        assert!(open.get(&ClientOrderId::new("a")).is_none());
        assert!(open.remove(&ClientOrderId::new("a")).is_none());
    }

    /// Removing every order must leave nothing behind for the next one to trip over.
    #[test]
    fn emptying_the_book_empties_the_queues() {
        let mut open = book([
            order("a", Side::Buy, Some(dec!(100)), 1_000),
            order("b", Side::Buy, Some(dec!(99)), 2_000),
        ]);

        open.remove(&ClientOrderId::new("a"));
        open.remove(&ClientOrderId::new("b"));

        assert!(open.is_empty());
        assert!(resting_cids(&open, Side::Buy).is_empty());

        open.insert(order("c", Side::Buy, Some(dec!(98)), 3_000), None)
            .unwrap();
        assert_eq!(resting_cids(&open, Side::Buy), ["c"]);
    }

    /// A second order under a resting id is refused, handed back whole with its reservation, and
    /// the order already resting keeps its place.
    #[test]
    fn a_second_order_under_a_resting_id_is_refused_and_the_first_keeps_its_place() {
        let mut open = book([
            order("taken", Side::Buy, Some(dec!(100)), 1_000),
            order("other", Side::Buy, Some(dec!(99)), 1_000),
        ]);
        let held = Reservation {
            asset: AssetNameExchange::new("usdt"),
            amount: dec!(98),
        };

        let Err(AlreadyResting(refused)) = open.insert(
            order("taken", Side::Buy, Some(dec!(98)), 2_000),
            Some(held.clone()),
        ) else {
            panic!("an id already resting is refused");
        };

        assert_eq!(
            refused.order.price,
            Some(dec!(98)),
            "the refused order comes back"
        );
        assert_eq!(
            refused.reservation,
            Some(held),
            "and so does what the caller may already hold against it"
        );
        assert_eq!(open.len(), 2, "nothing was added");
        assert_eq!(
            open.get(&ClientOrderId::new("taken"))
                .map(|order| order.price),
            Some(Some(dec!(100))),
            "the resting order is the one still held"
        );
        assert_eq!(
            resting_cids(&open, Side::Buy),
            ["taken", "other"],
            "at its own price and place, appearing exactly once"
        );
    }

    /// An order with no limit price is held but cannot be matched.
    #[test]
    fn an_order_without_a_price_is_held_but_never_rests() {
        let open = book([
            order("priceless", Side::Buy, None, 1_000),
            order("priced", Side::Buy, Some(dec!(100)), 1_000),
        ]);

        assert_eq!(open.len(), 2, "a snapshot must still report it");
        assert!(open.get(&ClientOrderId::new("priceless")).is_some());
        assert_eq!(
            resting_cids(&open, Side::Buy),
            ["priced"],
            "an order with no price to wait at has nothing to wait for"
        );
    }

    /// Only orders that carry a deadline are indexed for the sweep, and only once reached.
    #[test]
    fn only_orders_past_their_deadline_are_swept() {
        let open = book([
            order("no_deadline", Side::Buy, Some(dec!(100)), 1_000),
            gtd("due", 5_000),
            gtd("later", 9_000),
        ]);

        assert!(
            open.expired_as_of(at(4_999)).is_empty(),
            "nothing is due before the earliest deadline"
        );
        assert_eq!(
            open.expired_as_of(at(5_000)),
            [ClientOrderId::new("due")],
            "a deadline is reached at its instant, not after it"
        );
        assert_eq!(
            open.expired_as_of(at(9_999)),
            [ClientOrderId::new("due"), ClientOrderId::new("later")],
            "earliest deadline first, and an order without one is never swept"
        );
    }

    /// Two orders sharing one deadline are both swept, in a reproducible order.
    ///
    /// Without the id in the key they compare equal and a `BTreeSet` keeps one — an order silently
    /// outliving its own deadline.
    #[test]
    fn orders_sharing_a_deadline_are_both_swept_in_id_order() {
        let open = book([gtd("b", 5_000), gtd("a", 5_000)]);

        assert_eq!(
            open.expired_as_of(at(5_000)),
            [ClientOrderId::new("a"), ClientOrderId::new("b")],
            "the tie is broken by id, which does not depend on map iteration"
        );
    }

    /// An order that leaves the book takes its deadline with it, whichever way it left.
    #[test]
    fn a_removed_order_is_no_longer_swept() {
        let mut open = book([gtd("gone", 5_000), gtd("stays", 5_000)]);

        open.remove(&ClientOrderId::new("gone"));

        assert_eq!(
            open.expired_as_of(at(5_000)),
            [ClientOrderId::new("stays")],
            "a deadline that outlived its order would retire an order twice"
        );
    }

    /// A refused order brings no deadline of its own, and the resting order keeps its own.
    #[test]
    fn a_refused_order_leaves_the_resting_ones_deadline() {
        let mut open = book([gtd("taken", 5_000)]);

        assert!(open.insert(gtd("taken", 9_000), None).is_err());

        assert_eq!(
            open.expired_as_of(at(9_000)),
            [ClientOrderId::new("taken")],
            "the resting order's deadline, once: the refused one's never reached the index"
        );
        assert_eq!(open.expired_as_of(at(5_000)), [ClientOrderId::new("taken")]);
    }

    /// An id is free again once its order has left the book, and the next order under it brings
    /// its own deadline.
    #[test]
    fn an_id_is_free_again_once_its_order_leaves_the_book() {
        let mut open = book([gtd("reused", 5_000)]);
        open.remove(&ClientOrderId::new("reused")).unwrap();

        open.insert(order("reused", Side::Buy, Some(dec!(100)), 6_000), None)
            .unwrap();

        assert!(
            open.expired_as_of(at(9_000)).is_empty(),
            "the first order's deadline left with it"
        );
        assert_eq!(resting_cids(&open, Side::Buy), ["reused"]);
    }

    #[test]
    fn iter_reports_every_open_order() {
        let open = book([
            order("a", Side::Buy, Some(dec!(100)), 1_000),
            order("b", Side::Sell, Some(dec!(101)), 1_000),
            order("c", Side::Buy, None, 1_000),
        ]);

        let mut all = cids(Vec::leak(open.iter().cloned().collect::<Vec<_>>()).iter());
        all.sort();
        assert_eq!(all, ["a", "b", "c"]);
    }
}
