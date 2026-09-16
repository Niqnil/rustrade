//! Resting orders, held in the order a venue would match them.

use crate::order::{
    Order, UnindexedOrder,
    id::ClientOrderId,
    state::{ActiveOrderState, Open},
};
use chrono::{DateTime, Utc};
use fnv::FnvHashMap;
use rust_decimal::Decimal;
use rustrade_instrument::{Side, exchange::ExchangeId, instrument::name::InstrumentNameExchange};
use std::collections::BTreeMap;

/// An open order held by a simulated venue.
pub type OpenOrder = Order<ExchangeId, InstrumentNameExchange, Open>;

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
#[derive(Debug, Default)]
pub struct OpenOrders {
    by_id: FnvHashMap<ClientOrderId, OpenOrder>,
    /// Per instrument and side, best first. Values are keys into [`by_id`](Self::by_id).
    queues: FnvHashMap<(InstrumentNameExchange, Side), BTreeMap<QueuePosition, ClientOrderId>>,
    /// Monotone across every instrument, so insertion order is total even across queues.
    seq: u64,
}

impl OpenOrders {
    /// Inserts `order`, replacing any order already held under its [`ClientOrderId`].
    ///
    /// A replacement keeps the new order's price and arrival, so amending an order loses its queue
    /// position — which is what a venue does when the amendment changes price.
    pub fn insert(&mut self, order: OpenOrder) {
        if let Some(replaced) = self.by_id.insert(order.key.cid.clone(), order.clone()) {
            self.dequeue(&replaced);
        }

        if let Some(price) = order.price {
            self.seq += 1;
            self.queues
                .entry((order.key.instrument.clone(), order.side))
                .or_default()
                .insert(
                    QueuePosition::new(order.side, price, order.state.time_exchange, self.seq),
                    order.key.cid,
                );
        }
    }

    /// Removes and returns the order held under `cid`, if there is one.
    pub fn remove(&mut self, cid: &ClientOrderId) -> Option<OpenOrder> {
        let order = self.by_id.remove(cid)?;
        self.dequeue(&order);
        Some(order)
    }

    pub fn get(&self, cid: &ClientOrderId) -> Option<&OpenOrder> {
        self.by_id.get(cid)
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
        self.by_id.values()
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
        if let Some(position) = queue
            .range(QueuePosition::new(order.side, price, order.state.time_exchange, 0)..)
            .find(|(_, queued)| *queued == &order.key.cid)
            .map(|(position, _)| *position)
        {
            queue.remove(&position);
        }

        if queue.is_empty() {
            self.queues.remove(&key);
        }
    }
}

impl FromIterator<OpenOrder> for OpenOrders {
    fn from_iter<T: IntoIterator<Item = OpenOrder>>(orders: T) -> Self {
        let mut open = Self::default();
        for order in orders {
            open.insert(order);
        }
        open
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
        id::{OrderId, StrategyId},
    };
    use rust_decimal_macros::dec;

    fn at(millis: i64) -> DateTime<Utc> {
        DateTime::from_timestamp_millis(millis).unwrap()
    }

    fn instrument() -> InstrumentNameExchange {
        InstrumentNameExchange::new("btc_usdt")
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
                id: OrderId::new(cid),
                time_exchange: at(millis),
                filled_quantity: Decimal::ZERO,
            },
        }
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
        let open: OpenOrders = [
            order("low", Side::Buy, Some(dec!(99)), 1_000),
            order("high_late", Side::Buy, Some(dec!(101)), 3_000),
            order("high_early", Side::Buy, Some(dec!(101)), 2_000),
            order("mid", Side::Buy, Some(dec!(100)), 500),
        ]
        .into_iter()
        .collect();

        assert_eq!(
            resting_cids(&open, Side::Buy),
            ["high_early", "high_late", "mid", "low"],
            "price first, then arrival — not insertion order, and not the map's order"
        );
    }

    /// The best offer is the lowest one. Same rule, opposite price direction.
    #[test]
    fn sells_rest_lowest_price_then_earliest_first() {
        let open: OpenOrders = [
            order("high", Side::Sell, Some(dec!(101)), 1_000),
            order("low_late", Side::Sell, Some(dec!(99)), 3_000),
            order("low_early", Side::Sell, Some(dec!(99)), 2_000),
        ]
        .into_iter()
        .collect();

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
        let open: OpenOrders = [
            order("first", Side::Buy, Some(dec!(100)), 1_000),
            order("second", Side::Buy, Some(dec!(100)), 1_000),
        ]
        .into_iter()
        .collect();

        assert_eq!(open.len(), 2);
        assert_eq!(
            resting_cids(&open, Side::Buy),
            ["first", "second"],
            "the tie is broken by insertion, which is deterministic"
        );
    }

    #[test]
    fn the_two_sides_are_separate_queues() {
        let open: OpenOrders = [
            order("bid", Side::Buy, Some(dec!(99)), 1_000),
            order("ask", Side::Sell, Some(dec!(101)), 1_000),
        ]
        .into_iter()
        .collect();

        assert_eq!(resting_cids(&open, Side::Buy), ["bid"]);
        assert_eq!(resting_cids(&open, Side::Sell), ["ask"]);
    }

    #[test]
    fn a_removed_order_leaves_the_queue() {
        let mut open: OpenOrders = [
            order("a", Side::Buy, Some(dec!(100)), 1_000),
            order("b", Side::Buy, Some(dec!(99)), 1_000),
        ]
        .into_iter()
        .collect();

        let removed = open.remove(&ClientOrderId::new("a")).expect("a is open");
        assert_eq!(removed.key.cid, ClientOrderId::new("a"));

        assert_eq!(resting_cids(&open, Side::Buy), ["b"]);
        assert_eq!(open.len(), 1);
        assert!(open.get(&ClientOrderId::new("a")).is_none());
        assert!(open.remove(&ClientOrderId::new("a")).is_none());
    }

    /// Removing every order must leave nothing behind for the next one to trip over.
    #[test]
    fn emptying_the_book_empties_the_queues() {
        let mut open: OpenOrders = [
            order("a", Side::Buy, Some(dec!(100)), 1_000),
            order("b", Side::Buy, Some(dec!(99)), 2_000),
        ]
        .into_iter()
        .collect();

        open.remove(&ClientOrderId::new("a"));
        open.remove(&ClientOrderId::new("b"));

        assert!(open.is_empty());
        assert!(resting_cids(&open, Side::Buy).is_empty());

        open.insert(order("c", Side::Buy, Some(dec!(98)), 3_000));
        assert_eq!(resting_cids(&open, Side::Buy), ["c"]);
    }

    /// Re-inserting under one id replaces the order and its queue position, rather than leaving the
    /// old position pointing at the new order.
    #[test]
    fn reinserting_one_id_replaces_its_queue_position() {
        let mut open: OpenOrders = [
            order("amended", Side::Buy, Some(dec!(100)), 1_000),
            order("other", Side::Buy, Some(dec!(99)), 1_000),
        ]
        .into_iter()
        .collect();

        // Amended down to behind `other`.
        open.insert(order("amended", Side::Buy, Some(dec!(98)), 2_000));

        assert_eq!(open.len(), 2, "a replacement is not a second order");
        assert_eq!(
            resting_cids(&open, Side::Buy),
            ["other", "amended"],
            "the amended order takes its new price's place, and appears exactly once"
        );
    }

    /// An order with no limit price is held but cannot be matched.
    #[test]
    fn an_order_without_a_price_is_held_but_never_rests() {
        let open: OpenOrders = [
            order("priceless", Side::Buy, None, 1_000),
            order("priced", Side::Buy, Some(dec!(100)), 1_000),
        ]
        .into_iter()
        .collect();

        assert_eq!(open.len(), 2, "a snapshot must still report it");
        assert!(open.get(&ClientOrderId::new("priceless")).is_some());
        assert_eq!(
            resting_cids(&open, Side::Buy),
            ["priced"],
            "an order with no price to wait at has nothing to wait for"
        );
    }

    #[test]
    fn iter_reports_every_open_order() {
        let open: OpenOrders = [
            order("a", Side::Buy, Some(dec!(100)), 1_000),
            order("b", Side::Sell, Some(dec!(101)), 1_000),
            order("c", Side::Buy, None, 1_000),
        ]
        .into_iter()
        .collect();

        let mut all = cids(Vec::leak(open.iter().cloned().collect::<Vec<_>>()).iter());
        all.sort();
        assert_eq!(all, ["a", "b", "c"]);
    }
}
