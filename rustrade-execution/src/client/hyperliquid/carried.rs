//! The fills a Hyperliquid modify carries from the order it replaces into the one replacing it.
//!
//! A modify gives an order a new `oid` and restarts its size and fill: every record of the new
//! `oid`, on the account stream or from a lookup, counts only what filled since the modify. A
//! client reports an order's quantities as totals, so it adds back what filled before, which it
//! learns when it reads the modify on the account stream (see
//! [`order_updates_to_account_events`](super::common::order_updates_to_account_events)).

use crate::order::{
    Order,
    report::UnindexedOrderReport,
    state::{ActiveOrderState, Cancelled, Filled, InactiveOrderState, Open, OrderState},
};
use lru::LruCache;
use parking_lot::Mutex;
use rust_decimal::Decimal;
use std::{num::NonZeroUsize, sync::Arc};
use tracing::warn;

/// How many replacing orders [`CarriedFills`] remembers before it forgets the least recently used.
///
/// The bound is on modifies, not on open orders: each modify takes an entry, and only a report of
/// the order refreshes it. An order modified once and not read since is forgotten after this many
/// later modifies, live or not, which is logged. Each entry is a list node holding an `oid`, a
/// decimal and its links, indexed by a hash table, so a full cache takes a few MiB; it is
/// allocated at the first modify a client sees, so a client that sees none takes nothing.
const CARRIED_ORDERS: NonZeroUsize = match NonZeroUsize::new(65_536) {
    Some(capacity) => capacity,
    None => panic!("a non-zero literal"),
};

/// What filled of each order before the modifies that replaced it, by the `oid` that replaced
/// it, shared by a client's account stream and its lookups.
///
/// Learned only from the account stream, and held in memory: an order modified while no stream
/// of this client was reading, or before the process started, has its earlier fill left out of
/// every report of it. An entry outlives the order's end, so a late lookup of it still adds the
/// fill back, until it is the least recently used of more than [`CARRIED_ORDERS`] (logged at
/// `warn`, as the order's reports then leave its earlier fill out).
#[derive(Debug, Clone)]
pub(super) struct CarriedFills(Arc<Mutex<Carried>>);

/// The cache behind [`CarriedFills`], made at the first entry: [`LruCache::new`] allocates its
/// table for the whole bound at once.
#[derive(Debug)]
struct Carried {
    capacity: NonZeroUsize,
    cache: Option<LruCache<u64, Decimal>>,
}

impl Default for CarriedFills {
    fn default() -> Self {
        Self::with_capacity(CARRIED_ORDERS)
    }
}

impl CarriedFills {
    fn with_capacity(capacity: NonZeroUsize) -> Self {
        Self(Arc::new(Mutex::new(Carried {
            capacity,
            cache: None,
        })))
    }

    /// Record `filled` under `oid`, and give the entry of another order forgotten to make room.
    fn put(&self, oid: u64, filled: Decimal) -> Option<(u64, Decimal)> {
        let mut carried = self.0.lock();
        let capacity = carried.capacity;
        carried
            .cache
            .get_or_insert_with(|| LruCache::new(capacity))
            .push(oid, filled)
            .filter(|(forgotten, _)| *forgotten != oid)
    }

    /// What filled before the modify that gave the order its `oid`: zero for an order no modify
    /// seen replaced.
    pub(super) fn get(&self, oid: u64) -> Decimal {
        self.0
            .lock()
            .cache
            .as_mut()
            .and_then(|cache| cache.get(&oid).copied())
            .unwrap_or_default()
    }

    /// Record that `filled` filled before the modify that gave the order its `oid`.
    pub(super) fn carry(&self, oid: u64, filled: Decimal) {
        if let Some((forgotten, forgotten_filled)) = self.put(oid, filled) {
            warn!(
                oid = forgotten,
                filled = %forgotten_filled,
                "Hyperliquid order a modify replaced is forgotten: its reports now leave out what \
                 filled before the modify"
            );
        }
    }
}

/// Add `carried`, filled before a modify, to `report`'s quantity and fill.
pub(super) fn carry_into_report(report: &mut UnindexedOrderReport, carried: Decimal) {
    if carried.is_zero() {
        return;
    }
    report.order.quantity += carried;
    match &mut report.order.state {
        OrderState::Active(ActiveOrderState::Open(open)) => open.filled_quantity += carried,
        OrderState::Inactive(InactiveOrderState::Cancelled(Cancelled {
            filled_quantity: Some(filled),
            ..
        }))
        | OrderState::Inactive(InactiveOrderState::FullyFilled(Filled {
            filled_quantity: filled,
            ..
        })) => *filled += carried,
        // A fill unknown stays unknown, and the rest carry none.
        _ => {}
    }
}

/// Add `carried`, filled before a modify, to an open `order`'s quantity and fill.
pub(super) fn carry_into_open<ExchangeKey, InstrumentKey>(
    order: &mut Order<ExchangeKey, InstrumentKey, Open>,
    carried: Decimal,
) {
    order.quantity += carried;
    order.state.filled_quantity += carried;
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use crate::{
        error::OrderError,
        order::{
            OrderKey, OrderKind, TimeInForce,
            id::{ClientOrderId, OrderId, StrategyId, VenueOrderId},
            report::{OrderReport, ReportedTerms},
        },
    };
    use chrono::DateTime;
    use rust_decimal_macros::dec;
    use rustrade_instrument::{
        Side, exchange::ExchangeId, instrument::name::InstrumentNameExchange,
    };

    fn order<State>(state: State) -> Order<ExchangeId, InstrumentNameExchange, State> {
        Order {
            key: OrderKey {
                exchange: ExchangeId::HyperliquidPerp,
                instrument: InstrumentNameExchange::from("ETH-USDC-PERP"),
                strategy: StrategyId::unknown(),
                cid: ClientOrderId::new("c"),
            },
            side: Side::Buy,
            price: Some(dec!(1600)),
            quantity: dec!(2),
            kind: OrderKind::Limit,
            time_in_force: TimeInForce::Unreported,
            state,
        }
    }

    fn report(state: crate::order::state::UnindexedOrderState) -> UnindexedOrderReport {
        OrderReport::new(order(state), ReportedTerms::NONE.with_quantity(true))
    }

    fn open(filled: Decimal) -> Open {
        Open {
            id: VenueOrderId::Assigned(OrderId::new("1")),
            time_exchange: DateTime::UNIX_EPOCH,
            filled_quantity: filled,
        }
    }

    #[test]
    fn a_carried_fill_is_added_to_the_quantity_and_any_known_fill() {
        let cases = [
            (OrderState::active(open(dec!(0.5))), Some(dec!(1.5))),
            (
                OrderState::inactive(Cancelled::new(
                    OrderId::new("1"),
                    DateTime::UNIX_EPOCH,
                    Some(dec!(0.5)),
                )),
                Some(dec!(1.5)),
            ),
            (
                OrderState::inactive(Cancelled::new(
                    OrderId::new("1"),
                    DateTime::UNIX_EPOCH,
                    None,
                )),
                None,
            ),
            (
                OrderState::fully_filled(Filled::new(
                    OrderId::new("1"),
                    DateTime::UNIX_EPOCH,
                    dec!(2),
                    None,
                )),
                Some(dec!(3)),
            ),
        ];
        for (state, expected) in cases {
            let mut report = report(state);
            carry_into_report(&mut report, dec!(1));
            assert_eq!(report.order.quantity, dec!(3));
            let filled = match &report.order.state {
                OrderState::Active(ActiveOrderState::Open(open)) => Some(open.filled_quantity),
                OrderState::Inactive(InactiveOrderState::Cancelled(cancelled)) => {
                    cancelled.filled_quantity
                }
                OrderState::Inactive(InactiveOrderState::FullyFilled(filled)) => {
                    Some(filled.filled_quantity)
                }
                other => panic!("unexpected state {other:?}"),
            };
            assert_eq!(filled, expected);
        }
    }

    #[test]
    fn a_rejection_carries_only_its_quantity_and_zero_changes_nothing() {
        let rejected = OrderState::inactive(OrderError::Rejected(
            crate::error::ApiError::OrderRejected("no".into()),
        ));
        let mut changed = report(rejected.clone());
        carry_into_report(&mut changed, dec!(1));
        assert_eq!(changed.order.quantity, dec!(3));
        assert_eq!(changed.order.state, rejected);

        let mut unchanged = report(OrderState::active(open(dec!(0.5))));
        let before = unchanged.clone();
        carry_into_report(&mut unchanged, Decimal::ZERO);
        assert_eq!(unchanged, before);
    }

    #[test]
    fn an_open_order_carries_into_its_quantity_and_fill() {
        let mut order = order(open(dec!(0.5)));
        carry_into_open(&mut order, dec!(1));
        assert_eq!(order.quantity, dec!(3));
        assert_eq!(order.state.filled_quantity, dec!(1.5));
    }

    #[test]
    fn the_least_recently_read_order_is_forgotten_past_the_bound() {
        let carried = CarriedFills::with_capacity(NonZeroUsize::new(2).unwrap());
        assert_eq!(carried.put(1, dec!(0.1)), None);
        assert_eq!(carried.put(2, dec!(0.2)), None);
        // A report of 1 refreshes it, so 2 is the one forgotten.
        assert_eq!(carried.get(1), dec!(0.1));
        assert_eq!(carried.put(3, dec!(0.3)), Some((2, dec!(0.2))));

        assert_eq!(carried.get(1), dec!(0.1));
        assert_eq!(carried.get(2), Decimal::ZERO);
        assert_eq!(carried.get(3), dec!(0.3));
        // Recording an order again replaces its entry and forgets none.
        assert_eq!(carried.put(3, dec!(0.4)), None);
        carried.carry(1, dec!(0.5));
        assert_eq!(carried.get(1), dec!(0.5));
        assert_eq!(carried.get(3), dec!(0.4));
    }

    #[test]
    fn carried_fills_are_shared_by_clones_and_zero_when_unknown() {
        let carried = CarriedFills::default();
        assert_eq!(carried.get(7), Decimal::ZERO);
        assert!(
            carried.0.lock().cache.is_none(),
            "nothing is allocated until a modify"
        );
        carried.clone().carry(7, dec!(0.25));
        assert_eq!(carried.get(7), dec!(0.25));
    }
}
