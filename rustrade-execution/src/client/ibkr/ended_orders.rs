//! How this client's orders ended, read back from IB's listing of the account's completed orders:
//! for [`OrderStatusClient`](crate::client::OrderStatusClient), and for the account stream's check
//! of the orders that ended while it was disconnected.
//!
//! IB lists a completed order without its API order id, so an order is found by its order
//! reference, which carries the client order id it was placed with ([`order::order_ref`]). Its
//! API client id is not relied on either: after a Gateway restart IB lists the orders completed
//! before it under client id 0.

use super::{
    LISTING_STALL_TIMEOUT, ListingLock, cancelled_at_expiry,
    execution::{self, ExecutionRevision, try_decimal_or_warn},
    order::{self, OrderContext, OrderIdMap, PendingCancels, Registration},
    read_listing,
};
use crate::{
    client::order_recovery::OrderLookup,
    error::{ApiError, OrderError, UnindexedClientError},
    order::{
        Order, UnindexedOrderKey,
        id::{ClientOrderId, OrderId},
        state::{Cancelled, Expired, Filled, InactiveOrderState},
    },
};
use chrono::{DateTime, Utc};
use fnv::FnvHashMap;
use ibapi::{
    client::blocking::Client,
    orders::{ExecutionFilter, Executions, OrderData, OrderStatusKind, Orders},
};
use lru::LruCache;
use parking_lot::Mutex;
use rust_decimal::Decimal;
use rustrade_instrument::ibkr::ContractRegistry;
use smol_str::{SmolStr, format_smolstr};
use std::{cell::OnceCell, num::NonZeroUsize, ops::ControlFlow, sync::Arc, time::Duration};
use tracing::{debug, warn};

/// How many days of executions are read for the fill of an order that ended cancelled or
/// expired.
const EXECUTION_DAYS: i32 = 7;

/// How long ago an order may have been placed for a read of [`EXECUTION_DAYS`] days of executions
/// to cover its whole life, so that finding none of them means it filled nothing. A day short of
/// the read, since IB counts its days in a time zone of its own.
const EXECUTIONS_COVER: Duration = Duration::from_secs(6 * 24 * 60 * 60);

/// How far IB's clock, which stamps an order's completion to the second, and this host's may be
/// apart, when a completion is compared with when this client began tracking the order.
const COMPLETION_CLOCK_SLACK: chrono::Duration = chrono::Duration::seconds(5);

/// How many earlier orders' completions [`EarlierCompletions`] remembers warning about.
const EARLIER_COMPLETIONS_REMEMBERED: usize = 1_024;

/// The completions [`ended_order`] took as an earlier order's, under a client order id since
/// reused, so that it warns about each only once.
///
/// IB keeps a completed order in its listing, so each lookup of a reused client order id meets
/// the earlier order's completion again, and reusing an id after its order ended is legitimate.
/// Each later meeting is logged at `debug!`. Bounded and LRU: a completion forgotten is warned
/// about again.
///
/// Shared by every clone of the client, like the order id map.
#[derive(Debug, Clone)]
pub(super) struct EarlierCompletions(Arc<Mutex<LruCache<(ClientOrderId, DateTime<Utc>), ()>>>);

impl EarlierCompletions {
    pub(super) fn new() -> Self {
        Self::with_capacity(EARLIER_COMPLETIONS_REMEMBERED)
    }

    fn with_capacity(capacity: usize) -> Self {
        let capacity = NonZeroUsize::new(capacity).unwrap_or(NonZeroUsize::MIN);
        Self(Arc::new(Mutex::new(LruCache::new(capacity))))
    }

    /// Note the completion at `completed_at` under `cid`, answering whether it is new.
    fn first_meeting(&self, cid: &ClientOrderId, completed_at: DateTime<Utc>) -> bool {
        self.0.lock().put((cid.clone(), completed_at), ()).is_none()
    }
}

/// How IB's completion text starts for an order it rejected after accepting it, such as a
/// good-till-date order whose expiry has passed, which it lists as `Cancelled`.
const REJECTED_PREFIX: &str = "Rejected";

/// One of the account's completed orders.
#[derive(Debug, Clone)]
pub(super) struct CompletedOrder {
    data: OrderData,
    /// When IB says it completed, or `None` when that does not parse.
    time: Option<DateTime<Utc>>,
}

/// The account's completed orders that may be this API client's, by the client order id their
/// order reference carries.
///
/// An order of another API client is left out, but one listed under client id 0 is kept, since IB
/// lists every order completed before a Gateway restart under it. An order without a reference,
/// such as one entered in TWS, cannot be named by a client order id and is left out. When several
/// orders carry the same reference, the one that completed last is kept.
#[derive(Debug, Default)]
pub(super) struct CompletedOrders(FnvHashMap<SmolStr, CompletedOrder>);

impl CompletedOrders {
    #[cfg(test)]
    fn from_listing(listing: impl IntoIterator<Item = Orders>, api_client_id: i32) -> Self {
        let mut orders = Self::default();
        for item in listing {
            orders.push(item, api_client_id);
        }
        orders
    }

    /// Take in one item of the listing, as IB sends it.
    pub(super) fn push(&mut self, item: Orders, api_client_id: i32) {
        let Orders::OrderData(data) = item else {
            return;
        };
        let client_id = data.order.client_id;
        if (client_id != api_client_id && client_id != 0) || data.order.order_ref.is_empty() {
            return;
        }
        let order = CompletedOrder {
            time: execution::parse_ib_timestamp(&data.order_state.completed_time),
            data,
        };
        match self.0.get(order.data.order.order_ref.as_str()) {
            // Listed in completion order, so a later one with no time is taken as later.
            Some(kept) if kept.time > order.time && order.time.is_some() => {}
            _ => {
                self.0
                    .insert(SmolStr::new(&order.data.order.order_ref), order);
            }
        }
    }

    fn get(&self, cid: &ClientOrderId) -> Option<&CompletedOrder> {
        self.0.get(cid.0.as_str())
    }
}

/// How much of each order filled, by the client order id its order reference carries, from the
/// account's executions.
///
/// Each execution counts at its latest revision, so a corrected one is counted once, as corrected.
/// Like [`CompletedOrders`], only this API client's executions and those listed under client id
/// 0 count.
#[derive(Debug, Default)]
pub(super) struct ExecutedQuantities(FnvHashMap<SmolStr, FnvHashMap<SmolStr, (u32, Decimal)>>);

impl ExecutedQuantities {
    #[cfg(test)]
    fn from_executions(
        executions: impl IntoIterator<Item = Executions>,
        api_client_id: i32,
    ) -> Self {
        let mut executed = Self::default();
        for item in executions {
            executed.push(item, api_client_id);
        }
        executed
    }

    /// Take in one item of the executions read, as IB sends it.
    pub(super) fn push(&mut self, item: Executions, api_client_id: i32) {
        let Executions::ExecutionData(data) = item else {
            return;
        };
        let execution = &data.execution;
        if (execution.client_id != api_client_id && execution.client_id != 0)
            || execution.order_reference.is_empty()
        {
            return;
        }
        let Some(shares) = try_decimal_or_warn(
            execution.shares,
            format_args!("shares of execution {}", execution.execution_id),
        ) else {
            return;
        };
        let (id, revision) = ExecutionRevision::parse(&execution.execution_id)
            .map_or((execution.execution_id.as_str(), 0), |revision| {
                (revision.execution, revision.revision)
            });
        let revisions = match self.0.get_mut(execution.order_reference.as_str()) {
            Some(revisions) => revisions,
            None => self
                .0
                .entry(SmolStr::new(&execution.order_reference))
                .or_default(),
        };
        match revisions.get(id) {
            Some((kept, _)) if *kept >= revision => {}
            _ => {
                revisions.insert(SmolStr::new(id), (revision, shares));
            }
        }
    }

    /// How much of the order under `cid` the executions show filled, or `None` when they show
    /// none of it.
    fn of(&self, cid: &ClientOrderId) -> Option<Decimal> {
        self.0
            .get(cid.0.as_str())
            .map(|revisions| revisions.values().map(|(_, shares)| *shares).sum())
    }
}

/// What this client knows of an order it placed and still tracks.
#[derive(Debug, Clone)]
pub(super) struct Tracked {
    ib_id: i32,
    context: OrderContext,
    /// When and how it came to be tracked.
    registration: Registration,
    /// Whether a cancel of it was requested through this client.
    cancel_requested: bool,
}

impl Tracked {
    /// The order this client tracks under `cid`, if any.
    pub(super) fn find(
        cid: &ClientOrderId,
        order_ids: &OrderIdMap,
        pending_cancels: &PendingCancels,
    ) -> Option<Self> {
        let ib_id = order_ids.get_ib_id(cid)?;
        let (_, context) = order_ids.get_client_id_and_context(ib_id)?;
        Some(Self {
            ib_id,
            context,
            registration: order_ids.registration(cid)?,
            cancel_requested: pending_cancels.contains(ib_id),
        })
    }
}

/// How the order under `key` ended, as `completed` lists it.
///
/// `tracked` is the order as this client placed it, when it still tracks it, whose shape is then
/// reported rather than IB's. `executed` reads how much of it filled from its executions: called
/// only for an order that ended cancelled or expired, since a filled order's fill is in the
/// listing, and a rejected one filled nothing.
///
/// A filled order is [`FullyFilled`](InactiveOrderState::FullyFilled) with the quantity IB
/// lists filled, without an average price. A `Cancelled` order is
/// [`OpenFailed`](InactiveOrderState::OpenFailed) when IB's completion text says it rejected it,
/// as it does for a placement it accepted and then rejected; otherwise
/// [`Expired`](InactiveOrderState::Expired) or [`Cancelled`](InactiveOrderState::Cancelled) by the
/// rule the account stream applies (see [`cancelled_at_expiry`]), with its fill: what the
/// executions show, or nothing when they show none of it and cover the order's whole life, which
/// they do for an order this client placed under [`EXECUTIONS_COVER`] ago, not one it adopted
/// from a listing, whose placement it does not know; otherwise `None`. An `Inactive` order is
/// `OpenFailed`.
///
/// The order is not ended ([`OrderLookup::NotEnded`]) when IB lists another status, a shape this
/// client never sends for an order it does not track, or a completion from before this client
/// tracked the order under its id, give or take [`COMPLETION_CLOCK_SLACK`]: that one is an earlier
/// order's, under an id since reused. That is warned about the first time `earlier` meets it.
///
/// The ended order carries the IB order id when this client tracks the order, and otherwise IB's
/// permanent id for it, since IB lists a completed order without its API order id.
pub(super) fn ended_order<E>(
    key: &UnindexedOrderKey,
    completed: &CompletedOrder,
    tracked: Option<&Tracked>,
    earlier: &EarlierCompletions,
    executed: impl FnOnce() -> Result<Option<Decimal>, E>,
) -> Result<OrderLookup, E> {
    let data = &completed.data;
    let tracked = tracked.filter(|tracked| tracked.context.instrument == key.instrument);
    // A completion from before this client tracked the order under its id is an earlier
    // order's, whose id has since been reused: the order asked about has not ended as it says.
    if let (Some(tracked), Some(completed_at)) = (tracked, completed.time)
        && let Ok(age) = chrono::Duration::from_std(tracked.registration.age)
        && completed_at + COMPLETION_CLOCK_SLACK < Utc::now() - age
    {
        // A warning, since IB's clock lagging this host's by more than the slack would hold a
        // genuinely ended order here at every check. Once per completion, since a client order
        // id reused after its order ended, the usual cause, meets it at every lookup.
        if earlier.first_meeting(&key.cid, completed_at) {
            warn!(
                cid = %key.cid,
                %completed_at,
                tracked_for_secs = age.num_seconds(),
                "Completed IBKR order predates the order now under its client order id; taking \
                 it as an earlier order's, as when a client order id is reused after its order \
                 ended. If it is this order's, IB's clock lags this host's. Not repeated for this \
                 completion"
            );
        } else {
            debug!(
                cid = %key.cid,
                %completed_at,
                "Completed IBKR order predates the order now under its client order id; taking \
                 it as an earlier order's"
            );
        }
        return Ok(OrderLookup::NotEnded);
    }
    let context = match tracked {
        Some(tracked) => tracked.context.clone(),
        None => match order::order_shape_from_ib(&data.order) {
            Ok(shape) => {
                // IB lists a filled order with a total quantity of zero.
                let quantity = if data.order.total_quantity > 0.0 {
                    data.order.total_quantity
                } else {
                    data.order.filled_quantity
                };
                OrderContext {
                    instrument: key.instrument.clone(),
                    side: order::action_to_side(&data.order.action),
                    price: shape.price,
                    quantity: execution::parse_decimal_or_warn(quantity, "total_quantity"),
                    kind: shape.kind,
                    time_in_force: shape.time_in_force,
                }
            }
            Err(reason) => {
                warn!(
                    cid = %key.cid,
                    perm_id = data.order.perm_id,
                    %reason,
                    "Completed IBKR order cannot be read back; not reporting how it ended"
                );
                return Ok(OrderLookup::NotEnded);
            }
        },
    };
    let order_id = tracked.map_or_else(
        || OrderId::new(format_smolstr!("{}", data.order.perm_id)),
        |tracked| execution::ib_order_id(tracked.ib_id),
    );
    let time = completed.time.unwrap_or_else(Utc::now);

    let state = match &data.order_state.status {
        OrderStatusKind::Filled => {
            let filled = try_decimal_or_warn(
                data.order.filled_quantity,
                format_args!("filled quantity of completed order {}", data.order.perm_id),
            )
            .unwrap_or(context.quantity);
            InactiveOrderState::FullyFilled(Filled::new(order_id, time, filled, None))
        }
        OrderStatusKind::Cancelled
            if data
                .order_state
                .completed_status
                .starts_with(REJECTED_PREFIX) =>
        {
            InactiveOrderState::OpenFailed(OrderError::Rejected(ApiError::OrderRejected(
                data.order_state.completed_status.trim().to_owned(),
            )))
        }
        OrderStatusKind::Cancelled => {
            let filled = match executed()? {
                Some(filled) => Some(filled),
                None => tracked
                    .is_some_and(|tracked| {
                        !tracked.registration.adopted && tracked.registration.age < EXECUTIONS_COVER
                    })
                    .then_some(Decimal::ZERO),
            };
            let cancel_requested = tracked.is_some_and(|tracked| tracked.cancel_requested);
            if cancelled_at_expiry(cancel_requested, &context.time_in_force, time) {
                InactiveOrderState::Expired(Expired::new(order_id, time, filled))
            } else {
                InactiveOrderState::Cancelled(Cancelled::new(order_id, time, filled))
            }
        }
        OrderStatusKind::Inactive => {
            InactiveOrderState::OpenFailed(OrderError::Rejected(ApiError::OrderRejected(
                "IB status: Inactive (order blocked by validation/margin/exchange)".into(),
            )))
        }
        other => {
            debug!(cid = %key.cid, status = ?other, "Completed IBKR order in a status that has not ended");
            return Ok(OrderLookup::NotEnded);
        }
    };

    Ok(OrderLookup::Ended(Box::new(Order {
        key: key.clone(),
        side: context.side,
        price: context.price,
        quantity: context.quantity,
        kind: context.kind,
        time_in_force: context.time_in_force,
        state,
    })))
}

/// Looks up how this client's orders ended, reading the account's completed orders the first
/// time it needs them, and its executions the first time an order's fill needs them. Each is read
/// once, however many orders are looked up.
pub(super) struct EndedOrderReader<'a> {
    client: &'a Client,
    listings: &'a ListingLock,
    contracts: &'a ContractRegistry,
    order_ids: &'a OrderIdMap,
    pending_cancels: &'a PendingCancels,
    earlier: &'a EarlierCompletions,
    completed: OnceCell<CompletedOrders>,
    executed: OnceCell<ExecutedQuantities>,
}

impl<'a> EndedOrderReader<'a> {
    pub(super) fn new(
        client: &'a Client,
        listings: &'a ListingLock,
        contracts: &'a ContractRegistry,
        order_ids: &'a OrderIdMap,
        pending_cancels: &'a PendingCancels,
        earlier: &'a EarlierCompletions,
    ) -> Self {
        Self {
            client,
            listings,
            contracts,
            order_ids,
            pending_cancels,
            earlier,
            completed: OnceCell::new(),
            executed: OnceCell::new(),
        }
    }

    /// How the order under `key` ended, as [`ended_order`] reads it.
    ///
    /// [`OrderLookup::Unknown`] when IB lists no completed order under its client order id, or
    /// lists it in another instrument than the key's. An order still live is listed among the
    /// open orders instead, so to tell it from one IB does not know, ask only about an order the
    /// open-order listing does not show.
    ///
    /// # Errors
    /// When a listing cannot be read: see [`read_listing`].
    pub(super) fn lookup(
        &self,
        key: &UnindexedOrderKey,
    ) -> Result<OrderLookup, UnindexedClientError> {
        let completed = match self.completed.get() {
            Some(completed) => completed,
            None => {
                let api_client_id = self.client.client_id();
                let mut completed = CompletedOrders::default();
                self.listings
                    .completed_orders(self.client, |item| completed.push(item, api_client_id))?;
                self.completed.get_or_init(|| completed)
            }
        };
        let Some(order) = completed.get(&key.cid) else {
            return Ok(OrderLookup::Unknown);
        };
        if self
            .contracts
            .get_name_by_con_id(order.data.contract.contract_id)
            .as_ref()
            != Some(&key.instrument)
        {
            return Ok(OrderLookup::Unknown);
        }
        let tracked = Tracked::find(&key.cid, self.order_ids, self.pending_cancels);
        ended_order(key, order, tracked.as_ref(), self.earlier, || {
            Ok(self.executed()?.of(&key.cid))
        })
    }

    fn executed(&self) -> Result<&ExecutedQuantities, UnindexedClientError> {
        if let Some(executed) = self.executed.get() {
            return Ok(executed);
        }
        let subscription = self
            .client
            .executions(ExecutionFilter {
                last_n_days: EXECUTION_DAYS,
                ..ExecutionFilter::default()
            })
            .map_err(|e| UnindexedClientError::Internal(format!("executions: {e}")))?;
        let api_client_id = self.client.client_id();
        let mut executed = ExecutedQuantities::default();
        read_listing(
            "executions",
            LISTING_STALL_TIMEOUT,
            || self.client.is_connected(),
            |timeout| subscription.next_timeout(timeout),
            |item| {
                executed.push(item, api_client_id);
                ControlFlow::Continue(())
            },
        )?;
        Ok(self.executed.get_or_init(|| executed))
    }
}

/// Forget the order `ended` reports as ended, so its client order id may name a new order: as
/// the account stream does when it reports the order's end itself.
pub(super) fn release_ended(
    ended: &crate::order::UnindexedInactiveOrder,
    order_ids: &OrderIdMap,
    pending_cancels: &PendingCancels,
) {
    let Some(ib_id) = order_ids.get_ib_id(&ended.key.cid) else {
        return;
    };
    // Whether a cancel was requested no longer matters: the order has ended.
    let _ = pending_cancels.remove(ib_id);
    match ended.state {
        // Its executions and commissions may still arrive, and resolve by IB order id.
        InactiveOrderState::FullyFilled(_) => {
            order_ids.release_client_id(ib_id);
        }
        _ => {
            order_ids.remove_by_ib_id(ib_id);
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use crate::order::{OrderKind, TimeInForce, id::StrategyId};
    use ibapi::orders::{Execution, ExecutionData, OrderState as IbOrderState};
    use rust_decimal_macros::dec;
    use rustrade_instrument::{
        Side, exchange::ExchangeId, instrument::name::InstrumentNameExchange,
    };

    const API_CLIENT: i32 = 903;

    fn key(cid: &str) -> UnindexedOrderKey {
        UnindexedOrderKey {
            exchange: ExchangeId::Ibkr,
            instrument: InstrumentNameExchange::new("AAPL"),
            strategy: StrategyId::new("strategy"),
            cid: ClientOrderId::new(cid),
        }
    }

    fn completed(
        client_id: i32,
        order_ref: &str,
        status: OrderStatusKind,
        completed_time: &str,
        completed_status: &str,
    ) -> Orders {
        Orders::OrderData(OrderData {
            order_id: -1,
            order: ibapi::orders::Order {
                client_id,
                order_ref: order_ref.to_owned(),
                perm_id: 818545845,
                action: ibapi::orders::Action::Buy,
                order_type: "LMT".to_owned(),
                limit_price: Some(150.0),
                total_quantity: 10.0,
                filled_quantity: 4.0,
                tif: ibapi::orders::TimeInForce::GoodTillCanceled,
                ..ibapi::orders::Order::default()
            },
            order_state: IbOrderState {
                status,
                completed_time: completed_time.to_owned(),
                completed_status: completed_status.to_owned(),
                ..IbOrderState::default()
            },
            ..OrderData::default()
        })
    }

    fn one(order: Orders) -> CompletedOrder {
        let orders = CompletedOrders::from_listing([order], API_CLIENT);
        orders.0.into_values().next().unwrap()
    }

    fn tracked(age_days: u64, time_in_force: TimeInForce, cancel_requested: bool) -> Tracked {
        Tracked {
            ib_id: 12,
            context: OrderContext {
                instrument: InstrumentNameExchange::new("AAPL"),
                side: Side::Buy,
                price: Some(dec!(150)),
                quantity: dec!(10),
                kind: OrderKind::Limit,
                time_in_force,
            },
            registration: Registration {
                age: Duration::from_secs(age_days * 24 * 60 * 60),
                adopted: false,
            },
            cancel_requested,
        }
    }

    fn ended(lookup: OrderLookup) -> crate::order::UnindexedInactiveOrder {
        match lookup {
            OrderLookup::Ended(order) => *order,
            other => panic!("not ended: {other:?}"),
        }
    }

    const GTC: TimeInForce = TimeInForce::GoodUntilCancelled { post_only: false };

    /// Another API client's completed orders are left out, but those IB lists under client id
    /// 0, as it does after a Gateway restart, are kept, and so is the latest of several under one
    /// reference.
    #[test]
    fn completed_orders_are_this_clients_or_client_zeros_by_reference() {
        let orders = CompletedOrders::from_listing(
            [
                completed(
                    API_CLIENT,
                    "a",
                    OrderStatusKind::Cancelled,
                    "20261007 10:00:00 UTC",
                    "",
                ),
                completed(
                    API_CLIENT,
                    "a",
                    OrderStatusKind::Filled,
                    "20261007 11:00:00 UTC",
                    "",
                ),
                completed(
                    API_CLIENT,
                    "a",
                    OrderStatusKind::Inactive,
                    "20261007 09:00:00 UTC",
                    "",
                ),
                completed(0, "before-restart", OrderStatusKind::Filled, "", ""),
                completed(904, "other-client", OrderStatusKind::Filled, "", ""),
                completed(API_CLIENT, "", OrderStatusKind::Filled, "", ""),
            ],
            API_CLIENT,
        );

        assert_eq!(orders.0.len(), 2, "{orders:?}");
        let latest = orders.get(&ClientOrderId::new("a")).unwrap();
        assert_eq!(latest.data.order_state.status, OrderStatusKind::Filled);
        assert!(orders.get(&ClientOrderId::new("before-restart")).is_some());
    }

    /// An order's fill is the shares of each of its executions at its latest revision.
    #[test]
    fn executed_quantity_counts_each_execution_once_at_its_latest_revision() {
        let execution = |client_id, order_ref: &str, id: &str, shares| {
            Executions::ExecutionData(ExecutionData {
                execution: Execution {
                    client_id,
                    order_reference: order_ref.to_owned(),
                    execution_id: id.to_owned(),
                    shares,
                    ..Execution::default()
                },
                ..ExecutionData::default()
            })
        };
        let executed = ExecutedQuantities::from_executions(
            [
                execution(API_CLIENT, "a", "0001f4e8.6706.01.01", 3.0),
                execution(API_CLIENT, "a", "0001f4e8.6706.01.02", 2.0),
                execution(0, "a", "0001f4e8.6707.01.01", 1.0),
                execution(904, "a", "0001f4e8.6708.01.01", 5.0),
                execution(API_CLIENT, "b", "0001f4e8.6709.01.01", 7.0),
            ],
            API_CLIENT,
        );

        assert_eq!(executed.of(&ClientOrderId::new("a")), Some(dec!(3)));
        assert_eq!(executed.of(&ClientOrderId::new("b")), Some(dec!(7)));
        assert_eq!(executed.of(&ClientOrderId::new("c")), None);
    }

    /// A filled order reports what IB lists filled, though IB lists its total quantity as zero,
    /// and an untracked one carries IB's permanent id and the shape IB lists.
    #[test]
    fn a_filled_order_is_fully_filled() {
        let Orders::OrderData(mut data) = completed(
            0,
            "a",
            OrderStatusKind::Filled,
            "20261007 03:57:32 US/Eastern",
            "Filled Size: 4",
        ) else {
            unreachable!()
        };
        data.order.total_quantity = 0.0;
        let order = one(Orders::OrderData(data));

        let found = ended(
            ended_order(
                &key("a"),
                &order,
                None,
                &EarlierCompletions::new(),
                || -> Result<_, ()> { panic!("a filled order's fill is listed") },
            )
            .unwrap(),
        );

        assert_eq!(found.key, key("a"));
        assert_eq!(found.quantity, dec!(4));
        assert_eq!(found.kind, OrderKind::Limit);
        assert_eq!(found.price, Some(dec!(150)));
        let InactiveOrderState::FullyFilled(filled) = found.state else {
            panic!("{:?}", found.state)
        };
        assert_eq!(filled.id, OrderId::new("818545845"));
        assert_eq!(filled.filled_quantity, dec!(4));
        assert_eq!(
            filled.time_exchange,
            "2026-10-07T07:57:32Z".parse::<DateTime<Utc>>().unwrap()
        );
    }

    /// IB lists a placement it accepted and then rejected as cancelled, saying so in its
    /// completion text.
    #[test]
    fn a_placement_rejected_after_acceptance_failed_to_open() {
        let order = one(completed(
            API_CLIENT,
            "a",
            OrderStatusKind::Cancelled,
            "",
            "Rejected by System:\nGood-till-date order expired",
        ));
        let found = ended(
            ended_order(
                &key("a"),
                &order,
                None,
                &EarlierCompletions::new(),
                || -> Result<_, ()> { panic!("a rejected order filled nothing") },
            )
            .unwrap(),
        );
        assert!(matches!(
            found.state,
            InactiveOrderState::OpenFailed(OrderError::Rejected(ApiError::OrderRejected(ref reason)))
                if reason.starts_with("Rejected by System")
        ));
    }

    /// A cancelled order's fill is what its executions show; with none, nothing filled when the
    /// executions read covers its life, and unknown otherwise.
    #[test]
    fn a_cancelled_orders_fill_comes_from_its_executions() {
        let order = one(completed(
            API_CLIENT,
            "a",
            OrderStatusKind::Cancelled,
            "",
            "",
        ));
        let filled = |tracked: Option<&Tracked>, executed: Option<Decimal>| {
            let found = ended(
                ended_order(
                    &key("a"),
                    &order,
                    tracked,
                    &EarlierCompletions::new(),
                    || Ok::<_, ()>(executed),
                )
                .unwrap(),
            );
            let InactiveOrderState::Cancelled(cancelled) = found.state else {
                panic!("{:?}", found.state)
            };
            cancelled.filled_quantity
        };

        let young = tracked(1, GTC, true);
        let old = tracked(6, GTC, true);
        assert_eq!(filled(Some(&young), Some(dec!(2))), Some(dec!(2)));
        assert_eq!(filled(Some(&young), None), Some(Decimal::ZERO));
        assert_eq!(filled(Some(&old), None), None);
        assert_eq!(filled(None, None), None);
        assert_eq!(filled(None, Some(dec!(2))), Some(dec!(2)));
    }

    /// An adopted order's placement is unknown, so finding none of its executions says nothing
    /// of its fill.
    #[test]
    fn an_adopted_orders_fill_without_executions_is_unknown() {
        let order = one(completed(
            API_CLIENT,
            "a",
            OrderStatusKind::Cancelled,
            "",
            "",
        ));
        let mut adopted = tracked(1, GTC, false);
        adopted.registration.adopted = true;
        let found = ended(
            ended_order(
                &key("a"),
                &order,
                Some(&adopted),
                &EarlierCompletions::new(),
                || Ok::<_, ()>(None),
            )
            .unwrap(),
        );
        let InactiveOrderState::Cancelled(cancelled) = found.state else {
            panic!("{:?}", found.state)
        };
        assert_eq!(cancelled.filled_quantity, None);
    }

    /// A completion from before the tracked order under its id was placed is an earlier order's,
    /// so the tracked one has not ended; one from after it has.
    #[test]
    fn a_completion_older_than_the_tracked_order_is_an_earlier_orders() {
        let at = |ago: chrono::Duration| {
            let time = Utc::now() - ago;
            one(completed(
                API_CLIENT,
                "a",
                OrderStatusKind::Cancelled,
                &time.format("%Y%m%d %H:%M:%S UTC").to_string(),
                "",
            ))
        };
        let placed_an_hour_ago = Tracked {
            registration: Registration {
                age: Duration::from_secs(60 * 60),
                adopted: false,
            },
            ..tracked(0, GTC, false)
        };

        let noted = EarlierCompletions::new();
        let earlier = at(chrono::Duration::hours(2));
        assert!(matches!(
            ended_order(
                &key("a"),
                &earlier,
                Some(&placed_an_hour_ago),
                &noted,
                || Ok::<_, ()>(None)
            )
            .unwrap(),
            OrderLookup::NotEnded
        ));
        assert!(
            !noted.first_meeting(&ClientOrderId::new("a"), earlier.time.unwrap()),
            "the earlier order's completion is noted, so it is not warned about again"
        );
        let later = at(chrono::Duration::minutes(30));
        assert!(matches!(
            ended_order(
                &key("a"),
                &later,
                Some(&placed_an_hour_ago),
                &noted,
                || Ok::<_, ()>(None)
            )
            .unwrap(),
            OrderLookup::Ended(_)
        ));
    }

    /// IB keeps an earlier order's completion in its listing, so each lookup of its reused client
    /// order id meets it again: only the first meeting is new. Another completion under the id,
    /// or the same time under another id, is new too.
    #[test]
    fn an_earlier_orders_completion_is_new_only_at_its_first_meeting() {
        let noted = EarlierCompletions::new();
        let at = Utc::now() - chrono::Duration::hours(2);
        let a = ClientOrderId::new("a");

        assert!(noted.first_meeting(&a, at));
        assert!(!noted.first_meeting(&a, at));
        assert!(noted.first_meeting(&a, at - chrono::Duration::seconds(1)));
        assert!(noted.first_meeting(&ClientOrderId::new("b"), at));
    }

    /// The set is bounded: a completion it has forgotten is new again, and warned about again.
    #[test]
    fn a_forgotten_earlier_completion_is_new_again() {
        let noted = EarlierCompletions::with_capacity(1);
        let at = Utc::now();
        let (a, b) = (ClientOrderId::new("a"), ClientOrderId::new("b"));

        assert!(noted.first_meeting(&a, at));
        assert!(noted.first_meeting(&b, at));
        assert!(noted.first_meeting(&a, at), "a was evicted by b");
    }

    /// A cancelled day order that this client did not ask to cancel expired, as the account
    /// stream reads it; one it did ask to cancel was cancelled, under its IB order id.
    #[test]
    fn a_cancelled_day_order_expired_unless_its_cancel_was_requested() {
        let order = one(completed(
            API_CLIENT,
            "a",
            OrderStatusKind::Cancelled,
            "",
            "",
        ));
        let state = |tracked: &Tracked| {
            ended(
                ended_order(
                    &key("a"),
                    &order,
                    Some(tracked),
                    &EarlierCompletions::new(),
                    || Ok::<_, ()>(None),
                )
                .unwrap(),
            )
            .state
        };

        assert!(matches!(
            state(&tracked(1, TimeInForce::GoodUntilEndOfDay, false)),
            InactiveOrderState::Expired(_)
        ));
        let InactiveOrderState::Cancelled(cancelled) =
            state(&tracked(1, TimeInForce::GoodUntilEndOfDay, true))
        else {
            panic!("cancel was requested")
        };
        assert_eq!(cancelled.id, OrderId::new("12"));
    }

    /// An inactive order failed to open; one in a status that has not ended, or in a shape this
    /// client never sends and does not track, is not reported as ended.
    #[test]
    fn inactive_failed_and_unreadable_or_live_has_not_ended() {
        let inactive = one(completed(
            API_CLIENT,
            "a",
            OrderStatusKind::Inactive,
            "",
            "",
        ));
        assert!(matches!(
            ended(
                ended_order(
                    &key("a"),
                    &inactive,
                    None,
                    &EarlierCompletions::new(),
                    || Ok::<_, ()>(None)
                )
                .unwrap()
            )
            .state,
            InactiveOrderState::OpenFailed(_)
        ));

        let live = one(completed(
            API_CLIENT,
            "a",
            OrderStatusKind::Submitted,
            "",
            "",
        ));
        assert!(matches!(
            ended_order(&key("a"), &live, None, &EarlierCompletions::new(), || Ok::<
                _,
                (),
            >(
                None
            ))
            .unwrap(),
            OrderLookup::NotEnded
        ));

        let Orders::OrderData(mut data) =
            completed(API_CLIENT, "a", OrderStatusKind::Filled, "", "")
        else {
            unreachable!()
        };
        data.order.order_type = "REL".to_owned();
        let unreadable = one(Orders::OrderData(data));
        assert!(matches!(
            ended_order(
                &key("a"),
                &unreadable,
                None,
                &EarlierCompletions::new(),
                || Ok::<_, ()>(None)
            )
            .unwrap(),
            OrderLookup::NotEnded
        ));
    }

    /// The tracked shape is the one reported, unless it is another instrument's.
    #[test]
    fn a_tracked_order_keeps_the_shape_it_was_placed_with() {
        let order = one(completed(API_CLIENT, "a", OrderStatusKind::Filled, "", ""));
        let mut placed = tracked(1, TimeInForce::GoodUntilEndOfDay, false);
        placed.context.quantity = dec!(4);
        placed.context.kind = OrderKind::Market;
        placed.context.price = None;

        let found = ended(
            ended_order(
                &key("a"),
                &order,
                Some(&placed),
                &EarlierCompletions::new(),
                || Ok::<_, ()>(None),
            )
            .unwrap(),
        );
        assert_eq!(found.kind, OrderKind::Market);
        assert_eq!(found.time_in_force, TimeInForce::GoodUntilEndOfDay);

        placed.context.instrument = InstrumentNameExchange::new("MSFT");
        let found = ended(
            ended_order(
                &key("a"),
                &order,
                Some(&placed),
                &EarlierCompletions::new(),
                || Ok::<_, ()>(None),
            )
            .unwrap(),
        );
        assert_eq!(found.kind, OrderKind::Limit);
        assert_eq!(found.time_in_force, GTC);
    }

    /// Releasing an ended order frees its client order id: a filled one keeps its IB order id's
    /// entry, for executions still to come.
    #[test]
    fn releasing_an_ended_order_frees_its_id() {
        let order_ids = OrderIdMap::new();
        let pending_cancels = PendingCancels::new();
        let context = tracked(0, GTC, false).context;
        order_ids
            .register(ClientOrderId::new("a"), 12, context.clone())
            .unwrap();
        order_ids
            .register(ClientOrderId::new("b"), 13, context)
            .unwrap();
        pending_cancels.insert(13);

        let order = one(completed(API_CLIENT, "a", OrderStatusKind::Filled, "", ""));
        let filled = ended(
            ended_order(&key("a"), &order, None, &EarlierCompletions::new(), || {
                Ok::<_, ()>(None)
            })
            .unwrap(),
        );
        release_ended(&filled, &order_ids, &pending_cancels);
        assert_eq!(order_ids.get_ib_id(&ClientOrderId::new("a")), None);
        assert!(order_ids.contains(12));

        let order = one(completed(
            API_CLIENT,
            "b",
            OrderStatusKind::Cancelled,
            "",
            "",
        ));
        let cancelled = ended(
            ended_order(&key("b"), &order, None, &EarlierCompletions::new(), || {
                Ok::<_, ()>(None)
            })
            .unwrap(),
        );
        release_ended(&cancelled, &order_ids, &pending_cancels);
        assert!(!order_ids.contains(13));
        assert!(!pending_cancels.contains(13));
    }
}
