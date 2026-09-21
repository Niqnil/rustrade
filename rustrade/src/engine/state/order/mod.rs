use crate::engine::state::order::{
    in_flight_recorder::InFlightRequestRecorder, manager::OrderManager,
};
use derive_more::Constructor;
use fnv::FnvHashMap;
use rust_decimal::Decimal;
use rustrade_execution::order::{
    Order,
    id::{ClientOrderId, OrderId},
    request::{OrderRequestCancel, OrderRequestOpen, OrderResponseCancel},
    state::{ActiveOrderState, CancelInFlight, OrderState},
};
use rustrade_instrument::{exchange::ExchangeIndex, instrument::InstrumentIndex};
use rustrade_integration::collection::snapshot::Snapshot;
use serde::{Deserialize, Serialize};
use std::{collections::hash_map::Entry, fmt::Debug};
use tracing::{debug, error, warn};

pub mod in_flight_recorder;
pub mod manager;

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
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize, Constructor)]
pub struct Orders<ExchangeKey = ExchangeIndex, InstrumentKey = InstrumentIndex>(
    pub FnvHashMap<ClientOrderId, Order<ExchangeKey, InstrumentKey, ActiveOrderState>>,
);

impl<ExchangeKey, InstrumentKey> Default for Orders<ExchangeKey, InstrumentKey> {
    fn default() -> Self {
        Self(FnvHashMap::default())
    }
}

impl<ExchangeKey, InstrumentKey> Orders<ExchangeKey, InstrumentKey> {
    /// Remove all tracked orders, discarding any pending state.
    ///
    /// Used during contract expiry cleanup where the exchange silently voids all
    /// open and cancel-in-flight orders at expiry. The normal `cleanup_routing_tables`
    /// path cannot remove `CancelInFlight` entries because their cancel acks may
    /// never arrive after expiry, causing unbounded accumulation in a long-running engine.
    pub fn clear(&mut self) {
        self.0.clear();
    }

    /// Whether any tracked order is awaiting a response from the exchange.
    ///
    /// True for [`ActiveOrderState::OpenInFlight`] and [`ActiveOrderState::CancelInFlight`] — a
    /// request has been sent and its outcome is not yet known. An order merely *resting* at the
    /// exchange ([`ActiveOrderState::Open`]) is **not** in flight: nothing is owed in reply, and a
    /// limit order that never fills must not hold up a drain.
    pub fn has_request_in_flight(&self) -> bool {
        self.0.values().any(|order| {
            matches!(
                order.state,
                ActiveOrderState::OpenInFlight(_) | ActiveOrderState::CancelInFlight(_)
            )
        })
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
    /// Nothing happens unless the order is already tracked and `Open` under this exact venue
    /// [`OrderId`]. A fill for an untracked order cannot insert one, so this cannot resurrect an
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
        let Some(order) = self.0.get_mut(cid) else {
            return false;
        };
        let quantity = order.quantity;
        let ActiveOrderState::Open(open) = &mut order.state else {
            return false;
        };
        // A `ClientOrderId` identifies the order this engine sent; the venue's own id identifies
        // what it filled. Requiring both to agree keeps a fill off an order that merely shares the
        // slot -- a replaced order, or a reused client id.
        //
        // An order the venue never named has no id to agree with, so it cannot be advanced from
        // the fill stream at all. Reading two absent ids as a match is precisely the confusion
        // this comparison exists to prevent, so the absence is compared, not skipped.
        if open.id.assigned() != Some(order_id) {
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
            return true;
        }

        false
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
        self.0.values()
    }

    fn update_from_order_snapshot<AssetKey>(
        &mut self,
        snapshot: Snapshot<&Order<ExchangeKey, InstrumentKey, OrderState<AssetKey, InstrumentKey>>>,
    ) where
        AssetKey: Debug + Clone,
    {
        let Snapshot(snapshot) = snapshot;

        let (mut current_entry, update) = match (
            self.0.entry(snapshot.key.cid.clone()),
            snapshot.to_active(),
        ) {
            // Order untracked, input Snapshot is InactiveOrderState (ie/ finished), so ignore
            (Entry::Vacant(_), None) => {
                warn!(
                    exchange = ?snapshot.key.exchange,
                    instrument = ?snapshot.key.instrument,
                    strategy = %snapshot.key.strategy,
                    cid = %snapshot.key.cid,
                    update = ?snapshot,
                    "OrderManager received inactive order snapshot for untracked order - ignoring"
                );
                return;
            }

            // Order untracked, input Snapshot is ActiveOrderState, so insert
            (Entry::Vacant(entry), Some(update)) => {
                match &update.state {
                    ActiveOrderState::Open(open)
                        if open.quantity_remaining(update.quantity).is_zero() =>
                    {
                        debug!(
                            exchange = ?snapshot.key.exchange,
                            instrument = ?snapshot.key.instrument,
                            strategy = %snapshot.key.strategy,
                            cid = %snapshot.key.cid,
                            update = ?snapshot,
                            "OrderManager ignoring new Open order which is actually FulledFilled"
                        );
                    }
                    _active_order => {
                        debug!(
                            exchange = ?snapshot.key.exchange,
                            instrument = ?snapshot.key.instrument,
                            strategy = %snapshot.key.strategy,
                            cid = %snapshot.key.cid,
                            update = ?snapshot,
                            "OrderManager tracking new order"
                        );
                        entry.insert(update);
                    }
                }
                return;
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
                entry.remove();
                return;
            }

            // Order tracked, input Snapshot is ActiveOrderState, so forward for further processing
            (Entry::Occupied(entry), Some(update)) => (entry, update),
        };

        match (&current_entry.get().state, update.state) {
            (ActiveOrderState::OpenInFlight(_), ActiveOrderState::OpenInFlight(_)) => {
                warn!(
                    exchange = ?snapshot.key.exchange,
                    instrument = ?snapshot.key.instrument,
                    strategy = %snapshot.key.strategy,
                    cid = %snapshot.key.cid,
                    update = ?snapshot,
                    "OrderManager received a duplicate OpenInFlight recording - ignoring"
                );
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
                if open.quantity_remaining(update.quantity).is_zero() {
                    current_entry.remove();
                } else {
                    current_entry.get_mut().state = ActiveOrderState::Open(open);
                }
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
                current_entry.get_mut().state = ActiveOrderState::CancelInFlight(update);
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
                    error!(
                        exchange = ?snapshot.key.exchange,
                        instrument = ?snapshot.key.instrument,
                        strategy = %snapshot.key.strategy,
                        cid = %snapshot.key.cid,
                        tracked_id = %current.id,
                        update_id = %open.id,
                        "OrderManager received an Open snapshot naming a different exchange order than the one tracked under this ClientOrderId - ignoring"
                    );
                    return;
                }

                if current.is_superseded_by(&open) {
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
                    if open.quantity_remaining(update.quantity).is_zero() {
                        debug!(
                            exchange = ?snapshot.key.exchange,
                            instrument = ?snapshot.key.instrument,
                            strategy = %snapshot.key.strategy,
                            cid = %snapshot.key.cid,
                            update = ?snapshot,
                            "OrderManager removing an Open order a more recent snapshot reports as fully filled"
                        );
                        current_entry.remove();
                        return;
                    }

                    debug!(
                        exchange = ?snapshot.key.exchange,
                        instrument = ?snapshot.key.instrument,
                        strategy = %snapshot.key.strategy,
                        cid = %snapshot.key.cid,
                        update = ?snapshot,
                        "OrderManager updating an Open order from a more recent snapshot"
                    );
                    current_entry.get_mut().state = ActiveOrderState::Open(open);
                } else {
                    debug!(
                        exchange = ?snapshot.key.exchange,
                        instrument = ?snapshot.key.instrument,
                        strategy = %snapshot.key.strategy,
                        cid = %snapshot.key.cid,
                        update = ?snapshot,
                        "OrderManager received an out of sequence Open order snapshot - ignoring"
                    );
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

                current_entry.get_mut().state = ActiveOrderState::CancelInFlight(CancelInFlight {
                    order: Some(latest_open),
                })
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
            }
            (ActiveOrderState::CancelInFlight(current), ActiveOrderState::Open(update)) => {
                // Reached by `ClientOrderId` alone; see the `Open` -> `Open` arm.
                if current
                    .order
                    .as_ref()
                    .is_some_and(|held| held.id.contradicts(&update.id))
                {
                    error!(
                        exchange = ?snapshot.key.exchange,
                        instrument = ?snapshot.key.instrument,
                        strategy = %snapshot.key.strategy,
                        cid = %snapshot.key.cid,
                        update_id = %update.id,
                        "OrderManager received an Open snapshot naming a different exchange order than the tracked CancelInFlight - ignoring"
                    );
                    return;
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

                if update_open_is_latest {
                    current_entry.get_mut().state =
                        ActiveOrderState::CancelInFlight(CancelInFlight {
                            order: Some(update),
                        });
                }
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
            }
        }
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

        match (&order.get().state, &response.state) {
            (ActiveOrderState::OpenInFlight(_) | ActiveOrderState::Open(_), Ok(_)) => {
                warn!(
                    exchange = ?response.key.exchange,
                    instrument = ?response.key.instrument,
                    strategy = %response.key.strategy,
                    cid = %response.key.cid,
                    update = ?response,
                    "OrderManager received Ok(Cancelled) for tracked order not CancelInFlight - removing"
                );
                order.remove();
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
                order.remove();
            }
            (ActiveOrderState::OpenInFlight(_) | ActiveOrderState::Open(_), Err(error)) => {
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
                    debug!(
                        exchange = ?response.key.exchange,
                        instrument = ?response.key.instrument,
                        strategy = %response.key.strategy,
                        cid = %response.key.cid,
                        update = ?response,
                        ?error,
                        "OrderManager received Err(Cancelled) for previously Open order - setting Open"
                    );
                    order.get_mut().state = ActiveOrderState::Open(open.clone())
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
                    // -> it's expected that an Order snapshot is inbound
                    order.remove();
                }
            }
        }
    }
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
    ) {
        let Some(order) = self.0.get_mut(&request.key.cid) else {
            error!(
                cid = %request.key.cid,
                event = ?request,
                "OrderManager cannot mark CancelInFlight for untracked Order - ignoring"
            );
            return;
        };

        order.state = ActiveOrderState::CancelInFlight(CancelInFlight {
            order: order.state.open_meta().cloned(),
        });
    }

    fn record_in_flight_open(&mut self, request: &OrderRequestOpen<ExchangeKey, InstrumentKey>) {
        if let Some(duplicate_cid_order) =
            self.0.insert(request.key.cid.clone(), Order::from(request))
        {
            error!(
                cid = %duplicate_cid_order.key.cid,
                event = ?duplicate_cid_order,
                "OrderManager upserted Order OpenInFlight with duplicate ClientOrderId"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{engine::state::order::Orders, test_utils::time_plus_secs};
    use chrono::{DateTime, Utc};
    use rust_decimal_macros::dec;
    use rustrade_execution::{
        error::{ConnectivityError, OrderError},
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

    fn orders(
        orders: impl IntoIterator<Item = Order<ExchangeId, u64, ActiveOrderState>>,
    ) -> Orders<ExchangeId, u64> {
        Orders(
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

    fn order_cancel_in_flight(cid: ClientOrderId) -> Order<ExchangeId, u64, ActiveOrderState> {
        order(
            cid,
            ActiveOrderState::CancelInFlight(CancelInFlight { order: None }),
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
            state: OrderState::inactive(OrderError::Connectivity(ConnectivityError::Timeout)),
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
            .map(|order| (order.key.cid.clone(), Order::from(&order)))
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
            state: RequestOpen {
                side: Side::Buy,
                price: Some(dec!(1)),
                quantity: dec!(1),
                kind: OrderKind::Limit,
                time_in_force: TimeInForce::GoodUntilEndOfDay,
                position_id: None,
                reduce_only: false,
                market: None,
            },
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
    fn test_update_from_order_snapshot() {
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
                    ActiveOrderState::OpenInFlight(OpenInFlight),
                )]),
                input: order_snapshot_cancelled(cid.clone()),
                expected: Orders::default(),
            },
            TestCase {
                name: "tracked OpenInFlight, Snapshot is inactive fully filled, so remove",
                state: orders([order(
                    cid.clone(),
                    ActiveOrderState::OpenInFlight(OpenInFlight),
                )]),
                input: order_snapshot_fully_filled(cid.clone()),
                expected: Orders::default(),
            },
            TestCase {
                name: "tracked OpenInFlight, Snapshot is inactive failed, so remove",
                state: orders([order(
                    cid.clone(),
                    ActiveOrderState::OpenInFlight(OpenInFlight),
                )]),
                input: order_snapshot_failed(cid.clone()),
                expected: Orders::default(),
            },
            TestCase {
                name: "tracked OpenInFlight, Snapshot is inactive expired, so remove",
                state: orders([order(
                    cid.clone(),
                    ActiveOrderState::OpenInFlight(OpenInFlight),
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
                    ActiveOrderState::CancelInFlight(CancelInFlight { order: None }),
                )]),
                input: order_snapshot_cancelled(cid.clone()),
                expected: Orders::default(),
            },
            TestCase {
                name: "tracked CancelInFlight, Snapshot is inactive fully filled, so remove",
                state: orders([order(
                    cid.clone(),
                    ActiveOrderState::CancelInFlight(CancelInFlight { order: None }),
                )]),
                input: order_snapshot_fully_filled(cid.clone()),
                expected: Orders::default(),
            },
            TestCase {
                name: "tracked CancelInFlight, Snapshot is inactive failed, so remove",
                state: orders([order(
                    cid.clone(),
                    ActiveOrderState::CancelInFlight(CancelInFlight { order: None }),
                )]),
                input: order_snapshot_failed(cid.clone()),
                expected: Orders::default(),
            },
            TestCase {
                name: "tracked CancelInFlight, Snapshot is inactive expired, so remove",
                state: orders([order(
                    cid.clone(),
                    ActiveOrderState::CancelInFlight(CancelInFlight { order: None }),
                )]),
                input: order_snapshot_expired(cid.clone()),
                expected: Orders::default(),
            },
            TestCase {
                name: "tracked OpenInFlight, Snapshot is active OpenInFlight, so ignore duplicate",
                state: orders([order(
                    cid.clone(),
                    ActiveOrderState::OpenInFlight(OpenInFlight),
                )]),
                input: Snapshot(order(cid.clone(), OrderState::active(OpenInFlight))),
                expected: orders([order(
                    cid.clone(),
                    ActiveOrderState::OpenInFlight(OpenInFlight),
                )]),
            },
            TestCase {
                name: "tracked OpenInFlight, Snapshot is active Open but fully filled, so remove",
                state: orders([order(
                    cid.clone(),
                    ActiveOrderState::OpenInFlight(OpenInFlight),
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
                    ActiveOrderState::OpenInFlight(OpenInFlight),
                )]),
                input: order_snapshot_open(cid.clone(), time_base),
                expected: orders([order(cid.clone(), ActiveOrderState::Open(open(time_base)))]),
            },
            TestCase {
                name: "tracked OpenInFlight, Snapshot is active CancelInFlight, so update",
                state: orders([order(
                    cid.clone(),
                    ActiveOrderState::OpenInFlight(OpenInFlight),
                )]),
                input: Snapshot(order(
                    cid.clone(),
                    OrderState::active(CancelInFlight { order: None }),
                )),
                expected: orders([order(
                    cid.clone(),
                    ActiveOrderState::CancelInFlight(CancelInFlight { order: None }),
                )]),
            },
            TestCase {
                name: "tracked Open, Snapshot is active OpenInFlight, so ignore",
                state: orders([order(cid.clone(), ActiveOrderState::Open(open(time_base)))]),
                input: Snapshot(order(cid.clone(), OrderState::active(OpenInFlight))),
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
                    }),
                )),
                expected: orders([order(
                    cid.clone(),
                    ActiveOrderState::CancelInFlight(CancelInFlight {
                        order: Some(open(time_plus_secs(time_base, 2))),
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
                    }),
                )),
                expected: orders([order(
                    cid.clone(),
                    ActiveOrderState::CancelInFlight(CancelInFlight {
                        order: Some(open(time_plus_secs(time_base, 2))),
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
                    OrderState::active(CancelInFlight { order: None }),
                )),
                expected: orders([order(
                    cid.clone(),
                    ActiveOrderState::CancelInFlight(CancelInFlight {
                        order: Some(open(time_plus_secs(time_base, 1))),
                    }),
                )]),
            },
            TestCase {
                name: "tracked CancelInFlight, Snapshot is active OpenInFlight, so ignore",
                state: orders([order(
                    cid.clone(),
                    ActiveOrderState::CancelInFlight(CancelInFlight { order: None }),
                )]),
                input: Snapshot(order(cid.clone(), OrderState::active(OpenInFlight))),
                expected: orders([order(
                    cid.clone(),
                    ActiveOrderState::CancelInFlight(CancelInFlight { order: None }),
                )]),
            },
            TestCase {
                name: "tracked CancelInFlight w/ None Open, Snapshot is active Open, update accordingly",
                state: orders([order(
                    cid.clone(),
                    ActiveOrderState::CancelInFlight(CancelInFlight { order: None }),
                )]),
                input: order_snapshot_open(cid.clone(), time_base),
                expected: orders([order(
                    cid.clone(),
                    ActiveOrderState::CancelInFlight(CancelInFlight {
                        order: Some(open(time_plus_secs(time_base, 0))),
                    }),
                )]),
            },
            TestCase {
                name: "tracked CancelInFlight, Snapshot is active Open w/ older time, so ignore",
                state: orders([order(
                    cid.clone(),
                    ActiveOrderState::CancelInFlight(CancelInFlight {
                        order: Some(open(time_plus_secs(time_base, 2))),
                    }),
                )]),
                input: order_snapshot_open(cid.clone(), time_plus_secs(time_base, 1)),
                expected: orders([order(
                    cid.clone(),
                    ActiveOrderState::CancelInFlight(CancelInFlight {
                        order: Some(open(time_plus_secs(time_base, 2))),
                    }),
                )]),
            },
            TestCase {
                name: "tracked CancelInFlight, Snapshot is active Open w/ newer time, so update accordingly",
                state: orders([order(
                    cid.clone(),
                    ActiveOrderState::CancelInFlight(CancelInFlight {
                        order: Some(open(time_plus_secs(time_base, 1))),
                    }),
                )]),
                input: order_snapshot_open(cid.clone(), time_plus_secs(time_base, 2)),
                expected: orders([order(
                    cid.clone(),
                    ActiveOrderState::CancelInFlight(CancelInFlight {
                        order: Some(open(time_plus_secs(time_base, 2))),
                    }),
                )]),
            },
            TestCase {
                name: "tracked CancelInFlight, Snapshot is active CancelInFlight, so ignore duplicate",
                state: orders([order(
                    cid.clone(),
                    ActiveOrderState::CancelInFlight(CancelInFlight { order: None }),
                )]),
                input: Snapshot(order(
                    cid.clone(),
                    OrderState::active(CancelInFlight { order: None }),
                )),
                expected: orders([order(
                    cid.clone(),
                    ActiveOrderState::CancelInFlight(CancelInFlight { order: None }),
                )]),
            },
        ];

        for mut test in cases.into_iter() {
            test.state.update_from_order_snapshot(test.input.as_ref());
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
        state.update_from_order_snapshot(fill.as_ref());

        // A reconciliation fetch returns the same order with the venue's true cumulative, stamped
        // with creation time -- older than what the stream already applied.
        let recovered: Snapshot<Order<ExchangeId, u64, OrderState<u64, u64>>> = Snapshot(order(
            cid.clone(),
            OrderState::active(open_filled(time_created, filled_at_venue)),
        ));
        state.update_from_order_snapshot(recovered.as_ref());

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
        state.update_from_order_snapshot(fill.as_ref());

        // Same cumulative, creation stamp. Neither key admits it, so it is discarded -- exactly as
        // before the fill key existed.
        let stale: Snapshot<Order<ExchangeId, u64, OrderState<u64, u64>>> = Snapshot(order(
            cid.clone(),
            OrderState::active(open_filled(time_created, filled)),
        ));
        state.update_from_order_snapshot(stale.as_ref());

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
        state.update_from_order_snapshot(arrived_first.as_ref());

        // Processed second, so stamped later -- but it describes an earlier state of the order.
        // Only the cumulative gives it away.
        let overtaken: Snapshot<Order<ExchangeId, u64, OrderState<u64, u64>>> = Snapshot(order(
            cid.clone(),
            OrderState::active(open_filled(time_plus_secs(time_base, 2), dec!(0.3))),
        ));
        state.update_from_order_snapshot(overtaken.as_ref());

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
        state.update_from_order_snapshot(foreign.as_ref());

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
        state.update_from_order_snapshot(named.as_ref());

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
            }),
        ));
        state.update_from_order_snapshot(cancelling.as_ref());

        assert_eq!(
            state,
            orders([order(
                cid.clone(),
                ActiveOrderState::CancelInFlight(CancelInFlight {
                    order: Some(open_assigned("A", time_base)),
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
        state.update_from_order_snapshot(foreign.as_ref());

        assert_eq!(
            state,
            orders([order(
                cid,
                ActiveOrderState::CancelInFlight(CancelInFlight {
                    order: Some(open_assigned("A", time_base)),
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
                state: orders([order(cid.clone(), ActiveOrderState::from(OpenInFlight))]),
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
                    ActiveOrderState::from(CancelInFlight { order: None }),
                )]),
                input: response_cancel_ok(cid.clone()),
                expected: Orders::default(),
            },
            TestCase {
                name: "tracked OpenInFlight, response Err, so ignore",
                state: orders([order(cid.clone(), ActiveOrderState::from(OpenInFlight))]),
                input: response_cancel_err(cid.clone()),
                expected: orders([order(cid.clone(), ActiveOrderState::from(OpenInFlight))]),
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
                    }),
                )]),
                input: response_cancel_err(cid.clone()),
                expected: orders([order(cid.clone(), ActiveOrderState::from(open(time_base)))]),
            },
            TestCase {
                name: "tracked CancelInFlight w/ None Open, response Err, so remove",
                state: orders([order(
                    cid.clone(),
                    ActiveOrderState::from(CancelInFlight { order: None }),
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
                test.state.record_in_flight_cancel(&in_flight);
            }
            assert_eq!(test.state, test.expected, "TC{index} failed")
        }
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
                expected: Orders(request_opens([request_open(cid_1.clone())])),
            },
            TestCase {
                // TC1: Insert InFlight that is already tracked
                state: Orders(request_opens([request_open(cid_1.clone())])),
                input: vec![request_open(cid_1.clone())],
                expected: Orders(request_opens([request_open(cid_1.clone())])),
            },
            TestCase {
                // TC2: Insert one untracked InFlight, and one already tracked
                state: Orders(request_opens([request_open(cid_1.clone())])),
                input: vec![request_open(cid_1.clone()), request_open(cid_2.clone())],
                expected: Orders(request_opens([request_open(cid_1), request_open(cid_2)])),
            },
        ];

        for (index, mut test) in cases.into_iter().enumerate() {
            for in_flight in test.input {
                test.state.record_in_flight_open(&in_flight);
            }
            assert_eq!(test.state, test.expected, "TC{index} failed")
        }
    }
}
