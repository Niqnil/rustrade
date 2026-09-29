use crate::engine::state::order::in_flight_recorder::InFlightRequestRecorder;
use rustrade_execution::order::{
    Order,
    request::OrderResponseCancel,
    state::{ActiveOrderState, OrderState},
};
use rustrade_integration::collection::snapshot::Snapshot;
use std::fmt::Debug;

/// Synchronous order manager that tracks the lifecycle of active exchange orders.
///
/// See [`Orders`](super::Orders) for an example implementation.
pub trait OrderManager<ExchangeKey, InstrumentKey>
where
    Self: InFlightRequestRecorder<ExchangeKey, InstrumentKey>,
{
    fn orders<'a>(
        &'a self,
    ) -> impl Iterator<Item = &'a Order<ExchangeKey, InstrumentKey, ActiveOrderState>>
    where
        ExchangeKey: 'a,
        InstrumentKey: 'a;

    /// Apply an order snapshot to the tracked order it describes.
    ///
    /// # Resolution
    ///
    /// The snapshot is matched to a tracked order by its `ClientOrderId`, and that alone. Callers
    /// must therefore keep one `ClientOrderId` to one exchange order: reusing one across two
    /// orders puts both in the same slot.
    ///
    /// Where a tracked state and an incoming one both name a venue order and the two disagree, the
    /// update is refused and reported, rather than overwriting one order's state with another's.
    /// An order the venue has not named -- `VenueOrderId::ClientAssigned` -- carries no identifier
    /// to disagree with, so such a snapshot is applied, and a later snapshot bringing the venue's
    /// own identifier is adopted in the ordinary way.
    ///
    /// Terminal snapshots are resolved by `ClientOrderId` alone with no such check, because the
    /// states they carry hold a plain `OrderId` that nothing compares for identity.
    ///
    /// # Ordering
    ///
    /// Successive `Open` states for one order are ordered by `Open::is_superseded_by`, and one that
    /// does not supersede the state already held is discarded. That test rests on cumulative fill
    /// being append-only for a single venue order as well as on `Open::time_exchange`: a snapshot
    /// reporting strictly *less* filled is refused however it is stamped, and one reporting
    /// strictly more is admitted however it is stamped.
    ///
    /// This is what gives the gate any force at a venue whose stamps are applied locally on
    /// receipt rather than reported by the venue -- IBKR's `orderStatus` callback carries no
    /// timestamp field at all -- where ordering on the stamp alone admits every snapshot and
    /// degrades to last-writer-wins.
    ///
    /// Producers must still stamp `Open::time_exchange` with the venue's last-update time wherever
    /// the venue supplies one; see its own documentation for what a creation stamp costs here. The
    /// fill invariant narrows that cost, it does not remove it: it cannot order two snapshots that
    /// report the same cumulative fill, which is every snapshot of an order resting unfilled.
    fn update_from_order_snapshot<AssetKey>(
        &mut self,
        snapshot: Snapshot<&Order<ExchangeKey, InstrumentKey, OrderState<AssetKey, InstrumentKey>>>,
    ) where
        AssetKey: Debug + Clone;

    fn update_from_cancel_response<AssetKey>(
        &mut self,
        response: &OrderResponseCancel<ExchangeKey, AssetKey, InstrumentKey>,
    ) where
        AssetKey: Debug + Clone;
}
