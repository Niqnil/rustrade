use crate::engine::state::order::{SnapshotApplied, in_flight_recorder::InFlightRequestRecorder};
use rustrade_execution::order::{
    Order,
    report::OrderReport,
    request::{OrderResponseAmend, OrderResponseCancel},
    state::ActiveOrderState,
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
    /// own identifier is adopted in the ordinary way. The reverse never happens: a snapshot naming
    /// no venue order is applied to an order the venue has named with that identifier kept, since
    /// an identifier once assigned is never unassigned.
    ///
    /// Terminal snapshots are resolved by `ClientOrderId` alone with no such check, because the
    /// states they carry hold a plain `OrderId` that nothing compares for identity.
    ///
    /// Two exceptions, for an order replaced at the venue (see "Orders replaced at the venue" on
    /// [`Orders`](super::Orders)): a snapshot linked to the order it replaces
    /// ([`OrderReport::replaces`]) moves the tracked order to the venue order id, and the client
    /// order id, it names, and a snapshot naming a venue order id the order was replaced from,
    /// working or terminal, is ignored as late. Each replace followed is returned.
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
    ///
    /// # Terms
    ///
    /// The order's price, quantity, kind and time in force are taken from a snapshot only as far
    /// as its [`ReportedTerms`](rustrade_execution::order::report::ReportedTerms) say the venue
    /// stated them, under the rules in [`terms`](super::terms): a new quantity, a fixed limit
    /// price, or a fixed trigger within the same kind, and the venue's value for a stand-in the
    /// order holds. A change of kind or time in force is not adopted but reported as diverged.
    /// A term the snapshot does not state is never read, so retirement is decided against the
    /// quantity tracked once the snapshot's terms are applied.
    ///
    /// Terms are adopted:
    /// - from the venue's first report of an order whose open is in flight;
    /// - from a later snapshot reporting no less filled than the state held, each stated term
    ///   only when the snapshot is stamped no earlier than the venue last stated that term,
    ///   whether or not its state supersedes the one held (see "Terms times" on
    ///   [`TrackedOrder`](super::TrackedOrder)). A snapshot stating no terms, or only some,
    ///   stamped later on another path, so cannot hide a change. A change from a snapshot
    ///   whose state does not supersede the one held still retires an order it leaves nothing to
    ///   fill;
    /// - likewise behind a cancel in flight, though the order is not retired there: the cancel's
    ///   answer ends it.
    ///
    /// While an amend is in flight nothing is adopted, since the amend's answer settles the
    /// terms. The order is retired only once nothing is left of the greater of its quantity and
    /// the amend's, and a snapshot stating terms that match neither the order before the amend nor
    /// after it is reported as diverged.
    ///
    /// An order is retired when a superseding snapshot leaves it nothing to fill, including one
    /// that reports more filled than its quantity, which is also warned of.
    ///
    /// An untracked order is tracked on the snapshot's terms as they stand, stated or not, and
    /// returned as adopted when the snapshot is a venue's report rather than the engine's own
    /// record of an open request. A venue's report of an untracked working order whose quantity is
    /// zero or negative is not tracked, and is returned as not adopted: such a quantity is a
    /// placeholder, and tracked it would read as an order with nothing left to fill.
    ///
    /// Returns what the snapshot did: whether it retired the order, the replace it followed, the
    /// terms it changed or left diverged, and the order adopted or refused.
    fn update_from_order_snapshot<AssetKey>(
        &mut self,
        snapshot: Snapshot<&OrderReport<ExchangeKey, AssetKey, InstrumentKey>>,
    ) -> SnapshotApplied<ExchangeKey, InstrumentKey>
    where
        AssetKey: Debug + Clone;

    fn update_from_cancel_response<AssetKey>(
        &mut self,
        response: &OrderResponseCancel<ExchangeKey, AssetKey, InstrumentKey>,
    ) where
        AssetKey: Debug + Clone;

    /// Apply the answer to an amend.
    ///
    /// # Contract
    /// - `Ok(Amended)` commits the amended price, quantity and kind to an order with its amend in
    ///   flight, or with a cancel sent since: a late answer behind a cancel still says what the
    ///   order was while it rested. An
    ///   [`AmendInFlight`](rustrade_execution::order::state::AmendInFlight) order returns to
    ///   `Open`, carrying the identifier the venue now addresses it by. An order the amend left
    ///   with nothing to fill stops being tracked.
    /// - `Ok(Amended)` is ignored for an `Open` order, which has no amend in flight, and for an
    ///   answer about another venue order: one whose
    ///   [`previous_id`](rustrade_execution::order::amend::Amended::previous_id), or else whose
    ///   order's identifier, contradicts the identifier tracked
    ///   ([`VenueOrderId::contradicts`](rustrade_execution::order::id::VenueOrderId::contradicts)),
    ///   unless the order's identifier after the amend is the one tracked: a snapshot linked to
    ///   the order before the amend may have moved the order to it first.
    /// - A `previous_id` the venue assigned is recorded as an identifier the order was replaced
    ///   from, so a late report naming it is ignored, as for a replace the venue reports (see
    ///   "Orders replaced at the venue" on [`Orders`](super::Orders)).
    /// - A refusal or rejection returns an `AmendInFlight` order to `Open`, unchanged, or stops
    ///   tracking it when fills while the amend was in flight left it nothing to fill.
    /// - An unknown outcome
    ///   ([`OrderError::is_outcome_unknown`](rustrade_execution::error::OrderError::is_outcome_unknown))
    ///   leaves the amend in flight: the `ExecutionManager` looks the order up and answers again.
    fn update_from_amend_response<AssetKey>(
        &mut self,
        response: &OrderResponseAmend<ExchangeKey, AssetKey, InstrumentKey>,
    ) where
        AssetKey: Debug + Clone;
}
