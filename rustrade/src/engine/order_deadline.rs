//! Cancelling what is left of an order whose
//! [`cancel_after`](rustrade_execution::order::request::RequestOpen::cancel_after) ran out.
//!
//! The driver that sent the order times it and reports the deadline on the account stream as an
//! [`AccountEventKind::OrderDeadline`](rustrade_execution::AccountEventKind::OrderDeadline). The
//! engine answers with an ordinary cancel, so a cancel the venue refuses, or one whose outcome is
//! unknown, takes the paths every cancel takes. What it did is reported as an
//! [`EngineOutput::OrderDeadline`].
//!
//! An open the venue has not acknowledged yet is held here and cancelled once it is, because some
//! venues cancel only by their own order id, which the acknowledgement brings.
//!
//! A drain drops every deadline, as the driver does: it accepts no more requests once draining,
//! so a cancel could not reach the venue.

use crate::engine::{
    Engine, EngineOutput,
    action::send_requests::SendRequestsOutput,
    audit::ProcessAudit,
    clock::EngineClock,
    execution_tx::ExecutionTxMap,
    state::{EngineState, order::in_flight_recorder::InFlightRequestRecorder},
};
use rustrade_execution::order::{
    OrderKey, deadline::OrderDeadline, id::ClientOrderId, request::RequestCancel,
    state::ActiveOrderState,
};
use rustrade_instrument::{exchange::ExchangeIndex, instrument::InstrumentIndex};
use rustrade_integration::collection::none_one_or_many::NoneOneOrMany;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

/// What the engine did about an order whose `cancel_after` ran out.
///
/// One for each [`AccountEventKind::OrderDeadline`](rustrade_execution::AccountEventKind::OrderDeadline),
/// and one [`Cancel`](Self::Cancel) later for an order that was
/// [`AwaitingAck`](Self::AwaitingAck), on the event that acknowledged it.
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize)]
#[non_exhaustive]
pub enum OrderDeadlineOutput<ExchangeKey = ExchangeIndex, InstrumentKey = InstrumentIndex> {
    /// A cancel was sent, or failed to send (see
    /// [`SendRequestsOutput::errors`]). The venue's answer arrives as for any cancel.
    Cancel(SendRequestsOutput<RequestCancel, ExchangeKey, InstrumentKey>),
    /// The venue had not acknowledged the open yet. It is cancelled once it is, and dropped if the
    /// open is refused or the order ends first.
    AwaitingAck(OrderKey<ExchangeKey, InstrumentKey>),
    /// A cancel was already in flight, so none was sent. If that cancel fails, the order is left
    /// working: a deadline sends at most one cancel.
    AlreadyCancelling(OrderKey<ExchangeKey, InstrumentKey>),
    /// The engine tracks no such order: it ended, or was never tracked. Nothing was sent.
    NotTracked(OrderKey<ExchangeKey, InstrumentKey>),
    /// The engine is draining, so nothing was sent. A drain drops the deadlines still to come,
    /// because the driver accepts no more requests and a cancel could not reach the venue.
    Draining(OrderKey<ExchangeKey, InstrumentKey>),
}

/// The orders whose deadline ran out before the venue acknowledged them, to cancel once it has.
///
/// Held by the [`Engine`], not its state: only the engine sends the cancel, and a deadline lives
/// no longer than the driver's memory of it, so neither a snapshot nor the audit replica carries
/// one.
///
/// Ordered, so that the orders acknowledged by one event are cancelled in the same order on every
/// run. It holds a handful at most.
#[derive(Debug, Clone, Default, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub(crate) struct AwaitingAck(BTreeSet<(InstrumentIndex, ClientOrderId)>);

impl<Clock, GlobalData, InstrumentData, ExecutionTxs, Strategy, Risk>
    Engine<Clock, EngineState<GlobalData, InstrumentData>, ExecutionTxs, Strategy, Risk>
where
    Clock: EngineClock,
    InstrumentData: InFlightRequestRecorder,
    ExecutionTxs: ExecutionTxMap,
{
    /// Act on `deadline`: cancel the order if it is working, hold it for its acknowledgement if
    /// the venue has not given one, and otherwise send nothing. Nothing is sent during a drain.
    pub(crate) fn on_order_deadline(&mut self, deadline: &OrderDeadline) -> OrderDeadlineOutput {
        let key = &deadline.key;
        if self.meta.draining {
            return OrderDeadlineOutput::Draining(key.clone());
        }
        let state = self
            .state
            .instruments
            .get_index(&key.instrument)
            .and_then(|instrument| instrument.orders.0.get(&key.cid))
            .map(|order| &order.state);
        match state {
            None => OrderDeadlineOutput::NotTracked(key.clone()),
            Some(ActiveOrderState::CancelInFlight(_)) => {
                OrderDeadlineOutput::AlreadyCancelling(key.clone())
            }
            Some(ActiveOrderState::OpenInFlight(_)) => {
                self.awaiting_ack
                    .0
                    .insert((key.instrument, key.cid.clone()));
                OrderDeadlineOutput::AwaitingAck(key.clone())
            }
            Some(ActiveOrderState::Open(_) | ActiveOrderState::AmendInFlight(_)) => {
                OrderDeadlineOutput::Cancel(self.cancel_tracked(key.instrument, &key.cid))
            }
        }
    }

    /// Cancel each order held for its acknowledgement that now has one, and drop each that ended
    /// or has a cancel in flight. Cheap when nothing is held, which is almost always.
    pub(crate) fn cancel_acknowledged_past_deadline(&mut self) -> Vec<OrderDeadlineOutput> {
        if self.awaiting_ack.0.is_empty() {
            return Vec::new();
        }
        let mut ready = Vec::new();
        self.awaiting_ack.0.retain(|(instrument, cid)| {
            let state = self
                .state
                .instruments
                .get_index(instrument)
                .and_then(|state| state.orders.0.get(cid))
                .map(|order| &order.state);
            match state {
                Some(ActiveOrderState::OpenInFlight(_)) => true,
                Some(ActiveOrderState::Open(_) | ActiveOrderState::AmendInFlight(_)) => {
                    ready.push((*instrument, cid.clone()));
                    false
                }
                // Ended, refused, or cancelled by the strategy meanwhile: nothing is left to do.
                Some(ActiveOrderState::CancelInFlight(_)) | None => false,
            }
        });
        ready
            .into_iter()
            .map(|(instrument, cid)| {
                OrderDeadlineOutput::Cancel(self.cancel_tracked(instrument, &cid))
            })
            .collect()
    }

    /// Send a cancel of the tracked order `cid` of `instrument`, as a strategy's cancel is sent.
    fn cancel_tracked(
        &mut self,
        instrument: InstrumentIndex,
        cid: &ClientOrderId,
    ) -> SendRequestsOutput<RequestCancel> {
        let request = self
            .state
            .instruments
            .get_index(&instrument)
            .and_then(|state| state.orders.0.get(cid))
            .and_then(|order| order.to_request_cancel());
        self.send_cancel_requests(request)
    }
}

/// `audit` with `output` added, and any unrecoverable error its cancel met added to its errors.
pub(crate) fn add_to_audit<Event, OnTradingDisabled, OnDisconnect>(
    audit: ProcessAudit<Event, EngineOutput<OnTradingDisabled, OnDisconnect>>,
    output: OrderDeadlineOutput,
) -> ProcessAudit<Event, EngineOutput<OnTradingDisabled, OnDisconnect>> {
    let unrecoverable = match &output {
        OrderDeadlineOutput::Cancel(cancel) => cancel.unrecoverable_errors(),
        _ => NoneOneOrMany::None,
    };
    audit
        .add_errors(unrecoverable)
        .add_output(EngineOutput::OrderDeadline(output))
}
