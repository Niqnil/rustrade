use crate::engine::state::EngineState;
use rustrade_execution::order::request::{OrderRequestCancel, OrderRequestOpen};
use rustrade_instrument::{exchange::ExchangeIndex, instrument::InstrumentIndex};

/// Synchronous in-flight open and in-flight cancel order request tracker.
///
/// See [`Orders`](super::Orders) for an example implementation.
pub trait InFlightRequestRecorder<ExchangeKey = ExchangeIndex, InstrumentKey = InstrumentIndex> {
    fn record_in_flight_cancels<'a>(
        &mut self,
        requests: impl IntoIterator<Item = &'a OrderRequestCancel<ExchangeKey, InstrumentKey>>,
    ) where
        ExchangeKey: 'a,
        InstrumentKey: 'a,
    {
        requests
            .into_iter()
            .for_each(|request| self.record_in_flight_cancel(request))
    }

    fn record_in_flight_opens<'a>(
        &mut self,
        requests: impl IntoIterator<Item = &'a OrderRequestOpen<ExchangeKey, InstrumentKey>>,
    ) where
        ExchangeKey: 'a,
        InstrumentKey: 'a,
    {
        requests
            .into_iter()
            .for_each(|request| self.record_in_flight_open(request))
    }

    fn record_in_flight_cancel(&mut self, request: &OrderRequestCancel<ExchangeKey, InstrumentKey>);

    fn record_in_flight_open(&mut self, request: &OrderRequestOpen<ExchangeKey, InstrumentKey>);
}

/// Records into the request's instrument state.
///
/// # Panics
/// Each method panics if the request's instrument is not tracked, as
/// [`InstrumentStates::instrument_index_mut`](crate::engine::state::instrument::InstrumentStates::instrument_index_mut)
/// does. The `Engine` never records one: it rejects a request for an untracked instrument before
/// sending it (see [`TracksInstrument`](crate::engine::state::TracksInstrument)). A caller that
/// records here itself must check first.
impl<GlobalData, InstrumentData> InFlightRequestRecorder<ExchangeIndex, InstrumentIndex>
    for EngineState<GlobalData, InstrumentData>
where
    InstrumentData: InFlightRequestRecorder<ExchangeIndex, InstrumentIndex>,
{
    fn record_in_flight_cancel(
        &mut self,
        request: &OrderRequestCancel<ExchangeIndex, InstrumentIndex>,
    ) {
        let instrument_state = self
            .instruments
            .instrument_index_mut(&request.key.instrument);

        instrument_state.orders.record_in_flight_cancel(request);
        instrument_state.data.record_in_flight_cancel(request);
    }

    fn record_in_flight_open(
        &mut self,
        request: &OrderRequestOpen<ExchangeIndex, InstrumentIndex>,
    ) {
        let instrument_state = self
            .instruments
            .instrument_index_mut(&request.key.instrument);

        instrument_state.orders.record_in_flight_open(request);
        instrument_state.data.record_in_flight_open(request);
        instrument_state.tear_sheet.record_open_requested();

        // Store CID → PositionId mapping for hedging-mode fill routing.
        if let Some(position_id) = &request.state.position_id {
            instrument_state
                .position_ids
                .insert(request.key.cid.clone(), position_id.clone());
        }
    }
}
