use crate::engine::state::EngineState;
use chrono::{DateTime, Utc};
use rustrade_execution::order::request::{OrderRequestCancel, OrderRequestOpen};
use rustrade_instrument::{exchange::ExchangeIndex, instrument::InstrumentIndex};

/// Synchronous in-flight open and in-flight cancel order request tracker.
///
/// `time_sent` is when the `Engine` sent the requests, by its `EngineClock`, so a backtest records
/// the simulated time. [`Orders`](super::Orders) keeps it on the in-flight order state, where the
/// `Engine`'s in-flight deadline reads it.
///
/// See [`Orders`](super::Orders) for an example implementation.
pub trait InFlightRequestRecorder<ExchangeKey = ExchangeIndex, InstrumentKey = InstrumentIndex> {
    fn record_in_flight_cancels<'a>(
        &mut self,
        requests: impl IntoIterator<Item = &'a OrderRequestCancel<ExchangeKey, InstrumentKey>>,
        time_sent: DateTime<Utc>,
    ) where
        ExchangeKey: 'a,
        InstrumentKey: 'a,
    {
        requests
            .into_iter()
            .for_each(|request| self.record_in_flight_cancel(request, time_sent))
    }

    fn record_in_flight_opens<'a>(
        &mut self,
        requests: impl IntoIterator<Item = &'a OrderRequestOpen<ExchangeKey, InstrumentKey>>,
        time_sent: DateTime<Utc>,
    ) where
        ExchangeKey: 'a,
        InstrumentKey: 'a,
    {
        requests
            .into_iter()
            .for_each(|request| self.record_in_flight_open(request, time_sent))
    }

    fn record_in_flight_cancel(
        &mut self,
        request: &OrderRequestCancel<ExchangeKey, InstrumentKey>,
        time_sent: DateTime<Utc>,
    );

    fn record_in_flight_open(
        &mut self,
        request: &OrderRequestOpen<ExchangeKey, InstrumentKey>,
        time_sent: DateTime<Utc>,
    );
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
        time_sent: DateTime<Utc>,
    ) {
        let instrument_state = self
            .instruments
            .instrument_index_mut(&request.key.instrument);

        instrument_state
            .orders
            .record_in_flight_cancel(request, time_sent);
        instrument_state
            .data
            .record_in_flight_cancel(request, time_sent);
    }

    fn record_in_flight_open(
        &mut self,
        request: &OrderRequestOpen<ExchangeIndex, InstrumentIndex>,
        time_sent: DateTime<Utc>,
    ) {
        let instrument_state = self
            .instruments
            .instrument_index_mut(&request.key.instrument);

        instrument_state
            .orders
            .record_in_flight_open(request, time_sent);
        instrument_state
            .data
            .record_in_flight_open(request, time_sent);
        instrument_state.tear_sheet.record_open_requested();

        // Store CID → PositionId mapping for hedging-mode fill routing.
        if let Some(position_id) = &request.state.position_id {
            instrument_state
                .position_ids
                .insert(request.key.cid.clone(), position_id.clone());
        }
    }
}
