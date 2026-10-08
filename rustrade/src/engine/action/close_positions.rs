use crate::{
    engine::{
        Engine,
        action::send_requests::SendCancelsAndOpensOutput,
        clock::EngineClock,
        execution_tx::ExecutionTxMap,
        state::{
            MarketSnapshotSource, TracksInstrument, TracksOrder,
            instrument::filter::InstrumentFilter,
            order::in_flight_recorder::InFlightRequestRecorder,
        },
    },
    strategy::close_positions::ClosePositionsStrategy,
};
use rustrade_instrument::{
    asset::AssetIndex, exchange::ExchangeIndex, instrument::InstrumentIndex,
};
use std::fmt::Debug;

/// Trait that defines how the [`Engine`] generates & sends order requests for closing open
/// positions.
///
/// # Type Parameters
/// * `ExchangeKey` - Type used to identify an exchange (defaults to [`ExchangeIndex`]).
/// * `AssetKey` - Type used to identify an asset (defaults to [`AssetIndex`]).
/// * `InstrumentKey` - Type used to identify an instrument (defaults to [`InstrumentIndex`]).
pub trait ClosePositions<
    ExchangeKey = ExchangeIndex,
    AssetKey = AssetIndex,
    InstrumentKey = InstrumentIndex,
>
{
    /// Generates and sends order requests to close open positions.
    ///
    /// Uses the provided [`InstrumentFilter`] to determine which positions to close.
    fn close_positions(
        &mut self,
        filter: &InstrumentFilter<ExchangeKey, AssetKey, InstrumentKey>,
    ) -> SendCancelsAndOpensOutput<ExchangeKey, InstrumentKey>;
}

impl<Clock, State, ExecutionTxs, Strategy, Risk, ExchangeKey, AssetKey, InstrumentKey>
    ClosePositions<ExchangeKey, AssetKey, InstrumentKey>
    for Engine<Clock, State, ExecutionTxs, Strategy, Risk>
where
    Clock: EngineClock,
    State: InFlightRequestRecorder<ExchangeKey, InstrumentKey>
        + MarketSnapshotSource<InstrumentKey>
        + TracksInstrument<InstrumentKey>
        + TracksOrder<InstrumentKey>,
    ExecutionTxs: ExecutionTxMap<ExchangeKey, InstrumentKey>,
    Strategy: ClosePositionsStrategy<ExchangeKey, AssetKey, InstrumentKey, State = State>,
    ExchangeKey: Debug + Clone,
    InstrumentKey: Debug + Clone + PartialEq,
{
    fn close_positions(
        &mut self,
        filter: &InstrumentFilter<ExchangeKey, AssetKey, InstrumentKey>,
    ) -> SendCancelsAndOpensOutput<ExchangeKey, InstrumentKey> {
        // Generate orders
        let (cancels, opens) = self.strategy.close_positions_requests(&self.state, filter);

        // Collect both Iterators, since the strategy may have borrowed the state to build them,
        // and sending records in-flight requests on it. An empty Vec does not allocate.
        let cancels: Vec<_> = cancels.into_iter().collect();
        let opens: Vec<_> = opens.into_iter().collect();

        // Bypass risk checks...

        // Send order requests, stamping each open with the market this state holds now -- see
        // `MarketSnapshotSource`. A close is an ordinary open request to the venue, and a simulated
        // one needs a price just as much as an entry does.
        let cancels = self.send_cancel_requests(cancels);
        let opens = self.send_open_requests(opens);

        SendCancelsAndOpensOutput::new(cancels, opens)
    }
}
