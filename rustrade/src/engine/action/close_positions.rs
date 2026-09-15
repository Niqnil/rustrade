use crate::{
    engine::{
        Engine,
        action::send_requests::{SendCancelsAndOpensOutput, SendRequests},
        execution_tx::ExecutionTxMap,
        state::{
            MarketSnapshotSource, instrument::filter::InstrumentFilter,
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
    State:
        InFlightRequestRecorder<ExchangeKey, InstrumentKey> + MarketSnapshotSource<InstrumentKey>,
    ExecutionTxs: ExecutionTxMap<ExchangeKey, InstrumentKey>,
    Strategy: ClosePositionsStrategy<ExchangeKey, AssetKey, InstrumentKey, State = State>,
    ExchangeKey: Debug + Clone,
    InstrumentKey: Debug + Clone,
{
    fn close_positions(
        &mut self,
        filter: &InstrumentFilter<ExchangeKey, AssetKey, InstrumentKey>,
    ) -> SendCancelsAndOpensOutput<ExchangeKey, InstrumentKey> {
        // Generate orders
        let (cancels, opens) = self.strategy.close_positions_requests(&self.state, filter);

        // Bypass risk checks...

        // Send order requests, stamping each open with the market this state holds now -- see
        // `MarketSnapshotSource`. A close is an ordinary open request to the venue, and a simulated
        // one needs a price just as much as an entry does.
        let cancels = self.send_requests(cancels);
        let opens = self.send_requests(opens.into_iter().map(|mut open| {
            open.state.market = self.state.market_snapshot(&open.key.instrument);
            open
        }));

        // Record in flight order requests
        self.state.record_in_flight_cancels(cancels.sent_iter());
        self.state.record_in_flight_opens(opens.sent_iter());

        SendCancelsAndOpensOutput::new(cancels, opens)
    }
}
