use rustrade_execution::order::request::{
    CancelThenOpen, OrderAmend, OrderRequestCancel, OrderRequestOpen,
};
use rustrade_instrument::{exchange::ExchangeIndex, instrument::InstrumentIndex};

/// Strategy interface for generating algorithmic open and cancel order requests based on the
/// current `EngineState`.
///
/// # Orders need not name the instrument that motivated them
/// [`Self::generate_algo_orders`] receives the whole state and returns orders keyed by arbitrary
/// exchange and instrument keys. Nothing requires an order to name the instrument whose prices
/// produced the signal, which is what lets a strategy decide on one instrument and trade another —
/// a CFD or index proxy driving an order on the corresponding future, a spot series driving a
/// perpetual.
///
/// Both instruments are registered as ordinary `Instrument`s and keep **separate** ledgers: the
/// position and its `pnl_unrealised` live on the instrument the orders name, and the instrument that
/// was merely priced holds no position. The conversion between them — hedge ratio, contract
/// multiplier, quantity, basis — is a trading decision this trait cannot make for you, and an
/// incorrect one produces a plausible-looking position rather than an error.
///
/// Register an execution client only for the venues actually traded on. Venues that supply prices
/// alone have no account to connect to, and declaring them as execution venues would leave global
/// connectivity permanently degraded — see
/// [`EngineStateBuilder::execution_venues`](crate::engine::state::builder::EngineStateBuilder::execution_venues).
///
/// Contrast [`DataVenue`](rustrade_instrument::instrument::data_venue::DataVenue), which is for the
/// different case of **one** instrument priced on a venue other than the one it trades on.
///
/// # Type Parameters
/// * `ExchangeKey` - Type used to identify an exchange (defaults to [`ExchangeIndex`]).
/// * `InstrumentKey` - Type used to identify an instrument (defaults to [`InstrumentIndex`]).
pub trait AlgoStrategy<ExchangeKey = ExchangeIndex, InstrumentKey = InstrumentIndex> {
    /// State used by the `AlgoStrategy` to determine what open and cancel requests to generate.
    ///
    /// For a strategy run by the [`Engine`](crate::engine::Engine), this is the full `EngineState`
    /// of the trading system.
    ///
    /// eg/ `EngineState<DefaultGlobalData, DefaultInstrumentMarketData>`
    type State;

    /// Generate algorithmic orders based on current system `State`.
    fn generate_algo_orders(
        &self,
        state: &Self::State,
    ) -> (
        impl IntoIterator<Item = OrderRequestCancel<ExchangeKey, InstrumentKey>>,
        impl IntoIterator<Item = OrderRequestOpen<ExchangeKey, InstrumentKey>>,
    );

    /// Generate [`CancelThenOpen`] groups based on current system `State`: opens to send only
    /// once the cancels they depend on have been answered.
    ///
    /// For a pair that must not race, such as an exit behind the cancel of a resting stop, or a
    /// stop moved to a new trigger. The requests [`generate_algo_orders`](Self::generate_algo_orders)
    /// returns are sent concurrently, in no guaranteed order, and are not ordered against these
    /// groups.
    ///
    /// The [`Engine`](crate::engine::Engine) calls this right after
    /// [`generate_algo_orders`](Self::generate_algo_orders), with the same state, and passes the
    /// groups to [`RiskManager::check_cancel_then_opens`](crate::risk::RiskManager::check_cancel_then_opens).
    ///
    /// The default generates none.
    fn generate_cancel_then_opens(
        &self,
        state: &Self::State,
    ) -> impl IntoIterator<Item = CancelThenOpen<ExchangeKey, InstrumentKey>> {
        let _ = state;
        std::iter::empty()
    }

    /// Generate amends of working orders based on current system `State`: each names an order by
    /// its key and says what to change (see
    /// [`AmendPatch`](rustrade_execution::order::amend::AmendPatch)).
    ///
    /// The [`Engine`](crate::engine::Engine) calls this right after
    /// [`generate_cancel_then_opens`](Self::generate_cancel_then_opens), with the same state. It
    /// builds each amend from the order it tracks, refusing one it cannot send (see
    /// [`PreparesAmend`](crate::engine::state::PreparesAmend)), passes the rest to
    /// [`RiskManager::check_amends`](crate::risk::RiskManager::check_amends), and sends the
    /// approved ones after every other request.
    ///
    /// An amend is refused at the `ExecutionManager` when the exchange's client cannot make it
    /// ([`ExecutionClient::amend_support`](rustrade_execution::client::ExecutionClient::amend_support)).
    /// The refusal arrives as the amend's answer, and the order is unchanged: send a
    /// [`CancelThenOpen`] instead if a new order will do.
    ///
    /// The default generates none.
    fn generate_amends(
        &self,
        state: &Self::State,
    ) -> impl IntoIterator<Item = OrderAmend<ExchangeKey, InstrumentKey>> {
        let _ = state;
        std::iter::empty()
    }
}
