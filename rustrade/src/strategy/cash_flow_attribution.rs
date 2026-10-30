use crate::engine::state::{EngineState, instrument::filter::InstrumentFilter};
use rustrade_execution::cash_flow::CashFlow;
use rustrade_instrument::{
    asset::AssetIndex, exchange::ExchangeIndex, instrument::InstrumentIndex,
};

/// Strategy interface that decides which position, if any, a cash flow on the account as a whole
/// belongs to.
///
/// A venue can report a [`CashFlow`] that names no instrument, such as cross-margin interest. The
/// [`Engine`](crate::engine::Engine) applies such a flow to no position by itself, because no rule
/// is right for every account (see
/// [`EngineState::update_from_account`](crate::engine::state::EngineState::update_from_account),
/// § "A cash flow on the account as a whole"). This trait lets the strategy supply the rule.
///
/// The `Engine` asks it in one place, so a single policy serves live trading, a backtest and a
/// stepped simulation alike.
pub trait CashFlowAttributionStrategy {
    /// State used to decide the attribution.
    ///
    /// For Barter ecosystem strategies, this is the full `EngineState` of the trading system.
    ///
    /// eg/ `EngineState<DefaultGlobalData, DefaultInstrumentMarketData>`
    type State;

    /// Return the instrument whose open position the account-level `flow`, posted by `exchange`,
    /// belongs to, or `None` to leave the flow on the account.
    ///
    /// The `Engine` calls this for every [`CashFlow`] account event whose
    /// [`instrument`](CashFlow::instrument) is `None`, before the event updates the state or is
    /// recorded in the audit. A flow the venue already attributed never reaches it.
    ///
    /// On `Some(instrument)`, the `Engine` sets the flow's `instrument` and the flow goes to that
    /// instrument's open position's carry, exactly as a flow the venue attributed would. Since the
    /// audit records the attributed flow, a state replica applies it the same way.
    ///
    /// # Caller obligations
    /// Return an instrument that `state` tracks on `exchange`. The `Engine` ignores any other
    /// answer, with a warning, and leaves the flow on the account.
    ///
    /// [`route_to_base`] implements one common rule; delegate to it when it fits the account.
    fn attribute_cash_flow(
        &mut self,
        state: &Self::State,
        exchange: ExchangeIndex,
        flow: &CashFlow<AssetIndex, InstrumentIndex>,
    ) -> Option<InstrumentIndex>;
}

/// Attribute an account-level `flow` to the only instrument on `exchange` whose base asset is the
/// flow's asset and that holds an open position. `None` when no instrument, or more than one,
/// qualifies.
///
/// Opt-in: [`DefaultStrategy`](super::DefaultStrategy) leaves every account-level flow on the
/// account. This rule fits an account where interest on a borrowed base asset is charged on the
/// short of the only pair holding it. It does not fit every account:
/// - It attributes interest on a quote asset to nothing, unless some pair has that asset as its
///   base, since every pair quoted in it shares that interest.
/// - Cross-margin interest is charged on the account's whole net loan, which can include
///   borrowing that no open position caused. This rule still gives all of it to the position.
///
/// A [`CashFlowAttributionStrategy`] delegates to it in one line:
/// ```rust
/// use rustrade::{
///     engine::state::EngineState,
///     strategy::cash_flow_attribution::{CashFlowAttributionStrategy, route_to_base},
/// };
/// use rustrade_execution::cash_flow::CashFlow;
/// use rustrade_instrument::{
///     asset::AssetIndex, exchange::ExchangeIndex, instrument::InstrumentIndex,
/// };
/// # use std::marker::PhantomData;
///
/// struct MyStrategy<GlobalData, InstrumentData>(PhantomData<(GlobalData, InstrumentData)>);
///
/// impl<GlobalData, InstrumentData> CashFlowAttributionStrategy
///     for MyStrategy<GlobalData, InstrumentData>
/// {
///     type State = EngineState<GlobalData, InstrumentData>;
///
///     fn attribute_cash_flow(
///         &mut self,
///         state: &Self::State,
///         exchange: ExchangeIndex,
///         flow: &CashFlow<AssetIndex, InstrumentIndex>,
///     ) -> Option<InstrumentIndex> {
///         route_to_base(state, exchange, flow)
///     }
/// }
/// ```
pub fn route_to_base<GlobalData, InstrumentData>(
    state: &EngineState<GlobalData, InstrumentData>,
    exchange: ExchangeIndex,
    flow: &CashFlow<AssetIndex, InstrumentIndex>,
) -> Option<InstrumentIndex> {
    let filter = InstrumentFilter::exchanges([exchange]);
    let mut held = state.instruments.instruments(&filter).filter(|instrument| {
        instrument.instrument.underlying.base == flow.asset
            && !instrument.position.positions.is_empty()
    });
    match (held.next(), held.next()) {
        (Some(only), None) => Some(only.key),
        _ => None,
    }
}
