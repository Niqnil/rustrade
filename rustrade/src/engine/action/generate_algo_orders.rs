use crate::{
    engine::{
        Engine,
        action::send_requests::{SendCancelsAndOpensOutput, SendRequests, SendRequestsOutput},
        error::UnrecoverableEngineError,
        execution_tx::ExecutionTxMap,
        state::{MarketSnapshotSource, order::in_flight_recorder::InFlightRequestRecorder},
    },
    risk::{RiskApproved, RiskManager, RiskRefused},
    strategy::algo::AlgoStrategy,
};
use rustrade_execution::order::request::{
    OrderRequestCancel, OrderRequestOpen, RequestCancel, RequestOpen,
};
use rustrade_instrument::{exchange::ExchangeIndex, instrument::InstrumentIndex};
use rustrade_integration::collection::{none_one_or_many::NoneOneOrMany, one_or_many::OneOrMany};
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

/// Trait that defines how the [`Engine`] generates and sends algorithmic order requests.
///
/// # Type Parameters
/// * `ExchangeKey` - Type used to identify an exchange (defaults to [`ExchangeIndex`]).
/// * `InstrumentKey` - Type used to identify an instrument (defaults to [`InstrumentIndex`]).
pub trait GenerateAlgoOrders<ExchangeKey = ExchangeIndex, InstrumentKey = InstrumentIndex> {
    /// Generates and sends algorithmic order requests.
    ///
    /// Returns a [`GenerateAlgoOrdersOutput`] containing work done:
    /// - Generated orders that were approved by the [`RiskManager`] and sent for execution.
    /// - Generated cancel requests that were refused by the [`RiskManager`].
    /// - Generated open requests that were refused by the [`RiskManager`].
    fn generate_algo_orders(&mut self) -> GenerateAlgoOrdersOutput<ExchangeKey, InstrumentKey>;
}

impl<Clock, State, ExecutionTxs, Strategy, Risk, ExchangeKey, InstrumentKey>
    GenerateAlgoOrders<ExchangeKey, InstrumentKey>
    for Engine<Clock, State, ExecutionTxs, Strategy, Risk>
where
    State:
        InFlightRequestRecorder<ExchangeKey, InstrumentKey> + MarketSnapshotSource<InstrumentKey>,
    ExecutionTxs: ExecutionTxMap<ExchangeKey, InstrumentKey>,
    Strategy: AlgoStrategy<ExchangeKey, InstrumentKey, State = State>,
    Risk: RiskManager<ExchangeKey, InstrumentKey, State = State>,
    ExchangeKey: Debug + Clone,
    InstrumentKey: Debug + Clone,
{
    fn generate_algo_orders(&mut self) -> GenerateAlgoOrdersOutput<ExchangeKey, InstrumentKey> {
        // Generate orders
        let (cancels, opens) = self.strategy.generate_algo_orders(&self.state);

        // RiskApprove & RiskRefuse order requests
        let (cancels, opens, refused_cancels, refused_opens) =
            self.risk.check(&self.state, cancels, opens);

        // Send risk approved order requests.
        //
        // Each open is stamped with the market this state holds *now*, before it goes out. This is
        // the request's decision point, and the only instant with a well-defined position on a
        // simulated timeline -- see `MarketSnapshotSource`.
        let cancels = self.send_requests(cancels.into_iter().map(|RiskApproved(cancel)| cancel));
        let opens = self.send_requests(opens.into_iter().map(|RiskApproved(mut open)| {
            open.state.market = self.state.market_snapshot(&open.key.instrument);
            open
        }));

        // Collect remaining Iterators (so we can access &mut self)
        let cancels_refused = refused_cancels.into_iter().map(Box::new).collect();
        let opens_refused = refused_opens.into_iter().map(Box::new).collect();

        // Record in flight order requests
        self.state.record_in_flight_cancels(cancels.sent_iter());
        self.state.record_in_flight_opens(opens.sent_iter());

        GenerateAlgoOrdersOutput::new(cancels, opens, cancels_refused, opens_refused)
    }
}

/// Summary of work done by the [`Engine`] action [`GenerateAlgoOrders::generate_algo_orders`].
///
/// Contains the complete result of an algorithmic order generation action,
/// including successful and risk-refused orders, as well as any errors that occurred.
///
/// # Size
/// Every order/refusal payload is boxed inside its [`NoneOneOrMany`] field — via
/// [`SendRequestsOutput`] for the sent/errored requests and the `*_refused` fields below — so this
/// aggregate stays small (~144 B) instead of inlining six full `OrderEvent`s (~928 B, #195). That is
/// small enough for [`EngineOutput`]'s `AlgoOrders` variant to carry it **inline** (it sits under the
/// ~232 B `PositionExit` variant that floors `EngineOutput`), so no outer box is needed. `Box<T>` is
/// serde-transparent, so the wire format is unchanged.
///
/// [`EngineOutput`]: crate::engine::EngineOutput
/// [`SendRequestsOutput`]: crate::engine::action::send_requests::SendRequestsOutput
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize)]
pub struct GenerateAlgoOrdersOutput<ExchangeKey = ExchangeIndex, InstrumentKey = InstrumentIndex> {
    /// Generates orders that were approved by the [`RiskManager`] and sent for execution.
    pub cancels_and_opens: SendCancelsAndOpensOutput<ExchangeKey, InstrumentKey>,
    /// Generated cancel requests that were refused by the [`RiskManager`] (payload boxed — see the
    /// type's `# Size` note).
    pub cancels_refused:
        NoneOneOrMany<Box<RiskRefused<OrderRequestCancel<ExchangeKey, InstrumentKey>>>>,
    /// Generated open requests that were refused by the [`RiskManager`] (payload boxed — see the
    /// type's `# Size` note).
    pub opens_refused:
        NoneOneOrMany<Box<RiskRefused<OrderRequestOpen<ExchangeKey, InstrumentKey>>>>,
}

impl<ExchangeKey, InstrumentKey> GenerateAlgoOrdersOutput<ExchangeKey, InstrumentKey> {
    /// Construct a new [`GenerateAlgoOrdersOutput`].
    pub fn new(
        cancels: SendRequestsOutput<RequestCancel, ExchangeKey, InstrumentKey>,
        opens: SendRequestsOutput<RequestOpen, ExchangeKey, InstrumentKey>,
        cancels_refused: NoneOneOrMany<
            Box<RiskRefused<OrderRequestCancel<ExchangeKey, InstrumentKey>>>,
        >,
        opens_refused: NoneOneOrMany<
            Box<RiskRefused<OrderRequestOpen<ExchangeKey, InstrumentKey>>>,
        >,
    ) -> Self {
        Self {
            cancels_and_opens: SendCancelsAndOpensOutput::new(cancels, opens),
            cancels_refused,
            opens_refused,
        }
    }

    /// Returns `true` if no `GenerateAlgoOrdersOutput` is completely empty.
    pub fn is_empty(&self) -> bool {
        self.cancels_and_opens.is_empty()
            && self.cancels_refused.is_none()
            && self.opens_refused.is_none()
    }

    /// Returns any unrecoverable errors that occurred during order request generation & sending.
    pub fn unrecoverable_errors(&self) -> Option<OneOrMany<UnrecoverableEngineError>> {
        self.cancels_and_opens.unrecoverable_errors().into_option()
    }
}

impl<ExchangeKey, InstrumentKey> Default for GenerateAlgoOrdersOutput<ExchangeKey, InstrumentKey> {
    fn default() -> Self {
        Self {
            cancels_and_opens: SendCancelsAndOpensOutput::default(),
            cancels_refused: NoneOneOrMany::None,
            opens_refused: NoneOneOrMany::None,
        }
    }
}

#[cfg(test)]
mod size_guard {
    use super::*;

    /// Regression guard for #195: `GenerateAlgoOrdersOutput` boxes each order/refusal payload inside
    /// its six `NoneOneOrMany` fields, so the aggregate stays ~144 B instead of inlining six full
    /// `OrderEvent`s (~928 B). This bound (<= 160 B) catches a re-inlining of any of those payloads.
    #[test]
    fn generate_algo_orders_output_stays_small() {
        let size = std::mem::size_of::<GenerateAlgoOrdersOutput<ExchangeIndex, InstrumentIndex>>();
        assert!(
            size <= 160,
            "GenerateAlgoOrdersOutput grew to {size} B (expected <= 160): did a SendRequestsOutput \
             or *_refused payload lose its Box? Payloads must stay boxed to keep the aggregate small \
             (#195)."
        );
    }
}
