use crate::{
    engine::{
        Engine,
        action::send_requests::{
            SendCancelThenOpensOutput, SendCancelsAndOpensOutput, SendRequestsOutput,
        },
        clock::EngineClock,
        error::UnrecoverableEngineError,
        execution_tx::ExecutionTxMap,
        state::{
            MarketSnapshotSource, TracksInstrument, TracksOrder,
            order::in_flight_recorder::InFlightRequestRecorder,
        },
    },
    risk::{RiskApproved, RiskManager, RiskRefused},
    strategy::algo::AlgoStrategy,
};
use rustrade_execution::order::request::{
    CancelThenOpen, OrderRequestCancel, OrderRequestOpen, RequestCancel, RequestOpen,
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
    /// - Generated [`CancelThenOpen`] groups, sent or refused.
    fn generate_algo_orders(&mut self) -> GenerateAlgoOrdersOutput<ExchangeKey, InstrumentKey>;
}

impl<Clock, State, ExecutionTxs, Strategy, Risk, ExchangeKey, InstrumentKey>
    GenerateAlgoOrders<ExchangeKey, InstrumentKey>
    for Engine<Clock, State, ExecutionTxs, Strategy, Risk>
where
    Clock: EngineClock,
    State: InFlightRequestRecorder<ExchangeKey, InstrumentKey>
        + MarketSnapshotSource<InstrumentKey>
        + TracksInstrument<InstrumentKey>
        + TracksOrder<InstrumentKey>,
    ExecutionTxs: ExecutionTxMap<ExchangeKey, InstrumentKey>,
    Strategy: AlgoStrategy<ExchangeKey, InstrumentKey, State = State>,
    Risk: RiskManager<ExchangeKey, InstrumentKey, State = State>,
    ExchangeKey: Debug + Clone + PartialEq,
    InstrumentKey: Debug + Clone + PartialEq,
{
    fn generate_algo_orders(&mut self) -> GenerateAlgoOrdersOutput<ExchangeKey, InstrumentKey> {
        // Generate orders
        let (cancels, opens) = self.strategy.generate_algo_orders(&self.state);
        let groups = self.strategy.generate_cancel_then_opens(&self.state);

        // RiskApprove & RiskRefuse order requests
        let (cancels, opens, refused_cancels, refused_opens) =
            self.risk.check(&self.state, cancels, opens);
        let (groups, refused_groups) = self.risk.check_cancel_then_opens(&self.state, groups);

        // Collect every Iterator, since the strategy and risk manager may have borrowed the state
        // to build them, and sending records in-flight requests on it. An empty Vec does not
        // allocate.
        let cancels: Vec<_> = cancels
            .into_iter()
            .map(|RiskApproved(cancel)| cancel)
            .collect();
        let opens: Vec<_> = opens.into_iter().map(|RiskApproved(open)| open).collect();
        let cancels_refused = refused_cancels.into_iter().map(Box::new).collect();
        let opens_refused = refused_opens.into_iter().map(Box::new).collect();
        let groups: Vec<_> = groups
            .into_iter()
            .map(|RiskApproved(group)| group)
            .collect();
        let groups_refused: NoneOneOrMany<_> = refused_groups.into_iter().collect();

        // Send risk approved order requests.
        //
        // Each open is stamped with the market this state holds *now*, before it goes out. This is
        // the request's decision point, and the only instant with a well-defined position on a
        // simulated timeline -- see `MarketSnapshotSource`.
        // Groups go last, so an open of theirs that reuses the id of a plain open just sent is
        // refused as a duplicate.
        let cancels = self.send_cancel_requests(cancels);
        let opens = self.send_open_requests(opens);
        let groups = self.send_cancel_then_open_requests(groups);

        GenerateAlgoOrdersOutput::new(cancels, opens, cancels_refused, opens_refused)
            .with_cancel_then_opens(CancelThenOpensOutput::new(groups, groups_refused))
    }
}

/// Summary of work done by the [`Engine`] action [`GenerateAlgoOrders::generate_algo_orders`].
///
/// Contains the complete result of an algorithmic order generation action,
/// including successful and risk-refused orders, as well as any errors that occurred.
///
/// # Size
/// Every order/refusal payload is boxed inside its [`NoneOneOrMany`] field — via
/// [`SendRequestsOutput`] for the sent/errored requests and the `*_refused` fields below — and the
/// rare [`CancelThenOpen`] groups are boxed whole in `cancel_then_opens`, so this aggregate stays
/// small (~152 B) instead of inlining six full `OrderEvent`s (~928 B, #195). That is
/// small enough for [`EngineOutput`]'s `AlgoOrders` variant to carry it **inline** (it is the
/// variant that floors `EngineOutput`'s size now that `PositionExit` is boxed), so no outer box is
/// needed. `Box<T>` is serde-transparent, so the wire format is unchanged.
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
    /// Generated [`CancelThenOpen`] groups, sent or refused; `None` when there were none.
    ///
    /// Boxed whole, and `None` rather than an empty output, so the common output without a group
    /// stays small and allocation-free: see the type's `# Size` note.
    pub cancel_then_opens: Option<Box<CancelThenOpensOutput<ExchangeKey, InstrumentKey>>>,
}

/// The [`CancelThenOpen`] groups of one [`GenerateAlgoOrdersOutput`].
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize)]
pub struct CancelThenOpensOutput<ExchangeKey = ExchangeIndex, InstrumentKey = InstrumentIndex> {
    /// Groups approved by the [`RiskManager`], sent or refused by the [`Engine`].
    pub sent: SendCancelThenOpensOutput<ExchangeKey, InstrumentKey>,
    /// Groups refused by the [`RiskManager`].
    pub refused: NoneOneOrMany<RiskRefused<CancelThenOpen<ExchangeKey, InstrumentKey>>>,
}

impl<ExchangeKey, InstrumentKey> CancelThenOpensOutput<ExchangeKey, InstrumentKey> {
    /// Construct a new [`CancelThenOpensOutput`].
    pub fn new(
        sent: SendCancelThenOpensOutput<ExchangeKey, InstrumentKey>,
        refused: NoneOneOrMany<RiskRefused<CancelThenOpen<ExchangeKey, InstrumentKey>>>,
    ) -> Self {
        Self { sent, refused }
    }

    /// Returns `true` if no group was sent or refused.
    pub fn is_empty(&self) -> bool {
        self.sent.is_empty() && self.refused.is_none()
    }
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
            cancel_then_opens: None,
        }
    }

    /// Set the [`CancelThenOpen`] groups, leaving [`cancel_then_opens`](Self::cancel_then_opens)
    /// `None` when `groups` is empty.
    #[must_use]
    pub fn with_cancel_then_opens(
        self,
        groups: CancelThenOpensOutput<ExchangeKey, InstrumentKey>,
    ) -> Self {
        Self {
            cancel_then_opens: (!groups.is_empty()).then(|| Box::new(groups)),
            ..self
        }
    }

    /// Returns `true` if no `GenerateAlgoOrdersOutput` is completely empty.
    pub fn is_empty(&self) -> bool {
        self.cancels_and_opens.is_empty()
            && self.cancels_refused.is_none()
            && self.opens_refused.is_none()
            && self
                .cancel_then_opens
                .as_ref()
                .is_none_or(|groups| groups.is_empty())
    }

    /// Returns any unrecoverable errors that occurred during order request generation & sending.
    pub fn unrecoverable_errors(&self) -> Option<OneOrMany<UnrecoverableEngineError>> {
        let groups = self
            .cancel_then_opens
            .as_ref()
            .map(|groups| groups.sent.unrecoverable_errors())
            .unwrap_or_default();
        self.cancels_and_opens
            .unrecoverable_errors()
            .extend(groups)
            .into_option()
    }
}

impl<ExchangeKey, InstrumentKey> Default for GenerateAlgoOrdersOutput<ExchangeKey, InstrumentKey> {
    fn default() -> Self {
        Self {
            cancels_and_opens: SendCancelsAndOpensOutput::default(),
            cancels_refused: NoneOneOrMany::None,
            opens_refused: NoneOneOrMany::None,
            cancel_then_opens: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::error::EngineError;
    use rustrade_execution::order::{
        OrderKey, OrderKind, TimeInForce,
        id::{ClientOrderId, StrategyId},
    };
    use rustrade_instrument::Side;

    fn group() -> CancelThenOpen {
        let key = |cid: &str| OrderKey {
            exchange: ExchangeIndex(0),
            instrument: InstrumentIndex(0),
            strategy: StrategyId::new("test"),
            cid: ClientOrderId::new(cid),
        };
        CancelThenOpen::new(
            vec![OrderRequestCancel {
                key: key("stop"),
                state: RequestCancel { id: None },
            }],
            vec![OrderRequestOpen {
                key: key("exit"),
                state: RequestOpen::new(
                    Side::Sell,
                    None,
                    rust_decimal::Decimal::ONE,
                    OrderKind::Market,
                    TimeInForce::ImmediateOrCancel,
                ),
            }],
        )
    }

    fn sent() -> SendCancelThenOpensOutput {
        SendCancelThenOpensOutput::new(NoneOneOrMany::One(Box::new(group())), NoneOneOrMany::None)
    }

    fn refused() -> NoneOneOrMany<RiskRefused<CancelThenOpen>> {
        NoneOneOrMany::One(RiskRefused::new(group(), "no"))
    }

    /// Groups alone, sent or refused, make an output that is not empty; no groups leave
    /// `cancel_then_opens` `None`, and an empty one set by hand counts as none.
    #[test]
    fn groups_alone_make_an_output_that_is_not_empty() {
        assert!(GenerateAlgoOrdersOutput::<ExchangeIndex, InstrumentIndex>::default().is_empty());

        let with = |groups| GenerateAlgoOrdersOutput::default().with_cancel_then_opens(groups);
        let only_sent = with(CancelThenOpensOutput::new(sent(), NoneOneOrMany::None));
        assert!(!only_sent.is_empty());
        let only_refused = with(CancelThenOpensOutput::new(
            SendCancelThenOpensOutput::default(),
            refused(),
        ));
        assert!(!only_refused.is_empty());

        let none = with(CancelThenOpensOutput::new(
            SendCancelThenOpensOutput::default(),
            NoneOneOrMany::None,
        ));
        assert!(none.cancel_then_opens.is_none());
        let by_hand: GenerateAlgoOrdersOutput = GenerateAlgoOrdersOutput {
            cancel_then_opens: Some(Box::new(CancelThenOpensOutput::new(
                SendCancelThenOpensOutput::default(),
                NoneOneOrMany::None,
            ))),
            ..GenerateAlgoOrdersOutput::default()
        };
        assert!(by_hand.is_empty());
    }

    /// A group that failed to send for a terminated channel is an unrecoverable error of the whole
    /// output; one refused for a recoverable reason is not.
    #[test]
    fn a_groups_unrecoverable_send_error_is_reported() {
        let terminated = UnrecoverableEngineError::ExecutionChannelTerminated("gone".to_string());
        let errors = SendCancelThenOpensOutput::new(
            NoneOneOrMany::None,
            NoneOneOrMany::Many(vec![
                Box::new((group(), EngineError::Unrecoverable(terminated.clone()))),
                Box::new((
                    group(),
                    EngineError::Recoverable(
                        crate::engine::error::RecoverableEngineError::DuplicateClientOrderId(
                            "exit".to_string(),
                        ),
                    ),
                )),
            ]),
        );
        assert!(!errors.is_empty());
        assert!(sent().unrecoverable_errors().is_none());

        let output = GenerateAlgoOrdersOutput::default()
            .with_cancel_then_opens(CancelThenOpensOutput::new(errors, NoneOneOrMany::None));
        assert_eq!(
            output.unrecoverable_errors(),
            Some(OneOrMany::One(terminated))
        );
    }
}

#[cfg(test)]
mod size_guard {
    use super::*;

    /// Regression guard for #195: `GenerateAlgoOrdersOutput` boxes each order/refusal payload inside
    /// its six `NoneOneOrMany` fields, and its groups whole, so the aggregate stays ~152 B instead of
    /// inlining six full `OrderEvent`s (~928 B). This bound (<= 160 B) catches a re-inlining of any
    /// of those payloads.
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
