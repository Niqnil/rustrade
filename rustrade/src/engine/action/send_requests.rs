use crate::{
    engine::{
        Engine,
        error::{EngineError, RecoverableEngineError, UnrecoverableEngineError},
        execution_tx::ExecutionTxMap,
        state::{
            MarketSnapshotSource, TracksInstrument,
            order::in_flight_recorder::InFlightRequestRecorder,
        },
    },
    execution::request::ExecutionRequest,
};
use derive_more::Constructor;
use itertools::Itertools;
use rustrade_execution::order::{
    OrderEvent,
    request::{OrderRequestCancel, OrderRequestOpen, RequestCancel, RequestOpen},
};
use rustrade_instrument::{exchange::ExchangeIndex, instrument::InstrumentIndex};
use rustrade_integration::{
    Unrecoverable, channel::Tx, collection::none_one_or_many::NoneOneOrMany,
};
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use tracing::{error, warn};

impl<Clock, State, ExecutionTxs, Strategy, Risk>
    Engine<Clock, State, ExecutionTxs, Strategy, Risk>
{
    /// Send open requests on behalf of an `Engine` action: reject any for an untracked instrument,
    /// stamp the rest with the market the state holds now (see [`MarketSnapshotSource`]), send
    /// them, and record those sent as in flight.
    pub(crate) fn send_open_requests<ExchangeKey, InstrumentKey>(
        &mut self,
        requests: impl IntoIterator<Item = OrderRequestOpen<ExchangeKey, InstrumentKey>>,
    ) -> SendRequestsOutput<RequestOpen, ExchangeKey, InstrumentKey>
    where
        State: TracksInstrument<InstrumentKey>
            + MarketSnapshotSource<InstrumentKey>
            + InFlightRequestRecorder<ExchangeKey, InstrumentKey>,
        ExecutionTxs: ExecutionTxMap<ExchangeKey, InstrumentKey>,
        ExchangeKey: Debug + Clone,
        InstrumentKey: Debug + Clone,
    {
        let output = self.send_tracked_requests(requests, |state, open| {
            open.state.market = state.market_snapshot(&open.key.instrument);
        });
        self.state.record_in_flight_opens(output.sent_iter());
        output
    }

    /// Send cancel requests on behalf of an `Engine` action: reject any for an untracked
    /// instrument, send the rest, and record those sent as in flight.
    pub(crate) fn send_cancel_requests<ExchangeKey, InstrumentKey>(
        &mut self,
        requests: impl IntoIterator<Item = OrderRequestCancel<ExchangeKey, InstrumentKey>>,
    ) -> SendRequestsOutput<RequestCancel, ExchangeKey, InstrumentKey>
    where
        State:
            TracksInstrument<InstrumentKey> + InFlightRequestRecorder<ExchangeKey, InstrumentKey>,
        ExecutionTxs: ExecutionTxMap<ExchangeKey, InstrumentKey>,
        ExchangeKey: Debug + Clone,
        InstrumentKey: Debug + Clone,
    {
        let output = self.send_tracked_requests(requests, |_, _| {});
        self.state.record_in_flight_cancels(output.sent_iter());
        output
    }

    /// Send each request whose instrument the state tracks, after `prepare` has adjusted it, and
    /// reject the others unsent as [`RecoverableEngineError::UnknownInstrument`]. Errors keep the
    /// input order, whichever step produced them.
    fn send_tracked_requests<Kind, ExchangeKey, InstrumentKey>(
        &self,
        requests: impl IntoIterator<Item = OrderEvent<Kind, ExchangeKey, InstrumentKey>>,
        prepare: impl Fn(&State, &mut OrderEvent<Kind, ExchangeKey, InstrumentKey>),
    ) -> SendRequestsOutput<Kind, ExchangeKey, InstrumentKey>
    where
        State: TracksInstrument<InstrumentKey>,
        ExecutionTxs: ExecutionTxMap<ExchangeKey, InstrumentKey>,
        Kind: Debug + Clone,
        ExchangeKey: Debug + Clone,
        InstrumentKey: Debug + Clone,
        ExecutionRequest<ExchangeKey, InstrumentKey>:
            From<OrderEvent<Kind, ExchangeKey, InstrumentKey>>,
    {
        let (sent, errors): (Vec<_>, Vec<_>) = requests
            .into_iter()
            .map(|mut request| {
                if !self.state.tracks_instrument(&request.key.instrument) {
                    // The whole request is returned in `errors`; the log names what finds it.
                    warn!(
                        instrument = ?request.key.instrument,
                        strategy = %request.key.strategy,
                        cid = %request.key.cid,
                        "order request for an instrument the Engine does not track -- rejected, \
                         not sent"
                    );
                    let error =
                        EngineError::Recoverable(RecoverableEngineError::UnknownInstrument(
                            format!("{:?}", request.key.instrument),
                        ));
                    return Err(Box::new((request, error)));
                }

                prepare(&self.state, &mut request);
                match self.send_request(&request) {
                    Ok(()) => Ok(Box::new(request)),
                    Err(error) => Err(Box::new((request, error))),
                }
            })
            .partition_result();

        SendRequestsOutput::new(sent.into_iter().collect(), errors.into_iter().collect())
    }

    /// Send one request down its exchange's execution channel, exactly as given.
    ///
    /// Private: it neither checks the instrument is tracked nor records the request as in flight.
    /// Every request the `Engine` sends goes through [`Self::send_tracked_requests`], which does
    /// the first, and its callers, which do the second.
    fn send_request<Kind, ExchangeKey, InstrumentKey>(
        &self,
        request: &OrderEvent<Kind, ExchangeKey, InstrumentKey>,
    ) -> Result<(), EngineError>
    where
        ExecutionTxs: ExecutionTxMap<ExchangeKey, InstrumentKey>,
        Kind: Debug + Clone,
        ExchangeKey: Debug + Clone,
        InstrumentKey: Debug + Clone,
        ExecutionRequest<ExchangeKey, InstrumentKey>:
            From<OrderEvent<Kind, ExchangeKey, InstrumentKey>>,
    {
        match self
            .execution_txs
            .find(&request.key.exchange)?
            .send(ExecutionRequest::from(request.clone()))
        {
            Ok(()) => Ok(()),
            Err(error) if error.is_unrecoverable() => {
                error!(
                    exchange = ?request.key.exchange,
                    ?request,
                    ?error,
                    "failed to send ExecutionRequest due to terminated channel"
                );
                Err(EngineError::Unrecoverable(
                    UnrecoverableEngineError::ExecutionChannelTerminated(format!(
                        "{:?} execution channel terminated: {:?}",
                        request.key.exchange, error
                    )),
                ))
            }
            Err(error) => {
                error!(
                    exchange = ?request.key.exchange,
                    ?request,
                    ?error,
                    "failed to send ExecutionRequest due to unhealthy channel"
                );
                Err(EngineError::Recoverable(
                    RecoverableEngineError::ExecutionChannelUnhealthy(format!(
                        "{:?} execution channel unhealthy: {:?}",
                        request.key.exchange, error
                    )),
                ))
            }
        }
    }
}

/// Summary of cancel and open order requests sent by the [`Engine`] to the `ExecutionManager`.
#[derive(
    Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize, Constructor,
)]
pub struct SendCancelsAndOpensOutput<ExchangeKey = ExchangeIndex, InstrumentKey = InstrumentIndex> {
    /// Cancel order requests that were sent for execution.
    pub cancels: SendRequestsOutput<RequestCancel, ExchangeKey, InstrumentKey>,
    /// Open order requests that were sent for execution.
    pub opens: SendRequestsOutput<RequestOpen, ExchangeKey, InstrumentKey>,
}

impl<ExchangeKey, InstrumentKey> SendCancelsAndOpensOutput<ExchangeKey, InstrumentKey> {
    /// Returns `true` if no `SendCancelsAndOpensOutput` is completely empty.
    pub fn is_empty(&self) -> bool {
        self.cancels.is_empty() && self.opens.is_empty()
    }

    /// Returns any unrecoverable errors that occurred during order request sending.
    pub fn unrecoverable_errors(&self) -> NoneOneOrMany<UnrecoverableEngineError> {
        self.cancels
            .unrecoverable_errors()
            .extend(self.opens.unrecoverable_errors())
    }
}

impl<ExchangeKey, InstrumentKey> Default for SendCancelsAndOpensOutput<ExchangeKey, InstrumentKey> {
    fn default() -> Self {
        Self {
            cancels: SendRequestsOutput::default(),
            opens: SendRequestsOutput::default(),
        }
    }
}

/// Summary of order requests (cancel _or_ open) sent by the [`Engine`] to the `ExecutionManager`.
///
/// # Size
/// Each [`OrderEvent`] payload is boxed inside its [`NoneOneOrMany`] field. An unboxed
/// `OrderEvent` (~184 B for an open) inlined into [`NoneOneOrMany::One`] is the root of the size of
/// the aggregates that embed this type
/// ([`GenerateAlgoOrdersOutput`](super::generate_algo_orders::GenerateAlgoOrdersOutput),
/// [`SendCancelsAndOpensOutput`], [`ActionOutput`](super::ActionOutput)); boxing keeps each field to
/// a pointer (#195). `Box<T>` is serde-transparent, so the wire format is unchanged.
///
/// The box is deliberately at the *item* level (`NoneOneOrMany<Box<T>>`), not the *field* level
/// (`Box<NoneOneOrMany<T>>`). Item-level boxing keeps the empty/no-order case allocation-free —
/// [`NoneOneOrMany::None`] holds no box, so the common per-tick path that constructs an empty output
/// (see [`is_empty`](Self::is_empty)) never touches the heap. A field-level box would instead
/// allocate one heap block on *every* output, empty or not, regressing that hot path; its only edge
/// is one fewer allocation on the rare multi-order (`Many`) tick, which is dominated by the
/// per-order `request.clone()` + channel send already on that path. Item-level boxing is the right
/// trade for a type whose empty case is the overwhelmingly common one.
#[derive(
    Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize, Constructor,
)]
pub struct SendRequestsOutput<Kind, ExchangeKey = ExchangeIndex, InstrumentKey = InstrumentIndex> {
    /// Order requests successfully sent for execution (payload boxed — see the type's `# Size` note).
    pub sent: NoneOneOrMany<Box<OrderEvent<Kind, ExchangeKey, InstrumentKey>>>,
    /// Order requests that failed to send, each paired with the [`EngineError`] that occurred
    /// (payload boxed — see the type's `# Size` note).
    pub errors: NoneOneOrMany<Box<(OrderEvent<Kind, ExchangeKey, InstrumentKey>, EngineError)>>,
}

impl<Kind, ExchangeKey, InstrumentKey> SendRequestsOutput<Kind, ExchangeKey, InstrumentKey> {
    /// Returns `true` if no `SendRequestsOutput` is completely empty.
    pub fn is_empty(&self) -> bool {
        self.sent.is_none() && self.errors.is_none()
    }

    /// Iterates the successfully-sent order requests, dereferencing through the boxed payload (see
    /// the type's `# Size` note) so callers receive `&OrderEvent` rather than `&Box<OrderEvent>`.
    pub fn sent_iter(&self) -> impl Iterator<Item = &OrderEvent<Kind, ExchangeKey, InstrumentKey>> {
        self.sent.iter().map(|order| &**order)
    }

    /// Returns any unrecoverable errors that occurred during order request sending.
    pub fn unrecoverable_errors(&self) -> NoneOneOrMany<UnrecoverableEngineError> {
        self.errors
            .iter()
            .filter_map(|entry| {
                // `entry` is `&Box<(OrderEvent, EngineError)>` (payload boxed, see `# Size`);
                // deref through the box to destructure the tuple.
                let (_, error) = &**entry;
                match error {
                    EngineError::Unrecoverable(error) => Some(error.clone()),
                    _ => None,
                }
            })
            .collect()
    }
}

impl<Kind, ExchangeKey, InstrumentKey> Default
    for SendRequestsOutput<Kind, ExchangeKey, InstrumentKey>
{
    fn default() -> Self {
        Self {
            sent: NoneOneOrMany::default(),
            errors: NoneOneOrMany::default(),
        }
    }
}
