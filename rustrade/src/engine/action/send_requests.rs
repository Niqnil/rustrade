use crate::{
    engine::{
        Engine,
        clock::EngineClock,
        error::{EngineError, RecoverableEngineError, UnrecoverableEngineError},
        execution_tx::ExecutionTxMap,
        state::{
            MarketSnapshotSource, PreparesAmend, TracksInstrument, TracksOrder,
            order::in_flight_recorder::InFlightRequestRecorder,
        },
    },
    execution::request::ExecutionRequest,
};
use derive_more::Constructor;
use itertools::Itertools;
use rustrade_execution::{
    error::AmendRefusal,
    order::{
        OrderEvent,
        id::ClientOrderId,
        request::{
            CancelThenOpen, OrderAmend, OrderRequestAmend, OrderRequestCancel, OrderRequestOpen,
            RequestCancel, RequestOpen,
        },
    },
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
    /// and any under a client order id already in use (see [`TracksOrder`]), stamp the rest with
    /// the market the state holds now (see [`MarketSnapshotSource`]), send them, and record those
    /// sent as in flight.
    pub(crate) fn send_open_requests<ExchangeKey, InstrumentKey>(
        &mut self,
        requests: impl IntoIterator<Item = OrderRequestOpen<ExchangeKey, InstrumentKey>>,
    ) -> SendRequestsOutput<RequestOpen, ExchangeKey, InstrumentKey>
    where
        Clock: EngineClock,
        State: TracksInstrument<InstrumentKey>
            + TracksOrder<InstrumentKey>
            + MarketSnapshotSource<InstrumentKey>
            + InFlightRequestRecorder<ExchangeKey, InstrumentKey>,
        ExecutionTxs: ExecutionTxMap<ExchangeKey, InstrumentKey>,
        ExchangeKey: Debug + Clone,
        InstrumentKey: Debug + Clone + PartialEq,
    {
        // The ids this batch has already claimed: the state records none of them until the whole
        // batch is sent. A batch is a handful of requests, so a scan costs about what hashing would.
        let mut claimed: Vec<(InstrumentKey, ClientOrderId)> = Vec::new();
        let output = self.send_tracked_requests(requests, |state, open| {
            let (instrument, cid) = (&open.key.instrument, &open.key.cid);
            if state.tracks_order(instrument, cid)
                || claimed.iter().any(|(i, c)| i == instrument && c == cid)
            {
                // The whole request is returned in `errors`; the log names what finds it.
                warn!(
                    instrument = ?instrument,
                    strategy = %open.key.strategy,
                    cid = %cid,
                    "open request under a client order id the Engine already tracks an order \
                     under -- rejected, not sent"
                );
                return Err(EngineError::Recoverable(
                    RecoverableEngineError::DuplicateClientOrderId(cid.to_string()),
                ));
            }
            claimed.push((instrument.clone(), cid.clone()));
            open.state.market = state.market_snapshot(instrument);
            Ok(())
        });
        if !output.sent.is_none() {
            let time_sent = self.clock.time();
            self.state
                .record_in_flight_opens(output.sent_iter(), time_sent);
            let clock = &self.clock;
            self.in_flight.on_sent(time_sent, || clock.instant());
        }
        output
    }

    /// Send cancel requests on behalf of an `Engine` action: reject any for an untracked
    /// instrument, send the rest, and record those sent as in flight.
    pub(crate) fn send_cancel_requests<ExchangeKey, InstrumentKey>(
        &mut self,
        requests: impl IntoIterator<Item = OrderRequestCancel<ExchangeKey, InstrumentKey>>,
    ) -> SendRequestsOutput<RequestCancel, ExchangeKey, InstrumentKey>
    where
        Clock: EngineClock,
        State:
            TracksInstrument<InstrumentKey> + InFlightRequestRecorder<ExchangeKey, InstrumentKey>,
        ExecutionTxs: ExecutionTxMap<ExchangeKey, InstrumentKey>,
        ExchangeKey: Debug + Clone,
        InstrumentKey: Debug + Clone,
    {
        let output = self.send_tracked_requests(requests, |_, _| Ok(()));
        if !output.sent.is_none() {
            let time_sent = self.clock.time();
            self.state
                .record_in_flight_cancels(output.sent_iter(), time_sent);
            let clock = &self.clock;
            self.in_flight.on_sent(time_sent, || clock.instant());
        }
        output
    }

    /// Send [`CancelThenOpen`] groups on behalf of an `Engine` action, each whole or not at all.
    ///
    /// A group is refused unsent if it is not valid (see [`CancelThenOpen::exchange`]), if a leg
    /// names an instrument the state does not track, or if an open names a client order id already
    /// in use, by an order the state tracks or by an open earlier in this call (see
    /// [`TracksOrder`]). Each open of the rest is stamped with the market the state holds now (see
    /// [`MarketSnapshotSource`]), and the group is sent to its exchange as one
    /// [`ExecutionRequest::CancelThenOpen`]. Every leg of a sent group is recorded as in flight
    /// now, its opens included, though they leave only once the cancels are answered: the
    /// in-flight deadline allows for that wait (see
    /// [`InFlightDeadlines::deadline_for`](crate::engine::in_flight::InFlightDeadlines::deadline_for)).
    pub(crate) fn send_cancel_then_open_requests<ExchangeKey, InstrumentKey>(
        &mut self,
        groups: impl IntoIterator<Item = CancelThenOpen<ExchangeKey, InstrumentKey>>,
    ) -> SendCancelThenOpensOutput<ExchangeKey, InstrumentKey>
    where
        Clock: EngineClock,
        State: TracksInstrument<InstrumentKey>
            + TracksOrder<InstrumentKey>
            + MarketSnapshotSource<InstrumentKey>
            + InFlightRequestRecorder<ExchangeKey, InstrumentKey>,
        ExecutionTxs: ExecutionTxMap<ExchangeKey, InstrumentKey>,
        ExchangeKey: Debug + Clone + PartialEq,
        InstrumentKey: Debug + Clone + PartialEq,
    {
        // As in `send_open_requests`: the ids the groups sent so far have claimed.
        let mut claimed: Vec<(InstrumentKey, ClientOrderId)> = Vec::new();
        let (sent, errors): (Vec<_>, Vec<_>) = groups
            .into_iter()
            .map(|mut group| {
                let exchange = match self.prepare_cancel_then_open(&mut group, &claimed) {
                    Ok(exchange) => exchange,
                    Err(error) => return Err(Box::new((group, error))),
                };
                claimed.extend(
                    group
                        .opens
                        .iter()
                        .map(|open| (open.key.instrument.clone(), open.key.cid.clone())),
                );
                match self.send_execution_request(
                    &exchange,
                    ExecutionRequest::CancelThenOpen(group.clone()),
                    &group,
                ) {
                    Ok(()) => Ok(Box::new(group)),
                    Err(error) => Err(Box::new((group, error))),
                }
            })
            .partition_result();
        let output = SendCancelThenOpensOutput::new(
            sent.into_iter().collect(),
            errors.into_iter().collect(),
        );

        if !output.sent.is_none() {
            let time_sent = self.clock.time();
            self.state.record_in_flight_cancels(
                output.sent_iter().flat_map(|group| group.cancels.iter()),
                time_sent,
            );
            self.state.record_in_flight_opens(
                output.sent_iter().flat_map(|group| group.opens.iter()),
                time_sent,
            );
            let clock = &self.clock;
            self.in_flight.on_sent(time_sent, || clock.instant());
        }
        output
    }

    /// Check `group` can be sent, given the open ids `claimed` earlier in the same call, and stamp
    /// its opens with the market the state holds now. Returns the exchange the group is for.
    /// Send amends, each built from the order the state tracks under its key
    /// ([`PreparesAmend`]), and record those sent as in flight.
    ///
    /// An amend is refused, unsent, with [`RecoverableEngineError::AmendRefused`] when the state
    /// refuses to prepare it, or when an earlier amend in the batch names the same order: one
    /// request at a time per order.
    pub(crate) fn send_amend_requests<ExchangeKey, InstrumentKey>(
        &mut self,
        amends: impl IntoIterator<Item = OrderAmend<ExchangeKey, InstrumentKey>>,
    ) -> SendAmendsOutput<ExchangeKey, InstrumentKey>
    where
        Clock: EngineClock,
        State: PreparesAmend<ExchangeKey, InstrumentKey>
            + InFlightRequestRecorder<ExchangeKey, InstrumentKey>,
        ExecutionTxs: ExecutionTxMap<ExchangeKey, InstrumentKey>,
        ExchangeKey: Debug + Clone,
        InstrumentKey: Debug + Clone + PartialEq,
    {
        // The orders this batch has already amended: the state records none until it is sent.
        let mut claimed: Vec<(InstrumentKey, ClientOrderId)> = Vec::new();
        let (sent, errors): (Vec<_>, Vec<_>) = amends
            .into_iter()
            .map(|amend| {
                let (instrument, cid) = (&amend.key.instrument, &amend.key.cid);
                let prepared = if claimed.iter().any(|(i, c)| i == instrument && c == cid) {
                    Err(AmendRefusal::RequestInFlight)
                } else {
                    self.state.prepare_amend(&amend)
                };
                let request = match prepared {
                    Ok(request) => request,
                    Err(refusal) => {
                        warn!(
                            instrument = ?instrument,
                            strategy = %amend.key.strategy,
                            cid = %cid,
                            %refusal,
                            "amend refused by the Engine -- not sent"
                        );
                        return Err(Box::new((
                            amend,
                            EngineError::Recoverable(RecoverableEngineError::AmendRefused(refusal)),
                        )));
                    }
                };
                claimed.push((instrument.clone(), cid.clone()));
                let request = OrderRequestAmend {
                    key: amend.key.clone(),
                    state: request,
                };
                match self.send_execution_request(
                    &request.key.exchange,
                    ExecutionRequest::Amend(request.clone()),
                    &request,
                ) {
                    Ok(()) => Ok(Box::new(request)),
                    Err(error) => Err(Box::new((amend, error))),
                }
            })
            .partition_result();

        let output =
            SendAmendsOutput::new(sent.into_iter().collect(), errors.into_iter().collect());
        if !output.sent.is_none() {
            let time_sent = self.clock.time();
            for request in output.sent_iter() {
                self.state.record_in_flight_amend(request, time_sent);
            }
            let clock = &self.clock;
            self.in_flight.on_sent(time_sent, || clock.instant());
        }
        output
    }

    fn prepare_cancel_then_open<ExchangeKey, InstrumentKey>(
        &self,
        group: &mut CancelThenOpen<ExchangeKey, InstrumentKey>,
        claimed: &[(InstrumentKey, ClientOrderId)],
    ) -> Result<ExchangeKey, EngineError>
    where
        State: TracksInstrument<InstrumentKey>
            + TracksOrder<InstrumentKey>
            + MarketSnapshotSource<InstrumentKey>,
        ExchangeKey: Debug + Clone + PartialEq,
        InstrumentKey: Debug + Clone + PartialEq,
    {
        let exchange = group.exchange().cloned().map_err(|error| {
            warn!(
                ?error,
                ?group,
                "invalid CancelThenOpen group -- rejected, not sent"
            );
            EngineError::Recoverable(RecoverableEngineError::InvalidCancelThenOpen(error))
        })?;

        let keys = group
            .cancels
            .iter()
            .map(|cancel| &cancel.key)
            .chain(group.opens.iter().map(|open| &open.key));
        for key in keys {
            if !self.state.tracks_instrument(&key.instrument) {
                warn!(
                    instrument = ?key.instrument,
                    strategy = %key.strategy,
                    cid = %key.cid,
                    "CancelThenOpen group with a leg for an instrument the Engine does not track \
                     -- rejected, not sent"
                );
                return Err(EngineError::Recoverable(
                    RecoverableEngineError::UnknownInstrument(format!("{:?}", key.instrument)),
                ));
            }
        }

        for (index, open) in group.opens.iter().enumerate() {
            let (instrument, cid) = (&open.key.instrument, &open.key.cid);
            let earlier_in_group = group.opens[..index]
                .iter()
                .any(|other| other.key.instrument == *instrument && other.key.cid == *cid);
            if self.state.tracks_order(instrument, cid)
                || earlier_in_group
                || claimed.iter().any(|(i, c)| i == instrument && c == cid)
            {
                warn!(
                    instrument = ?instrument,
                    strategy = %open.key.strategy,
                    cid = %cid,
                    "CancelThenOpen group with an open under a client order id the Engine already \
                     tracks an order under -- rejected, not sent"
                );
                return Err(EngineError::Recoverable(
                    RecoverableEngineError::DuplicateClientOrderId(cid.to_string()),
                ));
            }
        }

        for open in &mut group.opens {
            open.state.market = self.state.market_snapshot(&open.key.instrument);
        }
        Ok(exchange)
    }

    /// Send each request whose instrument the state tracks and that `prepare` accepts, after
    /// `prepare` has adjusted it. Reject the others unsent: one for an untracked instrument as
    /// [`RecoverableEngineError::UnknownInstrument`], before `prepare` sees it, and one `prepare`
    /// refuses with the error it returns. Errors keep the input order, whichever step produced
    /// them.
    fn send_tracked_requests<Kind, ExchangeKey, InstrumentKey>(
        &self,
        requests: impl IntoIterator<Item = OrderEvent<Kind, ExchangeKey, InstrumentKey>>,
        mut prepare: impl FnMut(
            &State,
            &mut OrderEvent<Kind, ExchangeKey, InstrumentKey>,
        ) -> Result<(), EngineError>,
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

                if let Err(error) = prepare(&self.state, &mut request) {
                    return Err(Box::new((request, error)));
                }
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
        self.send_execution_request(
            &request.key.exchange,
            ExecutionRequest::from(request.clone()),
            request,
        )
    }

    /// Send `request` down the execution channel of `exchange`, exactly as given, logging
    /// `original`, what it was built from, if the send fails.
    ///
    /// Private, for the same reason as [`Self::send_request`]: its callers check and record.
    fn send_execution_request<ExchangeKey, InstrumentKey>(
        &self,
        exchange: &ExchangeKey,
        request: ExecutionRequest<ExchangeKey, InstrumentKey>,
        original: &dyn Debug,
    ) -> Result<(), EngineError>
    where
        ExecutionTxs: ExecutionTxMap<ExchangeKey, InstrumentKey>,
        ExchangeKey: Debug + Clone,
        InstrumentKey: Debug + Clone,
    {
        match self.execution_txs.find(exchange)?.send(request) {
            Ok(()) => Ok(()),
            Err(error) if error.is_unrecoverable() => {
                error!(
                    ?exchange,
                    request = ?original,
                    ?error,
                    "failed to send ExecutionRequest due to terminated channel"
                );
                Err(EngineError::Unrecoverable(
                    UnrecoverableEngineError::ExecutionChannelTerminated(format!(
                        "{exchange:?} execution channel terminated: {error:?}"
                    )),
                ))
            }
            Err(error) => {
                error!(
                    ?exchange,
                    request = ?original,
                    ?error,
                    "failed to send ExecutionRequest due to unhealthy channel"
                );
                Err(EngineError::Recoverable(
                    RecoverableEngineError::ExecutionChannelUnhealthy(format!(
                        "{exchange:?} execution channel unhealthy: {error:?}"
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

/// Summary of the [`CancelThenOpen`] groups the [`Engine`] sent to the `ExecutionManager`, or
/// refused unsent.
///
/// Each group is boxed inside its [`NoneOneOrMany`] field, as in [`SendRequestsOutput`].
#[derive(
    Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize, Constructor,
)]
pub struct SendCancelThenOpensOutput<ExchangeKey = ExchangeIndex, InstrumentKey = InstrumentIndex> {
    /// Groups sent for execution, whole.
    pub sent: NoneOneOrMany<Box<CancelThenOpen<ExchangeKey, InstrumentKey>>>,
    /// Groups not sent, none of their legs included, each paired with the [`EngineError`] that
    /// stopped it.
    pub errors: NoneOneOrMany<Box<(CancelThenOpen<ExchangeKey, InstrumentKey>, EngineError)>>,
}

impl<ExchangeKey, InstrumentKey> SendCancelThenOpensOutput<ExchangeKey, InstrumentKey> {
    /// Returns `true` if no group was sent or refused.
    pub fn is_empty(&self) -> bool {
        self.sent.is_none() && self.errors.is_none()
    }

    /// Iterates the groups sent, dereferencing through the boxed payload.
    pub fn sent_iter(&self) -> impl Iterator<Item = &CancelThenOpen<ExchangeKey, InstrumentKey>> {
        self.sent.iter().map(|group| &**group)
    }

    /// Returns any unrecoverable errors that occurred while sending the groups.
    pub fn unrecoverable_errors(&self) -> NoneOneOrMany<UnrecoverableEngineError> {
        self.errors
            .iter()
            .filter_map(|entry| match &entry.1 {
                EngineError::Unrecoverable(error) => Some(error.clone()),
                EngineError::Recoverable(_) => None,
            })
            .collect()
    }
}

/// Summary of the amends sent by the [`Engine`] to the `ExecutionManager`.
///
/// Each amend is boxed inside its [`NoneOneOrMany`] field, as in [`SendRequestsOutput`].
#[derive(
    Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize, Constructor,
)]
pub struct SendAmendsOutput<ExchangeKey = ExchangeIndex, InstrumentKey = InstrumentIndex> {
    /// Amends sent for execution, as built from the orders they amend.
    pub sent: NoneOneOrMany<Box<OrderRequestAmend<ExchangeKey, InstrumentKey>>>,
    /// Amends not sent, as asked for, each paired with the [`EngineError`] that stopped it:
    /// [`RecoverableEngineError::AmendRefused`] when the `Engine` refused it.
    pub errors: NoneOneOrMany<Box<(OrderAmend<ExchangeKey, InstrumentKey>, EngineError)>>,
}

impl<ExchangeKey, InstrumentKey> SendAmendsOutput<ExchangeKey, InstrumentKey> {
    /// Returns `true` if no amend was sent or refused.
    pub fn is_empty(&self) -> bool {
        self.sent.is_none() && self.errors.is_none()
    }

    /// Iterates the amends sent, dereferencing through the boxed payload.
    pub fn sent_iter(
        &self,
    ) -> impl Iterator<Item = &OrderRequestAmend<ExchangeKey, InstrumentKey>> {
        self.sent.iter().map(|amend| &**amend)
    }

    /// Returns any unrecoverable errors that occurred while sending the amends.
    pub fn unrecoverable_errors(&self) -> NoneOneOrMany<UnrecoverableEngineError> {
        self.errors
            .iter()
            .filter_map(|entry| match &entry.1 {
                EngineError::Unrecoverable(error) => Some(error.clone()),
                EngineError::Recoverable(_) => None,
            })
            .collect()
    }
}

impl<ExchangeKey, InstrumentKey> Default for SendAmendsOutput<ExchangeKey, InstrumentKey> {
    fn default() -> Self {
        Self {
            sent: NoneOneOrMany::default(),
            errors: NoneOneOrMany::default(),
        }
    }
}

impl<ExchangeKey, InstrumentKey> Default for SendCancelThenOpensOutput<ExchangeKey, InstrumentKey> {
    fn default() -> Self {
        Self {
            sent: NoneOneOrMany::default(),
            errors: NoneOneOrMany::default(),
        }
    }
}
