//! One WebSocket per API key, shared by every stream a subscriber and its clones open.
//!
//! # Why the connection is shared
//! The provider allows a key a single WebSocket — a second is refused with `TOO_MANY_CONNECTIONS`
//! on the free `registered` tier — and that one socket serves every dataset, subscription kind and
//! option underlying against one subscription cap. A socket per stream could therefore never run
//! two streams on one key.
//!
//! # Shape
//! An actor task owns the socket. Each stream a [`LseSubscriber`](super::live::LseSubscriber) opens
//! *attaches* to it and is
//! handed an [`LseAttachment`]: the raw frames for its own symbols, which it parses exactly as it
//! would parse a socket of its own. The actor reads only what routing needs — a frame's `type`,
//! `symbol` and `replay` stamp — and forwards the frame itself, a reference-counted clone, so each
//! stream's decoder stays the only full parse.
//!
//! - **Connect lazily, close when idle.** The socket opens on the first attach and closes once no
//!   stream remains attached.
//! - **Subscribe once per symbol.** A symbol or option underlying the socket already holds is not
//!   sent again: the attach is confirmed at once, and its frames fan out to every stream holding
//!   it. One symbol served by two datasets or two kinds costs one slot.
//! - **Unsubscribe on the last detach.** Dropping an attachment frees every symbol and underlying
//!   no other stream holds.
//! - **One subscribe handshake at a time.** Attaches are serialised, so a rejection naming no
//!   symbol (`LIMIT_REACHED`, `INVALID_START`) belongs to the attach in flight and fails it. One
//!   arriving outside a handshake is logged once, by the connection. A detach waits for the
//!   handshake in flight too, so a dropped stream's symbols stay subscribed until that handshake
//!   ends; frames keep flowing to every attached stream meanwhile.
//! - **Every guard is per attach, and the cap is per connection.** The offered-symbol check runs
//!   against each attach's own batch, before anything is sent; the subscription cap is checked
//!   against everything the connection would then hold, because every stream sharing it draws on
//!   the same slots.
//!
//! # Reconnect
//! Every stream on the connection **ends together** when the socket is lost, and each is
//! re-initialised by the usual reconnect wrapper. The first to re-attach reconnects and
//! re-subscribes everything the lost socket held; the rest re-attach to that one reconnect.
//!
//! - **A symbol has one replay window per connection.** The provider answers a repeated subscribe
//!   carrying `start` with "Already subscribed" and replays nothing, so each resumed symbol's window
//!   opens from the **earliest** watermark among the streams holding it. Each stream then drops,
//!   silently, the ticks before its own watermark — ones it had already delivered — and skips the
//!   delivered prefix at its watermark exactly as a stream of its own would. See
//!   [`LseSubscriber::with_resume`](super::live::LseSubscriber::with_resume).
//! - **Replayed frames reach only the streams that asked for them.** A stream that holds a resumed
//!   symbol without a watermark of its own — it does not resume, or has delivered nothing for that
//!   symbol yet — receives the live frames and none of the replay. This relies on the provider
//!   stamping replayed ticks `replay: true`, as it does today.
//! - **A stream keeps its place for [`REATTACH_GRACE`] after the socket is lost.** Frames for a
//!   stream that has not re-attached yet are held until then, and discarded with a warning if it
//!   has not; its symbols are then released. A stream dropped while the socket was down is
//!   forgotten the same way, so no later reconnect re-subscribes its symbols. The grace runs from
//!   the loss, and a reconnect that fails does not extend it.
//!
//! A stream attaching to a symbol the socket already holds cannot be given a replay at all, for
//! the same reason; if it has a watermark to resume from, that is reported rather than passed over.
//!
//! # Sharing is by clone, and only by clone
//! Clones of one subscriber share one connection. Two subscribers built separately for one key open
//! two sockets, and the provider refuses the second — visibly, with `TOO_MANY_CONNECTIONS`, which is
//! left to surface rather than papered over by a process-wide registry.

pub use crate::subscriber::shared::REATTACH_GRACE;

use super::{
    live::{
        LseCredentials, OfferedSymbols, authenticate, check_subscription_cap,
        check_symbols_are_offered, subscribe_message, subscribe_options_message,
        unsubscribe_message, unsubscribe_options_message,
    },
    mapper::subscription_id,
    resume::{LseResumeKey, LseResumeState},
    subscription::LseSubResponse,
    transformer::{LseResume, ResumeContext},
};
use crate::exchange::osi;
use crate::subscriber::shared::{
    self, AttachId, Attachment, Batch, Connections, Frame, Handshake, Protocol, Registration,
    Registry,
};
use crate::subscriber::shared_stream::{SharedTransport, sealed};
use chrono::{DateTime, Utc};
use fnv::{FnvHashMap, FnvHashSet};
use futures::Stream;
use rustrade_instrument::exchange::ExchangeId;
use rustrade_integration::{
    Validator,
    error::SocketError,
    protocol::websocket::{WebSocket, WsError, WsMessage, connect},
    subscription::SubscriptionId,
};
use serde::Deserialize;
use smol_str::SmolStr;
use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};
use tracing::{debug, warn};
use url::Url;

/// What one subscribe occupies on the socket: one slot of the connection's shared cap.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub(super) enum Slot {
    Symbol(SmolStr),
    Underlying(SmolStr),
}

impl Slot {
    fn subscribe(&self, start: Option<DateTime<Utc>>) -> WsMessage {
        match self {
            Self::Symbol(symbol) => subscribe_message(symbol, start),
            Self::Underlying(underlying) => subscribe_options_message(underlying),
        }
    }

    fn unsubscribe(&self) -> WsMessage {
        match self {
            Self::Symbol(symbol) => unsubscribe_message(symbol),
            Self::Underlying(underlying) => unsubscribe_options_message(underlying),
        }
    }
}

/// The London Strategic Edge side of a shared connection.
#[derive(Debug)]
pub(super) struct Lse;

/// The handle an `LseSubscriber` and its clones share: the way into the connection task.
#[derive(Debug)]
pub(super) struct LseConnection(Connections<Lse>);

impl LseConnection {
    pub(super) fn new(credentials: LseCredentials) -> Self {
        Self(Connections::new(credentials))
    }

    /// Attach one stream's batch to the connection, connecting first if nothing is attached.
    pub(super) async fn attach(
        &self,
        request: AttachRequest,
    ) -> Result<LseAttachment, SocketError> {
        self.0.attach(request.into()).await.map(LseAttachment)
    }
}

/// One stream's batch, as the connection needs it.
#[derive(Debug)]
pub(super) struct AttachRequest {
    pub(super) exchange: ExchangeId,
    pub(super) url: Url,
    /// [`SubscriptionKind::as_str`](crate::subscription::SubscriptionKind::as_str) of the batch.
    pub(super) kind: &'static str,
    /// The distinct symbols requested, in request order. Option contracts on the options dataset.
    pub(super) markets: Vec<SmolStr>,
    /// The distinct option underlyings, on the options dataset only.
    pub(super) underlyings: Option<Vec<SmolStr>>,
    pub(super) timeout: Duration,
    pub(super) resume: Option<Arc<LseResumeState>>,
}

impl From<AttachRequest> for shared::AttachRequest<Lse> {
    fn from(request: AttachRequest) -> Self {
        let AttachRequest {
            exchange,
            url,
            kind,
            markets,
            underlyings,
            timeout,
            resume,
        } = request;

        let per_underlying = underlyings.is_some();
        let slots = match underlyings {
            Some(underlyings) => underlyings.into_iter().map(Slot::Underlying).collect(),
            None => markets.iter().cloned().map(Slot::Symbol).collect(),
        };

        Self {
            exchange,
            url,
            kind,
            slots,
            timeout,
            batch: LseBatch {
                kind,
                markets,
                per_underlying,
                resume,
                starts: FnvHashMap::default(),
                replayed: FnvHashSet::default(),
            },
        }
    }
}

/// What a London Strategic Edge registration keeps beyond its exchange, kind and slots.
#[derive(Debug)]
pub(super) struct LseBatch {
    /// [`SubscriptionKind::as_str`](crate::subscription::SubscriptionKind::as_str) of the batch,
    /// which the resume state is partitioned by.
    kind: &'static str,
    /// The symbols requested, in request order: option contracts on the options dataset.
    markets: Vec<SmolStr>,
    /// Whether the batch subscribes per option underlying rather than per symbol.
    per_underlying: bool,
    resume: Option<Arc<LseResumeState>>,

    /// The replay windows opened on this registration's behalf on the current socket.
    starts: FnvHashMap<SubscriptionId, DateTime<Utc>>,
    /// The symbols among `starts`, spelled as the frames spell them, so a replayed frame can be
    /// matched without rebuilding a subscription identifier per frame.
    replayed: FnvHashSet<SmolStr>,
}

impl Batch for LseBatch {
    type Attached = LseResume;

    /// Equal markets, and the same resume state by identity: the replay windows opened for a
    /// registration were chosen from its own state, so a clone resuming from a different one, or
    /// not at all, is a different stream. The markets tell two option batches on the same
    /// underlyings apart.
    fn is_same_stream(&self, other: &Self) -> bool {
        let same_resume = match (&self.resume, &other.resume) {
            (Some(held), Some(requested)) => Arc::ptr_eq(held, requested),
            (None, None) => true,
            _ => false,
        };

        self.markets == other.markets && same_resume
    }

    /// The resume state, the kind it is filed under, and the replay windows opened for the stream
    /// on the current socket. A window may open earlier than the stream's own watermark, because a
    /// symbol has one per connection, opened at the earliest watermark any stream holding it
    /// needs.
    ///
    /// Nothing for option contracts, which never resume -- see `LseOptions`. Handed over, the
    /// state would skip live prints at the watermark's instant as though a replay had re-sent
    /// them, when no replay was asked for.
    fn attached(&self) -> LseResume {
        if self.per_underlying {
            return LseResume::default();
        }

        LseResume(self.resume.as_ref().map(|state| ResumeContext {
            state: Arc::clone(state),
            kind: self.kind,
            starts: self.starts.clone(),
        }))
    }

    fn reset(&mut self) {
        self.starts.clear();
        self.replayed.clear();
    }
}

/// What the `authenticated` frame said about the socket.
pub(super) struct Session {
    offered: OfferedSymbols,
    max_subscriptions: Option<u32>,
}

/// Waits for one confirmation per subscribe sent.
#[derive(Debug)]
pub(super) struct Confirmations {
    expected: usize,
    confirmed: usize,
}

impl Handshake<LseSubResponse> for Confirmations {
    fn is_settled(&self) -> bool {
        self.confirmed >= self.expected
    }

    fn observe(&mut self, response: LseSubResponse) {
        self.confirmed += 1;
        debug!(
            confirmed = self.confirmed,
            expected = self.expected,
            ?response,
            "London Strategic Edge confirmation"
        );
    }

    fn timed_out(&self, timeout: Duration) -> String {
        format!(
            "subscription validation timeout reached: {timeout:?}; {} of {} subscriptions confirmed",
            self.confirmed, self.expected
        )
    }
}

impl Protocol for Lse {
    const NAME: &'static str = "London Strategic Edge";
    // The provider allows a key one socket, whatever the endpoint.
    const CONNECTION_PER_ENDPOINT: bool = false;
    // Each slot is its own subscribe, so a refusal says nothing of those sent before it.
    const REFUSAL_IS_ATOMIC: bool = false;
    const DISCARDED_ON_LOSS: &'static str = ". A stream that resumes asks for them again from its \
        watermark on the next reconnect; one that does not never receives them";
    const DISCARDED_ON_EXPIRY: &'static str =
        ", which gets no replay for a symbol another stream still holds";

    type Credentials = LseCredentials;
    type Slot = Slot;
    type Batch = LseBatch;
    type Session = Session;
    type Answer = LseSubResponse;
    type Handshake = Confirmations;

    async fn connect(
        credentials: &LseCredentials,
        url: &Url,
    ) -> Result<(WebSocket, Session), SocketError> {
        let mut websocket = connect(url.clone()).await?;
        let authenticated = authenticate(&mut websocket, credentials).await?;
        debug!(
            %url,
            tier = ?authenticated.tier,
            offered = authenticated.symbols.len(),
            "authenticated to London Strategic Edge WebSocket",
        );

        Ok((
            websocket,
            Session {
                offered: OfferedSymbols::new(authenticated.symbols),
                max_subscriptions: authenticated.max_subscriptions,
            },
        ))
    }

    fn check(
        request: &shared::AttachRequest<Self>,
        session: &Session,
        registry: &Registry<Self>,
    ) -> Result<(), SocketError> {
        // The offered-symbol list does not hold every underlying that has options, and an
        // underlying without them is rejected by name rather than confirmed - see `LseOptions`.
        if !request.batch.per_underlying {
            check_symbols_are_offered(request.exchange, &request.batch.markets, &session.offered)?;
        }

        let added = request
            .slots
            .iter()
            .filter(|slot| !registry.holds(slot))
            .count();

        check_subscription_cap(
            request.exchange,
            added,
            registry.held(),
            session.max_subscriptions,
        )
    }

    fn subscribe(
        registry: &mut Registry<Self>,
        id: AttachId,
        sending: &[Slot],
    ) -> (Vec<WsMessage>, Confirmations) {
        let starts = plan_starts(registry, sending);

        let joined = joined_without_replay(registry, id, sending);
        if !joined.is_empty()
            && let Some(registration) = registry.get(id)
        {
            warn!(
                exchange = %registration.exchange,
                symbols = ?joined,
                "London Strategic Edge symbols this stream resumes were already streaming on the \
                 shared connection, and the provider replays nothing for a symbol it already \
                 streams; they resume live, and what was missed since the last delivery is not \
                 recovered",
            );
        }

        let messages = sending
            .iter()
            .map(|slot| {
                let start = match slot {
                    Slot::Symbol(symbol) => starts.get(symbol.as_str()).copied(),
                    Slot::Underlying(_) => None,
                };
                slot.subscribe(start)
            })
            .collect();

        (
            messages,
            Confirmations {
                expected: sending.len(),
                confirmed: 0,
            },
        )
    }

    /// Answered by an `unsubscribed` frame, which no handshake could mistake for a confirmation.
    fn release(slots: &[Slot]) -> (Vec<WsMessage>, Option<Confirmations>) {
        (slots.iter().map(Slot::unsubscribe).collect(), None)
    }

    fn read(registry: &mut Registry<Self>, message: &WsMessage) -> Frame<LseSubResponse> {
        match classify(message) {
            Classified::Route { symbol, replay } => {
                route(registry, symbol, replay, message);
                Frame::Answers(Vec::new())
            }
            Classified::Answer(answer) => Frame::Answers(vec![answer]),
            Classified::Closed(cause) => Frame::Closed(cause),
            Classified::Ignored => Frame::Answers(Vec::new()),
        }
    }

    /// Prefix an in-handshake rejection with what the stream cannot see: its cap is shared.
    fn refused(error: SocketError) -> SocketError {
        match error {
            SocketError::Subscribe(message) => SocketError::Subscribe(format!(
                "{message} (the connection and its subscription cap are shared by every stream \
                 opened by this subscriber and its clones)"
            )),
            other => other,
        }
    }
}

/// One stream's view of the shared London Strategic Edge connection.
///
/// A stream of the raw frames addressed to the stream's own symbols, in the order the socket
/// delivered them, so it is parsed exactly as a socket of the stream's own would be. It ends when
/// the connection is lost, which is what drives the stream's reconnect.
///
/// Dropping it detaches the stream: every symbol no other stream holds is unsubscribed, and the
/// socket closes once nothing remains attached.
///
/// # Keep it drained
/// The queue behind it is unbounded. The connection reads the one socket for every stream sharing
/// it, so it never waits for a slow one — that would stall the rest — and a stream it serves is
/// not back-pressured the way a socket of its own would be. Frames for a stream that stops being
/// polled accumulate in memory until it is polled again or dropped.
///
/// See [`connection`](self) for how the connection is shared.
#[derive(Debug)]
pub struct LseAttachment(Attachment<Lse>);

impl Stream for LseAttachment {
    type Item = Result<WsMessage, WsError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.0).poll_next(cx)
    }
}

impl sealed::Sealed for LseAttachment {}

/// A stream is handed what it needs to resume, if it resumes.
impl SharedTransport for LseAttachment {
    type Attached = LseResume;

    fn take_attached(&mut self) -> LseResume {
        self.0.take_attached()
    }
}

/// The instant `registration` last delivered `symbol` at, if it resumes and has.
fn watermark(registration: &Registration<Lse>, symbol: &str) -> Option<DateTime<Utc>> {
    let key = LseResumeKey::new(
        registration.exchange,
        subscription_id(symbol),
        registration.kind,
    );

    registration
        .batch
        .resume
        .as_ref()?
        .watermark(&key)
        .map(|watermark| watermark.time_exchange)
}

/// Open each resumed symbol in `sending` at the earliest watermark among the registrations holding
/// it, record the window on each of them, and return the `start` to send per symbol.
///
/// A registration with no watermark for a symbol asks for no window, and does not hold the
/// symbol's start back either.
fn plan_starts(
    registry: &mut Registry<Lse>,
    sending: &[Slot],
) -> FnvHashMap<SmolStr, DateTime<Utc>> {
    let mut starts = FnvHashMap::default();

    for slot in sending {
        let Slot::Symbol(symbol) = slot else {
            continue;
        };
        let Some(holders) = registry.holders(slot) else {
            continue;
        };

        let marks = holders
            .iter()
            .filter_map(|id| Some((*id, watermark(registry.get(*id)?, symbol)?)))
            .collect::<Vec<_>>();

        let Some(start) = marks.iter().map(|(_, watermark)| *watermark).min() else {
            continue;
        };

        starts.insert(symbol.clone(), start);

        for (id, _) in marks {
            if let Some(registration) = registry.get_mut(id) {
                registration
                    .batch
                    .starts
                    .insert(subscription_id(symbol), start);
                registration.batch.replayed.insert(symbol.clone());
            }
        }
    }

    starts
}

/// The symbols `id` holds that were already on the socket, and that it has a watermark for: the
/// ones it wanted a replay of and cannot be given one.
fn joined_without_replay(registry: &Registry<Lse>, id: AttachId, sending: &[Slot]) -> Vec<SmolStr> {
    let Some(registration) = registry.get(id) else {
        return Vec::new();
    };

    registration
        .slots
        .iter()
        .filter(|slot| !sending.contains(slot))
        .filter_map(|slot| match slot {
            Slot::Symbol(symbol) => watermark(registration, symbol)
                .is_some()
                .then(|| symbol.clone()),
            Slot::Underlying(_) => None,
        })
        .collect()
}

/// Deliver one frame to every registration holding `symbol`.
///
/// An exact symbol reaches plain subscriptions; an option contract reaches the options
/// subscriptions holding its underlying. A replayed frame reaches only the registrations a window
/// was opened for.
fn route(registry: &mut Registry<Lse>, symbol: SmolStr, replay: bool, frame: &WsMessage) {
    let slot = Slot::Symbol(symbol);
    let Slot::Symbol(symbol) = &slot else {
        return;
    };
    let accepts =
        |registration: &Registration<Lse>| !replay || registration.batch.replayed.contains(symbol);

    if registry.route(&slot, accepts, frame) {
        return;
    }

    // An underlying is short enough to be held inline, so this allocates nothing.
    if let Some(underlying) = osi::root(symbol)
        && registry.route(&Slot::Underlying(SmolStr::new(underlying)), accepts, frame)
    {
        return;
    }

    // Normal for a moment after an unsubscribe, while frames already in flight arrive.
    debug!(
        %symbol,
        "London Strategic Edge frame for a symbol no stream holds"
    );
}

/// The part of a frame routing reads.
///
/// Short strings deserialise inline — a symbol, an OSI contract and a frame type all fit — so this
/// allocates nothing on the per-frame path, whether or not the provider escapes a character.
#[derive(Deserialize)]
struct Envelope {
    #[serde(rename = "type", default)]
    kind: Option<SmolStr>,
    #[serde(default)]
    symbol: Option<SmolStr>,
    #[serde(default)]
    replay: Option<bool>,
}

enum Classified {
    Route { symbol: SmolStr, replay: bool },
    Answer(Result<LseSubResponse, SocketError>),
    Closed(String),
    Ignored,
}

fn classify(message: &WsMessage) -> Classified {
    let text = match message {
        WsMessage::Text(text) => text.as_str(),
        WsMessage::Binary(bytes) => match std::str::from_utf8(bytes) {
            Ok(text) => text,
            Err(_) => {
                warn!(
                    len = bytes.len(),
                    "London Strategic Edge sent a binary frame that is not UTF-8; ignored"
                );
                return Classified::Ignored;
            }
        },
        WsMessage::Close(frame) => {
            return Classified::Closed(format!("closed by the provider: {frame:?}"));
        }
        // Pings are answered by the socket itself.
        _ => return Classified::Ignored,
    };

    let envelope = match serde_json::from_str::<Envelope>(text) {
        Ok(envelope) => envelope,
        Err(error) => {
            warn!(
                %error,
                frame = %text.chars().take(200).collect::<String>(),
                "London Strategic Edge sent a frame that is not a JSON object; ignored",
            );
            return Classified::Ignored;
        }
    };

    match (envelope.kind.as_deref(), envelope.symbol) {
        (Some("subscribed" | "options_subscribed" | "error"), _) => Classified::Answer(
            serde_json::from_str::<LseSubResponse>(text)
                .map_err(|error| SocketError::Deserialise {
                    error,
                    payload: text.to_owned(),
                })
                .and_then(Validator::validate),
        ),
        // The answer to an unsubscribe, which nothing awaits.
        (Some("unsubscribed" | "options_unsubscribed"), _) => {
            debug!(
                frame = text,
                "London Strategic Edge released a subscription"
            );
            Classified::Ignored
        }
        // A replay's boundaries belong to the replay: only a stream that asked for one reads them.
        (Some("replay_started" | "replay_complete"), Some(symbol)) => Classified::Route {
            symbol,
            replay: true,
        },
        (_, Some(symbol)) => Classified::Route {
            symbol,
            replay: envelope.replay.unwrap_or(false),
        },
        (kind, None) => {
            debug!(?kind, "London Strategic Edge control frame");
            Classified::Ignored
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use crate::subscription::{SubscriptionKind, book::OrderBooksL1, trade::PublicTrades};
    use tokio::sync::mpsc;

    const TRADES: &str = "public_trades";

    fn at(spelling: &str) -> DateTime<Utc> {
        spelling.parse::<DateTime<Utc>>().unwrap()
    }

    fn text(value: serde_json::Value) -> WsMessage {
        WsMessage::text(value.to_string())
    }

    fn request(
        exchange: ExchangeId,
        kind: &'static str,
        markets: &[&str],
        resume: Option<&Arc<LseResumeState>>,
    ) -> AttachRequest {
        AttachRequest {
            exchange,
            url: Url::parse("ws://127.0.0.1:1").unwrap(),
            kind,
            markets: markets.iter().map(SmolStr::new).collect(),
            underlyings: None,
            timeout: Duration::from_secs(1),
            resume: resume.cloned(),
        }
    }

    fn options_request(contracts: &[&str], underlyings: &[&str]) -> AttachRequest {
        AttachRequest {
            underlyings: Some(underlyings.iter().map(SmolStr::new).collect()),
            ..request(ExchangeId::LseOptions, TRADES, contracts, None)
        }
    }

    fn live(
        registry: &mut Registry<Lse>,
        request: AttachRequest,
    ) -> (AttachId, mpsc::UnboundedReceiver<WsMessage>) {
        let (tx, rx) = mpsc::unbounded_channel();
        (registry.insert_live(request.into(), tx), rx)
    }

    fn record(
        state: &LseResumeState,
        exchange: ExchangeId,
        symbol: &str,
        kind: &'static str,
        time: &str,
    ) {
        state.record(
            &LseResumeKey::new(exchange, subscription_id(symbol), kind),
            at(time),
        );
    }

    fn drain(rx: &mut mpsc::UnboundedReceiver<WsMessage>) -> Vec<WsMessage> {
        std::iter::from_fn(|| rx.try_recv().ok()).collect()
    }

    fn symbol(name: &str) -> Slot {
        Slot::Symbol(SmolStr::new(name))
    }

    /// Route one frame exactly as the connection task does.
    fn deliver(registry: &mut Registry<Lse>, frame: &WsMessage) {
        let answers = match Lse::read(registry, frame) {
            Frame::Answers(answers) => answers,
            Frame::Closed(cause) => panic!("expected a routed frame, got a close: {cause}"),
        };
        assert!(answers.is_empty(), "expected a routed frame, got answers");
    }

    fn starts(registry: &Registry<Lse>, id: AttachId) -> FnvHashMap<SubscriptionId, DateTime<Utc>> {
        registry.get(id).unwrap().batch.starts.clone()
    }

    #[test]
    fn a_symbol_held_by_two_streams_is_one_slot_and_reaches_both() {
        let mut registry = Registry::default();
        let (_, mut trades) = live(
            &mut registry,
            request(ExchangeId::LseCrypto, TRADES, &["BTC/USD"], None),
        );
        let (_, mut books) = live(
            &mut registry,
            request(
                ExchangeId::LseCrypto,
                OrderBooksL1.as_str(),
                &["BTC/USD", "ETH/USD"],
                None,
            ),
        );

        assert_eq!(registry.held(), 2);

        let frame = text(serde_json::json!({"type": "tick", "symbol": "BTC/USD"}));
        deliver(&mut registry, &frame);

        assert_eq!(drain(&mut trades), std::slice::from_ref(&frame));
        assert_eq!(drain(&mut books), [frame]);
    }

    #[test]
    fn an_option_contract_reaches_the_streams_holding_its_underlying() {
        let mut registry = Registry::default();
        let (_, mut options) = live(
            &mut registry,
            options_request(&["SPY260930C00700000"], &["SPY"]),
        );
        let (_, mut equities) = live(
            &mut registry,
            request(ExchangeId::LseEquities, TRADES, &["SPY"], None),
        );

        let contract = text(serde_json::json!({"symbol": "SPY260930C00701000"}));
        let share = text(serde_json::json!({"symbol": "SPY"}));
        deliver(&mut registry, &contract);
        deliver(&mut registry, &share);

        assert_eq!(drain(&mut options), [contract]);
        assert_eq!(drain(&mut equities), [share]);
    }

    #[test]
    fn two_option_batches_on_one_underlying_are_different_streams() {
        let held = || options_request(&["SPY260930C00700000"], &["SPY"]).into();
        let other: shared::AttachRequest<Lse> =
            options_request(&["SPY260930P00600000"], &["SPY"]).into();

        let registration: shared::AttachRequest<Lse> = held();
        assert!(registration.batch.is_same_stream(&held().batch));
        assert!(!registration.batch.is_same_stream(&other.batch));
    }

    #[test]
    fn a_different_resume_state_is_a_different_stream() {
        let state = Arc::new(LseResumeState::new());
        let batch = |resume| -> shared::AttachRequest<Lse> {
            request(ExchangeId::LseCrypto, TRADES, &["BTC/USD"], resume).into()
        };

        assert!(
            batch(Some(&state))
                .batch
                .is_same_stream(&batch(Some(&state)).batch)
        );
        assert!(!batch(Some(&state)).batch.is_same_stream(&batch(None).batch));
        assert!(
            !batch(Some(&state))
                .batch
                .is_same_stream(&batch(Some(&Arc::new(LseResumeState::new()))).batch)
        );
    }

    /// The provider replays nothing for a repeated subscribe, so each symbol gets one window: the
    /// earliest any holder needs. Every holder with a watermark is told the window it was given.
    #[test]
    fn a_shared_symbol_opens_one_window_at_the_earliest_watermark() {
        let state = Arc::new(LseResumeState::new());
        record(
            &state,
            ExchangeId::LseCrypto,
            "BTC/USD",
            TRADES,
            "2026-08-14T10:00:02Z",
        );
        let books = OrderBooksL1.as_str();
        record(
            &state,
            ExchangeId::LseCrypto,
            "BTC/USD",
            books,
            "2026-08-14T10:00:01Z",
        );

        let mut registry = Registry::default();
        let (trades, _rx1) = live(
            &mut registry,
            request(ExchangeId::LseCrypto, TRADES, &["BTC/USD"], Some(&state)),
        );
        let (quotes, _rx2) = live(
            &mut registry,
            request(ExchangeId::LseCrypto, books, &["BTC/USD"], Some(&state)),
        );

        let planned = plan_starts(&mut registry, &[symbol("BTC/USD")]);

        assert_eq!(planned.get("BTC/USD"), Some(&at("2026-08-14T10:00:01Z")));
        for id in [trades, quotes] {
            assert_eq!(
                starts(&registry, id).get(&subscription_id("BTC/USD")),
                Some(&at("2026-08-14T10:00:01Z")),
            );
        }
    }

    /// A stream is handed its subscriber's resume state, filed under its own kind, with the windows
    /// opened for it; one that does not resume, and an option batch, which never does, nothing.
    #[test]
    fn a_stream_is_handed_what_it_resumes_from_and_an_option_batch_nothing() {
        let state = Arc::new(LseResumeState::new());
        record(
            &state,
            ExchangeId::LseCrypto,
            "BTC/USD",
            TRADES,
            "2026-08-14T10:00:01Z",
        );

        let mut registry = Registry::default();
        let (resumed, _rx1) = live(
            &mut registry,
            request(ExchangeId::LseCrypto, TRADES, &["BTC/USD"], Some(&state)),
        );
        let (unresumed, _rx2) = live(
            &mut registry,
            request(ExchangeId::LseCrypto, TRADES, &["ETH/USD"], None),
        );
        let (options, _rx3) = live(
            &mut registry,
            AttachRequest {
                resume: Some(Arc::clone(&state)),
                ..options_request(&["SPY260930C00700000"], &["SPY"])
            },
        );
        plan_starts(&mut registry, &[symbol("BTC/USD")]);

        let attached = |id| registry.get(id).unwrap().batch.attached().0;

        let context = attached(resumed).expect("a resuming stream was handed nothing");
        assert!(Arc::ptr_eq(&context.state, &state));
        assert_eq!(context.kind, TRADES);
        assert_eq!(
            context.starts.get(&subscription_id("BTC/USD")),
            Some(&at("2026-08-14T10:00:01Z")),
        );
        assert!(attached(unresumed).is_none());
        assert!(attached(options).is_none());
    }

    /// A window is asked for on the strength of what the holder itself delivered, per dataset and
    /// per kind, never what another dataset or kind did.
    #[test]
    fn a_holder_with_no_watermark_of_its_own_asks_for_no_window() {
        let state = Arc::new(LseResumeState::new());
        record(
            &state,
            ExchangeId::LseEquities,
            "AAPL",
            TRADES,
            "2026-08-14T10:00:00Z",
        );

        let mut registry = Registry::default();
        let (futures, _rx1) = live(
            &mut registry,
            request(ExchangeId::LseFutures, TRADES, &["AAPL"], Some(&state)),
        );
        let (books, _rx2) = live(
            &mut registry,
            request(
                ExchangeId::LseEquities,
                OrderBooksL1.as_str(),
                &["AAPL"],
                Some(&state),
            ),
        );

        assert!(plan_starts(&mut registry, &[symbol("AAPL")]).is_empty());
        assert!(starts(&registry, futures).is_empty());
        assert!(starts(&registry, books).is_empty());
    }

    #[test]
    fn a_replayed_frame_reaches_only_the_holders_a_window_was_opened_for() {
        let state = Arc::new(LseResumeState::new());
        record(
            &state,
            ExchangeId::LseCrypto,
            "BTC/USD",
            TRADES,
            "2026-08-14T10:00:00Z",
        );

        let mut registry = Registry::default();
        let (_, mut resumed) = live(
            &mut registry,
            request(ExchangeId::LseCrypto, TRADES, &["BTC/USD"], Some(&state)),
        );
        let (_, mut plain) = live(
            &mut registry,
            request(
                ExchangeId::LseCrypto,
                OrderBooksL1.as_str(),
                &["BTC/USD"],
                None,
            ),
        );
        plan_starts(&mut registry, &[symbol("BTC/USD")]);

        let replayed = text(serde_json::json!({"symbol": "BTC/USD", "replay": true}));
        let current = text(serde_json::json!({"symbol": "BTC/USD"}));
        deliver(&mut registry, &replayed);
        deliver(&mut registry, &current);

        assert_eq!(drain(&mut resumed), [replayed, current.clone()]);
        assert_eq!(drain(&mut plain), [current]);
    }

    #[test]
    fn a_resumed_symbol_already_on_the_socket_is_reported_as_joined_without_a_replay() {
        let state = Arc::new(LseResumeState::new());
        record(
            &state,
            ExchangeId::LseCrypto,
            "BTC/USD",
            TRADES,
            "2026-08-14T10:00:00Z",
        );

        let mut registry = Registry::default();
        let (id, _rx) = live(
            &mut registry,
            request(
                ExchangeId::LseCrypto,
                TRADES,
                &["BTC/USD", "ETH/USD"],
                Some(&state),
            ),
        );

        assert_eq!(
            joined_without_replay(&registry, id, &[symbol("ETH/USD")]),
            ["BTC/USD"]
        );
        assert!(
            joined_without_replay(&registry, id, &[symbol("BTC/USD"), symbol("ETH/USD")])
                .is_empty()
        );
    }

    #[test]
    fn frames_are_classified_by_their_routing_key_alone() {
        let classify_json = |value: serde_json::Value| classify(&text(value));

        assert!(matches!(
            classify_json(serde_json::json!({"type": "tick", "symbol": "BTC/USD", "price": 1.0})),
            Classified::Route { symbol, replay: false } if symbol == "BTC/USD"
        ));
        assert!(matches!(
            classify_json(serde_json::json!({"type": "tick", "symbol": "BTC/USD", "replay": true})),
            Classified::Route { replay: true, .. }
        ));
        assert!(matches!(
            classify_json(serde_json::json!({"type": "replay_started", "symbol": "BTC/USD"})),
            Classified::Route { replay: true, .. }
        ));
        assert!(matches!(
            classify_json(
                serde_json::json!({"type": "subscribed", "symbol": "BTC/USD",
                "message": "Already subscribed"})
            ),
            Classified::Answer(Ok(_))
        ));
        assert!(matches!(
            classify_json(serde_json::json!({"type": "options_subscribed", "underlying": "SPY"})),
            Classified::Answer(Ok(_))
        ));
        assert!(matches!(
            classify_json(serde_json::json!({"type": "error", "code": "LIMIT_REACHED"})),
            Classified::Answer(Err(_))
        ));
        assert!(matches!(
            classify_json(serde_json::json!({"type": "unsubscribed", "symbol": "BTC/USD"})),
            Classified::Ignored
        ));
        assert!(matches!(
            classify_json(serde_json::json!({"type": "welcome"})),
            Classified::Ignored
        ));
        assert!(matches!(
            classify(&WsMessage::Close(None)),
            Classified::Closed(_)
        ));
    }

    #[test]
    fn a_handshake_settles_once_every_subscribe_sent_is_confirmed() {
        let mut registry = Registry::default();
        let (id, _rx) = live(
            &mut registry,
            request(ExchangeId::LseCrypto, TRADES, &["BTC/USD", "ETH/USD"], None),
        );
        let sending = [symbol("BTC/USD"), symbol("ETH/USD")];

        let (messages, mut handshake) = Lse::subscribe(&mut registry, id, &sending);
        assert_eq!(
            messages,
            [
                text(serde_json::json!({"action": "subscribe", "symbol": "BTC/USD"})),
                text(serde_json::json!({"action": "subscribe", "symbol": "ETH/USD"})),
            ]
        );

        let confirmation = || {
            let frame = text(serde_json::json!({"type": "subscribed", "symbol": "BTC/USD"}));
            match classify(&frame) {
                Classified::Answer(answer) => answer.unwrap(),
                _ => panic!("expected an answer"),
            }
        };

        assert!(!handshake.is_settled());
        handshake.observe(confirmation());
        assert!(!handshake.is_settled());
        assert!(
            handshake
                .timed_out(Duration::from_secs(1))
                .ends_with("1 of 2 subscriptions confirmed")
        );
        handshake.observe(confirmation());
        assert!(handshake.is_settled());

        let (nothing, handshake) = Lse::subscribe(&mut registry, id, &[]);
        assert!(nothing.is_empty());
        assert!(
            handshake.is_settled(),
            "an attach sending nothing waits for nothing"
        );
    }

    #[test]
    fn the_symbol_a_slot_occupies_is_released_with_the_matching_unsubscribe() {
        let (messages, handshake) =
            Lse::release(&[symbol("BTC/USD"), Slot::Underlying("SPY".into())]);

        assert_eq!(
            messages,
            [
                text(serde_json::json!({"action": "unsubscribe", "symbol": "BTC/USD"})),
                text(serde_json::json!({"action": "unsubscribe_options", "underlying": "SPY"})),
            ]
        );
        assert!(handshake.is_none(), "an unsubscribe is not awaited");
    }

    #[test]
    fn a_trades_kind_spells_itself_as_the_tests_assume() {
        assert_eq!(PublicTrades.as_str(), TRADES);
    }
}
