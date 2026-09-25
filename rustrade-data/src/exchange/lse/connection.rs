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
//! - **Frames for a stream that has not re-attached yet are held** for [`REATTACH_GRACE`], then
//!   discarded with a warning, and its symbols released.
//!
//! A stream attaching to a symbol the socket already holds cannot be given a replay at all, for
//! the same reason; if it has a watermark to resume from, that is reported rather than passed over.
//!
//! # Sharing is by clone, and only by clone
//! Clones of one subscriber share one connection. Two subscribers built separately for one key open
//! two sockets, and the provider refuses the second — visibly, with `TOO_MANY_CONNECTIONS`, which is
//! left to surface rather than papered over by a process-wide registry.

use super::{
    live::{
        LseCredentials, OfferedSymbols, authenticate, check_subscription_cap,
        check_symbols_are_offered, subscribe_message, subscribe_options_message,
        unsubscribe_message, unsubscribe_options_message,
    },
    osi,
    resume::{LseResumeKey, LseResumeState, subscription_id},
    subscription::LseSubResponse,
};
use chrono::{DateTime, Utc};
use fnv::{FnvHashMap, FnvHashSet};
use futures::{SinkExt, Stream, StreamExt};
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
    future,
    pin::Pin,
    sync::{Arc, Mutex, PoisonError},
    task::{Context, Poll},
    time::Duration,
};
use tokio::{
    sync::{mpsc, oneshot},
    time::Instant,
};
use tracing::{debug, error, warn};
use url::Url;

/// How long the connection holds frames for a stream that has not re-attached after a reconnect.
///
/// A stream re-attaches as soon as it has drained what it was sent before the socket was lost, which
/// is normally milliseconds. The bound covers a consumer that stalls for a while; past it the held
/// frames are discarded with a warning and the stream's symbols released, and a late re-attach is
/// treated as a new attach: a symbol another stream still holds resumes live, with no replay.
///
/// Held frames are reference-counted clones of what every other stream receives, so the cost is
/// bounded by what the provider sends within the window, a replay burst included.
pub const REATTACH_GRACE: Duration = Duration::from_secs(30);

/// Identifies one attach for the lifetime of the connection actor. Never reused.
type AttachId = u64;

/// What one subscribe occupies on the socket: one slot of the connection's shared cap.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
enum Slot {
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

/// The handle an `LseSubscriber` and its clones share: the way into the connection actor.
///
/// The actor is spawned on the first attach rather than on construction, because a subscriber can
/// be built outside a runtime, and again if the runtime that ran it has since shut down.
#[derive(Debug)]
pub(super) struct LseConnection {
    credentials: LseCredentials,
    commands: Mutex<Option<mpsc::UnboundedSender<Command>>>,
}

impl LseConnection {
    pub(super) fn new(credentials: LseCredentials) -> Self {
        Self {
            credentials,
            commands: Mutex::new(None),
        }
    }

    /// Attach one stream's batch to the connection, connecting first if nothing is attached.
    pub(super) async fn attach(
        &self,
        request: AttachRequest,
    ) -> Result<LseAttachment, SocketError> {
        let (reply, answer) = oneshot::channel();

        self.commands()
            .send(Command::Attach(Box::new(request), reply))
            .map_err(|_| actor_stopped())?;

        answer.await.map_err(|_| actor_stopped())?
    }

    fn commands(&self) -> mpsc::UnboundedSender<Command> {
        // A poisoned lock guards a sender, which is valid whatever state the panic left behind.
        let mut commands = self.commands.lock().unwrap_or_else(PoisonError::into_inner);

        if let Some(commands) = commands.as_ref()
            && !commands.is_closed()
        {
            return commands.clone();
        }

        let (tx, rx) = mpsc::unbounded_channel();
        let actor = tokio::spawn(Actor::new(self.credentials.clone(), rx, tx.downgrade()).run());
        tokio::spawn(report_panic(actor));
        *commands = Some(tx.clone());

        tx
    }
}

/// Log a panic of the connection actor, which its streams would otherwise see only as a lost socket.
///
/// Every stream sharing the connection ends when the actor does, and the next attach spawns a new
/// one, so the panic is not propagated anywhere; this is the one place it is reported.
async fn report_panic(actor: tokio::task::JoinHandle<()>) {
    if let Err(error) = actor.await
        && error.is_panic()
    {
        error!(
            %error,
            "the London Strategic Edge connection task panicked; every stream sharing the \
             connection ends, and the next to re-attach starts a new connection",
        );
    }
}

fn actor_stopped() -> SocketError {
    SocketError::Subscribe(
        "the London Strategic Edge connection task stopped before answering".to_owned(),
    )
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

impl AttachRequest {
    fn slots(&self) -> Vec<Slot> {
        match &self.underlyings {
            Some(underlyings) => underlyings.iter().cloned().map(Slot::Underlying).collect(),
            None => self.markets.iter().cloned().map(Slot::Symbol).collect(),
        }
    }
}

#[derive(Debug)]
enum Command {
    Attach(
        Box<AttachRequest>,
        oneshot::Sender<Result<LseAttachment, SocketError>>,
    ),
    Detach(AttachId),
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
pub struct LseAttachment {
    frames: mpsc::UnboundedReceiver<WsMessage>,
    starts: FnvHashMap<SubscriptionId, DateTime<Utc>>,
    _detach: Detach,
}

impl LseAttachment {
    /// The replay windows opened for this stream: each subscription the connection resumed on its
    /// behalf, and the instant the window opens at.
    ///
    /// That instant may be earlier than the stream's own watermark, because a symbol has one window
    /// per connection and it opens at the earliest watermark any stream holding it needs.
    pub(super) fn take_starts(&mut self) -> FnvHashMap<SubscriptionId, DateTime<Utc>> {
        std::mem::take(&mut self.starts)
    }
}

impl Stream for LseAttachment {
    type Item = Result<WsMessage, WsError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.frames.poll_recv(cx).map(|frame| frame.map(Ok))
    }
}

/// Tells the actor an attachment is gone.
#[derive(Debug)]
struct Detach {
    id: AttachId,
    commands: Option<mpsc::UnboundedSender<Command>>,
}

impl Drop for Detach {
    fn drop(&mut self) {
        if let Some(commands) = &self.commands {
            // The actor having stopped already is the one way this fails, and then there is
            // nothing left to detach from.
            let _ = commands.send(Command::Detach(self.id));
        }
    }
}

/// One attached stream's batch, and where its frames go.
#[derive(Debug)]
struct Registration {
    exchange: ExchangeId,
    kind: &'static str,
    markets: Vec<SmolStr>,
    slots: Vec<Slot>,
    resume: Option<Arc<LseResumeState>>,

    /// The replay windows opened on this registration's behalf on the current socket.
    starts: FnvHashMap<SubscriptionId, DateTime<Utc>>,
    /// The symbols among `starts`, spelled as the frames spell them, so a replayed frame can be
    /// matched without rebuilding a subscription identifier per frame.
    replayed: FnvHashSet<SmolStr>,

    sink: Sink,
}

#[derive(Debug)]
enum Sink {
    /// Attached: frames go straight to the stream.
    Live(mpsc::UnboundedSender<WsMessage>),
    /// Re-subscribed by a reconnect the stream has not re-attached to yet.
    Held {
        frames: Vec<WsMessage>,
        until: Instant,
    },
    /// Its socket was lost, and nothing has reconnected since.
    Lost,
}

impl Registration {
    /// Whether `request` is this registration's stream re-attaching.
    ///
    /// The reconnect wrapper re-initialises a stream with the batch it was built with, and the
    /// batch reaches this point sorted and de-duplicated, so an equal batch is the same stream —
    /// or an exact duplicate of it, which is interchangeable with it. The resume state is compared
    /// by identity: the replay windows opened for a registration were chosen from its own state, so
    /// a clone resuming from a different one, or not at all, is a different stream.
    fn is_requested_by(&self, request: &AttachRequest) -> bool {
        let same_resume = match (&self.resume, &request.resume) {
            (Some(held), Some(requested)) => Arc::ptr_eq(held, requested),
            (None, None) => true,
            _ => false,
        };

        self.exchange == request.exchange
            && self.kind == request.kind
            && self.markets == request.markets
            && same_resume
    }

    fn is_routed(&self) -> bool {
        !matches!(self.sink, Sink::Lost)
    }

    /// The instant this registration last delivered `symbol` at, if it resumes and has.
    fn watermark(&self, symbol: &str) -> Option<DateTime<Utc>> {
        let key = LseResumeKey::new(self.exchange, subscription_id(symbol), self.kind);

        self.resume
            .as_ref()?
            .watermark(&key)
            .map(|watermark| watermark.time_exchange)
    }
}

/// Which attaches hold each symbol and underlying — equivalently, what the socket holds.
#[derive(Debug, Default)]
struct Index {
    by_symbol: FnvHashMap<SmolStr, Vec<AttachId>>,
    by_underlying: FnvHashMap<SmolStr, Vec<AttachId>>,
}

impl Index {
    fn map<'a>(
        &mut self,
        slot: &'a Slot,
    ) -> (&mut FnvHashMap<SmolStr, Vec<AttachId>>, &'a SmolStr) {
        match slot {
            Slot::Symbol(symbol) => (&mut self.by_symbol, symbol),
            Slot::Underlying(underlying) => (&mut self.by_underlying, underlying),
        }
    }

    fn link(&mut self, id: AttachId, slots: &[Slot]) {
        for slot in slots {
            let (map, key) = self.map(slot);
            map.entry(key.clone()).or_default().push(id);
        }
    }

    /// Remove `id` from `slots`, returning those nothing holds any longer.
    fn unlink(&mut self, id: AttachId, slots: &[Slot]) -> Vec<Slot> {
        let mut orphaned = Vec::new();

        for slot in slots {
            let (map, key) = self.map(slot);
            if let Some(holders) = map.get_mut(key.as_str()) {
                holders.retain(|holder| *holder != id);
                if holders.is_empty() {
                    map.remove(key.as_str());
                    orphaned.push(slot.clone());
                }
            }
        }

        orphaned
    }

    fn contains(&self, slot: &Slot) -> bool {
        match slot {
            Slot::Symbol(symbol) => self.by_symbol.contains_key(symbol.as_str()),
            Slot::Underlying(underlying) => self.by_underlying.contains_key(underlying.as_str()),
        }
    }

    fn len(&self) -> usize {
        self.by_symbol.len() + self.by_underlying.len()
    }

    fn slots(&self) -> impl Iterator<Item = Slot> + '_ {
        let symbols = self.by_symbol.keys().cloned().map(Slot::Symbol);
        let underlyings = self.by_underlying.keys().cloned().map(Slot::Underlying);

        symbols.chain(underlyings)
    }

    fn clear(&mut self) {
        self.by_symbol.clear();
        self.by_underlying.clear();
    }
}

/// Every registration and the routing index over them. Holds no socket, so it is tested directly.
#[derive(Debug, Default)]
struct Registry {
    next_id: AttachId,
    registrations: FnvHashMap<AttachId, Registration>,
    index: Index,
}

impl Registry {
    fn is_empty(&self) -> bool {
        self.registrations.is_empty()
    }

    fn insert(&mut self, registration: Registration) -> AttachId {
        let id = self.next_id;
        self.next_id += 1;

        if registration.is_routed() {
            self.index.link(id, &registration.slots);
        }
        self.registrations.insert(id, registration);

        id
    }

    fn insert_live(
        &mut self,
        request: AttachRequest,
        slots: Vec<Slot>,
        frames: mpsc::UnboundedSender<WsMessage>,
    ) -> AttachId {
        self.insert(Registration {
            exchange: request.exchange,
            kind: request.kind,
            markets: request.markets,
            slots,
            resume: request.resume,
            starts: FnvHashMap::default(),
            replayed: FnvHashSet::default(),
            sink: Sink::Live(frames),
        })
    }

    /// Remove a registration in any state, returning the slots nothing holds any longer.
    fn remove(&mut self, id: AttachId) -> Vec<Slot> {
        match self.registrations.remove(&id) {
            Some(registration) if registration.is_routed() => {
                self.index.unlink(id, &registration.slots)
            }
            _ => Vec::new(),
        }
    }

    /// Remove an attached registration, returning the slots nothing holds any longer.
    ///
    /// `None` for anything else. A registration whose socket was lost is waiting for its stream to
    /// re-attach: the stream's old attachment is dropped as it ends, and that must not remove it.
    fn remove_live(&mut self, id: AttachId) -> Option<Vec<Slot>> {
        let live = matches!(
            self.registrations
                .get(&id)
                .map(|registration| &registration.sink),
            Some(Sink::Live(_))
        );

        live.then(|| self.remove(id))
    }

    /// Re-attach `request`'s stream to the registration a reconnect is holding frames for.
    ///
    /// The registration moves to a fresh identifier, so a late detach from the stream's previous
    /// attachment cannot remove it. Returns that identifier and the replay windows opened for it.
    fn reattach(
        &mut self,
        request: &AttachRequest,
        frames: &mpsc::UnboundedSender<WsMessage>,
    ) -> Option<(AttachId, FnvHashMap<SubscriptionId, DateTime<Utc>>)> {
        let held = self
            .registrations
            .iter()
            .find(|(_, registration)| {
                matches!(registration.sink, Sink::Held { .. })
                    && registration.is_requested_by(request)
            })
            .map(|(id, _)| *id)?;

        let mut registration = self.registrations.remove(&held)?;
        self.index.unlink(held, &registration.slots);

        if let Sink::Held { frames: kept, .. } =
            std::mem::replace(&mut registration.sink, Sink::Live(frames.clone()))
        {
            for frame in kept {
                // Fails only if the stream has already dropped the receiver it is about to be
                // handed, and then its detach will follow.
                let _ = frames.send(frame);
            }
        }

        let starts = registration.starts.clone();
        Some((self.insert(registration), starts))
    }

    /// Drop the lost registration `request`'s stream left behind: the request supersedes it.
    fn forget_lost(&mut self, request: &AttachRequest) {
        let lost = self.registrations.iter().find_map(|(id, registration)| {
            (!registration.is_routed() && registration.is_requested_by(request)).then_some(*id)
        });

        if let Some(id) = lost {
            self.registrations.remove(&id);
        }
    }

    /// Every attached or held registration loses its socket.
    ///
    /// Returns each held registration whose frames go with it: its stream had not re-attached to
    /// the reconnect that re-subscribed it before that socket was lost as well.
    fn lose_all(&mut self) -> Vec<Discarded> {
        let discarded = self
            .registrations
            .values_mut()
            .filter_map(|registration| {
                match std::mem::replace(&mut registration.sink, Sink::Lost) {
                    Sink::Held { frames, .. } if !frames.is_empty() => {
                        Some(Discarded::new(registration, frames.len()))
                    }
                    _ => None,
                }
            })
            .collect();
        self.index.clear();

        discarded
    }

    /// A new socket re-subscribes what the lost one held, and holds frames for each registration
    /// until its stream re-attaches or `until` passes.
    fn hold_lost(&mut self, until: Instant) {
        for (id, registration) in &mut self.registrations {
            if registration.is_routed() {
                continue;
            }

            registration.sink = Sink::Held {
                frames: Vec::new(),
                until,
            };
            registration.starts.clear();
            registration.replayed.clear();
            self.index.link(*id, &registration.slots);
        }
    }

    /// Open each resumed symbol in `sending` at the earliest watermark among the registrations
    /// holding it, record the window on each of them, and return the `start` to send per symbol.
    ///
    /// A registration with no watermark for a symbol asks for no window, and does not hold the
    /// symbol's start back either.
    fn plan_starts(&mut self, sending: &[Slot]) -> FnvHashMap<SmolStr, DateTime<Utc>> {
        let mut starts = FnvHashMap::default();

        for slot in sending {
            let Slot::Symbol(symbol) = slot else {
                continue;
            };
            let Some(holders) = self.index.by_symbol.get(symbol.as_str()) else {
                continue;
            };

            let marks = holders
                .iter()
                .filter_map(|id| {
                    let watermark = self.registrations.get(id)?.watermark(symbol)?;
                    Some((*id, watermark))
                })
                .collect::<Vec<_>>();

            let Some(start) = marks.iter().map(|(_, watermark)| *watermark).min() else {
                continue;
            };

            starts.insert(symbol.clone(), start);

            for (id, _) in marks {
                if let Some(registration) = self.registrations.get_mut(&id) {
                    registration.starts.insert(subscription_id(symbol), start);
                    registration.replayed.insert(symbol.clone());
                }
            }
        }

        starts
    }

    /// The symbols `id` holds that were already on the socket, and that it has a watermark for:
    /// the ones it wanted a replay of and cannot be given one.
    fn joined_without_replay(&self, id: AttachId, sending: &[Slot]) -> Vec<SmolStr> {
        let Some(registration) = self.registrations.get(&id) else {
            return Vec::new();
        };

        registration
            .slots
            .iter()
            .filter(|slot| !sending.contains(slot))
            .filter_map(|slot| match slot {
                Slot::Symbol(symbol) => registration
                    .watermark(symbol)
                    .is_some()
                    .then(|| symbol.clone()),
                Slot::Underlying(_) => None,
            })
            .collect()
    }

    fn starts(&self, id: AttachId) -> FnvHashMap<SubscriptionId, DateTime<Utc>> {
        self.registrations
            .get(&id)
            .map(|registration| registration.starts.clone())
            .unwrap_or_default()
    }

    /// Deliver one frame to every registration holding `symbol`.
    ///
    /// An exact symbol reaches plain subscriptions; an option contract reaches the options
    /// subscriptions holding its underlying. A replayed frame reaches only the registrations a
    /// window was opened for.
    fn route(&mut self, symbol: &str, replay: bool, frame: &WsMessage) {
        let holders = self.index.by_symbol.get(symbol).or_else(|| {
            osi::root(symbol).and_then(|underlying| self.index.by_underlying.get(underlying))
        });

        let Some(holders) = holders else {
            // Normal for a moment after an unsubscribe, while frames already in flight arrive.
            debug!(
                symbol,
                "London Strategic Edge frame for a symbol no stream holds"
            );
            return;
        };

        for id in holders {
            let Some(registration) = self.registrations.get_mut(id) else {
                continue;
            };
            if replay && !registration.replayed.contains(symbol) {
                continue;
            }

            match &mut registration.sink {
                // A closed receiver means the stream is gone and its detach is on the way.
                Sink::Live(tx) => {
                    let _ = tx.send(frame.clone());
                }
                Sink::Held { frames, .. } => frames.push(frame.clone()),
                Sink::Lost => {}
            }
        }
    }

    fn next_expiry(&self) -> Option<Instant> {
        self.registrations
            .values()
            .filter_map(|registration| match registration.sink {
                Sink::Held { until, .. } => Some(until),
                _ => None,
            })
            .min()
    }

    /// Discard every held registration whose stream has not re-attached by `now`.
    ///
    /// Returns what was discarded, and the slots nothing holds any longer.
    fn expire(&mut self, now: Instant) -> (Vec<Discarded>, Vec<Slot>) {
        let expired = self
            .registrations
            .iter()
            .filter_map(|(id, registration)| match registration.sink {
                Sink::Held { until, .. } if until <= now => Some(*id),
                _ => None,
            })
            .collect::<Vec<_>>();

        let mut discarded = Vec::with_capacity(expired.len());
        let mut orphaned = Vec::new();

        for id in expired {
            if let Some(registration) = self.registrations.remove(&id) {
                orphaned.extend(self.index.unlink(id, &registration.slots));
                let frames = match &registration.sink {
                    Sink::Held { frames, .. } => frames.len(),
                    _ => 0,
                };
                discarded.push(Discarded::new(&registration, frames));
            }
        }

        (discarded, orphaned)
    }
}

/// The frames held for a stream that never received them, for the warning that reports them.
#[derive(Debug, PartialEq, Eq)]
struct Discarded {
    exchange: ExchangeId,
    kind: &'static str,
    subscriptions: usize,
    frames: usize,
}

impl Discarded {
    fn new(registration: &Registration, frames: usize) -> Self {
        Self {
            exchange: registration.exchange,
            kind: registration.kind,
            subscriptions: registration.markets.len(),
            frames,
        }
    }
}

/// The socket, and what the handshake said about it.
struct Socket {
    websocket: WebSocket,
    offered: OfferedSymbols,
    max_subscriptions: Option<u32>,
    /// The slots subscribed on this socket and confirmed.
    subscribed: FnvHashSet<Slot>,
}

/// The task that owns the socket.
struct Actor {
    credentials: LseCredentials,
    commands: mpsc::UnboundedReceiver<Command>,
    /// Handed to each attachment for its detach. Weak, so the actor stops once every subscriber
    /// clone and every attachment is gone.
    detach: mpsc::WeakUnboundedSender<Command>,
    socket: Option<Socket>,
    /// The endpoint every registration was attached through: that of the last socket opened.
    endpoint: Option<Url>,
    registry: Registry,
}

enum Event {
    Command(Option<Command>),
    Frame(Option<Result<WsMessage, WsError>>),
    Expiry,
}

/// What reading one frame amounted to.
enum Read {
    /// Routed, or nothing to act on.
    Done,
    /// A subscribe was answered: confirmed, or rejected.
    Answer(Result<LseSubResponse, SocketError>),
    /// The socket is gone, for the reason given.
    Lost(String),
}

impl Actor {
    fn new(
        credentials: LseCredentials,
        commands: mpsc::UnboundedReceiver<Command>,
        detach: mpsc::WeakUnboundedSender<Command>,
    ) -> Self {
        Self {
            credentials,
            commands,
            detach,
            socket: None,
            endpoint: None,
            registry: Registry::default(),
        }
    }

    async fn run(mut self) {
        loop {
            let expiry = self.registry.next_expiry();

            let event = tokio::select! {
                command = self.commands.recv() => Event::Command(command),
                frame = next_frame(&mut self.socket) => Event::Frame(frame),
                () = sleep_until(expiry) => Event::Expiry,
            };

            match event {
                Event::Command(Some(Command::Attach(request, reply))) => {
                    let attached = self.attach(*request).await;
                    // A caller that stopped waiting drops the attachment with the reply, and its
                    // detach follows.
                    let _ = reply.send(attached);
                }
                Event::Command(Some(Command::Detach(id))) => self.detach(id).await,
                Event::Command(None) => break,
                Event::Frame(frame) => match self.read(frame) {
                    Read::Answer(Ok(response)) => {
                        debug!(
                            ?response,
                            "London Strategic Edge answer outside a subscribe"
                        );
                    }
                    Read::Answer(Err(error)) => warn!(
                        %error,
                        "London Strategic Edge rejected something outside a subscribe; the \
                         rejection need not name a symbol, so check whether a stream on this \
                         connection has gone quiet",
                    ),
                    Read::Done | Read::Lost(_) => {}
                },
                Event::Expiry => self.expire().await,
            }
        }

        self.close().await;
    }

    async fn attach(&mut self, request: AttachRequest) -> Result<LseAttachment, SocketError> {
        // Checked whether or not the socket is open: a reconnect re-subscribes every registration
        // on the endpoint the request names, so they must all have been attached through it.
        if let Some(endpoint) = &self.endpoint
            && *endpoint != request.url
            && !self.registry.is_empty()
        {
            return Err(SocketError::Subscribe(format!(
                "{} is served from {}, but this London Strategic Edge connection is open to {} - \
                 streams sharing a connection must share an endpoint",
                request.exchange, request.url, endpoint,
            )));
        }

        let fresh = match &self.socket {
            Some(_) => {
                let (tx, rx) = mpsc::unbounded_channel();
                if let Some((id, starts)) = self.registry.reattach(&request, &tx) {
                    debug!(
                        exchange = %request.exchange,
                        "re-attached to the London Strategic Edge connection's reconnect",
                    );
                    return Ok(self.attachment(id, rx, starts));
                }

                false
            }
            None => {
                self.registry.forget_lost(&request);
                self.connect(&request.url).await?;
                self.registry.hold_lost(Instant::now() + REATTACH_GRACE);

                true
            }
        };

        let attached = self.subscribe(request, fresh).await;

        // A reconnect that did not complete leaves nothing trustworthy on the socket, so every
        // stream goes back to waiting for the next one.
        if attached.is_err() && fresh {
            self.disconnect().await;
        }

        attached
    }

    /// Check `request`, register it, and subscribe whatever the socket does not hold yet.
    ///
    /// On a `fresh` socket that is every registration's slots — the reconnect re-subscribing what
    /// the lost one held — and on an established one only the request's own.
    async fn subscribe(
        &mut self,
        request: AttachRequest,
        fresh: bool,
    ) -> Result<LseAttachment, SocketError> {
        let Some(socket) = &self.socket else {
            return Err(SocketError::Subscribe(
                "London Strategic Edge connection is not open".to_owned(),
            ));
        };

        let exchange = request.exchange;
        let timeout = request.timeout;
        let slots = request.slots();

        // The offered-symbol list does not hold every underlying that has options, and an
        // underlying without them is rejected by name rather than confirmed - see `LseOptions`.
        if request.underlyings.is_none() {
            check_symbols_are_offered(exchange, &request.markets, &socket.offered)?;
        }

        let added = slots
            .iter()
            .filter(|slot| !self.registry.index.contains(slot))
            .count();
        check_subscription_cap(
            exchange,
            added,
            self.registry.index.len(),
            socket.max_subscriptions,
        )?;

        // Request order first, so a batch reaches the wire in the order it was asked for.
        let mut seen = FnvHashSet::default();
        let sending = slots
            .iter()
            .cloned()
            .chain(
                fresh
                    .then(|| self.registry.index.slots())
                    .into_iter()
                    .flatten(),
            )
            .filter(|slot| !socket.subscribed.contains(slot) && seen.insert(slot.clone()))
            .collect::<Vec<_>>();

        let (tx, rx) = mpsc::unbounded_channel();
        let id = self.registry.insert_live(request, slots, tx);

        match self
            .subscribe_registered(id, exchange, &sending, timeout)
            .await
        {
            Ok(()) => {
                if let Some(socket) = self.socket.as_mut() {
                    socket.subscribed.extend(sending);
                }

                let starts = self.registry.starts(id);
                Ok(self.attachment(id, rx, starts))
            }
            Err(error) => {
                let orphaned = self.registry.remove(id);
                if !fresh {
                    // What this attach sent may be held by the provider even though the
                    // handshake failed; unsubscribing reclaims the slots.
                    self.release(orphaned).await;
                }

                Err(error)
            }
        }
    }

    async fn subscribe_registered(
        &mut self,
        id: AttachId,
        exchange: ExchangeId,
        sending: &[Slot],
        timeout: Duration,
    ) -> Result<(), SocketError> {
        let starts = self.registry.plan_starts(sending);

        let joined = self.registry.joined_without_replay(id, sending);
        if !joined.is_empty() {
            warn!(
                %exchange,
                symbols = ?joined,
                "London Strategic Edge symbols this stream resumes were already streaming on the \
                 shared connection, and the provider replays nothing for a symbol it already \
                 streams; they resume live, and what was missed since the last delivery is not \
                 recovered",
            );
        }

        for slot in sending {
            let start = match slot {
                Slot::Symbol(symbol) => starts.get(symbol.as_str()).copied(),
                Slot::Underlying(_) => None,
            };

            let message = slot.subscribe(start);
            debug!(%exchange, payload = ?message, "sending London Strategic Edge subscription");
            self.send(message).await?;
        }

        self.await_confirmations(sending.len(), timeout).await?;

        // Only for what was sent: an attach whose symbols the socket already holds sends nothing.
        if !sending.is_empty() {
            debug!(
                %exchange,
                count = sending.len(),
                "London Strategic Edge subscriptions confirmed",
            );
        }
        Ok(())
    }

    /// Read until `expected` subscribes are confirmed, routing everything else as usual.
    async fn await_confirmations(
        &mut self,
        expected: usize,
        timeout: Duration,
    ) -> Result<(), SocketError> {
        let deadline = Instant::now() + timeout;
        let mut confirmed = 0;

        while confirmed < expected {
            let frame = tokio::time::timeout_at(deadline, next_frame(&mut self.socket))
                .await
                .map_err(|_| {
                    SocketError::Subscribe(format!(
                        "subscription validation timeout reached: {timeout:?}"
                    ))
                })?;

            match self.read(frame) {
                Read::Answer(Ok(response)) => {
                    confirmed += 1;
                    debug!(
                        confirmed,
                        expected,
                        ?response,
                        "London Strategic Edge confirmation"
                    );
                }
                Read::Answer(Err(error)) => return Err(shared_cap_context(error)),
                Read::Lost(cause) => {
                    return Err(SocketError::Subscribe(format!(
                        "London Strategic Edge connection lost while subscribing: {cause}"
                    )));
                }
                Read::Done => {}
            }
        }

        Ok(())
    }

    async fn detach(&mut self, id: AttachId) {
        if let Some(orphaned) = self.registry.remove_live(id) {
            self.release(orphaned).await;
        }
    }

    async fn expire(&mut self) {
        let (discarded, orphaned) = self.registry.expire(Instant::now());

        for discarded in discarded {
            warn!(
                exchange = %discarded.exchange,
                kind = discarded.kind,
                subscriptions = discarded.subscriptions,
                discarded_frames = discarded.frames,
                grace = ?REATTACH_GRACE,
                "a London Strategic Edge stream did not re-attach after a reconnect; the frames \
                 held for it are discarded, and a later re-attach is a new attach, which gets no \
                 replay for a symbol another stream still holds",
            );
        }

        self.release(orphaned).await;
    }

    /// Unsubscribe `slots`, which nothing holds any longer, or close the socket if nothing at all
    /// remains attached.
    async fn release(&mut self, slots: Vec<Slot>) {
        if self.registry.is_empty() {
            self.close().await;
            return;
        }

        for slot in slots {
            if let Some(socket) = self.socket.as_mut() {
                socket.subscribed.remove(&slot);
            }

            let message = slot.unsubscribe();
            debug!(payload = ?message, "releasing a London Strategic Edge subscription");
            if self.send(message).await.is_err() {
                return;
            }
        }
    }

    async fn connect(&mut self, url: &Url) -> Result<(), SocketError> {
        let mut websocket = connect(url.clone()).await?;
        let authenticated = authenticate(&mut websocket, &self.credentials).await?;
        debug!(
            %url,
            tier = ?authenticated.tier,
            offered = authenticated.symbols.len(),
            "authenticated to London Strategic Edge WebSocket",
        );

        self.endpoint = Some(url.clone());
        self.socket = Some(Socket {
            websocket,
            offered: OfferedSymbols::new(authenticated.symbols),
            max_subscriptions: authenticated.max_subscriptions,
            subscribed: FnvHashSet::default(),
        });

        Ok(())
    }

    /// Send one frame. A failed send loses the socket.
    async fn send(&mut self, message: WsMessage) -> Result<(), SocketError> {
        let Some(socket) = self.socket.as_mut() else {
            return Err(SocketError::Subscribe(
                "London Strategic Edge connection is not open".to_owned(),
            ));
        };

        if let Err(error) = socket.websocket.send(message).await {
            let cause = error.to_string();
            self.lose(&cause);
            return Err(SocketError::WebSocket(Box::new(error)));
        }

        Ok(())
    }

    /// Classify one read, routing it if it is addressed to a stream.
    fn read(&mut self, frame: Option<Result<WsMessage, WsError>>) -> Read {
        let message = match frame {
            Some(Ok(message)) => message,
            Some(Err(error)) => return self.lose(&error.to_string()),
            None => return self.lose("the socket ended"),
        };

        match classify(&message) {
            Frame::Route { symbol, replay } => {
                self.registry.route(&symbol, replay, &message);
                Read::Done
            }
            Frame::Answer(answer) => Read::Answer(answer),
            Frame::Closed(cause) => self.lose(&cause),
            Frame::Ignored => Read::Done,
        }
    }

    /// The socket is gone: every stream on it ends, and waits for one of them to reconnect.
    fn lose(&mut self, cause: &str) -> Read {
        if self.socket.take().is_some() {
            warn!(
                cause,
                streams = self.registry.registrations.len(),
                "London Strategic Edge connection lost; every stream sharing it ends, and the \
                 first to re-attach reconnects for all of them",
            );
        }
        self.lose_all();

        Read::Lost(cause.to_owned())
    }

    /// Close the socket deliberately, and leave every registration waiting for a reconnect.
    async fn disconnect(&mut self) {
        self.close().await;
        self.lose_all();
    }

    fn lose_all(&mut self) {
        for discarded in self.registry.lose_all() {
            warn!(
                exchange = %discarded.exchange,
                kind = discarded.kind,
                subscriptions = discarded.subscriptions,
                discarded_frames = discarded.frames,
                "a London Strategic Edge reconnect was lost before this stream re-attached to it; \
                 the frames held for it are discarded. A stream that resumes asks for them again \
                 from its watermark on the next reconnect; one that does not never receives them",
            );
        }
    }

    async fn close(&mut self) {
        if let Some(mut socket) = self.socket.take() {
            debug!(endpoint = ?self.endpoint, "closing London Strategic Edge connection");
            // Best effort: the connection is being discarded either way.
            let _ = socket.websocket.close(None).await;
        }
    }

    fn attachment(
        &self,
        id: AttachId,
        frames: mpsc::UnboundedReceiver<WsMessage>,
        starts: FnvHashMap<SubscriptionId, DateTime<Utc>>,
    ) -> LseAttachment {
        LseAttachment {
            frames,
            starts,
            _detach: Detach {
                id,
                commands: self.detach.upgrade(),
            },
        }
    }
}

/// Prefix an in-handshake rejection with what the stream cannot see: its cap is shared.
fn shared_cap_context(error: SocketError) -> SocketError {
    match error {
        SocketError::Subscribe(message) => SocketError::Subscribe(format!(
            "{message} (the connection and its subscription cap are shared by every stream opened \
             by this subscriber and its clones)"
        )),
        other => other,
    }
}

async fn next_frame(socket: &mut Option<Socket>) -> Option<Result<WsMessage, WsError>> {
    match socket {
        Some(socket) => socket.websocket.next().await,
        None => future::pending().await,
    }
}

async fn sleep_until(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => future::pending().await,
    }
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

enum Frame {
    Route { symbol: SmolStr, replay: bool },
    Answer(Result<LseSubResponse, SocketError>),
    Closed(String),
    Ignored,
}

fn classify(message: &WsMessage) -> Frame {
    let text = match message {
        WsMessage::Text(text) => text.as_str(),
        WsMessage::Binary(bytes) => match std::str::from_utf8(bytes) {
            Ok(text) => text,
            Err(_) => {
                warn!(
                    len = bytes.len(),
                    "London Strategic Edge sent a binary frame that is not UTF-8; ignored"
                );
                return Frame::Ignored;
            }
        },
        WsMessage::Close(frame) => {
            return Frame::Closed(format!("closed by the provider: {frame:?}"));
        }
        // Pings are answered by the socket itself.
        _ => return Frame::Ignored,
    };

    let envelope = match serde_json::from_str::<Envelope>(text) {
        Ok(envelope) => envelope,
        Err(error) => {
            warn!(
                %error,
                frame = %text.chars().take(200).collect::<String>(),
                "London Strategic Edge sent a frame that is not a JSON object; ignored",
            );
            return Frame::Ignored;
        }
    };

    match (envelope.kind.as_deref(), envelope.symbol) {
        (Some("subscribed" | "options_subscribed" | "error"), _) => Frame::Answer(
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
            Frame::Ignored
        }
        // A replay's boundaries belong to the replay: only a stream that asked for one reads them.
        (Some("replay_started" | "replay_complete"), Some(symbol)) => Frame::Route {
            symbol,
            replay: true,
        },
        (_, Some(symbol)) => Frame::Route {
            symbol,
            replay: envelope.replay.unwrap_or(false),
        },
        (kind, None) => {
            debug!(?kind, "London Strategic Edge control frame");
            Frame::Ignored
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use crate::subscription::{SubscriptionKind, book::OrderBooksL1, trade::PublicTrades};

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
        registry: &mut Registry,
        request: AttachRequest,
    ) -> (AttachId, mpsc::UnboundedReceiver<WsMessage>) {
        let (tx, rx) = mpsc::unbounded_channel();
        let slots = request.slots();
        (registry.insert_live(request, slots, tx), rx)
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

        assert_eq!(registry.index.len(), 2);

        let frame = text(serde_json::json!({"type": "tick", "symbol": "BTC/USD"}));
        registry.route("BTC/USD", false, &frame);

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
        registry.route("SPY260930C00701000", false, &contract);
        registry.route("SPY", false, &share);

        assert_eq!(drain(&mut options), [contract]);
        assert_eq!(drain(&mut equities), [share]);
    }

    #[test]
    fn the_last_detach_orphans_a_slot_and_an_earlier_one_does_not() {
        let mut registry = Registry::default();
        let (first, _rx1) = live(
            &mut registry,
            request(ExchangeId::LseCrypto, TRADES, &["BTC/USD", "ETH/USD"], None),
        );
        let (second, _rx2) = live(
            &mut registry,
            request(ExchangeId::LseFx, TRADES, &["BTC/USD"], None),
        );

        assert_eq!(registry.remove_live(first), Some(vec![symbol("ETH/USD")]));
        assert_eq!(registry.remove_live(second), Some(vec![symbol("BTC/USD")]));
        assert!(registry.is_empty());
    }

    /// The stream's old attachment is dropped as the stream ends, which is before it re-attaches.
    /// That detach must not remove the registration the reconnect re-subscribes for it.
    #[test]
    fn a_detach_after_the_socket_is_lost_keeps_the_registration() {
        let mut registry = Registry::default();
        let (id, _rx) = live(
            &mut registry,
            request(ExchangeId::LseCrypto, TRADES, &["BTC/USD"], None),
        );

        registry.lose_all();

        assert_eq!(registry.remove_live(id), None);
        assert!(!registry.is_empty());
    }

    #[test]
    fn a_reconnect_holds_frames_until_the_stream_re_attaches_then_delivers_them_in_order() {
        let mut registry = Registry::default();
        let batch = || request(ExchangeId::LseCrypto, TRADES, &["BTC/USD"], None);
        let (old, _rx) = live(&mut registry, batch());

        registry.lose_all();
        registry.hold_lost(Instant::now() + REATTACH_GRACE);

        let first = text(serde_json::json!({"symbol": "BTC/USD", "n": 1}));
        let second = text(serde_json::json!({"symbol": "BTC/USD", "n": 2}));
        registry.route("BTC/USD", false, &first);
        registry.route("BTC/USD", false, &second);

        let (tx, mut rx) = mpsc::unbounded_channel();
        let (id, _) = registry.reattach(&batch(), &tx).unwrap();

        assert_ne!(
            id, old,
            "a re-attach must not be removable by the old detach"
        );
        assert_eq!(drain(&mut rx), [first, second]);
        assert_eq!(registry.remove_live(old), None);
    }

    #[test]
    fn a_different_batch_does_not_re_attach_to_a_held_registration() {
        let mut registry = Registry::default();
        live(
            &mut registry,
            request(ExchangeId::LseCrypto, TRADES, &["BTC/USD"], None),
        );
        registry.lose_all();
        registry.hold_lost(Instant::now() + REATTACH_GRACE);

        let (tx, _rx) = mpsc::unbounded_channel();
        let state = Arc::new(LseResumeState::new());
        for other in [
            request(ExchangeId::LseCrypto, TRADES, &["ETH/USD"], None),
            request(
                ExchangeId::LseCrypto,
                OrderBooksL1.as_str(),
                &["BTC/USD"],
                None,
            ),
            request(ExchangeId::LseFx, TRADES, &["BTC/USD"], None),
            request(ExchangeId::LseCrypto, TRADES, &["BTC/USD"], Some(&state)),
        ] {
            assert!(registry.reattach(&other, &tx).is_none(), "{other:?}");
        }
    }

    #[test]
    fn a_held_registration_expires_and_releases_what_only_it_held() {
        let mut registry = Registry::default();
        let (_, _rx) = live(
            &mut registry,
            request(ExchangeId::LseCrypto, TRADES, &["BTC/USD", "ETH/USD"], None),
        );
        registry.lose_all();

        let until = Instant::now() + REATTACH_GRACE;
        registry.hold_lost(until);
        let (_, _rx2) = live(
            &mut registry,
            request(
                ExchangeId::LseCrypto,
                OrderBooksL1.as_str(),
                &["BTC/USD"],
                None,
            ),
        );
        registry.route("BTC/USD", false, &text(serde_json::json!({})));

        assert_eq!(registry.next_expiry(), Some(until));
        assert!(
            registry
                .expire(until - Duration::from_millis(1))
                .0
                .is_empty()
        );

        let (discarded, orphaned) = registry.expire(until);
        assert_eq!(
            discarded,
            [Discarded {
                exchange: ExchangeId::LseCrypto,
                kind: TRADES,
                subscriptions: 2,
                frames: 1,
            }]
        );
        assert_eq!(orphaned, [symbol("ETH/USD")]);
        assert_eq!(registry.next_expiry(), None);
    }

    /// A second loss inside the grace window takes the frames held for a stream that has not
    /// re-attached yet. They must be reported, as an expiry reports them, and the registration kept
    /// for the next reconnect.
    #[test]
    fn a_loss_before_a_held_stream_re_attaches_reports_the_frames_it_discards() {
        let mut registry = Registry::default();
        let batch = || request(ExchangeId::LseCrypto, TRADES, &["BTC/USD"], None);
        let quiet = || {
            request(
                ExchangeId::LseCrypto,
                OrderBooksL1.as_str(),
                &["ETH/USD"],
                None,
            )
        };
        live(&mut registry, batch());
        live(&mut registry, quiet());

        assert!(registry.lose_all().is_empty(), "nothing was held yet");

        registry.hold_lost(Instant::now() + REATTACH_GRACE);
        registry.route("BTC/USD", false, &text(serde_json::json!({})));
        registry.route("BTC/USD", false, &text(serde_json::json!({})));

        assert_eq!(
            registry.lose_all(),
            [Discarded {
                exchange: ExchangeId::LseCrypto,
                kind: TRADES,
                subscriptions: 1,
                frames: 2,
            }],
            "a held registration with no frames discards nothing, and is not reported",
        );

        registry.hold_lost(Instant::now() + REATTACH_GRACE);
        let (tx, mut rx) = mpsc::unbounded_channel();
        assert!(registry.reattach(&batch(), &tx).is_some());
        assert!(drain(&mut rx).is_empty(), "the discarded frames were held");
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

        let starts = registry.plan_starts(&[symbol("BTC/USD")]);

        assert_eq!(starts.get("BTC/USD"), Some(&at("2026-08-14T10:00:01Z")));
        for id in [trades, quotes] {
            assert_eq!(
                registry.starts(id).get(&subscription_id("BTC/USD")),
                Some(&at("2026-08-14T10:00:01Z")),
            );
        }
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

        assert!(registry.plan_starts(&[symbol("AAPL")]).is_empty());
        assert!(registry.starts(futures).is_empty());
        assert!(registry.starts(books).is_empty());
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
        registry.plan_starts(&[symbol("BTC/USD")]);

        let replayed = text(serde_json::json!({"symbol": "BTC/USD", "replay": true}));
        let current = text(serde_json::json!({"symbol": "BTC/USD"}));
        registry.route("BTC/USD", true, &replayed);
        registry.route("BTC/USD", false, &current);

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
            registry.joined_without_replay(id, &[symbol("ETH/USD")]),
            ["BTC/USD"]
        );
        assert!(
            registry
                .joined_without_replay(id, &[symbol("BTC/USD"), symbol("ETH/USD")])
                .is_empty()
        );
    }

    #[test]
    fn frames_are_classified_by_their_routing_key_alone() {
        let classify_json = |value: serde_json::Value| classify(&text(value));

        assert!(matches!(
            classify_json(serde_json::json!({"type": "tick", "symbol": "BTC/USD", "price": 1.0})),
            Frame::Route { symbol, replay: false } if symbol == "BTC/USD"
        ));
        assert!(matches!(
            classify_json(serde_json::json!({"type": "tick", "symbol": "BTC/USD", "replay": true})),
            Frame::Route { replay: true, .. }
        ));
        assert!(matches!(
            classify_json(serde_json::json!({"type": "replay_started", "symbol": "BTC/USD"})),
            Frame::Route { replay: true, .. }
        ));
        assert!(matches!(
            classify_json(
                serde_json::json!({"type": "subscribed", "symbol": "BTC/USD",
                "message": "Already subscribed"})
            ),
            Frame::Answer(Ok(_))
        ));
        assert!(matches!(
            classify_json(serde_json::json!({"type": "options_subscribed", "underlying": "SPY"})),
            Frame::Answer(Ok(_))
        ));
        assert!(matches!(
            classify_json(serde_json::json!({"type": "error", "code": "LIMIT_REACHED"})),
            Frame::Answer(Err(_))
        ));
        assert!(matches!(
            classify_json(serde_json::json!({"type": "unsubscribed", "symbol": "BTC/USD"})),
            Frame::Ignored
        ));
        assert!(matches!(
            classify_json(serde_json::json!({"type": "welcome"})),
            Frame::Ignored
        ));
        assert!(matches!(
            classify(&WsMessage::Close(None)),
            Frame::Closed(_)
        ));
    }

    #[test]
    fn the_symbol_a_slot_occupies_is_released_with_the_matching_unsubscribe() {
        assert_eq!(
            symbol("BTC/USD").unsubscribe(),
            text(serde_json::json!({"action": "unsubscribe", "symbol": "BTC/USD"}))
        );
        assert_eq!(
            Slot::Underlying("SPY".into()).unsubscribe(),
            text(serde_json::json!({"action": "unsubscribe_options", "underlying": "SPY"}))
        );
    }

    #[test]
    fn a_trades_kind_spells_itself_as_the_tests_assume() {
        assert_eq!(PublicTrades.as_str(), TRADES);
    }
}
