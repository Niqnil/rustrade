//! One WebSocket per feed, shared by every stream an [`AlpacaSubscriber`](super::AlpacaSubscriber)
//! and its clones open on it.
//!
//! # Why the connection is shared
//! Alpaca allows an account one market data connection **per feed** — crypto, IEX and SIP each
//! count separately — and refuses a second on the same feed at authentication with
//! `connection limit exceeded`. One socket per stream could therefore never stream trades and
//! quotes from one feed at once.
//!
//! # Shape
//! A task per feed owns that feed's socket. Each stream *attaches* to it and is handed an
//! [`AlpacaAttachment`]: the frames carrying its own subscriptions, which it parses exactly as it
//! would parse a socket of its own.
//!
//! Alpaca packs messages for several symbols and channels into one frame — a JSON array such as
//! `[{"T":"t","S":"AAPL",..},{"T":"q","S":"MSFT",..}]` — so the connection splits frames rather
//! than forwarding them whole: a stream decoding an element for a subscription it does not hold
//! would report it as an error. The connection reads only each element's type `T` and symbol `S`.
//! A stream every element of a frame belongs to receives the frame itself, a reference-counted
//! clone; any other receives a frame holding only its own elements, copied verbatim. Each stream's
//! decoder stays the only full parse.
//!
//! - **Connect lazily, close when idle.** A feed's socket opens on its first attach and closes once
//!   no stream remains attached to it.
//! - **Subscribe once per channel and symbol.** A `(channel, symbol)` pair the socket already holds
//!   is not sent again, and its elements reach every stream holding it.
//! - **Unsubscribe on the last detach.** Dropping an attachment unsubscribes every pair no other
//!   stream holds.
//! - **One handshake at a time.** Alpaca answers both a subscribe and an unsubscribe with the
//!   connection's whole subscription state, and a refusal names nothing, so a frame answers only
//!   the request in flight. Attaches and detaches are therefore serialised, each waiting for its
//!   own answer; frames keep flowing to every attached stream meanwhile.
//! - **The subscription cap is the connection's.** Alpaca limits how many pairs one connection
//!   holds — 30 on the free IEX plan, as last measured — and refuses a subscribe that would pass
//!   it, adding none of it. The cap depends on the plan and is not announced, so it is not checked
//!   here: the refusal fails the attach, and says the cap is shared.
//!
//! # Reconnect
//! Every stream on a socket **ends together** when it is lost, and each is re-initialised by the
//! usual reconnect wrapper. The first to re-attach reconnects and re-subscribes everything the lost
//! socket held; the rest re-attach to that one reconnect. Frames for a stream that has not
//! re-attached yet are held for [`REATTACH_GRACE`], then discarded with a warning, and its pairs
//! released. Alpaca replays nothing, so what the provider sent while no socket was open is missed.
//!
//! # Sharing is by clone, and only by clone
//! Clones of one subscriber share one connection per feed. Two subscribers built separately for
//! one account open two sockets, and Alpaca refuses the second — visibly, which is left to surface
//! rather than papered over by a process-wide registry.

use super::{
    AlpacaCredentials, alpaca_authenticate,
    channel::AlpacaChannel,
    channel_message,
    subscription::{AlpacaSubResponse, AlpacaSubResponseInner},
    validator::covered,
};
use fnv::{FnvHashMap, FnvHashSet};
use futures::{SinkExt, Stream, StreamExt};
use rustrade_instrument::exchange::ExchangeId;
use rustrade_integration::{
    Validator,
    error::SocketError,
    protocol::websocket::{WebSocket, WsError, WsMessage, connect},
};
use serde::Deserialize;
use serde_json::value::RawValue;
use smol_str::SmolStr;
use std::{
    future,
    pin::Pin,
    sync::{Mutex, PoisonError},
    task::{Context, Poll},
    time::Duration,
};
use tokio::{
    sync::{mpsc, oneshot},
    time::Instant,
};
use tracing::{debug, error, warn};
use url::Url;

/// How long a connection holds frames for a stream that has not re-attached after a reconnect.
///
/// A stream re-attaches as soon as it has drained what it was sent before the socket was lost,
/// which is normally milliseconds. The bound covers a consumer that stalls for a while; past it the
/// held frames are discarded with a warning and the stream's pairs released, and a late re-attach
/// is treated as a new attach.
pub const REATTACH_GRACE: Duration = Duration::from_secs(30);

/// How long a detach waits for Alpaca to answer its unsubscribe before moving on.
const RELEASE_TIMEOUT: Duration = Duration::from_secs(10);

/// Identifies one attach for the lifetime of the connection task. Never reused.
type AttachId = u64;

/// One subscription on the socket: a channel and a symbol, spelled as Alpaca spells it.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub(super) struct Slot {
    pub(super) channel: AlpacaChannel,
    pub(super) symbol: SmolStr,
}

fn subscribe_message(slots: &[Slot]) -> WsMessage {
    channel_message("subscribe", slot_pairs(slots))
}

fn unsubscribe_message(slots: &[Slot]) -> WsMessage {
    channel_message("unsubscribe", slot_pairs(slots))
}

fn slot_pairs(slots: &[Slot]) -> impl Iterator<Item = (AlpacaChannel, &str)> {
    slots
        .iter()
        .map(|slot| (slot.channel, slot.symbol.as_str()))
}

/// The handle an `AlpacaSubscriber` and its clones share: the way into each feed's connection task.
///
/// A feed's task is spawned on its first attach rather than on construction, because a subscriber
/// can be built outside a runtime, and again if the runtime that ran it has since shut down.
#[derive(Debug)]
pub(super) struct AlpacaConnections {
    credentials: AlpacaCredentials,
    feeds: Mutex<FnvHashMap<Url, mpsc::UnboundedSender<Command>>>,
}

impl AlpacaConnections {
    pub(super) fn new(credentials: AlpacaCredentials) -> Self {
        Self {
            credentials,
            feeds: Mutex::new(FnvHashMap::default()),
        }
    }

    /// Attach one stream's batch to its feed's connection, connecting first if nothing is attached.
    pub(super) async fn attach(
        &self,
        request: AttachRequest,
    ) -> Result<AlpacaAttachment, SocketError> {
        let (reply, answer) = oneshot::channel();

        self.commands(&request.url)
            .send(Command::Attach(Box::new(request), reply))
            .map_err(|_| actor_stopped())?;

        answer.await.map_err(|_| actor_stopped())?
    }

    fn commands(&self, url: &Url) -> mpsc::UnboundedSender<Command> {
        // A poisoned lock guards senders, which are valid whatever state the panic left behind.
        let mut feeds = self.feeds.lock().unwrap_or_else(PoisonError::into_inner);

        if let Some(commands) = feeds.get(url)
            && !commands.is_closed()
        {
            return commands.clone();
        }

        let (tx, rx) = mpsc::unbounded_channel();
        let actor = Actor::new(self.credentials.clone(), url.clone(), rx, tx.downgrade());
        tokio::spawn(report_panic(tokio::spawn(actor.run())));
        feeds.insert(url.clone(), tx.clone());

        tx
    }
}

/// Log a panic of a connection task, which its streams would otherwise see only as a lost socket.
///
/// Every stream sharing the connection ends when the task does, and the next attach spawns a new
/// one, so the panic is not propagated anywhere; this is the one place it is reported.
async fn report_panic(actor: tokio::task::JoinHandle<()>) {
    if let Err(error) = actor.await
        && error.is_panic()
    {
        error!(
            %error,
            "the Alpaca connection task panicked; every stream sharing the connection ends, and \
             the next to re-attach starts a new connection",
        );
    }
}

fn actor_stopped() -> SocketError {
    SocketError::Subscribe("the Alpaca connection task stopped before answering".to_owned())
}

/// One stream's batch, as the connection needs it.
#[derive(Debug)]
pub(super) struct AttachRequest {
    pub(super) exchange: ExchangeId,
    pub(super) url: Url,
    /// The distinct pairs requested, in request order.
    pub(super) slots: Vec<Slot>,
    pub(super) timeout: Duration,
}

#[derive(Debug)]
enum Command {
    Attach(
        Box<AttachRequest>,
        oneshot::Sender<Result<AlpacaAttachment, SocketError>>,
    ),
    Detach(AttachId),
}

/// One stream's view of an Alpaca feed's shared connection.
///
/// A stream of frames holding only the stream's own subscriptions, in the order the socket
/// delivered them, so it is parsed exactly as a socket of the stream's own would be. It ends when
/// the connection is lost, which is what drives the stream's reconnect.
///
/// Dropping it detaches the stream: every subscription no other stream holds is unsubscribed, and
/// the socket closes once nothing remains attached.
///
/// # Keep it drained
/// The queue behind it is unbounded. The connection reads one socket for every stream sharing it,
/// so it never waits for a slow one — that would stall the rest — and a stream it serves is not
/// back-pressured the way a socket of its own would be. Frames for a stream that stops being polled
/// accumulate in memory until it is polled again or dropped.
///
/// See [`connection`](self) for how the connection is shared.
#[derive(Debug)]
pub struct AlpacaAttachment {
    frames: mpsc::UnboundedReceiver<WsMessage>,
    _detach: Detach,
}

impl Stream for AlpacaAttachment {
    type Item = Result<WsMessage, WsError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.frames.poll_recv(cx).map(|frame| frame.map(Ok))
    }
}

/// Tells the connection task an attachment is gone.
#[derive(Debug)]
struct Detach {
    id: AttachId,
    commands: Option<mpsc::UnboundedSender<Command>>,
}

impl Drop for Detach {
    fn drop(&mut self) {
        if let Some(commands) = &self.commands {
            // The task having stopped already is the one way this fails, and then there is
            // nothing left to detach from.
            let _ = commands.send(Command::Detach(self.id));
        }
    }
}

/// One attached stream's batch, and where its frames go.
#[derive(Debug)]
struct Registration {
    exchange: ExchangeId,
    slots: Vec<Slot>,
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
    /// The reconnect wrapper re-initialises a stream with the batch it was built with, so an equal
    /// batch is the same stream — or an exact duplicate of it, which is interchangeable with it.
    fn is_requested_by(&self, request: &AttachRequest) -> bool {
        self.exchange == request.exchange && self.slots == request.slots
    }

    fn is_routed(&self) -> bool {
        !matches!(self.sink, Sink::Lost)
    }

    fn deliver(&mut self, id: AttachId, frame: WsMessage) {
        match &mut self.sink {
            Sink::Live(tx) => {
                if tx.send(frame).is_err() {
                    // Normal for a moment: the stream is gone and its detach is on the way.
                    debug!(
                        exchange = %self.exchange,
                        id,
                        "Alpaca frame for a stream that has already dropped its attachment",
                    );
                }
            }
            Sink::Held { frames, .. } => frames.push(frame),
            Sink::Lost => {}
        }
    }
}

/// Which attaches hold each pair — equivalently, what the socket holds.
///
/// One map per channel, so a frame element's symbol is looked up as the `&str` it arrives as.
#[derive(Debug, Default)]
struct Index {
    trades: FnvHashMap<SmolStr, Vec<AttachId>>,
    quotes: FnvHashMap<SmolStr, Vec<AttachId>>,
}

impl Index {
    fn map(&mut self, channel: AlpacaChannel) -> &mut FnvHashMap<SmolStr, Vec<AttachId>> {
        match channel {
            AlpacaChannel::Trades => &mut self.trades,
            AlpacaChannel::Quotes => &mut self.quotes,
        }
    }

    fn holders(&self, channel: AlpacaChannel, symbol: &str) -> Option<&Vec<AttachId>> {
        match channel {
            AlpacaChannel::Trades => self.trades.get(symbol),
            AlpacaChannel::Quotes => self.quotes.get(symbol),
        }
    }

    fn link(&mut self, id: AttachId, slots: &[Slot]) {
        for slot in slots {
            self.map(slot.channel)
                .entry(slot.symbol.clone())
                .or_default()
                .push(id);
        }
    }

    /// Remove `id` from `slots`, returning those nothing holds any longer.
    fn unlink(&mut self, id: AttachId, slots: &[Slot]) -> Vec<Slot> {
        let mut orphaned = Vec::new();

        for slot in slots {
            let map = self.map(slot.channel);
            if let Some(holders) = map.get_mut(slot.symbol.as_str()) {
                holders.retain(|holder| *holder != id);
                if holders.is_empty() {
                    map.remove(slot.symbol.as_str());
                    orphaned.push(slot.clone());
                }
            }
        }

        orphaned
    }

    fn slots(&self) -> impl Iterator<Item = Slot> + '_ {
        fn held(
            channel: AlpacaChannel,
            map: &FnvHashMap<SmolStr, Vec<AttachId>>,
        ) -> impl Iterator<Item = Slot> + '_ {
            map.keys().map(move |symbol| Slot {
                channel,
                symbol: symbol.clone(),
            })
        }

        held(AlpacaChannel::Trades, &self.trades).chain(held(AlpacaChannel::Quotes, &self.quotes))
    }

    fn clear(&mut self) {
        self.trades.clear();
        self.quotes.clear();
    }

    /// A frame of only the `elements` `id` holds, in their original order and spelling.
    fn share(&self, id: AttachId, elements: &[Element<'_>]) -> String {
        // Sized for every element, so it never grows: bounded by the frame it is cut from.
        let capacity = elements
            .iter()
            .map(|element| element.raw.get().len() + 1)
            .sum::<usize>()
            + 1;
        let mut share = String::with_capacity(capacity);

        share.push('[');
        for element in elements.iter().filter(|element| {
            self.holders(element.channel, &element.symbol)
                .is_some_and(|holders| holders.contains(&id))
        }) {
            if share.len() > 1 {
                share.push(',');
            }
            share.push_str(element.raw.get());
        }
        share.push(']');

        share
    }
}

/// One data element of a frame, as routing reads it.
#[derive(Debug)]
struct Element<'a> {
    channel: AlpacaChannel,
    symbol: SmolStr,
    raw: &'a RawValue,
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
        frames: mpsc::UnboundedSender<WsMessage>,
    ) -> AttachId {
        self.insert(Registration {
            exchange: request.exchange,
            slots: request.slots,
            sink: Sink::Live(frames),
        })
    }

    /// Remove a registration in any state, returning the pairs nothing holds any longer.
    fn remove(&mut self, id: AttachId) -> Vec<Slot> {
        match self.registrations.remove(&id) {
            Some(registration) if registration.is_routed() => {
                self.index.unlink(id, &registration.slots)
            }
            _ => Vec::new(),
        }
    }

    /// Remove an attached registration, returning the pairs nothing holds any longer.
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
    /// attachment cannot remove it.
    fn reattach(
        &mut self,
        request: &AttachRequest,
        frames: &mpsc::UnboundedSender<WsMessage>,
    ) -> Option<AttachId> {
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

        Some(self.insert(registration))
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
            self.index.link(*id, &registration.slots);
        }
    }

    /// Deliver a frame's data `elements` to the registrations holding them.
    ///
    /// A registration every element of `frame` belongs to — `total` counts the frame's elements of
    /// every kind — receives `frame` itself. Any other receives a frame of only its own elements,
    /// in their original order and spelling.
    fn route(&mut self, frame: &WsMessage, total: usize, elements: &[Element<'_>]) {
        // Counted before anything is copied, so a stream owning the whole frame costs no copy.
        let mut counts = FnvHashMap::<AttachId, usize>::default();

        for element in elements {
            let Some(holders) = self.index.holders(element.channel, &element.symbol) else {
                // Normal for a moment after an unsubscribe, while frames already in flight arrive.
                debug!(
                    channel = element.channel.as_ref(),
                    symbol = %element.symbol,
                    "Alpaca message for a subscription no stream holds",
                );
                continue;
            };

            for id in holders {
                *counts.entry(*id).or_default() += 1;
            }
        }

        for (id, count) in counts {
            let Some(registration) = self.registrations.get_mut(&id) else {
                continue;
            };

            let frame = if count == total {
                frame.clone()
            } else {
                WsMessage::text(self.index.share(id, elements))
            };

            registration.deliver(id, frame);
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
    /// Returns what was discarded, and the pairs nothing holds any longer.
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
    subscriptions: usize,
    frames: usize,
}

impl Discarded {
    fn new(registration: &Registration, frames: usize) -> Self {
        Self {
            exchange: registration.exchange,
            subscriptions: registration.slots.len(),
            frames,
        }
    }
}

/// The socket, and the pairs subscribed on it and confirmed.
struct Socket {
    websocket: WebSocket,
    subscribed: FnvHashSet<Slot>,
}

/// The task that owns one feed's socket.
struct Actor {
    credentials: AlpacaCredentials,
    url: Url,
    commands: mpsc::UnboundedReceiver<Command>,
    /// Handed to each attachment for its detach. Weak, so the task stops once every subscriber
    /// clone and every attachment is gone.
    detach: mpsc::WeakUnboundedSender<Command>,
    socket: Option<Socket>,
    registry: Registry,
}

enum Event {
    Command(Option<Command>),
    Frame(Option<Result<WsMessage, WsError>>),
    Expiry,
}

/// What reading one frame amounted to.
enum Read {
    /// Routed, and what the frame answered, if anything: the connection's subscription state, or
    /// a refusal.
    Done(Vec<Result<FnvHashSet<Slot>, SocketError>>),
    /// The socket is gone, for the reason given.
    Lost(String),
}

/// What a handshake waits for Alpaca to report.
enum Awaiting<'a> {
    /// Every pair subscribed.
    Subscribed(&'a [Slot]),
    /// None of the pairs subscribed any longer.
    Released(&'a [Slot]),
}

impl Awaiting<'_> {
    fn is_answered_by(&self, state: &FnvHashSet<Slot>) -> bool {
        match self {
            Self::Subscribed(slots) => slots.iter().all(|slot| state.contains(slot)),
            Self::Released(slots) => !slots.iter().any(|slot| state.contains(slot)),
        }
    }

    /// Why a handshake that saw `state` last, if any state at all, timed out: the pairs it still
    /// waits on, in request order.
    fn timed_out(&self, timeout: Duration, state: Option<&FnvHashSet<Slot>>) -> String {
        let reported = |slot: &Slot| state.is_some_and(|state| state.contains(slot));
        let (outstanding, asked) = match self {
            Self::Subscribed(slots) => (
                slots
                    .iter()
                    .filter(|slot| !reported(slot))
                    .collect::<Vec<_>>(),
                "subscribed",
            ),
            Self::Released(slots) => (
                slots
                    .iter()
                    .filter(|slot| reported(slot))
                    .collect::<Vec<_>>(),
                "unsubscribed",
            ),
        };

        let outstanding = outstanding
            .iter()
            .map(|slot| format!("{} {}", slot.channel.as_ref(), slot.symbol))
            .collect::<Vec<_>>()
            .join(", ");

        format!(
            "subscription validation timeout reached: {timeout:?}; Alpaca never reported as \
             {asked}: {outstanding}"
        )
    }
}

impl Actor {
    fn new(
        credentials: AlpacaCredentials,
        url: Url,
        commands: mpsc::UnboundedReceiver<Command>,
        detach: mpsc::WeakUnboundedSender<Command>,
    ) -> Self {
        Self {
            credentials,
            url,
            commands,
            detach,
            socket: None,
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
                Event::Frame(frame) => {
                    if let Read::Done(answers) = self.read(frame) {
                        for answer in answers {
                            report_unawaited(answer);
                        }
                    }
                }
                Event::Expiry => self.expire().await,
            }
        }

        self.close().await;
    }

    async fn attach(&mut self, request: AttachRequest) -> Result<AlpacaAttachment, SocketError> {
        let fresh = match &self.socket {
            Some(_) => {
                let (tx, rx) = mpsc::unbounded_channel();
                if let Some(id) = self.registry.reattach(&request, &tx) {
                    debug!(
                        exchange = %request.exchange,
                        "re-attached to the Alpaca connection's reconnect",
                    );
                    return Ok(self.attachment(id, rx));
                }

                false
            }
            None => {
                self.registry.forget_lost(&request);
                self.connect().await?;
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

    /// Register `request` and subscribe whatever the socket does not hold yet.
    ///
    /// On a `fresh` socket that is every registration's pairs — the reconnect re-subscribing what
    /// the lost one held — and on an established one only the request's own.
    async fn subscribe(
        &mut self,
        request: AttachRequest,
        fresh: bool,
    ) -> Result<AlpacaAttachment, SocketError> {
        let Some(socket) = &self.socket else {
            return Err(SocketError::Subscribe(
                "Alpaca connection is not open".to_owned(),
            ));
        };

        let exchange = request.exchange;
        let timeout = request.timeout;

        // Request order first, so a batch reaches the wire in the order it was asked for.
        let mut seen = FnvHashSet::default();
        let sending = request
            .slots
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
        let id = self.registry.insert_live(request, tx);

        if sending.is_empty() {
            return Ok(self.attachment(id, rx));
        }

        let message = subscribe_message(&sending);
        debug!(%exchange, payload = ?message, "sending Alpaca subscription");

        let subscribed = match self.send(message).await {
            Ok(()) => {
                self.handshake(Awaiting::Subscribed(&sending), timeout)
                    .await
            }
            Err(error) => Err(error),
        };

        match subscribed {
            Ok(()) => {
                debug!(%exchange, count = sending.len(), "Alpaca subscriptions confirmed");
                if let Some(socket) = self.socket.as_mut() {
                    socket.subscribed.extend(sending);
                }

                Ok(self.attachment(id, rx))
            }
            Err(Refusal::Refused(error)) => {
                // Alpaca refuses a subscribe whole, so nothing it named was added and there is
                // nothing to unsubscribe.
                self.registry.remove(id);
                Err(shared_cap_context(error))
            }
            Err(Refusal::Failed(error)) => {
                let orphaned = self.registry.remove(id);
                if !fresh {
                    // What this attach sent may be held by the provider even though the
                    // handshake failed; unsubscribing reclaims it.
                    self.release(orphaned).await;
                }

                Err(error)
            }
        }
    }

    /// Read until Alpaca reports the state `awaiting` asks for, routing everything else as usual.
    async fn handshake(
        &mut self,
        awaiting: Awaiting<'_>,
        timeout: Duration,
    ) -> Result<(), Refusal> {
        let deadline = Instant::now() + timeout;
        // The latest state Alpaca reported, so a timeout can name what it never reported.
        let mut last = None;

        loop {
            let Ok(frame) = tokio::time::timeout_at(deadline, next_frame(&mut self.socket)).await
            else {
                return Err(Refusal::Failed(SocketError::Subscribe(
                    awaiting.timed_out(timeout, last.as_ref()),
                )));
            };

            match self.read(frame) {
                Read::Done(answers) => {
                    for answer in answers {
                        match answer {
                            Ok(state) if awaiting.is_answered_by(&state) => return Ok(()),
                            // The whole state, not yet the one asked for.
                            Ok(state) => {
                                debug!(held = state.len(), "Alpaca subscription state");
                                last = Some(state);
                            }
                            Err(error) => return Err(Refusal::Refused(error)),
                        }
                    }
                }
                Read::Lost(cause) => {
                    return Err(Refusal::Failed(SocketError::Subscribe(format!(
                        "Alpaca connection lost while subscribing: {cause}"
                    ))));
                }
            }
        }
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
                subscriptions = discarded.subscriptions,
                discarded_frames = discarded.frames,
                grace = ?REATTACH_GRACE,
                "an Alpaca stream did not re-attach after a reconnect; the frames held for it are \
                 discarded, and a later re-attach is a new attach",
            );
        }

        self.release(orphaned).await;
    }

    /// Unsubscribe `slots`, which nothing holds any longer, or close the socket if nothing at all
    /// remains attached. Waits for Alpaca's answer, so the next handshake cannot read it.
    async fn release(&mut self, slots: Vec<Slot>) {
        if self.registry.is_empty() {
            self.close().await;
            return;
        }

        let Some(socket) = self.socket.as_mut() else {
            return;
        };
        if slots.is_empty() {
            return;
        }

        // Not only what was confirmed: an attach whose handshake timed out releases what it sent,
        // which the provider may hold unconfirmed.
        for slot in &slots {
            socket.subscribed.remove(slot);
        }

        let message = unsubscribe_message(&slots);
        debug!(payload = ?message, "releasing Alpaca subscriptions");
        if self.send(message).await.is_err() {
            return;
        }

        match self
            .handshake(Awaiting::Released(&slots), RELEASE_TIMEOUT)
            .await
        {
            Ok(()) => debug!(count = slots.len(), "Alpaca subscriptions released"),
            Err(Refusal::Refused(error) | Refusal::Failed(error)) => warn!(
                %error,
                "Alpaca did not confirm releasing subscriptions no stream holds any longer; they \
                 may still count against the connection's subscription cap",
            ),
        }
    }

    async fn connect(&mut self) -> Result<(), SocketError> {
        let mut websocket = connect(self.url.clone()).await?;
        alpaca_authenticate(&mut websocket, &self.credentials)
            .await
            .map_err(connection_limit_context)?;
        debug!(url = %self.url, "authenticated to Alpaca WebSocket");

        self.socket = Some(Socket {
            websocket,
            subscribed: FnvHashSet::default(),
        });

        Ok(())
    }

    /// Send one frame. A failed send loses the socket.
    async fn send(&mut self, message: WsMessage) -> Result<(), Refusal> {
        let Some(socket) = self.socket.as_mut() else {
            return Err(Refusal::Failed(SocketError::Subscribe(
                "Alpaca connection is not open".to_owned(),
            )));
        };

        if let Err(error) = socket.websocket.send(message).await {
            let cause = error.to_string();
            self.lose(&cause);
            return Err(Refusal::Failed(SocketError::WebSocket(Box::new(error))));
        }

        Ok(())
    }

    /// Classify one read, routing whatever it carries for the streams.
    fn read(&mut self, frame: Option<Result<WsMessage, WsError>>) -> Read {
        let message = match frame {
            Some(Ok(message)) => message,
            Some(Err(error)) => return self.lose(&error.to_string()),
            None => return self.lose("the socket ended"),
        };

        match classify(&message) {
            Frame::Elements {
                total,
                data,
                answers,
            } => {
                if !data.is_empty() {
                    self.registry.route(&message, total, &data);
                }
                Read::Done(answers)
            }
            Frame::Closed(cause) => self.lose(&cause),
            Frame::Ignored => Read::Done(Vec::new()),
        }
    }

    /// The socket is gone: every stream on it ends, and waits for one of them to reconnect.
    fn lose(&mut self, cause: &str) -> Read {
        if self.socket.take().is_some() {
            warn!(
                cause,
                url = %self.url,
                streams = self.registry.registrations.len(),
                "Alpaca connection lost; every stream sharing it ends, and the first to re-attach \
                 reconnects for all of them",
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
                subscriptions = discarded.subscriptions,
                discarded_frames = discarded.frames,
                "an Alpaca reconnect was lost before this stream re-attached to it; the frames \
                 held for it are discarded",
            );
        }
    }

    async fn close(&mut self) {
        if let Some(mut socket) = self.socket.take() {
            debug!(url = %self.url, "closing Alpaca connection");
            // Best effort: the connection is being discarded either way.
            let _ = socket.websocket.close(None).await;
        }
    }

    fn attachment(
        &self,
        id: AttachId,
        frames: mpsc::UnboundedReceiver<WsMessage>,
    ) -> AlpacaAttachment {
        AlpacaAttachment {
            frames,
            _detach: Detach {
                id,
                commands: self.detach.upgrade(),
            },
        }
    }
}

/// Why a handshake did not end with the state it asked for.
enum Refusal {
    /// Alpaca answered with an error, which it applies to the whole request.
    Refused(SocketError),
    /// No answer: a timeout, a failed send or a lost socket. What the provider holds is unknown.
    Failed(SocketError),
}

/// Log an answer no handshake was waiting for.
fn report_unawaited(answer: Result<FnvHashSet<Slot>, SocketError>) {
    match answer {
        Ok(state) => debug!(
            held = state.len(),
            "Alpaca subscription state outside a handshake"
        ),
        Err(error) => warn!(
            %error,
            "Alpaca reported an error outside a subscribe; the error need not name a symbol, so \
             check whether a stream on this connection has gone quiet",
        ),
    }
}

/// Add to a refusal for the subscription cap what the stream cannot see: the cap is shared.
fn shared_cap_context(error: SocketError) -> SocketError {
    match error {
        SocketError::Subscribe(message) if message.contains("symbol limit exceeded") => {
            SocketError::Subscribe(format!(
                "{message} (the connection and its subscription cap are shared by every stream \
                 this subscriber and its clones open on the feed)"
            ))
        }
        other => other,
    }
}

/// Add to a refused authentication what usually causes it.
fn connection_limit_context(error: SocketError) -> SocketError {
    match error {
        SocketError::Subscribe(message) if message.contains("connection limit exceeded") => {
            SocketError::Subscribe(format!(
                "{message} (Alpaca allows one connection per feed: another subscriber built \
                 separately, or another process, holds it. Streams sharing a feed must be opened \
                 by clones of one subscriber)"
            ))
        }
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

/// The part of a frame element routing reads.
///
/// Short strings deserialise inline — a message type and a symbol both fit — so this allocates
/// nothing per element, whether or not the provider escapes a character.
#[derive(Deserialize)]
struct Envelope {
    #[serde(rename = "T")]
    kind: SmolStr,
    #[serde(rename = "S", default)]
    symbol: Option<SmolStr>,
}

enum Frame<'a> {
    Elements {
        /// Every element in the frame, of every kind.
        total: usize,
        data: Vec<Element<'a>>,
        answers: Vec<Result<FnvHashSet<Slot>, SocketError>>,
    },
    Closed(String),
    Ignored,
}

fn classify(message: &WsMessage) -> Frame<'_> {
    let text = match message {
        WsMessage::Text(text) => text.as_str(),
        WsMessage::Binary(bytes) => match std::str::from_utf8(bytes) {
            Ok(text) => text,
            Err(_) => {
                warn!(
                    len = bytes.len(),
                    "Alpaca sent a binary frame that is not UTF-8; ignored"
                );
                return Frame::Ignored;
            }
        },
        WsMessage::Close(frame) => return Frame::Closed(format!("closed by Alpaca: {frame:?}")),
        // Pings are answered by the socket itself.
        _ => return Frame::Ignored,
    };

    let elements = match serde_json::from_str::<Vec<&RawValue>>(text) {
        Ok(elements) => elements,
        Err(error) => {
            warn!(
                %error,
                frame = %text.chars().take(200).collect::<String>(),
                "Alpaca sent a frame that is not a JSON array; ignored",
            );
            return Frame::Ignored;
        }
    };

    let total = elements.len();
    let mut data = Vec::with_capacity(total);
    let mut answers = Vec::new();

    for raw in elements {
        let envelope = match serde_json::from_str::<Envelope>(raw.get()) {
            Ok(envelope) => envelope,
            Err(error) => {
                warn!(
                    %error,
                    element = %raw.get().chars().take(200).collect::<String>(),
                    "Alpaca sent a message with no type; ignored",
                );
                continue;
            }
        };

        let channel = match envelope.kind.as_str() {
            "subscription" | "error" => {
                answers.push(answer(raw));
                continue;
            }
            // Corrections and cancel errors accompany trades, and belong to whoever holds them.
            "t" | "c" | "x" => AlpacaChannel::Trades,
            "q" => AlpacaChannel::Quotes,
            kind => {
                debug!(kind, "Alpaca message of a type no stream subscribes to");
                continue;
            }
        };

        let Some(symbol) = envelope.symbol else {
            warn!(
                element = %raw.get().chars().take(200).collect::<String>(),
                "Alpaca sent a market data message with no symbol; ignored",
            );
            continue;
        };

        data.push(Element {
            channel,
            symbol,
            raw,
        });
    }

    Frame::Elements {
        total,
        data,
        answers,
    }
}

/// The connection's subscription state a `subscription` message reports, or the refusal an `error`
/// message carries.
fn answer(raw: &RawValue) -> Result<FnvHashSet<Slot>, SocketError> {
    let inner = serde_json::from_str::<AlpacaSubResponseInner>(raw.get()).map_err(|error| {
        SocketError::Deserialise {
            error,
            payload: raw.get().to_owned(),
        }
    })?;

    let response = AlpacaSubResponse(vec![inner]).validate()?;

    Ok(response
        .0
        .iter()
        .flat_map(covered)
        .map(|(channel, symbol)| Slot {
            channel,
            symbol: symbol.clone(),
        })
        .collect())
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use serde_json::{Value, json};

    const TRADES: AlpacaChannel = AlpacaChannel::Trades;
    const QUOTES: AlpacaChannel = AlpacaChannel::Quotes;

    fn slot(channel: AlpacaChannel, symbol: &str) -> Slot {
        Slot {
            channel,
            symbol: SmolStr::new(symbol),
        }
    }

    fn request(pairs: &[(AlpacaChannel, &str)]) -> AttachRequest {
        AttachRequest {
            exchange: ExchangeId::AlpacaIex,
            url: Url::parse("ws://127.0.0.1:1").unwrap(),
            slots: pairs
                .iter()
                .map(|(channel, symbol)| slot(*channel, symbol))
                .collect(),
            timeout: Duration::from_secs(1),
        }
    }

    fn attach(
        registry: &mut Registry,
        pairs: &[(AlpacaChannel, &str)],
    ) -> (AttachId, mpsc::UnboundedReceiver<WsMessage>) {
        let (tx, rx) = mpsc::unbounded_channel();
        (registry.insert_live(request(pairs), tx), rx)
    }

    fn trade(symbol: &str) -> Value {
        json!({"T": "t", "S": symbol, "i": 1, "p": 1.5, "s": 2, "t": "2026-09-25T10:00:00Z"})
    }

    fn quote(symbol: &str) -> Value {
        json!({"T": "q", "S": symbol, "bp": 1.0, "bs": 1, "ap": 2.0, "as": 1,
               "t": "2026-09-25T10:00:00Z"})
    }

    fn frame(elements: &[Value]) -> WsMessage {
        WsMessage::text(Value::Array(elements.to_vec()).to_string())
    }

    /// Route one frame through the registry exactly as the connection task does.
    fn deliver(registry: &mut Registry, message: &WsMessage) {
        match classify(message) {
            Frame::Elements { total, data, .. } => registry.route(message, total, &data),
            Frame::Closed(_) | Frame::Ignored => panic!("expected a frame of elements"),
        }
    }

    /// Every frame waiting on `rx`, each parsed back into its elements.
    fn received(rx: &mut mpsc::UnboundedReceiver<WsMessage>) -> Vec<Vec<Value>> {
        std::iter::from_fn(|| rx.try_recv().ok())
            .map(|message| match message {
                WsMessage::Text(text) => serde_json::from_str(text.as_str()).unwrap(),
                other => panic!("expected a text frame, got {other:?}"),
            })
            .collect()
    }

    fn answers(message: &WsMessage) -> Vec<Result<FnvHashSet<Slot>, SocketError>> {
        match classify(message) {
            Frame::Elements { answers, .. } => answers,
            Frame::Closed(_) | Frame::Ignored => Vec::new(),
        }
    }

    fn only<T: std::fmt::Debug>(mut items: Vec<T>) -> T {
        assert_eq!(items.len(), 1, "expected exactly one, got {items:?}");
        items.remove(0)
    }

    #[test]
    fn a_mixed_frame_is_split_per_stream_in_its_original_order() {
        let mut registry = Registry::default();
        let (_, mut apple) = attach(&mut registry, &[(TRADES, "AAPL")]);
        let (_, mut microsoft) = attach(&mut registry, &[(QUOTES, "MSFT"), (TRADES, "MSFT")]);

        deliver(
            &mut registry,
            &frame(&[trade("MSFT"), trade("AAPL"), quote("MSFT"), quote("AAPL")]),
        );

        assert_eq!(received(&mut apple), [vec![trade("AAPL")]]);
        assert_eq!(
            received(&mut microsoft),
            [vec![trade("MSFT"), quote("MSFT")]],
            "a stream's share keeps the order the provider sent it in",
        );
    }

    #[test]
    fn a_frame_wholly_for_one_stream_is_forwarded_as_sent() {
        let mut registry = Registry::default();
        let (_, mut rx) = attach(&mut registry, &[(TRADES, "BTC/USD"), (QUOTES, "BTC/USD")]);

        // Spacing no re-serialisation would reproduce, so an unchanged frame is distinguishable.
        let sent = WsMessage::text(format!("[ {} ,{}]", trade("BTC/USD"), quote("BTC/USD")));
        deliver(&mut registry, &sent);

        assert_eq!(rx.try_recv().unwrap(), sent);
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn a_pair_held_by_two_streams_reaches_both_and_an_unheld_one_neither() {
        let mut registry = Registry::default();
        let (_, mut first) = attach(&mut registry, &[(TRADES, "AAPL")]);
        let (_, mut second) = attach(&mut registry, &[(TRADES, "AAPL")]);

        deliver(&mut registry, &frame(&[trade("AAPL"), trade("TSLA")]));

        assert_eq!(received(&mut first), [vec![trade("AAPL")]]);
        assert_eq!(received(&mut second), [vec![trade("AAPL")]]);
    }

    #[test]
    fn corrections_and_cancel_errors_reach_the_streams_holding_the_trades() {
        let mut registry = Registry::default();
        let (_, mut trades) = attach(&mut registry, &[(TRADES, "AAPL")]);
        let (_, mut quotes) = attach(&mut registry, &[(QUOTES, "AAPL")]);

        let correction = json!({"T": "c", "S": "AAPL", "x": "V", "oi": 1, "ci": 2});
        let cancel = json!({"T": "x", "S": "AAPL", "i": 1, "a": "C"});
        deliver(&mut registry, &frame(&[correction.clone(), cancel.clone()]));

        assert_eq!(received(&mut trades), [vec![correction, cancel]]);
        assert!(received(&mut quotes).is_empty());
    }

    #[test]
    fn a_frame_carrying_control_messages_is_never_forwarded_whole() {
        let mut registry = Registry::default();
        let (_, mut rx) = attach(&mut registry, &[(TRADES, "AAPL")]);

        let state = json!({"T": "subscription", "trades": ["AAPL"], "quotes": []});
        deliver(&mut registry, &frame(&[state, trade("AAPL")]));

        assert_eq!(received(&mut rx), [vec![trade("AAPL")]]);
    }

    #[test]
    fn a_subscription_message_reports_the_whole_state_and_an_error_refuses() {
        let state = frame(&[json!({
            "T": "subscription",
            "trades": ["BTC/USD"],
            "quotes": ["ETH/USD", "BTC/USD"],
            "orderbooks": [],
            "bars": [],
        })]);
        let held = only(answers(&state)).unwrap();
        assert_eq!(
            held,
            FnvHashSet::from_iter([
                slot(TRADES, "BTC/USD"),
                slot(QUOTES, "ETH/USD"),
                slot(QUOTES, "BTC/USD"),
            ]),
        );

        let refusal = frame(&[json!({"T": "error", "code": 405, "msg": "symbol limit exceeded"})]);
        let error = only(answers(&refusal)).unwrap_err();
        assert!(
            error.to_string().contains("symbol limit exceeded"),
            "{error}"
        );
    }

    #[test]
    fn a_success_message_answers_nothing_and_reaches_no_stream() {
        let mut registry = Registry::default();
        let (_, mut rx) = attach(&mut registry, &[(TRADES, "AAPL")]);

        let success = frame(&[json!({"T": "success", "msg": "authenticated"})]);
        assert!(answers(&success).is_empty());
        deliver(&mut registry, &success);

        assert!(received(&mut rx).is_empty());
    }

    #[test]
    fn a_market_data_message_with_no_symbol_is_dropped_and_the_rest_routed() {
        let mut registry = Registry::default();
        let (_, mut rx) = attach(&mut registry, &[(TRADES, "AAPL")]);

        let anonymous = json!({"T": "t", "i": 1, "p": 1.0});
        deliver(&mut registry, &frame(&[anonymous, trade("AAPL")]));

        assert_eq!(received(&mut rx), [vec![trade("AAPL")]]);
    }

    #[test]
    fn only_a_cap_refusal_is_told_the_cap_is_shared() {
        let refusal = |msg: &str| {
            let frame = frame(&[json!({"T": "error", "code": 405, "msg": msg})]);
            shared_cap_context(only(answers(&frame)).unwrap_err()).to_string()
        };

        assert!(refusal("symbol limit exceeded").contains("shared by every stream"));
        assert!(!refusal("invalid syntax").contains("shared by every stream"));
    }

    #[test]
    fn a_timed_out_handshake_names_what_alpaca_never_reported() {
        let slots = [slot(TRADES, "AAPL"), slot(QUOTES, "MSFT")];
        let state = FnvHashSet::from_iter([slot(TRADES, "AAPL")]);
        let timeout = Duration::from_secs(1);

        let subscribing = Awaiting::Subscribed(&slots).timed_out(timeout, Some(&state));
        assert!(
            subscribing.ends_with("subscribed: quotes MSFT"),
            "{subscribing}"
        );

        let nothing_reported = Awaiting::Subscribed(&slots).timed_out(timeout, None);
        assert!(
            nothing_reported.ends_with("trades AAPL, quotes MSFT"),
            "{nothing_reported}"
        );

        let releasing = Awaiting::Released(&slots).timed_out(timeout, Some(&state));
        assert!(
            releasing.ends_with("unsubscribed: trades AAPL"),
            "{releasing}"
        );
    }

    #[test]
    fn a_frame_that_is_not_an_array_is_ignored() {
        assert!(matches!(
            classify(&WsMessage::text(r#"{"T":"t"}"#)),
            Frame::Ignored
        ));
    }

    #[test]
    fn a_state_answers_a_subscribe_only_once_it_holds_every_pair() {
        let sending = [slot(TRADES, "AAPL"), slot(QUOTES, "AAPL")];
        let awaiting = Awaiting::Subscribed(&sending);

        assert!(!awaiting.is_answered_by(&FnvHashSet::from_iter([slot(TRADES, "AAPL")])));
        assert!(awaiting.is_answered_by(&FnvHashSet::from_iter([
            slot(TRADES, "AAPL"),
            slot(QUOTES, "AAPL"),
            slot(TRADES, "MSFT"),
        ])));
    }

    #[test]
    fn a_state_answers_an_unsubscribe_only_once_it_holds_none_of_the_pairs() {
        let releasing = [slot(TRADES, "AAPL")];
        let awaiting = Awaiting::Released(&releasing);

        assert!(!awaiting.is_answered_by(&FnvHashSet::from_iter([slot(TRADES, "AAPL")])));
        assert!(awaiting.is_answered_by(&FnvHashSet::from_iter([slot(QUOTES, "AAPL")])));
    }

    #[test]
    fn the_last_detach_orphans_a_pair_and_an_earlier_one_does_not() {
        let mut registry = Registry::default();
        let (first, _first_rx) = attach(&mut registry, &[(TRADES, "AAPL"), (QUOTES, "AAPL")]);
        let (second, _second_rx) = attach(&mut registry, &[(TRADES, "AAPL")]);

        assert_eq!(
            registry.remove_live(first).unwrap(),
            [slot(QUOTES, "AAPL")],
            "a pair another stream holds stays subscribed",
        );
        assert_eq!(
            registry.remove_live(second).unwrap(),
            [slot(TRADES, "AAPL")]
        );
        assert!(registry.is_empty());
    }

    #[test]
    fn a_detach_after_the_socket_is_lost_keeps_the_registration() {
        let mut registry = Registry::default();
        let (id, _rx) = attach(&mut registry, &[(TRADES, "AAPL")]);

        registry.lose_all();

        assert!(registry.remove_live(id).is_none());
        assert!(!registry.is_empty());
    }

    #[test]
    fn a_reconnect_holds_frames_until_the_stream_re_attaches_then_delivers_them_in_order() {
        let mut registry = Registry::default();
        let (_, _old) = attach(&mut registry, &[(TRADES, "AAPL")]);

        registry.lose_all();
        registry.hold_lost(Instant::now() + REATTACH_GRACE);

        deliver(&mut registry, &frame(&[trade("AAPL")]));
        deliver(&mut registry, &frame(&[trade("AAPL"), trade("AAPL")]));

        let (tx, mut rx) = mpsc::unbounded_channel();
        assert!(
            registry
                .reattach(&request(&[(TRADES, "AAPL")]), &tx)
                .is_some()
        );

        assert_eq!(
            received(&mut rx),
            [vec![trade("AAPL")], vec![trade("AAPL"), trade("AAPL")]],
        );
    }

    #[test]
    fn a_different_batch_does_not_re_attach_to_a_held_registration() {
        let mut registry = Registry::default();
        let (_, _old) = attach(&mut registry, &[(TRADES, "AAPL")]);

        registry.lose_all();
        registry.hold_lost(Instant::now() + REATTACH_GRACE);

        let (tx, _rx) = mpsc::unbounded_channel();
        assert!(
            registry
                .reattach(&request(&[(QUOTES, "AAPL")]), &tx)
                .is_none()
        );
    }

    #[test]
    fn a_held_registration_expires_and_releases_what_only_it_held() {
        let mut registry = Registry::default();
        let (_, _old) = attach(&mut registry, &[(TRADES, "AAPL"), (QUOTES, "AAPL")]);

        registry.lose_all();
        let until = Instant::now();
        registry.hold_lost(until);

        // Another stream, on a pair the held one also holds.
        let (_, _live) = attach(&mut registry, &[(QUOTES, "AAPL")]);
        deliver(&mut registry, &frame(&[trade("AAPL")]));

        assert_eq!(registry.next_expiry(), Some(until));
        let (discarded, orphaned) = registry.expire(until);

        assert_eq!(
            discarded,
            [Discarded {
                exchange: ExchangeId::AlpacaIex,
                subscriptions: 2,
                frames: 1,
            }],
        );
        assert_eq!(orphaned, [slot(TRADES, "AAPL")]);
        assert_eq!(registry.next_expiry(), None);
    }

    #[test]
    fn subscribe_and_unsubscribe_payloads_group_pairs_by_channel() {
        let pairs = [
            slot(TRADES, "AAPL"),
            slot(QUOTES, "MSFT"),
            slot(TRADES, "SPY"),
        ];

        let parse = |message: WsMessage| match message {
            WsMessage::Text(text) => serde_json::from_str::<Value>(text.as_str()).unwrap(),
            other => panic!("expected a text frame, got {other:?}"),
        };

        assert_eq!(
            parse(subscribe_message(&pairs)),
            json!({"action": "subscribe", "trades": ["AAPL", "SPY"], "quotes": ["MSFT"]}),
        );
        assert_eq!(
            parse(unsubscribe_message(&pairs[..1])),
            json!({"action": "unsubscribe", "trades": ["AAPL"]}),
        );
    }
}
