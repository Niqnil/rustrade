//! One WebSocket shared by every stream a subscriber and its clones open.
//!
//! Some providers cap how many WebSockets a key or an account may hold at once, often at one. A
//! socket per stream could then never run two streams at once, so the streams of such a provider
//! share a connection instead. This module is the part of that sharing every provider has in
//! common; a provider supplies the rest through [`Protocol`].
//!
//! # Shape
//! A task owns the socket. Each stream *attaches* to it and is handed an [`Attachment`]: the raw
//! frames carrying its own subscriptions, which it parses exactly as it would parse a socket of its
//! own. The task reads only what routing needs and forwards the frame itself, so each stream's
//! decoder stays the only full parse.
//!
//! - **Connect lazily, close when idle.** The socket opens on the first attach and closes once no
//!   stream remains attached.
//! - **Subscribe once per slot.** A [`Slot`](Protocol::Slot) the socket already holds is not sent
//!   again, and its frames reach every stream holding it.
//! - **Unsubscribe on the last detach.** Dropping an attachment releases every slot no other
//!   stream holds.
//! - **One handshake at a time.** Attaches and detaches are serialised, so an answer naming nothing
//!   belongs to the request in flight. Frames keep flowing to every attached stream meanwhile.
//!
//! # Reconnect
//! Every stream on the socket **ends together** when it is lost, and each is re-initialised by the
//! usual reconnect wrapper. The first to re-attach reconnects and re-subscribes everything the lost
//! socket held; the rest re-attach to that one reconnect, and frames for them are held until they
//! do.
//!
//! A stream that has not re-attached within [`REATTACH_GRACE`] of its socket being lost is
//! forgotten: frames held for it are discarded with a warning, and its slots released. The
//! deadline is set once, when the socket is lost, so a reconnect that fails does not extend it, and
//! a stream dropped while its socket was down cannot be re-subscribed by one reconnect after another.
//!
//! # Sharing is by clone, and only by clone
//! Clones of one subscriber share its connections. Two subscribers built separately open a socket
//! each, and a provider that caps connections refuses the second — visibly, which is left to
//! surface rather than papered over by a process-wide registry.

// A toolkit for the providers that share a connection, each of which uses only part of it, so a
// build enabling some of them leaves the rest unused. A build enabling all of them, as CI's does,
// still reports anything none of them uses.
#![cfg_attr(
    not(all(feature = "alpaca", feature = "lse")),
    allow(dead_code, unused_imports)
)]

mod actor;
// Only the providers that pack several messages into one frame use it, and it needs
// `serde_json/raw_value`, which only their features enable.
#[cfg(any(feature = "alpaca", feature = "massive"))]
pub(crate) mod elements;
mod registry;

pub(crate) use registry::{Registration, Registry};

use self::actor::Actor;
use fnv::FnvHashMap;
use futures::Stream;
use rustrade_instrument::exchange::ExchangeId;
use rustrade_integration::{
    error::SocketError,
    protocol::websocket::{WebSocket, WsError, WsMessage},
};
use std::{
    fmt::Debug,
    future::Future,
    hash::Hash,
    pin::Pin,
    sync::{Mutex, PoisonError},
    task::{Context, Poll},
    time::Duration,
};
use tokio::sync::{mpsc, oneshot};
use tracing::error;
use url::Url;

/// How long a shared connection keeps a stream's place after the socket it was reading is lost.
///
/// A stream re-attaches as soon as it has drained what it was sent before the socket was lost,
/// which is normally milliseconds. The bound covers a consumer that stalls for a while, and a
/// stream dropped for good while no socket was open. Past it, frames held for the stream are
/// discarded with a warning and its subscriptions released, and a late re-attach is treated as a
/// new attach.
///
/// Held frames are reference-counted clones of what every other stream receives, so the cost is
/// bounded by what the provider sends within the window.
pub const REATTACH_GRACE: Duration = Duration::from_secs(30);

/// How long a release waits for the provider to confirm an unsubscribe, where it confirms one.
const RELEASE_TIMEOUT: Duration = Duration::from_secs(10);

/// Identifies one attach for the lifetime of the connection task. Never reused.
pub(crate) type AttachId = u64;

/// What a provider supplies to share one connection among its streams.
///
/// Everything provider-neutral — the task, the registry of attached streams, reconnect and expiry,
/// serialised handshakes — is this module's. A provider says how to authenticate, what to send,
/// how an answer settles a handshake, and where each frame goes.
pub(crate) trait Protocol: Debug + Sized + Send + 'static {
    /// The provider's name, as log lines and errors spell it.
    const NAME: &'static str;

    /// Whether each endpoint gets a connection of its own, or one connection serves every endpoint
    /// a subscriber's streams use.
    ///
    /// With one connection for all, a stream on a second endpoint is refused while any stream is
    /// attached through the first: a reconnect re-subscribes every stream on one endpoint.
    const CONNECTION_PER_ENDPOINT: bool;

    /// Whether a refused subscribe added none of what it asked for, so there is nothing to release.
    const REFUSAL_IS_ATOMIC: bool;

    /// Appended to the warning for frames discarded when a reconnect is lost before the stream
    /// they were held for re-attached.
    const DISCARDED_ON_LOSS: &'static str = "";

    /// Appended to the warning for frames discarded when a stream did not re-attach in time.
    const DISCARDED_ON_EXPIRY: &'static str = "";

    type Credentials: Clone + Debug + Send + Sync + 'static;

    /// One subscription on the socket: what the provider counts against its cap.
    type Slot: Clone + Eq + Hash + Debug + Send + 'static;

    /// What a stream's batch carries beyond its exchange, kind and slots.
    type Batch: Batch;

    /// What authentication reported about the socket.
    type Session: Send;

    /// A subscribe or unsubscribe answer, as a handshake reads it.
    type Answer: Debug + Send;

    /// Waits for the answers to one request.
    type Handshake: Handshake<Self::Answer>;

    /// Open and authenticate a socket to `url`.
    fn connect(
        credentials: &Self::Credentials,
        url: &Url,
    ) -> impl Future<Output = Result<(WebSocket, Self::Session), SocketError>> + Send;

    /// Refuse `request` before anything is sent on its behalf, if it cannot succeed.
    ///
    /// `registry` holds every stream attached so far, and its index what the socket holds.
    fn check(
        _request: &AttachRequest<Self>,
        _session: &Self::Session,
        _registry: &Registry<Self>,
    ) -> Result<(), SocketError> {
        Ok(())
    }

    /// What subscribes `sending` on behalf of registration `id`, and the handshake confirming it.
    ///
    /// `sending` may be empty: every slot the stream holds is already on the socket.
    fn subscribe(
        registry: &mut Registry<Self>,
        id: AttachId,
        sending: &[Self::Slot],
    ) -> (Vec<WsMessage>, Self::Handshake);

    /// What releases `slots`, and the handshake confirming it, if the provider confirms one in a
    /// way a later handshake could mistake for its own answer.
    fn release(slots: &[Self::Slot]) -> (Vec<WsMessage>, Option<Self::Handshake>);

    /// Read one frame: route what it carries for the streams, and return what it answers.
    fn read(registry: &mut Registry<Self>, message: &WsMessage) -> Frame<Self::Answer>;

    /// Add to a refusal what the stream cannot see for itself.
    fn refused(error: SocketError) -> SocketError {
        error
    }
}

/// A stream's batch, as a [`Protocol`] keeps it beyond its exchange, kind and slots.
pub(crate) trait Batch: Debug + Send + 'static {
    /// Handed to the stream with its attachment.
    type Attached: Debug + Default + Send;

    /// Whether `other` is this batch's stream re-attaching, given an equal exchange, kind and
    /// slots.
    fn is_same_stream(&self, other: &Self) -> bool;

    /// What the stream is handed with its attachment.
    fn attached(&self) -> Self::Attached;

    /// Forget whatever belonged to the socket that was lost: a reconnect starts afresh.
    fn reset(&mut self);
}

impl Batch for () {
    type Attached = ();

    fn is_same_stream(&self, _: &Self) -> bool {
        true
    }

    fn attached(&self) {}

    fn reset(&mut self) {}
}

/// Waits for the answers to one request.
pub(crate) trait Handshake<Answer>: Send {
    /// Whether every answer this handshake waits for has arrived.
    fn is_settled(&self) -> bool;

    /// Take one answer that is not a refusal.
    fn observe(&mut self, answer: Answer);

    /// Why the handshake did not settle within `timeout`.
    fn timed_out(&self, timeout: Duration) -> String;
}

/// What one frame amounted to, once whatever it carries for the streams is routed.
pub(crate) enum Frame<Answer> {
    /// The answers the frame carried, if any: each a confirmation, or a refusal.
    Answers(Vec<Result<Answer, SocketError>>),
    /// The provider closed the socket, for the reason given.
    Closed(String),
}

/// One stream's batch, as the connection needs it.
#[derive(Debug)]
pub(crate) struct AttachRequest<P: Protocol> {
    pub(crate) exchange: ExchangeId,
    pub(crate) url: Url,
    /// [`SubscriptionKind::as_str`](crate::subscription::SubscriptionKind::as_str) of the batch.
    pub(crate) kind: &'static str,
    /// The distinct slots requested, in request order.
    pub(crate) slots: Vec<P::Slot>,
    pub(crate) timeout: Duration,
    pub(crate) batch: P::Batch,
}

#[derive(Debug)]
enum Command<P: Protocol> {
    Attach(
        Box<AttachRequest<P>>,
        oneshot::Sender<Result<Attachment<P>, SocketError>>,
    ),
    Detach(AttachId),
}

/// The handle a subscriber and its clones share: the way into each of its connection tasks.
///
/// A task is spawned on its connection's first attach rather than on construction, because a
/// subscriber can be built outside a runtime, and again if the runtime that ran it has since shut
/// down.
#[derive(Debug)]
pub(crate) struct Connections<P: Protocol> {
    credentials: P::Credentials,
    /// Keyed by endpoint, or by `None` alone when one connection serves every endpoint.
    tasks: Mutex<FnvHashMap<Option<Url>, mpsc::UnboundedSender<Command<P>>>>,
}

impl<P: Protocol> Connections<P> {
    pub(crate) fn new(credentials: P::Credentials) -> Self {
        Self {
            credentials,
            tasks: Mutex::new(FnvHashMap::default()),
        }
    }

    /// Attach one stream's batch to its connection, connecting first if nothing is attached.
    pub(crate) async fn attach(
        &self,
        request: AttachRequest<P>,
    ) -> Result<Attachment<P>, SocketError> {
        let (reply, answer) = oneshot::channel();

        self.commands(&request.url)
            .send(Command::Attach(Box::new(request), reply))
            .map_err(|_| task_stopped::<P>())?;

        answer.await.map_err(|_| task_stopped::<P>())?
    }

    fn commands(&self, url: &Url) -> mpsc::UnboundedSender<Command<P>> {
        let key = P::CONNECTION_PER_ENDPOINT.then(|| url.clone());

        // A poisoned lock guards senders, which are valid whatever state the panic left behind.
        let mut tasks = self.tasks.lock().unwrap_or_else(PoisonError::into_inner);

        if let Some(commands) = tasks.get(&key)
            && !commands.is_closed()
        {
            return commands.clone();
        }

        let (tx, rx) = mpsc::unbounded_channel();
        let task = Actor::<P>::new(self.credentials.clone(), rx, tx.downgrade());
        tokio::spawn(report_panic::<P>(tokio::spawn(task.run())));
        tasks.insert(key, tx.clone());

        tx
    }
}

/// Log a panic of a connection task, which its streams would otherwise see only as a lost socket.
///
/// Every stream sharing the connection ends when the task does, and the next attach spawns a new
/// one, so the panic is not propagated anywhere; this is the one place it is reported.
async fn report_panic<P: Protocol>(task: tokio::task::JoinHandle<()>) {
    if let Err(error) = task.await
        && error.is_panic()
    {
        error!(
            %error,
            "the {} connection task panicked; every stream sharing the connection ends, and the \
             next to re-attach starts a new connection",
            P::NAME,
        );
    }
}

fn task_stopped<P: Protocol>() -> SocketError {
    SocketError::Subscribe(format!(
        "the {} connection task stopped before answering",
        P::NAME
    ))
}

/// One stream's view of a shared connection: the raw frames carrying its own subscriptions, in the
/// order the socket delivered them. It ends when the connection is lost.
///
/// Dropping it detaches the stream.
#[derive(Debug)]
pub(crate) struct Attachment<P: Protocol> {
    frames: mpsc::UnboundedReceiver<WsMessage>,
    attached: <P::Batch as Batch>::Attached,
    _detach: Detach<P>,
}

impl<P: Protocol> Attachment<P> {
    /// Take what the stream was handed with its attachment.
    pub(crate) fn take_attached(&mut self) -> <P::Batch as Batch>::Attached {
        std::mem::take(&mut self.attached)
    }
}

// Nothing in an attachment is ever pinned: its stream is polled through the receiver alone. Stated
// because an auto-derived `Unpin` would demand it of every provider's `Attached`.
impl<P: Protocol> Unpin for Attachment<P> {}

impl<P: Protocol> Stream for Attachment<P> {
    type Item = Result<WsMessage, WsError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.frames.poll_recv(cx).map(|frame| frame.map(Ok))
    }
}

/// Tells the connection task an attachment is gone.
#[derive(Debug)]
struct Detach<P: Protocol> {
    id: AttachId,
    commands: Option<mpsc::UnboundedSender<Command<P>>>,
}

impl<P: Protocol> Drop for Detach<P> {
    fn drop(&mut self) {
        if let Some(commands) = &self.commands {
            // The task having stopped already is the one way this fails, and then there is
            // nothing left to detach from.
            let _ = commands.send(Command::Detach(self.id));
        }
    }
}
