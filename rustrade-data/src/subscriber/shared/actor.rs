//! The task that owns a shared socket.

use super::{
    AttachId, AttachRequest, Attachment, Batch, Command, Detach, Frame, Handshake, Protocol,
    RELEASE_TIMEOUT,
    registry::{Discarded, Registry},
};
use fnv::FnvHashSet;
use futures::{SinkExt, StreamExt};
use rustrade_integration::{
    error::SocketError,
    protocol::websocket::{WebSocket, WsError, WsMessage},
};
use std::{future, time::Duration};
use tokio::{sync::mpsc, time::Instant};
use tracing::{debug, warn};
use url::Url;

/// The socket, what authentication reported about it, and what is subscribed on it.
struct Socket<P: Protocol> {
    websocket: WebSocket,
    session: P::Session,
    /// The slots subscribed on this socket and confirmed.
    subscribed: FnvHashSet<P::Slot>,
    /// When to ping next, for a provider that is pinged.
    keepalive: Option<Keepalive>,
}

/// Where a pinged socket is in its keepalive.
struct Keepalive {
    interval: Duration,
    next: Instant,
    /// Whether a ping went out with nothing read since.
    unanswered: bool,
}

impl Keepalive {
    fn new(interval: Duration) -> Self {
        Self {
            interval,
            next: Instant::now() + interval,
            unanswered: false,
        }
    }
}

pub(super) struct Actor<P: Protocol> {
    credentials: P::Credentials,
    commands: mpsc::UnboundedReceiver<Command<P>>,
    /// Handed to each attachment for its detach. Weak, so the task stops once every subscriber
    /// clone and every attachment is gone.
    detach: mpsc::WeakUnboundedSender<Command<P>>,
    socket: Option<Socket<P>>,
    /// The endpoint every registration was attached through: that of the last socket opened.
    endpoint: Option<Url>,
    registry: Registry<P>,
}

enum Event<P: Protocol> {
    Command(Option<Command<P>>),
    Frame(Option<Result<WsMessage, WsError>>),
    Expiry,
    Keepalive,
}

/// What reading one frame amounted to.
enum Read<Answer> {
    /// Routed, and what the frame answered, if anything.
    Done(Vec<Result<Answer, SocketError>>),
    /// The socket is gone, for the reason given.
    Lost(String),
}

/// Why a handshake did not settle.
enum Refusal {
    /// The provider answered with a refusal.
    Refused(SocketError),
    /// No answer: a timeout, a failed send or a lost socket. What the provider holds is unknown.
    Failed(SocketError),
}

impl<P: Protocol> Actor<P> {
    pub(super) fn new(
        credentials: P::Credentials,
        commands: mpsc::UnboundedReceiver<Command<P>>,
        detach: mpsc::WeakUnboundedSender<Command<P>>,
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

    pub(super) async fn run(mut self) {
        loop {
            let expiry = self.registry.next_expiry();
            let ping = self
                .socket
                .as_ref()
                .and_then(|socket| socket.keepalive.as_ref())
                .map(|keepalive| keepalive.next);

            let event = tokio::select! {
                command = self.commands.recv() => Event::Command(command),
                frame = next_frame(&mut self.socket) => Event::Frame(frame),
                () = sleep_until(expiry) => Event::Expiry,
                () = sleep_until(ping) => Event::Keepalive,
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
                            report_unawaited::<P>(answer);
                        }
                    }
                }
                Event::Expiry => self.expire().await,
                Event::Keepalive => self.keepalive().await,
            }
        }

        self.close().await;
    }

    async fn attach(&mut self, request: AttachRequest<P>) -> Result<Attachment<P>, SocketError> {
        // Checked whether or not the socket is open: a reconnect re-subscribes every registration
        // on the endpoint the request names, so they must all have been attached through it.
        if let Some(endpoint) = &self.endpoint
            && *endpoint != request.url
            && !self.registry.is_empty()
        {
            return Err(SocketError::Subscribe(format!(
                "{} is served from {}, but this {} connection is open to {} - streams sharing a \
                 connection must share an endpoint",
                request.exchange,
                request.url,
                P::NAME,
                endpoint,
            )));
        }

        let fresh = match &self.socket {
            Some(_) => {
                let (tx, rx) = mpsc::unbounded_channel();
                if let Some(id) = self.registry.reattach(&request, &tx) {
                    debug!(
                        exchange = %request.exchange,
                        "re-attached to the {} connection's reconnect",
                        P::NAME,
                    );
                    return Ok(self.attachment(id, rx));
                }

                false
            }
            None => {
                self.registry.forget_lost(&request);
                self.connect(&request.url).await?;
                self.registry.hold_lost();

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
        request: AttachRequest<P>,
        fresh: bool,
    ) -> Result<Attachment<P>, SocketError> {
        let Some(socket) = &self.socket else {
            return Err(not_open::<P>());
        };

        P::check(&request, &socket.session, &self.registry)?;

        let exchange = request.exchange;
        let timeout = request.timeout;

        // Request order first, so a batch reaches the wire in the order it was asked for. Scoped so
        // no borrowed slot is held across the handshake's awaits, which would demand `Sync` of it.
        let sending = {
            let mut seen = FnvHashSet::default();
            request
                .slots
                .iter()
                .chain(fresh.then(|| self.registry.slots()).into_iter().flatten())
                .filter(|slot| !socket.subscribed.contains(*slot) && seen.insert(*slot))
                .cloned()
                .collect::<Vec<_>>()
        };

        let (tx, rx) = mpsc::unbounded_channel();
        let id = self.registry.insert_live(request, tx);

        let (messages, mut handshake) = P::subscribe(&mut self.registry, id, &sending);

        let mut subscribed = Ok(());
        for message in messages {
            debug!(%exchange, payload = ?message, "sending {} subscription", P::NAME);
            subscribed = self.send(message).await;
            if subscribed.is_err() {
                break;
            }
        }
        if subscribed.is_ok() {
            subscribed = self.handshake(&mut handshake, timeout).await;
        }

        match subscribed {
            Ok(()) => {
                // Only for what was sent: an attach whose slots the socket already holds sends
                // nothing.
                if !sending.is_empty() {
                    debug!(
                        %exchange,
                        count = sending.len(),
                        "{} subscriptions confirmed",
                        P::NAME,
                    );
                }
                if let Some(socket) = self.socket.as_mut() {
                    socket.subscribed.extend(sending);
                }

                Ok(self.attachment(id, rx))
            }
            Err(Refusal::Refused(error)) => {
                let orphaned = self.registry.remove(id);
                if !fresh {
                    // A refusal that added nothing leaves nothing to unsubscribe, but the release
                    // still closes the socket should no stream remain.
                    let orphaned = if P::REFUSAL_IS_ATOMIC {
                        Vec::new()
                    } else {
                        orphaned
                    };
                    self.release(orphaned).await;
                }

                Err(P::refused(error))
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

    /// Read until `handshake` settles, routing everything else as usual.
    async fn handshake(
        &mut self,
        handshake: &mut P::Handshake,
        timeout: Duration,
    ) -> Result<(), Refusal> {
        if handshake.is_settled() {
            return Ok(());
        }

        let deadline = Instant::now() + timeout;

        loop {
            let Ok(frame) = tokio::time::timeout_at(deadline, next_frame(&mut self.socket)).await
            else {
                return Err(Refusal::Failed(SocketError::Subscribe(
                    handshake.timed_out(timeout),
                )));
            };

            match self.read(frame) {
                Read::Done(answers) => {
                    // Every confirmation in the frame is observed before any refusal in it is
                    // acted on, wherever in the frame the provider put it: the refusal can then
                    // say what was left unconfirmed, and a refusal sharing a frame with the answer
                    // that would settle the handshake is not lost to it.
                    let mut refusal = None;
                    for answer in answers {
                        match answer {
                            Ok(answer) => handshake.observe(answer),
                            Err(error) => {
                                if let Some(earlier) = refusal.replace(error) {
                                    report_unawaited::<P>(Err(earlier));
                                }
                            }
                        }
                    }

                    if let Some(error) = refusal {
                        return Err(Refusal::Refused(handshake.refusal(error)));
                    }
                    if handshake.is_settled() {
                        return Ok(());
                    }
                }
                Read::Lost(cause) => {
                    return Err(Refusal::Failed(SocketError::Subscribe(format!(
                        "{} connection lost while subscribing: {cause}",
                        P::NAME
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
        let expired = self.registry.expire(Instant::now());

        for discarded in expired.discarded {
            warn!(
                exchange = %discarded.exchange,
                kind = discarded.kind,
                subscriptions = discarded.subscriptions,
                discarded_frames = discarded.frames,
                grace = ?super::REATTACH_GRACE,
                "a {} stream did not re-attach after a reconnect; the frames held for it are \
                 discarded, and a later re-attach is a new attach{}",
                P::NAME,
                P::DISCARDED_ON_EXPIRY,
            );
        }

        for forgotten in expired.forgotten {
            // Usual for a stream dropped while its socket was down; nothing was held for it.
            debug!(
                exchange = %forgotten.exchange,
                kind = forgotten.kind,
                subscriptions = forgotten.subscriptions,
                grace = ?super::REATTACH_GRACE,
                "a {} stream did not re-attach after its connection was lost; it is forgotten, \
                 and a later re-attach is a new attach",
                P::NAME,
            );
        }

        self.release(expired.orphaned).await;
    }

    /// Unsubscribe `slots`, which nothing holds any longer, or close the socket if nothing at all
    /// remains attached.
    ///
    /// Where the provider confirms an unsubscribe, waits for it, so the next handshake cannot read
    /// it as its own answer.
    async fn release(&mut self, slots: Vec<P::Slot>) {
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

        // Not only what was confirmed: an attach whose handshake failed releases what it sent,
        // which the provider may hold unconfirmed.
        for slot in &slots {
            socket.subscribed.remove(slot);
        }

        let (messages, handshake) = P::release(&slots);
        for message in messages {
            debug!(payload = ?message, "releasing {} subscriptions", P::NAME);
            if self.send(message).await.is_err() {
                return;
            }
        }

        let Some(mut handshake) = handshake else {
            return;
        };

        match self.handshake(&mut handshake, RELEASE_TIMEOUT).await {
            Ok(()) => debug!(count = slots.len(), "{} subscriptions released", P::NAME),
            Err(Refusal::Refused(error) | Refusal::Failed(error)) => warn!(
                %error,
                "{} did not confirm releasing subscriptions no stream holds any longer; they may \
                 still count against the connection's subscription cap",
                P::NAME,
            ),
        }
    }

    async fn connect(&mut self, url: &Url) -> Result<(), SocketError> {
        let (websocket, session) = P::connect(&self.credentials, url).await?;

        self.endpoint = Some(url.clone());
        self.socket = Some(Socket {
            websocket,
            session,
            subscribed: FnvHashSet::default(),
            keepalive: P::KEEPALIVE.map(Keepalive::new),
        });

        Ok(())
    }

    /// Send one frame. A failed send loses the socket.
    async fn send(&mut self, message: WsMessage) -> Result<(), Refusal> {
        let Some(socket) = self.socket.as_mut() else {
            return Err(Refusal::Failed(not_open::<P>()));
        };

        if let Err(error) = socket.websocket.send(message).await {
            let cause = error.to_string();
            self.lose(&cause);
            return Err(Refusal::Failed(SocketError::WebSocket(Box::new(error))));
        }

        Ok(())
    }

    /// Ping the provider, or lose the socket if it delivered nothing since the last ping.
    async fn keepalive(&mut self) {
        let Some(keepalive) = self
            .socket
            .as_mut()
            .and_then(|socket| socket.keepalive.as_mut())
        else {
            return;
        };

        if keepalive.unanswered {
            let cause = format!(
                "nothing received from {} within {:?} of a ping",
                P::NAME,
                keepalive.interval
            );
            self.lose(&cause);
            return;
        }

        keepalive.unanswered = true;
        keepalive.next = Instant::now() + keepalive.interval;

        // A failed send loses the socket, which is all there is to do about it.
        let _ = self.send(WsMessage::Ping(Default::default())).await;
    }

    /// Classify one read, routing whatever it carries for the streams.
    fn read(&mut self, frame: Option<Result<WsMessage, WsError>>) -> Read<P::Answer> {
        let message = match frame {
            Some(Ok(message)) => message,
            Some(Err(error)) => return self.lose(&error.to_string()),
            None => return self.lose("the socket ended"),
        };

        // Anything read, not only the pong, shows the socket alive: a pong can queue behind a
        // burst of frames.
        if let Some(keepalive) = self
            .socket
            .as_mut()
            .and_then(|socket| socket.keepalive.as_mut())
        {
            keepalive.unanswered = false;
        }

        match P::read(&mut self.registry, &message) {
            Frame::Answers(answers) => Read::Done(answers),
            Frame::Closed(cause) => self.lose(&cause),
        }
    }

    /// The socket is gone: every stream on it ends, and waits for one of them to reconnect.
    fn lose(&mut self, cause: &str) -> Read<P::Answer> {
        if self.socket.take().is_some() {
            warn!(
                cause,
                endpoint = self.endpoint.as_ref().map(tracing::field::display),
                streams = self.registry.len(),
                "{} connection lost; every stream sharing it ends, and the first to re-attach \
                 reconnects for all of them",
                P::NAME,
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
        for Discarded {
            exchange,
            kind,
            subscriptions,
            frames,
        } in self.registry.lose_all(Instant::now())
        {
            warn!(
                %exchange,
                kind,
                subscriptions,
                discarded_frames = frames,
                "a {} reconnect was lost before this stream re-attached to it; the frames held \
                 for it are discarded{}",
                P::NAME,
                P::DISCARDED_ON_LOSS,
            );
        }
    }

    async fn close(&mut self) {
        if let Some(mut socket) = self.socket.take() {
            debug!(
                endpoint = self.endpoint.as_ref().map(tracing::field::display),
                "closing {} connection",
                P::NAME
            );
            // Best effort: the connection is being discarded either way.
            let _ = socket.websocket.close(None).await;
        }
    }

    fn attachment(
        &self,
        id: AttachId,
        frames: mpsc::UnboundedReceiver<WsMessage>,
    ) -> Attachment<P> {
        Attachment {
            frames,
            attached: self
                .registry
                .get(id)
                .map(|registration| registration.batch.attached())
                .unwrap_or_default(),
            _detach: Detach {
                id,
                commands: self.detach.upgrade(),
            },
        }
    }
}

fn not_open<P: Protocol>() -> SocketError {
    SocketError::Subscribe(format!("{} connection is not open", P::NAME))
}

/// Log an answer no handshake was waiting for.
fn report_unawaited<P: Protocol>(answer: Result<P::Answer, SocketError>) {
    match answer {
        Ok(answer) => debug!(?answer, "{} answer outside a handshake", P::NAME),
        Err(error) => warn!(
            %error,
            "{} reported an error outside a subscribe; the error need not name a subscription, so \
             check whether a stream on this connection has gone quiet",
            P::NAME,
        ),
    }
}

async fn next_frame<P: Protocol>(
    socket: &mut Option<Socket<P>>,
) -> Option<Result<WsMessage, WsError>> {
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

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use crate::subscriber::shared::{Connections, Frame};
    use futures::FutureExt;
    use rustrade_instrument::exchange::ExchangeId;
    use rustrade_integration::protocol::websocket::connect;
    use tokio::net::TcpListener;
    use tokio_tungstenite::WebSocketStream;

    const KEEPALIVE: Duration = Duration::from_millis(100);

    /// Serve one WebSocket on a local port, handing the accepted socket to `serve`.
    async fn server<F, Fut>(serve: F) -> Url
    where
        F: FnOnce(WebSocketStream<tokio::net::TcpStream>) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send,
    {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = Url::parse(&format!("ws://{}", listener.local_addr().unwrap())).unwrap();

        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let websocket = tokio_tungstenite::accept_async(stream).await.unwrap();
            serve(websocket).await;
        });

        url
    }

    fn request<P: Protocol<Slot = &'static str, Batch = ()>>(url: Url) -> AttachRequest<P> {
        AttachRequest {
            exchange: ExchangeId::Other,
            url,
            kind: "public_trades",
            slots: vec!["AAPL", "MSFT"],
            timeout: Duration::from_secs(5),
            batch: (),
        }
    }

    /// A pinged provider whose subscribes need no answer.
    #[derive(Debug)]
    struct Pinged;

    struct Settled;

    impl Handshake<()> for Settled {
        fn is_settled(&self) -> bool {
            true
        }

        fn observe(&mut self, (): ()) {}

        fn timed_out(&self, _: Duration) -> String {
            String::new()
        }
    }

    impl Protocol for Pinged {
        const NAME: &'static str = "Pinged";
        const CONNECTION_PER_ENDPOINT: bool = false;
        const REFUSAL_IS_ATOMIC: bool = false;
        const KEEPALIVE: Option<Duration> = Some(KEEPALIVE);

        type Credentials = ();
        type Slot = &'static str;
        type Batch = ();
        type Session = ();
        type Answer = ();
        type Handshake = Settled;

        async fn connect(_: &(), url: &Url) -> Result<(WebSocket, ()), SocketError> {
            Ok((connect(url.clone()).await?, ()))
        }

        fn subscribe(
            _: &mut Registry<Self>,
            _: AttachId,
            _: &[&'static str],
        ) -> (Vec<WsMessage>, Settled) {
            (Vec::new(), Settled)
        }

        fn release(_: &[&'static str]) -> (Vec<WsMessage>, Option<Settled>) {
            (Vec::new(), None)
        }

        fn read(_: &mut Registry<Self>, _: &WsMessage) -> Frame<()> {
            Frame::Answers(Vec::new())
        }
    }

    #[tokio::test]
    async fn a_socket_that_answers_its_pings_is_kept() {
        // Reading is what answers a ping: tungstenite queues the pong as the ping is read.
        let url =
            server(
                |mut websocket| async move { while let Some(Ok(_)) = websocket.next().await {} },
            )
            .await;

        let connections = Connections::<Pinged>::new(());
        let mut attachment = connections.attach(request(url)).await.unwrap();

        let ended = tokio::time::timeout(KEEPALIVE * 6, attachment.next()).await;
        assert!(ended.is_err(), "the attachment ended: {ended:?}");
    }

    #[tokio::test]
    async fn a_socket_silent_after_a_ping_is_lost() {
        // Never reads, so never answers a ping, and holds the socket open meanwhile.
        let url = server(|websocket| async move {
            tokio::time::sleep(Duration::from_secs(30)).await;
            drop(websocket);
        })
        .await;

        let connections = Connections::<Pinged>::new(());
        let mut attachment = connections.attach(request(url)).await.unwrap();

        // Lost at the second ping at the latest, after two intervals.
        let ended = tokio::time::timeout(KEEPALIVE * 4, attachment.next())
            .await
            .expect("a socket that never answered a ping was not lost");
        assert!(
            ended.is_none(),
            "expected the attachment to end, got {ended:?}"
        );
    }

    /// A provider answering a subscribe with a frame of one character per slot: `y` a
    /// confirmation, anything else a refusal naming nothing.
    #[derive(Debug)]
    struct Answered;

    struct Counted {
        expected: usize,
        confirmed: usize,
    }

    impl Handshake<()> for Counted {
        fn is_settled(&self) -> bool {
            self.confirmed >= self.expected
        }

        fn observe(&mut self, (): ()) {
            self.confirmed += 1;
        }

        fn timed_out(&self, _: Duration) -> String {
            "timed out".to_owned()
        }

        fn refusal(&self, error: SocketError) -> SocketError {
            SocketError::Subscribe(format!(
                "{error}; {} of {} confirmed",
                self.confirmed, self.expected
            ))
        }
    }

    impl Protocol for Answered {
        const NAME: &'static str = "Answered";
        const CONNECTION_PER_ENDPOINT: bool = false;
        const REFUSAL_IS_ATOMIC: bool = false;

        type Credentials = ();
        type Slot = &'static str;
        type Batch = ();
        type Session = ();
        type Answer = ();
        type Handshake = Counted;

        async fn connect(_: &(), url: &Url) -> Result<(WebSocket, ()), SocketError> {
            Ok((connect(url.clone()).await?, ()))
        }

        fn subscribe(
            _: &mut Registry<Self>,
            _: AttachId,
            sending: &[&'static str],
        ) -> (Vec<WsMessage>, Counted) {
            (
                vec![WsMessage::text(sending.join(","))],
                Counted {
                    expected: sending.len(),
                    confirmed: 0,
                },
            )
        }

        fn release(_: &[&'static str]) -> (Vec<WsMessage>, Option<Counted>) {
            (Vec::new(), None)
        }

        fn read(_: &mut Registry<Self>, message: &WsMessage) -> Frame<()> {
            let WsMessage::Text(text) = message else {
                return Frame::Answers(Vec::new());
            };

            Frame::Answers(
                text.chars()
                    .map(|answer| match answer {
                        'y' => Ok(()),
                        _ => Err(SocketError::Subscribe("refused".to_owned())),
                    })
                    .collect(),
            )
        }
    }

    /// Answer the one subscribe with `answers`, then hold the socket open.
    async fn answering(answers: &'static str) -> Url {
        server(move |mut websocket| async move {
            websocket.next().await.unwrap().unwrap();
            websocket.send(WsMessage::text(answers)).await.unwrap();
            while let Some(Ok(_)) = websocket.next().await {}
        })
        .await
    }

    #[tokio::test]
    async fn a_refusal_ahead_of_the_confirmations_in_its_frame_counts_them() {
        let connections = Connections::<Answered>::new(());

        let error = connections
            .attach(request(answering("ny").await))
            .await
            .expect_err("a refused subscribe attached");

        assert!(
            error.to_string().contains("refused; 1 of 2 confirmed"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn a_refusal_sharing_a_frame_with_the_settling_answer_is_not_lost() {
        let connections = Connections::<Answered>::new(());

        let error = connections
            .attach(request(answering("yyn").await))
            .await
            .expect_err("a refused subscribe attached");

        assert!(
            error.to_string().contains("refused; 2 of 2 confirmed"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn confirmations_alone_settle_the_handshake() {
        let connections = Connections::<Answered>::new(());

        let mut attachment = connections
            .attach(request(answering("yy").await))
            .await
            .unwrap();

        assert!(attachment.next().now_or_never().is_none());
    }
}
