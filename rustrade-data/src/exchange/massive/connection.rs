//! One WebSocket per Massive cluster, shared by every stream a
//! [`MassiveSubscriber`](super::MassiveSubscriber) and its clones open on it.
//!
//! # Why the connection is shared
//! Massive allows a key a fixed number of WebSocket connections **per cluster** — stocks, options,
//! forex, crypto and the rest each count separately — one on an individual plan. Past the limit it
//! does not refuse the new connection: on every cluster measured it **closes an older one**, with a
//! `max_connections` status, and the newcomer keeps streaming. One socket per stream could
//! therefore never stream two kinds from one cluster at once.
//!
//! # Shape
//! A task per cluster owns that cluster's socket. Each stream *attaches* to it and is handed a
//! [`MassiveAttachment`]: the frames carrying its own subscriptions, which it parses exactly as it
//! would parse a socket of its own.
//!
//! Massive packs messages for several channels and markets into one frame, a JSON array, so the
//! connection splits frames: it reads only each message's channel, `ev`, and its market, and a
//! stream receives a frame of only its own messages.
//!
//! - **Connect lazily, close when idle.** A cluster's socket opens on its first attach and closes
//!   once no stream remains attached to it.
//! - **Subscribe once per channel and market.** A subscription the socket already holds is not
//!   sent again — Massive would not answer it — and its messages reach every stream holding it.
//! - **Unsubscribe on the last detach.** Dropping an attachment unsubscribes everything no other
//!   stream holds.
//! - **One subscribe at a time.** Massive confirms each subscription by name, but refuses one with
//!   a bare `not authorized` naming none, so a refusal belongs to the subscribe in flight. Attaches
//!   and detaches are therefore serialised; frames keep flowing to every attached stream meanwhile.
//! - **Pinged.** Massive drops sockets without a close, nightly among other times, so the
//!   connection pings every [`KEEPALIVE`] and treats a socket silent since the last ping as lost.
//!
//! # What Massive does not answer
//! Massive says nothing at all to a subscription for a channel it does not publish, so an attach
//! naming one waits out its timeout, and the timeout names what was never confirmed. It confirms a
//! subscription to a market it does not list, and then publishes nothing for it: a misspelt
//! market is indistinguishable from a quiet one.
//!
//! # Reconnect
//! Every stream on a socket **ends together** when it is lost, and each is re-initialised by the
//! usual reconnect wrapper. The first to re-attach reconnects and re-subscribes everything the lost
//! socket held; the rest re-attach to that one reconnect. Massive replays nothing, so what it
//! published while no socket was open is missed.
//!
//! A stream keeps its place for [`REATTACH_GRACE`] after the socket is lost; frames for a stream
//! that has not re-attached yet are held until then, and discarded with a warning if it has not.
//!
//! # Sharing is by clone, and only by clone
//! Clones of one subscriber share one connection per cluster. A second subscriber built separately
//! for the same key, or another process, opens a second socket, and Massive closes the older one.
//! Its streams reconnect — closing the other in turn. The warning logged for each lost connection
//! quotes Massive's `max_connections` status verbatim, so the contest can be recognised; the
//! library does not back off from it.

pub use crate::subscriber::shared::REATTACH_GRACE;

use super::MassiveCredentials;
use crate::{
    exchange::ExchangeSub,
    subscriber::{
        shared::{
            self, AttachId, Attachment, Connections, Frame, Handshake, Protocol, Registry,
            elements::{self, Element, Elements, excerpt},
        },
        shared_stream::{SharedTransport, sealed},
    },
};
use futures::{SinkExt, Stream, StreamExt};
use rustrade_instrument::exchange::ExchangeId;
use rustrade_integration::{
    error::SocketError,
    protocol::websocket::{WebSocket, WsError, WsMessage, connect},
};
use serde::{Deserialize, Deserializer};
use serde_json::json;
use smol_str::SmolStr;
use std::{
    fmt,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};
use tracing::{debug, warn};
use url::Url;

/// How often the connection pings Massive.
pub const KEEPALIVE: Duration = Duration::from_secs(20);

/// How long authentication may take, each of its two frames.
const AUTH_TIMEOUT: Duration = Duration::from_secs(10);

/// What a subscribe confirmation says before the subscription it confirms.
const SUBSCRIBED: &str = "subscribed to: ";

/// What an unsubscribe confirmation says before the subscription it confirms.
const UNSUBSCRIBED: &str = "unsubscribed to: ";

/// One subscription on the socket: a channel and a market, `XT.BTC-USD` on the wire.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub(super) struct Slot {
    /// The channel, as the event its messages carry.
    pub(super) event: SmolStr,
    pub(super) market: SmolStr,
}

impl Slot {
    /// The subscription a confirmation names, `XT.BTC-USD`. Channels hold no dot, so the first
    /// one ends the channel, and a market holding one, `BRK.B`, survives.
    fn parse(param: &str) -> Option<Self> {
        let (event, market) = param.split_once('.')?;
        (!event.is_empty() && !market.is_empty()).then(|| Self {
            event: SmolStr::new(event),
            market: SmolStr::new(market),
        })
    }
}

/// The subscription a channel and market spell.
impl<Channel, Market> From<&ExchangeSub<Channel, Market>> for Slot
where
    Channel: AsRef<str>,
    Market: AsRef<str>,
{
    fn from(sub: &ExchangeSub<Channel, Market>) -> Self {
        Self {
            event: SmolStr::new(sub.channel.as_ref()),
            market: SmolStr::new(sub.market.as_ref()),
        }
    }
}

impl fmt::Display for Slot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}", self.event, self.market)
    }
}

/// The `subscribe` or `unsubscribe` payload for `slots`: one message, every subscription in it.
pub(super) fn action(action: &str, slots: &[Slot]) -> WsMessage {
    let params = slots
        .iter()
        .map(Slot::to_string)
        .collect::<Vec<_>>()
        .join(",");

    WsMessage::text(json!({ "action": action, "params": params }).to_string())
}

/// The Massive side of a shared connection.
#[derive(Debug)]
pub(super) struct Massive;

/// The handle a `MassiveSubscriber` and its clones share: the way into each cluster's connection
/// task.
#[derive(Debug)]
pub(super) struct MassiveConnections(Connections<Massive>);

impl MassiveConnections {
    pub(super) fn new(credentials: MassiveCredentials) -> Self {
        Self(Connections::new(credentials))
    }

    /// Attach one stream's batch to its cluster's connection, connecting first if nothing is
    /// attached.
    pub(super) async fn attach(
        &self,
        request: AttachRequest,
    ) -> Result<MassiveAttachment, SocketError> {
        let AttachRequest {
            exchange,
            url,
            kind,
            slots,
            timeout,
        } = request;

        self.0
            .attach(shared::AttachRequest {
                exchange,
                url,
                kind,
                slots,
                timeout,
                batch: (),
            })
            .await
            .map(MassiveAttachment)
    }
}

/// One stream's batch, as the connection needs it.
#[derive(Debug)]
pub(super) struct AttachRequest {
    pub(super) exchange: ExchangeId,
    pub(super) url: Url,
    /// [`SubscriptionKind::as_str`](crate::subscription::SubscriptionKind::as_str) of the batch.
    pub(super) kind: &'static str,
    /// The distinct subscriptions requested, in request order.
    pub(super) slots: Vec<Slot>,
    pub(super) timeout: Duration,
}

/// Waits for Massive to confirm each subscription sent, by name.
#[derive(Debug)]
pub(super) struct Confirmations {
    /// Those still unconfirmed, in request order.
    outstanding: Vec<Slot>,
}

impl Confirmations {
    fn outstanding(&self) -> String {
        self.outstanding
            .iter()
            .map(Slot::to_string)
            .collect::<Vec<_>>()
            .join(", ")
    }
}

impl Handshake<Slot> for Confirmations {
    fn is_settled(&self) -> bool {
        self.outstanding.is_empty()
    }

    fn observe(&mut self, confirmed: Slot) {
        let before = self.outstanding.len();
        self.outstanding.retain(|slot| *slot != confirmed);

        if self.outstanding.len() == before {
            debug!(%confirmed, "Massive confirmed a subscription this subscribe did not send");
        }
    }

    fn timed_out(&self, timeout: Duration) -> String {
        format!(
            "subscription validation timeout reached: {timeout:?}; Massive never confirmed: {}. \
             Massive does not answer a subscription to a channel it does not publish on the \
             cluster, so one that waits for ever is usually a channel the cluster does not offer",
            self.outstanding()
        )
    }

    /// Name what the refusal may be for: Massive's names nothing, and whatever it refused is among
    /// what it never confirmed.
    fn refusal(&self, error: SocketError) -> SocketError {
        match error {
            SocketError::Subscribe(message) if !self.outstanding.is_empty() => {
                SocketError::Subscribe(format!(
                    "{message}; Massive refused a subscription among those it did not confirm: {}",
                    self.outstanding()
                ))
            }
            other => other,
        }
    }
}

impl Protocol for Massive {
    const NAME: &'static str = "Massive";
    // Massive caps connections per cluster, and each cluster has an endpoint of its own.
    const CONNECTION_PER_ENDPOINT: bool = true;
    // Massive refuses each subscription separately, confirming the rest of the subscribe.
    const REFUSAL_IS_ATOMIC: bool = false;
    const KEEPALIVE: Option<Duration> = Some(KEEPALIVE);

    type Credentials = MassiveCredentials;
    type Slot = Slot;
    type Batch = ();
    type Session = ();
    type Answer = Slot;
    type Handshake = Confirmations;

    async fn connect(
        credentials: &MassiveCredentials,
        url: &Url,
    ) -> Result<(WebSocket, ()), SocketError> {
        let mut websocket = connect(url.clone()).await?;
        authenticate(&mut websocket, credentials).await?;
        debug!(%url, "authenticated to Massive WebSocket");

        Ok((websocket, ()))
    }

    fn subscribe(
        _registry: &mut Registry<Self>,
        _id: AttachId,
        sending: &[Slot],
    ) -> (Vec<WsMessage>, Confirmations) {
        let messages = if sending.is_empty() {
            Vec::new()
        } else {
            vec![action("subscribe", sending)]
        };

        (
            messages,
            Confirmations {
                outstanding: sending.to_vec(),
            },
        )
    }

    /// Answered by an `unsubscribed to:` confirmation per subscription, which names it, so no
    /// later handshake could take it for its own answer, and there is nothing to wait for.
    fn release(slots: &[Slot]) -> (Vec<WsMessage>, Option<Confirmations>) {
        (vec![action("unsubscribe", slots)], None)
    }

    fn read(registry: &mut Registry<Self>, message: &WsMessage) -> Frame<Slot> {
        let raws = match elements::elements::<Self>(message) {
            Elements::Array(raws) => raws,
            Elements::Closed(cause) => return Frame::Closed(cause),
            Elements::Ignored => return Frame::Answers(Vec::new()),
        };

        let total = raws.len();
        let mut data = Vec::with_capacity(total);
        let mut answers = Vec::new();

        for raw in raws {
            let envelope = match serde_json::from_str::<Envelope>(raw.get()) {
                Ok(envelope) => envelope,
                Err(error) => {
                    warn!(
                        %error,
                        message = %excerpt(raw.get()),
                        "Massive sent a message that could not be read; ignored",
                    );
                    continue;
                }
            };

            if envelope.event == "status" {
                match status(envelope.status.as_deref(), envelope.message.as_deref()) {
                    Status::Answer(answer) => answers.push(answer),
                    Status::Evicted(cause) => return Frame::Closed(cause),
                    Status::Other => {}
                }
                continue;
            }

            let Some(market) = envelope.market() else {
                warn!(
                    message = %excerpt(raw.get()),
                    "Massive sent a market data message naming no market; ignored",
                );
                continue;
            };

            data.push(Element {
                slot: Slot {
                    event: envelope.event,
                    market,
                },
                raw,
            });
        }

        elements::route(registry, message, total, &data);
        Frame::Answers(answers)
    }

    /// Add to a refusal what the stream cannot see: the cluster's subscriptions are shared.
    fn refused(error: SocketError) -> SocketError {
        match error {
            SocketError::Subscribe(message) => SocketError::Subscribe(format!(
                "{message} (the connection is shared by every stream this subscriber and its \
                 clones open on the cluster)"
            )),
            other => other,
        }
    }
}

/// One stream's view of a Massive cluster's shared connection.
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
pub struct MassiveAttachment(Attachment<Massive>);

impl Stream for MassiveAttachment {
    type Item = Result<WsMessage, WsError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.0).poll_next(cx)
    }
}

impl sealed::Sealed for MassiveAttachment {}

/// Massive hands a stream nothing on attaching.
impl SharedTransport for MassiveAttachment {
    type Attached = ();

    fn take_attached(&mut self) {
        self.0.take_attached()
    }
}

/// The part of a message the connection reads.
///
/// Short strings deserialise inline — a channel and a market both fit — so this allocates nothing
/// per market data message.
#[derive(Deserialize)]
struct Envelope {
    #[serde(rename = "ev")]
    event: SmolStr,
    /// Stocks and options.
    #[serde(default)]
    sym: Option<SmolStr>,
    /// Crypto, and forex aggregates.
    #[serde(default)]
    pair: Option<SmolStr>,
    /// Forex quotes' pair — but crypto trades' price, so read only when it is a string.
    #[serde(default, deserialize_with = "text_only")]
    p: Option<SmolStr>,
    /// A status message's status.
    #[serde(default)]
    status: Option<String>,
    /// A status message's text.
    #[serde(default)]
    message: Option<String>,
}

impl Envelope {
    fn market(&self) -> Option<SmolStr> {
        self.sym
            .as_ref()
            .or(self.pair.as_ref())
            .or(self.p.as_ref())
            .cloned()
    }
}

/// A string, or nothing if the field holds anything else.
fn text_only<'de, D>(deserializer: D) -> Result<Option<SmolStr>, D::Error>
where
    D: Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum TextOrOther {
        Text(SmolStr),
        Other(serde::de::IgnoredAny),
    }

    Ok(match Option::<TextOrOther>::deserialize(deserializer)? {
        Some(TextOrOther::Text(text)) => Some(text),
        Some(TextOrOther::Other(_)) | None => None,
    })
}

/// What a status message amounts to.
enum Status {
    /// A confirmation of the subscribe in flight, or a refusal of it.
    Answer(Result<Slot, SocketError>),
    /// Massive closed this connection for another on the same key and cluster.
    Evicted(String),
    /// Nothing a handshake waits for.
    Other,
}

fn status(status: Option<&str>, message: Option<&str>) -> Status {
    let message = message.unwrap_or_default();

    match status {
        Some("success") => {
            if let Some(param) = message.strip_prefix(SUBSCRIBED) {
                match Slot::parse(param) {
                    Some(slot) => Status::Answer(Ok(slot)),
                    None => {
                        warn!(message, "Massive confirmed a subscription it did not name");
                        Status::Other
                    }
                }
            } else if message.starts_with(UNSUBSCRIBED) {
                debug!(message, "Massive confirmed an unsubscribe");
                Status::Other
            } else {
                warn!(
                    message,
                    "Massive reported a success this client does not recognise"
                );
                Status::Other
            }
        }
        Some("error") => Status::Answer(Err(SocketError::Subscribe(format!(
            "Massive refused a subscription: {message}"
        )))),
        Some("max_connections") => Status::Evicted(format!(
            "Massive closed the connection: max_connections: {message} (another connection opened \
             with this key on this cluster, by a subscriber built separately or another process, \
             took its place)"
        )),
        Some("connected" | "auth_success") => {
            debug!(message, "Massive status");
            Status::Other
        }
        other => {
            warn!(
                status = other.unwrap_or("<none>"),
                message, "Massive reported a status this client does not recognise"
            );
            Status::Other
        }
    }
}

/// Wait for Massive's `connected` status, send the key, and wait for `auth_success`.
async fn authenticate(
    websocket: &mut WebSocket,
    credentials: &MassiveCredentials,
) -> Result<(), SocketError> {
    let connected = next_status(websocket, "connected").await?;
    if connected.0.as_deref() != Some("connected") {
        // Not what Massive sends first, but nothing depends on it: authentication decides.
        debug!(status = ?connected, "Massive's first frame was not the expected connected status");
    }

    websocket
        .send(WsMessage::text(
            json!({ "action": "auth", "params": credentials.api_key() }).to_string(),
        ))
        .await
        .map_err(|error| SocketError::WebSocket(Box::new(error)))?;

    match next_status(websocket, "auth_success").await? {
        (Some(status), _) if status == "auth_success" => Ok(()),
        (status, message) => Err(SocketError::Subscribe(format!(
            "Massive refused authentication: {}: {}",
            status.as_deref().unwrap_or("<no status>"),
            message.as_deref().unwrap_or_default(),
        ))),
    }
}

/// The first status of the next frame, while authenticating.
async fn next_status(
    websocket: &mut WebSocket,
    awaiting: &str,
) -> Result<(Option<String>, Option<String>), SocketError> {
    let frame = tokio::time::timeout(AUTH_TIMEOUT, websocket.next())
        .await
        .map_err(|_| {
            SocketError::Subscribe(format!(
                "Massive sent nothing within {AUTH_TIMEOUT:?} while awaiting {awaiting}"
            ))
        })?;

    let text = match frame {
        Some(Ok(WsMessage::Text(text))) => text,
        Some(Ok(WsMessage::Close(close))) => {
            return Err(SocketError::Subscribe(format!(
                "Massive closed the connection while awaiting {awaiting}: {close:?}"
            )));
        }
        Some(Ok(other)) => {
            return Err(SocketError::Subscribe(format!(
                "Massive sent {other:?} while awaiting {awaiting}"
            )));
        }
        Some(Err(error)) => return Err(SocketError::WebSocket(Box::new(error))),
        None => {
            return Err(SocketError::Subscribe(format!(
                "Massive ended the connection while awaiting {awaiting}"
            )));
        }
    };

    let statuses = serde_json::from_str::<Vec<Envelope>>(text.as_str()).map_err(|error| {
        SocketError::Deserialise {
            error,
            payload: text.as_str().to_owned(),
        }
    })?;

    Ok(statuses
        .into_iter()
        .find(|envelope| envelope.event == "status")
        .map(|envelope| (envelope.status, envelope.message))
        .unwrap_or_default())
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use serde_json::{Value, json};
    use tokio::sync::mpsc;

    fn slot(event: &str, market: &str) -> Slot {
        Slot {
            event: SmolStr::new(event),
            market: SmolStr::new(market),
        }
    }

    fn attach(
        registry: &mut Registry<Massive>,
        slots: &[(&str, &str)],
    ) -> (AttachId, mpsc::UnboundedReceiver<WsMessage>) {
        let (tx, rx) = mpsc::unbounded_channel();
        let request = shared::AttachRequest {
            exchange: ExchangeId::MassiveCrypto,
            url: Url::parse("ws://127.0.0.1:1").unwrap(),
            kind: "public_trades",
            slots: slots
                .iter()
                .map(|(event, market)| slot(event, market))
                .collect(),
            timeout: Duration::from_secs(1),
            batch: (),
        };

        (registry.insert_live(request, tx), rx)
    }

    fn frame(elements: &[Value]) -> WsMessage {
        WsMessage::text(Value::Array(elements.to_vec()).to_string())
    }

    fn read(
        registry: &mut Registry<Massive>,
        message: &WsMessage,
    ) -> Vec<Result<Slot, SocketError>> {
        match Massive::read(registry, message) {
            Frame::Answers(answers) => answers,
            Frame::Closed(cause) => panic!("expected a frame of messages, got a close: {cause}"),
        }
    }

    fn received(rx: &mut mpsc::UnboundedReceiver<WsMessage>) -> Vec<Vec<Value>> {
        std::iter::from_fn(|| rx.try_recv().ok())
            .map(|message| match message {
                WsMessage::Text(text) => serde_json::from_str(text.as_str()).unwrap(),
                other => panic!("expected a text frame, got {other:?}"),
            })
            .collect()
    }

    fn trade(pair: &str) -> Value {
        json!({"ev": "XT", "pair": pair, "p": 82857.53, "s": 0.01, "t": 1, "c": [2], "i": "1"})
    }

    fn quote(pair: &str) -> Value {
        json!({"ev": "XQ", "pair": pair, "bp": 1.0, "bs": 1.0, "ap": 2.0, "as": 1.0, "t": 1})
    }

    fn status(status: &str, message: &str) -> Value {
        json!({"ev": "status", "status": status, "message": message})
    }

    #[test]
    fn a_mixed_frame_is_split_per_stream_and_its_status_answers() {
        let mut registry = Registry::default();
        let (_, mut trades) = attach(&mut registry, &[("XT", "BTC-USD")]);
        let (_, mut quotes) = attach(&mut registry, &[("XQ", "BTC-USD")]);

        let answers = read(
            &mut registry,
            &frame(&[
                trade("BTC-USD"),
                status("success", "subscribed to: XQ.BTC-USD"),
                quote("BTC-USD"),
                trade("ETH-USD"),
            ]),
        );

        assert_eq!(answers.len(), 1);
        assert_eq!(answers[0].as_ref().unwrap(), &slot("XQ", "BTC-USD"));
        assert_eq!(received(&mut trades), vec![vec![trade("BTC-USD")]]);
        assert_eq!(received(&mut quotes), vec![vec![quote("BTC-USD")]]);
    }

    #[test]
    fn a_frame_wholly_for_one_stream_is_forwarded_as_sent() {
        let mut registry = Registry::default();
        let (_, mut trades) = attach(&mut registry, &[("XT", "BTC-USD"), ("XT", "ETH-USD")]);

        let sent = frame(&[trade("BTC-USD"), trade("ETH-USD")]);
        read(&mut registry, &sent);

        assert_eq!(trades.try_recv().unwrap(), sent);
    }

    #[test]
    fn each_cluster_names_its_market_in_its_own_field() {
        let mut registry = Registry::default();
        let (_, mut stocks) = attach(&mut registry, &[("T", "BRK.B")]);
        let (_, mut forex) = attach(&mut registry, &[("C", "EUR/USD")]);
        let (_, mut aggregates) = attach(&mut registry, &[("CAS", "EUR/USD")]);

        let stock = json!({"ev": "T", "sym": "BRK.B", "p": 1.0, "s": 1, "t": 1});
        // Crypto's `p` is a price, forex's a pair: only a string is read as a market.
        let fx = json!({"ev": "C", "p": "EUR/USD", "a": 1.1, "b": 1.0, "t": 1});
        let bar = json!({"ev": "CAS", "pair": "EUR/USD", "o": 1, "h": 1, "l": 1, "c": 1, "v": 3,
                         "s": 0, "e": 1000});
        read(
            &mut registry,
            &frame(&[stock.clone(), fx.clone(), bar.clone()]),
        );

        assert_eq!(received(&mut stocks), vec![vec![stock]]);
        assert_eq!(received(&mut forex), vec![vec![fx]]);
        assert_eq!(received(&mut aggregates), vec![vec![bar]]);
    }

    #[test]
    fn a_refusal_names_nothing_and_an_unsubscribe_confirmation_answers_nothing() {
        let answers = read(
            &mut Registry::default(),
            &frame(&[
                status("error", "not authorized"),
                status("success", "subscribed to: XT.ETH-USD"),
                status("success", "unsubscribed to: XT.BTC-USD"),
            ]),
        );

        assert_eq!(answers.len(), 2);
        assert!(
            matches!(&answers[0], Err(SocketError::Subscribe(message)) if message.contains("not authorized"))
        );
        assert_eq!(answers[1].as_ref().unwrap(), &slot("XT", "ETH-USD"));
    }

    #[test]
    fn an_eviction_closes_the_connection_quoting_massive() {
        let evicted = frame(&[status(
            "max_connections",
            "Maximum number of websocket connections exceeded.",
        )]);

        match Massive::read(&mut Registry::default(), &evicted) {
            Frame::Closed(cause) => {
                assert!(cause.contains("max_connections"), "{cause}");
                assert!(
                    cause.contains("Maximum number of websocket connections"),
                    "{cause}"
                );
            }
            Frame::Answers(_) => panic!("an eviction must close the connection"),
        }
    }

    #[test]
    fn a_confirmation_names_its_subscription_even_when_the_market_holds_a_dot() {
        assert_eq!(Slot::parse("T.BRK.B"), Some(slot("T", "BRK.B")));
        assert_eq!(Slot::parse("C.EUR/USD"), Some(slot("C", "EUR/USD")));
        assert_eq!(Slot::parse("XT"), None);
        assert_eq!(Slot::parse(".BTC-USD"), None);
    }

    #[test]
    fn a_handshake_settles_on_every_confirmation_and_names_what_is_missing() {
        let (messages, mut handshake) = Massive::subscribe(
            &mut Registry::default(),
            0,
            &[slot("XT", "BTC-USD"), slot("XQ", "BTC-USD")],
        );

        assert_eq!(
            messages,
            vec![WsMessage::text(
                json!({"action": "subscribe", "params": "XT.BTC-USD,XQ.BTC-USD"}).to_string()
            )]
        );

        handshake.observe(slot("XQ", "BTC-USD"));
        assert!(!handshake.is_settled());

        let timed_out = handshake.timed_out(Duration::from_secs(10));
        assert!(
            timed_out.contains("never confirmed: XT.BTC-USD"),
            "{timed_out}"
        );

        let refused = handshake
            .refusal(SocketError::Subscribe("not authorized".to_owned()))
            .to_string();
        assert!(refused.contains("not authorized"), "{refused}");
        assert!(refused.contains("XT.BTC-USD"), "{refused}");

        handshake.observe(slot("XT", "BTC-USD"));
        assert!(handshake.is_settled());
    }

    #[test]
    fn an_attach_sending_nothing_waits_for_nothing() {
        let (messages, handshake) = Massive::subscribe(&mut Registry::default(), 0, &[]);

        assert!(messages.is_empty());
        assert!(handshake.is_settled());
    }

    #[test]
    fn a_release_is_one_unsubscribe_and_waits_for_nothing() {
        let (messages, handshake) =
            Massive::release(&[slot("XT", "BTC-USD"), slot("C", "EUR/USD")]);

        assert_eq!(
            messages,
            vec![WsMessage::text(
                json!({"action": "unsubscribe", "params": "XT.BTC-USD,C.EUR/USD"}).to_string()
            )]
        );
        assert!(handshake.is_none());
    }
}
