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
//! socket held; the rest re-attach to that one reconnect. Alpaca replays nothing, so what the
//! provider sent while no socket was open is missed.
//!
//! A stream keeps its place for [`REATTACH_GRACE`] after the socket is lost. Frames for a stream
//! that has not re-attached yet are held until then, and discarded with a warning if it has not;
//! its pairs are then released. A stream dropped while the socket was down is forgotten the same
//! way, so no later reconnect re-subscribes its pairs — which could otherwise push every reconnect
//! over the pair cap. The grace runs from the loss, and a reconnect that fails does not extend it.
//!
//! # Sharing is by clone, and only by clone
//! Clones of one subscriber share one connection per feed. Two subscribers built separately for
//! one account open two sockets, and Alpaca refuses the second — visibly, which is left to surface
//! rather than papered over by a process-wide registry.

pub use crate::subscriber::shared::REATTACH_GRACE;

use super::{
    AlpacaCredentials, alpaca_authenticate,
    channel::AlpacaChannel,
    channel_message,
    subscription::{AlpacaSubResponse, AlpacaSubResponseInner},
};
use crate::subscriber::shared::{
    self, AttachId, Attachment, Connections, Frame, Handshake, Protocol, Registry,
    elements::{self, Element, Elements, excerpt},
};
use crate::subscriber::shared_stream::{SharedTransport, sealed};
use fnv::FnvHashSet;
use futures::Stream;
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
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};
use tracing::{debug, warn};
use url::Url;

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

/// The Alpaca side of a shared connection.
#[derive(Debug)]
pub(super) struct Alpaca;

/// The handle an `AlpacaSubscriber` and its clones share: the way into each feed's connection task.
#[derive(Debug)]
pub(super) struct AlpacaConnections(Connections<Alpaca>);

impl AlpacaConnections {
    pub(super) fn new(credentials: AlpacaCredentials) -> Self {
        Self(Connections::new(credentials))
    }

    /// Attach one stream's batch to its feed's connection, connecting first if nothing is attached.
    pub(super) async fn attach(
        &self,
        request: AttachRequest,
    ) -> Result<AlpacaAttachment, SocketError> {
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
            .map(AlpacaAttachment)
    }
}

/// One stream's batch, as the connection needs it.
#[derive(Debug)]
pub(super) struct AttachRequest {
    pub(super) exchange: ExchangeId,
    pub(super) url: Url,
    /// [`SubscriptionKind::as_str`](crate::subscription::SubscriptionKind::as_str) of the batch.
    pub(super) kind: &'static str,
    /// The distinct pairs requested, in request order.
    pub(super) slots: Vec<Slot>,
    pub(super) timeout: Duration,
}

/// What a handshake waits for Alpaca to report.
#[derive(Debug)]
pub(super) struct Awaiting {
    asked: Asked,
    slots: Vec<Slot>,
    /// The latest state Alpaca reported, so a timeout can name what it never reported.
    last: Option<FnvHashSet<Slot>>,
}

#[derive(Debug)]
enum Asked {
    /// Every pair subscribed.
    Subscribed,
    /// None of the pairs subscribed any longer.
    Released,
}

impl Awaiting {
    fn new(asked: Asked, slots: &[Slot]) -> Self {
        Self {
            asked,
            slots: slots.to_vec(),
            last: None,
        }
    }

    /// Whether `state` reports `slot` the way this handshake asks for.
    fn settles(&self, state: &FnvHashSet<Slot>, slot: &Slot) -> bool {
        match self.asked {
            Asked::Subscribed => state.contains(slot),
            Asked::Released => !state.contains(slot),
        }
    }

    fn is_answered_by(&self, state: &FnvHashSet<Slot>) -> bool {
        self.slots.iter().all(|slot| self.settles(state, slot))
    }
}

impl Handshake<FnvHashSet<Slot>> for Awaiting {
    fn is_settled(&self) -> bool {
        self.slots.is_empty()
            || self
                .last
                .as_ref()
                .is_some_and(|state| self.is_answered_by(state))
    }

    /// Alpaca reports the connection's whole state, so the latest answer is the one that counts.
    fn observe(&mut self, state: FnvHashSet<Slot>) {
        debug!(held = state.len(), "Alpaca subscription state");
        self.last = Some(state);
    }

    /// The pairs it still waits on, in request order. With no state reported, that is every pair.
    fn timed_out(&self, timeout: Duration) -> String {
        let asked = match self.asked {
            Asked::Subscribed => "subscribed",
            Asked::Released => "unsubscribed",
        };

        let outstanding = self
            .slots
            .iter()
            .filter(|slot| {
                self.last
                    .as_ref()
                    .is_none_or(|state| !self.settles(state, slot))
            })
            .map(|slot| format!("{} {}", slot.channel.as_ref(), slot.symbol))
            .collect::<Vec<_>>()
            .join(", ");

        format!(
            "subscription validation timeout reached: {timeout:?}; Alpaca never reported as \
             {asked}: {outstanding}"
        )
    }
}

impl Protocol for Alpaca {
    const NAME: &'static str = "Alpaca";
    // Alpaca caps connections per feed, and each feed has an endpoint of its own.
    const CONNECTION_PER_ENDPOINT: bool = true;
    // Alpaca refuses a subscribe whole, adding none of it.
    const REFUSAL_IS_ATOMIC: bool = true;

    type Credentials = AlpacaCredentials;
    type Slot = Slot;
    type Batch = ();
    type Session = ();
    type Answer = FnvHashSet<Slot>;
    type Handshake = Awaiting;

    async fn connect(
        credentials: &AlpacaCredentials,
        url: &Url,
    ) -> Result<(WebSocket, ()), SocketError> {
        let mut websocket = connect(url.clone()).await?;
        alpaca_authenticate(&mut websocket, credentials)
            .await
            .map_err(connection_limit_context)?;
        debug!(%url, "authenticated to Alpaca WebSocket");

        Ok((websocket, ()))
    }

    fn subscribe(
        _registry: &mut Registry<Self>,
        _id: AttachId,
        sending: &[Slot],
    ) -> (Vec<WsMessage>, Awaiting) {
        let messages = if sending.is_empty() {
            Vec::new()
        } else {
            vec![subscribe_message(sending)]
        };

        (messages, Awaiting::new(Asked::Subscribed, sending))
    }

    /// Answered, like a subscribe, with the connection's whole state, which the next subscribe
    /// could otherwise take for its own answer.
    fn release(slots: &[Slot]) -> (Vec<WsMessage>, Option<Awaiting>) {
        (
            vec![unsubscribe_message(slots)],
            Some(Awaiting::new(Asked::Released, slots)),
        )
    }

    fn read(registry: &mut Registry<Self>, message: &WsMessage) -> Frame<FnvHashSet<Slot>> {
        match classify(message) {
            Classified::Elements {
                total,
                data,
                answers,
            } => {
                elements::route(registry, message, total, &data);
                Frame::Answers(answers)
            }
            Classified::Closed(cause) => Frame::Closed(cause),
            Classified::Ignored => Frame::Answers(Vec::new()),
        }
    }

    /// Add to a refusal for the subscription cap what the stream cannot see: the cap is shared.
    fn refused(error: SocketError) -> SocketError {
        match error {
            SocketError::Subscribe(message) if message.contains("symbol limit exceeded") => {
                SocketError::Subscribe(format!(
                    "{message} (the connection and its subscription cap are shared by every \
                     stream this subscriber and its clones open on the feed)"
                ))
            }
            other => other,
        }
    }
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
pub struct AlpacaAttachment(Attachment<Alpaca>);

impl Stream for AlpacaAttachment {
    type Item = Result<WsMessage, WsError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.0).poll_next(cx)
    }
}

impl sealed::Sealed for AlpacaAttachment {}

/// Alpaca hands a stream nothing on attaching.
impl SharedTransport for AlpacaAttachment {
    type Attached = ();

    fn take_attached(&mut self) {
        self.0.take_attached()
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

enum Classified<'a> {
    Elements {
        /// Every element in the frame, of every kind.
        total: usize,
        data: Vec<Element<'a, Slot>>,
        answers: Vec<Result<FnvHashSet<Slot>, SocketError>>,
    },
    Closed(String),
    Ignored,
}

fn classify(message: &WsMessage) -> Classified<'_> {
    let elements = match elements::elements::<Alpaca>(message) {
        Elements::Array(elements) => elements,
        Elements::Closed(cause) => return Classified::Closed(cause),
        Elements::Ignored => return Classified::Ignored,
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
                    element = %excerpt(raw.get()),
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
                element = %excerpt(raw.get()),
                "Alpaca sent a market data message with no symbol; ignored",
            );
            continue;
        };

        data.push(Element {
            slot: Slot { channel, symbol },
            raw,
        });
    }

    Classified::Elements {
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
        .flat_map(AlpacaSubResponseInner::covered)
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
    use tokio::sync::mpsc;

    const TRADES: AlpacaChannel = AlpacaChannel::Trades;
    const QUOTES: AlpacaChannel = AlpacaChannel::Quotes;

    fn slot(channel: AlpacaChannel, symbol: &str) -> Slot {
        Slot {
            channel,
            symbol: SmolStr::new(symbol),
        }
    }

    fn attach(
        registry: &mut Registry<Alpaca>,
        pairs: &[(AlpacaChannel, &str)],
    ) -> (AttachId, mpsc::UnboundedReceiver<WsMessage>) {
        let (tx, rx) = mpsc::unbounded_channel();
        let request = shared::AttachRequest {
            exchange: ExchangeId::AlpacaIex,
            url: Url::parse("ws://127.0.0.1:1").unwrap(),
            kind: "public_trades",
            slots: pairs
                .iter()
                .map(|(channel, symbol)| slot(*channel, symbol))
                .collect(),
            timeout: Duration::from_secs(1),
            batch: (),
        };

        (registry.insert_live(request, tx), rx)
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

    /// Route one frame through the registry exactly as the connection task does, returning what
    /// it answers.
    fn deliver(
        registry: &mut Registry<Alpaca>,
        message: &WsMessage,
    ) -> Vec<Result<FnvHashSet<Slot>, SocketError>> {
        match Alpaca::read(registry, message) {
            Frame::Answers(answers) => answers,
            Frame::Closed(cause) => panic!("expected a frame of elements, got a close: {cause}"),
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
        deliver(&mut Registry::default(), message)
    }

    fn only<T: std::fmt::Debug>(mut items: Vec<T>) -> T {
        assert_eq!(items.len(), 1, "expected exactly one, got {items:?}");
        items.remove(0)
    }

    fn state(slots: &[Slot]) -> FnvHashSet<Slot> {
        slots.iter().cloned().collect()
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
        assert!(deliver(&mut registry, &success).is_empty());

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
            Alpaca::refused(only(answers(&frame)).unwrap_err()).to_string()
        };

        assert!(refusal("symbol limit exceeded").contains("shared by every stream"));
        assert!(!refusal("invalid syntax").contains("shared by every stream"));
    }

    #[test]
    fn a_timed_out_handshake_names_what_alpaca_never_reported() {
        let slots = [slot(TRADES, "AAPL"), slot(QUOTES, "MSFT")];
        let timeout = Duration::from_secs(1);
        let reported = |asked, last: Option<&[Slot]>| {
            let mut awaiting = Awaiting::new(asked, &slots);
            if let Some(last) = last {
                awaiting.observe(state(last));
            }
            awaiting.timed_out(timeout)
        };

        let subscribing = reported(Asked::Subscribed, Some(&slots[..1]));
        assert!(
            subscribing.ends_with("subscribed: quotes MSFT"),
            "{subscribing}"
        );

        let nothing_reported = reported(Asked::Subscribed, None);
        assert!(
            nothing_reported.ends_with("trades AAPL, quotes MSFT"),
            "{nothing_reported}"
        );

        let releasing = reported(Asked::Released, Some(&slots[..1]));
        assert!(
            releasing.ends_with("unsubscribed: trades AAPL"),
            "{releasing}"
        );

        let nothing_released = reported(Asked::Released, None);
        assert!(
            nothing_released.ends_with("unsubscribed: trades AAPL, quotes MSFT"),
            "{nothing_released}"
        );
    }

    #[test]
    fn a_frame_that_is_not_an_array_is_ignored() {
        assert!(matches!(
            classify(&WsMessage::text(r#"{"T":"t"}"#)),
            Classified::Ignored
        ));
    }

    #[test]
    fn a_state_answers_a_subscribe_only_once_it_holds_every_pair() {
        let sending = [slot(TRADES, "AAPL"), slot(QUOTES, "AAPL")];
        let mut awaiting = Awaiting::new(Asked::Subscribed, &sending);
        assert!(!awaiting.is_settled());

        awaiting.observe(state(&[slot(TRADES, "AAPL")]));
        assert!(!awaiting.is_settled());

        awaiting.observe(state(&[
            slot(TRADES, "AAPL"),
            slot(QUOTES, "AAPL"),
            slot(TRADES, "MSFT"),
        ]));
        assert!(awaiting.is_settled());
    }

    #[test]
    fn a_state_answers_an_unsubscribe_only_once_it_holds_none_of_the_pairs() {
        let releasing = [slot(TRADES, "AAPL")];
        let mut awaiting = Awaiting::new(Asked::Released, &releasing);

        awaiting.observe(state(&[slot(TRADES, "AAPL")]));
        assert!(!awaiting.is_settled());

        awaiting.observe(state(&[slot(QUOTES, "AAPL")]));
        assert!(awaiting.is_settled());
    }

    #[test]
    fn an_attach_sending_nothing_waits_for_nothing() {
        let mut registry = Registry::default();
        let (id, _rx) = attach(&mut registry, &[(TRADES, "AAPL")]);

        let (messages, awaiting) = Alpaca::subscribe(&mut registry, id, &[]);

        assert!(messages.is_empty());
        assert!(awaiting.is_settled());
    }

    #[test]
    fn subscribe_and_unsubscribe_payloads_group_pairs_by_channel() {
        let pairs = [
            slot(TRADES, "AAPL"),
            slot(QUOTES, "MSFT"),
            slot(TRADES, "SPY"),
        ];

        let parse = |message: &WsMessage| match message {
            WsMessage::Text(text) => serde_json::from_str::<Value>(text.as_str()).unwrap(),
            other => panic!("expected a text frame, got {other:?}"),
        };

        let (subscribe, _) = Alpaca::subscribe(&mut Registry::default(), 0, &pairs);
        assert_eq!(
            parse(&only(subscribe)),
            json!({"action": "subscribe", "trades": ["AAPL", "SPY"], "quotes": ["MSFT"]}),
        );

        let (unsubscribe, awaiting) = Alpaca::release(&pairs[..1]);
        assert_eq!(
            parse(&only(unsubscribe)),
            json!({"action": "unsubscribe", "trades": ["AAPL"]}),
        );
        assert!(awaiting.is_some(), "an unsubscribe is awaited");
    }
}
