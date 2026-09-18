//! London Strategic Edge WebSocket handshake, driven against a synthetic in-process server.
//!
//! # Why this exists
//!
//! The unit tests beside the subscriber call its guards directly, which proves what each guard
//! *decides* but not what the client *does*. The contract this surface actually rests on is about
//! the wire:
//!
//! > every guard runs before the first subscribe leaves the client
//!
//! That is not a stylistic preference. Subscribing to a symbol the provider does not offer is
//! **confirmed rather than rejected** — it answers `subscribed`, never errors, never ticks, and
//! permanently consumes one of the connection's few subscription slots. A guard that rejected the
//! batch *after* sending would be indistinguishable from one that rejected it before, in every
//! assertion a unit test can make, while quietly spending slots that cannot be reclaimed without
//! reconnecting. Only a server that counts what arrived can tell the two apart, so these tests
//! assert **zero subscribe payloads reached the socket** on every rejection path.
//!
//! The same applies to what the handshake *reads*: a client that treated the first frame as the
//! answer to its `auth` would appear to work, because the server opens with an unsolicited
//! `welcome`. Here the server sends one, and the rejection that follows it is what proves the
//! client waited.
//!
//! # No provider data is involved
//!
//! Every frame below is hand-written to the shapes documented on the types that decode them. The
//! provider prohibits redistributing its data (<https://londonstrategicedge.com/terms>), so no
//! recorded response may be committed to this repository — which is precisely why a synthetic
//! server is worth building rather than replaying a capture. Live shape verification is the
//! separate, credential-gated job of `lse_ws_canary.rs`.
//!
//! # Why the server is real rather than mocked
//!
//! The subscriber's flow is inseparable from the socket: it connects, sends, waits for a specific
//! frame, sends again, then hands the still-open connection to the subscription validator. A mock
//! of that would be a re-implementation of it. `tokio-tungstenite` speaks the same protocol the
//! client does, so the only thing swapped out is the endpoint.
//!
//! # Running
//!
//! ```bash
//! cargo test --test lse_ws_handshake --features lse
//! ```
//!
//! No network, no credentials, no provider allowance spent — these run on every ordinary test run.

#![cfg(feature = "lse")]
#![allow(clippy::unwrap_used, clippy::expect_used)] // Test code: panics on bad input are acceptable

use chrono::{DateTime, Utc};
use futures_util::{SinkExt, StreamExt};
use rustrade_data::{
    Identifier, MarketStream, NoInitialSnapshots,
    error::DataError,
    event::MarketEvent,
    exchange::{
        ExchangeServer,
        lse::{
            Lse,
            channel::LseChannel,
            live::{LseCredentials, LseSubscriber},
            market::{LseMarket, LseServer, LseSymbolShape},
            resume::LseResumeState,
            stream::LseStream,
        },
        subscription::ExchangeSub,
    },
    subscriber::Subscriber,
    subscription::{
        Subscription,
        trade::{PublicTrade, PublicTrades},
    },
};
use rustrade_instrument::{
    exchange::ExchangeId,
    instrument::market_data::{MarketDataInstrument, kind::MarketDataInstrumentKind},
};
use rustrade_integration::subscription::SubscriptionId;
use serde_json::{Value, json};
use serial_test::serial;
use std::sync::{Arc, Mutex};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::tungstenite::Message;

/// The endpoint the connector under test resolves to, set by whichever harness is running.
///
/// [`ExchangeServer::websocket_url`] answers with a `&'static str` and takes no arguments, so the
/// port a freshly-bound listener was assigned has nowhere else to live. Every test therefore holds
/// this for its duration and runs `#[serial]` — which is also what lets one connector type serve
/// every scenario, so the tests exercise the real `Connector` implementation with nothing swapped
/// out but the endpoint.
static HARNESS_URL: Mutex<Option<&'static str>> = Mutex::new(None);

/// A connector identical to the shipped ones except for where it points.
///
/// Declaring a server rather than a whole connector is deliberate: `Lse<Server>` carries the real
/// [`Connector`](rustrade_data::exchange::Connector) implementation, the real `Identifier` impls
/// and the real symbol spelling, so what these tests drive is production code. A bespoke connector
/// would only be a look-alike, and could drift from the thing it stands in for.
#[derive(Copy, Clone, Debug, Default)]
struct HarnessServer;

impl ExchangeServer for HarnessServer {
    // Crypto because its symbols are pair-shaped and its published category is a single known
    // value, so the category cross-check has something definite to agree with.
    const ID: ExchangeId = ExchangeId::LseCrypto;

    fn websocket_url() -> &'static str {
        HARNESS_URL
            .lock()
            .unwrap()
            .expect("a harness must be started before the connector resolves its endpoint")
    }
}

impl LseServer for HarnessServer {
    const SYMBOL_SHAPE: LseSymbolShape = LseSymbolShape::Pair;
}

type HarnessLse = Lse<HarnessServer>;

/// How the synthetic server answers.
struct Script {
    /// The frame sent in reply to `auth`.
    auth_reply: Value,
    /// Close the connection instead of ever answering `auth`.
    close_without_answering: bool,
    /// Frames to send ahead of each subscription confirmation.
    ///
    /// These are what the subscription validator cannot read as a response and hands back as
    /// buffered events — the path a replayed tick takes when it arrives while other symbols are
    /// still being confirmed.
    frames_before_confirmation: Vec<Value>,

    /// Frames to send after each subscription confirmation.
    ///
    /// These reach the stream itself rather than the handshake, which is what lets a test drive a
    /// market event out of the far end instead of stopping at `subscribe`.
    frames_after_confirmation: Vec<Value>,
}

impl Default for Script {
    fn default() -> Self {
        Self {
            auth_reply: authenticated(&[("BTC/USD", Some("Crypto")), ("ETH/USD", None)], 16),
            close_without_answering: false,
            frames_before_confirmation: Vec::new(),
            frames_after_confirmation: Vec::new(),
        }
    }
}

/// A successful `auth` answer offering `symbols`.
fn authenticated(symbols: &[(&str, Option<&str>)], max_subscriptions: u32) -> Value {
    let symbols = symbols
        .iter()
        .map(|(symbol, category)| match category {
            Some(category) => json!({"symbol": symbol, "category": category}),
            // Roughly half the real entries carry no category key at all, so the harness offers
            // both shapes rather than the tidy one only.
            None => json!({"symbol": symbol}),
        })
        .collect::<Vec<_>>();

    json!({
        "type": "authenticated",
        "tier": "registered",
        "max_subscriptions": max_subscriptions,
        "symbols": symbols,
    })
}

/// A running synthetic server, and the subscribe payloads it has been sent.
struct Harness {
    subscribes: Arc<Mutex<Vec<Value>>>,
    served: tokio::task::JoinHandle<()>,
}

impl Harness {
    /// Bind an ephemeral port, publish it to the connector, and serve one connection.
    async fn start(script: Script) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();

        // Leaked so the endpoint can satisfy `websocket_url`'s `&'static str`. One short string per
        // test, in a test binary that exits immediately afterwards, is a bounded cost; the
        // alternative is threading a lifetime through a public trait to serve a test.
        let url: &'static str = Box::leak(format!("ws://{address}").into_boxed_str());
        *HARNESS_URL.lock().unwrap() = Some(url);

        let subscribes = Arc::new(Mutex::new(Vec::new()));
        let recorded = Arc::clone(&subscribes);

        let served = tokio::spawn(async move {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            serve(stream, script, recorded).await;
        });

        Self { subscribes, served }
    }

    /// A sample of the subscribe payloads recorded so far, in the order they arrived.
    ///
    /// # ⚠️ Sound for content assertions only, never for counts
    /// The server task is still running and still writing to this vector. What is safe to conclude
    /// from a sample is that a payload *present* in it was really sent, and what it contained —
    /// so this is the right tool for asserting the shape of a payload already known to exist,
    /// which is what the confirmation the client just returned on establishes.
    ///
    /// It cannot say how many were sent, or that none was. A payload written to the socket and not
    /// yet read by the server is indistinguishable here from one never sent, so an assertion on
    /// `len()` would report a guard as running before the first send when it ran after it, and a
    /// duplicate subscribe as absent when it was merely late. Those need [`Self::drained`], which
    /// waits for the server to finish reading rather than sampling it mid-write.
    fn subscribes(&self) -> Vec<Value> {
        self.subscribes.lock().unwrap().clone()
    }

    /// Everything that reached the socket, read after the connection has closed.
    ///
    /// The server's read loop ends when the client's end of the connection goes away, so awaiting
    /// that loop is what turns "nothing more was sent" from a guess about timing into an
    /// observation. This is the only sound basis for a count.
    ///
    /// The connection must already be closing when this is called:
    /// - on a **rejection** path, `subscribe` drops it as it returns, so nothing more is needed;
    /// - on a **success** path the client keeps it, so drop the [`Subscribed`] first — and read
    ///   anything wanted from it beforehand, since dropping it takes the buffered events with it.
    ///
    /// [`Subscribed`]: rustrade_data::subscriber::Subscribed
    async fn drained(self) -> Vec<Value> {
        let Self { subscribes, served } = self;

        tokio::time::timeout(std::time::Duration::from_secs(5), served)
            .await
            .expect("the connection should have closed once the client released it")
            .expect("the harness server panicked");

        subscribes.lock().unwrap().clone()
    }
}

/// Speak the provider's side of the protocol for one connection.
async fn serve(stream: TcpStream, script: Script, subscribes: Arc<Mutex<Vec<Value>>>) {
    let Ok(mut websocket) = tokio_tungstenite::accept_async(stream).await else {
        return;
    };

    // The real server greets before it is asked anything. Sending it here is what makes the
    // "waited for the right frame" assertions meaningful rather than vacuous.
    let welcome = json!({"type": "welcome", "message": "connected", "symbols_available": 8516});
    if websocket
        .send(Message::text(welcome.to_string()))
        .await
        .is_err()
    {
        return;
    }

    while let Some(Ok(message)) = websocket.next().await {
        let Message::Text(text) = message else {
            continue;
        };
        let Ok(payload) = serde_json::from_str::<Value>(text.as_str()) else {
            continue;
        };

        match payload["action"].as_str() {
            Some("auth") => {
                if script.close_without_answering {
                    let _ = websocket.close(None).await;
                    return;
                }
                if websocket
                    .send(Message::text(script.auth_reply.to_string()))
                    .await
                    .is_err()
                {
                    return;
                }
            }
            Some("subscribe") => {
                let symbol = payload["symbol"].as_str().unwrap_or_default().to_owned();
                let count = {
                    let mut recorded = subscribes.lock().unwrap();
                    recorded.push(payload.clone());
                    recorded.len()
                };

                for frame in &script.frames_before_confirmation {
                    if websocket
                        .send(Message::text(frame.to_string()))
                        .await
                        .is_err()
                    {
                        return;
                    }
                }

                let confirmation = json!({
                    "type": "subscribed", "symbol": symbol, "count": count, "max": 16,
                });
                if websocket
                    .send(Message::text(confirmation.to_string()))
                    .await
                    .is_err()
                {
                    return;
                }

                for frame in &script.frames_after_confirmation {
                    if websocket
                        .send(Message::text(frame.to_string()))
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
            }
            // The client sends nothing else during a handshake; anything here would be a change in
            // the flow under test, and ignoring it lets the assertion below report that as silence
            // rather than as a protocol error from the harness.
            _ => {}
        }
    }
}

fn subscription(base: &str) -> Subscription<HarnessLse, MarketDataInstrument, PublicTrades> {
    Subscription::from((
        HarnessLse::default(),
        base,
        "usd",
        MarketDataInstrumentKind::Spot,
        PublicTrades,
    ))
}

/// The identifier a subscription's watermark is filed under.
///
/// Derived the way the connector derives it, rather than spelled out, so this pins the resume
/// behaviour and not the identifier's format.
fn subscription_id(base: &str) -> SubscriptionId {
    ExchangeSub::<LseChannel, LseMarket>::new(&subscription(base)).id()
}

fn subscriber() -> LseSubscriber {
    LseSubscriber::new(LseCredentials::new("harness-key"))
}

/// The symbols named by the subscribe payloads that reached the socket.
fn symbols(subscribes: &[Value]) -> Vec<&str> {
    subscribes
        .iter()
        .map(|payload| payload["symbol"].as_str().unwrap())
        .collect()
}

#[tokio::test]
#[serial]
async fn a_full_handshake_subscribes_each_distinct_symbol_exactly_once() {
    let harness = Harness::start(Script::default()).await;

    // The batch names one symbol twice. Two subscriptions over one symbol are one slot and one
    // confirmation, so a second payload would leave the validator waiting on a confirmation the
    // provider will never send -- a hang, not an error.
    let subscriptions = [
        subscription("btc"),
        subscription("eth"),
        subscription("btc"),
    ];
    let subscribed = subscriber().subscribe(&subscriptions).await.unwrap();

    // Read the client's side before releasing the connection: dropping `Subscribed` takes the
    // buffered events with it.
    let instruments = subscribed.map.0.len();
    let maps_btc = subscribed.map.0.contains_key(&subscription_id("btc"));
    let buffered = subscribed.buffered_websocket_events.len();

    // "Exactly once" is a count, so it needs the server to have finished reading rather than a
    // sample of it mid-write -- a duplicate subscribe still in flight would otherwise read as
    // absent, which is the very thing this test is here to rule out. Dropping the connection ends
    // the server's read loop, and `drained` waits for it.
    drop(subscribed);
    let sent = harness.drained().await;

    assert_eq!(symbols(&sent), vec!["BTC/USD", "ETH/USD"]);
    assert_eq!(instruments, 2);
    assert!(maps_btc);
    assert_eq!(buffered, 0);
}

/// A live subscribe carries the symbol and nothing else — no replay window is opened for a
/// subscriber that was never asked to resume.
#[tokio::test]
#[serial]
async fn a_subscriber_without_resume_opens_no_replay_window_on_the_wire() {
    let harness = Harness::start(Script::default()).await;

    let subscribed = subscriber()
        .subscribe(&[subscription("btc")])
        .await
        .unwrap();

    // The claim is that *no* payload carried a window, which is a statement about every payload
    // sent and so needs the drained view rather than a sample of it.
    drop(subscribed);
    let sent = harness.drained().await;

    assert_eq!(sent.len(), 1);
    assert_eq!(
        sent[0],
        json!({"action": "subscribe", "symbol": "BTC/USD"}),
        "a subscriber with no resume state must send the plain payload",
    );
}

/// The failure this whole guard exists for. The provider confirms a symbol it does not offer, never
/// ticks it, and holds the slot until the connection is torn down — so rejecting after sending
/// would cost exactly what rejecting is meant to save.
#[tokio::test]
#[serial]
async fn a_symbol_the_provider_does_not_offer_costs_no_subscription_slot() {
    let harness = Harness::start(Script::default()).await;

    // BTC/USD is offered; the batch is rejected for the company it keeps, and neither is sent.
    let subscriptions = [subscription("btc"), subscription("nope")];
    let error = subscriber()
        .subscribe(&subscriptions)
        .await
        .unwrap_err()
        .to_string();

    assert!(error.contains("NOPE/USD"), "{error}");

    let sent = harness.drained().await;
    assert!(
        sent.is_empty(),
        "the batch was rejected only after spending slots: {sent:?}",
    );
}

/// An over-cap batch is rejected *anonymously* by the provider — its error does not name the symbol
/// it refused — so there is no partial subscription to recover. Failing before sending is the only
/// outcome that leaves the caller with a connection in a state they can reason about.
#[tokio::test]
#[serial]
async fn an_over_cap_batch_costs_no_subscription_slot() {
    let script = Script {
        auth_reply: authenticated(&[("BTC/USD", Some("Crypto")), ("ETH/USD", None)], 1),
        ..Script::default()
    };
    let harness = Harness::start(script).await;

    let subscriptions = [subscription("btc"), subscription("eth")];
    let error = subscriber()
        .subscribe(&subscriptions)
        .await
        .unwrap_err()
        .to_string();

    assert!(error.contains("at most 1"), "{error}");

    let sent = harness.drained().await;
    assert!(
        sent.is_empty(),
        "an over-cap batch reached the socket: {sent:?}"
    );
}

/// The server greets with an unsolicited `welcome` before the key is ever sent. A client that took
/// the first frame for the answer would authenticate against that greeting and sail past a
/// rejected key — so the rejection here arrives *after* a welcome, and the client must still report
/// it rather than proceed.
#[tokio::test]
#[serial]
async fn a_rejected_key_is_reported_rather_than_read_past() {
    let script = Script {
        auth_reply: json!({
            "type": "error", "code": "INVALID_KEY", "message": "invalid api key",
        }),
        ..Script::default()
    };
    let harness = Harness::start(script).await;

    let error = subscriber()
        .subscribe(&[subscription("btc")])
        .await
        .unwrap_err()
        .to_string();

    assert!(error.contains("INVALID_KEY"), "{error}");

    let sent = harness.drained().await;
    assert!(
        sent.is_empty(),
        "subscribed on an unauthenticated connection: {sent:?}",
    );
}

/// A connection dropped mid-handshake must be reported, not waited out: the client is otherwise
/// blocked until its authentication timeout for a connection it already knows is gone.
#[tokio::test]
#[serial]
async fn a_connection_closed_during_authentication_is_reported() {
    let script = Script {
        close_without_answering: true,
        ..Script::default()
    };
    let harness = Harness::start(script).await;

    let error = subscriber()
        .subscribe(&[subscription("btc")])
        .await
        .unwrap_err()
        .to_string();

    assert!(
        error.contains("closed"),
        "a closed connection should say so: {error}",
    );
    assert!(harness.drained().await.is_empty());
}

/// Ticks and replay boundaries arrive while *other* symbols in the batch are still being confirmed,
/// and neither deserialises as a subscription response. They must survive as buffered events: the
/// clamp check reads the replay boundary out of that buffer, and the stream replays the ticks from
/// it. A frame dropped here is market data lost before the stream ever starts.
#[tokio::test]
#[serial]
async fn frames_arriving_during_validation_are_buffered_rather_than_dropped() {
    let script = Script {
        frames_before_confirmation: vec![
            json!({"type": "replay_started", "symbol": "BTC/USD",
                   "from": "2026-08-14T10:16:55.161234+00:00"}),
            json!({"type": "tick", "symbol": "BTC/USD",
                   "ts": "2026-08-14T10:16:55.161234+00:00",
                   "price": 42000.5, "bid": 42000.5, "ask": 42001.0, "volume": 0.00155}),
        ],
        ..Script::default()
    };
    let harness = Harness::start(script).await;

    let subscribed = subscriber()
        .subscribe(&[subscription("btc")])
        .await
        .unwrap();

    // Collected before the connection is released, because dropping `Subscribed` takes the
    // buffered events with it.
    let buffered = subscribed
        .buffered_websocket_events
        .iter()
        .map(|message| match message {
            rustrade_integration::protocol::websocket::WsMessage::Text(text) => {
                serde_json::from_str::<Value>(text.as_str()).unwrap()
            }
            other => panic!("unexpected buffered frame: {other:?}"),
        })
        .collect::<Vec<_>>();

    drop(subscribed);
    assert_eq!(symbols(&harness.drained().await), vec!["BTC/USD"]);

    assert_eq!(
        buffered.len(),
        2,
        "both frames should have been buffered: {buffered:?}",
    );
    assert_eq!(buffered[0]["type"], "replay_started");
    assert_eq!(buffered[1]["type"], "tick");
}

/// A tick frame as the provider spells it, live or replayed.
fn tick_frame(symbol: &str, ts: &str, price: f64, replay: bool) -> Value {
    let mut frame = json!({
        "type": "tick", "symbol": symbol, "ts": ts,
        "price": price, "bid": price, "ask": price + 0.5, "volume": 0.00155,
    });

    // Live ticks carry no `replay` key at all; only replayed ones are stamped.
    if replay {
        frame["replay"] = json!(true);
    }

    frame
}

/// Assemble the market stream the connector actually serves, resume state and all.
async fn stream(
    subscriber: &LseSubscriber,
    subscriptions: &[Subscription<HarnessLse, MarketDataInstrument, PublicTrades>],
) -> LseStream<HarnessLse, MarketDataInstrument, PublicTrades> {
    <LseStream<_, _, _> as MarketStream<HarnessLse, MarketDataInstrument, PublicTrades>>::init::<
        NoInitialSnapshots,
    >(subscriber, subscriptions)
    .await
    .unwrap()
}

/// The next market event, or a failure naming which of the three ways it did not arrive.
async fn next_event<S>(stream: &mut S) -> MarketEvent<MarketDataInstrument, PublicTrade>
where
    S: futures_util::Stream<
            Item = Result<MarketEvent<MarketDataInstrument, PublicTrade>, DataError>,
        > + Unpin,
{
    tokio::time::timeout(std::time::Duration::from_secs(5), stream.next())
        .await
        .expect("the stream produced no event before the timeout")
        .expect("the stream ended instead of producing an event")
        .expect("the stream yielded an error instead of an event")
}

/// The invariant [`LseStream`] exists for, end to end.
///
/// A replayed tick can already be sitting in the handshake's buffered events: it fails to
/// deserialise as a subscription response while other symbols are still confirming, and lands
/// there. The resume state must therefore reach the transformer **before** those buffered events
/// are processed, which is the only reason this connector does not use the blanket WebSocket
/// stream.
///
/// The pieces are covered in isolation elsewhere — the transformer's skip logic in its own unit
/// tests, the buffering in `frames_arriving_during_validation_are_buffered_rather_than_dropped`.
/// This is what joins them: reorder the two statements in `LseStream::init` and every other test in
/// this repository still passes, while every reconnect of a resumed subscription starts delivering
/// duplicates.
#[tokio::test]
#[serial]
async fn a_replayed_tick_buffered_during_validation_is_skipped_on_a_resumed_reconnect() {
    const INSTANT: &str = "2026-08-14T10:16:55.161234+00:00";
    const LATER: &str = "2026-08-14T10:16:55.161235+00:00";

    let state = Arc::new(LseResumeState::new());
    let subscriber = subscriber().with_resume(Arc::clone(&state));
    let subscriptions = [subscription("btc")];

    // The first connection delivers one tick, which becomes the watermark.
    let first = Harness::start(Script {
        frames_after_confirmation: vec![tick_frame("BTC/USD", INSTANT, 42000.5, false)],
        ..Script::default()
    })
    .await;

    let mut opened = stream(&subscriber, &subscriptions).await;
    assert_eq!(
        next_event(&mut opened).await.time_exchange,
        INSTANT.parse::<DateTime<Utc>>().unwrap(),
    );
    assert_eq!(
        first.subscribes()[0].get("start"),
        None,
        "the first subscribe has nothing to resume from and must open no window",
    );
    drop(opened);

    // The reconnect: the provider replays that same tick, and it arrives *before* the confirmation
    // -- so the stream meets it in the buffered events, not on the socket.
    let second = Harness::start(Script {
        frames_before_confirmation: vec![tick_frame("BTC/USD", INSTANT, 42000.5, true)],
        frames_after_confirmation: vec![tick_frame("BTC/USD", LATER, 42001.0, false)],
        ..Script::default()
    })
    .await;

    let mut resumed = stream(&subscriber, &subscriptions).await;

    assert!(
        second.subscribes()[0]["start"].is_number(),
        "the reconnect must carry the resume window: {:?}",
        second.subscribes()[0],
    );

    assert_eq!(
        next_event(&mut resumed).await.time_exchange,
        LATER.parse::<DateTime<Utc>>().unwrap(),
        "the first event out of a resumed stream was the replayed duplicate, so the resume state \
         did not reach the transformer before the buffered events were processed",
    );
}
