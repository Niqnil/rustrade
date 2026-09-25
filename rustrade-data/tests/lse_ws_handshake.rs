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
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use rustrade_data::{
    MarketStream, NoInitialSnapshots,
    error::DataError,
    event::MarketEvent,
    exchange::{
        ExchangeServer,
        lse::{
            Lse,
            live::{LseCredentials, LseSubscriber},
            mapper::LseSubMapper,
            market::{LseQuoteServer, LseServer, LseSymbolShape},
            resume::LseResumeState,
            stream::LseStream,
        },
    },
    subscriber::{Subscriber, mapper::SubscriptionMapper},
    subscription::{Subscription, SubscriptionMeta, book::OrderBooksL1, trade::PublicTrades},
};
use rustrade_instrument::{
    exchange::ExchangeId,
    instrument::{
        kind::option::{OptionExercise, OptionKind},
        market_data::{
            MarketDataInstrument,
            kind::{MarketDataInstrumentKind, MarketDataOptionContract},
        },
    },
};
use rustrade_integration::subscription::SubscriptionId;
use serde_json::{Value, json};
use serial_test::serial;
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::mpsc,
};
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

impl LseQuoteServer for HarnessServer {}

type HarnessLse = Lse<HarnessServer>;

/// The options dataset, pointed at the harness.
///
/// Declared as the shipped options server is — the options identifier and the OSI spelling — so the
/// per-underlying subscribe, the per-underlying confirmation count and the unregistered-contract
/// count all run exactly as they do against the provider.
#[derive(Copy, Clone, Debug, Default)]
struct OptionsHarnessServer;

impl ExchangeServer for OptionsHarnessServer {
    const ID: ExchangeId = ExchangeId::LseOptions;

    fn websocket_url() -> &'static str {
        HarnessServer::websocket_url()
    }
}

impl LseServer for OptionsHarnessServer {
    const SYMBOL_SHAPE: LseSymbolShape = LseSymbolShape::OptionContract;
}

type OptionsHarnessLse = Lse<OptionsHarnessServer>;

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

    /// Option underlyings answered with the provider's `INVALID_UNDERLYING` rejection rather than
    /// a confirmation.
    underlyings_without_options: Vec<&'static str>,
}

impl Default for Script {
    fn default() -> Self {
        Self {
            auth_reply: authenticated(&[("BTC/USD", Some("Crypto")), ("ETH/USD", None)], 16),
            close_without_answering: false,
            frames_before_confirmation: Vec::new(),
            frames_after_confirmation: Vec::new(),
            underlyings_without_options: Vec::new(),
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

        // What answers a subscribe: a confirmation in the shape the action calls for, or the
        // rejection an underlying with no options receives.
        let answer = match payload["action"].as_str() {
            Some("subscribe") => Some(json!({
                "type": "subscribed", "symbol": payload["symbol"], "max": 16,
            })),
            Some("subscribe_options") => {
                let underlying = payload["underlying"].as_str().unwrap_or_default();

                Some(
                    if script.underlyings_without_options.contains(&underlying) {
                        json!({
                            "type": "error", "code": "INVALID_UNDERLYING",
                            "message": format!("No options available for {underlying}"),
                        })
                    } else {
                        json!({
                            "type": "options_subscribed", "underlying": underlying,
                            "contracts": 1000, "max": 100,
                        })
                    },
                )
            }
            _ => None,
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
            Some("subscribe" | "subscribe_options") => {
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

                let mut confirmation = answer.unwrap_or_default();
                if confirmation["type"] != "error" {
                    confirmation["count"] = json!(count);
                }
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
    let SubscriptionMeta { instrument_map, .. } = LseSubMapper::map(&[subscription(base)]);
    instrument_map.0.into_keys().next().unwrap()
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

/// Ticks arrive while *other* symbols in the batch are still being confirmed, and must reach the
/// stream: a frame dropped here is market data lost before the stream ever starts. A replay's
/// boundary frame arriving alongside must not, when nothing asked this stream for a replay — it
/// belongs to whichever stream on the connection did.
#[tokio::test]
#[serial]
async fn frames_arriving_during_the_handshake_reach_the_stream_rather_than_being_dropped() {
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

    let mut subscribed = subscriber()
        .subscribe(&[subscription("btc")])
        .await
        .unwrap();

    // The connection routes a batch's frames into its transport from the moment the batch is
    // registered, so nothing is left to buffer.
    assert!(subscribed.buffered_websocket_events.is_empty());

    let mut received = Vec::new();
    while let Ok(Some(frame)) =
        tokio::time::timeout(Duration::from_millis(200), subscribed.transport.next()).await
    {
        let rustrade_integration::protocol::websocket::WsMessage::Text(text) = frame.unwrap()
        else {
            panic!("expected a text frame");
        };
        received.push(serde_json::from_str::<Value>(text.as_str()).unwrap());
    }

    drop(subscribed);
    assert_eq!(symbols(&harness.drained().await), vec!["BTC/USD"]);

    assert_eq!(received.len(), 1, "{received:?}");
    assert_eq!(received[0]["type"], "tick");
}

/// The live half of the same path: a tick sent before the confirmation reaches the stream.
#[tokio::test]
#[serial]
async fn a_live_tick_sent_before_the_confirmation_reaches_the_stream() {
    const INSTANT: &str = "2026-08-14T10:16:55.161234+00:00";

    let harness = Harness::start(Script {
        frames_before_confirmation: vec![tick_frame("BTC/USD", INSTANT, 42000.5, false)],
        ..Script::default()
    })
    .await;

    let mut opened = stream(&subscriber(), &[subscription("btc")]).await;
    assert_eq!(
        next_event(&mut opened).await.time_exchange,
        INSTANT.parse::<DateTime<Utc>>().unwrap(),
    );

    drop(opened);
    drop(harness.drained().await);
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
async fn next_event<S, Event>(stream: &mut S) -> MarketEvent<MarketDataInstrument, Event>
where
    S: futures_util::Stream<Item = Result<MarketEvent<MarketDataInstrument, Event>, DataError>>
        + Unpin,
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

// ---------------------------------------------------------------------------------------------
// Option contracts: subscribed per underlying, confirmed per underlying, delivered per contract.
// ---------------------------------------------------------------------------------------------

/// A subscription to one option contract on `root`, expiring 30 Sep 2026.
fn contract(
    root: &str,
    kind: OptionKind,
    strike: Decimal,
) -> Subscription<OptionsHarnessLse, MarketDataInstrument, PublicTrades> {
    Subscription::from((
        OptionsHarnessLse::default(),
        root,
        "usd",
        MarketDataInstrumentKind::Option(MarketDataOptionContract {
            kind,
            exercise: OptionExercise::American,
            expiry: "2026-09-30T20:00:00Z".parse().unwrap(),
            strike,
        }),
        PublicTrades,
    ))
}

/// An option contract tick as the options channel spells it: no quote, a whole-second instant and
/// a contract label.
fn option_tick_frame(symbol: &str) -> Value {
    json!({
        "type": "tick", "symbol": symbol, "ts": "2026-09-30T15:00:00+00:00",
        "price": 1.25, "bid": null, "ask": null, "volume": 3, "name": "a contract label",
    })
}

/// Three contracts over two underlyings are two subscribes and two confirmations. The validator
/// finishing at all is the proof it expected two: expecting three — one per contract — it would wait
/// out its timeout for a confirmation the provider never sends, and fail.
#[tokio::test]
#[serial]
async fn an_options_batch_subscribes_and_is_confirmed_once_per_underlying() {
    let harness = Harness::start(Script::default()).await;

    let subscriptions = [
        contract("spy", OptionKind::Call, dec!(700)),
        contract("qqq", OptionKind::Put, dec!(500)),
        contract("spy", OptionKind::Put, dec!(650)),
    ];
    let subscribed = subscriber().subscribe(&subscriptions).await.unwrap();
    let instruments = subscribed.map.0.len();

    drop(subscribed);
    let sent = harness.drained().await;

    assert_eq!(
        sent,
        [
            json!({"action": "subscribe_options", "underlying": "SPY"}),
            json!({"action": "subscribe_options", "underlying": "QQQ"}),
        ]
    );
    assert_eq!(instruments, 3, "every contract is its own instrument");
}

/// Options do not resume, so a subscriber configured to must still subscribe the options channel
/// plainly and deliver from it — the resume state is withheld from the options stream rather than
/// failing it, and no replay window reaches the wire.
#[tokio::test]
#[serial]
async fn a_resuming_subscriber_streams_options_without_a_replay_window() {
    let registered = contract("spy", OptionKind::Call, dec!(700));
    let script = Script {
        frames_after_confirmation: vec![option_tick_frame("SPY260930C00700000")],
        ..Script::default()
    };
    let harness = Harness::start(script).await;

    let subscriber = subscriber().with_resume(Arc::new(LseResumeState::new()));
    let mut stream =
        <LseStream<_, _, _> as MarketStream<
            OptionsHarnessLse,
            MarketDataInstrument,
            PublicTrades,
        >>::init::<NoInitialSnapshots>(&subscriber, std::slice::from_ref(&registered))
        .await
        .unwrap();

    assert_eq!(
        next_event(&mut stream).await.instrument,
        registered.instrument
    );

    drop(stream);
    assert_eq!(
        harness.drained().await,
        [json!({"action": "subscribe_options", "underlying": "SPY"})]
    );
}

/// An instrument with no OSI spelling is refused before a connection is even opened.
#[tokio::test]
#[serial]
async fn a_contract_with_no_osi_symbol_is_refused_before_connecting() {
    let harness = Harness::start(Script::default()).await;

    let not_an_option = Subscription::from((
        OptionsHarnessLse::default(),
        "spy",
        "usd",
        MarketDataInstrumentKind::Spot,
        PublicTrades,
    ));
    let error = subscriber()
        .subscribe(&[contract("spy", OptionKind::Call, dec!(700)), not_an_option])
        .await
        .unwrap_err()
        .to_string();

    assert!(error.contains("no OSI symbol"), "{error}");

    // No connection was ever opened, so the server is still waiting to accept one and nothing can be
    // in flight: the sample is complete, and the task is stopped rather than awaited.
    assert!(harness.subscribes().is_empty());
    harness.served.abort();
}

/// Unlike an unknown symbol, which the provider confirms, an underlying with no options is rejected
/// by name — so the provider's own rejection is the guard, and it must fail the batch.
#[tokio::test]
#[serial]
async fn an_underlying_with_no_options_fails_the_batch_naming_it() {
    let script = Script {
        underlyings_without_options: vec!["NOPE"],
        ..Script::default()
    };
    let harness = Harness::start(script).await;

    let error = subscriber()
        .subscribe(&[
            contract("spy", OptionKind::Call, dec!(700)),
            contract("nope", OptionKind::Call, dec!(10)),
        ])
        .await
        .unwrap_err()
        .to_string();

    assert!(error.contains("INVALID_UNDERLYING"), "{error}");
    assert!(error.contains("NOPE"), "{error}");
    drop(harness.drained().await);
}

/// The whole chain arrives, and only the registered contract may come out of the stream. The
/// unregistered print ahead of it must be dropped as counted rather than surface as an error —
/// `next_event` fails on an error, so the first event being the registered print proves both.
#[tokio::test]
#[serial]
async fn only_registered_contracts_reach_the_options_stream() {
    let registered = contract("spy", OptionKind::Call, dec!(700));
    let script = Script {
        frames_after_confirmation: vec![
            option_tick_frame("SPY260930C00701000"),
            option_tick_frame("SPY260930C00700000"),
        ],
        ..Script::default()
    };
    let _harness = Harness::start(script).await;

    let mut stream =
        <LseStream<_, _, _> as MarketStream<
            OptionsHarnessLse,
            MarketDataInstrument,
            PublicTrades,
        >>::init::<NoInitialSnapshots>(&subscriber(), std::slice::from_ref(&registered))
        .await
        .unwrap();

    let event = next_event(&mut stream).await;

    assert_eq!(event.instrument, registered.instrument);
    assert_eq!(event.exchange, ExchangeId::LseOptions);
    assert_eq!(event.kind.price, dec!(1.25));
    assert_eq!(event.kind.amount, dec!(3));
}

/// A root of four or more characters used to take the identifier past the inline limit, and it is
/// the same identifier the instrument map files the contract under. The unregistered print on the
/// same root ahead of it must still be dropped, and the registered one must still resolve.
#[tokio::test]
#[serial]
async fn a_contract_on_a_long_root_reaches_the_options_stream() {
    let registered = contract("googl", OptionKind::Call, dec!(700));
    let script = Script {
        frames_after_confirmation: vec![
            option_tick_frame("GOOGL260930C00701000"),
            option_tick_frame("GOOGL260930C00700000"),
        ],
        ..Script::default()
    };
    let _harness = Harness::start(script).await;

    let mut stream =
        <LseStream<_, _, _> as MarketStream<
            OptionsHarnessLse,
            MarketDataInstrument,
            PublicTrades,
        >>::init::<NoInitialSnapshots>(&subscriber(), std::slice::from_ref(&registered))
        .await
        .unwrap();

    let event = next_event(&mut stream).await;

    assert_eq!(event.instrument, registered.instrument);
    assert_eq!(event.exchange, ExchangeId::LseOptions);
    assert_eq!(event.kind.price, dec!(1.25));
}

// ---------------------------------------------------------------------------------------------
// One connection per key: every stream a subscriber and its clones open shares one socket.
// ---------------------------------------------------------------------------------------------

/// What a test tells the connection it is serving.
enum Control {
    Send(Value),
    /// Drop the socket without a close frame, as a network failure would.
    Drop,
}

/// A synthetic provider that stays up across connections and is driven by the test.
///
/// The scripted [`Harness`] serves one connection and answers from a script, which suits a single
/// handshake. Sharing is about what happens *between* handshakes — a second stream joining, one
/// leaving, the socket failing under all of them — so this one serves every connection the client
/// opens, logs every payload but `auth` against the connection it arrived on, and sends frames when
/// the test says to.
struct Provider {
    log: Arc<Mutex<Vec<(usize, Value)>>>,
    connections: Arc<AtomicUsize>,
    closed: Arc<AtomicUsize>,
    control: Arc<Mutex<Option<mpsc::UnboundedSender<Control>>>>,
    accepting: tokio::task::JoinHandle<()>,
}

impl Provider {
    async fn start(
        auth_reply: Value,
        underlyings_without_options: &'static [&'static str],
    ) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url: &'static str =
            Box::leak(format!("ws://{}", listener.local_addr().unwrap()).into_boxed_str());
        *HARNESS_URL.lock().unwrap() = Some(url);

        let log = Arc::<Mutex<Vec<(usize, Value)>>>::default();
        let connections = Arc::<AtomicUsize>::default();
        let closed = Arc::<AtomicUsize>::default();
        let control = Arc::<Mutex<Option<mpsc::UnboundedSender<Control>>>>::default();

        let shared = (
            Arc::clone(&log),
            Arc::clone(&connections),
            Arc::clone(&closed),
            Arc::clone(&control),
        );

        let accepting = tokio::spawn(async move {
            let (log, connections, closed, control) = shared;
            while let Ok((stream, _)) = listener.accept().await {
                let number = connections.fetch_add(1, Ordering::SeqCst) + 1;
                let (tx, rx) = mpsc::unbounded_channel();
                *control.lock().unwrap() = Some(tx);

                tokio::spawn(serve_connection(
                    stream,
                    number,
                    auth_reply.clone(),
                    underlyings_without_options,
                    Arc::clone(&log),
                    rx,
                    Arc::clone(&closed),
                ));
            }
        });

        Self {
            log,
            connections,
            closed,
            control,
            accepting,
        }
    }

    /// Send `frame` on the newest connection.
    fn push(&self, frame: Value) {
        self.control().send(Control::Send(frame)).unwrap();
    }

    fn drop_connection(&self) {
        self.control().send(Control::Drop).unwrap();
    }

    fn control(&self) -> mpsc::UnboundedSender<Control> {
        self.control
            .lock()
            .unwrap()
            .clone()
            .expect("no connection has been opened yet")
    }

    fn connections(&self) -> usize {
        self.connections.load(Ordering::SeqCst)
    }

    /// The payloads received on connection `number`, in arrival order.
    fn sent_on(&self, number: usize) -> Vec<Value> {
        self.log
            .lock()
            .unwrap()
            .iter()
            .filter(|(on, _)| *on == number)
            .map(|(_, payload)| payload.clone())
            .collect()
    }

    /// Wait until `done` holds, failing with `what` after five seconds.
    ///
    /// The client acts on its own task, so an effect it has promised — an unsubscribe, a closed
    /// socket — is observed by waiting for it rather than by sampling once.
    async fn until(&self, what: &str, done: impl Fn(&Self) -> bool) {
        let waited = tokio::time::timeout(Duration::from_secs(5), async {
            while !done(self) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;

        assert!(
            waited.is_ok(),
            "timed out waiting for {what}: {:?}",
            self.log
        );
    }

    /// Wait for every connection opened so far to have closed. Only then is a log complete.
    async fn all_closed(&self) {
        self.until("every connection to close", |provider| {
            provider.closed.load(Ordering::SeqCst) == provider.connections()
        })
        .await;
    }
}

impl Drop for Provider {
    fn drop(&mut self) {
        self.accepting.abort();
    }
}

async fn serve_connection(
    stream: TcpStream,
    number: usize,
    auth_reply: Value,
    underlyings_without_options: &'static [&'static str],
    log: Arc<Mutex<Vec<(usize, Value)>>>,
    mut control: mpsc::UnboundedReceiver<Control>,
    closed: Arc<AtomicUsize>,
) {
    let Ok(mut websocket) = tokio_tungstenite::accept_async(stream).await else {
        closed.fetch_add(1, Ordering::SeqCst);
        return;
    };

    let welcome = json!({"type": "welcome", "message": "connected"});
    let _ = websocket.send(Message::text(welcome.to_string())).await;

    loop {
        let payload = tokio::select! {
            command = control.recv() => match command {
                Some(Control::Send(frame)) => {
                    if websocket.send(Message::text(frame.to_string())).await.is_err() {
                        break;
                    }
                    continue;
                }
                Some(Control::Drop) | None => break,
            },
            message = websocket.next() => match message {
                Some(Ok(Message::Text(text))) => match serde_json::from_str::<Value>(text.as_str()) {
                    Ok(payload) => payload,
                    Err(_) => continue,
                },
                Some(Ok(_)) => continue,
                Some(Err(_)) | None => break,
            },
        };

        let action = payload["action"].as_str().unwrap_or_default().to_owned();
        if action != "auth" {
            log.lock().unwrap().push((number, payload.clone()));
        }

        let answer = match action.as_str() {
            "auth" => auth_reply.clone(),
            "subscribe" => json!({"type": "subscribed", "symbol": payload["symbol"], "max": 16}),
            "subscribe_options" => {
                let underlying = payload["underlying"].as_str().unwrap_or_default();
                if underlyings_without_options.contains(&underlying) {
                    json!({"type": "error", "code": "INVALID_UNDERLYING",
                           "message": format!("No options available for {underlying}")})
                } else {
                    json!({"type": "options_subscribed", "underlying": underlying, "max": 100})
                }
            }
            "unsubscribe" => json!({"type": "unsubscribed", "symbol": payload["symbol"]}),
            "unsubscribe_options" => {
                json!({"type": "options_unsubscribed", "underlying": payload["underlying"]})
            }
            _ => continue,
        };

        if websocket
            .send(Message::text(answer.to_string()))
            .await
            .is_err()
        {
            break;
        }
    }

    closed.fetch_add(1, Ordering::SeqCst);
}

fn offering(symbols: &[&str], max_subscriptions: u32) -> Value {
    let symbols = symbols
        .iter()
        .map(|symbol| (*symbol, None))
        .collect::<Vec<_>>();

    authenticated(&symbols, max_subscriptions)
}

fn books(base: &str) -> Subscription<HarnessLse, MarketDataInstrument, OrderBooksL1> {
    Subscription::from((
        HarnessLse::default(),
        base,
        "usd",
        MarketDataInstrumentKind::Spot,
        OrderBooksL1,
    ))
}

async fn books_stream(
    subscriber: &LseSubscriber,
    subscriptions: &[Subscription<HarnessLse, MarketDataInstrument, OrderBooksL1>],
) -> LseStream<HarnessLse, MarketDataInstrument, OrderBooksL1> {
    <LseStream<_, _, _> as MarketStream<HarnessLse, MarketDataInstrument, OrderBooksL1>>::init::<
        NoInitialSnapshots,
    >(subscriber, subscriptions)
    .await
    .unwrap()
}

/// Wait for `stream` to end, which it must do without yielding anything first.
async fn ended<S>(stream: &mut S)
where
    S: futures_util::Stream + Unpin,
    S::Item: std::fmt::Debug,
{
    let next = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("the stream did not end when its connection was lost");

    assert!(next.is_none(), "expected the stream to end, got {next:?}");
}

fn at(spelling: &str) -> DateTime<Utc> {
    spelling.parse::<DateTime<Utc>>().unwrap()
}

/// The instant a subscribe's `start` names, to the microsecond it is sent at.
fn start_of(payload: &Value) -> Option<DateTime<Utc>> {
    let seconds = payload.get("start")?.as_f64()?;
    // The payload carries microseconds, and a float of present-day epoch seconds holds them.
    #[allow(clippy::cast_possible_truncation)]
    let micros = (seconds * 1_000_000.0).round() as i64;

    DateTime::from_timestamp_micros(micros)
}

/// The failure the whole connection design exists for: the provider allows a key one socket, so two
/// streams on one key must share one — authenticated once, and each symbol subscribed once however
/// many streams hold it.
#[tokio::test]
#[serial]
async fn clones_share_one_connection_and_one_subscription_per_symbol() {
    let provider = Provider::start(offering(&["BTC/USD", "ETH/USD"], 16), &[]).await;
    let subscriber = subscriber();

    let mut bitcoin = stream(&subscriber.clone(), &[subscription("btc")]).await;
    let mut both = stream(&subscriber, &[subscription("btc"), subscription("eth")]).await;

    assert_eq!(provider.connections(), 1);
    assert_eq!(
        symbols(&provider.sent_on(1)),
        ["BTC/USD", "ETH/USD"],
        "a symbol already on the connection must not be subscribed again",
    );

    provider.push(tick_frame("ETH/USD", "2026-08-14T10:00:00Z", 3000.0, false));
    provider.push(tick_frame(
        "BTC/USD",
        "2026-08-14T10:00:01Z",
        60000.0,
        false,
    ));

    let first = next_event(&mut bitcoin).await;
    assert_eq!(first.instrument, subscription("btc").instrument);

    assert_eq!(
        next_event(&mut both).await.instrument,
        subscription("eth").instrument
    );
    assert_eq!(
        next_event(&mut both).await.instrument,
        subscription("btc").instrument
    );
}

/// Every stream on the connection draws on one cap. A batch that would fit on a connection of its
/// own is refused when others already hold the slots — before anything is sent, and saying why.
#[tokio::test]
#[serial]
async fn the_cap_counts_what_other_streams_on_the_connection_hold() {
    let provider = Provider::start(offering(&["BTC/USD", "ETH/USD", "SOL/USD"], 2), &[]).await;
    let subscriber = subscriber();

    let held = stream(&subscriber, &[subscription("btc"), subscription("eth")]).await;

    let error = subscriber
        .clone()
        .subscribe(&[subscription("sol")])
        .await
        .unwrap_err()
        .to_string();

    assert!(error.contains("already holds"), "{error}");
    assert!(error.contains("shared by every stream"), "{error}");

    drop(held);
    provider.all_closed().await;
    assert_eq!(symbols(&provider.sent_on(1)), ["BTC/USD", "ETH/USD"]);
}

/// Detaching frees exactly what no other stream holds, and the last one out closes the socket —
/// the connection's lifetime is its streams', so a key is never left holding a socket idle.
#[tokio::test]
#[serial]
async fn a_detach_releases_only_what_no_other_stream_holds_and_the_last_closes_the_socket() {
    let provider = Provider::start(offering(&["BTC/USD", "ETH/USD"], 16), &[]).await;
    let subscriber = subscriber();

    let bitcoin = stream(&subscriber, &[subscription("btc")]).await;
    let both = stream(&subscriber, &[subscription("btc"), subscription("eth")]).await;

    drop(both);
    provider
        .until("ETH/USD to be released", |provider| {
            provider
                .sent_on(1)
                .iter()
                .any(|payload| payload["action"] == "unsubscribe")
        })
        .await;

    drop(bitcoin);
    provider.all_closed().await;

    let released = provider
        .sent_on(1)
        .into_iter()
        .filter(|payload| payload["action"] == "unsubscribe")
        .collect::<Vec<_>>();
    assert_eq!(
        released,
        [json!({"action": "unsubscribe", "symbol": "ETH/USD"})]
    );
}

/// The reconnect, end to end, over a symbol two streams hold at different watermarks.
///
/// The provider replays nothing for a repeated subscribe, so the symbol gets one window, opened at
/// the earlier watermark. The stream that had delivered further must drop what it already
/// delivered, silently and exactly; the one behind must receive what it missed. Both streams end
/// with the socket, and one reconnect serves them both — the second to re-attach receives the frames
/// held for it in the meantime.
#[tokio::test]
#[serial]
async fn one_reconnect_resumes_every_stream_from_its_own_watermark() {
    const T1: &str = "2026-08-14T10:16:55.161234+00:00";
    const T2: &str = "2026-08-14T10:16:55.161235+00:00";
    const T3: &str = "2026-08-14T10:16:55.161236+00:00";

    let provider = Provider::start(offering(&["BTC/USD"], 16), &[]).await;
    let subscriber = subscriber().with_resume(Arc::new(LseResumeState::new()));

    // Both kinds deliver T1. Then the books stream leaves, the trades stream delivers T2 alone,
    // and the books stream comes back: its watermark is T1, the trades stream's is T2.
    let mut trades = stream(&subscriber, &[subscription("btc")]).await;
    let mut quotes = books_stream(&subscriber, &[books("btc")]).await;

    provider.push(tick_frame("BTC/USD", T1, 1.0, false));
    assert_eq!(next_event(&mut trades).await.time_exchange, at(T1));
    assert_eq!(next_event(&mut quotes).await.time_exchange, at(T1));

    drop(quotes);
    provider.push(tick_frame("BTC/USD", T2, 2.0, false));
    assert_eq!(next_event(&mut trades).await.time_exchange, at(T2));

    let mut quotes = books_stream(&subscriber, &[books("btc")]).await;

    // The socket fails under both.
    provider.drop_connection();
    ended(&mut trades).await;
    ended(&mut quotes).await;
    drop((trades, quotes));

    // The trades stream re-attaches first and reconnects for both.
    let mut trades = stream(&subscriber, &[subscription("btc")]).await;
    assert_eq!(provider.connections(), 2);

    let resubscribed = provider.sent_on(2);
    assert_eq!(symbols(&resubscribed), ["BTC/USD"], "{resubscribed:?}");
    assert_eq!(
        start_of(&resubscribed[0]),
        Some(at(T1)),
        "the window must open at the earliest watermark any stream holding the symbol needs",
    );

    // The replay, then the first live tick -- before the books stream has re-attached.
    provider.push(tick_frame("BTC/USD", T1, 1.0, true));
    provider.push(tick_frame("BTC/USD", T2, 2.0, true));
    provider.push(tick_frame("BTC/USD", T3, 3.0, false));

    assert_eq!(
        next_event(&mut trades).await.time_exchange,
        at(T3),
        "the trades stream delivered T1 and T2 already, and must drop both",
    );

    let mut quotes = books_stream(&subscriber, &[books("btc")]).await;
    assert_eq!(provider.connections(), 2, "a re-attach must not reconnect");
    assert_eq!(
        next_event(&mut quotes).await.time_exchange,
        at(T2),
        "the books stream delivered T1 only; T2 is its gap and must be replayed to it",
    );
    assert_eq!(next_event(&mut quotes).await.time_exchange, at(T3));
}

/// The socket is lost again while the frames held for a stream that has not re-attached are still
/// waiting. Those frames are discarded, but the stream's registration is not: the next reconnect
/// re-subscribes it, and because it never delivered what was held, its watermark still asks for it
/// and the replay recovers it. The stream that did receive those frames drops their replay.
#[tokio::test]
#[serial]
async fn a_second_loss_before_a_stream_re_attaches_is_recovered_by_the_next_reconnect() {
    const T1: &str = "2026-08-14T10:16:55.161234+00:00";
    const T2: &str = "2026-08-14T10:16:55.161235+00:00";
    const T3: &str = "2026-08-14T10:16:55.161236+00:00";

    let provider = Provider::start(offering(&["BTC/USD"], 16), &[]).await;
    let subscriber = subscriber().with_resume(Arc::new(LseResumeState::new()));

    let mut trades = stream(&subscriber, &[subscription("btc")]).await;
    let mut quotes = books_stream(&subscriber, &[books("btc")]).await;

    provider.push(tick_frame("BTC/USD", T1, 1.0, false));
    assert_eq!(next_event(&mut trades).await.time_exchange, at(T1));
    assert_eq!(next_event(&mut quotes).await.time_exchange, at(T1));

    provider.drop_connection();
    ended(&mut trades).await;
    ended(&mut quotes).await;
    drop((trades, quotes));

    // Only the trades stream re-attaches, so T2 is held for the books stream -- routing hands a
    // frame to every holder at once, so it is held by the time the trades stream has it.
    let mut trades = stream(&subscriber, &[subscription("btc")]).await;
    provider.push(tick_frame("BTC/USD", T2, 2.0, false));
    assert_eq!(next_event(&mut trades).await.time_exchange, at(T2));

    provider.drop_connection();
    ended(&mut trades).await;
    drop(trades);

    // This time the books stream reconnects for both, from the earlier of the two watermarks.
    let mut quotes = books_stream(&subscriber, &[books("btc")]).await;
    assert_eq!(provider.connections(), 3);
    assert_eq!(start_of(&provider.sent_on(3)[0]), Some(at(T1)));

    provider.push(tick_frame("BTC/USD", T2, 2.0, true));
    provider.push(tick_frame("BTC/USD", T3, 3.0, false));

    assert_eq!(
        next_event(&mut quotes).await.time_exchange,
        at(T2),
        "the frame discarded with the lost reconnect must be recovered by the replay",
    );
    assert_eq!(next_event(&mut quotes).await.time_exchange, at(T3));

    let mut trades = stream(&subscriber, &[subscription("btc")]).await;
    assert_eq!(provider.connections(), 3, "a re-attach must not reconnect");
    assert_eq!(
        next_event(&mut trades).await.time_exchange,
        at(T3),
        "the trades stream delivered T2 already, and must drop its replay",
    );
}

/// A stream holding a resumed symbol without resuming it itself must get the live frames and none
/// of the replay — the replay is another stream's gap, and to this one it is a burst of duplicates.
#[tokio::test]
#[serial]
async fn a_replay_reaches_only_the_streams_that_asked_for_one() {
    const T1: &str = "2026-08-14T10:16:55.161234+00:00";
    const T2: &str = "2026-08-14T10:16:55.161235+00:00";

    let provider = Provider::start(offering(&["BTC/USD", "ETH/USD"], 16), &[]).await;
    let plain = subscriber();
    let resumed = plain.clone().with_resume(Arc::new(LseResumeState::new()));

    let mut resuming = stream(&resumed, &[subscription("btc")]).await;
    let mut live = stream(&plain, &[subscription("btc"), subscription("eth")]).await;

    provider.push(tick_frame("BTC/USD", T1, 1.0, false));
    assert_eq!(next_event(&mut resuming).await.time_exchange, at(T1));
    assert_eq!(next_event(&mut live).await.time_exchange, at(T1));

    provider.drop_connection();
    ended(&mut resuming).await;
    ended(&mut live).await;
    drop((resuming, live));

    let mut resuming = stream(&resumed, &[subscription("btc")]).await;
    provider.push(tick_frame("BTC/USD", T1, 1.0, true));
    provider.push(tick_frame("BTC/USD", T2, 2.0, false));

    let mut live = stream(&plain, &[subscription("btc"), subscription("eth")]).await;

    assert_eq!(next_event(&mut resuming).await.time_exchange, at(T2));
    assert_eq!(
        next_event(&mut live).await.time_exchange,
        at(T2),
        "the replayed T1 reached a stream that asked for no replay",
    );
}

/// A rejection names no symbol, but attaches are serialised, so it belongs to the attach in flight:
/// that attach fails, what it sent is released so a failed batch holds no slot, and every other
/// stream on the connection carries on.
#[tokio::test]
#[serial]
async fn a_rejected_attach_releases_what_it_sent_and_leaves_the_connection_up() {
    let provider = Provider::start(offering(&["BTC/USD"], 16), &["NOPE"]).await;
    let subscriber = subscriber();

    let mut bitcoin = stream(&subscriber, &[subscription("btc")]).await;

    let error = subscriber
        .subscribe(&[
            contract("spy", OptionKind::Call, dec!(700)),
            contract("nope", OptionKind::Call, dec!(10)),
        ])
        .await
        .unwrap_err()
        .to_string();

    assert!(error.contains("INVALID_UNDERLYING"), "{error}");
    assert!(error.contains("shared by every stream"), "{error}");

    provider
        .until("SPY to be released", |provider| {
            provider
                .sent_on(1)
                .contains(&json!({"action": "unsubscribe_options", "underlying": "SPY"}))
        })
        .await;

    provider.push(tick_frame("BTC/USD", "2026-08-14T10:00:00Z", 1.0, false));
    assert_eq!(
        next_event(&mut bitcoin).await.instrument,
        subscription("btc").instrument
    );
    assert_eq!(provider.connections(), 1);
}

/// Option chains and plain symbols share the socket and its cap, and each frame reaches the stream
/// that registered it: a contract by its underlying, a symbol by its spelling.
#[tokio::test]
#[serial]
async fn option_and_plain_streams_share_one_connection() {
    let provider = Provider::start(offering(&["BTC/USD"], 16), &[]).await;
    let subscriber = subscriber();
    let registered = contract("spy", OptionKind::Call, dec!(700));

    let mut bitcoin = stream(&subscriber, &[subscription("btc")]).await;
    let mut options =
        <LseStream<_, _, _> as MarketStream<
            OptionsHarnessLse,
            MarketDataInstrument,
            PublicTrades,
        >>::init::<NoInitialSnapshots>(&subscriber, std::slice::from_ref(&registered))
        .await
        .unwrap();

    assert_eq!(provider.connections(), 1);

    provider.push(option_tick_frame("SPY260930C00700000"));
    provider.push(tick_frame("BTC/USD", "2026-08-14T10:00:00Z", 1.0, false));

    assert_eq!(
        next_event(&mut options).await.instrument,
        registered.instrument
    );
    assert_eq!(
        next_event(&mut bitcoin).await.instrument,
        subscription("btc").instrument
    );
}
