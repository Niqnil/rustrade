//! Massive's shared WebSocket connection, driven against a synthetic in-process server.
//!
//! # Why this exists
//!
//! Massive allows a key a fixed number of connections per cluster — one on an individual plan —
//! and past it closes the older connection, so every stream a `MassiveSubscriber` and its clones
//! open on a cluster has to share one socket. What that promises is about the wire — how many
//! connections were opened, what each subscribe and unsubscribe carried, which frames reached which
//! stream — and only a server that records the conversation can check it. These tests count
//! connections and payloads, and split frames that mix subscriptions from different streams.
//!
//! # No provider data is involved
//!
//! Massive's terms prohibit redistributing its data. Every frame here is hand-written to the
//! shapes the decoders document. Live behaviour is the separate, credential-gated job of
//! `massive_integration.rs`.
//!
//! # Running
//!
//! ```bash
//! cargo test --test massive_ws_shared --features massive
//! ```
//!
//! No network, no credentials — these run on every ordinary test run.

#![cfg(feature = "massive")]
#![allow(clippy::unwrap_used, clippy::expect_used)] // Test code: panics on bad input are acceptable

use futures_util::{SinkExt, StreamExt};
use rust_decimal_macros::dec;
use rustrade_data::{
    Identifier, MarketStream, NoInitialSnapshots,
    error::DataError,
    event::MarketEvent,
    exchange::{
        ExchangeServer,
        massive::{
            Massive, MassiveCredentials, MassiveSubscriber,
            channel::MassiveChannel,
            market::{MassiveMarket, MassiveServer, MassiveSymbols, MassiveTradeServer},
            message::{MassiveKind, MassiveTransformer},
            stream::MassiveStream,
        },
    },
    subscription::{
        Subscription,
        book::OrderBooksL1,
        candle::{CandleInterval, Candles},
        quote::Quotes,
        trade::PublicTrades,
    },
};
use rustrade_instrument::{
    exchange::ExchangeId,
    instrument::market_data::{MarketDataInstrument, kind::MarketDataInstrumentKind},
};
use serde_json::{Value, json};
use serial_test::serial;
use std::{
    collections::BTreeSet,
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
use tokio_tungstenite::tungstenite::{
    Message,
    protocol::{CloseFrame, frame::coding::CloseCode},
};

/// The endpoint each harness cluster resolves to, set by the provider serving it.
///
/// [`ExchangeServer::websocket_url`] answers with a `&'static str` and takes no arguments, so the
/// port a freshly bound listener was assigned has nowhere else to live. Every test therefore runs
/// `#[serial]`.
static CRYPTO_URL: Mutex<Option<&'static str>> = Mutex::new(None);
static FOREX_URL: Mutex<Option<&'static str>> = Mutex::new(None);

/// A cluster identical to the shipped crypto one except for where it points: `Massive<Server>`
/// carries the real connector, channels and market spelling, so what these tests drive is
/// production code.
#[derive(Copy, Clone, Debug, Default)]
struct CryptoServer;

impl ExchangeServer for CryptoServer {
    const ID: ExchangeId = ExchangeId::MassiveCrypto;

    fn websocket_url() -> &'static str {
        CRYPTO_URL
            .lock()
            .unwrap()
            .expect("a provider must be started before the connector resolves its endpoint")
    }
}

impl MassiveServer for CryptoServer {
    const SYMBOLS: MassiveSymbols = MassiveSymbols::CryptoPair;
    const QUOTES: &'static str = "XQ";
    const SECOND_AGGREGATES: &'static str = "XAS";
    const MINUTE_AGGREGATES: &'static str = "XA";
}

impl MassiveTradeServer for CryptoServer {
    const TRADES: &'static str = "XT";
}

/// The forex cluster's twin, on an endpoint of its own.
#[derive(Copy, Clone, Debug, Default)]
struct ForexServer;

impl ExchangeServer for ForexServer {
    const ID: ExchangeId = ExchangeId::MassiveForex;

    fn websocket_url() -> &'static str {
        FOREX_URL
            .lock()
            .unwrap()
            .expect("a provider must be started before the connector resolves its endpoint")
    }
}

impl MassiveServer for ForexServer {
    const SYMBOLS: MassiveSymbols = MassiveSymbols::CurrencyPair;
    const QUOTES: &'static str = "C";
    const SECOND_AGGREGATES: &'static str = "CAS";
    const MINUTE_AGGREGATES: &'static str = "CA";
}

enum Control {
    Send(Value),
    /// Close the socket as Massive does when a newer connection takes its place.
    Evict,
}

/// A synthetic Massive cluster that stays up across connections and is driven by the test.
///
/// It keeps each connection's subscriptions as Massive does: it confirms each new one by name,
/// says nothing to one the connection already holds, refuses the `refused` ones with a bare
/// `not authorized` placed **first** in the answer, whatever order they were sent in, and confirms
/// an unsubscribe only for what the connection held. It pings ahead of its `connected` and
/// `auth_success` statuses. It logs every payload but `auth` against the connection it arrived on,
/// and sends frames when the test says to.
struct Provider {
    log: Arc<Mutex<Vec<(usize, Value)>>>,
    connections: Arc<AtomicUsize>,
    closed: Arc<AtomicUsize>,
    control: Arc<Mutex<Option<mpsc::UnboundedSender<Control>>>>,
    accepting: tokio::task::JoinHandle<()>,
}

impl Provider {
    async fn start(url: &Mutex<Option<&'static str>>) -> Self {
        Self::refusing(url, &[]).await
    }

    async fn refusing(url: &Mutex<Option<&'static str>>, refused: &'static [&'static str]) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address: &'static str =
            Box::leak(format!("ws://{}", listener.local_addr().unwrap()).into_boxed_str());
        *url.lock().unwrap() = Some(address);

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
                    refused,
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

    fn evict(&self) {
        self.control().send(Control::Evict).unwrap();
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

    fn closed(&self) -> usize {
        self.closed.load(Ordering::SeqCst)
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
}

impl Drop for Provider {
    fn drop(&mut self) {
        self.accepting.abort();
    }
}

fn status(status: &str, message: &str) -> Value {
    json!({"ev": "status", "status": status, "message": message})
}

fn subscribe(params: &str) -> Value {
    json!({"action": "subscribe", "params": params})
}

fn unsubscribe(params: &str) -> Value {
    json!({"action": "unsubscribe", "params": params})
}

async fn serve_connection(
    stream: TcpStream,
    number: usize,
    refused: &'static [&'static str],
    log: Arc<Mutex<Vec<(usize, Value)>>>,
    mut control: mpsc::UnboundedReceiver<Control>,
    closed: Arc<AtomicUsize>,
) {
    let Ok(mut websocket) = tokio_tungstenite::accept_async(stream).await else {
        closed.fetch_add(1, Ordering::SeqCst);
        return;
    };

    // A ping ahead of each authentication status, which the client must skip rather than read as
    // its answer.
    let _ = websocket.send(Message::Ping(Vec::new().into())).await;
    let connected = json!([status("connected", "Connected Successfully")]);
    let _ = websocket.send(Message::text(connected.to_string())).await;

    let mut held = BTreeSet::<String>::new();

    loop {
        let payload = tokio::select! {
            command = control.recv() => match command {
                Some(Control::Send(frame)) => {
                    if websocket.send(Message::text(frame.to_string())).await.is_err() {
                        break;
                    }
                    continue;
                }
                Some(Control::Evict) => {
                    let evicted = json!([status(
                        "max_connections",
                        "Maximum number of websocket connections exceeded. You may only use 1 \
                         connections at a time.",
                    )]);
                    let _ = websocket.send(Message::text(evicted.to_string())).await;
                    let _ = websocket
                        .close(Some(CloseFrame {
                            code: CloseCode::Policy,
                            reason: "max_connections".into(),
                        }))
                        .await;
                    break;
                }
                None => break,
            },
            message = websocket.next() => match message {
                Some(Ok(Message::Text(text))) => match serde_json::from_str::<Value>(text.as_str()) {
                    Ok(payload) => payload,
                    Err(_) => continue,
                },
                Some(Ok(Message::Close(_))) | Some(Err(_)) | None => break,
                Some(Ok(_)) => continue,
            },
        };

        let action = payload["action"].as_str().unwrap_or_default().to_owned();
        if action != "auth" {
            log.lock().unwrap().push((number, payload.clone()));
        }

        let params = payload["params"]
            .as_str()
            .unwrap_or_default()
            .split(',')
            .filter(|param| !param.is_empty())
            .map(str::to_owned)
            .collect::<Vec<_>>();

        let answer = match action.as_str() {
            "auth" => {
                if websocket
                    .send(Message::Ping(Vec::new().into()))
                    .await
                    .is_err()
                {
                    break;
                }
                vec![status("auth_success", "authenticated")]
            }
            "subscribe" => {
                let mut refusals = Vec::new();
                let mut confirmations = Vec::new();
                for param in params {
                    if refused.contains(&param.as_str()) {
                        refusals.push(status("error", "not authorized"));
                    } else if held.insert(param.clone()) {
                        confirmations.push(status("success", &format!("subscribed to: {param}")));
                    }
                }
                refusals.into_iter().chain(confirmations).collect()
            }
            "unsubscribe" => params
                .into_iter()
                .filter(|param| held.remove(param))
                .map(|param| status("success", &format!("unsubscribed to: {param}")))
                .collect(),
            _ => continue,
        };

        if answer.is_empty() {
            continue;
        }
        if websocket
            .send(Message::text(Value::Array(answer).to_string()))
            .await
            .is_err()
        {
            break;
        }
    }

    closed.fetch_add(1, Ordering::SeqCst);
}

fn subscriber() -> MassiveSubscriber {
    MassiveSubscriber::new(MassiveCredentials::new("key"))
}

fn on<Server, Kind>(
    base: &str,
    quote: &str,
    kind: Kind,
) -> Subscription<Massive<Server>, MarketDataInstrument, Kind>
where
    Server: MassiveServer,
{
    Subscription::from((
        Massive::<Server>::default(),
        base,
        quote,
        MarketDataInstrumentKind::Spot,
        kind,
    ))
}

type Stream<Server, Kind> =
    MassiveStream<MassiveTransformer<Massive<Server>, MarketDataInstrument, Kind>>;

async fn stream<Server, Kind>(
    subscriber: &MassiveSubscriber,
    subscriptions: &[Subscription<Massive<Server>, MarketDataInstrument, Kind>],
) -> Result<Stream<Server, Kind>, DataError>
where
    Server: MassiveServer + std::fmt::Debug + Send + Sync,
    Kind: MassiveKind + Send + Sync,
    Kind::Event: Send + Sync,
    Stream<Server, Kind>: MarketStream<Massive<Server>, MarketDataInstrument, Kind>,
    Subscription<Massive<Server>, MarketDataInstrument, Kind>:
        Identifier<MassiveChannel> + Identifier<MassiveMarket>,
{
    <Stream<Server, Kind> as MarketStream<Massive<Server>, MarketDataInstrument, Kind>>::init::<
        NoInitialSnapshots,
    >(subscriber, subscriptions)
    .await
}

fn trade(pair: &str, id: &str) -> Value {
    json!({"ev": "XT", "pair": pair, "i": id, "p": 82857.5, "s": 0.01, "t": 1759056000000_i64,
           "c": [2], "x": 1})
}

fn quote(pair: &str) -> Value {
    json!({"ev": "XQ", "pair": pair, "bp": 82857.0, "bs": 0.5, "ap": 82858.0, "as": 0.25,
           "t": 1759056000000_i64, "x": 1})
}

/// The next item `stream` yields, which must be an event rather than an error.
async fn next_event<S, Event>(stream: &mut S) -> MarketEvent<MarketDataInstrument, Event>
where
    S: futures_util::Stream<Item = Result<MarketEvent<MarketDataInstrument, Event>, DataError>>
        + Unpin,
    Event: std::fmt::Debug,
{
    tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("no event within five seconds")
        .expect("the stream ended")
        .expect("expected an event, got an error")
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

/// Assert nothing more is waiting on `stream` — in particular no error event for a message
/// another stream subscribed to.
async fn quiet<S>(stream: &mut S)
where
    S: futures_util::Stream + Unpin,
    S::Item: std::fmt::Debug,
{
    let next = tokio::time::timeout(Duration::from_millis(100), stream.next()).await;
    assert!(next.is_err(), "expected nothing more, got {next:?}");
}

/// The failure the whole connection design exists for: a second socket on the key evicts the
/// first, so trades and quotes on one cluster must share one — and a frame mixing both reaches each
/// stream as only its own messages, never as an error for the other's.
#[tokio::test]
#[serial]
async fn trades_and_quotes_on_one_cluster_share_one_connection_and_each_gets_only_its_own() {
    let provider = Provider::start(&CRYPTO_URL).await;
    let subscriber = subscriber();

    let mut trading = stream(
        &subscriber.clone(),
        &[
            on::<CryptoServer, _>("btc", "usd", PublicTrades),
            on("eth", "usd", PublicTrades),
        ],
    )
    .await
    .unwrap();
    let mut quoting = stream(&subscriber, &[on::<CryptoServer, _>("btc", "usd", Quotes)])
        .await
        .unwrap();

    assert_eq!(provider.connections(), 1);
    assert_eq!(
        provider.sent_on(1),
        [subscribe("XT.BTC-USD,XT.ETH-USD"), subscribe("XQ.BTC-USD")],
    );

    provider.push(json!([
        trade("BTC-USD", "1"),
        quote("BTC-USD"),
        trade("ETH-USD", "2")
    ]));

    let first = next_event(&mut trading).await;
    assert_eq!(first.instrument.base.name().as_str(), "btc");
    assert_eq!(first.kind.id, "1");
    let second = next_event(&mut trading).await;
    assert_eq!(second.instrument.base.name().as_str(), "eth");
    assert_eq!(second.kind.id, "2");

    let quoted = next_event(&mut quoting).await;
    assert_eq!(quoted.instrument.base.name().as_str(), "btc");
    assert_eq!(quoted.kind.bid_price, dec!(82857.0));

    quiet(&mut trading).await;
    quiet(&mut quoting).await;
}

/// Massive says nothing to a subscription the connection already holds, so resending one would
/// leave the second stream waiting out its timeout.
#[tokio::test]
#[serial]
async fn a_subscription_another_stream_holds_is_not_sent_again_and_reaches_both() {
    let provider = Provider::start(&CRYPTO_URL).await;
    let subscriber = subscriber();

    let mut first = stream(
        &subscriber.clone(),
        &[on::<CryptoServer, _>("btc", "usd", PublicTrades)],
    )
    .await
    .unwrap();
    let mut second = stream(
        &subscriber,
        &[on::<CryptoServer, _>("btc", "usd", PublicTrades)],
    )
    .await
    .unwrap();

    assert_eq!(provider.sent_on(1), [subscribe("XT.BTC-USD")]);

    provider.push(json!([trade("BTC-USD", "7")]));

    let one = next_event(&mut first).await;
    let two = next_event(&mut second).await;
    assert_eq!((one.kind.id.as_str(), two.kind.id.as_str()), ("7", "7"));
}

#[tokio::test]
#[serial]
async fn a_detach_unsubscribes_only_what_no_other_stream_holds_and_the_last_closes_the_socket() {
    let provider = Provider::start(&CRYPTO_URL).await;
    let subscriber = subscriber();

    let both = stream(
        &subscriber.clone(),
        &[
            on::<CryptoServer, _>("btc", "usd", PublicTrades),
            on("eth", "usd", PublicTrades),
        ],
    )
    .await
    .unwrap();
    let bitcoin = stream(
        &subscriber,
        &[on::<CryptoServer, _>("btc", "usd", PublicTrades)],
    )
    .await
    .unwrap();

    drop(both);
    provider
        .until("the unsubscribe", |provider| provider.sent_on(1).len() == 2)
        .await;
    assert_eq!(provider.sent_on(1)[1], unsubscribe("XT.ETH-USD"));

    drop(bitcoin);
    provider
        .until("the socket to close", |provider| provider.closed() == 1)
        .await;
    assert_eq!(
        provider.sent_on(1).len(),
        2,
        "the last detach closes the socket rather than unsubscribing",
    );
}

/// Massive's refusal names nothing and arrives ahead of the confirmations sent before it. The
/// subscribe must still see every confirmation in the frame, and fail naming only what Massive
/// did not confirm — then release what it did subscribe, since nothing else holds it.
#[tokio::test]
#[serial]
async fn a_refusal_ahead_of_its_confirmations_fails_naming_only_what_went_unconfirmed() {
    let provider = Provider::refusing(&CRYPTO_URL, &["XQ.ETH-USD"]).await;
    let subscriber = subscriber();

    let mut trading = stream(
        &subscriber.clone(),
        &[on::<CryptoServer, _>("btc", "usd", PublicTrades)],
    )
    .await
    .unwrap();

    let refused = stream(
        &subscriber,
        &[
            on::<CryptoServer, _>("btc", "usd", Quotes),
            on("eth", "usd", Quotes),
        ],
    )
    .await
    .unwrap_err();
    let message = refused.to_string();
    assert!(message.contains("not authorized"), "{message}");
    let (_, unconfirmed) = message
        .split_once("among those it did not confirm: ")
        .unwrap_or_else(|| panic!("names what went unconfirmed: {message}"));
    assert!(unconfirmed.starts_with("XQ.ETH-USD"), "{message}");
    assert!(!unconfirmed.contains("XQ.BTC-USD"), "{message}");
    assert!(message.contains("shared by every stream"), "{message}");

    provider
        .until("the unsubscribe", |provider| provider.sent_on(1).len() == 3)
        .await;
    assert_eq!(
        provider.sent_on(1),
        [
            subscribe("XT.BTC-USD"),
            subscribe("XQ.BTC-USD,XQ.ETH-USD"),
            unsubscribe("XQ.BTC-USD,XQ.ETH-USD"),
        ],
    );
    assert_eq!(provider.connections(), 1);

    provider.push(json!([trade("BTC-USD", "3")]));
    assert_eq!(next_event(&mut trading).await.kind.id, "3");
}

/// Massive aggregates per second and per minute only. Any other interval is refused before a
/// connection is opened, rather than waiting out a timeout for a channel that does not exist.
#[tokio::test]
#[serial]
async fn an_unsupported_candle_interval_is_refused_without_connecting() {
    let provider = Provider::start(&CRYPTO_URL).await;

    let refused = stream(
        &subscriber(),
        &[on::<CryptoServer, _>(
            "btc",
            "usd",
            Candles {
                interval: CandleInterval::Min5,
            },
        )],
    )
    .await
    .unwrap_err();
    let message = refused.to_string();

    assert!(message.contains("Min5"), "{message}");
    assert_eq!(provider.connections(), 0);
}

#[tokio::test]
#[serial]
async fn candles_read_the_per_second_and_per_minute_channels() {
    let provider = Provider::start(&CRYPTO_URL).await;

    let mut candles = stream(
        &subscriber(),
        &[
            on::<CryptoServer, _>(
                "btc",
                "usd",
                Candles {
                    interval: CandleInterval::Sec1,
                },
            ),
            on(
                "btc",
                "usd",
                Candles {
                    interval: CandleInterval::Min1,
                },
            ),
        ],
    )
    .await
    .unwrap();

    assert_eq!(provider.sent_on(1), [subscribe("XAS.BTC-USD,XA.BTC-USD")]);

    provider.push(
        json!([{"ev": "XA", "pair": "BTC-USD", "o": 1.0, "h": 2.0, "l": 0.5,
                           "c": 1.5, "v": 10.0, "vw": 1.2, "z": 0.25,
                           "s": 1759056000000_i64, "e": 1759056060000_i64}]),
    );

    let candle = next_event(&mut candles).await;
    assert_eq!(candle.kind.close_time.timestamp_millis(), 1759056060000);
    assert_eq!(candle.kind.volume, Some(dec!(10.0)));
}

/// Another connection on the key evicts this one: every stream on it ends, so each reconnects.
#[tokio::test]
#[serial]
async fn an_eviction_ends_every_stream_on_the_connection() {
    let provider = Provider::start(&CRYPTO_URL).await;
    let subscriber = subscriber();

    let mut trading = stream(
        &subscriber.clone(),
        &[on::<CryptoServer, _>("btc", "usd", PublicTrades)],
    )
    .await
    .unwrap();
    let mut quoting = stream(
        &subscriber.clone(),
        &[on::<CryptoServer, _>("btc", "usd", Quotes)],
    )
    .await
    .unwrap();

    provider.evict();
    ended(&mut trading).await;
    ended(&mut quoting).await;
    drop((trading, quoting));

    let _trading = stream(
        &subscriber,
        &[on::<CryptoServer, _>("btc", "usd", PublicTrades)],
    )
    .await
    .unwrap();
    assert_eq!(provider.connections(), 2);
    let resubscribed = provider.sent_on(2);
    assert_eq!(resubscribed.len(), 1, "{resubscribed:?}");
    let params = resubscribed[0]["params"].as_str().unwrap();
    let params = params.split(',').collect::<BTreeSet<_>>();
    assert_eq!(params, BTreeSet::from(["XT.BTC-USD", "XQ.BTC-USD"]));
}

/// Forex is subscribed with the slash its messages carry. Massive also accepts `EUR-USD`, but
/// delivers its data as `EUR/USD`, which would reach no stream.
#[tokio::test]
#[serial]
async fn forex_subscribes_with_the_slash_its_messages_carry_on_a_socket_of_its_own() {
    let crypto = Provider::start(&CRYPTO_URL).await;
    let forex = Provider::start(&FOREX_URL).await;
    let subscriber = subscriber();

    let _coin = stream(
        &subscriber.clone(),
        &[on::<CryptoServer, _>("btc", "usd", PublicTrades)],
    )
    .await
    .unwrap();
    let mut books = stream(
        &subscriber,
        &[on::<ForexServer, _>("eur", "usd", OrderBooksL1)],
    )
    .await
    .unwrap();

    assert_eq!((crypto.connections(), forex.connections()), (1, 1));
    assert_eq!(forex.sent_on(1), [subscribe("C.EUR/USD")]);

    forex.push(
        json!([{"ev": "C", "p": "EUR/USD", "b": 1.1702, "a": 1.1703, "i": 0, "x": 48,
                        "t": 1759056000000_i64}]),
    );

    let book = next_event(&mut books).await;
    assert_eq!(book.exchange, ExchangeId::MassiveForex);
    let bid = book.kind.best_bid.unwrap();
    assert_eq!((bid.price, bid.amount), (dec!(1.1702), dec!(0)));
}
