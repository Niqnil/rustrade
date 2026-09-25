//! Alpaca's shared market data connection, driven against a synthetic in-process server.
//!
//! # Why this exists
//!
//! Alpaca allows an account one connection per feed and refuses a second at authentication, so
//! every stream an `AlpacaSubscriber` and its clones open on a feed has to share one socket. What
//! that promises is about the wire — how many connections were opened, what each subscribe and
//! unsubscribe carried, which frames reached which stream — and only a server that records the
//! conversation can check it. These tests count connections and payloads, and split frames that mix
//! subscriptions from different streams.
//!
//! # No provider data is involved
//!
//! Every frame is hand-written to the shapes the decoders document. Live behaviour is the separate,
//! credential-gated job of `alpaca_data.rs`.
//!
//! # Running
//!
//! ```bash
//! cargo test --test alpaca_ws_shared --features alpaca
//! ```
//!
//! No network, no credentials — these run on every ordinary test run.

#![cfg(feature = "alpaca")]
#![allow(clippy::unwrap_used, clippy::expect_used)] // Test code: panics on bad input are acceptable

use futures_util::{SinkExt, StreamExt};
use rust_decimal_macros::dec;
use rustrade_data::{
    MarketStream, NoInitialSnapshots,
    error::DataError,
    event::MarketEvent,
    exchange::{
        ExchangeServer,
        alpaca::{
            Alpaca, AlpacaCredentials, AlpacaSubscriber,
            market::{AlpacaServer, AlpacaSymbolShape},
            quote::AlpacaQuoteTransformer,
            stream::AlpacaStream,
            trade::AlpacaTradeTransformer,
        },
    },
    subscription::{
        Subscription,
        quote::{Quote, Quotes},
        trade::{PublicTrade, PublicTrades},
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
use tokio_tungstenite::tungstenite::Message;

/// The endpoint each harness feed resolves to, set by the provider serving it.
///
/// [`ExchangeServer::websocket_url`] answers with a `&'static str` and takes no arguments, so the
/// port a freshly bound listener was assigned has nowhere else to live. Every test therefore runs
/// `#[serial]`.
static EQUITIES_URL: Mutex<Option<&'static str>> = Mutex::new(None);
static CRYPTO_URL: Mutex<Option<&'static str>> = Mutex::new(None);

/// A feed identical to the shipped IEX one except for where it points: `Alpaca<Server>` carries
/// the real connector, identifiers and symbol spelling, so what these tests drive is production
/// code.
#[derive(Copy, Clone, Debug, Default)]
struct EquitiesServer;

impl ExchangeServer for EquitiesServer {
    const ID: ExchangeId = ExchangeId::AlpacaIex;

    fn websocket_url() -> &'static str {
        EQUITIES_URL
            .lock()
            .unwrap()
            .expect("a provider must be started before the connector resolves its endpoint")
    }
}

impl AlpacaServer for EquitiesServer {
    const SYMBOL_SHAPE: AlpacaSymbolShape = AlpacaSymbolShape::Ticker;
}

/// A second feed, on an endpoint of its own, as the crypto feed is.
#[derive(Copy, Clone, Debug, Default)]
struct CryptoServer;

impl ExchangeServer for CryptoServer {
    const ID: ExchangeId = ExchangeId::AlpacaCrypto;

    fn websocket_url() -> &'static str {
        CRYPTO_URL
            .lock()
            .unwrap()
            .expect("a provider must be started before the connector resolves its endpoint")
    }
}

impl AlpacaServer for CryptoServer {
    const SYMBOL_SHAPE: AlpacaSymbolShape = AlpacaSymbolShape::Pair;
}

type Equities = Alpaca<EquitiesServer>;
type Crypto = Alpaca<CryptoServer>;

enum Control {
    Send(Value),
    /// Drop the socket without a close frame, as a network failure would.
    Drop,
}

/// How the provider answers `auth`.
#[derive(Clone, Copy)]
enum Auth {
    Accept,
    /// Refuse it as Alpaca refuses a second connection on a feed.
    ConnectionLimit,
}

/// A synthetic Alpaca feed that stays up across connections and is driven by the test.
///
/// It keeps each connection's subscription state as Alpaca does — answering a subscribe or an
/// unsubscribe with the connection's whole state, and refusing a subscribe that would take it past
/// `cap` pairs without adding any of it — logs every payload but `auth` against the connection it
/// arrived on, and sends frames when the test says to.
struct Provider {
    log: Arc<Mutex<Vec<(usize, Value)>>>,
    connections: Arc<AtomicUsize>,
    closed: Arc<AtomicUsize>,
    control: Arc<Mutex<Option<mpsc::UnboundedSender<Control>>>>,
    accepting: tokio::task::JoinHandle<()>,
}

impl Provider {
    async fn start(url: &Mutex<Option<&'static str>>, auth: Auth, cap: usize) -> Self {
        Self::start_ignoring(url, auth, cap, &[]).await
    }

    /// As [`start`](Self::start), but the provider never registers a subscribe for `ignored`
    /// symbols: it answers with a state that leaves them out, as a venue dropping one quietly would.
    async fn start_ignoring(
        url: &Mutex<Option<&'static str>>,
        auth: Auth,
        cap: usize,
        ignored: &'static [&'static str],
    ) -> Self {
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
                    Behaviour { auth, cap, ignored },
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

/// The pairs a payload names for `channel`.
fn named<'a>(payload: &'a Value, channel: &str) -> impl Iterator<Item = String> + 'a {
    payload[channel]
        .as_array()
        .into_iter()
        .flatten()
        .map(|symbol| symbol.as_str().unwrap().to_owned())
}

/// How a provider answers, the same for every connection it serves.
#[derive(Clone, Copy)]
struct Behaviour {
    auth: Auth,
    cap: usize,
    ignored: &'static [&'static str],
}

async fn serve_connection(
    stream: TcpStream,
    number: usize,
    behaviour: Behaviour,
    log: Arc<Mutex<Vec<(usize, Value)>>>,
    mut control: mpsc::UnboundedReceiver<Control>,
    closed: Arc<AtomicUsize>,
) {
    let Behaviour { auth, cap, ignored } = behaviour;

    let Ok(mut websocket) = tokio_tungstenite::accept_async(stream).await else {
        closed.fetch_add(1, Ordering::SeqCst);
        return;
    };

    let connected = json!([{"T": "success", "msg": "connected"}]);
    let _ = websocket.send(Message::text(connected.to_string())).await;

    let mut trades = BTreeSet::<String>::new();
    let mut quotes = BTreeSet::<String>::new();

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
                Some(Ok(Message::Close(_))) | Some(Err(_)) | None => break,
                Some(Ok(_)) => continue,
            },
        };

        let action = payload["action"].as_str().unwrap_or_default().to_owned();
        if action != "auth" {
            log.lock().unwrap().push((number, payload.clone()));
        }

        let answer = match action.as_str() {
            "auth" => match auth {
                Auth::Accept => json!([{"T": "success", "msg": "authenticated"}]),
                Auth::ConnectionLimit => {
                    json!([{"T": "error", "code": 406, "msg": "connection limit exceeded"}])
                }
            },
            "subscribe" => {
                let mut next_trades = trades.clone();
                let mut next_quotes = quotes.clone();
                let registered = |symbol: &String| !ignored.contains(&symbol.as_str());
                next_trades.extend(named(&payload, "trades").filter(registered));
                next_quotes.extend(named(&payload, "quotes").filter(registered));

                if next_trades.len() + next_quotes.len() > cap {
                    json!([{"T": "error", "code": 405, "msg": "symbol limit exceeded"}])
                } else {
                    trades = next_trades;
                    quotes = next_quotes;
                    json!([{"T": "subscription", "trades": trades, "quotes": quotes, "bars": []}])
                }
            }
            "unsubscribe" => {
                for symbol in named(&payload, "trades") {
                    trades.remove(&symbol);
                }
                for symbol in named(&payload, "quotes") {
                    quotes.remove(&symbol);
                }
                json!([{"T": "subscription", "trades": trades, "quotes": quotes, "bars": []}])
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

fn subscriber() -> AlpacaSubscriber {
    AlpacaSubscriber::new(AlpacaCredentials::new("key", "secret"))
}

fn trades(base: &str) -> Subscription<Equities, MarketDataInstrument, PublicTrades> {
    Subscription::from((
        Equities::default(),
        base,
        "usd",
        MarketDataInstrumentKind::Spot,
        PublicTrades,
    ))
}

fn crypto_trades(base: &str) -> Subscription<Crypto, MarketDataInstrument, PublicTrades> {
    Subscription::from((
        Crypto::default(),
        base,
        "usd",
        MarketDataInstrumentKind::Spot,
        PublicTrades,
    ))
}

fn quotes(base: &str) -> Subscription<Equities, MarketDataInstrument, Quotes> {
    Subscription::from((
        Equities::default(),
        base,
        "usd",
        MarketDataInstrumentKind::Spot,
        Quotes,
    ))
}

type TradeStream<Server> =
    AlpacaStream<AlpacaTradeTransformer<Alpaca<Server>, MarketDataInstrument>>;
type QuoteStream = AlpacaStream<AlpacaQuoteTransformer<Equities, MarketDataInstrument>>;

async fn trade_stream<Server>(
    subscriber: &AlpacaSubscriber,
    subscriptions: &[Subscription<Alpaca<Server>, MarketDataInstrument, PublicTrades>],
) -> Result<TradeStream<Server>, DataError>
where
    Server: AlpacaServer + std::fmt::Debug + Send + Sync,
{
    <TradeStream<Server> as MarketStream<Alpaca<Server>, MarketDataInstrument, PublicTrades>>::init::<
        NoInitialSnapshots,
    >(subscriber, subscriptions)
    .await
}

async fn quote_stream(
    subscriber: &AlpacaSubscriber,
    subscriptions: &[Subscription<Equities, MarketDataInstrument, Quotes>],
) -> Result<QuoteStream, DataError> {
    <QuoteStream as MarketStream<Equities, MarketDataInstrument, Quotes>>::init::<
        NoInitialSnapshots,
    >(subscriber, subscriptions)
    .await
}

fn trade(symbol: &str, id: u64) -> Value {
    json!({"T": "t", "S": symbol, "i": id, "p": 150.25, "s": 100, "t": "2026-09-25T14:00:00Z"})
}

fn quote(symbol: &str) -> Value {
    json!({"T": "q", "S": symbol, "bp": 150.2, "bs": 200, "ap": 150.3, "as": 100,
           "t": "2026-09-25T14:00:00Z"})
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

/// The failure the whole connection design exists for: Alpaca allows one socket per feed, so
/// trades and quotes on one feed must share it — and a frame mixing both reaches each stream as
/// only its own messages, never as an error for the other's.
#[tokio::test]
#[serial]
async fn trades_and_quotes_on_one_feed_share_one_connection_and_each_gets_only_its_own() {
    let provider = Provider::start(&EQUITIES_URL, Auth::Accept, 30).await;
    let subscriber = subscriber();

    let mut trading = trade_stream(&subscriber.clone(), &[trades("aapl"), trades("msft")])
        .await
        .unwrap();
    let mut quoting = quote_stream(&subscriber, &[quotes("aapl")]).await.unwrap();

    assert_eq!(provider.connections(), 1);
    assert_eq!(
        provider.sent_on(1),
        [
            json!({"action": "subscribe", "trades": ["AAPL", "MSFT"]}),
            json!({"action": "subscribe", "quotes": ["AAPL"]}),
        ],
    );

    provider.push(json!([trade("AAPL", 1), quote("AAPL"), trade("MSFT", 2)]));

    let first = next_event(&mut trading).await;
    assert_eq!(first.instrument, trades("aapl").instrument);
    assert_eq!(first.kind.id, "1");
    let second = next_event(&mut trading).await;
    assert_eq!(second.instrument, trades("msft").instrument);
    assert_eq!(second.kind.id, "2");

    let book: MarketEvent<MarketDataInstrument, Quote> = next_event(&mut quoting).await;
    assert_eq!(book.instrument, quotes("aapl").instrument);
    assert_eq!(book.kind.bid_price, dec!(150.2));

    quiet(&mut trading).await;
    quiet(&mut quoting).await;
}

#[tokio::test]
#[serial]
async fn a_pair_another_stream_holds_is_not_subscribed_again_and_reaches_both() {
    let provider = Provider::start(&EQUITIES_URL, Auth::Accept, 30).await;
    let subscriber = subscriber();

    let mut first = trade_stream(&subscriber.clone(), &[trades("aapl")])
        .await
        .unwrap();
    let mut second = trade_stream(&subscriber, &[trades("aapl")]).await.unwrap();

    assert_eq!(
        provider.sent_on(1),
        [json!({"action": "subscribe", "trades": ["AAPL"]})],
    );

    provider.push(json!([trade("AAPL", 7)]));

    let one: MarketEvent<MarketDataInstrument, PublicTrade> = next_event(&mut first).await;
    let two: MarketEvent<MarketDataInstrument, PublicTrade> = next_event(&mut second).await;
    assert_eq!((one.kind.id.as_str(), two.kind.id.as_str()), ("7", "7"));
}

/// Alpaca refuses a subscribe past the connection's cap whole, naming nothing. The refused stream
/// must fail saying the cap is shared, send nothing to undo, and leave the others streaming.
#[tokio::test]
#[serial]
async fn a_refused_subscribe_fails_saying_the_cap_is_shared_and_leaves_the_connection_up() {
    let provider = Provider::start(&EQUITIES_URL, Auth::Accept, 1).await;
    let subscriber = subscriber();

    let mut trading = trade_stream(&subscriber.clone(), &[trades("aapl")])
        .await
        .unwrap();

    let refused = quote_stream(&subscriber, &[quotes("aapl")])
        .await
        .unwrap_err();
    let message = refused.to_string();
    assert!(message.contains("symbol limit exceeded"), "{message}");
    assert!(message.contains("shared by every stream"), "{message}");

    assert_eq!(provider.connections(), 1);
    assert_eq!(
        provider.sent_on(1),
        [
            json!({"action": "subscribe", "trades": ["AAPL"]}),
            json!({"action": "subscribe", "quotes": ["AAPL"]}),
        ],
        "a refused subscribe added nothing, so nothing is unsubscribed",
    );

    provider.push(json!([trade("AAPL", 3)]));
    assert_eq!(next_event(&mut trading).await.kind.id, "3");
}

#[tokio::test]
#[serial]
async fn a_detach_releases_only_what_no_other_stream_holds_and_the_last_closes_the_socket() {
    let provider = Provider::start(&EQUITIES_URL, Auth::Accept, 30).await;
    let subscriber = subscriber();

    let both = trade_stream(&subscriber.clone(), &[trades("aapl"), trades("msft")])
        .await
        .unwrap();
    let apple = trade_stream(&subscriber, &[trades("aapl")]).await.unwrap();

    drop(both);
    provider
        .until("the unsubscribe", |provider| provider.sent_on(1).len() == 2)
        .await;
    assert_eq!(
        provider.sent_on(1)[1],
        json!({"action": "unsubscribe", "trades": ["MSFT"]}),
    );

    drop(apple);
    provider
        .until("the socket to close", |provider| provider.closed() == 1)
        .await;
    assert_eq!(
        provider.sent_on(1).len(),
        2,
        "the last detach closes the socket rather than unsubscribing",
    );
}

/// Every stream on a lost socket ends; the first to re-initialise reconnects and re-subscribes
/// everything the lost socket held in one subscribe, and the rest join that reconnect.
#[tokio::test]
#[serial]
async fn a_lost_connection_ends_every_stream_and_one_reconnect_serves_them_all() {
    let provider = Provider::start(&EQUITIES_URL, Auth::Accept, 30).await;
    let subscriber = subscriber();

    let mut trading = trade_stream(&subscriber.clone(), &[trades("aapl")])
        .await
        .unwrap();
    let mut quoting = quote_stream(&subscriber, &[quotes("msft")]).await.unwrap();

    provider.drop_connection();
    ended(&mut trading).await;
    ended(&mut quoting).await;
    drop((trading, quoting));

    let mut trading = trade_stream(&subscriber.clone(), &[trades("aapl")])
        .await
        .unwrap();
    assert_eq!(provider.connections(), 2);

    let resubscribed = provider.sent_on(2);
    assert_eq!(resubscribed.len(), 1, "{resubscribed:?}");
    assert_eq!(resubscribed[0]["trades"], json!(["AAPL"]));
    assert_eq!(resubscribed[0]["quotes"], json!(["MSFT"]));

    // Sent before the quote stream re-attaches: held for it, not lost.
    provider.push(json!([trade("AAPL", 4), quote("MSFT")]));

    let mut quoting = quote_stream(&subscriber, &[quotes("msft")]).await.unwrap();
    assert_eq!(provider.connections(), 2);
    assert_eq!(provider.sent_on(2).len(), 1, "re-attaching sends nothing");

    assert_eq!(next_event(&mut trading).await.kind.id, "4");
    assert_eq!(
        next_event(&mut quoting).await.instrument,
        quotes("msft").instrument
    );
}

#[tokio::test]
#[serial]
async fn each_feed_gets_a_connection_of_its_own() {
    let equities = Provider::start(&EQUITIES_URL, Auth::Accept, 30).await;
    let crypto = Provider::start(&CRYPTO_URL, Auth::Accept, 30).await;
    let subscriber = subscriber();

    let _equity = trade_stream(&subscriber.clone(), &[trades("aapl")])
        .await
        .unwrap();
    let _coin = trade_stream(&subscriber, &[crypto_trades("btc")])
        .await
        .unwrap();

    assert_eq!((equities.connections(), crypto.connections()), (1, 1));
    assert_eq!(
        crypto.sent_on(1),
        [json!({"action": "subscribe", "trades": ["BTC/USD"]})],
    );
}

/// A separately built subscriber — or another process — holding the feed's one connection is
/// reported with what to do about it.
#[tokio::test]
#[serial]
async fn a_refused_connection_says_the_feed_allows_one_and_how_to_share_it() {
    let _provider = Provider::start(&EQUITIES_URL, Auth::ConnectionLimit, 30).await;

    let refused = trade_stream(&subscriber(), &[trades("aapl")])
        .await
        .unwrap_err();
    let message = refused.to_string();

    assert!(message.contains("connection limit exceeded"), "{message}");
    assert!(message.contains("clones of one subscriber"), "{message}");
}

/// A subscribe the provider answers without registering the pair fails once the subscription
/// timeout passes — and, since what the provider holds is then unknown, unsubscribes what it sent
/// rather than leaving it to count against the connection's cap.
#[tokio::test]
#[serial]
async fn an_unconfirmed_subscribe_times_out_and_releases_what_it_sent() {
    let provider = Provider::start_ignoring(&EQUITIES_URL, Auth::Accept, 30, &["TSLA"]).await;
    let subscriber = subscriber();

    let _apple = trade_stream(&subscriber.clone(), &[trades("aapl")])
        .await
        .unwrap();

    let failed = trade_stream(&subscriber, &[trades("tsla")])
        .await
        .unwrap_err();
    let message = failed.to_string();
    assert!(message.contains("timeout"), "{message}");
    assert!(
        message.contains("trades TSLA"),
        "names what went unconfirmed: {message}"
    );

    provider
        .until("the unsubscribe", |provider| provider.sent_on(1).len() == 3)
        .await;
    assert_eq!(
        provider.sent_on(1)[2],
        json!({"action": "unsubscribe", "trades": ["TSLA"]}),
    );
    assert_eq!(provider.connections(), 1);
}
