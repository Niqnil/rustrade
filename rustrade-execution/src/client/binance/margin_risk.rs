//! Binance margin account notices, and the Risk Data Stream that carries them.
//!
//! Binance reports a cross-margin account's margin-level status as `MARGIN_LEVEL_STATUS_CHANGE`.
//! Its own SDKs read that event from the Risk Data Stream (`wss://margin-stream.binance.com`, keyed
//! by a listen key from `POST /sapi/v1/margin/listen-key`), a stream separate from the
//! `userListenToken` user-data stream the margin client reads fills from. Whether the user-data
//! stream carries it too is not documented, so a cross account stream reads both, and the shared
//! dedup cache delivers a notice sent on both once (see [`DedupEventKind::Notice`]).
//!
//! The Risk Data Stream is a side channel: it never ends the account stream. A failure to get a
//! listen key or to connect is logged and retried with backoff, without limit, until the account
//! stream ends.
//!
//! [`DedupEventKind::Notice`]: crate::client::dedup::DedupEventKind::Notice

use super::shared::{
    CONNECT_TIMEOUT_SECS, RateLimitTracker, RequestKind, UnhandledEvents, frame_excerpt,
    log_unhandled_event, log_unrecognised_frame, response_decode_error, rest_call_with_retry,
};
use crate::{
    AccountEventKind, UnindexedAccountEvent,
    client::dedup::{SharedDedupCache, dedup_key_from_event, is_duplicate},
    notice::{AccountNotice, NoticeKind},
};
use binance_sdk::margin_trading::{
    rest_api::{KeepaliveUserDataStreamParams, RestApi},
    websocket_streams::{MarginLevelStatusChange, UserLiabilityChange},
};
use chrono::{TimeZone, Utc};
use futures::{SinkExt as _, StreamExt as _};
use rust_decimal::Decimal;
use rustrade_instrument::{exchange::ExchangeId, instrument::name::InstrumentNameExchange};
use rustrade_integration::protocol::websocket::{WebSocket, WsMessage, connect};
use smol_str::SmolStr;
use std::{
    future::Future,
    str::FromStr,
    sync::{Arc, atomic::AtomicU64},
    time::Duration,
};
use tokio::{sync::mpsc, time::Instant};
use tracing::{debug, info, trace, warn};

/// Base URL of Binance's margin Risk Data Stream, as binance-sdk names it
/// (`MARGIN_TRADING_RISK_WS_STREAMS_PROD_URL`).
pub(super) const RISK_STREAM_URL: &str =
    binance_sdk::common::constants::MARGIN_TRADING_RISK_WS_STREAMS_PROD_URL;

/// How often the listen key is kept alive. Binance expires one 60 minutes after its last
/// keepalive and recommends one every 30.
const LISTEN_KEY_KEEPALIVE: Duration = Duration::from_secs(30 * 60);
/// How often the client pings the socket, to learn that it is still up between the rare events.
const PING_INTERVAL: Duration = Duration::from_secs(60);
/// How long the socket may stay silent, pongs included, before it is taken as dead.
const IDLE_TIMEOUT: Duration = Duration::from_secs(3 * 60);
/// The first and the longest wait between attempts after a failure.
const RETRY_INITIAL: Duration = Duration::from_secs(1);
const RETRY_MAX: Duration = Duration::from_secs(5 * 60);

/// The listen-key requests the Risk Data Stream needs, so a test can stand in for the venue.
pub(super) trait RiskListenKeys: Send + Sync + 'static {
    /// Get a listen key. Binance returns the account's current one while it is valid.
    fn start(&self) -> impl Future<Output = Result<String, String>> + Send;
    /// Extend the listen key's validity.
    fn keepalive(&self, listen_key: &str) -> impl Future<Output = Result<(), String>> + Send;
}

/// [`RiskListenKeys`] over `/sapi/v1/margin/listen-key`, through binance-sdk's bindings.
///
/// Both requests are weight 1 and go through the client's rate limiter as queries. The key is not
/// closed when the stream ends (`DELETE` weighs 3,000); it lapses 60 minutes after the last
/// keepalive.
pub(super) struct SapiRiskListenKeys {
    pub(super) rest: Arc<RestApi>,
    pub(super) rate_limiter: Arc<RateLimitTracker>,
}

impl RiskListenKeys for SapiRiskListenKeys {
    async fn start(&self) -> Result<String, String> {
        let response =
            rest_call_with_retry(&self.rest, &self.rate_limiter, RequestKind::Query, |rest| {
                Box::pin(async move { rest.start_user_data_stream().await })
            })
            .await
            .map_err(|e| e.to_string())?;
        let data = response
            .data()
            .await
            .map_err(|e| response_decode_error(e).to_string())?;
        data.listen_key
            .ok_or_else(|| "listen-key response carried no listenKey".to_owned())
    }

    async fn keepalive(&self, listen_key: &str) -> Result<(), String> {
        let listen_key = listen_key.to_owned();
        rest_call_with_retry(&self.rest, &self.rate_limiter, RequestKind::Query, |rest| {
            let listen_key = listen_key.clone();
            Box::pin(async move {
                let params = KeepaliveUserDataStreamParams::builder(listen_key).build()?;
                rest.keepalive_user_data_stream(params).await
            })
        })
        .await
        .map(drop)
        .map_err(|e| e.to_string())
    }
}

/// Read the Risk Data Stream at `base_url` into `tx` until `tx` closes, reconnecting after any
/// failure. Returns only once the consumer has dropped the stream.
///
/// Each notice passes `dedup` before it is sent, as the user-data stream's do.
pub(super) async fn run_risk_stream(
    keys: impl RiskListenKeys,
    base_url: String,
    tx: mpsc::UnboundedSender<UnindexedAccountEvent>,
    dedup: SharedDedupCache,
) {
    let mut failures: u32 = 0;
    loop {
        let error = match risk_session(&keys, &base_url, &tx, &dedup).await {
            SessionEnd::ConsumerDropped => {
                debug!("BinanceMargin risk data stream: account stream dropped, stopping");
                return;
            }
            SessionEnd::Retry { error, connected } => {
                // A session that connected was not a failure to connect, so it starts the
                // backoff again.
                if connected {
                    failures = 0;
                }
                error
            }
        };
        let delay = retry_delay(failures);
        failures = failures.saturating_add(1);
        warn!(
            %error,
            retry_in_secs = delay.as_secs(),
            "BinanceMargin risk data stream down, margin notices arrive only if the user-data \
             stream carries them until it reconnects",
        );
        tokio::select! {
            () = tx.closed() => return,
            () = tokio::time::sleep(delay) => {}
        }
    }
}

/// The wait before the next attempt after `failures` consecutive failed ones.
fn retry_delay(failures: u32) -> Duration {
    RETRY_INITIAL
        .saturating_mul(2u32.saturating_pow(failures.min(16)))
        .min(RETRY_MAX)
}

enum SessionEnd {
    ConsumerDropped,
    /// The session ended. `connected` says whether it got as far as a confirmed subscription.
    Retry {
        error: String,
        connected: bool,
    },
}

/// One connection: get a listen key, connect, subscribe, then forward notices until the socket,
/// the key or the consumer goes.
async fn risk_session(
    keys: &impl RiskListenKeys,
    base_url: &str,
    tx: &mpsc::UnboundedSender<UnindexedAccountEvent>,
    dedup: &SharedDedupCache,
) -> SessionEnd {
    let retry = |error: String, connected: bool| SessionEnd::Retry { error, connected };

    let listen_key = match keys.start().await {
        Ok(key) => key,
        Err(e) => return retry(format!("listen key request failed: {e}"), false),
    };
    let (mut ws, early) = match connect_and_subscribe(base_url, &listen_key).await {
        Ok(subscribed) => subscribed,
        Err(e) => return retry(e, false),
    };
    info!("BinanceMargin risk data stream connected and subscribed");

    let start = Instant::now();
    let mut keepalive =
        tokio::time::interval_at(start + LISTEN_KEY_KEEPALIVE, LISTEN_KEY_KEEPALIVE);
    let mut ping = tokio::time::interval_at(start + PING_INTERVAL, PING_INTERVAL);
    let mut last_frame = start;
    let mut buf = Vec::with_capacity(1);

    for text in early {
        if let RiskFrame::KeyExpired = convert_risk_frame(&text, &mut buf) {
            let _ = ws.close(None).await;
            return retry("listen key expired".to_owned(), true);
        }
        if !forward(&mut buf, tx, dedup) {
            let _ = ws.close(None).await;
            return SessionEnd::ConsumerDropped;
        }
    }

    let end = loop {
        tokio::select! {
            // A dropped consumer ends the session at once, whatever else is ready.
            biased;
            () = tx.closed() => break SessionEnd::ConsumerDropped,
            message = ws.next() => {
                last_frame = Instant::now();
                match message {
                    Some(Ok(WsMessage::Text(text))) => {
                        if let RiskFrame::KeyExpired = convert_risk_frame(&text, &mut buf) {
                            break retry("listen key expired".to_owned(), true);
                        }
                        if !forward(&mut buf, tx, dedup) {
                            break SessionEnd::ConsumerDropped;
                        }
                    }
                    Some(Ok(WsMessage::Close(frame))) => {
                        break retry(format!("closed by the venue: {frame:?}"), true);
                    }
                    // tokio-tungstenite answers a ping itself; pongs and binary frames carry
                    // nothing here but show the socket is alive.
                    Some(Ok(_)) => {}
                    Some(Err(e)) => break retry(format!("socket error: {e}"), true),
                    None => break retry("socket ended".to_owned(), true),
                }
            }
            _ = keepalive.tick() => {
                // A failed keepalive is not fatal yet: the key has 30 minutes left, and the venue
                // sends listenKeyExpired if it lapses.
                if let Err(e) = keys.keepalive(&listen_key).await {
                    warn!(error = %e, "BinanceMargin risk data stream listen-key keepalive failed");
                }
            }
            _ = ping.tick() => {
                if last_frame.elapsed() > IDLE_TIMEOUT {
                    break retry(
                        format!("no frame for {}s, pongs included", IDLE_TIMEOUT.as_secs()),
                        true,
                    );
                }
                if let Err(e) = ws.send(WsMessage::Ping(Default::default())).await {
                    break retry(format!("ping failed: {e}"), true);
                }
            }
        }
    };
    // Best effort: the session is over either way.
    let _ = ws.close(None).await;
    end
}

/// Connect to the combined-stream endpoint and subscribe `listen_key`, as binance-sdk's
/// `risk_data` does, waiting for the venue to confirm the subscription.
///
/// The key goes in the `SUBSCRIBE` frame rather than the URL, which the WebSocket layer logs at
/// `debug` when it connects.
///
/// Returns the socket and any text frames that arrived before the acknowledgement, for the
/// session to handle first, so an event the venue sends that early is not lost.
async fn connect_and_subscribe(
    base_url: &str,
    listen_key: &str,
) -> Result<(WebSocket, Vec<String>), String> {
    const SUBSCRIBE_ID: u64 = 1;
    let timeout = Duration::from_secs(CONNECT_TIMEOUT_SECS);

    let mut ws = tokio::time::timeout(timeout, connect(format!("{base_url}/stream?streams=")))
        .await
        .map_err(|_| format!("connect timed out after {CONNECT_TIMEOUT_SECS}s"))?
        .map_err(|e| format!("connect failed: {e}"))?;

    let subscribe = serde_json::json!({
        "method": "SUBSCRIBE",
        "params": [listen_key],
        "id": SUBSCRIBE_ID,
    });
    ws.send(WsMessage::Text(subscribe.to_string().into()))
        .await
        .map_err(|e| format!("subscribe send failed: {e}"))?;

    let mut early = Vec::new();
    let ack = async {
        while let Some(message) = ws.next().await {
            let text = match message {
                Ok(WsMessage::Text(text)) => text,
                Ok(WsMessage::Close(frame)) => {
                    return Err(format!("closed while subscribing: {frame:?}"));
                }
                Ok(_) => continue,
                Err(e) => return Err(format!("socket error while subscribing: {e}")),
            };
            let reply = serde_json::from_str::<serde_json::Value>(&text).ok();
            let is_ack = reply
                .as_ref()
                .and_then(|reply| reply.get("id"))
                .and_then(serde_json::Value::as_u64)
                == Some(SUBSCRIBE_ID);
            let Some(reply) = reply.filter(|_| is_ack) else {
                // Not the acknowledgement: kept for the session, which handles it like any frame.
                early.push(text.to_string());
                continue;
            };
            return match reply.get("error") {
                None => Ok(()),
                Some(error) => Err(format!("subscribe refused: {error}")),
            };
        }
        Err("socket ended while subscribing".to_owned())
    };
    tokio::time::timeout(timeout, ack)
        .await
        .map_err(|_| format!("no subscribe ack within {CONNECT_TIMEOUT_SECS}s"))??;
    Ok((ws, early))
}

/// Send what `buf` holds through `dedup`, draining it. Returns `false` once the consumer has gone.
fn forward(
    buf: &mut Vec<UnindexedAccountEvent>,
    tx: &mpsc::UnboundedSender<UnindexedAccountEvent>,
    dedup: &SharedDedupCache,
) -> bool {
    for event in buf.drain(..) {
        if let Some(key) = dedup_key_from_event(&event)
            && is_duplicate(dedup, key)
        {
            debug!("BinanceMargin risk data stream: notice already delivered, skipping");
            continue;
        }
        if tx.send(event).is_err() {
            return false;
        }
    }
    true
}

/// What a Risk Data Stream frame asks of the session.
#[derive(Debug, PartialEq, Eq)]
enum RiskFrame {
    Continue,
    /// The listen key lapsed; the session must get a new one.
    KeyExpired,
}

/// Convert one Risk Data Stream frame, pushing any notice onto `buf`.
///
/// Frames come wrapped as `{ "stream": <listen key>, "data": { "e", … } }`; a bare event is read
/// too. Event types with no arm are logged by [`log_unhandled_event`], so a renamed event is seen.
fn convert_risk_frame(frame: &str, buf: &mut Vec<UnindexedAccountEvent>) -> RiskFrame {
    static UNRECOGNISED: AtomicU64 = AtomicU64::new(0);
    static UNHANDLED: UnhandledEvents = UnhandledEvents::new();

    let Ok(value) = serde_json::from_str::<serde_json::Value>(frame) else {
        log_unrecognised_frame("BinanceMargin risk", &UNRECOGNISED, frame);
        return RiskFrame::Continue;
    };
    let event = value.get("data").unwrap_or(&value);
    let Some(event_type) = event.get("e").and_then(serde_json::Value::as_str) else {
        if value.get("id").is_some() {
            // A reply to a request, such as a late subscribe ack.
            trace!("BinanceMargin risk data stream: ignoring a request reply");
        } else {
            log_unrecognised_frame("BinanceMargin risk", &UNRECOGNISED, frame);
        }
        return RiskFrame::Continue;
    };

    match event_type {
        "MARGIN_LEVEL_STATUS_CHANGE" => {
            match serde_json::from_value::<MarginLevelStatusChange>(event.clone()) {
                Ok(change) => {
                    log_margin_level_change(&change, "risk data");
                    buf.extend(margin_level_notice(change, None));
                }
                Err(e) => warn!(
                    error = %e,
                    frame = frame_excerpt(frame),
                    "BinanceMargin: undeserializable MARGIN_LEVEL_STATUS_CHANGE on the risk data \
                     stream, dropping"
                ),
            }
        }
        "USER_LIABILITY_CHANGE" => {
            match serde_json::from_value::<UserLiabilityChange>(event.clone()) {
                Ok(change) => log_liability_change(&change, "risk data"),
                Err(e) => warn!(
                    error = %e,
                    frame = frame_excerpt(frame),
                    "BinanceMargin: undeserializable USER_LIABILITY_CHANGE on the risk data \
                     stream, dropping"
                ),
            }
        }
        "listenKeyExpired" => return RiskFrame::KeyExpired,
        other => log_unhandled_event("BinanceMargin risk", &UNHANDLED, other, frame),
    }
    RiskFrame::Continue
}

/// Log that a `MARGIN_LEVEL_STATUS_CHANGE` arrived, naming the stream it arrived on.
///
/// At `info`, before dedup, so each copy is logged: which of Binance's streams sends the event is
/// not documented, and these lines are the record of it.
pub(super) fn log_margin_level_change(change: &MarginLevelStatusChange, stream: &'static str) {
    info!(
        stream,
        status = change.s.as_deref().unwrap_or("?"),
        margin_level = change.l.as_deref().unwrap_or("?"),
        time_exchange_ms = change.e_uppercase,
        "BinanceMargin MARGIN_LEVEL_STATUS_CHANGE received",
    );
}

/// Log a `USER_LIABILITY_CHANGE`, naming the stream it arrived on. It is not applied to balance
/// state: borrowing and repaying is a delta, and debt is refreshed by `account_snapshot`.
pub(super) fn log_liability_change(change: &UserLiabilityChange, stream: &'static str) {
    info!(
        stream,
        asset = change.a.as_deref().unwrap_or("?"),
        kind = change.t.as_deref().unwrap_or("?"),
        principal = change.p.as_deref().unwrap_or("?"),
        interest = change.i.as_deref().unwrap_or("?"),
        "BinanceMargin USER_LIABILITY_CHANGE (observable; not applied to balance state)",
    );
}

/// The [`NoticeKind`] for a Binance margin-level status.
///
/// Binance lists `EXCESSIVE`, `NORMAL`, `MARGIN_CALL`, `PRE_LIQUIDATION` and `FORCE_LIQUIDATION`
/// without defining them. `NORMAL` and `EXCESSIVE` both mean no margin call or liquidation
/// condition holds, so both are [`NoticeKind::MarginRestored`]. Any other status is
/// [`NoticeKind::Other`].
pub(super) fn margin_notice_kind(status: &str) -> NoticeKind {
    match status {
        "MARGIN_CALL" => NoticeKind::MarginCall,
        "PRE_LIQUIDATION" => NoticeKind::LiquidationWarning,
        "FORCE_LIQUIDATION" => NoticeKind::Liquidation,
        "NORMAL" | "EXCESSIVE" => NoticeKind::MarginRestored,
        _ => NoticeKind::Other,
    }
}

/// The notice for a `MARGIN_LEVEL_STATUS_CHANGE`, attributed to `instrument`.
///
/// `None`, logged at `warn`, for a change with no status, since it says nothing to classify. A
/// change with no time is stamped now, and one whose margin level cannot be read carries none,
/// both logged at `warn`: the notice matters more than either.
pub(super) fn margin_level_notice(
    change: MarginLevelStatusChange,
    instrument: Option<InstrumentNameExchange>,
) -> Option<UnindexedAccountEvent> {
    let Some(status) = change.s else {
        warn!(
            margin_level = change.l.as_deref().unwrap_or("?"),
            "BinanceMargin MARGIN_LEVEL_STATUS_CHANGE without a status (s), dropping"
        );
        return None;
    };
    let time_exchange = match change
        .e_uppercase
        .and_then(|ms| Utc.timestamp_millis_opt(ms).single())
    {
        Some(time) => time,
        None => {
            warn!(
                %status,
                "BinanceMargin MARGIN_LEVEL_STATUS_CHANGE without a readable time (E), using now"
            );
            Utc::now()
        }
    };
    let margin_level = change.l.as_deref().and_then(|level| {
        Decimal::from_str(level)
            .inspect_err(|e| {
                warn!(
                    %status,
                    margin_level = level,
                    error = %e,
                    "BinanceMargin MARGIN_LEVEL_STATUS_CHANGE margin level (l) unreadable, \
                     delivering the notice without it"
                );
            })
            .ok()
    });

    Some(UnindexedAccountEvent::new(
        ExchangeId::BinanceMargin,
        AccountEventKind::Notice(AccountNotice::new(
            margin_notice_kind(&status),
            instrument,
            time_exchange,
            margin_level,
            SmolStr::new(status),
        )),
    ))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use crate::client::dedup::new_dedup_cache;
    use std::sync::Mutex;
    use tokio_tungstenite::tungstenite::Message;

    fn margin_level(status: &str, time_ms: i64) -> serde_json::Value {
        serde_json::json!({
            "e": "MARGIN_LEVEL_STATUS_CHANGE", "E": time_ms, "l": "1.08", "s": status,
        })
    }

    /// A frame as the combined-stream endpoint wraps it.
    fn wrapped(event: serde_json::Value) -> String {
        serde_json::json!({ "stream": "listen-key", "data": event }).to_string()
    }

    fn notice_of(event: &UnindexedAccountEvent) -> &AccountNotice<InstrumentNameExchange> {
        match &event.kind {
            AccountEventKind::Notice(notice) => notice,
            other => panic!("expected a notice, got {other:?}"),
        }
    }

    #[test]
    fn a_margin_level_change_becomes_an_unattributed_notice_wrapped_or_bare() {
        for frame in [
            wrapped(margin_level("PRE_LIQUIDATION", 1_700_000_000_000)),
            margin_level("PRE_LIQUIDATION", 1_700_000_000_000).to_string(),
        ] {
            let mut buf = Vec::new();
            assert_eq!(convert_risk_frame(&frame, &mut buf), RiskFrame::Continue);
            let [event] = buf.as_slice() else {
                panic!("expected one event, got {buf:?}")
            };
            assert_eq!(event.exchange, ExchangeId::BinanceMargin);
            assert_eq!(
                notice_of(event),
                &AccountNotice::new(
                    NoticeKind::LiquidationWarning,
                    None,
                    Utc.timestamp_millis_opt(1_700_000_000_000).unwrap(),
                    Some(Decimal::new(108, 2)),
                    SmolStr::new("PRE_LIQUIDATION"),
                ),
            );
        }
    }

    #[test]
    fn other_frames_forward_nothing_and_an_expired_key_ends_the_session() {
        let liability = serde_json::json!({
            "e": "USER_LIABILITY_CHANGE", "E": 1_700_000_000_000_i64, "a": "BTC",
            "t": "BORROW", "p": "1", "i": "0",
        });
        for (frame, expected) in [
            (wrapped(liability), RiskFrame::Continue),
            (
                serde_json::json!({ "result": null, "id": 1 }).to_string(),
                RiskFrame::Continue,
            ),
            (
                wrapped(serde_json::json!({ "e": "someFutureEvent", "E": 1 })),
                RiskFrame::Continue,
            ),
            ("not json".to_owned(), RiskFrame::Continue),
            (
                wrapped(serde_json::json!({ "e": "listenKeyExpired", "E": 1 })),
                RiskFrame::KeyExpired,
            ),
        ] {
            let mut buf = Vec::new();
            assert_eq!(convert_risk_frame(&frame, &mut buf), expected, "{frame}");
            assert!(buf.is_empty(), "{frame}");
        }
    }

    #[test]
    fn retries_back_off_to_five_minutes() {
        let delays: Vec<u64> = (0..12).map(|n| retry_delay(n).as_secs()).collect();
        assert_eq!(delays, [1, 2, 4, 8, 16, 32, 64, 128, 256, 300, 300, 300]);
        assert_eq!(retry_delay(u32::MAX), RETRY_MAX);
    }

    /// Hands out `key-1`, `key-2`, … and records keepalives.
    #[derive(Default)]
    struct FakeKeys {
        issued: Mutex<u32>,
    }

    impl RiskListenKeys for Arc<FakeKeys> {
        async fn start(&self) -> Result<String, String> {
            let mut issued = self.issued.lock().unwrap();
            *issued += 1;
            Ok(format!("key-{issued}"))
        }

        async fn keepalive(&self, _listen_key: &str) -> Result<(), String> {
            Ok(())
        }
    }

    /// Accept one connection, check it subscribes `listen_key`, send `before_ack`, acknowledge,
    /// then send `frames` and keep the socket open until the client closes it.
    async fn serve(
        listener: &tokio::net::TcpListener,
        listen_key: &str,
        before_ack: Vec<String>,
        frames: Vec<String>,
    ) {
        let (stream, _) = listener.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
        let Some(Ok(Message::Text(subscribe))) = ws.next().await else {
            panic!("expected a SUBSCRIBE frame")
        };
        let subscribe: serde_json::Value = serde_json::from_str(&subscribe).unwrap();
        assert_eq!(subscribe["method"], "SUBSCRIBE");
        assert_eq!(subscribe["params"], serde_json::json!([listen_key]));
        for frame in before_ack {
            ws.send(Message::Text(frame.into())).await.unwrap();
        }
        let ack = serde_json::json!({ "result": null, "id": subscribe["id"] });
        ws.send(Message::Text(ack.to_string().into()))
            .await
            .unwrap();
        for frame in frames {
            ws.send(Message::Text(frame.into())).await.unwrap();
        }
        // Drain until the client goes, so the socket stays up meanwhile.
        while let Some(Ok(_)) = ws.next().await {}
    }

    /// A session forwards notices through the shared cache, gets a new key and reconnects when the
    /// venue expires the old one, and stops when the account stream is dropped.
    #[tokio::test]
    async fn the_stream_forwards_notices_once_and_reconnects_on_an_expired_key() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base_url = format!("ws://{}", listener.local_addr().unwrap());
        let keys = Arc::new(FakeKeys::default());
        let (tx, mut rx) = mpsc::unbounded_channel();
        let task = tokio::spawn(run_risk_stream(
            Arc::clone(&keys),
            base_url,
            tx,
            new_dedup_cache(),
        ));

        let call = wrapped(margin_level("MARGIN_CALL", 1_700_000_000_000));
        let expired = wrapped(serde_json::json!({ "e": "listenKeyExpired", "E": 1 }));
        serve(&listener, "key-1", vec![], vec![call.clone(), expired]).await;
        let restored = wrapped(margin_level("NORMAL", 1_700_000_060_000));
        // The same MARGIN_CALL again on the new connection is a duplicate.
        let server = async { serve(&listener, "key-2", vec![], vec![call, restored]).await };
        let received = async {
            let first = rx.recv().await.unwrap();
            let second = rx.recv().await.unwrap();
            (first, second)
        };
        let (first, second) = tokio::time::timeout(Duration::from_secs(10), async {
            tokio::select! {
                () = server => panic!("the server stopped before both notices arrived"),
                received = received => received,
            }
        })
        .await
        .unwrap();
        assert_eq!(notice_of(&first).kind, NoticeKind::MarginCall);
        assert_eq!(notice_of(&second).kind, NoticeKind::MarginRestored);
        assert_eq!(*keys.issued.lock().unwrap(), 2);

        drop(rx);
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .expect("the stream stops once the consumer is gone")
            .unwrap();
    }

    /// A refused subscription is a failed attempt, retried with a fresh key.
    #[tokio::test]
    async fn a_refused_subscription_is_retried() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base_url = format!("ws://{}", listener.local_addr().unwrap());
        let keys = Arc::new(FakeKeys::default());
        let (tx, mut rx) = mpsc::unbounded_channel();
        let task = tokio::spawn(run_risk_stream(
            Arc::clone(&keys),
            base_url,
            tx,
            new_dedup_cache(),
        ));

        // Refuse the first subscription.
        let (stream, _) = listener.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
        let Some(Ok(Message::Text(subscribe))) = ws.next().await else {
            panic!("expected a SUBSCRIBE frame")
        };
        let subscribe: serde_json::Value = serde_json::from_str(&subscribe).unwrap();
        let refusal = serde_json::json!({
            "error": { "code": 2, "msg": "Invalid request" }, "id": subscribe["id"],
        });
        ws.send(Message::Text(refusal.to_string().into()))
            .await
            .unwrap();

        let liquidation = wrapped(margin_level("FORCE_LIQUIDATION", 1_700_000_000_000));
        let server = async { serve(&listener, "key-2", vec![], vec![liquidation]).await };
        let event = tokio::time::timeout(Duration::from_secs(10), async {
            tokio::select! {
                () = server => panic!("the server stopped before the notice arrived"),
                event = rx.recv() => event.unwrap(),
            }
        })
        .await
        .unwrap();
        assert_eq!(notice_of(&event).kind, NoticeKind::Liquidation);

        drop(rx);
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .unwrap()
            .unwrap();
    }

    /// A notice the venue sends before acknowledging the subscription is still delivered.
    #[tokio::test]
    async fn a_notice_before_the_subscribe_ack_is_delivered() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base_url = format!("ws://{}", listener.local_addr().unwrap());
        let (tx, mut rx) = mpsc::unbounded_channel();
        let task = tokio::spawn(run_risk_stream(
            Arc::new(FakeKeys::default()),
            base_url,
            tx,
            new_dedup_cache(),
        ));

        let early = wrapped(margin_level("MARGIN_CALL", 1_700_000_000_000));
        let server = async { serve(&listener, "key-1", vec![early], vec![]).await };
        let event = tokio::time::timeout(Duration::from_secs(10), async {
            tokio::select! {
                () = server => panic!("the server stopped before the notice arrived"),
                event = rx.recv() => event.unwrap(),
            }
        })
        .await
        .unwrap();
        assert_eq!(notice_of(&event).kind, NoticeKind::MarginCall);

        drop(rx);
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .unwrap()
            .unwrap();
    }
}
