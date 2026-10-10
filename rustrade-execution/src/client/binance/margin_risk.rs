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
use futures::{FutureExt as _, Sink, SinkExt as _, StreamExt as _};
use rust_decimal::Decimal;
use rustrade_instrument::{exchange::ExchangeId, instrument::name::InstrumentNameExchange};
use rustrade_integration::protocol::websocket::{WebSocket, WsMessage, connect};
use serde::Deserialize as _;
use smol_str::SmolStr;
use std::{
    borrow::Cow,
    fmt::Display,
    future::Future,
    panic::AssertUnwindSafe,
    str::FromStr,
    sync::{Arc, atomic::AtomicU64},
    time::Duration,
};
use tokio::{
    sync::mpsc,
    time::{Instant, MissedTickBehavior},
};
use tracing::{debug, error, info, trace, warn};

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
/// How long a subscribed session must last for its end to restart the backoff.
const HEALTHY_SESSION: Duration = RETRY_MAX;
/// The longest a listen-key request may take, rate-limit waits included.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(60);
/// The longest a session waits to send a ping or close its socket.
const SOCKET_WRITE_TIMEOUT: Duration = Duration::from_secs(5);

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

/// The Risk Data Stream's task, aborted when this is dropped.
///
/// It runs in a task of its own, apart from the user-data manager, so that nothing in it, a panic
/// included, can end the account stream: the account stream's task holds this and drops it when
/// it ends, or when it is aborted because the consumer dropped the stream. A panic stops the Risk
/// Data Stream for the rest of the account stream's life, logged at `error`; it is not restarted.
pub(super) struct RiskStreamTask(tokio::task::JoinHandle<()>);

impl RiskStreamTask {
    /// Spawn [`run_risk_stream`].
    pub(super) fn spawn(
        keys: impl RiskListenKeys,
        base_url: String,
        tx: mpsc::UnboundedSender<UnindexedAccountEvent>,
        dedup: SharedDedupCache,
    ) -> Self {
        Self(tokio::spawn(async move {
            let run = AssertUnwindSafe(run_risk_stream(keys, base_url, tx, dedup));
            if let Err(payload) = run.catch_unwind().await {
                let panic = payload
                    .downcast_ref::<&str>()
                    .copied()
                    .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
                    .unwrap_or("(not a string)");
                error!(
                    panic,
                    "BinanceMargin risk data stream panicked and has stopped: margin notices now \
                     arrive only if the user-data stream carries them"
                );
            }
        }))
    }
}

impl Drop for RiskStreamTask {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Read the Risk Data Stream at `base_url` into `tx` until `tx` closes, reconnecting after any
/// failure. Returns only once the consumer has dropped the stream.
///
/// Each notice passes `dedup` before it is sent, as the user-data stream's do.
async fn run_risk_stream(
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
            SessionEnd::Retry { error, healthy } => {
                // Only a session that stayed up starts the backoff again: one that the venue
                // ends straight after the subscribe keeps backing off, rather than reconnecting
                // every second.
                if healthy {
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
    /// The session ended. `healthy` says whether it stayed subscribed for at least
    /// [`HEALTHY_SESSION`].
    Retry {
        error: String,
        healthy: bool,
    },
}

/// Why a subscribed session's loop stopped.
enum Stop {
    ConsumerDropped,
    Retry(String),
}

/// One connection: get a listen key, connect, subscribe, then forward notices until the socket,
/// the key or the consumer goes.
async fn risk_session(
    keys: &impl RiskListenKeys,
    base_url: &str,
    tx: &mpsc::UnboundedSender<UnindexedAccountEvent>,
    dedup: &SharedDedupCache,
) -> SessionEnd {
    let failed = |error: String| SessionEnd::Retry {
        error,
        healthy: false,
    };

    // Raced against the consumer, since the request can wait out a rate limit.
    let listen_key = tokio::select! {
        () = tx.closed() => return SessionEnd::ConsumerDropped,
        key = tokio::time::timeout(REQUEST_TIMEOUT, keys.start()) => match key {
            Ok(Ok(key)) => key,
            Ok(Err(e)) => return failed(format!("listen key request failed: {e}")),
            Err(_) => return failed(format!(
                "listen key request timed out after {}s",
                REQUEST_TIMEOUT.as_secs()
            )),
        },
    };
    // Every error from here on may quote the venue (a subscribe refusal, a close reason) and so
    // the key, and each one is logged when the stream retries.
    let masked = |error: String| redact_listen_key(&error, &listen_key).into_owned();
    let (mut ws, early) = match connect_and_subscribe(base_url, &listen_key).await {
        Ok(subscribed) => subscribed,
        Err(e) => return failed(masked(e)),
    };
    info!("BinanceMargin risk data stream connected and subscribed");

    let start = Instant::now();
    let stop = forward_session(keys, &listen_key, &mut ws, early, tx, dedup).await;
    // Best effort, and bounded: the session is over either way, and a half-open socket would
    // otherwise hold the close.
    let _ = tokio::time::timeout(SOCKET_WRITE_TIMEOUT, ws.close(None)).await;
    match stop {
        Stop::ConsumerDropped => SessionEnd::ConsumerDropped,
        Stop::Retry(error) => SessionEnd::Retry {
            error: masked(error),
            healthy: start.elapsed() >= HEALTHY_SESSION,
        },
    }
}

/// Forward a subscribed session's notices, `early` frames first, keeping the key alive and the
/// socket checked, until something ends it.
async fn forward_session(
    keys: &impl RiskListenKeys,
    listen_key: &str,
    ws: &mut WebSocket,
    early: Vec<String>,
    tx: &mpsc::UnboundedSender<UnindexedAccountEvent>,
    dedup: &SharedDedupCache,
) -> Stop {
    let mut buf = Vec::with_capacity(1);
    for text in early {
        if let RiskFrame::KeyExpired = convert_risk_frame(&text, listen_key, &mut buf) {
            return Stop::Retry("listen key expired".to_owned());
        }
        if !forward(&mut buf, tx, dedup) {
            return Stop::ConsumerDropped;
        }
    }

    let start = Instant::now();
    let mut keepalive =
        tokio::time::interval_at(start + LISTEN_KEY_KEEPALIVE, LISTEN_KEY_KEEPALIVE);
    let mut ping = tokio::time::interval_at(start + PING_INTERVAL, PING_INTERVAL);
    // After a stall, wait a full period rather than firing the missed ticks back to back.
    keepalive.set_missed_tick_behavior(MissedTickBehavior::Delay);
    ping.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut last_frame = start;

    loop {
        tokio::select! {
            // A dropped consumer ends the session at once, whatever else is ready.
            biased;
            () = tx.closed() => return Stop::ConsumerDropped,
            message = ws.next() => {
                last_frame = Instant::now();
                match message {
                    Some(Ok(WsMessage::Text(text))) => {
                        if let RiskFrame::KeyExpired =
                            convert_risk_frame(&text, listen_key, &mut buf)
                        {
                            return Stop::Retry("listen key expired".to_owned());
                        }
                        if !forward(&mut buf, tx, dedup) {
                            return Stop::ConsumerDropped;
                        }
                    }
                    Some(Ok(WsMessage::Close(frame))) => {
                        return Stop::Retry(format!("closed by the venue: {frame:?}"));
                    }
                    // tokio-tungstenite answers a ping itself; pongs and binary frames carry
                    // nothing here but show the socket is alive.
                    Some(Ok(_)) => {}
                    Some(Err(e)) => return Stop::Retry(format!("socket error: {e}")),
                    None => return Stop::Retry("socket ended".to_owned()),
                }
            }
            _ = keepalive.tick() => {
                // A failed keepalive is not fatal yet: the key has 30 minutes left, and the venue
                // sends listenKeyExpired if it lapses. Bounded, since the socket goes unread
                // meanwhile.
                match tokio::time::timeout(REQUEST_TIMEOUT, keys.keepalive(listen_key)).await {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => warn!(
                        error = %redact_listen_key(&e, listen_key),
                        "BinanceMargin risk data stream listen-key keepalive failed"
                    ),
                    Err(_) => warn!(
                        timeout_secs = REQUEST_TIMEOUT.as_secs(),
                        "BinanceMargin risk data stream listen-key keepalive timed out"
                    ),
                }
            }
            _ = ping.tick() => {
                if last_frame.elapsed() > IDLE_TIMEOUT {
                    return Stop::Retry(format!(
                        "no frame for {}s, pongs included",
                        IDLE_TIMEOUT.as_secs()
                    ));
                }
                if let Err(e) = send_ping(ws).await {
                    return Stop::Retry(e);
                }
            }
        }
    }
}

/// Send a ping, bounded by [`SOCKET_WRITE_TIMEOUT`]: a half-open socket with a full send buffer
/// would otherwise hold the session in the send, past its idle check.
async fn send_ping<S>(sink: &mut S) -> Result<(), String>
where
    S: Sink<WsMessage> + Unpin,
    S::Error: Display,
{
    match tokio::time::timeout(
        SOCKET_WRITE_TIMEOUT,
        sink.send(WsMessage::Ping(Default::default())),
    )
    .await
    {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => Err(format!("ping failed: {e}")),
        Err(_) => Err(format!(
            "ping not sent within {}s",
            SOCKET_WRITE_TIMEOUT.as_secs()
        )),
    }
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
pub(super) enum RiskFrame {
    Continue,
    /// The listen key lapsed; the session must get a new one.
    KeyExpired,
}

/// Convert one Risk Data Stream frame, pushing any notice onto `buf`.
///
/// Frames come wrapped as `{ "stream": <listen key>, "data": { "e", … } }`; a bare event is read
/// too. Event types with no arm are logged by [`log_unhandled_event`], so a renamed event is seen.
///
/// `listen_key` is the key the session subscribed with. A frame that is logged has it masked
/// first, by [`redact_listen_key`].
pub(super) fn convert_risk_frame(
    frame: &str,
    listen_key: &str,
    buf: &mut Vec<UnindexedAccountEvent>,
) -> RiskFrame {
    static UNRECOGNISED: AtomicU64 = AtomicU64::new(0);
    static UNHANDLED: UnhandledEvents = UnhandledEvents::new();

    let logged = || redact_listen_key(frame, listen_key);
    let Ok(value) = serde_json::from_str::<serde_json::Value>(frame) else {
        log_unrecognised_frame("BinanceMargin risk", &UNRECOGNISED, &logged());
        return RiskFrame::Continue;
    };
    let event = value.get("data").unwrap_or(&value);
    let Some(event_type) = event.get("e").and_then(serde_json::Value::as_str) else {
        if value.get("id").is_some() {
            // A reply to a request, such as a late subscribe ack.
            trace!("BinanceMargin risk data stream: ignoring a request reply");
        } else {
            log_unrecognised_frame("BinanceMargin risk", &UNRECOGNISED, &logged());
        }
        return RiskFrame::Continue;
    };

    match event_type {
        "MARGIN_LEVEL_STATUS_CHANGE" => match MarginLevelStatusChange::deserialize(event) {
            Ok(change) => {
                log_margin_level_change(&change, "risk data");
                buf.extend(margin_level_notice(change, None));
            }
            Err(e) => warn!(
                error = %e,
                frame = frame_excerpt(&logged()),
                "BinanceMargin: undeserializable MARGIN_LEVEL_STATUS_CHANGE on the risk data \
                 stream, dropping"
            ),
        },
        "USER_LIABILITY_CHANGE" => match UserLiabilityChange::deserialize(event) {
            Ok(change) => log_liability_change(&change, "risk data"),
            Err(e) => warn!(
                error = %e,
                frame = frame_excerpt(&logged()),
                "BinanceMargin: undeserializable USER_LIABILITY_CHANGE on the risk data \
                 stream, dropping"
            ),
        },
        "listenKeyExpired" => return RiskFrame::KeyExpired,
        other => log_unhandled_event("BinanceMargin risk", &UNHANDLED, other, &logged()),
    }
    RiskFrame::Continue
}

/// `text` with each occurrence of `listen_key` replaced by `<listen key>`, for a log line.
///
/// Anyone holding the key can read the account's Risk Data Stream until it lapses, and every frame
/// names it in its `stream` field, so no frame, venue reply or error that may quote one is logged
/// unmasked. Masking runs before a log helper cuts the text short, so no prefix of the key
/// survives either. It matches the key literally: Binance's keys are alphanumeric, so JSON or URL
/// encoding leaves them unchanged.
fn redact_listen_key<'a>(text: &'a str, listen_key: &str) -> Cow<'a, str> {
    if listen_key.is_empty() || !text.contains(listen_key) {
        Cow::Borrowed(text)
    } else {
        Cow::Owned(text.replace(listen_key, "<listen key>"))
    }
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

    const LISTEN_KEY: &str = "synthetic-listen-key-0123456789";

    /// A frame as the combined-stream endpoint wraps it.
    fn wrapped(event: serde_json::Value) -> String {
        serde_json::json!({ "stream": LISTEN_KEY, "data": event }).to_string()
    }

    /// Run `f`, returning every field of every log event it emitted on this thread, at any level.
    fn logged_fields<T>(f: impl FnOnce() -> T) -> (T, Vec<String>) {
        use tracing_subscriber::layer::SubscriberExt;

        #[derive(Default)]
        struct Fields(Arc<Mutex<Vec<String>>>);

        struct Visit<'a>(&'a mut Vec<String>);

        impl tracing::field::Visit for Visit<'_> {
            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                self.0.push(format!("{}={value:?}", field.name()));
            }
        }

        impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for Fields {
            fn on_event(
                &self,
                event: &tracing::Event<'_>,
                _: tracing_subscriber::layer::Context<'_, S>,
            ) {
                let mut fields = Vec::new();
                event.record(&mut Visit(&mut fields));
                self.0.lock().unwrap().extend(fields);
            }
        }

        let layer = Fields::default();
        let fields = Arc::clone(&layer.0);
        // Thread-local, so other tests running in parallel are not captured.
        let value =
            tracing::subscriber::with_default(tracing_subscriber::registry().with(layer), f);
        let fields = fields.lock().unwrap().clone();
        (value, fields)
    }

    /// Every frame that is logged names the listen key, so each must be logged with it masked:
    /// unrecognised frames, undeserializable events, and event types with no arm.
    #[test]
    fn a_logged_frame_never_shows_the_listen_key() {
        for frame in [
            // An event type no other test uses, so it takes the loud first-of-type path.
            wrapped(serde_json::json!({ "e": "aFrameNamingTheKey", "E": 1 })),
            wrapped(serde_json::json!({ "e": "MARGIN_LEVEL_STATUS_CHANGE", "s": 5 })),
            wrapped(serde_json::json!({ "e": "USER_LIABILITY_CHANGE", "a": 5 })),
            serde_json::json!({ "stream": LISTEN_KEY }).to_string(),
            format!("{LISTEN_KEY} is not json"),
        ] {
            let mut buf = Vec::new();
            let (_, fields) = logged_fields(|| convert_risk_frame(&frame, LISTEN_KEY, &mut buf));
            assert!(
                fields.iter().any(|field| field.contains("<listen key>")),
                "{frame}: no masked frame logged in {fields:?}"
            );
            assert!(
                fields.iter().all(|field| !field.contains(LISTEN_KEY)),
                "{frame}: the key was logged in {fields:?}"
            );
        }
    }

    #[test]
    fn the_listen_key_is_masked_wherever_it_appears_and_text_without_it_is_untouched() {
        let text = format!("{LISTEN_KEY} and {LISTEN_KEY}");
        assert_eq!(
            redact_listen_key(&text, LISTEN_KEY),
            "<listen key> and <listen key>"
        );
        assert!(matches!(
            redact_listen_key("no key here", LISTEN_KEY),
            Cow::Borrowed("no key here")
        ));
        assert!(matches!(
            redact_listen_key("no key here", ""),
            Cow::Borrowed("no key here")
        ));
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
            assert_eq!(
                convert_risk_frame(&frame, LISTEN_KEY, &mut buf),
                RiskFrame::Continue
            );
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
            assert_eq!(
                convert_risk_frame(&frame, LISTEN_KEY, &mut buf),
                expected,
                "{frame}"
            );
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

    /// A session's error, logged when the stream retries, has the key masked whether the venue
    /// quotes it refusing the subscribe or closing the socket.
    #[tokio::test]
    async fn a_session_error_quoting_the_key_is_masked() {
        use tokio_tungstenite::tungstenite::protocol::{CloseFrame, frame::coding::CloseCode};

        for refuse in [true, false] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let base_url = format!("ws://{}", listener.local_addr().unwrap());
            let server = tokio::spawn(async move {
                let (stream, _) = listener.accept().await.unwrap();
                let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
                let Some(Ok(Message::Text(subscribe))) = ws.next().await else {
                    panic!("expected a SUBSCRIBE frame")
                };
                let subscribe: serde_json::Value = serde_json::from_str(&subscribe).unwrap();
                let key = subscribe["params"][0].as_str().unwrap().to_owned();
                if refuse {
                    let refusal = serde_json::json!({
                        "id": subscribe["id"], "error": { "code": 2, "msg": format!("bad {key}") },
                    });
                    ws.send(Message::Text(refusal.to_string().into()))
                        .await
                        .unwrap();
                } else {
                    let ack = serde_json::json!({ "result": null, "id": subscribe["id"] });
                    ws.send(Message::Text(ack.to_string().into()))
                        .await
                        .unwrap();
                    let close = CloseFrame {
                        code: CloseCode::Policy,
                        reason: format!("bad {key}").into(),
                    };
                    ws.send(Message::Close(Some(close))).await.unwrap();
                }
                while let Some(Ok(_)) = ws.next().await {}
                key
            });

            let (tx, _rx) = mpsc::unbounded_channel();
            let end = risk_session(
                &Arc::new(FakeKeys::default()),
                &base_url,
                &tx,
                &new_dedup_cache(),
            )
            .await;
            let SessionEnd::Retry { error, .. } = end else {
                panic!("refuse={refuse}: expected a retry")
            };
            let key = server.await.unwrap();
            assert!(
                error.contains("bad <listen key>"),
                "refuse={refuse}: {error}"
            );
            assert!(!error.contains(&key), "refuse={refuse}: {error}");
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

    /// A listen-key request that never answers, counting how often it is made.
    #[derive(Default)]
    struct HangingKeys {
        starts: std::sync::atomic::AtomicU32,
    }

    impl HangingKeys {
        fn starts(&self) -> u32 {
            self.starts.load(std::sync::atomic::Ordering::Relaxed)
        }
    }

    impl RiskListenKeys for Arc<HangingKeys> {
        async fn start(&self) -> Result<String, String> {
            self.starts
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            std::future::pending().await
        }

        async fn keepalive(&self, _listen_key: &str) -> Result<(), String> {
            Ok(())
        }
    }

    /// A listen-key request that hangs times out and is retried with backoff, and the stream still
    /// stops as soon as the consumer goes, mid-request.
    #[tokio::test(start_paused = true)]
    async fn a_hanging_listen_key_request_times_out_and_does_not_outlive_the_consumer() {
        let (tx, rx) = mpsc::unbounded_channel();
        let keys = Arc::new(HangingKeys::default());
        let dedup = new_dedup_cache();

        // One session alone ends at the request timeout, as a failure to retry.
        let session = risk_session(&keys, "ws://unused", &tx, &dedup);
        let started = Instant::now();
        match session.await {
            SessionEnd::Retry { healthy, .. } => assert!(!healthy),
            SessionEnd::ConsumerDropped => panic!("the consumer is still there"),
        }
        assert_eq!(started.elapsed(), REQUEST_TIMEOUT);

        // The stream retries a timed-out request after the backoff, and stops, mid-request, once
        // the consumer drops.
        let keys = Arc::new(HangingKeys::default());
        let task = tokio::spawn(run_risk_stream(
            Arc::clone(&keys),
            "ws://unused".to_owned(),
            tx,
            dedup,
        ));
        tokio::time::sleep(REQUEST_TIMEOUT - Duration::from_millis(1)).await;
        assert_eq!(keys.starts(), 1, "still waiting on the first request");
        tokio::time::sleep(RETRY_INITIAL + Duration::from_millis(2)).await;
        assert_eq!(keys.starts(), 2, "retried after the first backoff");
        drop(rx);
        tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .expect("the stream stops once the consumer is gone")
            .unwrap();
    }

    /// Dropping the task handle stops the stream, which drops its sender.
    #[tokio::test(start_paused = true)]
    async fn dropping_the_task_handle_stops_the_stream() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let task = RiskStreamTask::spawn(
            Arc::new(HangingKeys::default()),
            "ws://unused".to_owned(),
            tx,
            new_dedup_cache(),
        );
        tokio::time::sleep(Duration::from_secs(1)).await;
        drop(task);
        let ended = tokio::time::timeout(Duration::from_secs(1), rx.recv())
            .await
            .expect("the aborted task drops its sender");
        assert!(ended.is_none());
    }

    /// A session the venue ends straight after the subscribe is not healthy, so the backoff keeps
    /// growing rather than reconnecting every second.
    #[tokio::test]
    async fn a_session_ended_at_once_is_not_healthy() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base_url = format!("ws://{}", listener.local_addr().unwrap());
        let (tx, _rx) = mpsc::unbounded_channel();
        let expired = wrapped(serde_json::json!({ "e": "listenKeyExpired", "E": 1 }));
        let keys = Arc::new(FakeKeys::default());
        let dedup = new_dedup_cache();
        let session = risk_session(&keys, &base_url, &tx, &dedup);
        let server = serve(&listener, "key-1", vec![], vec![expired]);
        let (end, ()) = tokio::join!(session, server);
        match end {
            SessionEnd::Retry { healthy, error } => {
                assert!(!healthy);
                assert_eq!(error, "listen key expired");
            }
            SessionEnd::ConsumerDropped => panic!("the consumer is still there"),
        }
    }

    /// A sink that never accepts a frame, as a socket whose send buffer stays full.
    struct StuckSink;

    impl Sink<WsMessage> for StuckSink {
        type Error = std::convert::Infallible;

        fn poll_ready(
            self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), Self::Error>> {
            std::task::Poll::Pending
        }

        fn start_send(self: std::pin::Pin<&mut Self>, _: WsMessage) -> Result<(), Self::Error> {
            Ok(())
        }

        fn poll_flush(
            self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), Self::Error>> {
            std::task::Poll::Pending
        }

        fn poll_close(
            self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), Self::Error>> {
            std::task::Poll::Pending
        }
    }

    /// A ping a stuck socket never takes fails after the write timeout, rather than holding the
    /// session.
    #[tokio::test(start_paused = true)]
    async fn a_ping_a_stuck_socket_never_takes_fails_after_the_write_timeout() {
        let started = Instant::now();
        let result = send_ping(&mut StuckSink).await;
        assert_eq!(started.elapsed(), SOCKET_WRITE_TIMEOUT);
        assert_eq!(result, Err("ping not sent within 5s".to_owned()));
    }

    /// Listen keys whose request panics.
    struct PanickingKeys;

    impl RiskListenKeys for PanickingKeys {
        async fn start(&self) -> Result<String, String> {
            panic!("listen-key bug")
        }

        async fn keepalive(&self, _listen_key: &str) -> Result<(), String> {
            Ok(())
        }
    }

    /// Every event logged on this thread while the guard lives, as its fields rendered
    /// `name=value`, space-separated.
    fn capture_logs() -> (Arc<Mutex<Vec<String>>>, tracing::subscriber::DefaultGuard) {
        use tracing_subscriber::layer::SubscriberExt;

        struct Fields<'a>(&'a mut String);

        impl tracing::field::Visit for Fields<'_> {
            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                self.0.push_str(&format!("{}={value:?} ", field.name()));
            }
        }

        struct Capture(Arc<Mutex<Vec<String>>>);

        impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for Capture {
            fn on_event(
                &self,
                event: &tracing::Event<'_>,
                _: tracing_subscriber::layer::Context<'_, S>,
            ) {
                let mut fields = String::new();
                event.record(&mut Fields(&mut fields));
                self.0.lock().unwrap().push(fields);
            }
        }

        let logs = Arc::new(Mutex::new(Vec::new()));
        let guard = tracing::subscriber::set_default(
            tracing_subscriber::registry().with(Capture(Arc::clone(&logs))),
        );
        (logs, guard)
    }

    /// A panic in the stream is caught, logged with its message, and drops the stream's sender;
    /// an abort is not logged as a panic.
    #[tokio::test]
    async fn a_panic_in_the_stream_is_logged_and_stops_it() {
        // The test runtime runs on this thread, so the thread's default subscriber sees the
        // spawned task's events.
        let (logs, _guard) = capture_logs();
        let panicked = |logs: &Mutex<Vec<String>>| {
            logs.lock()
                .unwrap()
                .iter()
                .any(|fields| fields.contains("panicked"))
        };

        let (tx, mut rx) = mpsc::unbounded_channel();
        let _task = RiskStreamTask::spawn(
            PanickingKeys,
            "ws://unused".to_owned(),
            tx,
            new_dedup_cache(),
        );
        assert!(rx.recv().await.is_none(), "the panic drops the sender");
        let logged = logs.lock().unwrap().clone();
        assert!(
            logged
                .iter()
                .any(|fields| fields.contains("panicked") && fields.contains("listen-key bug")),
            "{logged:?}"
        );

        logs.lock().unwrap().clear();
        let (tx, mut rx) = mpsc::unbounded_channel();
        let task = RiskStreamTask::spawn(
            Arc::new(HangingKeys::default()),
            "ws://unused".to_owned(),
            tx,
            new_dedup_cache(),
        );
        tokio::task::yield_now().await;
        drop(task);
        assert!(rx.recv().await.is_none(), "the abort drops the sender");
        assert!(!panicked(&logs), "an abort is not a panic");
    }
}
