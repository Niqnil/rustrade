//! The Binance margin user-data stream: one listen key from `POST /sapi/v2/margin/listen-key`,
//! read at `wss://fstream.binance.com/private/ws`.
//!
//! One key covers the whole margin account, cross and every isolated pair, and each frame says
//! which it is about: `is` is `false` for cross and `true` for isolated, and an isolated frame
//! names its pair in `s`. A client keeps the frames of the account it trades (see
//! [`StreamAccount`]) and drops the rest at `debug`.
//!
//! Frames are bare events, `{ "e": <type>, "E": <time>, .. }`. Fills, orders and balances keep the
//! payloads of the earlier margin streams. What the account owes arrives as `liabilityChange`, and
//! its margin level as `marginLevelChange`.

use super::{open_reconnect_recovery, recover_margin_ended_orders, recover_margin_fills};
use crate::client::binance::shared::{
    CONNECT_TIMEOUT_SECS, ExecutionReport, HEARTBEAT_TIMEOUT_SECS, RateLimitTracker, RequestKind,
    SIGNAL_RECOVERY_LOOKBACK_MS, SharedDedupCache, UnhandledEvents, UnrecoveredFills,
    account_position_time, classify_rest_query_error, convert_execution_report,
    dedup_key_from_event, disconnect_error, frame_excerpt, is_duplicate, log_unhandled_event,
    log_unrecognised_frame, resolve_snapshot_kind, response_decode_error, rest_call_with_retry,
    retry_or_end,
};
use crate::{
    AccountEventKind, InstrumentBalanceUpdate, UnindexedAccountEvent,
    balance::{AssetBalanceUpdate, BalanceUpdate, MarginDetails},
    client::{
        order_recovery::{KnownLiveOrders, SharedKnownLiveOrders, UncheckedOrders},
        reconnect_budget::{BackoffLimits, ReconnectBudget},
    },
    emit_stream_terminated,
    error::{ConnectivityError, UnindexedClientError},
    margin_debt::{MarginDebtKind, MarginDebtUpdate},
    notice::{AccountNotice, NoticeKind},
};
use binance_sdk::margin_trading::{
    rest_api::{KeepaliveUserDataStreamV2Params, RestApi},
    websocket_streams::{MarginLevelStatusChange, OutboundAccountPosition},
};
use chrono::{DateTime, TimeZone, Utc};
use futures::{Sink, SinkExt as _, StreamExt as _};
use rust_decimal::Decimal;
use rustrade_instrument::{
    asset::name::AssetNameExchange, exchange::ExchangeId, instrument::name::InstrumentNameExchange,
};
use rustrade_integration::{
    error::SocketError,
    protocol::websocket::{WebSocket, WsError, WsMessage, connect},
};
use serde::Deserialize;
use smol_str::SmolStr;
use std::{
    borrow::Cow,
    collections::HashMap,
    fmt::Display,
    future::Future,
    str::FromStr,
    sync::{Arc, atomic::AtomicBool, atomic::AtomicU64, atomic::Ordering},
    time::Duration,
};
use tokio::{
    sync::mpsc,
    time::{Instant, MissedTickBehavior},
};
use tracing::{debug, error, info, trace, warn};

/// Where the stream is read. The listen key is sent in a `SUBSCRIBE` frame rather than in the URL,
/// which the WebSocket layer logs at `debug` when it connects.
pub(super) const STREAM_URL: &str = "wss://fstream.binance.com/private/ws";

/// How often the listen key is kept alive. Binance expires one 60 minutes after its last
/// keepalive and recommends one every 30.
const LISTEN_KEY_KEEPALIVE: Duration = Duration::from_secs(30 * 60);
/// The longest a keepalive may take, rate-limit waits included.
const KEEPALIVE_TIMEOUT: Duration = Duration::from_secs(60);
/// The longest the stream waits to send a ping or close its socket.
const SOCKET_WRITE_TIMEOUT: Duration = Duration::from_secs(5);

// ---------------------------------------------------------------------------
// Listen keys
// ---------------------------------------------------------------------------

/// The listen-key requests the stream needs, so a test can stand in for the venue.
pub(super) trait ListenKeys: Send + Sync + 'static {
    /// Get a listen key. Binance returns the account's current one while it is valid.
    fn start(&self) -> impl Future<Output = Result<String, UnindexedClientError>> + Send;
    /// Extend the listen key's validity.
    fn keepalive(
        &self,
        listen_key: &str,
    ) -> impl Future<Output = Result<(), UnindexedClientError>> + Send;
}

/// [`ListenKeys`] over `/sapi/v2/margin/listen-key`, through binance-sdk's bindings.
///
/// Both requests weigh 1. Getting a key is [`RequestKind::Essential`], as the stream cannot
/// resume without one; a keepalive is a [`RequestKind::Query`], with half the key's life left to
/// wait in. The key is not closed when the stream ends: it lapses 60 minutes after the last
/// keepalive.
pub(super) struct SapiListenKeys {
    pub(super) rest: Arc<RestApi>,
    pub(super) rate_limiter: Arc<RateLimitTracker>,
}

impl ListenKeys for SapiListenKeys {
    async fn start(&self) -> Result<String, UnindexedClientError> {
        let response = rest_call_with_retry(
            &self.rest,
            &self.rate_limiter,
            RequestKind::Essential,
            |rest| Box::pin(async move { rest.start_user_data_stream_v2().await }),
        )
        .await
        .map_err(|e| classify_rest_query_error(&e, None))?;
        let data = response.data().await.map_err(response_decode_error)?;
        data.listen_key.ok_or_else(|| {
            UnindexedClientError::Internal("listen-key response carried no listenKey".to_owned())
        })
    }

    async fn keepalive(&self, listen_key: &str) -> Result<(), UnindexedClientError> {
        let listen_key = listen_key.to_owned();
        rest_call_with_retry(&self.rest, &self.rate_limiter, RequestKind::Query, |rest| {
            let listen_key = listen_key.clone();
            Box::pin(async move {
                let params = KeepaliveUserDataStreamV2Params::builder(listen_key).build()?;
                rest.keepalive_user_data_stream_v2(params).await
            })
        })
        .await
        .map(drop)
        .map_err(|e| classify_rest_query_error(&e, None))
    }
}

/// `text` with each occurrence of `listen_key` replaced by `<listen key>`, for a log line.
///
/// Anyone holding the key can read the account's stream until it lapses, so no frame, venue reply
/// or error that may quote one is logged unmasked. Masking runs before a log helper cuts the text
/// short, so no prefix of the key survives either. It matches the key literally: Binance's keys
/// are alphanumeric, so JSON or URL encoding leaves them unchanged.
fn redact_listen_key<'a>(text: &'a str, listen_key: &str) -> Cow<'a, str> {
    if listen_key.is_empty() || !text.contains(listen_key) {
        Cow::Borrowed(text)
    } else {
        Cow::Owned(text.replace(listen_key, "<listen key>"))
    }
}

// ---------------------------------------------------------------------------
// Which frames are this client's
// ---------------------------------------------------------------------------

/// The part of the margin account a client trades, which decides the frames it keeps.
pub(super) enum StreamAccount {
    /// The cross margin account: frames with `is: false`.
    Cross,
    /// The configured isolated pairs: frames with `is: true` whose `s` is one of them. Each pair
    /// maps to its base and quote asset, when they are known, which route its balance frames.
    Isolated {
        pairs: HashMap<InstrumentNameExchange, Option<(AssetNameExchange, AssetNameExchange)>>,
    },
}

/// Whose account a kept frame is about.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum FrameScope {
    /// The cross account as a whole.
    Account,
    /// One of the client's isolated pairs.
    Pair(InstrumentNameExchange),
    /// The frame does not say: it has no `is`, or is isolated without a readable pair. Each event
    /// type decides what to do with it (see [`convert_stream_frame`]).
    Unknown,
}

impl StreamAccount {
    /// The scope of a frame with `is` and the pair `pair`, or `None` when it is about another
    /// part of the account than this client's. `pair` is the frame's `s` on the event types where
    /// `s` names the pair; on `marginLevelChange` it is the status, and is not passed.
    fn scope(&self, is: Option<bool>, pair: Option<&str>) -> Option<FrameScope> {
        match (self, is) {
            (Self::Cross, Some(false)) => Some(FrameScope::Account),
            (Self::Isolated { .. }, Some(false)) | (Self::Cross, Some(true)) => None,
            (Self::Isolated { pairs }, Some(true)) => match pair {
                Some(pair) => pairs
                    .get_key_value(pair)
                    .map(|(pair, _)| FrameScope::Pair(pair.clone())),
                None => Some(FrameScope::Unknown),
            },
            (_, None) => Some(FrameScope::Unknown),
        }
    }

    /// Whether an order on `symbol` may be this client's: on cross any may be, on isolated only
    /// one on a configured pair.
    fn may_trade(&self, symbol: &str) -> bool {
        match self {
            Self::Cross => true,
            Self::Isolated { pairs } => pairs.contains_key(symbol),
        }
    }
}

// ---------------------------------------------------------------------------
// Frame conversion
// ---------------------------------------------------------------------------

/// What [`convert_stream_frame`] found a frame to be.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum FrameOutcome {
    /// A reply to a request, such as a late subscribe acknowledgement.
    Response,
    /// An event the venue pushed; `key_expired` when it said the listen key lapsed.
    Pushed { key_expired: bool },
}

/// The fields every frame is read for before its type's own pass.
#[derive(Deserialize)]
struct FrameHeader<'a> {
    /// Present on a reply to a request, which is not an event.
    #[serde(default)]
    id: Option<serde::de::IgnoredAny>,
    #[serde(borrow, default)]
    e: Option<Cow<'a, str>>,
    /// `true` for an isolated pair's frame, `false` for cross.
    #[serde(rename = "is", default)]
    is_isolated: Option<bool>,
    /// The isolated pair, on every event type but `marginLevelChange`, where it is the status.
    #[serde(borrow, default)]
    s: Option<Cow<'a, str>>,
}

/// Convert one stream frame for `account`, pushing what it reports onto `buf`.
///
/// - `executionReport`: the fill and the order's state (see [`convert_execution_report`]). One
///   without `is` is kept by its symbol, and the ambiguity logged: on cross every symbol, on
///   isolated a configured pair.
/// - `outboundAccountPosition`: on cross, asset-keyed `BalanceStreamUpdate`s; on isolated, the
///   pair's [`InstrumentBalanceUpdate`]s. One that does not say whose it is is dropped with a
///   `warn`, as a balance applied to the wrong account would be wrong until the next snapshot.
/// - `balanceUpdate` (a transfer, deposit or withdrawal) is not forwarded: the next
///   `outboundAccountPosition` carries the balance it left.
/// - `liabilityChange`: one [`MarginDebtUpdate`] per asset it lists, naming the pair on isolated.
///   One that does not say whose it is is dropped with a `warn`, since without its pair it would
///   read as the cross account's debt.
/// - `marginLevelChange`: a [`AccountNotice`]. Its `s` is the status, so it never names a pair: an
///   isolated one is delivered naming no instrument, with a `warn`, as is one with no `is`, since
///   losing a margin call costs more than losing its attribution.
/// - `listenKeyExpired` ends the session, for a new key.
///
/// Event types with no arm are logged by [`log_unhandled_event`], so a renamed event is seen.
/// Every logged frame has the listen key masked first.
pub(super) fn convert_stream_frame(
    frame: &str,
    listen_key: &str,
    account: &StreamAccount,
    buf: &mut Vec<UnindexedAccountEvent>,
) -> FrameOutcome {
    static UNRECOGNISED: AtomicU64 = AtomicU64::new(0);
    static UNHANDLED: UnhandledEvents = UnhandledEvents::new();
    let logged = || redact_listen_key(frame, listen_key);
    let pushed = FrameOutcome::Pushed { key_expired: false };

    let header = match serde_json::from_str::<FrameHeader<'_>>(frame) {
        Ok(header) if header.id.is_some() => return FrameOutcome::Response,
        Ok(header) => header,
        Err(_) => {
            log_unrecognised_frame("BinanceMargin", &UNRECOGNISED, &logged());
            return pushed;
        }
    };
    let Some(event_type) = header.e.as_deref() else {
        log_unrecognised_frame("BinanceMargin", &UNRECOGNISED, &logged());
        return pushed;
    };
    let pair = header.s.as_deref();

    // Each arm scopes the frame from its header before decoding it, so a frame of another part of
    // the account costs no decode and logs nothing above `trace`.
    match event_type {
        "executionReport" => {
            let keep = match account.scope(header.is_isolated, pair) {
                Some(FrameScope::Account | FrameScope::Pair(_)) => true,
                Some(FrameScope::Unknown) => {
                    // A report's `s` is its symbol.
                    let symbol = pair.unwrap_or_default();
                    log_unattributed_report(symbol);
                    account.may_trade(symbol)
                }
                None => false,
            };
            if !keep {
                trace!(
                    symbol = pair.unwrap_or("?"),
                    "BinanceMargin: executionReport of another margin account, skipping"
                );
                return pushed;
            }
            let Ok(report) = serde_json::from_str::<ExecutionReport>(frame).inspect_err(|e| {
                warn!(error = %e, frame = frame_excerpt(&logged()), "BinanceMargin: undeserializable executionReport, dropping");
            }) else {
                return pushed;
            };
            // Evidence for a future margin-liquidation marker: Binance documents none for margin,
            // so a forced liquidation's report looks like any other. These fields are where one
            // would show.
            debug!(
                symbol = report.symbol.as_deref().unwrap_or("?"),
                order_id = ?report.order_id,
                client_order_id = report.client_order_id.as_deref().unwrap_or("?"),
                orig_client_order_id = report.orig_client_order_id.as_deref().unwrap_or("?"),
                order_type = report.order_type.as_deref().unwrap_or("?"),
                execution_type = report.execution_type.as_deref().unwrap_or("?"),
                order_status = report.order_status.as_deref().unwrap_or("?"),
                "BinanceMargin executionReport"
            );
            convert_execution_report(&report, ExchangeId::BinanceMargin, buf);
        }
        "outboundAccountPosition" => {
            let scope = match account.scope(header.is_isolated, pair) {
                Some(FrameScope::Unknown) => {
                    warn!(
                        is = ?header.is_isolated,
                        pair = pair.unwrap_or("?"),
                        "BinanceMargin: outboundAccountPosition that does not say which margin \
                         account it is about, dropping it"
                    );
                    return pushed;
                }
                Some(scope) => scope,
                None => {
                    trace!(
                        "BinanceMargin: outboundAccountPosition of another margin account, skipping"
                    );
                    return pushed;
                }
            };
            let Ok(position) =
                serde_json::from_str::<OutboundAccountPosition>(frame).inspect_err(|e| {
                    warn!(error = %e, frame = frame_excerpt(&logged()), "BinanceMargin: undeserializable outboundAccountPosition, dropping");
                })
            else {
                return pushed;
            };
            match (scope, account) {
                (FrameScope::Pair(instrument), StreamAccount::Isolated { pairs }) => {
                    match pairs.get(&instrument) {
                        Some(Some((base, quote))) => {
                            route_isolated_account_position(
                                position,
                                &instrument,
                                base,
                                quote,
                                buf,
                            );
                        }
                        _ => warn!(
                            instrument = %instrument.name(),
                            "BinanceMargin isolated: no base/quote known for the pair, dropping \
                             its balance frame"
                        ),
                    }
                }
                // `scope` gives a pair only on isolated, and `Unknown` returned above.
                _ => convert_margin_account_position(position, buf),
            }
        }
        "balanceUpdate" => {
            // A transfer, deposit or withdrawal; the outboundAccountPosition that follows carries
            // the balance. No log: this fires on every transfer.
        }
        "liabilityChange" => {
            let instrument = match account.scope(header.is_isolated, pair) {
                Some(FrameScope::Account) => None,
                Some(FrameScope::Pair(instrument)) => Some(instrument),
                Some(FrameScope::Unknown) => {
                    warn!(
                        is = ?header.is_isolated,
                        pair = pair.unwrap_or("?"),
                        "BinanceMargin: liabilityChange that does not say which margin account it \
                         is about, dropping it rather than apply it to the wrong one"
                    );
                    return pushed;
                }
                None => {
                    trace!("BinanceMargin: liabilityChange of another margin account, skipping");
                    return pushed;
                }
            };
            let Ok(change) = serde_json::from_str::<LiabilityChange>(frame).inspect_err(|e| {
                warn!(error = %e, frame = frame_excerpt(&logged()), "BinanceMargin: undeserializable liabilityChange, dropping");
            }) else {
                return pushed;
            };
            log_liability_change(&change);
            buf.extend(liability_debt_updates(change, instrument));
        }
        "marginLevelChange" => {
            // `s` is the status here, so no pair is read from it.
            let Some(scope) = account.scope(header.is_isolated, None) else {
                trace!("BinanceMargin: marginLevelChange of another margin account, skipping");
                return pushed;
            };
            let Ok(change) =
                serde_json::from_str::<MarginLevelStatusChange>(frame).inspect_err(|e| {
                    warn!(error = %e, frame = frame_excerpt(&logged()), "BinanceMargin: undeserializable marginLevelChange, dropping");
                })
            else {
                return pushed;
            };
            log_margin_level_change(&change);
            if scope != FrameScope::Account {
                warn!(
                    is = ?header.is_isolated,
                    status = change.s.as_deref().unwrap_or("?"),
                    "BinanceMargin: marginLevelChange that names no pair, delivering the notice \
                     without an instrument"
                );
            }
            buf.extend(margin_level_notice(change, None));
        }
        "listenKeyExpired" => {
            warn!(
                "BinanceMargin user-data stream: the listen key expired, reconnecting with a new one"
            );
            return FrameOutcome::Pushed { key_expired: true };
        }
        "listStatus" => {
            // OCO/OTO list status: this client places no order lists, and each leg's own
            // executionReport carries the order state. Ignored knowingly.
            trace!("BinanceMargin ignoring listStatus user data event");
        }
        other => log_unhandled_event("BinanceMargin", &UNHANDLED, other, &logged()),
    }
    pushed
}

/// Log that an `executionReport` said neither cross nor isolated: `warn` the first time, `debug`
/// after, since every report would repeat it.
fn log_unattributed_report(symbol: &str) {
    static WARNED: AtomicBool = AtomicBool::new(false);
    if WARNED.swap(true, Ordering::Relaxed) {
        debug!(
            symbol,
            "BinanceMargin: executionReport without `is`, kept by its symbol"
        );
    } else {
        warn!(
            symbol,
            "BinanceMargin: executionReport without `is`, so it may be another margin account's \
             order on the same symbol; keeping it by its symbol (further ones at debug)"
        );
    }
}

/// Convert a cross `outboundAccountPosition` to `BalanceStreamUpdate`s, one per asset.
///
/// The frame is a `free`/`locked` partial (no debt), so it emits `BalanceStreamUpdate`, which
/// structurally cannot clobber the `margin` debt established by a REST `BalanceSnapshot`.
pub(super) fn convert_margin_account_position(
    position: OutboundAccountPosition,
    buf: &mut Vec<UnindexedAccountEvent>,
) {
    let time_exchange =
        account_position_time(ExchangeId::BinanceMargin, position.u, position.e_uppercase);

    for b in position.b_uppercase.unwrap_or_default() {
        let asset = match b.a {
            Some(a) => AssetNameExchange::new(a),
            None => {
                warn!("BinanceMargin account position entry missing asset name");
                continue;
            }
        };
        let free = match b.f.as_deref().and_then(|s| Decimal::from_str(s).ok()) {
            Some(v) => v,
            None => {
                warn!(%asset, "BinanceMargin account position missing/unparseable 'free' field");
                continue;
            }
        };
        let locked = match b.l.as_deref().and_then(|s| Decimal::from_str(s).ok()) {
            Some(v) => v,
            None => {
                warn!(%asset, "BinanceMargin account position missing/unparseable 'locked' field");
                continue;
            }
        };
        let update =
            AssetBalanceUpdate::new(asset, BalanceUpdate::new(free, locked), time_exchange);
        buf.push(UnindexedAccountEvent::new(
            ExchangeId::BinanceMargin,
            AccountEventKind::BalanceStreamUpdate(
                rustrade_integration::collection::snapshot::Snapshot::new(update),
            ),
        ));
    }
}

/// Convert an isolated pair's `outboundAccountPosition` to [`InstrumentBalanceUpdate`]s, one per
/// listed asset of the pair.
///
/// An entry is kept when its asset **equals** the pair's base or quote (never string-matched
/// against the symbol). Binance lists only the assets whose balance changed, so a frame may carry
/// one side or both; each yields its own event and the other side is not reported (never
/// fabricated as zero). An entry with no asset name or an unparseable `free`/`locked` is skipped
/// with a `warn!`, and an asset that is neither base nor quote at `debug!`. The engine does not
/// store this variant; the consumer reads it off the account-event feed.
fn route_isolated_account_position(
    position: OutboundAccountPosition,
    instrument: &InstrumentNameExchange,
    base_asset: &AssetNameExchange,
    quote_asset: &AssetNameExchange,
    buf: &mut Vec<UnindexedAccountEvent>,
) {
    let time_exchange =
        account_position_time(ExchangeId::BinanceMargin, position.u, position.e_uppercase);

    for b in position.b_uppercase.unwrap_or_default() {
        let Some(asset) = b.a.as_deref() else {
            warn!(instrument = %instrument.name(), "BinanceMargin isolated account position entry missing asset name");
            continue;
        };
        if asset != base_asset.name().as_str() && asset != quote_asset.name().as_str() {
            debug!(%asset, instrument = %instrument.name(), "BinanceMargin isolated account position: skipping asset outside the pair");
            continue;
        }
        let (Some(free), Some(locked)) = (
            b.f.as_deref().and_then(|s| Decimal::from_str(s).ok()),
            b.l.as_deref().and_then(|s| Decimal::from_str(s).ok()),
        ) else {
            warn!(%asset, instrument = %instrument.name(), "BinanceMargin isolated account position missing/unparseable free/locked");
            continue;
        };
        buf.push(UnindexedAccountEvent::new(
            ExchangeId::BinanceMargin,
            AccountEventKind::InstrumentBalanceUpdate(InstrumentBalanceUpdate::new(
                instrument.clone(),
                AssetBalanceUpdate::new(
                    AssetNameExchange::new(asset),
                    BalanceUpdate::new(free, locked),
                    time_exchange,
                ),
            )),
        ));
    }
}

// ---------------------------------------------------------------------------
// Margin notices and debt
// ---------------------------------------------------------------------------

/// A `liabilityChange`: what the account owes after a change, for each asset it lists.
#[derive(Debug, Deserialize)]
struct LiabilityChange {
    #[serde(rename = "E", default)]
    time_ms: Option<i64>,
    /// `BORROW`, `REPAY`, `INTEREST` or `DEBT_CHANGE`, as Binance documents it.
    #[serde(rename = "t", default)]
    kind: Option<String>,
    #[serde(rename = "L", default)]
    liabilities: Vec<Liability>,
}

/// One asset's debt in a [`LiabilityChange`].
#[derive(Debug, Deserialize)]
struct Liability {
    #[serde(rename = "a", default)]
    asset: Option<String>,
    /// The principal owed.
    #[serde(rename = "p", default)]
    principal: Option<String>,
    /// The interest owed.
    #[serde(rename = "i", default)]
    interest: Option<String>,
}

/// Log that a `marginLevelChange` arrived, at `info`: it is rare and says the account is near a
/// margin call or liquidation, or has left one.
fn log_margin_level_change(change: &MarginLevelStatusChange) {
    info!(
        status = change.s.as_deref().unwrap_or("?"),
        margin_level = change.l.as_deref().unwrap_or("?"),
        time_exchange_ms = change.e_uppercase,
        "BinanceMargin marginLevelChange received",
    );
}

/// Log that a `liabilityChange` arrived, at `info`, each asset's totals included.
fn log_liability_change(change: &LiabilityChange) {
    for liability in &change.liabilities {
        info!(
            kind = change.kind.as_deref().unwrap_or("?"),
            asset = liability.asset.as_deref().unwrap_or("?"),
            principal = liability.principal.as_deref().unwrap_or("?"),
            interest = liability.interest.as_deref().unwrap_or("?"),
            time_exchange_ms = change.time_ms,
            "BinanceMargin liabilityChange received",
        );
    }
    if change.liabilities.is_empty() {
        warn!(
            kind = change.kind.as_deref().unwrap_or("?"),
            "BinanceMargin liabilityChange that lists no asset (L)"
        );
    }
}

/// The [`NoticeKind`] for a Binance margin-level status.
///
/// Binance lists `EXCESSIVE`, `NORMAL`, `MARGIN_CALL`, `PRE_LIQUIDATION` and `FORCE_LIQUIDATION`
/// without defining them. `NORMAL` and `EXCESSIVE` both mean no margin call or liquidation
/// condition holds, so both are [`NoticeKind::MarginRestored`]. Any other status is
/// [`NoticeKind::Other`].
fn margin_notice_kind(status: &str) -> NoticeKind {
    match status {
        "MARGIN_CALL" => NoticeKind::MarginCall,
        "PRE_LIQUIDATION" => NoticeKind::LiquidationWarning,
        "FORCE_LIQUIDATION" => NoticeKind::Liquidation,
        "NORMAL" | "EXCESSIVE" => NoticeKind::MarginRestored,
        _ => NoticeKind::Other,
    }
}

/// The notice for a `marginLevelChange`, attributed to `instrument`.
///
/// `None`, logged at `warn`, for a change with no status, since it says nothing to classify. A
/// change with no time is stamped now, and one whose margin level cannot be read carries none,
/// both logged at `warn`: the notice matters more than either.
fn margin_level_notice(
    change: MarginLevelStatusChange,
    instrument: Option<InstrumentNameExchange>,
) -> Option<UnindexedAccountEvent> {
    let Some(status) = change.s else {
        warn!(
            margin_level = change.l.as_deref().unwrap_or("?"),
            "BinanceMargin marginLevelChange without a status (s), dropping"
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
                "BinanceMargin marginLevelChange without a readable time (E), using now"
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
                    "BinanceMargin marginLevelChange margin level (l) unreadable, delivering the \
                     notice without it"
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

/// The [`MarginDebtKind`] for a Binance liability change type (`t`).
///
/// `BORROW`, `REPAY` and `INTEREST` (the hourly charge) have been seen on a live account. Binance
/// also documents `DEBT_CHANGE`, which it does not define, so it is [`MarginDebtKind::Other`], as
/// is anything else.
fn margin_debt_kind(kind: &str) -> MarginDebtKind {
    match kind {
        "BORROW" => MarginDebtKind::Borrow,
        "REPAY" => MarginDebtKind::Repay,
        "INTEREST" => MarginDebtKind::Interest,
        _ => MarginDebtKind::Other,
    }
}

/// The debt updates for a `liabilityChange`, one per asset it lists, attributed to `instrument`.
///
/// Binance's principal (`p`) and interest (`i`) are what the asset owes after the change, as
/// observed live: a repay that cleared a loan reads zero, and an hourly charge leaves `p` as it was
/// and raises `i` to the whole interest owed. Binance's docs do not say either way.
///
/// An asset with no name, or whose principal or interest is missing or unreadable, is left out,
/// logged at `warn`: a total that cannot be read cannot replace the debt held. A change with no
/// readable time (`E`) is left out whole, as it could not be ordered against the debt held. A REST
/// snapshot restates the debt.
fn liability_debt_updates(
    change: LiabilityChange,
    instrument: Option<InstrumentNameExchange>,
) -> impl Iterator<Item = UnindexedAccountEvent> {
    let venue_kind = change.kind.unwrap_or_default();
    let time_exchange = change
        .time_ms
        .and_then(|ms| Utc.timestamp_millis_opt(ms).single());
    if time_exchange.is_none() {
        warn!(
            kind = %venue_kind,
            "BinanceMargin liabilityChange without a readable time (E), dropping"
        );
    }
    let liabilities = if time_exchange.is_some() {
        change.liabilities
    } else {
        Vec::new()
    };
    liabilities.into_iter().filter_map(move |liability| {
        let time_exchange = time_exchange?;
        let Some(asset) = liability.asset else {
            warn!(
                kind = %venue_kind,
                "BinanceMargin liabilityChange entry without an asset (a), dropping it"
            );
            return None;
        };
        let total = |field: &str, value: Option<&str>| match value.map(Decimal::from_str) {
            Some(Ok(total)) => Some(total),
            _ => {
                warn!(
                    %asset,
                    kind = %venue_kind,
                    field,
                    value = value.unwrap_or("<missing>"),
                    "BinanceMargin liabilityChange entry with a missing or unreadable total, \
                     dropping it"
                );
                None
            }
        };
        let borrowed = total("p", liability.principal.as_deref())?;
        let interest = total("i", liability.interest.as_deref())?;
        Some(UnindexedAccountEvent::new(
            ExchangeId::BinanceMargin,
            AccountEventKind::MarginDebt(MarginDebtUpdate::new(
                AssetNameExchange::new(asset),
                instrument.clone(),
                MarginDetails { borrowed, interest },
                margin_debt_kind(&venue_kind),
                SmolStr::new(&venue_kind),
                time_exchange,
            )),
        ))
    })
}

// ---------------------------------------------------------------------------
// Connection
// ---------------------------------------------------------------------------

/// A subscribed connection, with the text frames that arrived before the acknowledgement.
pub(super) struct Session {
    listen_key: String,
    ws: WebSocket,
    early: Vec<String>,
}

/// Connect to `url` and subscribe `listen_key`, waiting for the venue to confirm.
///
/// Returns the socket and any text frames that arrived before the acknowledgement, for the session
/// to handle first, so an event the venue sends that early is not lost. An error keeps a refused
/// handshake's tungstenite error beneath it, so a rate-limit refusal can be told apart
/// ([`RateLimitTracker::on_stream_refused`]); its text has the key masked.
async fn connect_and_subscribe(url: &str, listen_key: &str) -> anyhow::Result<Session> {
    const SUBSCRIBE_ID: u64 = 1;
    let timeout = Duration::from_secs(CONNECT_TIMEOUT_SECS);
    let masked = |text: String| redact_listen_key(&text, listen_key).into_owned();

    let mut ws = match tokio::time::timeout(timeout, connect(url)).await {
        Err(_) => {
            anyhow::bail!("BinanceMargin stream connect timed out after {CONNECT_TIMEOUT_SECS}s")
        }
        Ok(Err(SocketError::WebSocket(e))) => {
            let message = format!("BinanceMargin stream connect failed: {e}");
            return Err(anyhow::Error::new(*e).context(message));
        }
        Ok(Err(e)) => anyhow::bail!("BinanceMargin stream connect failed: {e}"),
        Ok(Ok(ws)) => ws,
    };

    let subscribe = serde_json::json!({
        "method": "SUBSCRIBE",
        "params": [listen_key],
        "id": SUBSCRIBE_ID,
    });
    ws.send(WsMessage::Text(subscribe.to_string().into()))
        .await
        .map_err(|e| anyhow::anyhow!("BinanceMargin stream subscribe send failed: {e}"))?;

    let mut early = Vec::new();
    let ack = async {
        while let Some(message) = ws.next().await {
            let text = match message {
                Ok(WsMessage::Text(text)) => text,
                Ok(WsMessage::Close(frame)) => {
                    return Err(masked(format!("closed while subscribing: {frame:?}")));
                }
                Ok(_) => continue,
                Err(e) => return Err(masked(format!("socket error while subscribing: {e}"))),
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
                Some(error) => Err(masked(format!("subscribe refused: {error}"))),
            };
        }
        Err("socket ended while subscribing".to_owned())
    };
    match tokio::time::timeout(timeout, ack).await {
        Ok(Ok(())) => Ok(Session {
            listen_key: listen_key.to_owned(),
            ws,
            early,
        }),
        Ok(Err(e)) => anyhow::bail!("BinanceMargin stream {e}"),
        Err(_) => {
            anyhow::bail!("BinanceMargin stream: no subscribe ack within {CONNECT_TIMEOUT_SECS}s")
        }
    }
}

/// Send a ping, bounded by [`SOCKET_WRITE_TIMEOUT`]: a half-open socket with a full send buffer
/// would otherwise hold the session in the send, past its heartbeat check.
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

// ---------------------------------------------------------------------------
// The stream
// ---------------------------------------------------------------------------

/// Why a session ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SessionEnd {
    /// The consumer dropped the stream.
    ConsumerDropped,
    /// The venue closed the socket, it failed, or a ping could not be sent.
    Disconnected,
    /// No frame, pong or ping in a heartbeat window.
    HeartbeatTimeout,
    /// The venue said the listen key lapsed.
    KeyExpired,
}

impl SessionEnd {
    /// Where the gap this end leaves begins, for fill recovery: a short lookback before `now`,
    /// and for a heartbeat timeout two heartbeat windows more. A check that finds proof of life
    /// only says the connection was alive at some point in the window before it, so a connection
    /// that died just after one check is declared dead two checks later. `None` when the
    /// consumer dropped the stream, which leaves nothing to recover.
    fn gap_start(self, now: DateTime<Utc>) -> Option<DateTime<Utc>> {
        let lookback = match self {
            Self::ConsumerDropped => return None,
            Self::HeartbeatTimeout => {
                chrono::Duration::seconds(2 * HEARTBEAT_TIMEOUT_SECS as i64)
                    + chrono::Duration::milliseconds(SIGNAL_RECOVERY_LOOKBACK_MS)
            }
            Self::Disconnected | Self::KeyExpired => {
                chrono::Duration::milliseconds(SIGNAL_RECOVERY_LOOKBACK_MS)
            }
        };
        Some(now - lookback)
    }

    /// The failure this end is reported as, should the reconnect budget run out after it.
    fn failure(self) -> String {
        match self {
            Self::KeyExpired => "listen key expired".to_owned(),
            _ => disconnect_error(self == Self::HeartbeatTimeout),
        }
    }
}

/// Everything a margin account stream runs on, shared by its sessions.
pub(super) struct MarginStream<K> {
    pub(super) keys: K,
    pub(super) url: String,
    pub(super) rest: Arc<RestApi>,
    pub(super) rate_limiter: Arc<RateLimitTracker>,
    pub(super) known: SharedKnownLiveOrders,
    pub(super) account: StreamAccount,
    /// The instruments whose fills and order endings a reconnect recovers.
    pub(super) instruments: Vec<InstrumentNameExchange>,
    pub(super) tx: mpsc::UnboundedSender<UnindexedAccountEvent>,
    pub(super) dedup: SharedDedupCache,
}

impl<K: ListenKeys> MarginStream<K> {
    fn is_isolated(&self) -> bool {
        matches!(self.account, StreamAccount::Isolated { .. })
    }

    /// Get a listen key, connect and subscribe, for the stream's first session.
    ///
    /// # Errors
    /// The listen-key request's error; [`ApiError::RateLimit`](crate::error::ApiError::RateLimit)
    /// when a rate-limit cooldown is too long to wait out or Binance refuses the connect for one;
    /// otherwise [`ConnectivityError::Socket`].
    pub(super) async fn open(&self) -> Result<Session, UnindexedClientError> {
        let listen_key = self.keys.start().await?;
        // The key request waited out a cooldown; one a concurrent refusal started since must not
        // be connected into.
        self.rate_limiter.before_stream_open().await?;
        connect_and_subscribe(&self.url, &listen_key)
            .await
            .map_err(|e| {
                self.rate_limiter.stream_open_error(&e, |e| {
                    UnindexedClientError::Connectivity(ConnectivityError::Socket(format!("{e:#}")))
                })
            })
    }

    /// Get a listen key, connect and subscribe, after a session ended. `Err` holds what failed,
    /// already logged and with the key masked.
    async fn reconnect(&self) -> Result<Session, String> {
        let listen_key = self.keys.start().await.map_err(|e| {
            error!(%e, "BinanceMargin listen-key request failed");
            e.to_string()
        })?;
        // As in `open`: the key request may have waited out a cooldown, and one a concurrent
        // refusal started since must not be connected into. A long one fails this attempt, and
        // `retry_or_end` then ends the stream for it.
        self.rate_limiter
            .before_stream_connect()
            .await
            .map_err(|reason| {
                warn!(
                    ?reason,
                    "BinanceMargin reconnect held back by a rate-limit cooldown"
                );
                format!("{reason:?}")
            })?;
        connect_and_subscribe(&self.url, &listen_key)
            .await
            .map_err(|e| {
                error!(error = %format_args!("{e:#}"), "BinanceMargin stream connect failed");
                self.rate_limiter.on_stream_refused(&e);
                format!("{e:#}")
            })
    }

    /// Run the stream from `initial` until the consumer drops it or it ends: reconnecting, with
    /// backoff, after each session, and recovering what each gap missed. The end is reported as
    /// [`AccountEventKind::StreamTerminated`].
    pub(super) async fn run(self, initial: Session) {
        let mut budget = ReconnectBudget::new("Binance", BackoffLimits::ACCOUNT_STREAM);
        let mut disconnect_time: Option<DateTime<Utc>> = None;
        // Gaps not yet recovered: read at reconnect, and retried on a timer while connected.
        let mut unrecovered = UnrecoveredFills::default();
        // Instruments whose known-live orders a reconnect has yet to check, likewise.
        let mut unchecked = UncheckedOrders::default();
        let mut current = Some(initial);

        loop {
            let session = match current.take() {
                Some(session) => session,
                None => {
                    // Not during a rate-limit cooldown: a short one is waited out without spending
                    // an attempt, and a longer one ends the stream, since a reconnect would be
                    // refused.
                    if let Err(reason) = self.rate_limiter.before_stream_connect().await {
                        emit_stream_terminated(&self.tx, ExchangeId::BinanceMargin, reason);
                        return;
                    }
                    match self.reconnect().await {
                        Ok(session) => session,
                        Err(e) => {
                            if let Err(reason) =
                                retry_or_end(&mut budget, &self.rate_limiter, || e).await
                            {
                                emit_stream_terminated(&self.tx, ExchangeId::BinanceMargin, reason);
                                return;
                            }
                            continue;
                        }
                    }
                }
            };
            info!(
                is_isolated = self.is_isolated(),
                "BinanceMargin account_stream connected and subscribed"
            );
            // The budget is not reset here: a venue that accepts each connection and drops it at
            // once would then be retried at the first backoff wait forever. The session resets it
            // once the connection survives a heartbeat window.

            if let Some(disconnected) = disconnect_time.take() {
                open_reconnect_recovery(
                    &mut unrecovered,
                    &mut unchecked,
                    &self.known,
                    &self.instruments,
                    disconnected,
                );
            }
            let end = self
                .session(session, &mut budget, &mut unrecovered, &mut unchecked)
                .await;

            let Some(gap_start) = end.gap_start(Utc::now()) else {
                debug!("BinanceMargin connection manager exiting");
                return;
            };
            disconnect_time = Some(gap_start);
            if self.tx.is_closed() {
                debug!("BinanceMargin connection manager exiting");
                return;
            }
            if let Err(reason) =
                retry_or_end(&mut budget, &self.rate_limiter, || end.failure()).await
            {
                emit_stream_terminated(&self.tx, ExchangeId::BinanceMargin, reason);
                return;
            }
        }
    }

    /// Read one session: forward its frames, keep its key alive, check its heartbeat, and recover
    /// what the gap before it missed, until something ends it. Its socket is closed on the way out.
    async fn session(
        &self,
        session: Session,
        budget: &mut ReconnectBudget,
        unrecovered: &mut UnrecoveredFills,
        unchecked: &mut UncheckedOrders,
    ) -> SessionEnd {
        let Session {
            listen_key,
            mut ws,
            early,
        } = session;
        let mut buf = Vec::with_capacity(32);
        for text in early {
            if let Err(end) = self.forward_frame(&text, &listen_key, &mut buf) {
                close(&mut ws).await;
                return end;
            }
        }

        let is_isolated = self.is_isolated();
        // Fills first, then how the orders held as live ended: an order whose fills are not all
        // recovered is not checked yet. Run beside the socket, so live frames keep flowing; a gap
        // or check not finished is retried as it falls due. It never completes, and dropping it
        // when the session ends loses nothing: each gap, and each instrument's order check, is
        // settled in one step as soon as its read ends.
        let recovery = async {
            loop {
                recover_margin_fills(
                    &self.rest,
                    &self.rate_limiter,
                    unrecovered,
                    &self.tx,
                    &self.dedup,
                    &self.known,
                    is_isolated,
                )
                .await;
                recover_margin_ended_orders(
                    &self.rest,
                    &self.rate_limiter,
                    &self.known,
                    unchecked,
                    unrecovered,
                    &self.tx,
                    is_isolated,
                )
                .await;
                let next_check = unchecked.next_due(unrecovered);
                match unrecovered.next_due().into_iter().chain(next_check).min() {
                    Some(due) => tokio::time::sleep_until(due).await,
                    None => std::future::pending::<()>().await,
                }
            }
        };
        // Kept alive as the session opens, then every 30 minutes: Binance hands back a key that is
        // still live without saying how long it has left, and does not document that handing it
        // back extends it. A failed keepalive is not fatal: the key then has at least 30 minutes
        // left, and the venue sends listenKeyExpired if it lapses.
        let keepalive = async {
            let mut ticks = tokio::time::interval(LISTEN_KEY_KEEPALIVE);
            ticks.set_missed_tick_behavior(MissedTickBehavior::Delay);
            loop {
                ticks.tick().await;
                match tokio::time::timeout(KEEPALIVE_TIMEOUT, self.keys.keepalive(&listen_key))
                    .await
                {
                    Ok(Ok(())) => debug!("BinanceMargin listen key kept alive"),
                    Ok(Err(e)) => warn!(
                        error = %redact_listen_key(&e.to_string(), &listen_key),
                        "BinanceMargin listen-key keepalive failed"
                    ),
                    Err(_) => warn!(
                        timeout_secs = KEEPALIVE_TIMEOUT.as_secs(),
                        "BinanceMargin listen-key keepalive timed out"
                    ),
                }
            }
        };
        tokio::pin!(recovery, keepalive);

        let heartbeat_period = Duration::from_secs(HEARTBEAT_TIMEOUT_SECS);
        let mut heartbeat =
            tokio::time::interval_at(Instant::now() + heartbeat_period, heartbeat_period);
        heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);
        // Set by every pushed frame, ping and pong. The venue pushes rarely, so each check pings,
        // and the pong proves the next window.
        let mut alive = false;
        // The first window is a grace: a slow first pong is not a heartbeat timeout.
        let mut grace = true;
        let end = match send_ping(&mut ws).await {
            Err(e) => {
                warn!(error = %e, "BinanceMargin stream disconnected, will attempt reconnect");
                SessionEnd::Disconnected
            }
            Ok(()) => loop {
                tokio::select! {
                    // A dropped consumer ends the session at once, whatever else is ready.
                    biased;
                    () = self.tx.closed() => {
                        debug!("BinanceMargin account_stream consumer dropped, terminating");
                        break SessionEnd::ConsumerDropped;
                    }
                    message = ws.next() => {
                        match self.on_message(message, &listen_key, &mut buf) {
                            Ok(proves_alive) => alive |= proves_alive,
                            Err(end) => break end,
                        }
                    }
                    _ = heartbeat.tick() => {
                        let in_grace = std::mem::take(&mut grace);
                        if std::mem::take(&mut alive) {
                            // A frame, ping or pong in this window proves the connection stable:
                            // clear the failures before it, so only consecutive ones exhaust the
                            // budget.
                            budget.reset();
                        } else if !in_grace {
                            warn!(
                                "BinanceMargin heartbeat timeout ({HEARTBEAT_TIMEOUT_SECS}s), \
                                 will attempt reconnect"
                            );
                            break SessionEnd::HeartbeatTimeout;
                        }
                        // In the grace, a silent window resets nothing: a venue that accepts the
                        // connection and stays silent spends the budget.
                        if let Err(e) = send_ping(&mut ws).await {
                            warn!(error = %e, "BinanceMargin stream disconnected, will attempt reconnect");
                            break SessionEnd::Disconnected;
                        }
                    }
                    // Last: the arms above end the session and win when ready together.
                    () = &mut keepalive => {}
                    () = &mut recovery => {}
                }
            },
        };
        close(&mut ws).await;
        end
    }

    /// Handle what the socket yielded. `Ok` says whether it proves the connection alive, `Err` how
    /// the session ends.
    fn on_message(
        &self,
        message: Option<Result<WsMessage, WsError>>,
        listen_key: &str,
        buf: &mut Vec<UnindexedAccountEvent>,
    ) -> Result<bool, SessionEnd> {
        match message {
            Some(Ok(WsMessage::Text(text))) => match self.forward_frame(&text, listen_key, buf)? {
                // A late reply to a request proves only the connect.
                FrameOutcome::Response => Ok(false),
                FrameOutcome::Pushed { .. } => Ok(true),
            },
            Some(Ok(WsMessage::Close(frame))) => {
                warn!(
                    frame = %redact_listen_key(&format!("{frame:?}"), listen_key),
                    "BinanceMargin stream closed by the venue, will attempt reconnect"
                );
                Err(SessionEnd::Disconnected)
            }
            // A ping (which tokio-tungstenite answers itself) or a pong.
            Some(Ok(_)) => Ok(true),
            Some(Err(e)) => {
                warn!(
                    error = %redact_listen_key(&e.to_string(), listen_key),
                    "BinanceMargin stream socket error, will attempt reconnect"
                );
                Err(SessionEnd::Disconnected)
            }
            None => {
                warn!("BinanceMargin stream ended, will attempt reconnect");
                Err(SessionEnd::Disconnected)
            }
        }
    }

    /// Convert `text` and send what it reports through the dedup gate. `Err` says how the session
    /// ends, when this frame ends it.
    fn forward_frame(
        &self,
        text: &str,
        listen_key: &str,
        buf: &mut Vec<UnindexedAccountEvent>,
    ) -> Result<FrameOutcome, SessionEnd> {
        let outcome = convert_stream_frame(text, listen_key, &self.account, buf);
        for mut event in buf.drain(..) {
            if let Some(key) = dedup_key_from_event(&event)
                && is_duplicate(&self.dedup, key)
            {
                trace!("BinanceMargin dedup: skipping duplicate event");
                continue;
            }
            // Held across the send, so an order this reports ending and a reconnect's check of it
            // reach the stream in the order they were decided, and the check reports only an order
            // not already reported. An isolated account's report never says whether a conditional
            // order trails, so it takes the kind the client knows the order by.
            // What resolving the kind has to say is logged once the lock is released.
            let mut note = None;
            let held = KnownLiveOrders::observes(&event.kind).then(|| {
                let mut held = self.known.lock();
                note = resolve_snapshot_kind(ExchangeId::BinanceMargin, &mut event, &|cid| {
                    held.kind_of(cid)
                });
                held.observe(&event);
                held
            });
            let sent = self.tx.send(event);
            drop(held);
            if let Some(note) = note {
                note.log();
            }
            if sent.is_err() {
                debug!("BinanceMargin account_stream receiver dropped");
                return Err(SessionEnd::ConsumerDropped);
            }
        }
        match outcome {
            FrameOutcome::Pushed { key_expired: true } => Err(SessionEnd::KeyExpired),
            outcome => Ok(outcome),
        }
    }
}

/// Close `ws`, best effort and bounded: the session is over either way, and a half-open socket
/// would otherwise hold the close.
async fn close(ws: &mut WebSocket) {
    let _ = tokio::time::timeout(SOCKET_WRITE_TIMEOUT, ws.close(None)).await;
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use crate::{
        client::dedup::new_dedup_cache,
        order::{
            ConditionalFamily, OrderKind, TrailingOffsetType,
            id::ClientOrderId,
            state::{ActiveOrderState, OrderState},
        },
    };
    use binance_sdk::common::config::ConfigurationRestApi;
    use binance_sdk::margin_trading::MarginTradingRestApi;
    use rust_decimal_macros::dec;
    use rustrade_instrument::Side;
    use std::sync::Mutex;
    use tokio_tungstenite::tungstenite::Message;

    // -- Frames ----------------------------------------------------------------------------------

    const LISTEN_KEY: &str = "synthetic-listen-key-0123456789";

    fn btcusdt() -> InstrumentNameExchange {
        InstrumentNameExchange::new("BTCUSDT")
    }

    /// An isolated client of BTCUSDT, whose base and quote are known.
    fn isolated() -> StreamAccount {
        StreamAccount::Isolated {
            pairs: HashMap::from([(
                btcusdt(),
                Some((
                    AssetNameExchange::new("BTC"),
                    AssetNameExchange::new("USDT"),
                )),
            )]),
        }
    }

    /// A cross account's frame: `event` with `is: false`, unless it already says.
    fn push(mut event: serde_json::Value) -> String {
        let fields = event.as_object_mut().unwrap();
        fields.entry("is").or_insert(serde_json::json!(false));
        event.to_string()
    }

    /// An isolated pair's frame: `event` with `is: true` and `s: <pair>`.
    fn push_isolated(pair: &str, mut event: serde_json::Value) -> String {
        event["is"] = serde_json::json!(true);
        event["s"] = serde_json::json!(pair);
        event.to_string()
    }

    /// Convert `frame` for a cross client. Returns whether it said the key expired.
    fn cross_frame(frame: &str, buf: &mut Vec<UnindexedAccountEvent>) -> bool {
        match convert_stream_frame(frame, LISTEN_KEY, &StreamAccount::Cross, buf) {
            FrameOutcome::Pushed { key_expired } => key_expired,
            FrameOutcome::Response => panic!("{frame} is not a reply"),
        }
    }

    /// What `frame` delivers to `account`.
    fn delivered(frame: &str, account: &StreamAccount) -> Vec<UnindexedAccountEvent> {
        let mut buf = Vec::new();
        assert_eq!(
            convert_stream_frame(frame, LISTEN_KEY, account, &mut buf),
            FrameOutcome::Pushed { key_expired: false },
            "{frame}"
        );
        buf
    }

    /// Run `f`, returning its value and the message of every log event it emitted on this thread,
    /// at any level. These arms report only through logs, so asserting on the return value alone
    /// could not tell which arm handled a frame.
    fn log_messages<T>(f: impl FnOnce() -> T) -> (T, Vec<String>) {
        logged(f, |field| field.name() == "message")
    }

    /// Run `f`, returning its value and every field of every log event it emitted on this thread,
    /// rendered `name=value`.
    fn logged_fields<T>(f: impl FnOnce() -> T) -> (T, Vec<String>) {
        logged(f, |_| true)
    }

    fn logged<T>(
        f: impl FnOnce() -> T,
        keep: fn(&tracing::field::Field) -> bool,
    ) -> (T, Vec<String>) {
        use tracing_subscriber::layer::SubscriberExt;

        struct Capture {
            lines: Arc<Mutex<Vec<String>>>,
            keep: fn(&tracing::field::Field) -> bool,
        }

        struct Visit<'a> {
            lines: &'a mut Vec<String>,
            keep: fn(&tracing::field::Field) -> bool,
        }

        impl tracing::field::Visit for Visit<'_> {
            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                if (self.keep)(field) {
                    self.lines.push(if field.name() == "message" {
                        format!("{value:?}")
                    } else {
                        format!("{}={value:?}", field.name())
                    });
                }
            }
        }

        impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for Capture {
            fn on_event(
                &self,
                event: &tracing::Event<'_>,
                _: tracing_subscriber::layer::Context<'_, S>,
            ) {
                let mut lines = Vec::new();
                event.record(&mut Visit {
                    lines: &mut lines,
                    keep: self.keep,
                });
                self.lines.lock().unwrap().extend(lines);
            }
        }

        let lines = Arc::new(Mutex::new(Vec::new()));
        let layer = Capture {
            lines: Arc::clone(&lines),
            keep,
        };
        // Thread-local, so other tests running in parallel are not captured.
        let value =
            tracing::subscriber::with_default(tracing_subscriber::registry().with(layer), f);
        let lines = lines.lock().unwrap().clone();
        (value, lines)
    }

    /// A reply to a request carries `id` and is not an event; it is not activity for the
    /// heartbeat either.
    #[test]
    fn a_reply_yields_no_events() {
        let mut buf = Vec::new();
        assert_eq!(
            convert_stream_frame(
                r#"{"result":null,"id":1}"#,
                LISTEN_KEY,
                &StreamAccount::Cross,
                &mut buf
            ),
            FrameOutcome::Response
        );
        assert!(buf.is_empty());
    }

    /// A frame that is not JSON, or has no `e` tag, is logged and converts to nothing.
    #[test]
    fn unrecognised_frames_yield_no_events() {
        for frame in ["not json", r#"{"E":1}"#, r#"{"e":5}"#] {
            let mut buf = Vec::new();
            assert!(!cross_frame(frame, &mut buf), "{frame}");
            assert!(buf.is_empty(), "{frame}");
        }
    }

    /// Every frame that is logged may name the listen key, so each is logged with it masked:
    /// unrecognised frames, undeserializable events, and event types with no arm.
    #[test]
    fn a_logged_frame_never_shows_the_listen_key() {
        for frame in [
            // An event type no other test uses, so it takes the loud first-of-type path.
            push(serde_json::json!({ "e": "aFrameNamingTheKey", "k": LISTEN_KEY })),
            push(serde_json::json!({ "e": "marginLevelChange", "s": 5, "k": LISTEN_KEY })),
            push(serde_json::json!({ "e": "liabilityChange", "L": 5, "k": LISTEN_KEY })),
            serde_json::json!({ "k": LISTEN_KEY }).to_string(),
            format!("{LISTEN_KEY} is not json"),
        ] {
            let mut buf = Vec::new();
            let (_, fields) = logged_fields(|| cross_frame(&frame, &mut buf));
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

    #[test]
    fn an_expired_key_ends_the_session() {
        let mut buf = Vec::new();
        assert!(cross_frame(
            r#"{"e":"listenKeyExpired","E":1,"listenKey":"k"}"#,
            &mut buf
        ));
        assert!(buf.is_empty());
    }

    #[test]
    fn margin_ws_trailing_stop_report_decodes_as_a_trailing_stop() {
        for trailing_delta in [serde_json::json!(25), serde_json::json!("25")] {
            let frame = push(serde_json::json!({
                "e": "executionReport", "s": "BTCUSDT", "S": "SELL", "o": "STOP_LOSS",
                "x": "NEW", "X": "NEW", "i": 12_345_i64, "c": "cid-1", "t": -1_i64,
                "p": "0.00000000", "P": "0.00000000", "d": trailing_delta, "D": 1_700_000_000_000_i64,
                "q": "2", "f": "GTC", "z": "0", "T": 1_700_000_000_000_i64,
            }));
            let mut buf = Vec::new();
            assert!(!cross_frame(&frame, &mut buf));
            let [event] = buf.as_slice() else {
                panic!("one snapshot expected for {frame}, got {buf:?}");
            };
            let AccountEventKind::OrderSnapshot(snapshot) = &event.kind else {
                panic!("expected an order snapshot, got {event:?}");
            };
            assert_eq!(
                snapshot.0.kind,
                OrderKind::TrailingStop {
                    offset: Decimal::from(25),
                    offset_type: TrailingOffsetType::BasisPoints,
                    activation_price: None,
                }
            );
            assert_eq!(snapshot.0.price, None);
        }
    }

    /// A partial fill carries two facts, and both must reach the consumer: the execution print
    /// (`l`/`L`) and the order's new cumulative filled quantity (`z`).
    ///
    /// The execution alone never moves the order -- `Orders::update_from_fill` writes only
    /// `filled_quantity` and needs the order already tracked -- so without the paired snapshot a
    /// partially filled margin order reads as having nothing filled until REST reconciliation
    /// refreshes it.
    #[test]
    fn margin_ws_execution_report_trade_maps_to_trade_and_order_snapshot() {
        let frame = push(serde_json::json!({
            "e": "executionReport", "s": "BTCUSDT", "S": "BUY", "o": "LIMIT",
            "x": "TRADE", "X": "PARTIALLY_FILLED", "i": 12_345_i64, "c": "cid-1",
            "t": 99_i64, "l": "0.5", "L": "48000", "z": "0.5",
            "p": "48000", "q": "2", "f": "GTC",
            "n": "0.001", "N": "BNB", "T": 1_700_000_000_000_i64,
        }));
        let mut buf = Vec::new();
        assert!(!cross_frame(&frame, &mut buf));
        assert_eq!(
            buf.len(),
            2,
            "a fill must produce the execution and the order snapshot, got: {buf:?}"
        );
        assert_eq!(buf[0].exchange, ExchangeId::BinanceMargin);
        match &buf[0].kind {
            AccountEventKind::Trade(t) => {
                assert_eq!(t.instrument.name().as_str(), "BTCUSDT");
                assert_eq!(t.side, Side::Buy);
                assert_eq!(t.price, Decimal::from(48_000));
                assert_eq!(t.quantity, Decimal::new(5, 1));
                assert_eq!(t.fees.asset.name().as_str(), "BNB"); // 3rd-party fee asset preserved
                assert_eq!(t.fees.fees, Decimal::new(1, 3));
                assert_eq!(t.client_order_id, Some(ClientOrderId::new("cid-1")));
            }
            other => panic!("expected Trade, got {other:?}"),
        }
        assert_eq!(buf[1].exchange, ExchangeId::BinanceMargin);
        match &buf[1].kind {
            AccountEventKind::OrderSnapshot(snap) => {
                assert_eq!(
                    snap.0.quantity,
                    Decimal::from(2),
                    "the order's total quantity"
                );
                match &snap.0.state {
                    OrderState::Active(ActiveOrderState::Open(open)) => {
                        assert_eq!(open.id.assigned().map(|id| id.0.as_str()), Some("12345"));
                        assert_eq!(
                            open.filled_quantity,
                            Decimal::new(5, 1),
                            "the snapshot must carry the order's cumulative filled quantity (z)"
                        );
                        assert_eq!(
                            open.time_exchange.timestamp_millis(),
                            1_700_000_000_000,
                            "stamped with the execution's transaction time (T), which is what \
                             gives the engine's staleness gate an ordering key"
                        );
                    }
                    other => panic!("expected an Open order, got {other:?}"),
                }
            }
            other => panic!("expected OrderSnapshot, got {other:?}"),
        }
    }

    /// The mirror of the case above: a `TRADE` whose order status says the order is no longer
    /// working must emit the execution *without* a snapshot, or it resurrects an order the
    /// engine has already retired as a resting one.
    #[test]
    fn margin_trade_for_an_order_no_longer_working_emits_no_snapshot() {
        for status in ["CANCELED", "EXPIRED", "REJECTED", "PENDING_CANCEL", "NEW"] {
            let frame = push(serde_json::json!({
                "e": "executionReport", "s": "BTCUSDT", "S": "BUY", "o": "LIMIT",
                "x": "TRADE", "X": status, "i": 12_345_i64, "c": "cid-1",
                "t": 99_i64, "l": "0.5", "L": "48000", "z": "0.5",
                "p": "48000", "q": "2", "f": "GTC", "T": 1_700_000_000_000_i64,
            }));
            let mut buf = Vec::new();
            assert!(!cross_frame(&frame, &mut buf));
            assert_eq!(
                buf.len(),
                1,
                "status {status} must produce the execution alone, got: {buf:?}"
            );
            assert!(
                matches!(buf[0].kind, AccountEventKind::Trade(_)),
                "the execution must still be reported for status {status}"
            );
        }
    }

    #[test]
    fn margin_ws_new_report_maps_to_active_order_snapshot() {
        let frame = push(serde_json::json!({
            "e": "executionReport", "s": "BTCUSDT", "S": "SELL", "o": "LIMIT",
            "x": "NEW", "X": "NEW", "i": 7_i64, "c": "cid-2",
            "p": "48000", "q": "1", "f": "GTC", "z": "0", "T": 1_700_000_000_000_i64,
        }));
        let mut buf = Vec::new();
        assert!(!cross_frame(&frame, &mut buf));
        assert_eq!(buf.len(), 1);
        match &buf[0].kind {
            AccountEventKind::OrderSnapshot(snap) => {
                assert_eq!(snap.0.side, Side::Sell);
                assert_eq!(snap.0.price, Some(Decimal::from(48_000)));
                assert_eq!(snap.0.quantity, Decimal::from(1));
                assert!(
                    matches!(snap.0.state, OrderState::Active(_)),
                    "NEW should be an active (resting) order"
                );
            }
            other => panic!("expected OrderSnapshot, got {other:?}"),
        }
    }

    /// The margin stream sends `"d": ""` on an order that does not trail: a stop-limit reported
    /// so is a fixed stop-limit at its `stopPrice`, without a warning.
    #[test]
    fn an_empty_trailing_delta_is_no_trailing_delta() {
        let frame = push(serde_json::json!({
            "e": "executionReport", "s": "BTCUSDT", "S": "SELL", "o": "STOP_LOSS_LIMIT",
            "x": "NEW", "X": "NEW", "i": 7_i64, "c": "cid-2", "d": "",
            "p": "47000", "P": "48000", "q": "1", "f": "GTC", "z": "0",
            "T": 1_700_000_000_000_i64,
        }));
        let (buf, messages) = log_messages(|| delivered(&frame, &StreamAccount::Cross));
        let [event] = buf.as_slice() else {
            panic!("expected one event, got {buf:?}")
        };
        let AccountEventKind::OrderSnapshot(snap) = &event.kind else {
            panic!("expected OrderSnapshot, got {:?}", event.kind)
        };
        assert_eq!(
            snap.0.kind,
            OrderKind::StopLimit {
                trigger_price: Decimal::from(48_000)
            }
        );
        assert!(
            !messages.iter().any(|m| m.contains("trailing delta")),
            "{messages:?}"
        );

        // Only an empty `d` reads as no trailing delta: one that cannot be read leaves the order
        // possibly trailing, so it is not described as a fixed stop-limit.
        let unreadable = frame.replace(r#""d":"""#, r#""d":"x""#);
        assert_ne!(unreadable, frame);
        assert_eq!(
            snapshot_kind(&delivered(&unreadable, &StreamAccount::Cross)),
            unresolved_stop_limit()
        );
    }

    /// An isolated account's stream sends `"d": ""` on a trailing order too, and so does a report
    /// that does not say which account it is about: there the empty `d` leaves the kind
    /// unresolved, with the order's trigger, rather than describing a fixed stop-limit.
    #[test]
    fn an_empty_trailing_delta_on_an_isolated_report_leaves_the_kind_unresolved() {
        let event = serde_json::json!({
            "e": "executionReport", "S": "SELL", "o": "STOP_LOSS_LIMIT",
            "x": "NEW", "X": "NEW", "i": 7_i64, "c": "cid-2", "d": "",
            "p": "47000", "P": "48000", "q": "1", "f": "GTC", "z": "0",
            "T": 1_700_000_000_000_i64,
        });
        let isolated_frame = push_isolated("BTCUSDT", event.clone());
        let buf = delivered(&isolated_frame, &isolated());
        assert_eq!(snapshot_kind(&buf), unresolved_stop_limit());
        let AccountEventKind::OrderSnapshot(snap) = &buf[0].kind else {
            unreachable!("checked by snapshot_kind")
        };
        assert_eq!(
            snap.0.price,
            Some(Decimal::from(47_000)),
            "a stop-limit keeps its limit"
        );

        let mut unlabelled = event;
        unlabelled["s"] = serde_json::json!("BTCUSDT");
        let unlabelled = unlabelled.to_string();
        assert!(!unlabelled.contains(r#""is""#));
        assert_eq!(
            snapshot_kind(&delivered(&unlabelled, &StreamAccount::Cross)),
            unresolved_stop_limit()
        );
    }

    /// The kind of the one order snapshot in `buf`.
    fn snapshot_kind(buf: &[UnindexedAccountEvent]) -> OrderKind {
        let [event] = buf else {
            panic!("expected one event, got {buf:?}")
        };
        let AccountEventKind::OrderSnapshot(snap) = &event.kind else {
            panic!("expected OrderSnapshot, got {:?}", event.kind)
        };
        snap.0.kind
    }

    /// The unresolved kind of a `STOP_LOSS_LIMIT` triggered at 48000.
    fn unresolved_stop_limit() -> OrderKind {
        OrderKind::ConditionalUnresolved {
            family: ConditionalFamily::StopLimit,
            trigger_price: Some(Decimal::from(48_000)),
        }
    }

    /// A cancel report names the cancel request in `c` and the cancelled order in `C`, and the
    /// cancel is keyed by the order's.
    #[test]
    fn margin_ws_canceled_report_maps_to_order_cancelled() {
        let frame = push(serde_json::json!({
            "e": "executionReport", "s": "BTCUSDT", "S": "BUY", "o": "LIMIT",
            "x": "CANCELED", "X": "CANCELED", "i": 8_i64, "c": "cancel-request",
            "C": "cid-3", "z": "0", "T": 1_700_000_000_000_i64,
        }));
        let mut buf = Vec::new();
        assert!(!cross_frame(&frame, &mut buf));
        assert_eq!(buf.len(), 1);
        match &buf[0].kind {
            AccountEventKind::OrderCancelled(resp) => {
                assert!(resp.state.is_ok());
                assert_eq!(resp.key.cid, ClientOrderId::new("cid-3"));
            }
            other => panic!("expected OrderCancelled, got {other:?}"),
        }
    }

    /// A report without `c` gives its fill no client order id, rather than the exchange id the
    /// order snapshot falls back to.
    #[test]
    fn margin_ws_trade_without_a_client_order_id_reports_none() {
        let frame = push(serde_json::json!({
            "e": "executionReport", "s": "BTCUSDT", "S": "SELL", "o": "MARKET",
            "x": "TRADE", "X": "FILLED", "i": 12_345_i64,
            "t": 99_i64, "l": "1", "L": "48000", "z": "1",
            "q": "1", "n": "0", "N": "USDT", "T": 1_700_000_000_000_i64,
        }));
        let mut buf = Vec::new();
        assert!(!cross_frame(&frame, &mut buf));
        let AccountEventKind::Trade(trade) = &buf[0].kind else {
            panic!("expected Trade, got {:?}", buf[0].kind);
        };
        assert_eq!(trade.order_id.0.as_str(), "12345");
        assert_eq!(trade.client_order_id, None);
    }

    #[test]
    fn margin_ws_execution_report_missing_symbol_is_dropped() {
        // Symbol absent → defensively dropped (observable warn), never a half-built event.
        let frame = push(serde_json::json!({
            "e": "executionReport", "S": "BUY", "x": "TRADE", "i": 1_i64, "t": 1_i64,
            "l": "1", "L": "100", "T": 1_700_000_000_000_i64,
        }));
        let mut buf = Vec::new();
        assert!(!cross_frame(&frame, &mut buf));
        assert!(buf.is_empty());
    }

    #[test]
    fn margin_ws_outbound_account_position_maps_to_balance_stream_updates() {
        let frame = push(serde_json::json!({
            "e": "outboundAccountPosition", "u": 1_700_000_000_000_i64,
            "B": [
                { "a": "USDT", "f": "100.0", "l": "5.0" },
                { "a": "BTC", "f": "0.5", "l": "0" },
            ],
        }));
        let mut buf = Vec::new();
        assert!(!cross_frame(&frame, &mut buf));
        assert_eq!(buf.len(), 2);
        // WS partial → BalanceStreamUpdate (free/locked), never BalanceSnapshot (no debt clobber).
        for ev in &buf {
            assert!(matches!(ev.kind, AccountEventKind::BalanceStreamUpdate(_)));
        }
    }

    /// A cross position is at its update time `u`, or at its event time `E` when it has no `u`.
    #[test]
    fn margin_ws_outbound_account_position_is_at_u_else_at_e() {
        let time_of = |frame: serde_json::Value| {
            let mut buf = Vec::new();
            assert!(!cross_frame(&push(frame), &mut buf));
            match &buf[..] {
                [event] => match &event.kind {
                    AccountEventKind::BalanceStreamUpdate(snap) => snap.0.time_exchange,
                    other => panic!("expected BalanceStreamUpdate, got {other:?}"),
                },
                other => panic!("expected one event, got {other:?}"),
            }
        };
        let balances = serde_json::json!([{ "a": "BTC", "f": "0.5", "l": "0" }]);
        assert_eq!(
            time_of(serde_json::json!({
                "e": "outboundAccountPosition", "E": 3_000, "u": 2_000, "B": balances,
            })),
            Utc.timestamp_millis_opt(2_000).unwrap()
        );
        assert_eq!(
            time_of(serde_json::json!({
                "e": "outboundAccountPosition", "E": 3_000, "B": balances,
            })),
            Utc.timestamp_millis_opt(3_000).unwrap()
        );
    }

    #[test]
    fn margin_ws_unhandled_event_types_are_ignored_without_teardown() {
        // listStatus is ignored knowingly; a type with no arm goes to log_unhandled_event. Neither
        // pushes events or signals a reconnect.
        for (e, expected) in [
            ("listStatus", "ignoring listStatus"),
            // First of its type, so the loud path: a warning naming the unhandled type.
            ("someFutureEvent", "of a type this client does not handle"),
        ] {
            let mut buf = Vec::new();
            let frame = push(serde_json::json!({ "e": e, "E": 1_700_000_000_000_i64 }));
            let (terminated, messages) = log_messages(|| cross_frame(&frame, &mut buf));
            assert!(!terminated, "{e}");
            assert!(buf.is_empty(), "{e}");
            assert!(
                messages.iter().any(|m| m.contains(expected)),
                "{e}: {messages:?}"
            );
        }
    }

    /// An amend leaves the order live, so its report emits no event, but it is reported loudly: the
    /// engine still holds the order under its old id and quantity.
    #[test]
    fn margin_ws_amend_report_is_logged_without_an_event() {
        let frame = push(serde_json::json!({
            "e": "executionReport", "s": "BTCUSDT", "S": "BUY", "o": "LIMIT",
            "x": "REPLACED", "X": "NEW", "i": 8_i64, "c": "amended", "C": "cid-3",
            "q": "0.5", "z": "0", "T": 1_700_000_000_000_i64,
        }));
        let mut buf = Vec::new();
        let (terminated, messages) = log_messages(|| cross_frame(&frame, &mut buf));
        assert!(!terminated);
        assert!(buf.is_empty(), "{buf:?}");
        assert!(
            messages
                .iter()
                .any(|m| m.contains("amended outside this client")),
            "{messages:?}"
        );
    }

    /// Self-trade prevention that leaves an order live, or names a status that is not known to
    /// end it, emits no event but is reported loudly, each case in its own words.
    #[test]
    fn margin_ws_trade_prevention_without_an_end_is_logged_without_an_event() {
        for (status, expected) in [
            (Some("PARTIALLY_FILLED"), "reduced a live order's quantity"),
            (Some("SOME_FUTURE_STATUS"), "unrecognised order status"),
            (None, "unrecognised order status"),
        ] {
            let mut report = serde_json::json!({
                "e": "executionReport", "s": "BTCUSDT", "S": "BUY", "o": "LIMIT",
                "x": "TRADE_PREVENTION", "i": 8_i64, "c": "cid-3", "C": "",
                "q": "1", "z": "0.5", "T": 1_700_000_000_000_i64,
            });
            if let Some(status) = status {
                report["X"] = serde_json::json!(status);
            }
            let frame = push(report);
            let mut buf = Vec::new();
            let (terminated, messages) = log_messages(|| cross_frame(&frame, &mut buf));
            assert!(!terminated, "{status:?}");
            assert!(buf.is_empty(), "{status:?}: {buf:?}");
            assert!(
                messages.iter().any(|m| m.contains(expected)),
                "{status:?}: {messages:?}"
            );
        }
    }

    // -- User-data stream: dedup -----------------------------------------------------------------

    #[test]
    fn margin_trade_event_dedup_by_key() {
        // A recovered fill and its live WS counterpart share (instrument, trade_id, kind) → the
        // dedup cache must drop the second occurrence (mirrors the reconnect fill-recovery path).
        let frame = push(serde_json::json!({
            "e": "executionReport", "s": "BTCUSDT", "S": "BUY", "o": "LIMIT",
            "x": "TRADE", "X": "FILLED", "i": 1_i64, "c": "cid", "t": 4_242_i64,
            "l": "1", "L": "100", "z": "1", "p": "100", "q": "1", "f": "GTC",
            "n": "0", "N": "USDT", "T": 1_700_000_000_000_i64,
        }));
        let mut buf = Vec::new();
        cross_frame(&frame, &mut buf);
        // Index rather than pop: a fill appends the execution *then* its order snapshot, and
        // popping would take the snapshot and silently test the wrong event's key.
        let event = buf.first().expect("a Trade event");
        assert!(
            matches!(event.kind, AccountEventKind::Trade(_)),
            "the execution is the first of the two events a fill produces"
        );

        let cache = new_dedup_cache();
        // DedupKey isn't Clone; re-derive it from the same event (deterministic) for the 2nd check.
        let first = dedup_key_from_event(event).expect("Trade events have a dedup key");
        assert!(!is_duplicate(&cache, first), "first sighting is fresh");
        let second = dedup_key_from_event(event).expect("Trade events have a dedup key");
        assert!(
            is_duplicate(&cache, second),
            "second sighting is a duplicate"
        );
    }

    /// Drive frames through the same two stages the live WebSocket callback does: convert, then
    /// apply the dedup gate. Returns what a consumer would actually receive.
    ///
    /// Every other user-data test in this file calls the converter directly. That is the wrong
    /// altitude to prove a fill reaches anyone: the converter can be perfectly correct while the
    /// gate downstream of it discards what the converter produced. Margin's gate is structurally
    /// identical to spot's, so it needs the same end-to-end coverage.
    fn drive_ws_pipeline(frames: &[&str]) -> Vec<UnindexedAccountEvent> {
        let cache = new_dedup_cache();
        let mut delivered = Vec::new();
        let mut buf = Vec::new();
        for frame in frames {
            let _ = cross_frame(frame, &mut buf);
            for ev in buf.drain(..) {
                if let Some(key) = dedup_key_from_event(&ev)
                    && is_duplicate(&cache, key)
                {
                    continue;
                }
                delivered.push(ev);
            }
        }
        delivered
    }

    fn ws_new(cumulative_filled: &str) -> String {
        push(serde_json::json!({
            "e": "executionReport", "s": "BTCUSDT", "S": "BUY", "o": "LIMIT",
            "x": "NEW", "X": "NEW", "i": 12_345_i64, "c": "client-1",
            "q": "2", "p": "100", "f": "GTC", "z": cumulative_filled,
            "T": 1_700_000_000_000_i64,
        }))
    }

    fn ws_partial_fill(cumulative_filled: &str) -> String {
        push(serde_json::json!({
            "e": "executionReport", "s": "BTCUSDT", "S": "BUY", "o": "LIMIT",
            "x": "TRADE", "X": "PARTIALLY_FILLED", "i": 12_345_i64, "c": "client-1",
            "q": "2", "p": "100", "f": "GTC", "z": cumulative_filled,
            "l": "1", "L": "100", "t": 555_i64, "n": "0.1", "N": "USDT",
            "T": 1_700_000_001_000_i64,
        }))
    }

    fn open_filled_quantities(events: &[UnindexedAccountEvent]) -> Vec<Decimal> {
        events
            .iter()
            .filter_map(|ev| match &ev.kind {
                AccountEventKind::OrderSnapshot(snap) => match &snap.0.state {
                    OrderState::Active(ActiveOrderState::Open(open)) => Some(open.filled_quantity),
                    _ => None,
                },
                _ => None,
            })
            .collect()
    }

    /// A fill's order snapshot must survive the dedup gate that sits between the converter and
    /// the consumer.
    ///
    /// The acknowledgement and the fill describe one order id, so a key built from the id alone
    /// would make the second look like a replay of the first. `Open::filled_quantity` would then
    /// never leave the `0` the acknowledgement carried -- the defect fixed in the converter,
    /// undone one stage later.
    #[test]
    fn margin_ws_fill_snapshot_survives_the_dedup_gate() {
        let delivered = drive_ws_pipeline(&[&ws_new("0"), &ws_partial_fill("1")]);

        assert_eq!(
            delivered.len(),
            3,
            "expected the ack snapshot, then the fill's execution and its snapshot, got: {delivered:?}"
        );
        assert_eq!(
            open_filled_quantities(&delivered),
            vec![Decimal::ZERO, Decimal::ONE],
            "the fill's snapshot must reach the consumer carrying the advanced filled quantity"
        );
    }

    /// The gate must still collapse a genuinely re-delivered frame, because a replayed
    /// acknowledgement for a retired order is re-inserted as a live resting order by the engine.
    #[test]
    fn margin_ws_replayed_frame_is_still_deduplicated() {
        let delivered = drive_ws_pipeline(&[
            &ws_new("0"),
            &ws_partial_fill("1"),
            &ws_new("0"),
            &ws_partial_fill("1"),
        ]);

        assert_eq!(
            delivered.len(),
            3,
            "re-delivering both frames must add nothing, got: {delivered:?}"
        );
    }

    /// An acknowledgement that already carries a partial fill, followed by the `TRADE` reporting
    /// that same fill, describes one order state twice -- so the gate collapses the second
    /// snapshot. That is correct, not a loss: the dedup key includes `filled_quantity`, the two
    /// snapshots agree on it, and the execution itself carries a distinct trade id and is
    /// delivered either way.
    #[test]
    fn margin_ws_an_ack_and_its_fill_reporting_one_state_deliver_that_state_once() {
        let delivered = drive_ws_pipeline(&[&ws_new("1"), &ws_partial_fill("1")]);

        assert_eq!(
            delivered.len(),
            2,
            "the ack snapshot and the execution, with the fill's duplicate snapshot collapsed, \
             got: {delivered:?}"
        );
        assert!(
            delivered
                .iter()
                .any(|ev| matches!(ev.kind, AccountEventKind::Trade(_))),
            "the execution must reach the consumer regardless"
        );
        assert_eq!(
            open_filled_quantities(&delivered),
            vec![Decimal::ONE],
            "one snapshot, carrying the state both reports agree on"
        );
    }

    // -- Which frames are this client's ----------------------------------------------------------

    fn fill_report() -> serde_json::Value {
        serde_json::json!({
            "e": "executionReport", "s": "BTCUSDT", "S": "BUY", "o": "LIMIT",
            "x": "TRADE", "X": "FILLED", "i": 1_i64, "c": "cid", "t": 5_i64,
            "l": "1", "L": "100", "z": "1", "p": "100", "q": "1", "f": "GTC",
            "n": "0", "N": "USDT", "T": 1_700_000_000_000_i64,
        })
    }

    /// One key carries the whole margin account, so each client keeps only its own part: cross
    /// the `is: false` frames, isolated the `is: true` frames of its configured pairs.
    #[test]
    fn each_client_keeps_only_its_own_accounts_frames() {
        let position = serde_json::json!({
            "e": "outboundAccountPosition", "E": 2_000, "u": 1_000,
            "B": [{ "a": "BTC", "f": "1", "l": "0" }],
        });
        let debt = serde_json::json!({
            "e": "liabilityChange", "E": 1_000, "t": "BORROW",
            "L": [{ "a": "BTC", "p": "1", "i": "0" }],
        });
        for event in [fill_report(), position, debt] {
            let cross = push(event.clone());
            let btcusdt = push_isolated("BTCUSDT", event.clone());
            let ethusdt = push_isolated("ETHUSDT", event);

            assert!(
                !delivered(&cross, &StreamAccount::Cross).is_empty(),
                "{cross}"
            );
            assert!(delivered(&cross, &isolated()).is_empty(), "{cross}");
            assert!(
                delivered(&btcusdt, &StreamAccount::Cross).is_empty(),
                "{btcusdt}"
            );
            assert!(!delivered(&btcusdt, &isolated()).is_empty(), "{btcusdt}");
            assert!(delivered(&ethusdt, &isolated()).is_empty(), "{ethusdt}");
        }
    }

    /// A frame of another part of the account is skipped on its header alone: it is not decoded,
    /// so a body that would not decode logs nothing, and its arrival is not logged at `info`.
    #[test]
    fn another_accounts_frame_is_skipped_undecoded() {
        for event in [
            serde_json::json!({ "e": "executionReport", "q": [1] }),
            serde_json::json!({ "e": "outboundAccountPosition", "E": 1, "B": 5 }),
            serde_json::json!({ "e": "liabilityChange", "E": 1, "t": "BORROW", "L": 5 }),
            serde_json::json!({ "e": "marginLevelChange", "E": 1, "l": [1] }),
        ] {
            let frame = push_isolated("BTCUSDT", event);
            let (buf, messages) = log_messages(|| delivered(&frame, &StreamAccount::Cross));
            assert!(buf.is_empty(), "{frame}");
            assert!(
                !messages
                    .iter()
                    .any(|m| m.contains("undeserializable") || m.contains("received")),
                "{frame}: {messages:?}"
            );
            // The client the frame is about does decode it: the body is what fails.
            let (_, messages) = log_messages(|| delivered(&frame, &isolated()));
            assert!(
                messages.iter().any(|m| m.contains("undeserializable")),
                "{frame}: {messages:?}"
            );
        }
    }

    /// An `executionReport` that does not say whose it is is kept by its symbol, and the
    /// ambiguity logged: on cross any symbol, on isolated a configured pair.
    #[test]
    fn a_report_without_is_is_kept_by_its_symbol() {
        let unsaid = |symbol: &str| {
            let mut report = fill_report();
            report["s"] = serde_json::json!(symbol);
            report.to_string()
        };
        let (cross, messages) =
            log_messages(|| delivered(&unsaid("ETHUSDT"), &StreamAccount::Cross));
        assert_eq!(cross.len(), 2);
        // At `warn` the first time in the process, at `debug` after: logged either way.
        assert!(
            messages.iter().any(|m| m.contains("without `is`")),
            "{messages:?}"
        );
        assert_eq!(delivered(&unsaid("BTCUSDT"), &isolated()).len(), 2);
        assert!(delivered(&unsaid("ETHUSDT"), &isolated()).is_empty());
    }

    /// A balance or debt frame that does not say whose it is is dropped with a warning, rather
    /// than applied to the wrong part of the account.
    #[test]
    fn a_balance_or_debt_frame_that_does_not_say_whose_it_is_is_dropped() {
        for frame in [
            serde_json::json!({
                "e": "outboundAccountPosition", "u": 1_000,
                "B": [{ "a": "BTC", "f": "1", "l": "0" }],
            }),
            serde_json::json!({
                "e": "liabilityChange", "E": 1_000, "t": "BORROW",
                "L": [{ "a": "BTC", "p": "1", "i": "0" }],
            }),
        ] {
            for account in [StreamAccount::Cross, isolated()] {
                let (events, messages) = log_messages(|| delivered(&frame.to_string(), &account));
                assert!(events.is_empty(), "{frame}: {events:?}");
                assert!(
                    messages
                        .iter()
                        .any(|m| m.contains("does not say which margin account")),
                    "{frame}: {messages:?}"
                );
            }
            // An isolated frame without its pair says only that it is isolated.
            let mut isolated_without_pair = frame.clone();
            isolated_without_pair["is"] = serde_json::json!(true);
            assert!(delivered(&isolated_without_pair.to_string(), &isolated()).is_empty());
        }
    }

    // -- Balances --------------------------------------------------------------------------------

    #[test]
    fn margin_ws_outbound_account_position_maps_to_balance_stream_updates_on_cross() {
        let frame = push(serde_json::json!({
            "e": "outboundAccountPosition", "u": 1_700_000_000_000_i64,
            "B": [
                { "a": "USDT", "f": "100.0", "l": "5.0" },
                { "a": "BTC", "f": "0.5", "l": "0" },
            ],
        }));
        let buf = delivered(&frame, &StreamAccount::Cross);
        assert_eq!(buf.len(), 2);
        // WS partial → BalanceStreamUpdate (free/locked), never BalanceSnapshot (no debt clobber).
        for ev in &buf {
            assert!(matches!(ev.kind, AccountEventKind::BalanceStreamUpdate(_)));
        }
    }

    /// `(instrument, asset, free, locked)` of each event, which must all be
    /// `InstrumentBalanceUpdate`s.
    fn isolated_balances(buf: &[UnindexedAccountEvent]) -> Vec<(&str, &str, Decimal, Decimal)> {
        buf.iter()
            .map(|event| match &event.kind {
                AccountEventKind::InstrumentBalanceUpdate(ibu) => (
                    ibu.instrument.name().as_str(),
                    ibu.balance.asset.name().as_str(),
                    ibu.balance.update.free,
                    ibu.balance.update.locked,
                ),
                other => panic!("expected InstrumentBalanceUpdate, got {other:?}"),
            })
            .collect()
    }

    fn isolated_position(balances: serde_json::Value) -> String {
        push_isolated(
            "BTCUSDT",
            serde_json::json!({
                "e": "outboundAccountPosition", "E": 1_700_000_000_500_i64,
                "u": 1_700_000_000_000_i64, "B": balances,
            }),
        )
    }

    #[test]
    fn isolated_outbound_position_routes_to_instrument_balance_update() {
        // Quote listed before base in `B` → assignment is by asset-name equality, not position.
        let buf = delivered(
            &isolated_position(serde_json::json!([
                { "a": "USDT", "f": "1000.0", "l": "50.0" },
                { "a": "BTC", "f": "0.5", "l": "0.1" },
            ])),
            &isolated(),
        );
        // One InstrumentBalanceUpdate per asset, in frame order — crucially NOT the asset-keyed
        // BalanceStreamUpdate (which would corrupt AssetStates).
        assert_eq!(
            isolated_balances(&buf),
            vec![
                ("BTCUSDT", "USDT", Decimal::from(1000), Decimal::from(50)),
                ("BTCUSDT", "BTC", Decimal::new(5, 1), Decimal::new(1, 1)),
            ]
        );
        let time = Utc.timestamp_millis_opt(1_700_000_000_000).unwrap();
        assert!(buf.iter().all(|event| matches!(
            &event.kind,
            AccountEventKind::InstrumentBalanceUpdate(ibu) if ibu.balance.time_exchange == time
        )));
    }

    #[test]
    fn isolated_outbound_position_is_at_u_else_at_e() {
        let time_of = |frame: serde_json::Value| match &delivered(
            &push_isolated("BTCUSDT", frame),
            &isolated(),
        )[..]
        {
            [event] => match &event.kind {
                AccountEventKind::InstrumentBalanceUpdate(update) => update.balance.time_exchange,
                other => panic!("expected InstrumentBalanceUpdate, got {other:?}"),
            },
            other => panic!("expected one event, got {other:?}"),
        };
        let balances = serde_json::json!([{ "a": "BTC", "f": "0.5", "l": "0" }]);
        assert_eq!(
            time_of(serde_json::json!({
                "e": "outboundAccountPosition", "E": 3_000, "u": 2_000, "B": balances,
            })),
            Utc.timestamp_millis_opt(2_000).unwrap()
        );
        assert_eq!(
            time_of(serde_json::json!({
                "e": "outboundAccountPosition", "E": 3_000, "B": balances,
            })),
            Utc.timestamp_millis_opt(3_000).unwrap()
        );
    }

    #[test]
    fn isolated_outbound_position_with_one_side_emits_that_side() {
        // Binance lists only the assets whose balance changed. A frame with one side yields that
        // side alone; the other is not reported, never fabricated as zero.
        for (listed, free) in [("BTC", Decimal::new(5, 1)), ("USDT", Decimal::from(20))] {
            let buf = delivered(
                &isolated_position(
                    serde_json::json!([{ "a": listed, "f": free.to_string(), "l": "0" }]),
                ),
                &isolated(),
            );
            assert_eq!(
                isolated_balances(&buf),
                vec![("BTCUSDT", listed, free, Decimal::ZERO)],
                "only {listed} listed"
            );
        }
    }

    #[test]
    fn isolated_outbound_position_skips_assets_outside_the_pair() {
        // An asset that is neither the pair's base nor its quote is skipped; the pair's own asset
        // on the same frame is still emitted. A frame with only stray assets emits nothing.
        let mixed = isolated_position(serde_json::json!([
            { "a": "BNB", "f": "3", "l": "0" }, { "a": "USDT", "f": "20", "l": "1" },
        ]));
        assert_eq!(
            isolated_balances(&delivered(&mixed, &isolated())),
            vec![("BTCUSDT", "USDT", Decimal::from(20), Decimal::ONE)]
        );
        let stray_only = isolated_position(serde_json::json!([{ "a": "BNB", "f": "3", "l": "0" }]));
        assert!(
            delivered(&stray_only, &isolated()).is_empty(),
            "no asset of the pair → nothing emitted"
        );
    }

    #[test]
    fn isolated_outbound_position_unparseable_entry_keeps_the_other_side() {
        // A bad free/locked on one asset skips that entry only; the pair's other asset is emitted.
        let frame = isolated_position(serde_json::json!([
            { "a": "BTC", "f": "x", "l": "0" }, { "a": "USDT", "f": "20", "l": "1" },
        ]));
        assert_eq!(
            isolated_balances(&delivered(&frame, &isolated())),
            vec![("BTCUSDT", "USDT", Decimal::from(20), Decimal::ONE)]
        );
    }

    /// A pair whose base and quote could not be read has its balance frames dropped; its fills
    /// still arrive.
    #[test]
    fn a_pair_without_known_assets_drops_balances_but_not_fills() {
        let account = StreamAccount::Isolated {
            pairs: HashMap::from([(btcusdt(), None)]),
        };
        let frame = isolated_position(serde_json::json!([{ "a": "BTC", "f": "1", "l": "0" }]));
        assert!(delivered(&frame, &account).is_empty());
        assert_eq!(
            delivered(&push_isolated("BTCUSDT", fill_report()), &account).len(),
            2
        );
    }

    /// A transfer's `balanceUpdate` is not forwarded: the `outboundAccountPosition` after it
    /// carries the balance.
    #[test]
    fn a_balance_update_is_not_forwarded() {
        let frame = push(serde_json::json!({
            "e": "balanceUpdate", "E": 1_000, "a": "BNB", "d": "0.001", "T": 1_000,
        }));
        let (buf, messages) = log_messages(|| delivered(&frame, &StreamAccount::Cross));
        assert!(buf.is_empty());
        assert!(messages.is_empty(), "{messages:?}");
    }

    // -- Margin debt -----------------------------------------------------------------------------

    /// `liabilityChange` as Binance documents it.
    fn documented_liability_change() -> serde_json::Value {
        serde_json::json!({
            "e": "liabilityChange", "E": 1_789_024_038_062_i64, "t": "BORROW",
            "L": [
                { "a": "USDT", "p": "520.00054700", "i": "0.00000000" },
                { "a": "BNB", "p": "0.00000000", "i": "0.00000568" },
            ],
        })
    }

    fn debt_updates(
        buf: &[UnindexedAccountEvent],
    ) -> Vec<&MarginDebtUpdate<AssetNameExchange, InstrumentNameExchange>> {
        buf.iter()
            .map(|event| match &event.kind {
                AccountEventKind::MarginDebt(update) => {
                    assert_eq!(event.exchange, ExchangeId::BinanceMargin);
                    update
                }
                other => panic!("expected MarginDebt, got {other:?}"),
            })
            .collect()
    }

    /// A cross liability change becomes one debt update per asset it lists: the totals after the
    /// change, its type mapped and kept verbatim, Binance's time, and no instrument.
    #[test]
    fn a_liability_change_is_the_accounts_debt_after_it_per_asset() {
        let buf = delivered(&push(documented_liability_change()), &StreamAccount::Cross);
        let time = Utc.timestamp_millis_opt(1_789_024_038_062).unwrap();
        assert_eq!(
            debt_updates(&buf),
            vec![
                &MarginDebtUpdate::new(
                    AssetNameExchange::new("USDT"),
                    None,
                    MarginDetails {
                        borrowed: dec!(520.000547),
                        interest: Decimal::ZERO,
                    },
                    MarginDebtKind::Borrow,
                    SmolStr::new("BORROW"),
                    time,
                ),
                &MarginDebtUpdate::new(
                    AssetNameExchange::new("BNB"),
                    None,
                    MarginDetails {
                        borrowed: Decimal::ZERO,
                        interest: dec!(0.00000568),
                    },
                    MarginDebtKind::Borrow,
                    SmolStr::new("BORROW"),
                    time,
                ),
            ]
        );
    }

    #[test]
    fn liability_change_types_map_to_debt_kinds() {
        for (t, kind) in [
            ("BORROW", MarginDebtKind::Borrow),
            ("REPAY", MarginDebtKind::Repay),
            ("INTEREST", MarginDebtKind::Interest),
            ("DEBT_CHANGE", MarginDebtKind::Other),
            ("SOME_FUTURE_TYPE", MarginDebtKind::Other),
        ] {
            let mut change = documented_liability_change();
            change["t"] = serde_json::json!(t);
            let buf = delivered(&push(change), &StreamAccount::Cross);
            for update in debt_updates(&buf) {
                assert_eq!(update.kind, kind, "{t}");
                assert_eq!(update.venue_kind, t, "the raw type is kept");
            }
        }
    }

    /// An isolated liability change names its pair.
    #[test]
    fn an_isolated_liability_change_names_its_pair() {
        let buf = delivered(
            &push_isolated("BTCUSDT", documented_liability_change()),
            &isolated(),
        );
        let updates = debt_updates(&buf);
        assert_eq!(updates.len(), 2);
        assert!(
            updates
                .iter()
                .all(|update| update.instrument == Some(btcusdt()))
        );
    }

    /// An entry that cannot be read is left out and the others kept; a change without a time is
    /// left out whole.
    #[test]
    fn an_unreadable_liability_entry_is_left_out() {
        for (field, value) in [
            ("a", serde_json::Value::Null),
            ("p", serde_json::json!("not a number")),
            ("i", serde_json::Value::Null),
        ] {
            let mut change = documented_liability_change();
            if value.is_null() {
                change["L"][0].as_object_mut().unwrap().remove(field);
            } else {
                change["L"][0][field] = value;
            }
            let buf = delivered(&push(change), &StreamAccount::Cross);
            let updates = debt_updates(&buf);
            assert_eq!(updates.len(), 1, "{field}");
            assert_eq!(updates[0].asset, AssetNameExchange::new("BNB"), "{field}");
        }

        let mut no_time = documented_liability_change();
        no_time.as_object_mut().unwrap().remove("E");
        assert!(delivered(&push(no_time), &StreamAccount::Cross).is_empty());
    }

    // -- Margin notices --------------------------------------------------------------------------

    /// `marginLevelChange` as Binance documents it.
    fn documented_margin_level_change() -> serde_json::Value {
        serde_json::json!({
            "e": "marginLevelChange", "E": 1_789_055_758_837_i64, "l": "1.2", "s": "MARGIN_CALL",
        })
    }

    /// The notice a converter pushed, which must be its only event.
    fn only_notice(buf: &[UnindexedAccountEvent]) -> &AccountNotice<InstrumentNameExchange> {
        match buf {
            [
                UnindexedAccountEvent {
                    exchange: ExchangeId::BinanceMargin,
                    kind: AccountEventKind::Notice(notice),
                },
            ] => notice,
            other => panic!("expected one BinanceMargin notice, got {other:?}"),
        }
    }

    #[test]
    fn a_margin_level_change_is_forwarded_as_a_notice() {
        let buf = delivered(
            &push(documented_margin_level_change()),
            &StreamAccount::Cross,
        );
        assert_eq!(
            only_notice(&buf),
            &AccountNotice::new(
                NoticeKind::MarginCall,
                None,
                Utc.timestamp_millis_opt(1_789_055_758_837).unwrap(),
                Some(dec!(1.2)),
                SmolStr::new("MARGIN_CALL"),
            ),
            "cross names no instrument"
        );
    }

    #[test]
    fn margin_level_statuses_map_to_notice_kinds() {
        for (status, kind) in [
            ("MARGIN_CALL", NoticeKind::MarginCall),
            ("PRE_LIQUIDATION", NoticeKind::LiquidationWarning),
            ("FORCE_LIQUIDATION", NoticeKind::Liquidation),
            ("NORMAL", NoticeKind::MarginRestored),
            ("EXCESSIVE", NoticeKind::MarginRestored),
            ("SOME_FUTURE_STATUS", NoticeKind::Other),
        ] {
            let mut change = documented_margin_level_change();
            change["s"] = serde_json::json!(status);
            let buf = delivered(&push(change), &StreamAccount::Cross);
            let notice = only_notice(&buf);
            assert_eq!(notice.kind, kind, "{status}");
            assert_eq!(notice.status, status, "the raw status is kept");
        }
    }

    #[test]
    fn a_margin_level_change_missing_parts_is_still_delivered_where_it_can_be() {
        // No readable margin level: delivered without one.
        let mut change = documented_margin_level_change();
        change["l"] = serde_json::json!("n/a");
        let buf = delivered(&push(change), &StreamAccount::Cross);
        assert_eq!(only_notice(&buf).margin_level, None);

        // No time: stamped now.
        let before = Utc::now();
        let mut change = documented_margin_level_change();
        change.as_object_mut().unwrap().remove("E");
        let buf = delivered(&push(change), &StreamAccount::Cross);
        assert!(only_notice(&buf).time_exchange >= before);

        // No status: nothing to classify, dropped.
        let mut change = documented_margin_level_change();
        change.as_object_mut().unwrap().remove("s");
        assert!(delivered(&push(change), &StreamAccount::Cross).is_empty());
    }

    /// Its `s` is the status, so a notice never names a pair: an isolated one reaches an
    /// isolated client unattributed, with a warning, and not a cross client. One with no `is`
    /// reaches both.
    #[test]
    fn a_margin_level_change_names_no_pair() {
        let mut isolated_change = documented_margin_level_change();
        isolated_change["is"] = serde_json::json!(true);
        let isolated_change = isolated_change.to_string();
        let (buf, messages) = log_messages(|| delivered(&isolated_change, &isolated()));
        assert_eq!(only_notice(&buf).instrument, None);
        assert_eq!(only_notice(&buf).kind, NoticeKind::MarginCall);
        assert!(
            messages.iter().any(|m| m.contains("names no pair")),
            "{messages:?}"
        );
        assert!(delivered(&isolated_change, &StreamAccount::Cross).is_empty());
        assert!(delivered(&push(documented_margin_level_change()), &isolated()).is_empty());

        let unsaid = documented_margin_level_change().to_string();
        assert_eq!(
            only_notice(&delivered(&unsaid, &StreamAccount::Cross)).instrument,
            None
        );
        assert_eq!(
            only_notice(&delivered(&unsaid, &isolated())).instrument,
            None
        );
    }

    /// A notice delivered twice is dropped the second time; a later one with the same status is
    /// new.
    #[test]
    fn a_repeated_notice_is_delivered_once() {
        let dedup = new_dedup_cache();
        let notice = |time_ms: i64| {
            let mut change = documented_margin_level_change();
            change["E"] = serde_json::json!(time_ms);
            delivered(&push(change), &StreamAccount::Cross)
                .pop()
                .unwrap()
        };
        let is_new = |event: &UnindexedAccountEvent| {
            !is_duplicate(&dedup, dedup_key_from_event(event).unwrap())
        };

        assert!(is_new(&notice(1_700_000_000_000)));
        assert!(!is_new(&notice(1_700_000_000_000)), "second copy dropped");
        assert!(is_new(&notice(1_700_000_060_000)), "a later notice is new");
    }

    /// The v2 stream's event names reach their own arms and decode there; the earlier streams'
    /// names, which this stream does not send, are logged as unhandled.
    #[test]
    fn the_margin_events_match_their_documented_names() {
        for (frame, expected) in [
            (documented_liability_change(), "liabilityChange received"),
            (
                documented_margin_level_change(),
                "marginLevelChange received",
            ),
        ] {
            let frame = push(frame);
            let (buf, messages) = log_messages(|| delivered(&frame, &StreamAccount::Cross));
            assert!(!buf.is_empty(), "{frame}");
            assert!(
                messages.iter().any(|m| m.contains(expected)),
                "{frame}: expected its own arm's log, got {messages:?}"
            );
            assert!(
                !messages
                    .iter()
                    .any(|m| m.contains("undeserializable") || m.contains("does not handle")),
                "{frame}: {messages:?}"
            );
        }
        for old_name in ["USER_LIABILITY_CHANGE", "MARGIN_LEVEL_STATUS_CHANGE"] {
            let frame = push(serde_json::json!({ "e": old_name, "E": 1 }));
            let (buf, messages) = log_messages(|| delivered(&frame, &StreamAccount::Cross));
            assert!(buf.is_empty(), "{old_name}");
            assert!(
                messages.iter().any(|m| m.contains("does not handle")),
                "{old_name}: {messages:?}"
            );
        }
    }

    // -- The session -----------------------------------------------------------------------------

    /// Hands out `key-1`, `key-2`, … and counts keepalives. With `cooldown_from` set, handing out
    /// any key after the first starts a 60 s rate-limit cooldown on it, as a refusal elsewhere
    /// on the same tracker would while the key request ran.
    #[derive(Default)]
    struct FakeKeys {
        issued: Mutex<u32>,
        keepalives: Mutex<Vec<String>>,
        cooldown_from: std::sync::OnceLock<Arc<RateLimitTracker>>,
    }

    impl ListenKeys for Arc<FakeKeys> {
        async fn start(&self) -> Result<String, UnindexedClientError> {
            let mut issued = self.issued.lock().unwrap();
            *issued += 1;
            if let Some(rate_limiter) = self.cooldown_from.get().filter(|_| *issued > 1) {
                rate_limiter.on_rate_limited(Some(Duration::from_secs(60)));
            }
            Ok(format!("key-{issued}"))
        }

        async fn keepalive(&self, listen_key: &str) -> Result<(), UnindexedClientError> {
            self.keepalives.lock().unwrap().push(listen_key.to_owned());
            Ok(())
        }
    }

    /// A gap begins a short lookback before its session ended, and after a heartbeat timeout two
    /// 30 s windows earlier still. A consumer that left leaves no gap.
    #[test]
    fn a_session_end_sets_where_its_gap_begins() {
        let now = Utc.timestamp_millis_opt(1_700_000_000_000).unwrap();
        let lookback = chrono::Duration::milliseconds(SIGNAL_RECOVERY_LOOKBACK_MS);
        assert!(lookback > chrono::Duration::zero());
        assert_eq!(SessionEnd::ConsumerDropped.gap_start(now), None);
        for end in [SessionEnd::Disconnected, SessionEnd::KeyExpired] {
            assert_eq!(end.gap_start(now), Some(now - lookback), "{end:?}");
        }
        assert_eq!(
            SessionEnd::HeartbeatTimeout.gap_start(now),
            Some(now - chrono::Duration::seconds(60) - lookback)
        );
    }

    /// Each end that spends a reconnect attempt names its own failure.
    #[test]
    fn a_session_end_names_its_failure() {
        assert_eq!(SessionEnd::KeyExpired.failure(), "listen key expired");
        assert_eq!(
            SessionEnd::HeartbeatTimeout.failure(),
            disconnect_error(true)
        );
        assert_eq!(SessionEnd::Disconnected.failure(), disconnect_error(false));
        assert_ne!(disconnect_error(true), disconnect_error(false));
    }

    /// Binance's listen-key requests at `server`.
    fn sapi_listen_keys(server: &wiremock::MockServer) -> SapiListenKeys {
        SapiListenKeys {
            rest: Arc::new(MarginTradingRestApi::from_config(
                ConfigurationRestApi::builder()
                    .api_key("key")
                    .api_secret("secret")
                    .base_path(server.uri())
                    .build()
                    .unwrap(),
            )),
            rate_limiter: Arc::new(RateLimitTracker::new(
                crate::client::binance::shared::WeightPool::Sapi,
            )),
        }
    }

    /// A key comes from `POST /sapi/v2/margin/listen-key`, and an answer without one is an error.
    #[tokio::test]
    async fn a_listen_key_is_read_from_the_v2_endpoint() {
        use wiremock::{
            Mock, MockServer, ResponseTemplate,
            matchers::{method, path},
        };

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/sapi/v2/margin/listen-key"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({ "listenKey": "lk-1" })),
            )
            .up_to_n_times(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/sapi/v2/margin/listen-key"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
            .mount(&server)
            .await;
        let keys = sapi_listen_keys(&server);

        assert_eq!(keys.start().await.unwrap(), "lk-1");
        assert!(matches!(
            keys.start().await,
            Err(UnindexedClientError::Internal(_))
        ));
    }

    /// A keepalive is a `PUT` naming the key, and one Binance refuses is an error.
    #[tokio::test]
    async fn a_keepalive_puts_the_key_and_reports_a_refusal() {
        use wiremock::{
            Mock, MockServer, ResponseTemplate,
            matchers::{method, path, query_param},
        };

        let server = MockServer::start().await;
        Mock::given(method("PUT"))
            .and(path("/sapi/v2/margin/listen-key"))
            .and(query_param("listenKey", "lk-1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("PUT"))
            .and(path("/sapi/v2/margin/listen-key"))
            .and(query_param("listenKey", "lk-gone"))
            .respond_with(ResponseTemplate::new(400).set_body_json(
                serde_json::json!({ "code": -1125, "msg": "This listenKey does not exist." }),
            ))
            .mount(&server)
            .await;
        let keys = sapi_listen_keys(&server);

        keys.keepalive("lk-1").await.unwrap();
        assert!(keys.keepalive("lk-gone").await.is_err());
    }

    /// A stream for `account` reading `url`, with no instruments to recover, so that its REST
    /// client is never called.
    fn margin_stream(
        keys: &Arc<FakeKeys>,
        url: String,
        account: StreamAccount,
    ) -> (
        MarginStream<Arc<FakeKeys>>,
        mpsc::UnboundedReceiver<UnindexedAccountEvent>,
    ) {
        let (tx, rx) = mpsc::unbounded_channel();
        let rest = ConfigurationRestApi::builder()
            .api_key("key")
            .api_secret("secret")
            .base_path("http://127.0.0.1:9")
            .build()
            .unwrap();
        let stream = MarginStream {
            keys: Arc::clone(keys),
            url,
            rest: Arc::new(MarginTradingRestApi::from_config(rest)),
            rate_limiter: Arc::new(RateLimitTracker::new(
                crate::client::binance::shared::WeightPool::Sapi,
            )),
            known: KnownLiveOrders::shared(ExchangeId::BinanceMargin),
            account,
            instruments: Vec::new(),
            tx,
            dedup: new_dedup_cache(),
        };
        (stream, rx)
    }

    async fn listen() -> (tokio::net::TcpListener, String) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        (listener, url)
    }

    /// How a stand-in connection behaves once it has sent its frames.
    #[derive(Clone, Copy)]
    enum After {
        /// Read until the client goes, answering its pings.
        Answer,
        /// Hold the socket without reading it, so no ping is answered.
        Ignore,
    }

    /// Accept one connection, check it subscribes `listen_key`, send `before_ack`, acknowledge,
    /// then send `frames` and behave as `after` says until the client closes the socket.
    async fn serve(
        listener: &tokio::net::TcpListener,
        listen_key: &str,
        before_ack: Vec<String>,
        frames: Vec<String>,
        after: After,
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
        match after {
            After::Answer => while let Some(Ok(_)) = ws.next().await {},
            After::Ignore => std::future::pending().await,
        }
    }

    fn notice(status: &str, time_ms: i64) -> String {
        push(serde_json::json!({
            "e": "marginLevelChange", "E": time_ms, "l": "1.08", "s": status,
        }))
    }

    fn notice_kind(event: &UnindexedAccountEvent) -> NoticeKind {
        match &event.kind {
            AccountEventKind::Notice(notice) => notice.kind,
            other => panic!("expected a notice, got {other:?}"),
        }
    }

    /// The stream forwards events through the dedup gate, gets a new key and reconnects when the
    /// venue expires the old one, and stops when the consumer drops it.
    #[tokio::test]
    async fn the_stream_forwards_events_once_and_reconnects_on_an_expired_key() {
        let (listener, url) = listen().await;
        let keys = Arc::new(FakeKeys::default());
        let (stream, mut rx) = margin_stream(&keys, url, StreamAccount::Cross);

        let task = tokio::spawn(async move {
            let session = stream.open().await.unwrap();
            stream.run(session).await;
        });
        let call = notice("MARGIN_CALL", 1_700_000_000_000);
        let expired = push(serde_json::json!({ "e": "listenKeyExpired", "E": 1 }));
        // Ends when the client closes the socket, for the expired key.
        serve(
            &listener,
            "key-1",
            vec![],
            vec![call.clone(), expired],
            After::Answer,
        )
        .await;

        // The same MARGIN_CALL again on the new connection is a duplicate.
        let restored = notice("NORMAL", 1_700_000_060_000);
        let server = serve(
            &listener,
            "key-2",
            vec![],
            vec![call, restored],
            After::Answer,
        );
        let received = async { (rx.recv().await.unwrap(), rx.recv().await.unwrap()) };
        let (first, second) = tokio::time::timeout(Duration::from_secs(10), async {
            tokio::select! {
                () = server => panic!("the server stopped before both notices arrived"),
                received = received => received,
            }
        })
        .await
        .unwrap();
        assert_eq!(notice_kind(&first), NoticeKind::MarginCall);
        assert_eq!(notice_kind(&second), NoticeKind::MarginRestored);
        assert_eq!(*keys.issued.lock().unwrap(), 2);

        drop(rx);
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .expect("the stream stops once the consumer is gone")
            .unwrap();
    }

    /// After a reconnect, the fills missed in the gap are read from the stream's own account: an
    /// isolated stream asks for its pair's isolated trades, a cross stream for cross trades.
    #[tokio::test]
    async fn a_reconnect_recovers_fills_from_the_streams_own_account() {
        use wiremock::{
            Mock, MockServer, ResponseTemplate,
            matchers::{method, path},
        };

        for (account, expected) in [(StreamAccount::Cross, "FALSE"), (isolated(), "TRUE")] {
            let venue = MockServer::start().await;
            Mock::given(method("GET"))
                .and(path("/sapi/v1/margin/myTrades"))
                .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([])))
                .mount(&venue)
                .await;
            let (listener, url) = listen().await;
            let keys = Arc::new(FakeKeys::default());
            let (mut stream, rx) = margin_stream(&keys, url, account);
            stream.rest = Arc::new(MarginTradingRestApi::from_config(
                ConfigurationRestApi::builder()
                    .api_key("key")
                    .api_secret("secret")
                    .base_path(venue.uri())
                    .build()
                    .unwrap(),
            ));
            stream.instruments = vec![btcusdt()];

            let task = tokio::spawn(async move {
                let session = stream.open().await.unwrap();
                stream.run(session).await;
            });
            let expired = push(serde_json::json!({ "e": "listenKeyExpired", "E": 1 }));
            serve(&listener, "key-1", vec![], vec![expired], After::Answer).await;
            let server = serve(&listener, "key-2", vec![], vec![], After::Answer);
            let recovered = async {
                loop {
                    let requests = venue.received_requests().await.unwrap_or_default();
                    if let Some(request) = requests.first() {
                        break request
                            .url
                            .query_pairs()
                            .into_owned()
                            .collect::<HashMap<_, _>>();
                    }
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            };
            let query = tokio::time::timeout(Duration::from_secs(10), async {
                tokio::select! {
                    () = server => panic!("the server stopped before the fills were recovered"),
                    query = recovered => query,
                }
            })
            .await
            .unwrap();
            assert_eq!(query.get("symbol").map(String::as_str), Some("BTCUSDT"));
            assert_eq!(
                query.get("isIsolated").map(String::as_str),
                Some(expected),
                "{query:?}"
            );

            drop(rx);
            task.abort();
        }
    }

    /// An event the venue sends before acknowledging the subscription is still delivered.
    #[tokio::test]
    async fn an_event_before_the_subscribe_ack_is_delivered() {
        let (listener, url) = listen().await;
        let keys = Arc::new(FakeKeys::default());
        let (stream, mut rx) = margin_stream(&keys, url, StreamAccount::Cross);

        let early = notice("MARGIN_CALL", 1_700_000_000_000);
        let server = async { serve(&listener, "key-1", vec![early], vec![], After::Answer).await };
        let run = async {
            let session = stream.open().await.unwrap();
            stream.run(session).await;
        };
        let event = tokio::time::timeout(Duration::from_secs(10), async {
            tokio::select! {
                () = server => panic!("the server stopped before the notice arrived"),
                () = run => panic!("the stream ended"),
                event = rx.recv() => event.unwrap(),
            }
        })
        .await
        .unwrap();
        assert_eq!(notice_kind(&event), NoticeKind::MarginCall);
    }

    /// An isolated account's report of a conditional order never says whether it trails, so the
    /// stream delivers it with the kind the client knows the order by: the kind it was placed
    /// with, or one the caller recalled. An order it does not know stays unresolved.
    #[tokio::test]
    async fn an_isolated_conditional_report_takes_the_kind_the_client_knows() {
        use rust_decimal_macros::dec;
        let (listener, url) = listen().await;
        let keys = Arc::new(FakeKeys::default());
        let (stream, mut rx) = margin_stream(&keys, url, isolated());
        let placed_kind = OrderKind::TrailingStopFixedLimit {
            offset: dec!(1000),
            offset_type: TrailingOffsetType::BasisPoints,
            activation_price: Some(dec!(48000)),
        };
        let recalled_kind = OrderKind::TrailingStopFixedLimit {
            offset: dec!(500),
            offset_type: TrailingOffsetType::BasisPoints,
            activation_price: None,
        };
        {
            let mut known = stream.known.lock();
            known.placed(
                &crate::order::OrderKey::new(
                    ExchangeId::BinanceMargin,
                    btcusdt(),
                    crate::order::id::StrategyId::unknown(),
                    ClientOrderId::new("placed"),
                ),
                dec!(1),
                placed_kind,
                &OrderState::active(crate::order::state::Open::new(
                    crate::order::id::VenueOrderId::Assigned(crate::order::id::OrderId::new("1")),
                    Utc::now(),
                    Decimal::ZERO,
                )),
            );
            known.recall([(ClientOrderId::new("recalled"), recalled_kind)]);
        }
        let report = |order_id: i64, cid: &str, stop_price: &str| {
            push_isolated(
                "BTCUSDT",
                serde_json::json!({
                    "e": "executionReport", "S": "BUY", "o": "STOP_LOSS_LIMIT",
                    "x": "NEW", "X": "NEW", "i": order_id, "c": cid, "d": "",
                    "p": "48000", "P": stop_price, "q": "1", "f": "GTC", "z": "0",
                    "T": 1_700_000_000_000_i64,
                }),
            )
        };
        let frames = vec![
            report(1, "placed", "48000"),
            report(2, "recalled", "0"),
            report(3, "unknown", "48000"),
        ];
        let server = async { serve(&listener, "key-1", vec![], frames, After::Answer).await };
        let run = async {
            let session = stream.open().await.unwrap();
            stream.run(session).await;
        };
        let received = async {
            let mut kinds = Vec::new();
            while kinds.len() < 3 {
                let event = rx.recv().await.unwrap();
                if let AccountEventKind::OrderSnapshot(snapshot) = &event.kind {
                    kinds.push((snapshot.0.key.cid.0.to_string(), snapshot.0.kind));
                }
            }
            kinds
        };
        let kinds = tokio::time::timeout(Duration::from_secs(10), async {
            tokio::select! {
                () = server => panic!("the server stopped before the reports arrived"),
                () = run => panic!("the stream ended"),
                kinds = received => kinds,
            }
        })
        .await
        .unwrap();
        assert_eq!(
            kinds,
            [
                ("placed".to_owned(), placed_kind),
                ("recalled".to_owned(), recalled_kind),
                (
                    "unknown".to_owned(),
                    OrderKind::ConditionalUnresolved {
                        family: ConditionalFamily::StopLimit,
                        trigger_price: Some(dec!(48000)),
                    }
                ),
            ]
        );
    }

    /// The stream resolves a report's kind under the lock it holds over the order set, and logs
    /// what it has to say about it once that lock is released.
    #[test]
    fn an_unresolved_report_is_logged_with_the_lock_released() {
        use crate::client::order_recovery::tests::logged_with_lock_free;
        let keys = Arc::new(FakeKeys::default());
        let (stream, mut rx) = margin_stream(&keys, "ws://127.0.0.1:9".to_owned(), isolated());
        let frame = push_isolated(
            "BTCUSDT",
            serde_json::json!({
                "e": "executionReport", "S": "BUY", "o": "STOP_LOSS_LIMIT",
                "x": "NEW", "X": "NEW", "i": 1, "c": "unknown", "d": "",
                "p": "48000", "P": "48000", "q": "1", "f": "GTC", "z": "0",
                "T": 1_700_000_000_000_i64,
            }),
        );

        let mut buf = Vec::new();
        let seen = logged_with_lock_free(&stream.known, || {
            stream.forward_frame(&frame, LISTEN_KEY, &mut buf).unwrap();
        });

        let event = rx.try_recv().unwrap();
        assert!(
            matches!(
                &event.kind,
                AccountEventKind::OrderSnapshot(snapshot)
                    if snapshot.0.kind.is_conditional_unresolved()
            ),
            "{event:?}"
        );
        // Sampled across the process, so the note may be at `warn` or `debug`.
        assert!(
            !seen.is_empty() && seen.iter().all(|(_, free)| *free),
            "{seen:?}"
        );
    }

    /// A refused subscription fails the open with the venue's reason, the key masked.
    #[tokio::test]
    async fn a_refused_subscription_fails_the_open_with_the_key_masked() {
        let (listener, url) = listen().await;
        let keys = Arc::new(FakeKeys::default());
        let (stream, _rx) = margin_stream(&keys, url, StreamAccount::Cross);
        let server = async {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
            let Some(Ok(Message::Text(subscribe))) = ws.next().await else {
                panic!("expected a SUBSCRIBE frame")
            };
            let subscribe: serde_json::Value = serde_json::from_str(&subscribe).unwrap();
            let key = subscribe["params"][0].as_str().unwrap().to_owned();
            let refusal = serde_json::json!({
                "id": subscribe["id"], "error": { "code": 2, "msg": format!("bad {key}") },
            });
            ws.send(Message::Text(refusal.to_string().into()))
                .await
                .unwrap();
            while let Some(Ok(_)) = ws.next().await {}
        };

        let (opened, ()) = tokio::join!(stream.open(), server);
        let Err(UnindexedClientError::Connectivity(ConnectivityError::Socket(error))) = opened
        else {
            panic!("expected a socket error, got {:?}", opened.map(|_| ()));
        };
        assert!(error.contains("bad <listen key>"), "{error}");
        assert!(!error.contains("key-1"), "{error}");
    }

    /// A handshake Binance refuses for a rate limit starts the cooldown, and fails the open as
    /// one.
    #[tokio::test]
    async fn a_throttled_handshake_starts_the_cooldown() {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

        let (listener, url) = listen().await;
        let keys = Arc::new(FakeKeys::default());
        let (stream, _rx) = margin_stream(&keys, url, StreamAccount::Cross);
        let server = async {
            let (mut tcp, _) = listener.accept().await.unwrap();
            let mut request = [0_u8; 1024];
            let _ = tcp.read(&mut request).await.unwrap();
            tcp.write_all(b"HTTP/1.1 429 Too Many Requests\r\ncontent-length: 0\r\n\r\n")
                .await
                .unwrap();
        };

        let (opened, ()) = tokio::join!(stream.open(), server);
        assert!(
            matches!(
                opened,
                Err(UnindexedClientError::Api(
                    crate::error::ApiError::RateLimit { .. }
                ))
            ),
            "{:?}",
            opened.map(|_| ())
        );
        assert!(stream.rate_limiter.cooldown_remaining().is_some());
    }

    /// A handshake Binance refuses with 418, an IP ban, starts its shortest ban, 2 minutes, and
    /// fails the open as a rate limit.
    #[tokio::test]
    async fn a_banned_handshake_starts_a_two_minute_cooldown() {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

        let (listener, url) = listen().await;
        let keys = Arc::new(FakeKeys::default());
        let (stream, _rx) = margin_stream(&keys, url, StreamAccount::Cross);
        let server = async {
            let (mut tcp, _) = listener.accept().await.unwrap();
            let mut request = [0_u8; 1024];
            let _ = tcp.read(&mut request).await.unwrap();
            tcp.write_all(b"HTTP/1.1 418 I'm a teapot\r\ncontent-length: 0\r\n\r\n")
                .await
                .unwrap();
        };

        let (opened, ()) = tokio::join!(stream.open(), server);
        assert!(
            matches!(
                opened,
                Err(UnindexedClientError::Api(
                    crate::error::ApiError::RateLimit { .. }
                ))
            ),
            "{:?}",
            opened.map(|_| ())
        );
        let remaining = stream.rate_limiter.cooldown_remaining().unwrap();
        assert!(remaining > Duration::from_secs(60), "{remaining:?}");
    }

    /// A connection whose pings go unanswered is dropped at the second heartbeat check, the first
    /// being a grace, and reconnected after the backoff; one that answers is kept, and its key is
    /// kept alive as the session opens and every 30 minutes after.
    #[tokio::test(start_paused = true)]
    async fn the_heartbeat_drops_a_silent_connection_and_keeps_an_answering_one() {
        let (listener, url) = listen().await;
        let keys = Arc::new(FakeKeys::default());
        let (stream, _rx) = margin_stream(&keys, url, StreamAccount::Cross);
        let first = serve(&listener, "key-1", vec![], vec![], After::Ignore);
        tokio::pin!(first);
        let opened = async { stream.open().await.unwrap() };
        let session = tokio::select! {
            () = &mut first => unreachable!("an ignoring server never ends"),
            session = opened => session,
        };
        // `first` is kept, unpolled: it holds its socket open and reads nothing, so no ping is
        // answered.
        let started = Instant::now();
        let task = tokio::spawn(stream.run(session));
        let (tcp, _) = listener.accept().await.unwrap();
        let reconnected_after = started.elapsed();
        let heartbeat = Duration::from_secs(HEARTBEAT_TIMEOUT_SECS);
        assert!(
            reconnected_after >= 2 * heartbeat + BackoffLimits::ACCOUNT_STREAM.initial,
            "{reconnected_after:?}"
        );
        assert!(
            reconnected_after < 3 * heartbeat + BackoffLimits::ACCOUNT_STREAM.initial,
            "{reconnected_after:?}"
        );

        // The second connection answers its pings: it stays up past several checks, and its
        // key is kept alive.
        let second = async {
            let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
            let Some(Ok(Message::Text(subscribe))) = ws.next().await else {
                panic!("expected a SUBSCRIBE frame")
            };
            let subscribe: serde_json::Value = serde_json::from_str(&subscribe).unwrap();
            let ack = serde_json::json!({ "result": null, "id": subscribe["id"] });
            ws.send(Message::Text(ack.to_string().into()))
                .await
                .unwrap();
            while let Some(Ok(_)) = ws.next().await {}
        };
        let third = async {
            let (_tcp, _) = listener.accept().await.unwrap();
            panic!("the answering connection was dropped");
        };
        tokio::pin!(second, third);
        // Each key is kept alive as its session opens, then once 30 minutes have passed.
        let opened = vec!["key-1".to_owned(), "key-2".to_owned()];
        let again = [opened.clone(), vec!["key-2".to_owned()]].concat();
        for (wait_secs, kept_alive) in [(29 * 60, opened), (2 * 60, again)] {
            tokio::select! {
                () = &mut second => panic!("the client closed the answering connection"),
                () = &mut third => {}
                () = tokio::time::sleep(Duration::from_secs(wait_secs)) => {}
            }
            assert_eq!(
                *keys.keepalives.lock().unwrap(),
                kept_alive,
                "{wait_secs} s on"
            );
        }
        assert_eq!(*keys.issued.lock().unwrap(), 2);
        task.abort();
    }

    /// A connection that drops while a long cooldown is active ends the stream at once with the
    /// time left: no reconnect is tried.
    #[tokio::test]
    async fn a_drop_during_a_long_cooldown_ends_the_stream_rate_limited() {
        use crate::error::StreamTerminationReason;

        let (listener, url) = listen().await;
        let keys = Arc::new(FakeKeys::default());
        let (stream, mut rx) = margin_stream(&keys, url, StreamAccount::Cross);
        // The venue closes the connection once it has been subscribed.
        let server = async {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
            let Some(Ok(Message::Text(subscribe))) = ws.next().await else {
                panic!("expected a SUBSCRIBE frame")
            };
            let subscribe: serde_json::Value = serde_json::from_str(&subscribe).unwrap();
            let ack = serde_json::json!({ "result": null, "id": subscribe["id"] });
            ws.send(Message::Text(ack.to_string().into()))
                .await
                .unwrap();
            ws
        };
        let (session, mut ws) = tokio::join!(stream.open(), server);
        stream
            .rate_limiter
            .on_rate_limited(Some(Duration::from_secs(60)));
        let task = tokio::spawn(stream.run(session.unwrap()));
        ws.close(None).await.unwrap();

        let event = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("the stream ends rate-limited within the timeout")
            .expect("it reports why before it ends");
        let AccountEventKind::StreamTerminated(StreamTerminationReason::RateLimited {
            retry_after,
        }) = event.kind
        else {
            panic!("expected a rate-limited termination, got {:?}", event.kind);
        };
        assert!(retry_after > Duration::from_secs(55), "{retry_after:?}");
        assert!(rx.recv().await.is_none(), "the stream has ended");
        task.await.unwrap();
        assert_eq!(*keys.issued.lock().unwrap(), 1, "no reconnect");
    }

    /// A cooldown that starts while a reconnect's key request runs stops the connect: the stream
    /// ends rate-limited, and nothing connects into the cooldown.
    #[tokio::test]
    async fn a_cooldown_started_during_the_key_request_holds_back_the_reconnect() {
        use crate::error::StreamTerminationReason;

        let (listener, url) = listen().await;
        let keys = Arc::new(FakeKeys::default());
        let (stream, mut rx) = margin_stream(&keys, url, StreamAccount::Cross);
        assert!(
            keys.cooldown_from
                .set(Arc::clone(&stream.rate_limiter))
                .is_ok()
        );
        let server = async {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
            let Some(Ok(Message::Text(subscribe))) = ws.next().await else {
                panic!("expected a SUBSCRIBE frame")
            };
            let subscribe: serde_json::Value = serde_json::from_str(&subscribe).unwrap();
            let ack = serde_json::json!({ "result": null, "id": subscribe["id"] });
            ws.send(Message::Text(ack.to_string().into()))
                .await
                .unwrap();
            ws
        };
        let (session, mut ws) = tokio::join!(stream.open(), server);
        let task = tokio::spawn(stream.run(session.unwrap()));
        ws.close(None).await.unwrap();

        let event = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("the stream ends rate-limited within the timeout")
            .expect("it reports why before it ends");
        assert!(
            matches!(
                event.kind,
                AccountEventKind::StreamTerminated(StreamTerminationReason::RateLimited { .. })
            ),
            "{:?}",
            event.kind
        );
        task.await.unwrap();
        assert_eq!(*keys.issued.lock().unwrap(), 2, "a key was requested");
        assert!(
            tokio::time::timeout(Duration::from_millis(200), listener.accept())
                .await
                .is_err(),
            "nothing connected into the cooldown"
        );
    }

    /// When the consumer drops the stream, its socket is closed.
    #[tokio::test]
    async fn a_dropped_stream_closes_its_socket() {
        let (listener, url) = listen().await;
        let keys = Arc::new(FakeKeys::default());
        let (stream, rx) = margin_stream(&keys, url, StreamAccount::Cross);
        let (closed_tx, closed) = tokio::sync::oneshot::channel();
        let server = async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
            let Some(Ok(Message::Text(subscribe))) = ws.next().await else {
                panic!("expected a SUBSCRIBE frame")
            };
            let subscribe: serde_json::Value = serde_json::from_str(&subscribe).unwrap();
            let ack = serde_json::json!({ "result": null, "id": subscribe["id"] });
            ws.send(Message::Text(ack.to_string().into()))
                .await
                .unwrap();
            tokio::spawn(async move {
                while let Some(Ok(message)) = ws.next().await {
                    if let Message::Close(_) = message {
                        let _ = closed_tx.send(());
                        return;
                    }
                }
            });
        };
        let (session, ()) = tokio::join!(stream.open(), server);
        let task = tokio::spawn(stream.run(session.unwrap()));

        drop(rx);
        tokio::time::timeout(Duration::from_secs(5), closed)
            .await
            .expect("the socket is closed")
            .unwrap();
        task.await.unwrap();
    }

    /// A ping a stuck socket never takes fails after the write timeout, rather than holding the
    /// session.
    #[tokio::test(start_paused = true)]
    async fn a_ping_a_stuck_socket_never_takes_fails_after_the_write_timeout() {
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

        let started = Instant::now();
        let result = send_ping(&mut StuckSink).await;
        assert_eq!(started.elapsed(), SOCKET_WRITE_TIMEOUT);
        assert_eq!(result, Err("ping not sent within 5s".to_owned()));
    }
}
