//! Shared Binance execution infrastructure.
//!
//! Exchange-agnostic building blocks used by both the spot and margin clients:
//! reconnect/backoff, rate-limit tracking, the connection-manager stream guard, Binance error
//! parsing/classification, and the Binance-string → rustrade-enum parsers (side/order-kind/TIF).
//!
//! Nothing here is spot- or margin-specific: the parsers operate on rustrade's own types
//! (`OrderKind`, `TimeInForce`, `Side`) or on Binance's stable wire strings, so both clients
//! reuse them unchanged. The two event converters join them the same way, each naming the field
//! subset it reads as a trait -- [`BinanceOrderFields`] for the REST order-response endpoints,
//! [`BinanceExecutionReportFields`] for the WebSocket user-data `executionReport`. Those subsets
//! are provably identical across every SDK type implementing them, even though the structs around
//! them are not, which is what makes one converter safe to share and keeps a venue name out of
//! both. The remaining SDK-typed converters, which differ between spot's WS-API enums and
//! margin's REST params, deliberately stay in their respective modules.
//!
//! Event deduplication lives in [`crate::client::dedup`], shared with the other clients that
//! need it, and is re-exported here so Binance call sites keep a single import site.

use crate::{
    AccountEventKind, UnindexedAccountEvent,
    error::{ApiError, ConnectivityError, OrderError, UnindexedClientError, UnindexedOrderError},
    order::{
        Order, OrderKey, OrderKind, TimeInForce, TrailingOffsetType, UnindexedInactiveOrder,
        UnindexedOrderKey,
        id::{ClientOrderId, OrderId, StrategyId, VenueOrderId},
        request::UnindexedOrderResponseCancel,
        state::{
            Cancelled, Expired, Filled, InactiveOrderState, Open, OrderState, UnindexedOrderState,
        },
    },
    trade::{AssetFees, Trade, TradeId},
};

// Deduplication moved to `client::dedup` when Hyperliquid needed the same machinery; re-exported
// here so the spot and margin call sites keep naming one module.
pub(crate) use crate::client::dedup::{
    SharedDedupCache, dedup_key_from_event, is_duplicate, new_dedup_cache,
};
use crate::client::order_recovery::{
    GAP_RETRY_BASE_SECS, GapFailure, MAX_GAP_RETRIES, PendingFills,
};
use binance_sdk::common::{
    errors::{ConnectorError, WebsocketError},
    models::{Interval, ParamBuildError, RateLimitType, RestApiResponse, WebsocketApiRateLimit},
};
use chrono::{DateTime, TimeZone, Utc};
use rust_decimal::Decimal;
use rustrade_instrument::{
    Side, asset::name::AssetNameExchange, exchange::ExchangeId,
    instrument::name::InstrumentNameExchange,
};
use smol_str::format_smolstr;
use std::{
    pin::Pin,
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicU32, AtomicU64, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};
use tracing::{debug, error, info, trace, warn};

// ---------------------------------------------------------------------------
// AbortOnDropStream — ensures connection_manager task is cleaned up
// ---------------------------------------------------------------------------

// AbortOnDropStream — new type not present upstream. Ensures connection_manager
// task is cancelled (at the next .await point) when the consumer drops the stream,
// preventing a background task leak and abandoned TCP connection.

/// Wrapper stream that aborts the connection_manager JoinHandle when dropped.
///
/// This ensures the channel and its associated task are cleaned up when the
/// consumer drops the account_stream. Note: `abort()` cancels at the next `.await`
/// point — if the task is mid-disconnect, the TCP connection may not close
/// gracefully. The OS will reclaim the socket via keepalive/FIN_WAIT timeout.
pub(crate) struct AbortOnDropStream<S> {
    inner: S,
    handle: tokio::task::JoinHandle<()>,
}

impl<S> AbortOnDropStream<S> {
    pub(crate) fn new(inner: S, handle: tokio::task::JoinHandle<()>) -> Self {
        Self { inner, handle }
    }
}

impl<S: futures::Stream + Unpin> futures::Stream for AbortOnDropStream<S> {
    type Item = S::Item;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.inner).poll_next(cx)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl<S> Drop for AbortOnDropStream<S> {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Initial backoff delay for reconnection attempts.
pub(crate) const INITIAL_BACKOFF_MS: u64 = 1_000;
/// Maximum backoff delay (cap for exponential growth).
pub(crate) const MAX_BACKOFF_MS: u64 = 30_000;
/// Maximum number of consecutive reconnect attempts before giving up.
pub(crate) const MAX_RECONNECT_ATTEMPTS: u32 = 10;
/// If no WS activity (messages, ping, pong) for this duration, force reconnect.
pub(crate) const HEARTBEAT_TIMEOUT_SECS: u64 = 30;
// Compile-time guard: reconnect sites cast this to i64 (`chrono::Duration::seconds`); ensure it
// never overflows.
const _: () = assert!(
    HEARTBEAT_TIMEOUT_SECS <= i64::MAX as u64,
    "HEARTBEAT_TIMEOUT_SECS overflows i64"
);
/// Timeout for fill recovery REST queries after reconnect.
pub(crate) const FILL_RECOVERY_TIMEOUT_SECS: u64 = 30;

/// How far past the start of the recovery that opens it a [`FillGap`] reaches.
///
/// Recovery starts after the live stream is subscribed, so a fill after that moment arrives live.
/// Binance stamps fills by its own clock, which may run ahead of this host's. Binance rejects a
/// signed request stamped more than `recvWindow` (5 s by default) from its clock, so a skew past
/// this margin would already fail every REST call. The overlap with live delivery it leaves is
/// seconds long, and the dedup cache absorbs it, as long as those fills have not left it by the
/// time the gap is read.
pub(crate) const GAP_END_SLACK_SECS: i64 = 10;

/// A span of one instrument's fills that a reconnect's recovery has not yet forwarded.
///
/// It runs from the disconnect to just after the recovery that opened it began
/// ([`GAP_END_SLACK_SECS`]). The live stream delivers every fill after that, so reading the gap
/// never re-reads a fill the stream already delivered, however long the gap waits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct FillGap {
    /// The first moment of the gap, in epoch milliseconds.
    pub(crate) start_ms: i64,
    /// The last moment of the gap, in epoch milliseconds.
    pub(crate) end_ms: i64,
    /// How many reads of the gap have failed or not finished.
    failures: u32,
    /// When the gap may next be read.
    due: tokio::time::Instant,
}

impl FillGap {
    /// Whether this is the same span as `other`, whatever its retry state.
    fn same_span(&self, other: &Self) -> bool {
        self.start_ms == other.start_ms && self.end_ms == other.end_ms
    }
}

/// The fills a reconnect's recovery has not yet forwarded, as [`FillGap`]s per instrument.
///
/// A reconnect opens a gap for every instrument ([`open`](Self::open)) before reading any, so a
/// recovery that fails or times out loses nothing: each gap stays until its fills are forwarded
/// ([`recovered`](Self::recovered)), and a failed one is retried with a backoff, connected or
/// across reconnects, until it has failed [`MAX_GAP_RETRIES`] retries
/// ([`failed`](Self::failed)). A reconnect does not bring a retry forward, so a flapping
/// connection cannot use the retries up.
#[derive(Debug, Default)]
pub(crate) struct UnrecoveredFills(fnv::FnvHashMap<InstrumentNameExchange, Vec<FillGap>>);

impl PendingFills for UnrecoveredFills {
    fn pending(&self, instrument: &InstrumentNameExchange) -> bool {
        self.covers(instrument)
    }
}

impl UnrecoveredFills {
    /// Open a gap for each of `instruments`, from `disconnect_time` to just after `now`, the start
    /// of the recovery that will read it, due at once.
    ///
    /// Where the new gap overlaps one the instrument already has, it starts after it instead: the
    /// kept gap covers that part and keeps its retry schedule, so no span is read twice and a
    /// reconnect never brings a retry forward. Disconnects come in time order, so an earlier part
    /// of the new gap that no kept gap covers is from before the previous disconnect, when the
    /// stream was live.
    pub(crate) fn open(
        &mut self,
        instruments: &[InstrumentNameExchange],
        disconnect_time: DateTime<Utc>,
        now: DateTime<Utc>,
    ) {
        let start_ms = disconnect_time.timestamp_millis();
        let end_ms = (now + chrono::Duration::seconds(GAP_END_SLACK_SECS)).timestamp_millis();
        let due = tokio::time::Instant::now();
        for instrument in instruments {
            let kept = self.0.get(instrument).map_or(&[][..], Vec::as_slice);
            let start_ms = kept
                .iter()
                .filter(|kept| kept.start_ms <= end_ms && start_ms <= kept.end_ms)
                .map(|kept| kept.end_ms + 1)
                .fold(start_ms, i64::max);
            if start_ms <= end_ms {
                self.0.entry(instrument.clone()).or_default().push(FillGap {
                    start_ms,
                    end_ms,
                    failures: 0,
                    due,
                });
            }
        }
    }

    /// The gaps due by `now`, to read.
    pub(crate) fn due(&self, now: tokio::time::Instant) -> Vec<(InstrumentNameExchange, FillGap)> {
        self.0
            .iter()
            .flat_map(|(instrument, gaps)| {
                gaps.iter()
                    .filter(move |gap| gap.due <= now)
                    .map(move |gap| (instrument.clone(), *gap))
            })
            .collect()
    }

    /// Whether a gap is left on `instrument`.
    pub(crate) fn covers(&self, instrument: &InstrumentNameExchange) -> bool {
        self.0.contains_key(instrument)
    }

    /// When the next gap is due, or `None` when there is none.
    pub(crate) fn next_due(&self) -> Option<tokio::time::Instant> {
        self.0.values().flatten().map(|gap| gap.due).min()
    }

    /// Whether no gap is left.
    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Make every gap due now, as if its retry delay had passed.
    #[cfg(test)]
    pub(crate) fn make_due(&mut self) {
        let now = tokio::time::Instant::now();
        for gap in self.0.values_mut().flatten() {
            gap.due = now;
        }
    }

    /// Record that `gap`'s fills have all been forwarded.
    pub(crate) fn recovered(&mut self, instrument: &InstrumentNameExchange, gap: &FillGap) {
        self.remove_where(instrument, |kept| kept.same_span(gap));
    }

    /// Record that a read of `gap` failed or did not finish at `now`: it is retried after a
    /// backoff, or dropped once it has failed [`MAX_GAP_RETRIES`] retries. Returns which, or
    /// `None` if the gap is no longer kept.
    pub(crate) fn failed(
        &mut self,
        instrument: &InstrumentNameExchange,
        gap: &FillGap,
        now: tokio::time::Instant,
    ) -> Option<GapFailure> {
        let gaps = self.0.get_mut(instrument)?;
        let index = gaps.iter().position(|kept| kept.same_span(gap))?;
        gaps[index].failures += 1;
        if gaps[index].failures > MAX_GAP_RETRIES {
            gaps.remove(index);
            if gaps.is_empty() {
                self.0.remove(instrument);
            }
            return Some(GapFailure::GivenUp);
        }
        let delay = Duration::from_secs(GAP_RETRY_BASE_SECS << (gaps[index].failures - 1));
        gaps[index].due = now + delay;
        Some(GapFailure::Retry(delay))
    }

    fn remove_where(
        &mut self,
        instrument: &InstrumentNameExchange,
        matches: impl Fn(&FillGap) -> bool,
    ) {
        if let Some(gaps) = self.0.get_mut(instrument) {
            gaps.retain(|gap| !matches(gap));
            if gaps.is_empty() {
                self.0.remove(instrument);
            }
        }
    }
}

/// A gap boundary in epoch milliseconds, as a time for a log line.
pub(crate) fn gap_time(ms: i64) -> DateTime<Utc> {
    Utc.timestamp_millis_opt(ms).single().unwrap_or_default()
}

/// Record that fill recovery on `venue` did not read `gap` of `instrument`, because of `reason`,
/// and log what follows: a retry, or, once it has failed [`MAX_GAP_RETRIES`] retries, giving the
/// gap up, which leaves its fills undelivered.
pub(crate) fn gap_failed(
    unrecovered: &mut UnrecoveredFills,
    venue: ExchangeId,
    instrument: &InstrumentNameExchange,
    gap: &FillGap,
    reason: &str,
) {
    let (start, end) = (gap_time(gap.start_ms), gap_time(gap.end_ms));
    match unrecovered.failed(instrument, gap, tokio::time::Instant::now()) {
        Some(GapFailure::Retry(delay)) => warn!(
            %venue,
            %instrument,
            %start,
            %end,
            retry_in_secs = delay.as_secs(),
            reason,
            "Binance fill recovery did not read this gap, retrying it later"
        ),
        Some(GapFailure::GivenUp) => error!(
            %venue,
            %instrument,
            %start,
            %end,
            retries = MAX_GAP_RETRIES,
            reason,
            "Binance fill recovery gave up on this gap: its fills are not delivered; read them \
             with fetch_trades"
        ),
        None => {}
    }
}

/// Drop the executions in `page` stamped after `end_ms`, and return whether there were any: a
/// walk over a span then has everything in it, since Binance returns executions in trade-id order,
/// which is time order. An execution without a time is kept.
pub(crate) fn drop_after<T: BinanceExecutionFields>(page: &mut Vec<T>, end_ms: i64) -> bool {
    let len = page.len();
    page.retain(|execution| execution.time().is_none_or(|time| time <= end_ms));
    page.len() < len
}
/// Timeout for the initial WebSocket API TCP+TLS handshake.
/// Without this, a network partition holds the write lock for up to 75–127 s
/// (OS TCP timeout), stalling all concurrent open_order/cancel_order callers.
pub(crate) const CONNECT_TIMEOUT_SECS: u64 = 15;
// Compile-time guard: fill-recovery sites cast this to i64 (`chrono::Duration::seconds`); ensure it
// never overflows.
const _: () = assert!(
    CONNECT_TIMEOUT_SECS <= i64::MAX as u64,
    "CONNECT_TIMEOUT_SECS overflows i64"
);
/// Extra lookback subtracted from Signal disconnect timestamps to cover Tokio scheduling
/// jitter between the actual WS close and when the monitor task records Utc::now().
/// The dedup cache absorbs any resulting duplicate fills.
pub(crate) const SIGNAL_RECOVERY_LOOKBACK_MS: i64 = 500;
/// Maximum trades per Binance REST query.
/// Stored as `usize` for direct use in `Vec::len()` comparisons; cast to `i32` at SDK call sites
/// (`MyTradesParams::limit(i32)`).
pub(crate) const BINANCE_MAX_TRADES: usize = 1000;
// Compile-time guard: SDK call sites cast this to i32; ensure it never overflows.
const _: () = assert!(
    BINANCE_MAX_TRADES <= i32::MAX as usize,
    "BINANCE_MAX_TRADES overflows i32"
);
/// Default delay when rate-limited (exponential backoff; Binance's `Retry-After`
/// header is not accessible through the SDK's `anyhow::Error` chain).
pub(crate) const DEFAULT_RATE_LIMIT_DELAY_SECS: u64 = 10;
/// Maximum number of REST retry attempts on rate-limit errors.
pub(crate) const MAX_RATE_LIMIT_RETRIES: u32 = 3;

// ---------------------------------------------------------------------------
// WS-API user-data frames
// ---------------------------------------------------------------------------

/// A text frame from a WS-API user-data subscription (`userDataStream.subscribe*`), as
/// binance-sdk hands it to a `subscribe_on_ws_events` callback: the raw frame, unchanged.
#[derive(Debug, PartialEq)]
pub(crate) enum UserDataFrame<'a> {
    /// An RPC response, such as the subscribe acknowledgement: it carries a top-level `id`.
    Response,
    /// A pushed event, which Binance wraps as `{ "subscriptionId", "event": { "e", .. } }`.
    Event {
        /// The subscription the event belongs to; it routes isolated-margin events.
        subscription_id: Option<i64>,
        /// The inner event's `e` tag.
        event_type: &'a str,
        /// The inner event, unparsed, for the matched branch's one typed pass.
        event: &'a str,
    },
    /// A frame shape this client does not know: neither of the above, or an event whose `e` tag
    /// is missing or not a plain string. Its caller logs it through [`log_unrecognised_frame`],
    /// since a change in how Binance delivers events would otherwise drop every event silently.
    Unrecognised,
}

/// Split a WS-API user-data frame into a [`UserDataFrame`].
///
/// Reads borrowed views only, with no `serde_json::Value` DOM: this runs on every inbound frame.
/// The inner event stays an unparsed slice, and only its `e` tag is read here.
pub(crate) fn parse_user_data_frame(frame: &str) -> UserDataFrame<'_> {
    use serde_json::value::RawValue;

    #[derive(serde::Deserialize)]
    struct Envelope<'a> {
        #[serde(borrow, default)]
        id: Option<&'a RawValue>,
        #[serde(borrow, default)]
        event: Option<&'a RawValue>,
        #[serde(rename = "subscriptionId", default)]
        subscription_id: Option<i64>,
    }
    // Discriminator-only view of the inner event: reads `e` without materialising the payload.
    // Binance places `e` first, so this is cheap next to the typed pass the caller then makes on
    // the matched branch only. A manual byte scan for `"e"` would be fragile to whitespace,
    // escaping and key order, so keep the typed two-pass read.
    #[derive(serde::Deserialize)]
    struct EventTag<'a> {
        #[serde(borrow, default)]
        e: Option<&'a str>,
    }

    let Ok(envelope) = serde_json::from_str::<Envelope<'_>>(frame) else {
        return UserDataFrame::Unrecognised;
    };
    if envelope.id.is_some() {
        return UserDataFrame::Response;
    }
    let Some(event) = envelope.event else {
        return UserDataFrame::Unrecognised;
    };
    let event = event.get();
    // A missing tag, or one that cannot be borrowed (escaped, or not a string), is unrecognised
    // rather than an empty type: an unknown type is ignored quietly, and this must not be.
    let Some(event_type) = serde_json::from_str::<EventTag<'_>>(event)
        .ok()
        .and_then(|tag| tag.e)
    else {
        return UserDataFrame::Unrecognised;
    };
    UserDataFrame::Event {
        subscription_id: envelope.subscription_id,
        event_type,
        event,
    }
}

/// Log an [`UserDataFrame::Unrecognised`] frame: at `warn` for the first, and for every 1000th
/// after it with the running count, and at `trace` otherwise. So a change in delivery that makes
/// every frame unrecognised is seen at once, without a warning per frame.
///
/// `seen` is the caller's process-wide counter, one per venue: it is shared by every stream of
/// that venue and never resets, so a later stream or outage warns again only at the next
/// thousandth frame, not on its first.
pub(crate) fn log_unrecognised_frame(venue: &'static str, seen: &AtomicU64, frame: &str) {
    let count = seen.fetch_add(1, Ordering::Relaxed) + 1;
    if count == 1 || count.is_multiple_of(1000) {
        warn!(
            venue,
            count,
            frame = frame_excerpt(frame),
            "Binance WS: unrecognised user-data frame (not an RPC response, nor an event envelope \
             with a readable `e` tag), ignoring it; further ones are logged at trace, with a \
             warning every 1000th"
        );
    } else {
        trace!(
            venue,
            count,
            frame = frame_excerpt(frame),
            "Binance WS: unrecognised user-data frame, ignoring it"
        );
    }
}

/// The first 200 characters of a frame, for a log line about it.
fn frame_excerpt(frame: &str) -> &str {
    frame
        .char_indices()
        .nth(200)
        .map_or(frame, |(end, _)| &frame[..end])
}

// ---------------------------------------------------------------------------
// Rate limit tracker
// ---------------------------------------------------------------------------

/// Binance's documented spot request-weight limit per minute. REST `/api` and the WS-API draw on
/// it together, per IP. A spot tracker starts from this value and replaces it with the limit each
/// WS-API response reports.
pub(crate) const SPOT_REQUEST_WEIGHT_PER_MINUTE: u32 = 6_000;
/// Binance's documented per-IP weight limit per minute for IP-weighted `/sapi` endpoints. No
/// response reports it, so a margin tracker keeps this value. The separate per-UID `/sapi` limit
/// is not tracked.
pub(crate) const SAPI_IP_WEIGHT_PER_MINUTE: u32 = 12_000;
/// Used weight, as a percentage of the per-minute limit, at which queries pause until the next
/// minute. Orders and cancels keep the remainder.
const WEIGHT_PAUSE_PERCENT: u64 = 90;
/// Added to the wait for the next minute to cover clock skew between this host and Binance, whose
/// weight counters reset on its own minute boundary.
const MINUTE_BOUNDARY_SLACK_MS: u64 = 1_000;

/// The Binance per-minute request-weight pool a [`RateLimitTracker`] follows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WeightPool {
    /// Spot REST `/api` and the spot WS-API, which share one per-IP pool. A REST response reports
    /// the weight used in `x-mbx-used-weight-1m`; a WS-API response reports it with the limit.
    Spot,
    /// IP-weighted margin `/sapi` endpoints, which report the weight used in
    /// `x-sapi-used-ip-weight-1m`.
    Sapi,
}

impl WeightPool {
    fn default_limit(self) -> u32 {
        match self {
            Self::Spot => SPOT_REQUEST_WEIGHT_PER_MINUTE,
            Self::Sapi => SAPI_IP_WEIGHT_PER_MINUTE,
        }
    }
}

/// What a request does, which decides the rate-limit waits it honours.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RequestKind {
    /// Places or cancels an order. Waits only for a rate-limit cooldown, so it can use the weight
    /// left once queries pause.
    Order,
    /// Reads account or order state. Also waits while the weight used is near the limit.
    Query,
    /// Reads what a reconnect's fill recovery needs, or what that recovery depends on. Like
    /// [`Order`](Self::Order), waits only for a rate-limit cooldown: the pause near the limit can
    /// outlast the recovery's time budget, which would leave its fills to a retry minutes later,
    /// or undelivered once every retry has failed.
    Essential,
}

#[derive(Default)]
struct Deadlines {
    /// Set by a rate-limit response. Every request waits for it.
    blocked_until: Option<tokio::time::Instant>,
    /// Set when the weight used nears the per-minute limit. Only queries wait for it.
    throttled_until: Option<tokio::time::Instant>,
}

/// Tracks rate-limit state across REST API calls.
///
/// Two waits, both shared by clones of the client (which share one `Arc<RateLimitTracker>`):
/// - a cooldown after Binance answered with a rate-limit error, which every request honours;
/// - a pause until the next minute once the weight used reaches [`WEIGHT_PAUSE_PERCENT`] of the
///   per-minute limit, which only [`RequestKind::Query`] requests honour, so the weight left
///   stays free for orders, cancels and [`RequestKind::Essential`] reads. The weight used comes
///   from successful responses ([`observe_rest`](Self::observe_rest),
///   [`observe_ws_api`](Self::observe_ws_api)); a rejected request's response is not observed,
///   so usage can only be under-counted.
pub(crate) struct RateLimitTracker {
    // parking_lot::Mutex — never poisons, consistent with SharedDedupCache
    deadlines: parking_lot::Mutex<Deadlines>,
    pool: WeightPool,
    /// The pool's per-minute weight limit.
    weight_limit: AtomicU32,
}

impl RateLimitTracker {
    pub(crate) fn new(pool: WeightPool) -> Self {
        Self {
            deadlines: parking_lot::Mutex::new(Deadlines::default()),
            pool,
            weight_limit: AtomicU32::new(pool.default_limit()),
        }
    }

    /// Sleep until the waits `kind` honours have passed. Returns immediately if none is active.
    ///
    /// Loops after waking to re-check the deadline: another task may have extended it while this
    /// task was sleeping.
    pub(crate) async fn wait_if_blocked(&self, kind: RequestKind) {
        loop {
            let deadline = {
                let deadlines = self.deadlines.lock();
                match kind {
                    RequestKind::Order | RequestKind::Essential => deadlines.blocked_until,
                    // `None` orders below `Some`, so this is whichever deadline is later.
                    RequestKind::Query => deadlines.blocked_until.max(deadlines.throttled_until),
                }
            };
            match deadline {
                None => return,
                Some(until) => {
                    let now = tokio::time::Instant::now();
                    if until <= now {
                        return;
                    }
                    // debug! not warn! — on_rate_limited and observe_used_weight already log
                    // the event; multiple concurrent callers all waiting during recover_fills
                    // would otherwise flood the log with identical lines.
                    // as_millis() returns u128; truncation impossible (u64::MAX ms ≈ 584M years)
                    #[allow(clippy::cast_possible_truncation)]
                    let delay_ms = (until - now).as_millis() as u64;
                    debug!(delay_ms, ?kind, "Binance request held back, waiting");
                    tokio::time::sleep_until(until).await;
                }
            }
        }
    }

    /// The per-minute weight limit the pause is measured against.
    #[cfg(test)]
    pub(crate) fn weight_limit(&self) -> u32 {
        self.weight_limit.load(Ordering::Relaxed)
    }

    /// Pause queries for `pause`, as a response near the weight limit would.
    #[cfg(test)]
    pub(crate) fn throttle(&self, pause: Duration) {
        let now = tokio::time::Instant::now();
        extend_deadline(&mut self.deadlines.lock().throttled_until, now, pause);
    }

    /// Whether a rate-limit cooldown is active now.
    pub(crate) fn is_blocked(&self) -> bool {
        let now = tokio::time::Instant::now();
        self.deadlines
            .lock()
            .blocked_until
            .is_some_and(|until| until > now)
    }

    /// Record a rate-limit event. Extends the cooldown if a longer one is already active.
    ///
    /// # Panics
    ///
    /// Panics if called outside a Tokio runtime context (`tokio::time::Instant::now()`
    /// requires an active runtime).
    pub(crate) fn on_rate_limited(&self, retry_after: Option<Duration>) {
        let delay = retry_after.unwrap_or(Duration::from_secs(DEFAULT_RATE_LIMIT_DELAY_SECS));
        let now = tokio::time::Instant::now();
        let was_blocked = extend_deadline(&mut self.deadlines.lock().blocked_until, now, delay);
        // only warn on mode entry; subsequent calls from the retry loop extend
        // the cooldown silently to avoid duplicate "entering degradation mode" lines.
        if was_blocked {
            debug!(
                delay_secs = delay.as_secs(),
                "Binance rate-limit cooldown extended"
            );
        } else {
            warn!(
                delay_secs = delay.as_secs(),
                "Binance entering rate-limit degradation mode"
            );
        }
    }

    /// Record the weight a successful REST response reports as used, for a request sent at
    /// `sent_ms` (from [`unix_ms`]).
    ///
    /// Reads the header of this tracker's [`WeightPool`]; a response without it changes nothing.
    pub(crate) fn observe_rest<D>(&self, response: &RestApiResponse<D>, sent_ms: u128) {
        let used = match self.pool {
            // The SDK has already parsed `x-mbx-used-weight-1m` into `rate_limits`.
            WeightPool::Spot => response
                .rate_limits
                .iter()
                .flatten()
                .find(|limit| {
                    is_weight_per_minute(
                        &limit.rate_limit_type,
                        &limit.interval,
                        limit.interval_num,
                    )
                })
                .map(|limit| limit.count),
            // The SDK parses only `x-mbx-*` headers. reqwest names headers in lowercase.
            WeightPool::Sapi => response
                .headers
                .get("x-sapi-used-ip-weight-1m")
                .and_then(|used| used.parse().ok()),
        };
        if let Some(used) = used {
            self.observe_used_weight(used, sent_ms);
        }
    }

    /// Record the weight limit and the weight used that a WS-API response reports, for a request
    /// sent at `sent_ms` (from [`unix_ms`]).
    ///
    /// The WS-API draws on the spot pool, so a margin tracker ignores these.
    pub(crate) fn observe_ws_api(
        &self,
        rate_limits: Option<&[WebsocketApiRateLimit]>,
        sent_ms: u128,
    ) {
        debug_assert_eq!(self.pool, WeightPool::Spot, "WS-API usage is spot weight");
        if self.pool != WeightPool::Spot {
            return;
        }
        let Some(weight) = rate_limits.into_iter().flatten().find(|limit| {
            is_weight_per_minute(&limit.rate_limit_type, &limit.interval, limit.interval_num)
        }) else {
            return;
        };
        // A limit of 0 would pause every query; keep the last good value instead. It rarely
        // changes, so it is only written when it does.
        let previous = self.weight_limit.load(Ordering::Relaxed);
        if weight.limit > 0 && weight.limit != previous {
            self.weight_limit.store(weight.limit, Ordering::Relaxed);
            debug!(
                previous,
                limit = weight.limit,
                "Binance request-weight limit per minute updated"
            );
        }
        self.observe_used_weight(weight.count, sent_ms);
    }

    /// Pause queries until the end of the minute the request was sent in, `sent_ms`, if `used`
    /// has reached [`WEIGHT_PAUSE_PERCENT`] of the limit.
    ///
    /// Binance counts a request in the minute it receives it, which is the minute it was sent in
    /// barring clock skew and a request in flight across the boundary. A response observed after
    /// that minute has ended describes a counter that has since reset, so it pauses nothing,
    /// rather than holding queries back for the whole of the next minute.
    fn observe_used_weight(&self, used: u32, sent_ms: u128) {
        let limit = self.weight_limit.load(Ordering::Relaxed);
        if u64::from(used) * 100 < u64::from(limit) * WEIGHT_PAUSE_PERCENT {
            return;
        }
        let Some(pause) = pause_after(sent_ms, unix_ms()) else {
            return;
        };
        let now = tokio::time::Instant::now();
        let was_throttled = extend_deadline(&mut self.deadlines.lock().throttled_until, now, pause);
        // info! not warn!: nothing was refused. The pause keeps the next query from being refused.
        if !was_throttled {
            // as_millis() returns u128; truncation impossible (at most ~61 s here)
            #[allow(clippy::cast_possible_truncation)]
            let pause_ms = pause.as_millis() as u64;
            info!(
                used,
                limit,
                pause_ms,
                "Binance request weight near the per-minute limit, pausing queries until the next minute"
            );
        }
    }

    // no clear() method — cooldowns expire naturally via wait_if_blocked().
    // A previous unconditional clear() on success raced with concurrent calls:
    // call A succeeds → clears cooldown → call B's 429 cooldown is erased.
}

/// Push `deadline` out to `now + delay` unless it is already later, and return whether it was
/// still active at `now`.
fn extend_deadline(
    deadline: &mut Option<tokio::time::Instant>,
    now: tokio::time::Instant,
    delay: Duration,
) -> bool {
    let was_active = deadline.is_some_and(|until| until > now);
    let new_deadline = now + delay;
    *deadline = Some(deadline.map_or(new_deadline, |existing| existing.max(new_deadline)));
    was_active
}

/// Whether a reported rate limit is the per-minute request weight, the one the pause follows.
fn is_weight_per_minute(kind: &RateLimitType, interval: &Interval, interval_num: u32) -> bool {
    *kind == RateLimitType::RequestWeight && *interval == Interval::Minute && interval_num == 1
}

/// The current time in Unix milliseconds, the clock a weight observation's send time is taken
/// from.
pub(crate) fn unix_ms() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

/// How long queries pause, observed at `now_ms`, for a request sent at `sent_ms` (both Unix
/// milliseconds): until just after the UTC minute it was sent in ends, or `None` once it has.
fn pause_after(sent_ms: u128, now_ms: u128) -> Option<Duration> {
    let resume_ms = sent_ms - sent_ms % 60_000 + 60_000 + u128::from(MINUTE_BOUNDARY_SLACK_MS);
    let remaining_ms = resume_ms.checked_sub(now_ms).filter(|&ms| ms > 0)?;
    // At most a minute plus the slack, so it fits in a u64.
    #[allow(clippy::cast_possible_truncation)]
    Some(Duration::from_millis(remaining_ms as u64))
}

/// Check if an anyhow::Error from binance-sdk REST is a rate-limit error.
/// Covers HTTP 429 / -1003 (WAF/queue overflow: requests rejected before execution),
/// -1015 (IP rate-limit ban), and a WAF 403 (`ConnectorError::ForbiddenError` without an auth
/// code or wording). All warrant the same backoff response.
pub(crate) fn is_rate_limit_error(e: &anyhow::Error) -> bool {
    // A WAF 403 carries none of the texts below; recognise it by type so it is backed off too.
    if let Some(ConnectorError::ForbiddenError { msg, code }) = e.downcast_ref::<ConnectorError>()
        && is_waf_block(&with_code(msg, *code))
    {
        return true;
    }
    // iterate the error chain and match against the actual Display strings
    // from binance-sdk's TooManyRequestsError and RateLimitBanError variants.
    // Note: cause.to_string() allocates per chain entry — acceptable since this
    // only runs on error paths.
    for cause in e.chain() {
        let msg = cause.to_string();
        if msg.contains("Too many requests")
            || msg.contains("been banned for exceeding rate limits")
            || has_rate_limit_error_code(&msg)
        {
            return true;
        }
    }
    false
}

/// Whether a WebSocket connect failed because Binance refused the handshake for rate limiting:
/// HTTP 429, or 418 for an IP ban.
///
/// depends on the SDK reporting a refused handshake as `WebsocketError::Handshake` holding
/// tungstenite's `HTTP error: <status>` text (not a public API contract).
pub(crate) fn is_handshake_rate_limit(e: &anyhow::Error) -> bool {
    e.chain().any(|cause| {
        matches!(
            cause.downcast_ref::<WebsocketError>(),
            Some(WebsocketError::Handshake(msg))
                if msg.contains("HTTP error: 429") || msg.contains("HTTP error: 418")
        )
    })
}

// ---------------------------------------------------------------------------
// Exponential backoff
// ---------------------------------------------------------------------------

pub(crate) struct ExponentialBackoff {
    attempt: u32,
    max_attempts: u32,
    initial_ms: u64,
    max_ms: u64,
}

impl ExponentialBackoff {
    pub(crate) fn new() -> Self {
        Self {
            attempt: 0,
            max_attempts: MAX_RECONNECT_ATTEMPTS,
            initial_ms: INITIAL_BACKOFF_MS,
            max_ms: MAX_BACKOFF_MS,
        }
    }

    pub(crate) fn reset(&mut self) {
        self.attempt = 0;
    }

    /// Number of reconnect attempts consumed so far.
    ///
    /// After [`wait`](Self::wait) has returned `false` this equals `max_attempts` — i.e. the
    /// number of attempts made before the budget was exhausted. Used to populate
    /// [`crate::error::StreamTerminationReason::ReconnectBudgetExhausted`].
    pub(crate) fn attempts(&self) -> u32 {
        self.attempt
    }

    /// Wait for the current backoff duration. Returns `false` if max attempts exhausted.
    pub(crate) async fn wait(&mut self) -> bool {
        if self.attempt >= self.max_attempts {
            return false;
        }
        let delay_ms = self
            .initial_ms
            .saturating_mul(2u64.saturating_pow(self.attempt))
            .min(self.max_ms);
        self.attempt += 1;
        debug!(
            attempt = self.attempt,
            max = self.max_attempts,
            delay_ms,
            "Binance reconnect backoff"
        );
        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
        true
    }
}

// ---------------------------------------------------------------------------
// Parsing helpers (Binance wire strings → rustrade enums)
// ---------------------------------------------------------------------------

pub(crate) fn parse_side(s: &str) -> Option<Side> {
    match s {
        "BUY" => Some(Side::Buy),
        "SELL" => Some(Side::Sell),
        _ => {
            warn!(side = s, "unknown Binance order side");
            None
        }
    }
}

pub(crate) fn parse_order_kind(t: &str) -> Option<OrderKind> {
    match t {
        "MARKET" => Some(OrderKind::Market),
        // STOP_LOSS and TAKE_PROFIT are conditional orders that Binance
        // triggers at a stop price. Barter's OrderKind has no conditional variant.
        // Drop them: mapping to Market would misrepresent conditional orders as
        // immediately executable in snapshots, which is more dangerous than omitting.
        "STOP_LOSS" | "TAKE_PROFIT" => {
            warn!(
                order_type = t,
                "dropping conditional Binance order type (no OrderKind equivalent)"
            );
            None
        }
        // STOP_LOSS_LIMIT and TAKE_PROFIT_LIMIT are conditional orders
        // with a limit price that enter the book only after a stop trigger.
        // Mapping to Limit is imprecise (treats them as resting limit orders)
        // but preserves visibility into open orders. Dropping them (like the
        // pure stop variants above) would lose order tracking entirely.
        "LIMIT" | "LIMIT_MAKER" | "STOP_LOSS_LIMIT" | "TAKE_PROFIT_LIMIT" => Some(OrderKind::Limit),
        _ => {
            warn!(order_type = t, "unsupported Binance order type");
            None
        }
    }
}

pub(crate) fn parse_time_in_force(tif: &str) -> TimeInForce {
    match tif {
        "GTC" => TimeInForce::GoodUntilCancelled { post_only: false },
        "GTX" => TimeInForce::GoodUntilCancelled { post_only: true },
        "IOC" => TimeInForce::ImmediateOrCancel,
        "FOK" => TimeInForce::FillOrKill,
        "GTD" => TimeInForce::GoodUntilEndOfDay,
        _ => {
            warn!(
                time_in_force = tif,
                "unknown Binance TimeInForce, defaulting to GTC"
            );
            TimeInForce::GoodUntilCancelled { post_only: false }
        }
    }
}

// ---------------------------------------------------------------------------
// Order-response conversion (REST open-order / all-order endpoints)
// ---------------------------------------------------------------------------

/// The subset of a Binance order-response struct that [`convert_open_order`] and
/// [`convert_ended_order`] read.
///
/// binance-sdk generates a *distinct* response type per endpoint, with no shared trait:
/// `AllOrdersResponseInner`, `GetOpenOrdersResponseInner` and `GetOrderResponse` on spot,
/// `QueryMarginAccountsOpenOrdersResponseInner` and `QueryMarginAccountsOrderResponse` on margin.
/// Those structs are emphatically not
/// interchangeable -- the spot family carries substantially more fields than the margin one, and
/// several fields share a name while differing in type -- but the thirteen named here are
/// identical in name and type across all of them.
///
/// Naming that read subset is what makes a single converter safe to share. The dependency surface
/// is explicit, so an SDK change to any *other* field cannot silently alter order parsing, and a
/// correction to the conversion reaches every Binance client at once instead of one of them.
pub(crate) trait BinanceOrderFields {
    fn order_id(&self) -> Option<i64>;
    fn client_order_id(&self) -> Option<&str>;
    fn side(&self) -> Option<&str>;
    fn price(&self) -> Option<&str>;
    fn orig_qty(&self) -> Option<&str>;
    fn executed_qty(&self) -> Option<&str>;
    fn order_type(&self) -> Option<&str>;
    fn time_in_force(&self) -> Option<&str>;
    fn time(&self) -> Option<i64>;
    /// When the order last changed state, as opposed to [`BinanceOrderFields::time`], which is when
    /// it was created. Every endpoint implemented below carries it.
    fn update_time(&self) -> Option<i64>;
    fn symbol(&self) -> Option<&str>;
    /// The order's status. `openOrders` serves only live orders, but `allOrders` also serves
    /// cancelled, expired and filled ones through the same accessors, so [`convert_open_order`]
    /// reads it rather than trusting the endpoint.
    fn status(&self) -> Option<&str>;
    /// The quote quantity the order has traded so far, spelled `cummulativeQuoteQty` by Binance.
    fn cumulative_quote_qty(&self) -> Option<&str>;
}

/// Implement [`BinanceOrderFields`] for SDK response types that share these field names.
///
/// Every struct listed below declares these thirteen fields with the same types, so the accessors
/// are identical; a macro keeps them from drifting apart under hand-editing.
macro_rules! impl_binance_order_fields {
    ($($t:ty),* $(,)?) => {
        $(
            impl BinanceOrderFields for $t {
                fn order_id(&self) -> Option<i64> { self.order_id }
                fn client_order_id(&self) -> Option<&str> { self.client_order_id.as_deref() }
                fn side(&self) -> Option<&str> { self.side.as_deref() }
                fn price(&self) -> Option<&str> { self.price.as_deref() }
                fn orig_qty(&self) -> Option<&str> { self.orig_qty.as_deref() }
                fn executed_qty(&self) -> Option<&str> { self.executed_qty.as_deref() }
                fn order_type(&self) -> Option<&str> { self.r#type.as_deref() }
                fn time_in_force(&self) -> Option<&str> { self.time_in_force.as_deref() }
                fn time(&self) -> Option<i64> { self.time }
                fn update_time(&self) -> Option<i64> { self.update_time }
                fn symbol(&self) -> Option<&str> { self.symbol.as_deref() }
                fn status(&self) -> Option<&str> { self.status.as_deref() }
                fn cumulative_quote_qty(&self) -> Option<&str> {
                    self.cummulative_quote_qty.as_deref()
                }
            }
        )*
    };
}

impl_binance_order_fields!(
    binance_sdk::spot::rest_api::AllOrdersResponseInner,
    binance_sdk::spot::rest_api::GetOpenOrdersResponseInner,
    binance_sdk::spot::rest_api::GetOrderResponse,
    binance_sdk::margin_trading::rest_api::QueryMarginAccountsOpenOrdersResponseInner,
    binance_sdk::margin_trading::rest_api::QueryMarginAccountsOrderResponse,
);

/// Whether a REST order response's status says the order is resting at the exchange, and may
/// therefore become an `Open` order.
///
/// `NEW` and `PARTIALLY_FILLED` are working orders. `PENDING_NEW` is an order-list leg that waits
/// for its working order to fill; it is at the exchange and `openOrders` returns it, so it is live
/// too. Every other status is terminal (`FILLED`, `CANCELED`, `REJECTED`, `EXPIRED`,
/// `EXPIRED_IN_MATCH`), unused (`PENDING_CANCEL`), or unknown to this version.
///
/// This is an allow-list for the same reason [`trade_order_is_live`] is one: an `Open` snapshot of
/// an order that is no longer live resurrects it. A cancelled order that had partly filled would
/// rest in engine state with quantity remaining, and nothing at the exchange would ever fill or
/// cancel it.
fn rest_order_is_open(status: &str) -> bool {
    matches!(status, "NEW" | "PARTIALLY_FILLED" | "PENDING_NEW")
}

/// Convert a Binance open order into rustrade's `Open` state order.
///
/// Returns `None`, with a warning, for an order whose status is not live (see
/// [`rest_order_is_open`]) or is missing, whichever endpoint served it. `openOrders` serves only
/// live orders, so there this never fires in practice. An order that has ended is read with
/// [`convert_ended_order`] instead.
///
/// `exchange` stamps the resulting [`OrderKey`] and every diagnostic below, so one
/// implementation serves each Binance client without a venue name baked into its warnings.
pub(crate) fn convert_open_order<T: BinanceOrderFields>(
    o: &T,
    exchange: ExchangeId,
    instrument: &InstrumentNameExchange,
) -> Option<Order<ExchangeId, InstrumentNameExchange, Open>> {
    match o.status() {
        Some(status) if rest_order_is_open(status) => {}
        Some(status) => {
            warn!(%exchange, %instrument, order_id = ?o.order_id(), status, "Binance order is not live, not converting it to an open order");
            return None;
        }
        None => {
            warn!(%exchange, %instrument, order_id = ?o.order_id(), "Binance open order missing status");
            return None;
        }
    }
    let row = convert_order_row(o, exchange, instrument)?;
    Some(row.map_state(|row| {
        Open::new(
            VenueOrderId::Assigned(row.order_id),
            row.time_exchange,
            row.filled_qty,
        )
    }))
}

/// How an order that has ended did end, from a REST order row such as `GET /api/v3/order`'s or
/// `GET /sapi/v1/margin/order`'s, under `key`, the key it was asked for.
///
/// `FILLED` becomes [`InactiveOrderState::FullyFilled`], with the average price worked out from
/// `cummulativeQuoteQty`, or none where Binance reports that negative (not available for some
/// historical orders); `CANCELED` becomes [`InactiveOrderState::Cancelled`], `EXPIRED` and
/// `EXPIRED_IN_MATCH` (self-trade prevention) [`InactiveOrderState::Expired`], each with what
/// filled before; and `REJECTED` becomes [`InactiveOrderState::OpenFailed`].
///
/// Returns `None` for an order still live, and, with a warning, for a row whose status is missing
/// or unknown or that cannot be converted. A caller reads `None` as "not ended", so such an order
/// is asked about again later rather than retired on a guess.
pub(crate) fn convert_ended_order<T: BinanceOrderFields>(
    o: &T,
    exchange: ExchangeId,
    key: &UnindexedOrderKey,
) -> Option<UnindexedInactiveOrder> {
    let instrument = &key.instrument;
    let Some(status) = o.status() else {
        warn!(%exchange, %instrument, cid = %key.cid, "Binance order missing status");
        return None;
    };
    if rest_order_is_open(status) {
        return None;
    }
    if !matches!(
        status,
        "FILLED" | "CANCELED" | "EXPIRED" | "EXPIRED_IN_MATCH" | "REJECTED"
    ) {
        warn!(%exchange, %instrument, cid = %key.cid, status, "Binance order has a status this version does not know, treating it as not ended");
        return None;
    }
    let row = convert_order_row(o, exchange, instrument)?;
    let avg_price = binance_avg_price(exchange, o.cumulative_quote_qty(), row.state.filled_qty);
    let state = ended_order_state(
        status,
        row.state.order_id.clone(),
        row.state.time_exchange,
        row.state.filled_qty,
        avg_price,
    )?;
    let mut order = row.map_state(|_| state);
    order.key = key.clone();
    Some(order)
}

/// How an order ended, from its Binance `status`, or `None` for a status that does not end it.
///
/// `FILLED` becomes [`InactiveOrderState::FullyFilled`] with `avg_price`; `CANCELED` becomes
/// [`InactiveOrderState::Cancelled`], `EXPIRED` and `EXPIRED_IN_MATCH` (self-trade prevention)
/// [`InactiveOrderState::Expired`], each with what filled before; and `REJECTED` becomes
/// [`InactiveOrderState::OpenFailed`]. Shared by the lookup of an order that ended
/// ([`convert_ended_order`]) and the response to placing one ([`placed_order_state`]).
pub(crate) fn ended_order_state<AssetKey, InstrumentKey>(
    status: &str,
    order_id: OrderId,
    time_exchange: DateTime<Utc>,
    filled_qty: Decimal,
    avg_price: Option<Decimal>,
) -> Option<InactiveOrderState<AssetKey, InstrumentKey>> {
    Some(match status {
        "FILLED" => InactiveOrderState::FullyFilled(Filled::new(
            order_id,
            time_exchange,
            filled_qty,
            avg_price,
        )),
        "CANCELED" => {
            InactiveOrderState::Cancelled(Cancelled::new(order_id, time_exchange, filled_qty))
        }
        "EXPIRED" | "EXPIRED_IN_MATCH" => {
            InactiveOrderState::Expired(Expired::new(order_id, time_exchange, filled_qty))
        }
        "REJECTED" => InactiveOrderState::OpenFailed(OrderError::Rejected(
            ApiError::OrderRejected(format!("Binance rejected order {order_id}")),
        )),
        _ => return None,
    })
}

/// The state the response to placing an order reports, from its `status`, `executedQty`
/// (`filled_qty`) and `cummulativeQuoteQty`.
///
/// An order that ended in the response itself is reported as it ended (see
/// [`ended_order_state`]): an IOC or FOK order that found no liquidity, or partly filled and
/// expired the rest, is `Expired` with what filled, not `Open`. A live status (`NEW`,
/// `PARTIALLY_FILLED`, `PENDING_NEW`), or none, as in an `ACK` response, reads from what filled:
/// `FullyFilled` once it covers `quantity`, otherwise `Open`. So does a status this version does
/// not know, with a warning, since the account stream reports the order's next state either way.
#[allow(clippy::too_many_arguments)] // Each is a distinct field of the response; a struct would only rename them.
pub(crate) fn placed_order_state(
    exchange: ExchangeId,
    instrument: &InstrumentNameExchange,
    status: Option<&str>,
    order_id: OrderId,
    time_exchange: DateTime<Utc>,
    filled_qty: Decimal,
    quantity: Decimal,
    cumulative_quote_qty: Option<&str>,
) -> UnindexedOrderState {
    let avg_price = || binance_avg_price(exchange, cumulative_quote_qty, filled_qty);
    if let Some(status) = status {
        if let Some(ended) = ended_order_state(
            status,
            order_id.clone(),
            time_exchange,
            filled_qty,
            avg_price(),
        ) {
            return OrderState::Inactive(ended);
        }
        if !rest_order_is_open(status) {
            warn!(%exchange, %instrument, %order_id, status, "Binance placed an order with a status this version does not know, reading it from what filled");
        }
    }
    if filled_qty >= quantity {
        OrderState::fully_filled(Filled::new(
            order_id,
            time_exchange,
            filled_qty,
            avg_price(),
        ))
    } else {
        OrderState::active(Open::new(
            VenueOrderId::Assigned(order_id),
            time_exchange,
            filled_qty,
        ))
    }
}

/// The average price of an order's fills: the quote traded (`cummulativeQuoteQty`) over the base
/// traded (`filled_qty`).
///
/// `None` when nothing filled or the quote is missing, when Binance reports it negative (its
/// "not available", for some historical orders), and, with a warning, when it does not parse.
pub(crate) fn binance_avg_price(
    exchange: ExchangeId,
    cumulative_quote_qty: Option<&str>,
    filled_qty: Decimal,
) -> Option<Decimal> {
    if filled_qty.is_zero() {
        return None;
    }
    let quote = cumulative_quote_qty?;
    match Decimal::from_str(quote) {
        Ok(quote) if quote.is_sign_negative() => None,
        Ok(quote) => quote.checked_div(filled_qty),
        Err(_) => {
            warn!(%exchange, cummulative_quote_qty = quote, "Binance: failed to parse cummulativeQuoteQty; average price unavailable");
            None
        }
    }
}

/// What a REST order row says about the order's state, whatever its status.
#[derive(Debug)]
struct OrderRow {
    order_id: OrderId,
    /// When the order last changed state.
    time_exchange: DateTime<Utc>,
    filled_qty: Decimal,
}

/// Convert the fields of a REST order row that do not depend on its status, shared by
/// [`convert_open_order`] and [`convert_ended_order`]. Returns `None`, with a warning, when a field
/// the order cannot be described without is missing or unparseable.
fn convert_order_row<T: BinanceOrderFields>(
    o: &T,
    exchange: ExchangeId,
    instrument: &InstrumentNameExchange,
) -> Option<Order<ExchangeId, InstrumentNameExchange, OrderRow>> {
    let Some(order_id_raw) = o.order_id() else {
        warn!(%exchange, %instrument, "Binance order missing orderId");
        return None;
    };
    let order_id = OrderId(format_smolstr!("{}", order_id_raw));
    if o.client_order_id().is_none() {
        warn!(%exchange, %instrument, order_id = %order_id_raw, "Binance order missing clientOrderId, using orderId as fallback — order may not reconcile with engine state");
    }
    let cid = ClientOrderId::new(
        o.client_order_id()
            .unwrap_or(&format_smolstr!("{}", order_id_raw)),
    );
    let side = match o.side() {
        // parse_side already logs a warning on unknown values
        Some(s) => parse_side(s)?,
        None => {
            warn!(%exchange, %instrument, order_id = %order_id_raw, "Binance order missing side");
            return None;
        }
    };
    let price = o.price().and_then(|s| Decimal::from_str(s).ok());
    let quantity = match o.orig_qty().and_then(|s| Decimal::from_str(s).ok()) {
        Some(v) => v,
        None => {
            warn!(%exchange, %instrument, order_id = %order_id_raw, "Binance order missing/unparseable origQty");
            return None;
        }
    };
    let filled_qty = match o.executed_qty() {
        Some(s) => match Decimal::from_str(s) {
            Ok(v) => v,
            Err(_) => {
                warn!(%exchange, %instrument, order_id = %order_id_raw, executed_qty = s, "Binance order unparseable executedQty, defaulting to 0");
                Decimal::ZERO
            }
        },
        None => Decimal::ZERO,
    };
    let kind = match o.order_type() {
        // parse_order_kind already logs a warning on unknown values
        Some(t) => parse_order_kind(t)?,
        None => {
            warn!(%exchange, %instrument, order_id = %order_id_raw, "Binance order missing type");
            return None;
        }
    };
    let time_in_force = parse_time_in_force(o.time_in_force().unwrap_or("GTC"));
    // `update_time` over `time`: `Open::time_exchange` orders an order's states, and the engine
    // discards a snapshot older than the state it already tracks. `time` is the creation stamp and
    // is identical across every snapshot of one order, so a snapshot carrying it is discarded the
    // moment a WebSocket fill has advanced the tracked order past creation -- which is exactly the
    // partially-filled order this fetch exists to reconcile. For an order that has ended it is
    // when it ended.
    let time_exchange = match o
        .update_time()
        .or_else(|| o.time())
        .and_then(|ms| Utc.timestamp_millis_opt(ms).single())
    {
        Some(ts) => ts,
        None => {
            warn!(%exchange, %instrument, order_id = %order_id_raw, "Binance order missing/unparseable time, using now");
            Utc::now()
        }
    };

    Some(Order {
        key: OrderKey::new(
            exchange,
            instrument.clone(),
            // Binance doesn't carry strategy IDs in any response field.
            // Callers must reconcile orders by ClientOrderId or OrderId — never StrategyId.
            StrategyId::unknown(),
            cid,
        ),
        side,
        price,
        quantity,
        kind,
        time_in_force,
        state: OrderRow {
            order_id,
            time_exchange,
            filled_qty,
        },
    })
}

/// One symbol's `openOrders` rows after conversion.
#[derive(Debug)]
pub(crate) struct OpenOrderListing {
    pub(crate) orders: Vec<Order<ExchangeId, InstrumentNameExchange, Open>>,
    /// Whether `orders` shows every row the venue listed under the `clientOrderId` it was placed
    /// with. This is the `orders_complete` an account snapshot built from the listing may claim.
    pub(crate) complete: bool,
}

/// Convert one symbol's `openOrders` rows with [`convert_open_order`], recording whether the
/// result can stand for the whole listing.
///
/// A row the converter drops may be a live order, and a row without a `clientOrderId` is kept under
/// its `orderId`, where the order it was placed as cannot be found. Either leaves the listing
/// unable to say that an order missing from it is gone, so either makes it incomplete. A dropped
/// row that really was finished makes it incomplete too; `openOrders` does not serve those, and
/// erring towards "incomplete" costs only a reconciliation.
pub(crate) fn convert_open_order_listing<T: BinanceOrderFields>(
    rows: &[T],
    exchange: ExchangeId,
    instrument: &InstrumentNameExchange,
) -> OpenOrderListing {
    let mut complete = true;
    let orders = rows
        .iter()
        .filter_map(|row| {
            let order = convert_open_order(row, exchange, instrument);
            complete &= order.is_some() && row.client_order_id().is_some();
            order
        })
        .collect();
    OpenOrderListing { orders, complete }
}

/// Convert an open-order response whose instrument is recovered from its own `symbol` field,
/// rather than supplied by the caller. Used by the no-symbol "return all" path
/// (each client's no-symbol "return all" fetch), where each order may belong to a different instrument. Drops
/// (with a warning) any order missing `symbol`; otherwise delegates to [`convert_open_order`].
pub(crate) fn convert_open_order_owned_symbol<T: BinanceOrderFields>(
    o: &T,
    exchange: ExchangeId,
) -> Option<Order<ExchangeId, InstrumentNameExchange, Open>> {
    let instrument = match o.symbol() {
        Some(s) => InstrumentNameExchange::new(s),
        None => {
            warn!(%exchange, "Binance open order missing symbol in return-all query, dropping order");
            return None;
        }
    };
    convert_open_order(o, exchange, &instrument)
}

// ---------------------------------------------------------------------------
// Recovered fills: the order's cumulative filled quantity
// ---------------------------------------------------------------------------

/// Where a `myTrades` walk starts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MyTradesFrom {
    /// Every execution on the instrument at or after this time, in epoch milliseconds.
    Time(i64),
    /// Every execution on the instrument from `start` to `end`, both included, in epoch
    /// milliseconds: a [`FillGap`].
    Span { start: i64, end: i64 },
    /// Every execution of this one order, from its first.
    ///
    /// Queried by `orderId` alone, Binance returns an order's oldest executions first, in
    /// ascending trade id, even when the order has more executions than `limit`; later pages
    /// continue with `fromId`. Observed on the Spot testnet (2026-09-24, a five-execution order
    /// read with `limit=2`); Margin serves the same parameters and is assumed to match.
    Order(i64),
}

/// The fields of a `myTrades` execution that fill recovery reads to rebuild an order's
/// cumulative filled quantity. Spot and margin serve different types that share these fields.
pub(crate) trait BinanceExecutionFields {
    /// The trade id, which Binance assigns in increasing order per symbol.
    fn id(&self) -> Option<i64>;
    fn order_id(&self) -> Option<i64>;
    /// The size of this execution.
    fn qty(&self) -> Option<&str>;
    /// When this execution happened, in epoch milliseconds.
    fn time(&self) -> Option<i64>;
}

macro_rules! impl_binance_execution_fields {
    ($($t:ty),* $(,)?) => {
        $(
            impl BinanceExecutionFields for $t {
                fn id(&self) -> Option<i64> { self.id }
                fn order_id(&self) -> Option<i64> { self.order_id }
                fn qty(&self) -> Option<&str> { self.qty.as_deref() }
                fn time(&self) -> Option<i64> { self.time }
            }
        )*
    };
}

impl_binance_execution_fields!(
    binance_sdk::spot::rest_api::MyTradesResponseInner,
    binance_sdk::margin_trading::rest_api::QueryMarginAccountsTradeListResponseInner,
);

/// How long fill recovery may spend reading recovered orders' executions, from the moment it
/// starts.
///
/// Half of [`FILL_RECOVERY_TIMEOUT_SECS`], which bounds the whole recovery and drops every fill
/// not yet sent when it expires. The lookups only enrich fills; they must never cost one. Past
/// this budget the remaining fills go out without a cumulative, as they did before it existed.
pub(crate) const ORDER_EXECUTIONS_BUDGET: Duration =
    Duration::from_secs(FILL_RECOVERY_TIMEOUT_SECS / 2);

/// How many orders' executions one instrument's recovery reads at once.
///
/// Recovery already runs up to eight instruments at once, so this allows 32 lookups in flight.
/// Each is weight 5 (Spot, `myTrades` with `orderId`) or 10 (Margin) against a limit of several
/// thousand a minute, and [`rest_call_with_retry`] backs off when Binance signals the limit.
pub(crate) const ORDER_EXECUTIONS_IN_FLIGHT: usize = 4;

/// The cumulative filled quantity of each recovered execution's order, as of that execution,
/// keyed by trade id.
///
/// `myTrades` reports executions only, with no cumulative, so a recovered fill cannot advance its
/// order on its own. For each order among `recovered`, this walks that order's executions from its
/// first with `fetch` and sums them in trade-id order. That is the figure the WebSocket reports as
/// `z` on the same execution, so a recovered fill and a live one carry the same
/// [`Trade::order_filled_quantity`]. Up to [`ORDER_EXECUTIONS_IN_FLIGHT`] orders are read at once.
///
/// An order is left out, and its recovered fills keep `None`, when its walk fails, when the walk
/// is unusable (see [`order_running_totals`]), or once `deadline` passes. Each case is logged.
/// `None` is what every recovered fill carried before this existed, and the order's state must
/// then be learned some other way.
pub(crate) async fn recovered_order_totals<T, F, Fut>(
    exchange: ExchangeId,
    instrument: &InstrumentNameExchange,
    recovered: &[T],
    deadline: tokio::time::Instant,
    fetch: F,
) -> fnv::FnvHashMap<i64, Decimal>
where
    T: BinanceExecutionFields,
    F: Fn(i64) -> Fut,
    Fut: Future<Output = Result<Vec<T>, UnindexedClientError>>,
{
    use futures::StreamExt as _;

    let mut order_ids: Vec<i64> = recovered.iter().filter_map(|t| t.order_id()).collect();
    order_ids.sort_unstable();
    order_ids.dedup();

    let fetch = &fetch;
    let mut lookups = futures::stream::iter(order_ids.iter().copied())
        .map(|order_id| async move { (order_id, fetch(order_id).await) })
        .buffer_unordered(ORDER_EXECUTIONS_IN_FLIGHT);

    let mut totals = fnv::FnvHashMap::default();
    let mut settled = 0;
    loop {
        let (order_id, walk) = match tokio::time::timeout_at(deadline, lookups.next()).await {
            Ok(Some(lookup)) => lookup,
            Ok(None) => break,
            Err(_elapsed) => {
                warn!(
                    %exchange, %instrument,
                    orders_left = order_ids.len() - settled,
                    budget = ?ORDER_EXECUTIONS_BUDGET,
                    "fill recovery ran out of time reading orders' executions; the remaining \
                     orders' recovered fills carry no cumulative filled quantity"
                );
                break;
            }
        };
        settled += 1;
        let executions = match walk {
            Ok(executions) => executions,
            Err(error) => {
                warn!(
                    %exchange, %instrument, order_id, %error,
                    "fill recovery could not read the order's executions; its recovered fills \
                     carry no cumulative filled quantity"
                );
                continue;
            }
        };
        match order_running_totals(order_id, &executions) {
            Some(running) => totals.extend(running),
            None => warn!(
                %exchange, %instrument, order_id, executions = executions.len(),
                "fill recovery read an order's executions it cannot sum (a missing id or size, \
                 or another order's execution); its recovered fills carry no cumulative filled \
                 quantity"
            ),
        }
    }
    totals
}

/// `(trade id, cumulative filled quantity as of that execution)` for every execution of one
/// order, or `None` if the walk cannot be trusted to sum.
///
/// A running total is only right if every execution is counted exactly once, in order. So the
/// walk is refused whole, rather than summed around a gap, when an execution has no id or no
/// parseable size, or belongs to a different order: the last would mean the venue ignored the
/// `orderId` filter, and summing another order's fills would report a quantity this order never
/// filled. An execution repeated across pages is counted once.
pub(crate) fn order_running_totals<T: BinanceExecutionFields>(
    order_id: i64,
    executions: &[T],
) -> Option<Vec<(i64, Decimal)>> {
    let mut sized = executions
        .iter()
        .map(|execution| {
            if execution.order_id() != Some(order_id) {
                return None;
            }
            let qty = Decimal::from_str(execution.qty()?).ok()?;
            Some((execution.id()?, qty))
        })
        .collect::<Option<Vec<_>>>()?;

    sized.sort_unstable_by_key(|&(id, _)| id);
    sized.dedup_by_key(|&mut (id, _)| id);

    let mut cumulative = Decimal::ZERO;
    Some(
        sized
            .into_iter()
            .map(|(id, qty)| {
                cumulative += qty;
                (id, cumulative)
            })
            .collect(),
    )
}

// ---------------------------------------------------------------------------
// executionReport conversion (WebSocket user-data stream)
// ---------------------------------------------------------------------------

/// The subset of a Binance `executionReport` that [`convert_execution_report`] reads.
///
/// As with [`BinanceOrderFields`], binance-sdk generates a distinct nominal type per stream
/// family and gives them no shared trait: `spot::websocket_api::ExecutionReport` and
/// `margin_trading::websocket_streams::ExecutionReport`. The two are not interchangeable -- spot
/// declares 55 fields to margin's 50, and eight fields sharing a name differ in type
/// (`Option<i64>` on spot against `Option<String>` on margin). None of those eight is named here;
/// the eighteen below are identical in name and type across both.
///
/// Naming that read subset is what makes a single converter safe to share, and keeps the
/// dependency on the SDK explicit: a change to any *other* field cannot silently alter execution
/// handling, and a correction to the conversion reaches every Binance client at once.
///
/// Accessors borrow rather than consume, so one report can serve both events a `TRADE` produces.
pub(crate) trait BinanceExecutionReportFields {
    /// Binance field `x`: what happened (`NEW`, `TRADE`, `CANCELED`, ...).
    fn execution_type(&self) -> Option<&str>;
    /// Binance field `X`: the order's status *after* this execution, which is not the same
    /// question as `execution_type` -- a `TRADE` may leave the order `PARTIALLY_FILLED`,
    /// `FILLED`, or already retired by a report that overtook it.
    fn order_status(&self) -> Option<&str>;
    /// Binance field `s`.
    fn symbol(&self) -> Option<&str>;
    /// Binance field `S`.
    fn side(&self) -> Option<&str>;
    /// Binance field `i`: the venue's order id.
    fn order_id(&self) -> Option<i64>;
    /// Binance field `c`.
    fn client_order_id(&self) -> Option<&str>;
    /// Binance field `T`: transaction time, in milliseconds.
    fn transaction_time(&self) -> Option<i64>;
    /// Binance field `t`: the execution's own id, present only on a `TRADE`.
    fn trade_id(&self) -> Option<i64>;
    /// Binance field `L`: the price of this execution alone.
    fn last_executed_price(&self) -> Option<&str>;
    /// Binance field `l`: the quantity of this execution alone.
    fn last_executed_quantity(&self) -> Option<&str>;
    /// Binance field `n`.
    fn commission_amount(&self) -> Option<&str>;
    /// Binance field `N`: the asset the commission was charged in, which need not be either leg
    /// of the traded pair.
    fn commission_asset(&self) -> Option<&str>;
    /// Binance field `z`: the *order's* cumulative filled quantity as of this execution, as
    /// opposed to `last_executed_quantity`, which is this execution alone.
    fn cumulative_filled_quantity(&self) -> Option<&str>;
    /// Binance field `r`.
    fn reject_reason(&self) -> Option<&str>;
    /// Binance field `o`.
    fn order_type(&self) -> Option<&str>;
    /// Binance field `p`: the order's limit price, `"0"` on order kinds that carry none.
    fn price(&self) -> Option<&str>;
    /// Binance field `q`: the order's total quantity, as opposed to any executed portion of it.
    fn order_quantity(&self) -> Option<&str>;
    /// Binance field `f`.
    fn time_in_force(&self) -> Option<&str>;
}

/// Implement [`BinanceExecutionReportFields`] for SDK types that share these field names.
///
/// Every struct listed below declares these eighteen fields with the same types, so the accessors
/// are identical; a macro keeps them from drifting apart under hand-editing.
macro_rules! impl_binance_execution_report_fields {
    ($($t:ty),* $(,)?) => {
        $(
            impl BinanceExecutionReportFields for $t {
                fn execution_type(&self) -> Option<&str> { self.x.as_deref() }
                fn order_status(&self) -> Option<&str> { self.x_uppercase.as_deref() }
                fn symbol(&self) -> Option<&str> { self.s.as_deref() }
                fn side(&self) -> Option<&str> { self.s_uppercase.as_deref() }
                fn order_id(&self) -> Option<i64> { self.i }
                fn client_order_id(&self) -> Option<&str> { self.c.as_deref() }
                fn transaction_time(&self) -> Option<i64> { self.t_uppercase }
                fn trade_id(&self) -> Option<i64> { self.t }
                fn last_executed_price(&self) -> Option<&str> { self.l_uppercase.as_deref() }
                fn last_executed_quantity(&self) -> Option<&str> { self.l.as_deref() }
                fn commission_amount(&self) -> Option<&str> { self.n.as_deref() }
                fn commission_asset(&self) -> Option<&str> { self.n_uppercase.as_deref() }
                fn cumulative_filled_quantity(&self) -> Option<&str> { self.z.as_deref() }
                fn reject_reason(&self) -> Option<&str> { self.r.as_deref() }
                fn order_type(&self) -> Option<&str> { self.o.as_deref() }
                fn price(&self) -> Option<&str> { self.p.as_deref() }
                fn order_quantity(&self) -> Option<&str> { self.q.as_deref() }
                fn time_in_force(&self) -> Option<&str> { self.f.as_deref() }
            }
        )*
    };
}

impl_binance_execution_report_fields!(
    binance_sdk::spot::websocket_api::ExecutionReport,
    binance_sdk::margin_trading::websocket_streams::ExecutionReport,
);

/// Whether a `TRADE` report's order status says the order is still live at the exchange, and may
/// therefore be written into engine state as an `Open` snapshot.
///
/// A `TRADE` report carries the order's status (`X`) alongside the execution. Only
/// `PARTIALLY_FILLED` and `FILLED` say the order reached this execution while working; every other
/// status means some other report owns the order's current state. Writing an `Open` snapshot from
/// one of those would resurrect an order the engine has already retired, leaving a resting order
/// that does not exist at the exchange -- a fill that arrives after its order's terminal report is
/// exactly the ordering this guards against.
fn trade_order_is_live(status: &str) -> bool {
    matches!(status, "PARTIALLY_FILLED" | "FILLED")
}

/// Convert a Binance `executionReport` into rustrade account events, appended to `buf`.
///
/// A `TRADE` report genuinely carries two facts -- the execution print (`l`/`L`) and the order's
/// new cumulative filled quantity (`z`) -- so it appends both a `Trade` and an `OrderSnapshot`.
/// Every other execution type appends at most one event, and an unusable report appends none.
///
/// The `Trade` is always first. A fully-filled snapshot retires its order, and routing a fill
/// against an order that has already been retired is a strictly harder problem than routing it
/// against a live one; appending the execution first keeps the easy ordering.
///
/// `exchange` stamps every event and every diagnostic below, so one implementation serves each
/// Binance client without a venue name baked into its warnings.
// Inherent complexity: one arm per Binance execution type (TRADE, NEW, CANCELED, EXPIRED,
// REJECTED, REPLACE), each validating the fields its own variant needs.
#[allow(clippy::cognitive_complexity)]
pub(crate) fn convert_execution_report<T: BinanceExecutionReportFields>(
    report: &T,
    exchange: ExchangeId,
    buf: &mut Vec<UnindexedAccountEvent>,
) {
    let Some(exec_type) = report.execution_type() else {
        warn!(%exchange, "Binance executionReport missing execution type (x), dropping");
        return;
    };
    let Some(symbol) = report.symbol().map(InstrumentNameExchange::new) else {
        warn!(%exchange, "Binance executionReport missing symbol (s), dropping");
        return;
    };
    // Check order_id first -- if it is missing the event is dropped, so avoid constructing cid.
    let Some(order_id_raw) = report.order_id() else {
        warn!(%exchange, %symbol, "Binance executionReport missing orderId (i), dropping");
        return;
    };
    let order_id = OrderId(format_smolstr!("{order_id_raw}"));
    let cid = match report.client_order_id() {
        Some(c) => ClientOrderId::new(c),
        None => ClientOrderId::new(order_id.0.as_str()),
    };

    let time_exchange = match report
        .transaction_time()
        .and_then(|ms| Utc.timestamp_millis_opt(ms).single())
    {
        Some(t) => t,
        None => {
            warn!(%exchange, %symbol, "Binance executionReport missing/unparseable transaction time (T), using now");
            Utc::now()
        }
    };

    match exec_type {
        "NEW" => {
            buf.extend(convert_order_snapshot(
                report,
                exchange,
                symbol,
                cid,
                order_id,
                time_exchange,
            ));
        }
        "TRADE" => {
            // Partial or full fill.
            let Some(trade_id) = report.trade_id() else {
                warn!(%exchange, %symbol, "Binance TRADE event missing trade ID (t), dropping");
                return;
            };
            let trade_id = TradeId(format_smolstr!("{trade_id}"));
            // parse_side already logs a warning on unknown values.
            let Some(side) = report.side().and_then(parse_side) else {
                warn!(%exchange, %symbol, "Binance TRADE event missing/unknown side (S), dropping");
                return;
            };
            let last_price = match report.last_executed_price() {
                Some(s) => match Decimal::from_str(s) {
                    Ok(v) => v,
                    Err(e) => {
                        warn!(%exchange, %symbol, error = %e, raw = s, "Binance TRADE event unparseable last price (L), dropping fill");
                        return;
                    }
                },
                None => {
                    warn!(%exchange, %symbol, "Binance TRADE event missing last price (L), dropping fill");
                    return;
                }
            };
            let last_qty = match report.last_executed_quantity() {
                Some(s) => match Decimal::from_str(s) {
                    Ok(v) => v,
                    Err(e) => {
                        warn!(%exchange, %symbol, error = %e, raw = s, "Binance TRADE event unparseable last qty (l), dropping fill");
                        return;
                    }
                },
                None => {
                    warn!(%exchange, %symbol, "Binance TRADE event missing last qty (l), dropping fill");
                    return;
                }
            };
            // Commission parse failure: log and default to 0 rather than dropping the fill.
            let commission = match Decimal::from_str(report.commission_amount().unwrap_or("0")) {
                Ok(v) => v,
                Err(e) => {
                    warn!(%exchange, %symbol, error = %e, "Binance TRADE event unparseable commission (n), defaulting to 0");
                    Decimal::ZERO
                }
            };
            // Use the venue's own commission asset (N: e.g. BNB, USDT, BTC). fees_quote is None
            // here; the indexer computes it if the fee is in the quote or base asset. The
            // "UNKNOWN" fallback (rare: the API omits N) will fail indexing.
            let fee_asset = report
                .commission_asset()
                .map(AssetNameExchange::from)
                .unwrap_or_else(|| AssetNameExchange::from("UNKNOWN"));
            // `z` is the order's cumulative filled quantity as of this execution, which is what
            // advances the order; `last_qty` above is this execution alone.
            let order_filled_quantity = report
                .cumulative_filled_quantity()
                .and_then(|s| Decimal::from_str(s).ok());

            let trade = Trade::new(
                trade_id,
                order_id.clone(),
                symbol.clone(),
                StrategyId::unknown(), // Binance doesn't carry strategy IDs
                time_exchange,
                side,
                last_price,
                last_qty,
                order_filled_quantity,
                AssetFees::new(fee_asset, commission, None),
            );
            buf.push(UnindexedAccountEvent::new(
                exchange,
                AccountEventKind::Trade(trade),
            ));

            // The execution alone does not move the order: `filled_quantity` is only ever carried
            // into engine state by an order snapshot, so without this second event a partially
            // filled order reads as having nothing filled until REST reconciliation refreshes it.
            // `convert_order_snapshot` reads field `z` (cumulative filled quantity), which is
            // exactly what a TRADE report carries, so the same builder serves both arms.
            let order_status = report.order_status().unwrap_or_default();
            if trade_order_is_live(order_status) {
                buf.extend(convert_order_snapshot(
                    report,
                    exchange,
                    symbol,
                    cid,
                    order_id,
                    time_exchange,
                ));
            } else {
                trace!(
                    %exchange,
                    %symbol,
                    status = order_status,
                    "Binance TRADE for an order the exchange no longer reports as working — \
                     emitting the execution without an order snapshot"
                );
            }
        }
        "CANCELED" | "EXPIRED" | "EXPIRED_IN_MATCH" => {
            buf.push(cancelled_event(
                report,
                exchange,
                symbol,
                cid,
                order_id,
                time_exchange,
            ));
        }
        "REJECTED" => {
            // Rejected by the matching engine after initial acceptance (e.g. insufficient funds
            // discovered post-validation). Mapped to OrderCancelled with an error state so the
            // engine removes this order.
            let reject_reason = report.reject_reason().unwrap_or("unknown");
            warn!(
                %exchange, %symbol, %order_id, reason = reject_reason,
                "Binance order REJECTED by matching engine"
            );
            let response = UnindexedOrderResponseCancel {
                key: OrderKey::new(exchange, symbol, StrategyId::unknown(), cid),
                state: Err(UnindexedOrderError::Rejected(ApiError::OrderRejected(
                    reject_reason.to_string(),
                ))),
            };
            buf.push(UnindexedAccountEvent::new(
                exchange,
                AccountEventKind::OrderCancelled(response),
            ));
        }
        "REPLACE" => {
            // REPLACE is emitted when an order is replaced via the cancel-replace endpoint. The
            // report describes the CANCELLED original order: field `i` is the original order id
            // (already extracted as `order_id` above). The replacement arrives as a subsequent
            // NEW report with its own order id. Emitting OrderCancelled for the original is what
            // removes it from the engine's open-order book.
            buf.push(cancelled_event(
                report,
                exchange,
                symbol,
                cid,
                order_id,
                time_exchange,
            ));
        }
        _ => {
            // PENDING_NEW and PENDING_CANCEL are transient; the terminal state
            // (NEW/CANCELED/...) follows shortly after.
            trace!(%exchange, exec_type, "Binance ignoring execution type");
        }
    }
}

/// Build the `OrderCancelled` event shared by the `CANCELED`/`EXPIRED`/`REPLACE` arms.
///
/// All of them report an order leaving the book with whatever it had filled (`z`) at that point,
/// and differ only in why -- which the caller has already established by matching on `x`.
fn cancelled_event<T: BinanceExecutionReportFields>(
    report: &T,
    exchange: ExchangeId,
    symbol: InstrumentNameExchange,
    cid: ClientOrderId,
    order_id: OrderId,
    time_exchange: DateTime<Utc>,
) -> UnindexedAccountEvent {
    let filled_qty = report
        .cumulative_filled_quantity()
        .and_then(|s| Decimal::from_str(s).ok())
        .unwrap_or(Decimal::ZERO);
    let response = UnindexedOrderResponseCancel {
        key: OrderKey::new(exchange, symbol, StrategyId::unknown(), cid),
        state: Ok(Cancelled::new(order_id, time_exchange, filled_qty)),
    };
    UnindexedAccountEvent::new(exchange, AccountEventKind::OrderCancelled(response))
}

/// Build the `OrderSnapshot` event for a report that leaves the order resting at the exchange.
///
/// Serves both the `NEW` arm and the live half of the `TRADE` arm: the fields describing the
/// order -- side, kind, price, quantity, TIF and cumulative filled quantity -- are carried
/// identically by both, so one builder covers the acknowledgement and every fill that follows it.
///
/// Returns `None` when a field the snapshot cannot be built without is missing or unparseable;
/// each such case is warned about individually.
fn convert_order_snapshot<T: BinanceExecutionReportFields>(
    report: &T,
    exchange: ExchangeId,
    symbol: InstrumentNameExchange,
    cid: ClientOrderId,
    order_id: OrderId,
    time_exchange: DateTime<Utc>,
) -> Option<UnindexedAccountEvent> {
    // parse_side already logs a warning on unknown values.
    let Some(side) = report.side().and_then(parse_side) else {
        warn!(%exchange, %symbol, "Binance order report missing/unknown side (S), dropping snapshot");
        return None;
    };
    // parse_order_kind already logs a warning on unknown values.
    let kind = parse_order_kind(report.order_type().unwrap_or("LIMIT"))?;
    let price: Option<Decimal> = match (report.price(), &kind) {
        (Some(p), _) => match Decimal::from_str(p) {
            Ok(v) if !v.is_zero() => Some(v),
            Ok(_) => {
                // Binance sends "0" / "0.00" as the price field for Market/Stop/TrailingStop
                // orders. Trace only when a zero arrives on an order kind that should carry a
                // limit price, so the surprising case is observable.
                if matches!(
                    kind,
                    OrderKind::Limit
                        | OrderKind::StopLimit { .. }
                        | OrderKind::TakeProfitLimit { .. }
                        | OrderKind::TrailingStopLimit { .. }
                ) {
                    trace!(%exchange, %symbol, %kind, "Binance order report has zero price (p) on limit-type order, treating as no limit price");
                }
                None
            }
            Err(e) => {
                warn!(%exchange, %symbol, price = p, error = %e, "Binance order report unparseable price (p), dropping snapshot");
                return None;
            }
        },
        (
            None,
            OrderKind::Market
            | OrderKind::Stop { .. }
            | OrderKind::TakeProfit { .. }
            | OrderKind::TrailingStop { .. },
        ) => {
            // Market, Stop and TakeProfit orders carry no limit price.
            None
        }
        (
            None,
            OrderKind::Limit
            | OrderKind::StopLimit { .. }
            | OrderKind::TakeProfitLimit { .. }
            | OrderKind::TrailingStopLimit { .. },
        ) => {
            warn!(%exchange, %symbol, "Binance limit-type order report missing price (p), dropping snapshot");
            return None;
        }
    };
    let quantity = match report.order_quantity() {
        Some(q) => match Decimal::from_str(q) {
            Ok(v) => v,
            Err(e) => {
                warn!(%exchange, %symbol, qty = q, error = %e, "Binance order report unparseable quantity (q), dropping snapshot");
                return None;
            }
        },
        None => {
            warn!(%exchange, %symbol, "Binance order report missing quantity (q), dropping snapshot");
            return None;
        }
    };
    let time_in_force = parse_time_in_force(report.time_in_force().unwrap_or("GTC"));
    // Field `z`: the order's cumulative filled quantity. Usually 0 on a NEW, but read it there
    // too in case of an immediate partial fill on an aggressive order; on a TRADE it is the whole
    // point of the snapshot.
    let filled_qty = report
        .cumulative_filled_quantity()
        .and_then(|s| Decimal::from_str(s).ok())
        .unwrap_or(Decimal::ZERO);

    let order = Order {
        key: OrderKey::new(
            exchange,
            symbol,
            StrategyId::unknown(), // Binance doesn't carry strategy IDs
            cid,
        ),
        side,
        price,
        quantity,
        kind,
        time_in_force,
        state: OrderState::active(Open::new(
            VenueOrderId::Assigned(order_id),
            time_exchange,
            filled_qty,
        )),
    };

    Some(UnindexedAccountEvent::new(
        exchange,
        AccountEventKind::OrderSnapshot(rustrade_integration::collection::snapshot::Snapshot::new(
            order,
        )),
    ))
}

// ---------------------------------------------------------------------------
// Error parsing / classification
// ---------------------------------------------------------------------------

/// Case-insensitive substring search that avoids the `to_lowercase` allocation.
fn contains_ignore_case(haystack: &str, needle: &str) -> bool {
    haystack
        .as_bytes()
        .windows(needle.len())
        .any(|w| w.eq_ignore_ascii_case(needle.as_bytes()))
}

/// Returns true if `msg` contains `code` as a standalone numeric token:
/// not immediately preceded or followed by another ASCII digit.
/// Prevents "-2013" from matching "-20130" (suffix guard) or "1-2013" (prefix guard).
///
/// Iterates all occurrences so that if the first match fails a digit-guard check
/// (e.g. `-2013` found inside `-20130`), a later valid occurrence is not missed.
pub(crate) fn contains_error_code(msg: &str, code: &str) -> bool {
    let code_len = code.len();
    let mut start = 0;
    while let Some(rel) = msg[start..].find(code) {
        let pos = start + rel;
        let prefix_ok = pos == 0 || !msg[..pos].ends_with(|c: char| c.is_ascii_digit());
        let suffix_ok = !msg[pos + code_len..].starts_with(|c: char| c.is_ascii_digit());
        if prefix_ok && suffix_ok {
            return true;
        }
        start = pos + 1;
    }
    false
}

/// Whether a Binance error message means the credentials were refused, on any endpoint.
///
/// By code: `-1002` ("You are not authorized to execute this request"), `-2015` ("Invalid
/// API-key, IP, or permissions for action"), `-1022` ("Signature for this request is not valid")
/// or `-2014` ("API-key format invalid"). By wording, for messages that arrive without a code.
fn is_auth_failure(msg: &str) -> bool {
    contains_error_code(msg, "-1002")
        || contains_error_code(msg, "-2015")
        || contains_error_code(msg, "-1022")
        || contains_error_code(msg, "-2014")
        || contains_ignore_case(msg, "invalid api-key")
        || contains_ignore_case(msg, "invalid signature")
        || contains_ignore_case(msg, "signature for this request is not valid")
}

/// Whether a Binance error message carries a code meaning the venue itself failed the request:
/// `-1000` (unknown error), `-1001` (internal error), `-1006` (unexpected response from the
/// message bus), `-1007` (backend timeout) or `-1008` (server overloaded). Binance says to retry
/// these, and for `-1006`/`-1007` states that the execution status is unknown, so an order that
/// fails this way may still have executed.
fn has_venue_failure_code(msg: &str) -> bool {
    ["-1000", "-1001", "-1006", "-1007", "-1008"]
        .iter()
        .any(|code| contains_error_code(msg, code))
}

/// Whether a Binance error message carries a throttling code, on any endpoint: `-1003` (too many
/// requests) or `-1015` (too many orders). Both are transient.
fn has_rate_limit_error_code(msg: &str) -> bool {
    contains_error_code(msg, "-1003") || contains_error_code(msg, "-1015")
}

/// Parse Binance error strings to rustrade ApiError, in an order's context.
///
/// Order and cancel paths call it through [`parse_binance_order_rejection`], which first catches
/// the venue-failure codes that leave an order's status unknown.
///
/// depends on binance-sdk's internal error formatting (not a public API contract).
/// If the SDK changes its error message format, these string matches may silently stop working.
/// Matches numeric error codes first (stable), then falls back to message text heuristics.
pub(crate) fn parse_binance_api_error(
    error_msg: String,
    instrument: &InstrumentNameExchange,
) -> ApiError<AssetNameExchange, InstrumentNameExchange> {
    // Match on Binance error codes first — these are stable numeric identifiers
    if is_auth_failure(&error_msg) {
        // Auth failures must not be retried as order rejections.
        return ApiError::Unauthenticated(error_msg);
    }
    if has_rate_limit_error_code(&error_msg) {
        return ApiError::RateLimit;
    }
    if contains_error_code(&error_msg, "-2011") {
        // -2011: "Unknown order sent" — typically means already cancelled/filled
        return ApiError::OrderAlreadyCancelled;
    }
    if contains_error_code(&error_msg, "-2013") {
        // -2013: "Order does not exist" — on cancel attempts this almost always means
        // the order was already filled or cancelled (a normal race condition).
        return ApiError::OrderAlreadyCancelled;
    }
    if contains_error_code(&error_msg, "-1121") {
        return ApiError::InstrumentInvalid(instrument.clone(), error_msg);
    }
    // -2010 (NEW_ORDER_REJECTED) is deliberately not matched by code: Binance gives it for dozens
    // of reasons ("Account has insufficient balance for requested action.", "Order would trigger
    // immediately.", "Trailing stop orders are not supported for this symbol.", ...), so only the
    // text tells a balance shortfall from the rest.

    // Fall back to case-insensitive message text heuristics (avoid to_lowercase allocation)
    if contains_ignore_case(&error_msg, "insufficient")
        || contains_ignore_case(&error_msg, "not enough")
    {
        // Binance does not name the asset that ran short, so it is left unnamed rather than
        // guessed from the order.
        ApiError::BalanceInsufficient(None, error_msg)
    } else if contains_ignore_case(&error_msg, "rate limit") {
        ApiError::RateLimit
    } else if contains_ignore_case(&error_msg, "unknown order") {
        // -2011/-2013 map to OrderAlreadyCancelled via numeric code above.
        // The text-only fallback here (no code present) maps "unknown order" to
        // OrderRejected — intentionally asymmetric. If the SDK strips error codes,
        // the same semantic maps to a different variant. Acceptable: numeric codes
        // are always present in practice; the text path is a defensive last resort.
        ApiError::OrderRejected(error_msg)
    } else if contains_ignore_case(&error_msg, "invalid symbol") {
        ApiError::InstrumentInvalid(instrument.clone(), error_msg)
    } else {
        ApiError::OrderRejected(error_msg)
    }
}

/// Parse the message of a Binance rejection of an *order* request (place or cancel, REST or WS
/// API) into an [`UnindexedOrderError`].
///
/// A venue failure ([`has_venue_failure_code`]) → [`OrderError::Connectivity`]: the order may
/// or may not have reached the matching engine, and reporting a definitive rejection for one that
/// executed would leave the caller blind to a fill. Everything else is a rejection, mapped by
/// [`parse_binance_api_error`]. The venue-failure check comes first on purpose: should a message
/// ever carry both a venue-failure code and an auth or throttle marker, "status unknown" is the
/// label that cannot hide a fill.
pub(crate) fn parse_binance_order_rejection(
    msg: String,
    instrument: &InstrumentNameExchange,
) -> UnindexedOrderError {
    if has_venue_failure_code(&msg) {
        OrderError::Connectivity(ConnectivityError::Socket(msg))
    } else {
        OrderError::Rejected(parse_binance_api_error(msg, instrument))
    }
}

/// Parse the message of a Binance rejection of a *query* (a non-order request) into a
/// [`UnindexedClientError`], by Binance code.
///
/// Deliberately separate from [`parse_binance_api_error`], which reads codes in an order's
/// context: there `-2011`/`-2013` mean the order was already cancelled, and anything unknown is an
/// [`ApiError::OrderRejected`]. A query has no order to reject, so an unrecognised code is
/// [`ApiError::RequestRejected`].
///
/// - an auth failure ([`is_auth_failure`]) → [`ApiError::Unauthenticated`];
///   `-1003`/`-1015` → [`ApiError::RateLimit`].
/// - a venue failure ([`has_venue_failure_code`]) → [`ConnectivityError::Socket`]: the venue
///   failed the request, not the other way round, and Binance says to retry.
/// - `-1021` (timestamp outside `recvWindow`) → [`ConnectivityError::Socket`]: the request took
///   too long to arrive, or the local clock drifted. The SDK stamps each attempt afresh, so a
///   retry can succeed; a clock that stays wrong keeps failing, so callers should bound retries.
/// - `-1121` → [`ApiError::InstrumentInvalid`] when the request named one instrument; otherwise
///   there is no instrument to attach, so it falls through.
/// - anything else → [`ApiError::RequestRejected`].
fn parse_binance_query_rejection(
    msg: String,
    instrument: Option<&InstrumentNameExchange>,
) -> UnindexedClientError {
    let api = if is_auth_failure(&msg) {
        ApiError::Unauthenticated(msg)
    } else if has_rate_limit_error_code(&msg) {
        ApiError::RateLimit
    } else if has_venue_failure_code(&msg) || contains_error_code(&msg, "-1021") {
        return UnindexedClientError::Connectivity(ConnectivityError::Socket(msg));
    } else if let Some(instrument) = instrument.filter(|_| contains_error_code(&msg, "-1121")) {
        ApiError::InstrumentInvalid(instrument.clone(), msg)
    } else {
        ApiError::RequestRejected(msg)
    };
    UnindexedClientError::Api(api)
}

/// Classify an `anyhow::Error` from a REST *query* (a non-order request, sent through
/// [`rest_call_with_retry`]) into an [`UnindexedClientError`], so that
/// [`is_transient`](crate::error::ClientError::is_transient) is true only for failures a retry
/// can cure.
///
/// Pass `instrument` when the request named a single instrument, so a `-1121` invalid-symbol
/// rejection can say which.
///
/// - The SDK's [`ParamBuildError`] (a request builder missing a required field) is a bug in this
///   client → [`UnindexedClientError::Internal`].
/// - A [`ConnectorError`] is classified by [`classify_connector_error`], and a venue rejection by
///   [`parse_binance_query_rejection`].
/// - Anything else failed before the request was sent: the SDK returns every network and venue
///   failure as a `ConnectorError`, so untyped text comes from its request plumbing (joining or
///   parsing the URL, signing, header values), which fails identically on retry. An auth failure
///   ([`is_auth_failure`]) → [`ApiError::Unauthenticated`], otherwise
///   [`UnindexedClientError::Internal`].
///
/// A response body that arrives but does not decode is a separate path: see
/// [`response_decode_error`].
pub(crate) fn classify_rest_query_error(
    e: &anyhow::Error,
    instrument: Option<&InstrumentNameExchange>,
) -> UnindexedClientError {
    if let Some(build) = e.downcast_ref::<ParamBuildError>() {
        return UnindexedClientError::Internal(format!("building Binance request: {build}"));
    }
    if let Some(ce) = e.downcast_ref::<ConnectorError>() {
        return match classify_connector_error(ce) {
            RequestFailure::RateLimited => UnindexedClientError::Api(ApiError::RateLimit),
            RequestFailure::Unauthenticated(msg) => {
                UnindexedClientError::Api(ApiError::Unauthenticated(msg))
            }
            RequestFailure::Transport(msg) => {
                UnindexedClientError::Connectivity(ConnectivityError::Socket(msg))
            }
            RequestFailure::Rejected(msg) => parse_binance_query_rejection(msg, instrument),
        };
    }

    let msg = format!("{e:#}");
    if is_auth_failure(&msg) {
        return UnindexedClientError::Api(ApiError::Unauthenticated(msg));
    }

    UnindexedClientError::Internal(msg)
}

/// Whether a REST query for one order failed because Binance does not know the order under the
/// symbol it named: `-2013` (the order does not exist) or `-1121` (the symbol does not). Either way
/// the venue has answered, and its answer is that there is no such order.
pub(crate) fn is_unknown_order(e: &anyhow::Error) -> bool {
    matches!(
        e.downcast_ref::<ConnectorError>(),
        Some(
            ConnectorError::BadRequestError {
                code: Some(-2013 | -1121),
                ..
            } | ConnectorError::NotFoundError {
                code: Some(-2013 | -1121),
                ..
            } | ConnectorError::ConnectorClientError {
                code: Some(-2013 | -1121),
                ..
            }
        )
    )
}

/// Map a failed `RestApiResponse::data()` into [`UnindexedClientError::Internal`].
///
/// In binance-sdk `data()` only runs `serde_json::from_str` over a body already read and
/// decoded, so a failure is a mismatch between a 2xx body and the SDK's model: in practice
/// deterministic, since retrying returns the same body. A body that could not be read or decoded in the
/// first place fails earlier, inside the request, and [`classify_rest_query_error`] reports it as
/// connectivity.
pub(crate) fn response_decode_error(e: ConnectorError) -> UnindexedClientError {
    UnindexedClientError::Internal(format!("decoding Binance response: {e}"))
}

// ---------------------------------------------------------------------------
// REST call retry wrapper
// ---------------------------------------------------------------------------

/// Execute a REST call with rate-limit awareness and retry.
///
/// Waits first for the rate-limit waits `kind` honours (see [`RateLimitTracker`]), and records
/// the weight a successful response reports as used, which can pause later queries.
///
/// Generic over the SDK `RestApi` type (`R`) so it serves both the spot
/// (`binance_sdk::spot::rest_api::RestApi`) and margin
/// (`binance_sdk::margin_trading::rest_api::RestApi`) clients — the helper never touches
/// `R` itself, it only hands an `Arc<R>` clone to the per-attempt closure. Also usable
/// from concurrent per-instrument futures that hold only `Arc<R>` + `Arc<RateLimitTracker>`.
pub(crate) async fn rest_call_with_retry<R, D>(
    rest: &Arc<R>,
    rate_limiter: &RateLimitTracker,
    kind: RequestKind,
    mut make_call: impl FnMut(
        Arc<R>,
    ) -> Pin<
        Box<dyn std::future::Future<Output = anyhow::Result<RestApiResponse<D>>> + Send>,
    >,
) -> anyhow::Result<RestApiResponse<D>>
where
    // `Arc<R>` is moved into the `+ Send` future, which requires `R: Send + Sync`. Both SDK
    // RestApi types satisfy this; stating it here surfaces the constraint at the definition
    // rather than as a confusing error at the call sites' `Box::pin(async move ...)`.
    R: Send + Sync,
{
    // on the last iteration (attempt == MAX_RATE_LIMIT_RETRIES) the rate-limit
    // guard `attempt < MAX_RATE_LIMIT_RETRIES` is false, so a rate-limit error falls
    // through to the catch-all `Err(e) => return Err(e)` arm. All three match arms
    // return on the last iteration, so the post-loop unreachable!() is a runtime
    // safety net — the loop body always returns before exhaustion.
    for attempt in 0..=MAX_RATE_LIMIT_RETRIES {
        rate_limiter.wait_if_blocked(kind).await;
        let sent_ms = unix_ms();
        match make_call(Arc::clone(rest)).await {
            Ok(response) => {
                rate_limiter.observe_rest(&response, sent_ms);
                return Ok(response);
            }
            Err(e) if is_rate_limit_error(&e) && attempt < MAX_RATE_LIMIT_RETRIES => {
                // exponential delay starting at 1s (not DEFAULT_RATE_LIMIT_DELAY_SECS=10s).
                // The retry loop uses an aggressive initial delay to recover quickly from
                // transient bursts. DEFAULT_RATE_LIMIT_DELAY_SECS is for the externally-set
                // "blocked" state (e.g. Retry-After header), not for per-call retries.
                let delay = Duration::from_secs(2u64.saturating_pow(attempt).min(30));
                warn!(
                    attempt = attempt.saturating_add(1),
                    max = MAX_RATE_LIMIT_RETRIES,
                    delay_secs = delay.as_secs(),
                    "Binance REST rate-limited, retrying"
                );
                rate_limiter.on_rate_limited(Some(delay));
            }
            Err(e) => return Err(e),
        }
    }
    unreachable!("Binance REST retries exhausted: loop invariant violated")
}

// ---------------------------------------------------------------------------
// Order-kind / TIF classification (rustrade enums → Binance semantics)
// ---------------------------------------------------------------------------

/// Venue-neutral Binance order type — the single source of truth for mapping a rustrade
/// [`OrderKind`] to Binance order semantics, shared by the spot and margin clients.
///
/// Each client maps this onto its own SDK's per-endpoint order-type enum (spot's WS-API
/// `OrderPlaceTypeEnum`, margin's REST `MarginAccountNewOrderTypeEnum`), so neither ever builds
/// the wire string by hand. Keeping the decision logic here (in [`classify_order_kind_tif`])
/// avoids duplicating the match arms across the two clients, which differ only in their SDK
/// output types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BinanceOrderType {
    Market,
    Limit,
    LimitMaker,
    StopLoss,
    StopLossLimit,
    TakeProfit,
    TakeProfitLimit,
}

/// Venue-neutral Binance time-in-force. Both spot and margin expose exactly `GTC`/`IOC`/`FOK`
/// on their order endpoints; post-only is modelled as [`BinanceOrderType::LimitMaker`] (no TIF),
/// matching Binance's own API.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BinanceTimeInForce {
    Gtc,
    Ioc,
    Fok,
}

/// Map a rustrade [`OrderKind`] + [`TimeInForce`] to Binance order semantics.
///
/// Returns `None` for combinations Binance does not support (so callers surface
/// `UnsupportedOrderType`). `TrailingStop`/`TrailingStopLimit` classify to
/// [`BinanceOrderType::StopLoss`]/`None` here (valid for spot, which sets `trailingDelta`);
/// the **margin** adapter rejects trailing kinds *before* calling this, since the margin SDK
/// has no `trailingDelta` binding.
pub(crate) fn classify_order_kind_tif(
    kind: OrderKind,
    tif: TimeInForce,
) -> Option<(BinanceOrderType, Option<BinanceTimeInForce>)> {
    match kind {
        OrderKind::Market => Some((BinanceOrderType::Market, None)),
        OrderKind::Limit => match tif {
            TimeInForce::GoodUntilCancelled { post_only: false } => {
                Some((BinanceOrderType::Limit, Some(BinanceTimeInForce::Gtc)))
            }
            TimeInForce::GoodUntilCancelled { post_only: true } => {
                // LIMIT_MAKER is Binance's post-only order type (rejects if
                // it would immediately match as taker)
                Some((BinanceOrderType::LimitMaker, None))
            }
            TimeInForce::FillOrKill => {
                Some((BinanceOrderType::Limit, Some(BinanceTimeInForce::Fok)))
            }
            TimeInForce::ImmediateOrCancel => {
                Some((BinanceOrderType::Limit, Some(BinanceTimeInForce::Ioc)))
            }
            // Binance does not support GTD (good-til-end-of-day), GTC-until-date, MOO, or MOC.
            // Surface as unsupported rather than silently coercing — these have venue-specific
            // semantics (e.g. an end-of-day auto-cancel) that a GTC coercion would silently drop,
            // risking an unintended resting/overnight order.
            TimeInForce::GoodUntilEndOfDay
            | TimeInForce::GoodTillDate { .. }
            | TimeInForce::AtOpen
            | TimeInForce::AtClose => {
                warn!(time_in_force = ?tif, "Binance does not support this TimeInForce");
                None
            }
        },
        // Conditional orders: stop_price/trailing_delta set separately by the caller.
        OrderKind::Stop { .. } => Some((BinanceOrderType::StopLoss, None)),
        OrderKind::StopLimit { .. } => match tif {
            // StopLimit requires TIF like regular Limit orders.
            TimeInForce::GoodUntilCancelled { post_only: false } => Some((
                BinanceOrderType::StopLossLimit,
                Some(BinanceTimeInForce::Gtc),
            )),
            TimeInForce::FillOrKill => Some((
                BinanceOrderType::StopLossLimit,
                Some(BinanceTimeInForce::Fok),
            )),
            TimeInForce::ImmediateOrCancel => Some((
                BinanceOrderType::StopLossLimit,
                Some(BinanceTimeInForce::Ioc),
            )),
            _ => {
                warn!(time_in_force = ?tif, "Binance StopLimit does not support this TimeInForce");
                None
            }
        },
        OrderKind::TakeProfit { .. } => Some((BinanceOrderType::TakeProfit, None)),
        OrderKind::TakeProfitLimit { .. } => match tif {
            // TakeProfitLimit requires TIF like regular Limit orders.
            TimeInForce::GoodUntilCancelled { post_only: false } => Some((
                BinanceOrderType::TakeProfitLimit,
                Some(BinanceTimeInForce::Gtc),
            )),
            TimeInForce::FillOrKill => Some((
                BinanceOrderType::TakeProfitLimit,
                Some(BinanceTimeInForce::Fok),
            )),
            TimeInForce::ImmediateOrCancel => Some((
                BinanceOrderType::TakeProfitLimit,
                Some(BinanceTimeInForce::Ioc),
            )),
            _ => {
                warn!(time_in_force = ?tif, "Binance TakeProfitLimit does not support this TimeInForce");
                None
            }
        },
        // TrailingStop: Binance uses STOP_LOSS with a trailingDelta parameter. Only
        // BasisPoints and Percentage are supported; Absolute requires manual conversion by
        // the caller: basis_points = (absolute / price) * 10000.
        OrderKind::TrailingStop { offset_type, .. } => match offset_type {
            TrailingOffsetType::BasisPoints | TrailingOffsetType::Percentage => {
                Some((BinanceOrderType::StopLoss, None))
            }
            TrailingOffsetType::Absolute => {
                warn!(
                    "Binance TrailingStop does not support Absolute offset; \
                     convert to basis points: (absolute / price) * 10000"
                );
                None
            }
        },
        // Binance does not support TrailingStopLimit.
        OrderKind::TrailingStopLimit { .. } => {
            warn!("Binance does not support TrailingStopLimit orders");
            None
        }
    }
}

// ---------------------------------------------------------------------------
// Request-error classification (REST and WS API)
// ---------------------------------------------------------------------------

/// How a binance-sdk request failed — a REST [`ConnectorError`] or a WS-API
/// [`WebsocketError::ResponseError`] — before the caller's context (an order or a query) decides
/// which rustrade error that is.
#[derive(Debug, Clone, PartialEq, Eq)]
enum RequestFailure {
    /// HTTP 429 or 418 (throttled or IP-banned), or a 403 that is not an auth failure.
    RateLimited,
    /// HTTP 401, or a 403 that is an auth failure. Carries the message with the Binance code
    /// spliced in.
    Unauthenticated(String),
    /// The request never completed, the venue failed it (5xx), or a response body could not be
    /// read. Whether the venue acted on it is unknown.
    Transport(String),
    /// The venue answered and refused the request. Carries the message with the Binance code
    /// spliced in, for a code-first parser.
    Rejected(String),
}

/// Splice a Binance code back into a message whose `Display` omits it.
fn with_code(msg: &str, code: Option<i64>) -> String {
    code.map_or_else(|| msg.to_owned(), |c| format!("{c} {msg}"))
}

/// Whether an HTTP 403 is Binance's web application firewall (WAF) limit rather than an auth
/// failure. Binance documents 403 as the WAF limit being violated, and a CDN block answers 403
/// too; it is an auth failure only when its code or wording says so. `msg` carries the code.
fn is_waf_block(msg: &str) -> bool {
    !is_auth_failure(msg)
}

/// Classify an HTTP 403 (REST or WS API). A WAF block was refused at the edge, so an order never
/// reached the matching engine; it is a limit to back off from, which [`is_rate_limit_error`]
/// also recognises so [`rest_call_with_retry`] backs off in place.
fn classify_forbidden(msg: String) -> RequestFailure {
    if is_waf_block(&msg) {
        // `RateLimited` carries no message, so log the body it replaces. A CDN's 403 body is a
        // full HTML page: log only its start.
        const LOG_EXCERPT_CHARS: usize = 256;
        let excerpt = msg
            .char_indices()
            .nth(LOG_EXCERPT_CHARS)
            .map_or(msg.as_str(), |(end, _)| &msg[..end]);
        warn!(body = %excerpt, "Binance answered 403 (WAF limit); reporting RateLimit");
        RequestFailure::RateLimited
    } else {
        RequestFailure::Unauthenticated(msg)
    }
}

/// Classify a binance-sdk WS-API [`WebsocketError::ResponseError`]: the venue answered a request
/// with a status of 400 or more. `msg` is the error's `Display`, which already carries `code`.
///
/// A negative `code` is Binance's own error code, for a code-first parser. When the response had
/// no error object, the SDK falls back to the HTTP status as `code` (and `"Unknown error"` as
/// the message), so a non-negative `code` is read as a status, as the REST path reads one:
/// 429/418 → rate-limited, 401 → unauthenticated, 403 → [`classify_forbidden`], 5xx →
/// transport (the venue failed it; the status of any order is unknown).
fn classify_ws_response_error(code: i64, msg: String) -> RequestFailure {
    match code {
        429 | 418 => RequestFailure::RateLimited,
        401 => RequestFailure::Unauthenticated(msg),
        403 => classify_forbidden(msg),
        500..=599 => RequestFailure::Transport(msg),
        _ => RequestFailure::Rejected(msg),
    }
}

/// Classify a binance-sdk REST [`ConnectorError`], shared by [`classify_rest_order_error`] and
/// [`classify_rest_query_error`].
///
/// `ConnectorError`'s `Display` **omits** the Binance numeric code (it lives in a separate `code`
/// field), so the code is spliced back into the carried message wherever the caller parses it.
fn classify_connector_error(ce: &ConnectorError) -> RequestFailure {
    match ce {
        ConnectorError::TooManyRequestsError { .. } | ConnectorError::RateLimitBanError { .. } => {
            RequestFailure::RateLimited
        }
        // Splice the code in so callers can tell auth-failure subtypes apart (e.g. -2014
        // invalid key vs -2015 IP/permission).
        ConnectorError::UnauthorizedError { msg, code } => {
            RequestFailure::Unauthenticated(with_code(msg, *code))
        }
        ConnectorError::ForbiddenError { msg, code } => classify_forbidden(with_code(msg, *code)),
        ConnectorError::ServerError { msg, .. } | ConnectorError::NetworkError(msg) => {
            RequestFailure::Transport(msg.clone())
        }
        ConnectorError::BadRequestError { msg, code }
        | ConnectorError::NotFoundError { msg, code }
        | ConnectorError::ConnectorClientError { msg, code } => {
            // A codeless `ConnectorClientError` from the SDK's `http_request` is a transport or
            // response-decode failure, not a venue decision (genuine Binance rejections carry a
            // numeric code). These prefixes are the SDK's codeless transport/decode sites: the
            // request never completed, or a 2xx body could not be read/decompressed/decoded.
            // Route them to Transport — the request's venue status is unknown — rather than
            // misreporting a definitive rejection. Match by prefix (not a blanket `code.is_none()`)
            // so an unusual HTTP error *status* with no Binance code still maps to a rejection.
            // (Body-deserialization failures surface via `.data()`, a separate path handled at
            // that call site.)
            //
            // These prefixes mirror the codeless error sites in the binance-sdk `http_request`
            // helper (binance-sdk `src/common`). They are not a stable public contract — re-verify
            // this list (and the test below that pins it) whenever the binance-sdk pin is bumped.
            const TRANSPORT_PREFIXES: [&str; 4] = [
                "HTTP request failed",
                "Failed to get response bytes",
                "Failed to decompress gzip response",
                "Failed to convert response to UTF-8",
            ];
            if code.is_none() && TRANSPORT_PREFIXES.iter().any(|p| msg.starts_with(p)) {
                RequestFailure::Transport(msg.clone())
            } else {
                RequestFailure::Rejected(with_code(msg, *code))
            }
        }
    }
}

/// Classify an `anyhow::Error` from a REST order/cancel call into an [`OrderError`].
///
/// REST errors differ from the WS-API path: binance-sdk surfaces them as
/// [`ConnectorError`], whose `Display` **omits** the Binance numeric code (it lives in a
/// separate `code` field). So the WS classifier [`classify_ws_order_error`] (which downcasts
/// to `WebsocketError`) does not apply here — we downcast to `ConnectorError` instead, classify
/// it with [`classify_connector_error`], and map a venue rejection with
/// [`parse_binance_order_rejection`].
///
/// - 401 → [`ApiError::Unauthenticated`]; 429/418 → [`ApiError::RateLimit`]; 403 →
///   `Unauthenticated` when it is an auth failure, else `RateLimit` (Binance's WAF limit).
/// - 400/404 / other client errors that carry a Binance code → mapped by code/text; a venue
///   failure code → [`OrderError::Connectivity`].
/// - Network/server failures (and the SDK's codeless transport/decode wrappers — failed HTTP
///   request, response-byte read, gzip, or UTF-8 decode) → [`OrderError::Connectivity`]: the
///   order may or may not have reached the matching engine.
pub(crate) fn classify_rest_order_error(
    e: &anyhow::Error,
    instrument: &InstrumentNameExchange,
) -> OrderError<AssetNameExchange, InstrumentNameExchange> {
    let Some(ce) = e.downcast_ref::<ConnectorError>() else {
        // Not an SDK ConnectorError — treat as opaque transport failure.
        return OrderError::Connectivity(ConnectivityError::Socket(format!("{e:#}")));
    };

    order_error_from(classify_connector_error(ce), instrument)
}

/// Classify an `anyhow::Error` from a WS-API order/cancel request.
///
/// binance-sdk returns both transport failures and venue responses with status >= 400 through
/// the same `Err`. Returns `None` unless it is a [`WebsocketError::ResponseError`]: the venue
/// answered, so the session is healthy and the caller keeps it. Any other error is a transport
/// failure for the caller to handle. `downcast_ref` searches the whole error chain, so context
/// layers do not hide the `ResponseError`. Re-verify on an SDK upgrade: if the SDK changes its
/// error wrapping, `downcast_ref` returns `None` and every rejection would be treated as a
/// transport failure.
pub(crate) fn classify_ws_order_error(
    e: &anyhow::Error,
    instrument: &InstrumentNameExchange,
) -> Option<UnindexedOrderError> {
    match e.downcast_ref::<WebsocketError>()? {
        // The typed error's own `Display`, not `e`'s: a context layer would hide the code.
        response @ WebsocketError::ResponseError { code, .. } => Some(order_error_from(
            classify_ws_response_error(*code, response.to_string()),
            instrument,
        )),
        _ => None,
    }
}

/// Map a [`RequestFailure`] of an order or cancel request to an [`OrderError`].
fn order_error_from(
    failure: RequestFailure,
    instrument: &InstrumentNameExchange,
) -> UnindexedOrderError {
    match failure {
        RequestFailure::RateLimited => OrderError::Rejected(ApiError::RateLimit),
        RequestFailure::Unauthenticated(msg) => {
            OrderError::Rejected(ApiError::Unauthenticated(msg))
        }
        RequestFailure::Transport(msg) => OrderError::Connectivity(ConnectivityError::Socket(msg)),
        RequestFailure::Rejected(msg) => parse_binance_order_rejection(msg, instrument),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A gap is due once opened, leaves when recovered, and when its read fails is retried with a
    /// doubling delay until it has failed every retry, then given up.
    #[tokio::test]
    async fn a_fill_gap_is_retried_with_backoff_then_given_up() {
        tokio::time::pause();
        let btc = InstrumentNameExchange::new("BTCUSDT");
        let eth = InstrumentNameExchange::new("ETHUSDT");
        let disconnect = Utc.timestamp_millis_opt(1_000_000).unwrap();
        let reconnect = Utc.timestamp_millis_opt(2_000_000).unwrap();
        let mut unrecovered = UnrecoveredFills::default();
        unrecovered.open(&[btc.clone(), eth.clone()], disconnect, reconnect);

        let due = unrecovered.due(tokio::time::Instant::now());
        assert_eq!(due.len(), 2);
        let gap_of = |instrument: &InstrumentNameExchange| {
            let Some((_, gap)) = due.iter().find(|(inst, _)| inst == instrument) else {
                panic!("{instrument} has a gap");
            };
            *gap
        };
        let gap = gap_of(&btc);
        assert_eq!(gap.start_ms, 1_000_000);
        assert_eq!(gap.end_ms, 2_000_000 + GAP_END_SLACK_SECS * 1_000);

        unrecovered.recovered(&eth, &gap_of(&eth));
        for retry in 0..MAX_GAP_RETRIES {
            let delay = Duration::from_secs(GAP_RETRY_BASE_SECS << retry);
            let now = tokio::time::Instant::now();
            assert_eq!(
                unrecovered.failed(&btc, &gap, now),
                Some(GapFailure::Retry(delay))
            );
            assert!(unrecovered.due(now).is_empty(), "not due before its delay");
            assert_eq!(unrecovered.next_due(), Some(now + delay));
            tokio::time::advance(delay).await;
            assert_eq!(unrecovered.due(tokio::time::Instant::now()).len(), 1);
        }
        assert_eq!(
            unrecovered.failed(&btc, &gap, tokio::time::Instant::now()),
            Some(GapFailure::GivenUp)
        );
        assert!(unrecovered.is_empty());
        assert_eq!(unrecovered.next_due(), None);
        assert_eq!(
            unrecovered.failed(&btc, &gap, tokio::time::Instant::now()),
            None
        );
    }

    /// A new gap that overlaps a kept one starts after it, so the kept one keeps its retry
    /// schedule and no span is read twice; a gap wholly covered is not opened.
    #[tokio::test]
    async fn an_overlapping_fill_gap_starts_after_the_kept_one() {
        tokio::time::pause();
        let btc = InstrumentNameExchange::new("BTCUSDT");
        let at = |secs: i64| Utc.timestamp_millis_opt(secs * 1_000).unwrap();
        let mut unrecovered = UnrecoveredFills::default();
        unrecovered.open(std::slice::from_ref(&btc), at(1_000), at(1_100));
        let (_, first) = unrecovered.due(tokio::time::Instant::now())[0].clone();
        unrecovered.failed(&btc, &first, tokio::time::Instant::now());

        // Disconnected again within the first gap's slack: only the rest is a new gap, due now.
        unrecovered.open(std::slice::from_ref(&btc), at(1_105), at(1_200));
        let due = unrecovered.due(tokio::time::Instant::now());
        assert_eq!(due.len(), 1, "the failed gap waits for its retry");
        assert_eq!(due[0].1.start_ms, first.end_ms + 1);
        assert_eq!(due[0].1.end_ms, (1_200 + GAP_END_SLACK_SECS) * 1_000);

        // A gap wholly inside kept ones adds nothing.
        unrecovered.open(std::slice::from_ref(&btc), at(1_150), at(1_150));
        assert_eq!(unrecovered.due(tokio::time::Instant::now()).len(), 1);
        tokio::time::advance(Duration::from_secs(GAP_RETRY_BASE_SECS)).await;
        assert_eq!(unrecovered.due(tokio::time::Instant::now()).len(), 2);
    }

    /// Executions after a span's end are dropped, and their presence ends the walk.
    #[test]
    fn drop_after_keeps_only_the_span() {
        let execution =
            |id: i64, time: Option<i64>| binance_sdk::spot::rest_api::MyTradesResponseInner {
                id: Some(id),
                time,
                ..Default::default()
            };
        let mut page = vec![
            execution(1, Some(100)),
            execution(2, None),
            execution(3, Some(200)),
            execution(4, Some(201)),
        ];
        assert!(drop_after(&mut page, 200));
        assert_eq!(
            page.iter().map(|e| e.id).collect::<Vec<_>>(),
            [Some(1), Some(2), Some(3)]
        );
        assert!(!drop_after(&mut page, 200));
    }

    #[test]
    fn parse_user_data_frame_splits_responses_events_and_unknown_shapes() {
        assert_eq!(
            parse_user_data_frame(r#"{"id":"abc","status":200,"result":{"subscriptionId":0}}"#),
            UserDataFrame::Response
        );
        assert_eq!(
            parse_user_data_frame(
                r#"{"subscriptionId":4,"event":{"e":"outboundAccountPosition","E":1}}"#
            ),
            UserDataFrame::Event {
                subscription_id: Some(4),
                event_type: "outboundAccountPosition",
                event: r#"{"e":"outboundAccountPosition","E":1}"#,
            }
        );
        // A frame with both `id` and `event` is a response.
        assert_eq!(
            parse_user_data_frame(r#"{"id":1,"subscriptionId":4,"event":{"e":"x"}}"#),
            UserDataFrame::Response
        );
        for frame in [
            // A bare event: the shape the spot converter used to assume.
            r#"{"e":"executionReport","i":1}"#,
            // An envelope whose event has no `e`, a non-string `e`, or an escaped one.
            r#"{"subscriptionId":4,"event":{"E":1}}"#,
            r#"{"subscriptionId":4,"event":{"e":7}}"#,
            r#"{"subscriptionId":4,"event":{"e":"executionRep\u006frt"}}"#,
            "not json",
            "[]",
        ] {
            assert_eq!(
                parse_user_data_frame(frame),
                UserDataFrame::Unrecognised,
                "{frame}"
            );
        }
    }

    #[test]
    fn frame_excerpt_cuts_at_200_characters_on_a_char_boundary() {
        assert_eq!(frame_excerpt("short"), "short");
        let exact = "a".repeat(200);
        assert_eq!(frame_excerpt(&exact), exact);
        let long = "é".repeat(300);
        assert_eq!(frame_excerpt(&long).chars().count(), 200);
    }

    /// A `myTrades` execution reduced to the fields recovery's order totals read.
    #[derive(Debug, Clone, Copy)]
    struct Execution {
        id: Option<i64>,
        order_id: Option<i64>,
        qty: Option<&'static str>,
    }

    impl BinanceExecutionFields for Execution {
        fn id(&self) -> Option<i64> {
            self.id
        }
        fn order_id(&self) -> Option<i64> {
            self.order_id
        }
        fn qty(&self) -> Option<&str> {
            self.qty
        }
        // Recovery's order totals never read the time.
        fn time(&self) -> Option<i64> {
            None
        }
    }

    fn execution(id: i64, order_id: i64, qty: &'static str) -> Execution {
        Execution {
            id: Some(id),
            order_id: Some(order_id),
            qty: Some(qty),
        }
    }

    fn btcusdt() -> InstrumentNameExchange {
        InstrumentNameExchange::new("BTCUSDT")
    }

    #[test]
    fn running_totals_accumulate_in_trade_id_order_counting_a_repeat_once() {
        // Out of order, and trade 12 twice, as an overlapping page would serve it.
        let executions = [
            execution(12, 7, "0.5"),
            execution(10, 7, "1"),
            execution(12, 7, "0.5"),
            execution(15, 7, "2"),
        ];

        assert_eq!(
            order_running_totals(7, &executions),
            Some(vec![
                (10, Decimal::ONE),
                (12, Decimal::new(15, 1)),
                (15, Decimal::new(35, 1)),
            ])
        );
    }

    #[test]
    fn running_totals_refuse_a_walk_holding_another_orders_execution() {
        // The venue ignoring `orderId` would serve the symbol's other orders; summing them would
        // report a quantity this order never filled.
        let executions = [execution(10, 7, "1"), execution(11, 8, "5")];

        assert_eq!(order_running_totals(7, &executions), None);
    }

    #[test]
    fn running_totals_refuse_a_walk_with_an_unreadable_execution() {
        let unsized_execution = Execution {
            qty: None,
            ..execution(11, 7, "0")
        };
        let unparseable = execution(11, 7, "abc");
        let unidentified = Execution {
            id: None,
            ..execution(11, 7, "1")
        };

        for broken in [unsized_execution, unparseable, unidentified] {
            assert_eq!(
                order_running_totals(7, &[execution(10, 7, "1"), broken]),
                None,
                "{broken:?}",
            );
        }
    }

    #[tokio::test]
    async fn a_recovered_fill_counts_what_its_order_filled_before_the_window() {
        // Only trade 12 fell inside the recovery window; trade 10 filled before the disconnect.
        let recovered = [execution(12, 7, "2")];
        let order_walk = vec![execution(10, 7, "1"), execution(12, 7, "2")];

        let totals = recovered_order_totals(
            ExchangeId::BinanceSpot,
            &btcusdt(),
            &recovered,
            tokio::time::Instant::now() + Duration::from_secs(5),
            |order_id| {
                assert_eq!(order_id, 7);
                let walk = order_walk.clone();
                async move { Ok(walk) }
            },
        )
        .await;

        assert_eq!(totals.get(&12), Some(&Decimal::new(3, 0)));
    }

    #[tokio::test]
    async fn an_order_whose_walk_fails_is_left_out_and_the_others_are_not() {
        let recovered = [execution(20, 1, "1"), execution(21, 2, "1")];

        let totals = recovered_order_totals(
            ExchangeId::BinanceMargin,
            &btcusdt(),
            &recovered,
            tokio::time::Instant::now() + Duration::from_secs(5),
            |order_id| async move {
                match order_id {
                    1 => Err(UnindexedClientError::Connectivity(
                        ConnectivityError::Socket("connection reset".into()),
                    )),
                    _ => Ok(vec![execution(21, 2, "1")]),
                }
            },
        )
        .await;

        assert_eq!(totals.get(&20), None);
        assert_eq!(totals.get(&21), Some(&Decimal::ONE));
    }

    #[tokio::test(start_paused = true)]
    async fn lookups_stop_at_the_deadline_keeping_the_walks_that_answered() {
        // Order 1's walk never answers; order 2's does. Recovery must return what it has at the
        // deadline, so the fills still go out inside the overall recovery timeout.
        let recovered = [execution(20, 1, "1"), execution(21, 2, "1")];
        let started = tokio::time::Instant::now();
        let deadline = started + Duration::from_secs(5);

        let totals = recovered_order_totals(
            ExchangeId::BinanceSpot,
            &btcusdt(),
            &recovered,
            deadline,
            |order_id| async move {
                if order_id == 1 {
                    std::future::pending::<()>().await;
                }
                Ok(vec![execution(21, 2, "1")])
            },
        )
        .await;

        assert_eq!(totals.get(&20), None);
        assert_eq!(totals.get(&21), Some(&Decimal::ONE));
        assert_eq!(tokio::time::Instant::now(), deadline);
    }

    #[tokio::test(start_paused = true)]
    async fn lookups_run_side_by_side_up_to_the_in_flight_bound() {
        // Each walk takes 4 s against a 5 s budget: one at a time, only the first would finish.
        // Side by side, the first ORDER_EXECUTIONS_IN_FLIGHT finish together at 4 s, and the one
        // over the bound starts only then, so the deadline cuts it.
        let recovered: Vec<Execution> = (1_i64..)
            .take(ORDER_EXECUTIONS_IN_FLIGHT + 1)
            .map(|order_id| execution(100 + order_id, order_id, "1"))
            .collect();
        let over_bound = recovered.last().and_then(|t| t.id);

        let totals = recovered_order_totals(
            ExchangeId::BinanceSpot,
            &btcusdt(),
            &recovered,
            tokio::time::Instant::now() + Duration::from_secs(5),
            |order_id| async move {
                tokio::time::sleep(Duration::from_secs(4)).await;
                Ok(vec![execution(100 + order_id, order_id, "1")])
            },
        )
        .await;

        assert_eq!(totals.len(), ORDER_EXECUTIONS_IN_FLIGHT, "{totals:?}");
        assert!(
            over_bound.is_some_and(|id| !totals.contains_key(&id)),
            "{totals:?}"
        );
    }

    fn classify(
        msg: &str,
        code: Option<i64>,
    ) -> OrderError<AssetNameExchange, InstrumentNameExchange> {
        let err = anyhow::Error::new(ConnectorError::ConnectorClientError {
            msg: msg.to_string(),
            code,
        });
        classify_rest_order_error(&err, &InstrumentNameExchange::new("BTCUSDT"))
    }

    #[test]
    fn codeless_transport_failures_map_to_connectivity() {
        // The SDK's codeless transport/decode wrappers (http_request error arm) must classify as
        // Connectivity — venue status unknown — never as a definitive rejection. Misreporting one
        // of these as Rejected risks a phantom position when the order actually reached the engine.
        // This test also pins the brittle prefix list against silent SDK message-format drift.
        for msg in [
            "HTTP request failed: connection reset",
            "Failed to get response bytes: error reading body",
            "Failed to decompress gzip response",
            "Failed to convert response to UTF-8: invalid utf-8 sequence",
        ] {
            assert!(
                matches!(classify(msg, None), OrderError::Connectivity(_)),
                "expected Connectivity for {msg:?}"
            );
        }
    }

    #[test]
    fn coded_error_maps_to_rejection() {
        // A genuine matching-engine rejection carries a Binance numeric code.
        assert!(matches!(
            classify("Account has insufficient balance.", Some(-2010)),
            OrderError::Rejected(_)
        ));
    }

    #[test]
    fn codeless_non_transport_status_error_maps_to_rejection() {
        // An unusual HTTP error *status* with no Binance code (SDK's catch-all `_` arm) did reach
        // the venue, so it must remain a rejection — not be swallowed as connectivity by an
        // over-broad `code.is_none()` guard. This is the distinction the prefix match preserves.
        assert!(matches!(
            classify("Conflict", None),
            OrderError::Rejected(_)
        ));
    }

    fn classify_err(ce: ConnectorError) -> OrderError<AssetNameExchange, InstrumentNameExchange> {
        classify_rest_order_error(
            &anyhow::Error::new(ce),
            &InstrumentNameExchange::new("BTCUSDT"),
        )
    }

    #[test]
    fn rate_limit_variants_map_to_rejection_ratelimit() {
        // 429/418 surface as RateLimit so callers can back off; venue did respond.
        for ce in [
            ConnectorError::TooManyRequestsError {
                msg: "Too many requests.".to_string(),
                code: Some(-1003),
            },
            ConnectorError::RateLimitBanError {
                msg: "IP banned.".to_string(),
                code: Some(-1003),
            },
        ] {
            assert!(matches!(
                classify_err(ce),
                OrderError::Rejected(ApiError::RateLimit)
            ));
        }
    }

    #[test]
    fn auth_variants_map_to_unauthenticated() {
        // A 401, or a 403 that is an auth failure, is a definitive auth rejection.
        for ce in [
            ConnectorError::UnauthorizedError {
                msg: "bad key".to_string(),
                code: Some(-2014),
            },
            ConnectorError::ForbiddenError {
                msg: "Invalid API-key, IP, or permissions for action.".to_string(),
                code: Some(-2015),
            },
        ] {
            assert!(matches!(
                classify_err(ce),
                OrderError::Rejected(ApiError::Unauthenticated(_))
            ));
        }
    }

    #[test]
    fn server_and_network_variants_map_to_connectivity() {
        // 5xx / transport failures leave the order's venue status unknown → Connectivity.
        for ce in [
            ConnectorError::ServerError {
                msg: "internal error".to_string(),
                status_code: Some(503),
            },
            ConnectorError::NetworkError("connection reset".to_string()),
        ] {
            assert!(matches!(classify_err(ce), OrderError::Connectivity(_)));
        }
    }

    // --- REST query-error classification ---

    fn classify_query(ce: ConnectorError) -> UnindexedClientError {
        classify_rest_query_error(
            &anyhow::Error::new(ce),
            Some(&InstrumentNameExchange::new("BTCUSDT")),
        )
    }

    fn bad_request(code: i64, msg: &str) -> ConnectorError {
        ConnectorError::BadRequestError {
            msg: msg.to_string(),
            code: Some(code),
        }
    }

    #[test]
    fn a_query_the_venue_refuses_is_a_request_rejection_carrying_its_code() {
        let err = classify_query(bad_request(
            -1127,
            "More than 24 hours between startTime and endTime.",
        ));
        let UnindexedClientError::Api(ApiError::RequestRejected(msg)) = &err else {
            panic!("expected RequestRejected, got {err:?}");
        };
        assert!(msg.contains("-1127"), "code missing from {msg:?}");
        assert!(!err.is_transient());
    }

    #[test]
    fn a_query_naming_an_invalid_symbol_is_an_invalid_instrument() {
        let err = classify_query(bad_request(-1121, "Invalid symbol."));
        assert!(
            matches!(
                &err,
                UnindexedClientError::Api(ApiError::InstrumentInvalid(instrument, _))
                    if instrument.name().as_str() == "BTCUSDT"
            ),
            "got {err:?}"
        );

        // With no single instrument in the request there is none to name.
        let err = classify_rest_query_error(
            &anyhow::Error::new(bad_request(-1121, "Invalid symbol.")),
            None,
        );
        assert!(
            matches!(err, UnindexedClientError::Api(ApiError::RequestRejected(_))),
            "got {err:?}"
        );
    }

    #[test]
    fn order_codes_do_not_leak_into_query_classification() {
        // On an order path -2011/-2013 mean "already cancelled"; a query has no order to cancel.
        for code in [-2011, -2013] {
            let err = classify_query(bad_request(code, "Unknown order sent."));
            assert!(
                matches!(err, UnindexedClientError::Api(ApiError::RequestRejected(_))),
                "{code}: got {err:?}"
            );
        }
    }

    #[test]
    fn query_rejections_with_auth_or_throttle_codes_keep_their_meaning() {
        for (code, msg) in [
            (-1002, "You are not authorized to execute this request."),
            (-2015, "Invalid API-key, IP, or permissions for action."),
            (-1022, "Signature for this request is not valid."),
            (-2014, "API-key format invalid."),
        ] {
            let err = classify_query(bad_request(code, msg));
            assert!(
                matches!(err, UnindexedClientError::Api(ApiError::Unauthenticated(_))),
                "{code}: got {err:?}"
            );
        }
        for code in [-1003, -1015] {
            let err = classify_query(bad_request(code, "Too much request weight used."));
            assert!(
                matches!(err, UnindexedClientError::Api(ApiError::RateLimit)),
                "{code}: got {err:?}"
            );
        }
    }

    #[test]
    fn query_rejections_a_retry_can_cure_stay_transient() {
        for code in [-1000, -1001, -1006, -1007, -1008, -1021] {
            let err = classify_query(bad_request(code, "Internal error; please try again."));
            assert!(
                matches!(err, UnindexedClientError::Connectivity(_)),
                "{code}: got {err:?}"
            );
            assert!(err.is_transient());
        }
    }

    #[test]
    fn query_transport_server_and_throttle_failures_classify_like_orders() {
        for ce in [
            ConnectorError::ConnectorClientError {
                msg: "HTTP request failed: connection reset".to_string(),
                code: None,
            },
            ConnectorError::ServerError {
                msg: "Server error: 503".to_string(),
                status_code: Some(503),
            },
            ConnectorError::NetworkError("connection reset".to_string()),
        ] {
            let err = classify_query(ce);
            assert!(
                matches!(err, UnindexedClientError::Connectivity(_)),
                "got {err:?}"
            );
            assert!(err.is_transient());
        }

        let err = classify_query(ConnectorError::TooManyRequestsError {
            msg: "Too many requests.".to_string(),
            code: Some(-1003),
        });
        assert!(matches!(
            err,
            UnindexedClientError::Api(ApiError::RateLimit)
        ));

        let err = classify_query(ConnectorError::UnauthorizedError {
            msg: "API-key format invalid.".to_string(),
            code: Some(-2014),
        });
        let UnindexedClientError::Api(ApiError::Unauthenticated(msg)) = &err else {
            panic!("expected Unauthenticated, got {err:?}");
        };
        assert!(msg.contains("-2014"), "code missing from {msg:?}");
    }

    #[test]
    fn a_request_the_client_failed_to_build_is_internal() {
        let err = classify_rest_query_error(
            &anyhow::Error::new(ParamBuildError::UninitializedField("symbol")),
            None,
        );
        assert!(
            matches!(&err, UnindexedClientError::Internal(msg) if msg.contains("symbol")),
            "got {err:?}"
        );
        assert!(!err.is_transient());
    }

    #[test]
    fn a_response_body_that_does_not_decode_is_internal() {
        // What binance-sdk's `data()` returns when the body does not fit its model: the
        // `serde_json` error as a codeless `ConnectorClientError`.
        let err = response_decode_error(ConnectorError::ConnectorClientError {
            msg: "invalid type: map, expected a sequence at line 1 column 0".to_string(),
            code: None,
        });
        assert!(
            matches!(&err, UnindexedClientError::Internal(msg) if msg.contains("invalid type")),
            "got {err:?}"
        );
        assert!(!err.is_transient());
    }

    #[test]
    fn untyped_query_errors_are_classified_by_their_text() {
        // Errors from the SDK's request plumbing arrive as untyped text, not a ConnectorError.
        for msg in [
            "Error -1002: unauthorized",
            // Code only, no auth wording: isolates the numeric-code branch.
            "Error -2015: permission denied for action",
            "invalid signature provided",
            "The signature for this request is not valid.",
            "Invalid API-key format",
        ] {
            let err = classify_rest_query_error(&anyhow::anyhow!("{msg}"), None);
            assert!(
                matches!(err, UnindexedClientError::Api(ApiError::Unauthenticated(_))),
                "{msg:?}: got {err:?}"
            );
        }

        // Anything else failed before the request left, and fails the same way on retry.
        let err = classify_rest_query_error(
            &anyhow::anyhow!("relative URL without a base").context("Failed to join base URL"),
            None,
        );
        assert!(
            matches!(err, UnindexedClientError::Internal(_)),
            "got {err:?}"
        );
        assert!(!err.is_transient());
    }

    #[test]
    fn a_forbidden_query_is_an_auth_failure_only_when_it_says_so() {
        // Binance documents 403 as its WAF limit: back off and retry.
        let err = classify_query(ConnectorError::ForbiddenError {
            msg: "<html>403 Forbidden</html>".to_string(),
            code: None,
        });
        assert!(
            matches!(err, UnindexedClientError::Api(ApiError::RateLimit)),
            "got {err:?}"
        );

        let err = classify_query(ConnectorError::ForbiddenError {
            msg: "Invalid API-key, IP, or permissions for action.".to_string(),
            code: Some(-2015),
        });
        assert!(
            matches!(err, UnindexedClientError::Api(ApiError::Unauthenticated(_))),
            "got {err:?}"
        );
    }

    #[test]
    fn query_classification_reads_the_sdk_error_through_added_context() {
        let err = classify_rest_query_error(
            &anyhow::Error::new(bad_request(-1127, "More than 24 hours.")).context("outer"),
            None,
        );
        assert!(
            matches!(err, UnindexedClientError::Api(ApiError::RequestRejected(_))),
            "got {err:?}"
        );
    }

    // --- Order rejection codes ---

    #[test]
    fn an_order_forbidden_by_the_waf_is_a_rate_limit() {
        // Refused at the edge: the order never reached the matching engine.
        assert!(matches!(
            classify_err(ConnectorError::ForbiddenError {
                msg: "<html>403 Forbidden</html>".to_string(),
                code: None,
            }),
            OrderError::Rejected(ApiError::RateLimit)
        ));
    }

    #[test]
    fn an_order_the_venue_failed_has_an_unknown_status() {
        // Binance: the order may have executed. A definitive rejection would hide a fill.
        for code in [-1000, -1001, -1006, -1007, -1008] {
            let err = classify_err(ConnectorError::BadRequestError {
                msg: "Timeout waiting for response from backend server.".to_string(),
                code: Some(code),
            });
            assert!(
                matches!(err, OrderError::Connectivity(_)),
                "{code}: got {err:?}"
            );
            assert!(err.is_transient());
        }

        // The WS-API reports the same codes in its `ResponseError` text.
        let err = parse_binance_order_rejection(
            "Server\u{2010}side response error (code -1007): Timeout waiting for response from \
             backend server. Send status unknown; execution status unknown."
                .to_string(),
            &InstrumentNameExchange::new("BTCUSDT"),
        );
        assert!(matches!(err, OrderError::Connectivity(_)), "got {err:?}");
    }

    #[test]
    fn an_order_with_a_bad_signature_or_key_format_is_unauthenticated() {
        for (code, msg) in [
            (-1022, "Signature for this request is not valid."),
            (-2014, "API-key format invalid."),
        ] {
            let err = classify_err(ConnectorError::BadRequestError {
                msg: msg.to_string(),
                code: Some(code),
            });
            assert!(
                matches!(err, OrderError::Rejected(ApiError::Unauthenticated(_))),
                "{code}: got {err:?}"
            );
        }
    }

    #[test]
    fn a_definitive_order_rejection_is_still_a_rejection() {
        let err = classify_err(ConnectorError::BadRequestError {
            msg: "Account has insufficient balance for requested action.".to_string(),
            code: Some(-2010),
        });
        assert!(
            matches!(
                err,
                OrderError::Rejected(ApiError::BalanceInsufficient(None, _))
            ),
            "got {err:?}"
        );
    }

    /// `-2010` is Binance's generic NEW_ORDER_REJECTED, so only its text says whether the balance
    /// ran short, and Binance never names the asset. Margin reaches the venue over REST, Spot over
    /// the WS API; both must read it the same way.
    #[test]
    fn a_2010_rejection_is_read_by_its_message() {
        let cases: [(&str, fn(&UnindexedOrderError) -> bool); 3] = [
            (
                "Account has insufficient balance for requested action.",
                |err| {
                    matches!(
                        err,
                        OrderError::Rejected(ApiError::BalanceInsufficient(None, _))
                    )
                },
            ),
            ("Order would trigger immediately.", |err| {
                matches!(err, OrderError::Rejected(ApiError::OrderRejected(_)))
            }),
            (
                "Trailing stop orders are not supported for this symbol.",
                |err| matches!(err, OrderError::Rejected(ApiError::OrderRejected(_))),
            ),
        ];
        for (msg, expected) in cases {
            let rest = classify_err(ConnectorError::BadRequestError {
                msg: msg.to_string(),
                code: Some(-2010),
            });
            assert!(expected(&rest), "REST {msg:?}: got {rest:?}");

            let ws = classify_ws(-2010, msg);
            assert!(
                ws.as_ref().is_some_and(expected),
                "WS API {msg:?}: got {ws:?}"
            );
        }
    }

    #[test]
    fn order_rejections_keep_their_order_meaning() {
        let instrument = InstrumentNameExchange::new("BTCUSDT");
        for (msg, expected) in [
            // Cancel races stay "already cancelled".
            (
                "-2013 Order does not exist.",
                ApiError::OrderAlreadyCancelled,
            ),
            ("-2011 Unknown order sent.", ApiError::OrderAlreadyCancelled),
            // A stale timestamp on an order is a definitive refusal: it did not execute.
            (
                "-1021 Timestamp for this request is outside of the recvWindow.",
                ApiError::OrderRejected(
                    "-1021 Timestamp for this request is outside of the recvWindow.".to_string(),
                ),
            ),
            // Auth by wording alone, with no code.
            (
                "Invalid API-key, IP, or permissions for action.",
                ApiError::Unauthenticated(
                    "Invalid API-key, IP, or permissions for action.".to_string(),
                ),
            ),
        ] {
            assert_eq!(
                parse_binance_order_rejection(msg.to_string(), &instrument),
                OrderError::Rejected(expected),
                "{msg:?}"
            );
        }
    }

    fn classify_ws(code: i64, message: &str) -> Option<UnindexedOrderError> {
        classify_ws_order_error(
            &anyhow::Error::new(WebsocketError::ResponseError {
                code,
                message: message.to_string(),
            }),
            &InstrumentNameExchange::new("BTCUSDT"),
        )
    }

    #[test]
    fn a_ws_response_without_an_error_body_is_read_by_its_status() {
        // With no error object the SDK reports the HTTP status as `code`, "Unknown error" as text.
        let err = classify_ws(503, "Unknown error");
        assert!(
            matches!(err, Some(OrderError::Connectivity(_))),
            "a 5xx leaves the order's status unknown: {err:?}"
        );
        for code in [429, 418, 403] {
            let err = classify_ws(code, "Unknown error");
            assert!(
                matches!(err, Some(OrderError::Rejected(ApiError::RateLimit))),
                "{code}: got {err:?}"
            );
        }
        let err = classify_ws(401, "Unknown error");
        assert!(
            matches!(
                err,
                Some(OrderError::Rejected(ApiError::Unauthenticated(_)))
            ),
            "got {err:?}"
        );
        let err = classify_ws(409, "Unknown error");
        assert!(
            matches!(err, Some(OrderError::Rejected(ApiError::OrderRejected(_)))),
            "got {err:?}"
        );
    }

    #[test]
    fn a_ws_response_with_a_binance_code_is_read_by_its_code() {
        let err = classify_ws(-1007, "Timeout waiting for response from backend server.");
        assert!(
            matches!(err, Some(OrderError::Connectivity(_))),
            "got {err:?}"
        );

        // A context layer must not hide the code.
        let wrapped = anyhow::Error::new(WebsocketError::ResponseError {
            code: -1007,
            message: "Timeout waiting for response from backend server.".to_string(),
        })
        .context("outer");
        let err = classify_ws_order_error(&wrapped, &InstrumentNameExchange::new("BTCUSDT"));
        assert!(
            matches!(err, Some(OrderError::Connectivity(_))),
            "got {err:?}"
        );
        let err = classify_ws(-1003, "Too much request weight used.");
        assert!(
            matches!(err, Some(OrderError::Rejected(ApiError::RateLimit))),
            "got {err:?}"
        );
    }

    #[test]
    fn a_waf_403_is_backed_off_as_a_rate_limit_but_an_auth_403_is_not() {
        let waf = anyhow::Error::new(ConnectorError::ForbiddenError {
            msg: "<html>403 Forbidden</html>".to_string(),
            code: None,
        });
        assert!(is_rate_limit_error(&waf));

        let auth = anyhow::Error::new(ConnectorError::ForbiddenError {
            msg: "Invalid API-key, IP, or permissions for action.".to_string(),
            code: Some(-2015),
        });
        assert!(!is_rate_limit_error(&auth));
    }

    /// The weight-per-minute entry a WS-API response carries.
    fn ws_weight(limit: u32, count: u32) -> Vec<WebsocketApiRateLimit> {
        vec![
            WebsocketApiRateLimit {
                rate_limit_type: RateLimitType::Orders,
                interval: Interval::Second,
                interval_num: 10,
                limit: 100,
                count: 100,
            },
            WebsocketApiRateLimit {
                rate_limit_type: RateLimitType::RequestWeight,
                interval: Interval::Minute,
                interval_num: 1,
                limit,
                count,
            },
        ]
    }

    /// Whether a wait of `kind` is still pending after 1 ms of (paused) time.
    async fn waits(tracker: &RateLimitTracker, kind: RequestKind) -> bool {
        tokio::time::timeout(Duration::from_millis(1), tracker.wait_if_blocked(kind))
            .await
            .is_err()
    }

    /// The pause runs to just past the end of the minute the request was sent in, and a
    /// response observed after that pauses nothing.
    #[test]
    fn pause_after_ends_just_past_the_send_minute() {
        let minute = 29_000_000 * 60_000_u128;
        let slack = MINUTE_BOUNDARY_SLACK_MS;
        let ms = |ms: u64| Some(Duration::from_millis(ms));
        assert_eq!(
            pause_after(minute + 15_000, minute + 15_000),
            ms(45_000 + slack)
        );
        assert_eq!(pause_after(minute, minute + 100), ms(59_900 + slack));
        assert_eq!(pause_after(minute + 59_999, minute + 59_999), ms(1 + slack));
        // Sent in the previous minute, observed during the slack: only the slack's rest.
        assert_eq!(pause_after(minute - 100, minute + 400), ms(slack - 400));
        // Sent in the previous minute, observed after it and the slack ended.
        assert_eq!(pause_after(minute - 100, minute + u128::from(slack)), None);
        assert_eq!(pause_after(minute - 100, minute + 30_000), None);
    }

    /// At the threshold, queries pause and orders do not; one under it, nothing pauses.
    #[tokio::test]
    async fn weight_near_the_limit_pauses_queries_but_not_orders() {
        tokio::time::pause();
        let tracker = RateLimitTracker::new(WeightPool::Spot);
        tracker.observe_ws_api(Some(&ws_weight(1_000, 899)), unix_ms());
        assert!(!waits(&tracker, RequestKind::Query).await, "below 90%");

        tracker.observe_ws_api(Some(&ws_weight(1_000, 900)), unix_ms());
        assert!(waits(&tracker, RequestKind::Query).await, "at 90%");
        assert!(!waits(&tracker, RequestKind::Order).await);
        assert!(
            !tracker.is_blocked(),
            "a pause is not a rate-limit cooldown"
        );

        tokio::time::advance(Duration::from_millis(60_000 + MINUTE_BOUNDARY_SLACK_MS)).await;
        assert!(!waits(&tracker, RequestKind::Query).await, "the pause ends");
    }

    /// Essential reads skip the pause near the limit, like orders, but wait out a cooldown.
    #[tokio::test]
    async fn essential_reads_skip_the_pause_but_not_a_cooldown() {
        tokio::time::pause();
        let tracker = RateLimitTracker::new(WeightPool::Spot);
        tracker.throttle(Duration::from_secs(30));
        assert!(waits(&tracker, RequestKind::Query).await);
        assert!(!waits(&tracker, RequestKind::Essential).await);

        tracker.on_rate_limited(Some(Duration::from_secs(5)));
        assert!(waits(&tracker, RequestKind::Essential).await);
    }

    /// A rate-limit cooldown holds back orders as well as queries.
    #[tokio::test]
    async fn a_rate_limit_cooldown_holds_back_both_kinds() {
        tokio::time::pause();
        let tracker = RateLimitTracker::new(WeightPool::Spot);
        tracker.on_rate_limited(Some(Duration::from_secs(5)));
        assert!(tracker.is_blocked());
        assert!(waits(&tracker, RequestKind::Order).await);
        assert!(waits(&tracker, RequestKind::Query).await);

        tokio::time::advance(Duration::from_secs(5)).await;
        assert!(!tracker.is_blocked());
        assert!(!waits(&tracker, RequestKind::Order).await);
    }

    /// A WS-API response replaces the default limit, so the same usage can cross the threshold
    /// under the reported limit that it stays under by default. A zero limit is ignored.
    #[tokio::test]
    async fn the_ws_api_limit_replaces_the_default() {
        tokio::time::pause();
        let tracker = RateLimitTracker::new(WeightPool::Spot);
        tracker.observe_ws_api(Some(&ws_weight(0, 1_000)), unix_ms());
        assert_eq!(
            tracker.weight_limit.load(Ordering::Relaxed),
            SPOT_REQUEST_WEIGHT_PER_MINUTE
        );
        assert!(!waits(&tracker, RequestKind::Query).await);

        tracker.observe_ws_api(Some(&ws_weight(1_100, 1_000)), unix_ms());
        assert_eq!(tracker.weight_limit.load(Ordering::Relaxed), 1_100);
        assert!(waits(&tracker, RequestKind::Query).await);
    }

    /// A late response, for a request sent in an earlier minute, pauses nothing.
    #[tokio::test]
    async fn a_response_from_an_earlier_minute_pauses_nothing() {
        tokio::time::pause();
        let tracker = RateLimitTracker::new(WeightPool::Spot);
        tracker.observe_ws_api(Some(&ws_weight(1_000, 1_000)), unix_ms() - 120_000);
        assert!(!waits(&tracker, RequestKind::Query).await);
    }

    /// A margin tracker ignores WS-API usage, which is spot weight. A debug build asserts that it
    /// is never given any.
    #[tokio::test]
    #[cfg_attr(
        debug_assertions,
        should_panic(expected = "WS-API usage is spot weight")
    )]
    async fn a_margin_tracker_ignores_ws_api_usage() {
        tokio::time::pause();
        let tracker = RateLimitTracker::new(WeightPool::Sapi);
        tracker.observe_ws_api(Some(&ws_weight(1_000, 1_000)), unix_ms());
        assert_eq!(
            tracker.weight_limit.load(Ordering::Relaxed),
            SAPI_IP_WEIGHT_PER_MINUTE
        );
        assert!(!waits(&tracker, RequestKind::Query).await);
    }

    /// A response without the weight-per-minute entry changes nothing.
    #[tokio::test]
    async fn a_response_without_weight_changes_nothing() {
        tokio::time::pause();
        let tracker = RateLimitTracker::new(WeightPool::Spot);
        tracker.observe_ws_api(None, unix_ms());
        tracker.observe_ws_api(Some(&ws_weight(1_000, 1_000)[..1]), unix_ms());
        assert_eq!(
            tracker.weight_limit.load(Ordering::Relaxed),
            SPOT_REQUEST_WEIGHT_PER_MINUTE
        );
        assert!(!waits(&tracker, RequestKind::Query).await);
    }

    /// A handshake Binance refused with 429 or 418 is a rate limit; other failures are not.
    #[test]
    fn handshake_rate_limit_is_recognised() {
        let handshake = |msg: &str| anyhow::Error::new(WebsocketError::Handshake(msg.into()));
        assert!(is_handshake_rate_limit(&handshake(
            "HTTP error: 429 Too Many Requests"
        )));
        assert!(is_handshake_rate_limit(&handshake(
            "HTTP error: 418 I'm a teapot"
        )));
        assert!(is_handshake_rate_limit(
            &handshake("HTTP error: 429 Too Many Requests").context("connecting")
        ));
        assert!(!is_handshake_rate_limit(&handshake(
            "HTTP error: 503 Service Unavailable"
        )));
        assert!(!is_handshake_rate_limit(&anyhow::Error::new(
            WebsocketError::Timeout
        )));
        assert!(!is_handshake_rate_limit(&anyhow::anyhow!(
            "HTTP error: 429 Too Many Requests"
        )));
    }

    /// What placing an order reports, by the response's status: an order that ended in the
    /// response is reported as it ended, not as open.
    #[test]
    fn a_placement_response_is_read_from_its_status() {
        use rust_decimal_macros::dec;

        let btc = InstrumentNameExchange::new("BTCUSDT");
        let time = Utc.timestamp_millis_opt(1_700_000_000_000).unwrap();
        let placed = |status: Option<&str>, filled: Decimal, quote: Option<&str>| {
            placed_order_state(
                ExchangeId::BinanceSpot,
                &btc,
                status,
                OrderId::new("7"),
                time,
                filled,
                dec!(2),
                quote,
            )
        };
        let expired = |filled| {
            OrderState::Inactive(InactiveOrderState::Expired(Expired::new(
                OrderId::new("7"),
                time,
                filled,
            )))
        };
        let open = |filled| {
            OrderState::active(Open::new(
                VenueOrderId::Assigned(OrderId::new("7")),
                time,
                filled,
            ))
        };

        assert_eq!(
            placed(Some("EXPIRED"), Decimal::ZERO, Some("0")),
            expired(Decimal::ZERO),
            "an IOC or FOK order that found no liquidity"
        );
        assert_eq!(
            placed(Some("EXPIRED"), dec!(1), Some("100")),
            expired(dec!(1)),
            "an IOC order that partly filled and expired the rest"
        );
        assert_eq!(
            placed(Some("EXPIRED_IN_MATCH"), Decimal::ZERO, None),
            expired(Decimal::ZERO),
            "expired by self-trade prevention"
        );
        assert_eq!(
            placed(Some("FILLED"), dec!(2), Some("201")),
            OrderState::fully_filled(Filled::new(
                OrderId::new("7"),
                time,
                dec!(2),
                Some(dec!(100.5))
            ))
        );
        assert_eq!(
            placed(Some("CANCELED"), dec!(1), None),
            OrderState::Inactive(InactiveOrderState::Cancelled(Cancelled::new(
                OrderId::new("7"),
                time,
                dec!(1)
            )))
        );
        assert!(matches!(
            placed(Some("REJECTED"), Decimal::ZERO, None),
            OrderState::Inactive(InactiveOrderState::OpenFailed(OrderError::Rejected(
                ApiError::OrderRejected(_)
            )))
        ));
        for status in [
            Some("NEW"),
            Some("PARTIALLY_FILLED"),
            Some("PENDING_NEW"),
            None,
            Some("FROZEN"),
        ] {
            assert_eq!(
                placed(status, dec!(1), Some("100")),
                open(dec!(1)),
                "{status:?} reads as open from what filled"
            );
        }
        assert!(
            matches!(
                placed(None, dec!(2), None),
                OrderState::Inactive(InactiveOrderState::FullyFilled(_))
            ),
            "an ACK that somehow reports everything filled"
        );
    }

    #[test]
    fn the_average_price_is_the_quote_traded_over_the_base() {
        use rust_decimal_macros::dec;

        let avg = |quote, filled| binance_avg_price(ExchangeId::BinanceSpot, quote, filled);
        assert_eq!(avg(Some("100"), dec!(4)), Some(dec!(25)));
        assert_eq!(avg(Some("100"), Decimal::ZERO), None, "nothing filled");
        assert_eq!(avg(None, dec!(4)), None, "no quote");
        assert_eq!(
            avg(Some("-1"), dec!(4)),
            None,
            "Binance's \"not available\""
        );
        assert_eq!(avg(Some("not-a-number"), dec!(4)), None);
    }
}
