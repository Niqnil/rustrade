//! Account-event deduplication, shared across execution clients.
//!
//! Every client that can be served the same fill twice needs this. Two situations produce a
//! duplicate, and both are ordinary rather than exceptional:
//!
//! - **A subscription replays on reconnect.** A venue that opens a stream with a snapshot of
//!   recent activity re-delivers everything in that snapshot each time the socket comes back.
//! - **Recovery overlaps the stream.** A client that refetches fills over a lookback window after
//!   a gap will refetch some the stream already delivered, because the window is deliberately
//!   wider than the gap.
//!
//! The cache is keyed on (instrument, id, kind) rather than on the id alone, and [`DedupKey`]
//! documents why. It is bounded and LRU, so it degrades by forgetting the oldest event rather
//! than by growing without limit.
//!
//! This module is exchange-agnostic: it operates on rustrade's own
//! [`UnindexedAccountEvent`](crate::UnindexedAccountEvent), never on any SDK's wire types.

use crate::{AccountEventKind, UnindexedAccountEvent, order::state::OrderState};
use lru::LruCache;
use smol_str::SmolStr;
use std::{num::NonZeroUsize, sync::Arc};

/// Size of the LRU dedup cache. 10k entries covers ~hours of high-frequency
/// trading at typical fill rates; each entry is ~84-88 bytes (DedupKey =
/// SmolStr[24] + SmolStr[24] + DedupEventKind[1] + padding[7] = 56 bytes, plus
/// LruCache node overhead: 2 linked-list pointers[16] + hashbrown slot[~12-16]
/// ≈ 28-32 bytes). At 10k: ~840-880 KB.
/// At very high fill rates (>333 distinct fills/sec sustained during the 30s
/// recovery window), LRU eviction could allow a fill to pass dedup twice.
/// Increase this constant if such volumes are expected.
pub(crate) const DEDUP_CACHE_SIZE: usize = 10_000;

/// Event kind discriminant for dedup keys. Using an enum instead of a SmolStr
/// constant avoids constructing a string value on every event in the hot path.
#[derive(Debug, Hash, Eq, PartialEq, Clone, Copy)]
pub(crate) enum DedupEventKind {
    Trade,
    New,
    Cancelled,
}

/// Dedup cache key: (instrument, event ID, event kind).
///
/// Using separate `SmolStr` fields avoids the `format!("{}:{}", ...)` construction
/// and the heap allocation when a combined string would exceed SmolStr's 23-byte
/// inline limit. Both fields are already `SmolStr` values — no allocation needed.
///
/// - For TRADE events: instrument + trade_id + `DedupEventKind::Trade`
/// - For NEW events: instrument + order_id + `DedupEventKind::New`
/// - For CANCELED/EXPIRED: instrument + order_id + `DedupEventKind::Cancelled`
#[derive(Debug, Hash, Eq, PartialEq)]
pub(crate) struct DedupKey {
    pub(crate) instrument: SmolStr,
    pub(crate) id: SmolStr,
    pub(crate) kind: DedupEventKind,
}
pub(crate) type SharedDedupCache = Arc<parking_lot::Mutex<LruCache<DedupKey, ()>>>;

pub(crate) fn new_dedup_cache() -> SharedDedupCache {
    // allow(clippy::unwrap_used) — NonZeroUsize::new on a literal constant
    // cannot fail at runtime.
    #[allow(clippy::unwrap_used)]
    Arc::new(parking_lot::Mutex::new(LruCache::new(
        NonZeroUsize::new(DEDUP_CACHE_SIZE).unwrap(),
    )))
}

/// Extract a dedup key from an account event, if applicable.
/// Returns None for events that don't need deduplication (e.g. balance snapshots).
pub(crate) fn dedup_key_from_event(event: &UnindexedAccountEvent) -> Option<DedupKey> {
    // The instrument is part of the key because venue ids are routinely per-symbol rather
    // than global — Binance trade and order ids are, and Hyperliquid's own documentation
    // says `tid` should be qualified by coin rather than treated as globally unique. Without
    // it, BTCUSDT trade 9001 and ETHUSDT trade 9001 collide during a multi-symbol recovery.
    match &event.kind {
        AccountEventKind::Trade(trade) => Some(DedupKey {
            instrument: trade.instrument.name().clone(),
            id: trade.id.0.clone(),
            kind: DedupEventKind::Trade,
        }),
        AccountEventKind::OrderSnapshot(snap) => {
            // OrderSnapshot wraps Order<..., OrderState<...>>
            // For NEW events the state is Active(Open { id, .. })
            match &snap.0.state {
                OrderState::Active(active) => {
                    // ActiveOrderState variants: OpenInFlight, Open, CancelInFlight
                    // We only get OrderSnapshot for NEW events (Open state)
                    use crate::order::state::ActiveOrderState;
                    match active {
                        ActiveOrderState::Open(open) => Some(DedupKey {
                            instrument: snap.0.key.instrument.name().clone(),
                            id: open.id.0.clone(),
                            kind: DedupEventKind::New,
                        }),
                        _ => None,
                    }
                }
                _ => None,
            }
        }
        AccountEventKind::OrderCancelled(resp) => match &resp.state {
            Ok(cancelled) => Some(DedupKey {
                instrument: resp.key.instrument.name().clone(),
                id: cancelled.id.0.clone(),
                kind: DedupEventKind::Cancelled,
            }),
            Err(_) => None, // error responses don't need dedup
        },
        _ => None, // BalanceSnapshot, BalanceStreamUpdate, InstrumentBalanceUpdate, Snapshot, StreamTerminated — no dedup needed
    }
}

/// Check and insert a dedup key. Returns true if the event is a duplicate.
/// Takes `key` by value to avoid cloning on the non-duplicate (common) path.
pub(crate) fn is_duplicate(cache: &SharedDedupCache, key: DedupKey) -> bool {
    // parking_lot::Mutex — never poisons (if a prior callback panicked, the mutex
    // auto-unlocks cleanly). Blocking in an async context is acceptable here: the lock is
    // held for two hash ops on a bounded LRU cache (~microseconds), so a stalled worker
    // thread is negligible even under the worst contention a client produces — Binance's
    // fill recovery runs `buffer_unordered(8)` against it while the WS callback task is
    // live, and is itself bounded by a recovery timeout.
    //
    // Note: with `worker_threads = 1` those concurrent recovery tasks all block on this
    // mutex sequentially; upgrade to `tokio::sync::Mutex` if a single-worker runtime is
    // required.
    let mut guard = cache.lock();
    // peek avoids promoting the duplicate to MRU position (we're about to
    // discard it anyway), saving a linked-list move on the early-exit path.
    if guard.peek(&key).is_some() {
        return true;
    }
    guard.put(key, ());
    false
}
