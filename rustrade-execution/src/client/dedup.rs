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
//! The cache is keyed on (instrument, id, kind) rather than on the id alone, and `DedupKey`
//! documents why. It is bounded and LRU, so it degrades by forgetting the oldest event rather
//! than by growing without limit.
//!
//! This module is exchange-agnostic: it operates on rustrade's own
//! [`UnindexedAccountEvent`], never on any SDK's wire types.

use crate::{
    AccountEventKind, UnindexedAccountEvent, margin_debt::MarginDebtKind, order::state::OrderState,
};
use lru::LruCache;
use rust_decimal::Decimal;
use smol_str::SmolStr;
use std::{num::NonZeroUsize, sync::Arc};

/// Size of the LRU dedup cache. 10k entries covers ~hours of high-frequency
/// trading at typical fill rates; each entry is ~100-104 bytes (`DedupKey` =
/// `SmolStr` 24 + `SmolStr` 24 + `DedupEventKind` 24 (8-aligned by `Notice`'s `i64`) = 72
/// bytes, plus
/// `LruCache` node overhead: 2 linked-list pointers 16 + hashbrown slot ~12-16
/// ≈ 28-32 bytes). At 10k: ~1.0 MB.
/// At very high fill rates (>333 distinct fills/sec sustained during the 30s
/// recovery window), LRU eviction could allow a fill to pass dedup twice.
/// Increase this constant if such volumes are expected.
pub(crate) const DEDUP_CACHE_SIZE: usize = 10_000;

/// Event kind discriminant for dedup keys. Using an enum instead of a SmolStr
/// constant avoids constructing a string value on every event in the hot path.
#[derive(Debug, Hash, Eq, PartialEq, Clone, Copy)]
pub(crate) enum DedupEventKind {
    Trade,
    /// An order-state snapshot, carrying the order's cumulative filled quantity.
    ///
    /// The quantity is part of the discriminant because one order produces a succession of
    /// these and they are not interchangeable: the acknowledgement and every fill that follows
    /// it all describe the same `OrderId` in a different state. Keyed on the id alone, the
    /// acknowledgement swallows every later snapshot for that order, and `Open::filled_quantity`
    /// never advances past the `0` the acknowledgement carried.
    ///
    /// Replay suppression is unaffected, because a re-delivered snapshot carries the quantity it
    /// carried the first time and so still collides. That matters beyond saving work: a replayed
    /// acknowledgement for an order that has since retired is a partially-filled `Open` snapshot
    /// for an untracked order, which `OrderManager`'s vacant-entry arm inserts as a live resting
    /// order. Suppressing the replay is what keeps that order retired.
    ///
    /// `Decimal`'s `Hash` normalises before hashing and its `Eq` is numeric, so `1.0` and `1.00`
    /// are the same key — the representation a venue happens to send does not split it.
    OrderState {
        filled_quantity: Decimal,
    },
    Cancelled,
    /// A funding payment, keyed on its instrument and funding time because the venue gives it no
    /// id. Only Hyperliquid's funding stream produces one, so it exists only with that client.
    #[cfg(feature = "hyperliquid")]
    Funding,
    /// An account notice, keyed on its instrument (empty for the account as a whole), the venue's
    /// status and the time the venue sent it, since a notice has no id. A venue that sends one
    /// notice twice, such as on two streams, sends it with the same time, so the second is
    /// dropped.
    Notice {
        time_exchange_ms: i64,
    },
    /// A margin debt update, keyed on its instrument (empty for the account as a whole), its asset,
    /// the time the venue made the change, what changed it and the totals after it, since an update
    /// has no id. A venue that sends one update twice, such as on two streams, sends it with the
    /// same time and totals, so the second is dropped.
    ///
    /// The kind and totals are part of the discriminant because two changes to one asset can share
    /// a millisecond, such as a repay that settles several loans at once. Each states the debt
    /// after it, so dropping the second would leave the earlier total held. `Decimal`'s `Hash`
    /// and `Eq` are numeric, as for [`OrderState`](Self::OrderState).
    MarginDebt {
        time_exchange_ms: i64,
        kind: MarginDebtKind,
        borrowed: Decimal,
        interest: Decimal,
    },
}

/// Dedup cache key: (instrument, event ID, event kind).
///
/// Using separate `SmolStr` fields avoids the `format!("{}:{}", ...)` construction
/// and the heap allocation when a combined string would exceed SmolStr's 23-byte
/// inline limit. Both fields are already `SmolStr` values — no allocation needed.
///
/// - For TRADE events: instrument + trade_id + `DedupEventKind::Trade`
/// - For order snapshots: instrument + order_id + `DedupEventKind::OrderState`, which carries the
///   cumulative filled quantity so that an acknowledgement and the fills after it stay distinct
/// - For CANCELED/EXPIRED: instrument + order_id + `DedupEventKind::Cancelled`
/// - For Hyperliquid funding payments: coin + funding time in milliseconds +
///   `DedupEventKind::Funding`, since a payment has no id
/// - For account notices: instrument (empty when the notice names none) + the venue's status +
///   `DedupEventKind::Notice`, carrying the time the venue sent it
/// - For margin debt updates: instrument (empty when the update names none) + the asset +
///   `DedupEventKind::MarginDebt`, carrying the time the venue made the change, its kind and the
///   totals after it
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
            // OrderSnapshot wraps Order<..., OrderState<...>>. An acknowledgement and a fill both
            // arrive here as Active(Open { .. }), distinguished only by `filled_quantity` -- so
            // that field is part of the key rather than a detail the key discards.
            match &snap.0.state {
                OrderState::Active(active) => {
                    // ActiveOrderState variants: OpenInFlight, Open, CancelInFlight
                    use crate::order::state::ActiveOrderState;
                    match active {
                        ActiveOrderState::Open(open) => Some(DedupKey {
                            instrument: snap.0.key.instrument.name().clone(),
                            // A venue that named the order gives the key; one that did not leaves
                            // the client id, which is stable across re-deliveries for the same
                            // reason the venue id is.
                            id: match open.id.assigned() {
                                Some(id) => id.0.clone(),
                                None => snap.0.key.cid.0.clone(),
                            },
                            kind: DedupEventKind::OrderState {
                                filled_quantity: open.filled_quantity,
                            },
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
        // FillRecoveryGaveUp has no key either: two give-ups of one span are two failed reads.
        // TradeAmended has none. Alpaca's amendments are not deduplicated: whether Alpaca can send
        // one twice is unknown, and TradeAmendment tells the consumer to apply it idempotently.
        // IBKR's EventSink tracks each execution's revisions itself, which also keeps an
        // execution from going out after its correction.
        AccountEventKind::Notice(notice) => Some(DedupKey {
            instrument: notice
                .instrument
                .as_ref()
                .map(|instrument| instrument.name().clone())
                .unwrap_or_default(),
            id: notice.status.clone(),
            kind: DedupEventKind::Notice {
                time_exchange_ms: notice.time_exchange.timestamp_millis(),
            },
        }),
        AccountEventKind::MarginDebt(update) => Some(DedupKey {
            instrument: update
                .instrument
                .as_ref()
                .map(|instrument| instrument.name().clone())
                .unwrap_or_default(),
            id: update.asset.name().clone(),
            kind: DedupEventKind::MarginDebt {
                time_exchange_ms: update.time_exchange.timestamp_millis(),
                kind: update.kind,
                borrowed: update.debt.borrowed,
                interest: update.debt.interest,
            },
        }),
        // CashFlow has none here: a venue may give a flow no id, and what identifies one then is
        // the producer's to say, so each producer keys its own (Hyperliquid's funding on coin and
        // time).
        _ => None, // BalanceSnapshot, BalanceStreamUpdate, InstrumentBalanceUpdate, InstrumentBalanceSnapshot, Snapshot, StreamTerminated, FillRecoveryGaveUp, TradeAmended, ReinitFailed, CashFlow — no dedup needed
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

/// A shared, bounded record of the fills already delivered, for a consumer that takes the same
/// fill from more than one source.
///
/// Fills reach a consumer by more than one path: the live stream, a read of the span the stream
/// missed (see [`ExecutionClient::fetch_trades`](crate::client::ExecutionClient::fetch_trades)),
/// and a replay when a stream reopens. Their spans overlap on purpose, so the same fill arrives
/// twice. [`is_duplicate`](Self::is_duplicate) says whether a [`Trade`](crate::AccountEventKind::Trade)
/// event is one already seen, keyed by instrument and [`TradeId`](crate::trade::TradeId):
/// whichever path delivered it, a fill has one id.
///
/// Only fills are recorded. An order snapshot, a cancellation or a notice can legitimately repeat
/// (an order amended without a new fill is a second `Open` snapshot with the same quantity), so
/// each is always passed on.
///
/// Clones share one record, which holds the 10,000 most recent keys and forgets the oldest first.
///
/// Call [`is_duplicate`](Self::is_duplicate) when the event is delivered, not when it is read: it
/// records the event as seen, so an event read but never delivered would be dropped on its retry.
#[derive(Debug, Clone)]
pub struct AccountEventDeduper(SharedDedupCache);

impl Default for AccountEventDeduper {
    fn default() -> Self {
        Self::new()
    }
}

impl AccountEventDeduper {
    /// An empty record.
    pub fn new() -> Self {
        Self(new_dedup_cache())
    }

    /// Whether `event` is a fill already seen. Records it as seen when it was not. Always `false`
    /// for an event that is not a fill.
    pub fn is_duplicate(&self, event: &UnindexedAccountEvent) -> bool {
        matches!(event.kind, AccountEventKind::Trade(_))
            && dedup_key_from_event(event).is_some_and(|key| is_duplicate(&self.0, key))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        order::id::{OrderId, StrategyId},
        trade::{AssetFees, Trade, TradeId},
    };
    use chrono::DateTime;
    use rust_decimal::Decimal;
    use rustrade_instrument::{
        Side, asset::name::AssetNameExchange, exchange::ExchangeId,
        instrument::name::InstrumentNameExchange,
    };

    fn trade_event(instrument: &str, id: &str) -> UnindexedAccountEvent {
        UnindexedAccountEvent {
            exchange: ExchangeId::BinanceSpot,
            kind: AccountEventKind::Trade(Trade::new(
                TradeId::new(id),
                OrderId::new("o"),
                InstrumentNameExchange::new(instrument),
                StrategyId::unknown(),
                DateTime::UNIX_EPOCH,
                Side::Buy,
                Decimal::ONE,
                Decimal::ONE,
                None,
                AssetFees::new(AssetNameExchange::from("usdt"), Decimal::ZERO, None),
            )),
        }
    }

    #[test]
    fn a_trade_is_a_duplicate_the_second_time_it_is_seen_by_instrument_and_id() {
        let deduper = AccountEventDeduper::new();

        assert!(!deduper.is_duplicate(&trade_event("btc_usdt", "1")));
        assert!(deduper.is_duplicate(&trade_event("btc_usdt", "1")));
        // The id is per instrument.
        assert!(!deduper.is_duplicate(&trade_event("eth_usdt", "1")));
        assert!(!deduper.is_duplicate(&trade_event("btc_usdt", "2")));
    }

    #[test]
    fn clones_share_one_record() {
        let deduper = AccountEventDeduper::new();
        let clone = deduper.clone();

        assert!(!deduper.is_duplicate(&trade_event("btc_usdt", "1")));

        assert!(clone.is_duplicate(&trade_event("btc_usdt", "1")));
    }

    #[test]
    fn an_event_that_is_not_a_fill_is_never_a_duplicate() {
        let deduper = AccountEventDeduper::new();
        let event = UnindexedAccountEvent {
            exchange: ExchangeId::BinanceSpot,
            kind: AccountEventKind::StreamTerminated(
                crate::error::StreamTerminationReason::RateLimited {
                    retry_after: std::time::Duration::from_secs(1),
                },
            ),
        };

        assert!(!deduper.is_duplicate(&event));
        assert!(!deduper.is_duplicate(&event));
    }

    #[test]
    fn a_repeated_notice_is_passed_on_because_only_fills_are_recorded() {
        let deduper = AccountEventDeduper::new();
        let notice = || UnindexedAccountEvent {
            exchange: ExchangeId::BinanceSpot,
            kind: AccountEventKind::Notice(crate::notice::AccountNotice::new(
                crate::notice::NoticeKind::Other,
                None,
                DateTime::UNIX_EPOCH,
                None,
                "STATUS".into(),
            )),
        };

        // The client-level dedup would treat the second as a duplicate.
        assert!(dedup_key_from_event(&notice()).is_some());
        assert!(!deduper.is_duplicate(&notice()));
        assert!(!deduper.is_duplicate(&notice()));
    }

    /// A copy of a debt update keys alike, but a second change to the same asset in the same
    /// millisecond does not: it states a different debt, which must reach the consumer.
    #[test]
    fn only_a_copy_of_a_margin_debt_update_keys_alike() {
        use crate::{
            balance::MarginDetails,
            margin_debt::{MarginDebtKind, MarginDebtUpdate},
        };

        let update = |kind, borrowed: i64, interest: i64| {
            dedup_key_from_event(&UnindexedAccountEvent {
                exchange: ExchangeId::BinanceMargin,
                kind: AccountEventKind::MarginDebt(MarginDebtUpdate::new(
                    AssetNameExchange::new("BTC"),
                    None,
                    MarginDetails::new(Decimal::new(borrowed, 8), Decimal::new(interest, 8)),
                    kind,
                    "VENUE".into(),
                    DateTime::UNIX_EPOCH,
                )),
            })
        };

        assert!(update(MarginDebtKind::Repay, 6_000, 2).is_some());
        assert_eq!(
            update(MarginDebtKind::Repay, 6_000, 2),
            update(MarginDebtKind::Repay, 6_000, 2)
        );
        assert_ne!(
            update(MarginDebtKind::Repay, 6_000, 2),
            update(MarginDebtKind::Repay, 3_000, 2)
        );
        assert_ne!(
            update(MarginDebtKind::Repay, 6_000, 2),
            update(MarginDebtKind::Repay, 6_000, 0)
        );
        assert_ne!(
            update(MarginDebtKind::Repay, 6_000, 2),
            update(MarginDebtKind::Interest, 6_000, 2)
        );
    }
}
