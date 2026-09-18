//! Gap-free resumption of a London Strategic Edge subscription across a reconnect.
//!
//! The provider serves a historical window before going live when a subscription carries a
//! `start`, which makes it possible to close the gap a reconnect would otherwise leave. This
//! module holds the state that survives a reconnect; [`transformer`](super::transformer) applies
//! it, and [`live`](super::live) sends it.

use super::channel::LseChannel;
use crate::{Identifier, exchange::ExchangeSub};
use chrono::{DateTime, Utc};
use fnv::FnvHashMap;
use rustrade_instrument::exchange::ExchangeId;
use rustrade_integration::subscription::SubscriptionId;
use std::sync::{Mutex, PoisonError};
use tracing::warn;

/// The newest instant delivered for one subscription, and how many events already delivered bear
/// exactly that instant.
///
/// The count is what makes resumption exact. `start` is **inclusive**, so resuming at the newest
/// instant re-delivers every event sharing it; the count says how many of those were already seen,
/// so they can be skipped by position. Resuming one tick *later* instead would be simpler and
/// would silently lose every event at that instant not yet consumed.
#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub(super) struct LseWatermark {
    /// The newest `time_exchange` delivered for this subscription.
    pub time_exchange: DateTime<Utc>,

    /// How many events already delivered carry exactly [`time_exchange`](Self::time_exchange).
    ///
    /// # This is a real count, not an "almost always 1" optimisation
    /// Tie density is per-dataset and can be large. Crypto timestamps are quantised to the
    /// millisecond, and **141 ticks sharing a single instant** were measured on one symbol; FX and
    /// CFD timestamps carry microseconds and were distinct on every tick sampled, making the count
    /// `1` there. Both are properties of today's feed rather than contract, which is why the
    /// arithmetic is written to be correct at any granularity.
    pub count_at_time: usize,
}

/// The key a watermark is filed under: the dataset it was delivered from, the subscription the
/// provider ticks it on, **and** the subscription kind the stream serves it as.
///
/// # Why the subscription alone is not enough
/// The provider publishes one data frame from one host, so a subscribe names a symbol and nothing
/// else. Every stream therefore files its ticks under a wire identifier — `tick|BTC/USD` — that
/// says nothing about which stream produced it. Two axes collapse onto it:
///
/// - **Kind.** Both supported kinds are decodings of the same frame, so one instrument subscribed
///   as trades and as top-of-book resolves to one identifier. This integration is required to serve
///   both kinds from every dataset, so that is the natural way to use the feature rather than an
///   exotic one.
/// - **Dataset.** The five `Lse<Server>` connectors share the endpoint and the identifier
///   construction, and [`LseSymbolShape::Bare`](super::market::LseSymbolShape::Bare) reconstructs a
///   symbol from the base asset alone — so `AAPL` on [`LseEquities`](super::LseEquities) and `AAPL`
///   on [`LseFutures`](super::LseFutures) are the same identifier. The pre-subscribe category guard
///   does not close this: it cross-checks only against datasets the provider labels, and it labels
///   none of those behind [`LseFutures`](super::LseFutures).
///
/// Sharing one watermark across either axis lets two streams advance it independently: whichever
/// ran ahead sets the resume point for both, and the one behind resumes past events it never
/// delivered — a silent gap. Both axes are therefore closed structurally here rather than left as a
/// caller obligation.
///
/// # What this key cannot close
/// Two streams that genuinely duplicate a `(dataset, symbol, kind)` triple — the same subscription
/// built twice and handed one state — are indistinguishable by construction, and no key can
/// separate them. That obligation is stated on [`LseResumeState`], which is where a caller meets it.
///
/// Neither the dataset nor the kind reaches the wire: both are partitions of this map, not part of
/// the subscription.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub(super) struct LseResumeKey {
    exchange: ExchangeId,
    subscription: SubscriptionId,
    kind: &'static str,
}

impl LseResumeKey {
    /// File a watermark under `subscription` on `exchange`, as served for `kind`.
    ///
    /// `kind` is [`SubscriptionKind::as_str`](crate::subscription::SubscriptionKind::as_str) —
    /// taken as a string rather than a type parameter because the map holds every kind at once.
    pub(super) fn new(
        exchange: ExchangeId,
        subscription: SubscriptionId,
        kind: &'static str,
    ) -> Self {
        Self {
            exchange,
            subscription,
            kind,
        }
    }

    /// The dataset this key partitions on.
    pub(super) fn exchange(&self) -> ExchangeId {
        self.exchange
    }

    /// The kind this key partitions on.
    pub(super) fn kind(&self) -> &'static str {
        self.kind
    }

    /// Consume the key for the subscription half, which is what a transformer keys its own
    /// per-connection bookkeeping on once it has filtered the snapshot to its own kind.
    pub(super) fn into_subscription(self) -> SubscriptionId {
        self.subscription
    }
}

/// Resume state shared between a [`LseSubscriber`](super::live::LseSubscriber) and the transformer
/// of every stream it opens.
///
/// # An opaque handle, deliberately
/// Construct it, hand it to
/// [`LseSubscriber::with_resume`](super::live::LseSubscriber::with_resume), and let delivery drive
/// it; there is no supported way to read or advance a watermark from outside. Reading one means
/// exposing the key it is filed under, and this provider's ticks are filed under a wire identifier
/// rather than anything a caller holds — publishing that identifier would freeze its spelling into
/// this contract and still hand back a map no caller could interpret, because deriving it is
/// one-way. A read surface keyed on the symbol is the shape to add if a caller ever needs one, and
/// adding it later against a stated need is a smaller commitment than taking one back.
///
/// # Reconnect, not crash recovery
/// The watermark advances on events the stream **emitted**, and lives in memory. It closes the gap
/// a reconnect opens; it does not survive the process. Recovering across a restart would require
/// the consumer to acknowledge what it durably stored, which is consumer policy and deliberately
/// not modelled here.
///
/// "Emitted" is one step short of "consumed": an event is recorded when the transformer produces
/// it, which is when it enters the stream's own buffer, not when the consumer polls it out. A
/// reconnect therefore resumes from the newest event the stream *produced*, and anything still
/// sitting in that buffer when the connection dropped is lost with it. Closing that last step would
/// mean the consumer acknowledging each event, which is the crash-recovery contract above and is
/// deliberately not modelled either.
///
/// # ⚠️ Sharing one state — the one obligation the caller carries
/// Watermarks are keyed by dataset, subscription **and** subscription kind, so one state serves any
/// set of streams that differ in any of those three. Two streams carrying the same symbol as
/// different kinds, or the same symbol on two datasets, each keep their own watermark and cannot
/// disturb one another.
///
/// **What is not closed, and cannot be:** two streams that duplicate a `(dataset, symbol, kind)`
/// triple. Nothing distinguishes them, so they share one watermark and advance it independently —
/// whichever runs ahead sets the resume point for both, and the one behind resumes past events it
/// never delivered, with no warning. [`StreamBuilder::subscribe`](crate::streams::builder::StreamBuilder::subscribe)
/// de-duplicates only *within* one batch, so this is reachable by building two `Streams` over the
/// same subscription and handing both the same state.
///
/// So: **give each duplicated triple its own state**, or do not duplicate one. Disjoint sets may
/// share freely.
///
/// # Opting in
/// Resumption is **off** unless [`LseSubscriber::with_resume`](super::live::LseSubscriber::with_resume)
/// is called, because the replay it triggers is not free: one hour of a single busy crypto symbol
/// replayed **107,395 ticks** before the first live tick, and a 24-hour window had not drained
/// after 30 seconds. A consumer that must stay current is better served by a gap than by a
/// multi-minute burst of backdated events; a consumer recording a continuous tape wants the
/// opposite. That is the caller's call to make, so it is not made here.
///
/// # Example
/// ```rust,ignore
/// use rustrade_data::exchange::lse::{live::LseSubscriber, resume::LseResumeState};
/// use std::sync::Arc;
///
/// let subscriber = LseSubscriber::from_env()?.with_resume(Arc::new(LseResumeState::new()));
/// ```
#[derive(Debug, Default)]
pub struct LseResumeState {
    // A plain `Mutex` rather than an `RwLock`. Within one reconnect chain the two accessors never
    // overlap at all -- the chain polls the outer stream (where the subscriber reads) only once the
    // inner stream (where the transformer writes) has fully drained. Across chains they do: this
    // state is documented as shareable between concurrently-spawned per-batch streams, and those
    // contend. A `Mutex` is still the right choice for that: the critical section is a hash lookup
    // and a field update with no allocation, which an `RwLock` would only make more expensive to
    // acquire. The lock is never held across an `await`.
    marks: Mutex<FnvHashMap<LseResumeKey, MarkState>>,
}

/// A watermark, plus the per-key bookkeeping that is not part of the resume arithmetic.
///
/// Kept beside [`LseWatermark`] rather than inside it so the watermark stays exactly the two values
/// resumption is computed from, and so equality on it means "the same resume point".
#[derive(Copy, Clone, Debug)]
struct MarkState {
    watermark: LseWatermark,

    /// Whether the out-of-order report has already fired for this key.
    ///
    /// A genuinely non-monotonic source would otherwise log once per late tick, which on a busy
    /// symbol is tens of thousands of identical lines.
    reported_out_of_order: bool,
}

impl LseResumeState {
    /// Construct empty resume state.
    pub fn new() -> Self {
        Self::default()
    }

    /// Record that an event bearing `time_exchange` was emitted under `key`.
    ///
    /// Advancing to a newer instant resets the count to one.
    ///
    /// # ⚠️ This assumes the feed is monotonic per subscription, and says so when it is not
    /// An instant *older* than the watermark is ignored rather than rolling it back: the watermark
    /// is the high-water mark of what was emitted, and moving it backwards would re-request events
    /// already seen. That is the right answer for a duplicate, but it is not free if the source is
    /// genuinely out of order — the resume point stays at the newest instant seen, so a reconnect
    /// asks for everything after it and the events between the late instant and the watermark are
    /// never re-requested. The reporting path in the transformer cannot see that either, because
    /// those frames are never sent.
    ///
    /// Every sample taken on this feed arrived in order, and the crypto tape is the one place the
    /// assumption is least obviously safe — it is an aggregated cross-venue series. So the
    /// assumption is stated rather than defended, and its first violation per key is logged.
    pub(super) fn record(&self, key: &LseResumeKey, time_exchange: DateTime<Utc>) {
        // The report is emitted *after* the guard is dropped. This state is documented as shareable
        // between concurrently-spawned per-batch streams, so the lock is contended, and a tracing
        // subscriber that writes synchronously would otherwise stall every other stream's `record`
        // for the length of a log write. The critical section stays a hash lookup and a field
        // update, which is what justifies the plain `Mutex` above.
        let out_of_order = {
            let mut marks = self.lock();

            match marks.get_mut(key) {
                Some(state) if time_exchange > state.watermark.time_exchange => {
                    state.watermark = LseWatermark {
                        time_exchange,
                        count_at_time: 1,
                    };

                    None
                }
                Some(state) if time_exchange == state.watermark.time_exchange => {
                    state.watermark.count_at_time += 1;

                    None
                }
                Some(state) if !state.reported_out_of_order => {
                    state.reported_out_of_order = true;

                    Some(state.watermark.time_exchange)
                }
                Some(_) => None,
                None => {
                    marks.insert(
                        key.clone(),
                        MarkState {
                            watermark: LseWatermark {
                                time_exchange,
                                count_at_time: 1,
                            },
                            reported_out_of_order: false,
                        },
                    );

                    None
                }
            }
        };

        if let Some(watermark) = out_of_order {
            warn!(
                exchange = %key.exchange,
                subscription = %key.subscription,
                kind = key.kind,
                instant = %time_exchange,
                watermark = %watermark,
                "London Strategic Edge emitted an event older than this subscription's resume \
                 point, which a monotonic feed cannot do; the resume point is left where it is, so \
                 a reconnect will not re-request the span between the two",
            );
        }
    }

    /// The watermark filed under `key`, if anything has been emitted for it.
    pub(super) fn watermark(&self, key: &LseResumeKey) -> Option<LseWatermark> {
        self.lock().get(key).map(|state| state.watermark)
    }

    /// Every watermark recorded so far, across every dataset and kind.
    ///
    /// Taken as a snapshot so a transformer can carry its own drop counters without holding the
    /// lock, or consulting it, per tick. The caller filters to its own dataset and kind.
    pub(super) fn snapshot(&self) -> FnvHashMap<LseResumeKey, LseWatermark> {
        self.lock()
            .iter()
            .map(|(key, state)| (key.clone(), state.watermark))
            .collect()
    }

    // A poisoned lock means some other thread panicked mid-update. The data behind it is a
    // high-water mark whose worst case is a slightly stale resume point, so recovering the inner
    // value is strictly better than propagating the panic and taking the market stream down.
    fn lock(&self) -> std::sync::MutexGuard<'_, FnvHashMap<LseResumeKey, MarkState>> {
        self.marks.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// The [`SubscriptionId`] the provider's ticks for `symbol` arrive under.
///
/// Shared with the tick decoder's own construction of the same identifier so the subscriber and
/// the stream cannot disagree about which subscription a watermark belongs to.
pub(super) fn subscription_id(symbol: &str) -> SubscriptionId {
    ExchangeSub::from((LseChannel::Tick, symbol)).id()
}

/// Render an instant as the epoch-seconds number the provider's `start` parameter expects.
///
/// # Why a float, and why microseconds
/// The server parses `start` with a float conversion first and only falls back to a datetime
/// parse, which accepts neither a fractional second nor a UTC offset — so the epoch form is the
/// only spelling that can express a sub-second resume point at all.
///
/// Microseconds are the provider's own resolution and the resolution it filters on: a `start`
/// placed 400 microseconds past an instant was measured to exclude every tick at that instant,
/// which neither a millisecond-flooring nor a millisecond-rounding server could do. An `f64`'s
/// spacing at present-day epochs is roughly a quarter of a microsecond, so this round-trips the
/// stored instant exactly rather than approximately.
pub(super) fn epoch_seconds(time: DateTime<Utc>) -> f64 {
    // The conversion is the point of this function, and its loss is bounded well below the
    // microsecond the value carries -- see the rustdoc above.
    #[allow(clippy::cast_precision_loss)]
    let micros = time.timestamp_micros() as f64;

    micros / 1_000_000.0
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use crate::subscription::{SubscriptionKind, book::OrderBooksL1, trade::PublicTrades};

    fn at(spelling: &str) -> DateTime<Utc> {
        spelling.parse::<DateTime<Utc>>().unwrap()
    }

    fn id() -> SubscriptionId {
        subscription_id("BTC/USD")
    }

    /// The key one symbol's trades are filed under.
    ///
    /// The kind comes from [`SubscriptionKind::as_str`] rather than a spelled-out literal, so
    /// these tests partition on the same string production files watermarks under.
    fn key() -> LseResumeKey {
        LseResumeKey::new(ExchangeId::LseCrypto, id(), PublicTrades.as_str())
    }

    fn key_for(symbol: &str) -> LseResumeKey {
        LseResumeKey::new(
            ExchangeId::LseCrypto,
            subscription_id(symbol),
            PublicTrades.as_str(),
        )
    }

    #[test]
    fn the_first_event_seeds_the_watermark_with_a_count_of_one() {
        let state = LseResumeState::new();
        state.record(&key(), at("2026-01-02T09:37:24.760146Z"));

        assert_eq!(
            state.watermark(&key()),
            Some(LseWatermark {
                time_exchange: at("2026-01-02T09:37:24.760146Z"),
                count_at_time: 1,
            })
        );
    }

    /// The whole point of carrying a count: repeats at one instant are ordinary on this feed, and
    /// resuming needs to know how many of them were already delivered.
    #[test]
    fn events_sharing_an_instant_accumulate_a_count() {
        let state = LseResumeState::new();
        for _ in 0..141 {
            state.record(&key(), at("2026-01-02T09:37:24.760Z"));
        }

        assert_eq!(state.watermark(&key()).unwrap().count_at_time, 141);
    }

    #[test]
    fn a_newer_instant_advances_the_watermark_and_resets_the_count() {
        let state = LseResumeState::new();
        state.record(&key(), at("2026-01-02T09:37:24.760146Z"));
        state.record(&key(), at("2026-01-02T09:37:24.760146Z"));
        state.record(&key(), at("2026-01-02T09:37:24.760147Z"));

        assert_eq!(
            state.watermark(&key()),
            Some(LseWatermark {
                time_exchange: at("2026-01-02T09:37:24.760147Z"),
                count_at_time: 1,
            })
        );
    }

    /// Rolling the watermark backwards would re-request events already delivered.
    #[test]
    fn an_older_instant_leaves_the_watermark_untouched() {
        let state = LseResumeState::new();
        state.record(&key(), at("2026-01-02T09:37:24.760147Z"));
        state.record(&key(), at("2026-01-02T09:37:24.760146Z"));

        assert_eq!(
            state.watermark(&key()),
            Some(LseWatermark {
                time_exchange: at("2026-01-02T09:37:24.760147Z"),
                count_at_time: 1,
            })
        );
    }

    #[test]
    fn subscriptions_are_tracked_independently() {
        let state = LseResumeState::new();
        state.record(&key_for("BTC/USD"), at("2026-01-02T09:00:00Z"));
        state.record(&key_for("ETH/USD"), at("2026-01-02T10:00:00Z"));

        assert_eq!(
            state.watermark(&key_for("BTC/USD")).unwrap().time_exchange,
            at("2026-01-02T09:00:00Z")
        );
        assert_eq!(
            state.watermark(&key_for("ETH/USD")).unwrap().time_exchange,
            at("2026-01-02T10:00:00Z")
        );
    }

    /// The provider has one channel, so both kinds tick under one wire identifier. Sharing a
    /// watermark between them would let whichever stream ran ahead set the resume point for both,
    /// and the one behind would resume past events it never delivered.
    #[test]
    fn one_symbol_served_as_two_kinds_keeps_two_watermarks() {
        let state = LseResumeState::new();
        let trades = LseResumeKey::new(ExchangeId::LseCrypto, id(), PublicTrades.as_str());
        let books = LseResumeKey::new(ExchangeId::LseCrypto, id(), OrderBooksL1.as_str());

        state.record(&trades, at("2026-01-02T09:00:00Z"));
        state.record(&trades, at("2026-01-02T10:00:00Z"));
        state.record(&books, at("2026-01-02T09:00:00Z"));

        assert_eq!(
            state.watermark(&trades).unwrap().time_exchange,
            at("2026-01-02T10:00:00Z"),
        );
        assert_eq!(
            state.watermark(&books).unwrap().time_exchange,
            at("2026-01-02T09:00:00Z"),
            "the trailing kind must resume from its own last delivery, not the other kind's",
        );
    }

    /// The five datasets share one endpoint and one identifier construction, and the `Bare` symbol
    /// shape rebuilds a symbol from the base asset alone — so one ticker subscribed on two of them
    /// produces one wire identifier. Sharing a watermark across that would let whichever stream ran
    /// ahead set the resume point for both.
    #[test]
    fn one_symbol_served_by_two_datasets_keeps_two_watermarks() {
        let state = LseResumeState::new();
        let bare = subscription_id("AAPL");
        let equities =
            LseResumeKey::new(ExchangeId::LseEquities, bare.clone(), PublicTrades.as_str());
        let futures =
            LseResumeKey::new(ExchangeId::LseFutures, bare.clone(), PublicTrades.as_str());

        assert_eq!(
            equities.clone().into_subscription(),
            futures.clone().into_subscription(),
            "this test is only meaningful while both datasets file under one identifier",
        );

        state.record(&equities, at("2026-01-02T09:00:00Z"));
        state.record(&equities, at("2026-01-02T10:00:00Z"));
        state.record(&futures, at("2026-01-02T09:00:00Z"));

        assert_eq!(
            state.watermark(&equities).unwrap().time_exchange,
            at("2026-01-02T10:00:00Z"),
        );
        assert_eq!(
            state.watermark(&futures).unwrap().time_exchange,
            at("2026-01-02T09:00:00Z"),
            "the trailing dataset must resume from its own last delivery, not the other dataset's",
        );
    }

    /// The kind partition is only a partition while distinct kinds spell themselves distinctly, and
    /// [`SubscriptionKind`] does not promise that — `Candles::as_str` answers `"candles"` for every
    /// interval. This provider serves no candles so nothing is reachable today, but the dependency
    /// is silent, and a collision here is a shared watermark rather than a compile error.
    #[test]
    fn the_two_served_kinds_spell_themselves_distinctly() {
        assert_ne!(PublicTrades.as_str(), OrderBooksL1.as_str());
    }

    #[test]
    fn an_untracked_subscription_has_no_watermark() {
        assert_eq!(LseResumeState::new().watermark(&key()), None);
    }

    /// The resume value must round-trip the stored instant exactly, because the server filters on
    /// it at microsecond resolution and the drop counter compares instants for equality.
    #[test]
    fn the_epoch_form_round_trips_an_instant_to_the_microsecond() {
        for spelling in [
            "2026-01-02T09:37:24.760146Z",
            "2026-08-14T10:16:55.161000Z",
            "2026-08-14T10:16:55.161999Z",
            "2026-01-02T09:37:24Z",
        ] {
            let instant = at(spelling);

            // Compared in the float domain rather than converting back, so the assertion needs no
            // truncating cast of its own.
            #[allow(clippy::cast_precision_loss)] // The bounded loss the function documents
            let expected = instant.timestamp_micros() as f64;
            let drift = epoch_seconds(instant) * 1_000_000.0 - expected;

            assert!(drift.abs() < 0.5, "{spelling} drifted {drift} microseconds");
        }
    }

    /// The tick decoder builds this identifier independently during deserialisation; if the two
    /// ever disagreed, every watermark would be filed under a subscription that never reads it.
    #[test]
    fn the_subscription_id_matches_the_one_the_tick_decoder_builds() {
        let tick: super::super::tick::LseMessage = serde_json::from_str(
            r#"{"type":"tick","symbol":"BTC/USD","ts":"2026-01-02T09:37:24.760146+00:00",
                "price":42000.5,"bid":42000.5,"ask":42001.0,"volume":0.00155}"#,
        )
        .unwrap();

        let super::super::tick::LseMessage::Tick(tick) = tick else {
            panic!("expected a tick");
        };

        assert_eq!(tick.subscription_id, subscription_id("BTC/USD"));
    }
}
