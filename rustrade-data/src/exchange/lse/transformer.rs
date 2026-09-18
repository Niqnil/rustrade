//! The London Strategic Edge stream transformer.
//!
//! Decoding is delegated wholesale to [`StatelessTransformer`] — one provider frame maps to one
//! event and nothing about it needs state. What this type adds is the consuming half of
//! [`resume`](super::resume): skipping the events a reconnect's replay re-delivers.

use super::{
    resume::{LseResumeKey, LseResumeState, LseWatermark},
    tick::LseMessage,
};
use crate::{
    Identifier,
    error::DataError,
    event::{MarketEvent, MarketIter},
    exchange::Connector,
    subscription::{Map, SubscriptionKind},
    transformer::{ExchangeTransformer, stateless::StatelessTransformer},
};
use chrono::{DateTime, Utc};
use fnv::FnvHashMap;
use rustrade_instrument::exchange::ExchangeId;
use rustrade_integration::{
    Transformer, protocol::websocket::WsMessage, subscription::SubscriptionId,
};
use serde::Deserialize;
use std::{cmp::Ordering, sync::Arc};
use tokio::sync::mpsc;
use tracing::warn;

/// Transformer for every London Strategic Edge subscription kind.
///
/// Carries no resume state unless the caller opted in via
/// [`LseSubscriber::with_resume`](super::live::LseSubscriber::with_resume); without it this is a
/// direct pass-through to [`StatelessTransformer`].
#[derive(Debug)]
pub struct LseTransformer<Exchange, InstrumentKey, Kind> {
    inner: StatelessTransformer<Exchange, InstrumentKey, Kind, LseMessage>,
    resume: Option<ResumeTracker>,
}

/// Per-connection resume bookkeeping: the shared watermark, and what this connection still owes
/// the replay window.
#[derive(Debug)]
struct ResumeTracker {
    state: Arc<LseResumeState>,

    /// The dataset this stream serves, which partitions the shared state alongside the kind — see
    /// [`LseResumeKey`]. Held as a value because the transformer's `Exchange` type parameter is
    /// only reachable where its bounds are in scope, which the per-tick path is not.
    exchange: ExchangeId,

    /// The kind this stream serves, which partitions the shared state — see
    /// [`LseResumeKey`]. Held as a string because the transformer's `Kind` type parameter has no
    /// value to call [`SubscriptionKind::as_str`] on.
    kind: &'static str,

    /// Keyed by subscription alone: the snapshot is filtered to this stream's dataset and kind on
    /// construction, so both are constant across every entry here.
    pending: FnvHashMap<SubscriptionId, PendingDrop>,
}

/// Events still expected to arrive twice for one subscription, and the instant they carry.
#[derive(Copy, Clone, Debug)]
struct PendingDrop {
    time_exchange: DateTime<Utc>,
    remaining: usize,

    /// Whether the out-of-contract-ordering warning has already fired for this subscription.
    ///
    /// A provider replaying from *before* the requested instant would otherwise warn once per
    /// replayed tick, which on a busy symbol is tens of thousands of identical lines.
    warned_older: bool,

    /// Whether the provider announced a replay window opening later than the one requested.
    ///
    /// A clamp explains a shortfall at the resume instant completely — the window simply does not
    /// reach it — so the shortfall must not also be reported as the provider's history changing
    /// under the subscription. Two reports with different causes for one event is worse than one,
    /// because only one of them is true.
    ///
    /// Set when the boundary frame is read, which the provider was measured to send before the
    /// window it announces. The suppression is therefore ordered, not absolute: see the
    /// `Ordering::Greater` arm of [`should_drop`](ResumeTracker::should_drop) for what happens
    /// under the reverse order, and why it is tolerated.
    clamped: bool,
}

impl<Exchange, InstrumentKey, Kind> LseTransformer<Exchange, InstrumentKey, Kind>
where
    Exchange: Connector + Send,
    InstrumentKey: Clone + Send + Sync,
    Kind: SubscriptionKind + Send,
    Kind::Event: Sync,
    MarketIter<InstrumentKey, Kind::Event>: From<(ExchangeId, InstrumentKey, LseMessage)>,
{
    /// Construct a transformer, optionally resuming from previously delivered events.
    ///
    /// `resume` carries the kind this stream serves alongside the shared state, because the state
    /// is partitioned by dataset *and* kind — see [`LseResumeKey`] — and this type's `Kind`
    /// parameter has no value to derive the latter from. The dataset comes from `Exchange::ID`.
    ///
    /// The drop counters are **snapshotted** here rather than consulted per tick, so the shared
    /// lock is taken once per connection to read them. Recording still takes it once per emitted
    /// tick; the critical section there is a hash lookup and a field update.
    pub(super) async fn new(
        instrument_map: Map<InstrumentKey>,
        initial_snapshots: &[MarketEvent<InstrumentKey, Kind::Event>],
        ws_sink_tx: mpsc::UnboundedSender<WsMessage>,
        resume: Option<(Arc<LseResumeState>, &'static str)>,
    ) -> Result<Self, DataError> {
        let inner =
            StatelessTransformer::init(instrument_map, initial_snapshots, ws_sink_tx).await?;

        let resume = resume.map(|(state, kind)| ResumeTracker {
            pending: state
                .snapshot()
                .into_iter()
                .filter(|(key, _)| key.exchange() == Exchange::ID && key.kind() == kind)
                .map(|(key, watermark)| (key.into_subscription(), PendingDrop::from(watermark)))
                .collect(),
            state,
            exchange: Exchange::ID,
            kind,
        });

        Ok(Self { inner, resume })
    }
}

impl From<LseWatermark> for PendingDrop {
    fn from(watermark: LseWatermark) -> Self {
        Self {
            time_exchange: watermark.time_exchange,
            remaining: watermark.count_at_time,
            warned_older: false,
            clamped: false,
        }
    }
}

impl<Exchange, InstrumentKey, Kind> ExchangeTransformer<Exchange, InstrumentKey, Kind>
    for LseTransformer<Exchange, InstrumentKey, Kind>
where
    Exchange: Connector + Send,
    InstrumentKey: Clone + Send + Sync,
    Kind: SubscriptionKind + Send,
    Kind::Event: Sync,
    MarketIter<InstrumentKey, Kind::Event>: From<(ExchangeId, InstrumentKey, LseMessage)>,
{
    /// Initialise without resumption.
    ///
    /// This is the trait's only seam and it is a static function, so it cannot carry the caller's
    /// resume state; the stream that wants resumption bypasses it and constructs the transformer
    /// directly.
    ///
    /// Initial snapshots are forwarded rather than dropped here, so this layer stops being the
    /// place they are lost. That is as far as forwarding reaches: the inner
    /// [`StatelessTransformer`]'s own `init` discards the argument unconditionally, as every
    /// stateless connector's does, and this provider's streams fetch no snapshot for it to
    /// discard. A future LSE kind that does fetch one would
    /// need a transformer not backed by `StatelessTransformer` before the snapshot reached it —
    /// forwarding is what makes that a visible gap rather than one hidden behind a `&[]` at the
    /// call site.
    async fn init(
        instrument_map: Map<InstrumentKey>,
        initial_snapshots: &[MarketEvent<InstrumentKey, Kind::Event>],
        ws_sink_tx: mpsc::UnboundedSender<WsMessage>,
    ) -> Result<Self, DataError> {
        Self::new(instrument_map, initial_snapshots, ws_sink_tx, None).await
    }
}

impl<Exchange, InstrumentKey, Kind> Transformer for LseTransformer<Exchange, InstrumentKey, Kind>
where
    Exchange: Connector,
    InstrumentKey: Clone,
    Kind: SubscriptionKind,
    MarketIter<InstrumentKey, Kind::Event>: From<(ExchangeId, InstrumentKey, LseMessage)>,
{
    type Error = DataError;
    type Input = LseMessage;
    type Output = MarketEvent<InstrumentKey, Kind::Event>;
    type OutputIter = Vec<Result<Self::Output, Self::Error>>;

    fn transform(&mut self, input: Self::Input) -> Self::OutputIter {
        // The replay boundary is consumed here rather than forwarded. It is the only evidence that
        // a requested window was clamped to the provider's retention, and it cannot be caught
        // during the handshake -- see `LseMessage::ReplayStarted`. No decoder has a market event to
        // build from it either way.
        if let LseMessage::ReplayStarted {
            subscription_id,
            from,
        } = &input
        {
            let (subscription_id, from) = (subscription_id.clone(), *from);

            if let Some(resume) = self.resume.as_mut() {
                resume.warn_if_clamped(&subscription_id, from);
            }

            return vec![];
        }

        // A rejection raised *after* the handshake completed -- a later `LIMIT_REACHED`, an
        // `INVALID_START` on a resumed symbol, an expired credential. The handshake validator
        // cannot see these: it has already stopped reading. Nothing downstream can act on one
        // either, since the provider does not name the symbol it rejected, so the frame yields no
        // market event -- but the stream simply stops producing events for whatever was dropped,
        // and a rejection is the last thing that may reach the consumer as silence.
        if let LseMessage::Error { code, message } = &input {
            warn!(
                exchange = %Exchange::ID,
                code = code.as_deref().unwrap_or("unknown"),
                message = message.as_deref().unwrap_or("no message"),
                "London Strategic Edge rejected something after the handshake completed; the \
                 rejection does not name a symbol, so check whether a subscription has gone quiet",
            );

            return vec![];
        }

        // Nothing to track when the caller did not opt in, and a control frame carries no instant
        // to track. Both go straight through.
        let Some((subscription_id, time_exchange, replay)) =
            self.resume.as_ref().and_then(|_| match &input {
                LseMessage::Tick(tick) => Some((
                    tick.subscription_id.clone(),
                    tick.time_exchange,
                    tick.replay,
                )),
                _ => None,
            })
        else {
            return self.inner.transform(input);
        };

        if let Some(resume) = self.resume.as_mut()
            && resume.should_drop(&subscription_id, time_exchange, replay)
        {
            return vec![];
        }

        let output = self.inner.transform(input);

        // Record against what was actually emitted. An unidentifiable instrument yields an error
        // rather than an event, and a watermark advanced past an event nobody received would
        // leave a gap on the next reconnect.
        if let Some(resume) = self.resume.as_ref()
            && output.iter().any(Result::is_ok)
        {
            resume.record(subscription_id, time_exchange);
        }

        output
    }
}

impl ResumeTracker {
    /// Whether this event was already delivered by the connection this one replaced.
    fn should_drop(
        &mut self,
        subscription_id: &SubscriptionId,
        time_exchange: DateTime<Utc>,
        replay: bool,
    ) -> bool {
        let exchange = self.exchange;
        let Some(pending) = self.pending.get_mut(subscription_id) else {
            return false;
        };

        match time_exchange.cmp(&pending.time_exchange) {
            // Inside the instant the watermark names, and the previous connection is known to have
            // delivered this many events there. `start` is inclusive, so the provider replays them
            // all and they are skipped by position.
            Ordering::Equal if pending.remaining > 0 => {
                pending.remaining -= 1;

                // The skip stays positional and does NOT require `replay`, deliberately: the flag
                // is absent on live ticks, so a provider that stopped stamping replayed ones would
                // turn every reconnect into a duplicate flood if the drop depended on it. But a
                // tick arriving unstamped inside the window is a live event being discarded as a
                // duplicate, which only happens if the replay under-delivered at this instant, so
                // it is reported. Bounded by the count the watermark carries.
                if !replay {
                    warn!(
                        %exchange,
                        subscription = %subscription_id,
                        instant = %time_exchange,
                        "London Strategic Edge delivered an unreplayed tick inside the resume \
                         window and it was skipped as an already-delivered duplicate; a genuinely \
                         new tick may have been lost",
                    );
                }

                true
            }
            // The instant is exhausted; anything further carrying it is genuinely new.
            Ordering::Equal => false,
            Ordering::Greater => {
                let undelivered = pending.remaining;
                let instant = pending.time_exchange;

                // The window is closed for good, but the entry is KEPT. It carries the instant that
                // went out as `start`, which is the only record of what was requested, and the
                // clamp check reads it out of here when the boundary frame arrives -- removing it
                // would make a clamp undetectable for any subscription whose first replayed tick
                // beat its own `replay_started`. Zeroing the count closes the drop window just as
                // removal did: `Ordering::Equal` with nothing remaining emits.
                pending.remaining = 0;

                // The replay held fewer events at the watermark's instant than were delivered from
                // it. Positional skipping assumes the replay reproduces what went out live -- an
                // assumption that holds on every measurement taken, including one instant carrying
                // 141 events -- so a shortfall means the provider's history changed underneath the
                // subscription. No event is lost by it, but the assumption is load-bearing enough
                // that its failure must be visible rather than inferred from a gap much later.
                //
                // Unless the window was clamped, which explains the shortfall completely: the
                // window does not reach the resume instant at all, so of course nothing replayed at
                // it. The clamp is reported with the right cause by `warn_if_clamped`.
                //
                // `clamped` is only set once the boundary frame has been seen, so the suppression
                // holds for the measured order -- boundary first -- and not for its reverse. Should
                // a replayed tick beat its own boundary frame, this warning fires before the clamp
                // is known and BOTH lines appear, of which only the clamp's is true. That is
                // tolerated rather than closed: the alternative is to defer this warning until
                // something proves no clamp is coming, and the only frame that could prove it is
                // `replay_complete`, whose delivery and ordering are unmeasured -- so deferring
                // would risk losing a true report entirely to avoid an extra false one. A redundant
                // line is the cheaper failure.
                if undelivered > 0 && !pending.clamped {
                    warn!(
                        %exchange,
                        subscription = %subscription_id,
                        instant = %instant,
                        undelivered,
                        "London Strategic Edge replay held fewer ticks at the resume instant than \
                         were delivered from it; the provider's history changed under the resume",
                    );
                }

                false
            }
            // Older than the watermark. `start` is inclusive, so the provider should never send
            // this; emitting is the safe answer, since dropping would discard a real event on the
            // strength of an assumption that has already been contradicted.
            Ordering::Less => {
                // The mirror image of the shortfall above, and the same load-bearing assumption
                // contradicted in the other direction -- most plausibly a `start` filter flooring
                // to a coarser resolution than the microseconds it was given. Its consequence is
                // worse, because everything between the served instant and the watermark is a
                // duplicate delivered into consumer state, so it must not be the quiet one. Fired
                // once per subscription: the condition holds for every replayed tick below the
                // watermark, and on a busy symbol that is tens of thousands of them.
                //
                // The entry now outlives the replay window -- it is kept so a late boundary frame
                // can still be recognised as a clamp -- so this arm is also reachable long after
                // the replay ended, for a live tick stamped before the resume instant. That is a
                // different cause with the same consequence, and the wording names neither rather
                // than asserting the wrong one.
                if !pending.warned_older {
                    pending.warned_older = true;

                    warn!(
                        %exchange,
                        subscription = %subscription_id,
                        instant = %time_exchange,
                        watermark = %pending.time_exchange,
                        "London Strategic Edge delivered a tick older than the resume instant, \
                         which an inclusive `start` should make impossible during the replay and \
                         a monotonic feed should make impossible after it; it is emitted rather \
                         than dropped, so expect duplicates back to the instant shown",
                    );
                }

                false
            }
        }
    }

    /// Warn when the provider opened a replay window later than the one asked for.
    ///
    /// # ⚠️ Silent clamping is the failure this exists to surface
    /// The provider retains 24 hours. A `start` older than that is **moved forward and served with
    /// no error** — a window requested 48 hours back was answered with one beginning exactly 24
    /// hours back. The `from` of the [`ReplayStarted`](LseMessage::ReplayStarted) frame is the only
    /// signal that it happened.
    ///
    /// The watermark is what went out as `start`, so it *is* the requested instant and no separate
    /// record of the request is needed. A window opened exactly at it is not a clamp: `start` is
    /// inclusive, so that is precisely the window asked for.
    ///
    /// A detected clamp is also recorded against the subscription, because it explains away the
    /// shortfall [`should_drop`](Self::should_drop) would otherwise report with a different and
    /// wrong cause.
    fn warn_if_clamped(&mut self, subscription_id: &SubscriptionId, from: DateTime<Utc>) {
        let Some(requested) = self.clamped_from(subscription_id, from) else {
            return;
        };

        if let Some(pending) = self.pending.get_mut(subscription_id) {
            pending.clamped = true;
        }

        warn!(
            exchange = %self.exchange,
            subscription = %subscription_id,
            requested = %requested,
            serving_from = %from,
            "London Strategic Edge clamped the requested replay window; events between the two \
             instants are beyond the provider's retention and are permanently lost",
        );
    }

    /// The instant a window was requested from, if the provider opened it later than that.
    ///
    /// `None` for a subscription this stream holds no watermark for — one that asked for no window.
    /// An entry is never removed once made, so a boundary frame that arrives *after* the replay has
    /// already passed the watermark is still answered correctly: the provider is measured to
    /// announce the window first, on both a crypto and an FX symbol, but under a clamp every
    /// replayed tick sits past the watermark by definition, so a single reordered frame would
    /// otherwise be enough to lose the only evidence the window was cut short.
    fn clamped_from(
        &self,
        subscription_id: &SubscriptionId,
        from: DateTime<Utc>,
    ) -> Option<DateTime<Utc>> {
        let requested = self.pending.get(subscription_id)?.time_exchange;

        // A window opened *at* the requested instant is not a clamp: `start` is inclusive, so that
        // is precisely the window that was asked for, and warning on it would make this fire on
        // every ordinary resume.
        (from > requested).then_some(requested)
    }

    /// Advance the shared watermark for an event this stream emitted.
    fn record(&self, subscription_id: SubscriptionId, time_exchange: DateTime<Utc>) {
        self.state.record(
            &LseResumeKey::new(self.exchange, subscription_id, self.kind),
            time_exchange,
        );
    }
}

/// Bound required by [`StatelessTransformer`]'s decode path, restated here so this module's own
/// bounds stay legible.
const _: fn() = || {
    fn assert_identifiable<T: Identifier<Option<SubscriptionId>> + for<'de> Deserialize<'de>>() {}
    assert_identifiable::<LseMessage>();
};

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use crate::{exchange::lse::LseCrypto, subscription::trade::PublicTrades};
    use rustrade_integration::subscription::SubscriptionId;

    type Subject = LseTransformer<LseCrypto, u8, PublicTrades>;

    /// The kind these tests serve, and the partition their watermarks are filed under.
    const KIND: &str = "public_trades";

    fn id() -> SubscriptionId {
        super::super::resume::subscription_id("BTC/USD")
    }

    fn key() -> LseResumeKey {
        LseResumeKey::new(ExchangeId::LseCrypto, id(), KIND)
    }

    /// A live tick, which carries no `replay` key at all.
    fn tick(ts: &str, price: &str) -> LseMessage {
        serde_json::from_str(&format!(
            r#"{{"type":"tick","symbol":"BTC/USD","ts":"{ts}","price":{price},
                "bid":{price},"ask":42001.0,"volume":0.00155}}"#
        ))
        .unwrap()
    }

    /// A tick served from the provider's replay buffer, which the provider stamps.
    fn replayed_tick(ts: &str, price: &str) -> LseMessage {
        serde_json::from_str(&format!(
            r#"{{"type":"tick","symbol":"BTC/USD","ts":"{ts}","replay":true,"price":{price},
                "bid":{price},"ask":42001.0,"volume":0.00155}}"#
        ))
        .unwrap()
    }

    async fn subject(resume: Option<Arc<LseResumeState>>) -> Subject {
        let map = Map([(id(), 1_u8)]
            .into_iter()
            .collect::<FnvHashMap<SubscriptionId, u8>>());
        let (tx, _rx) = mpsc::unbounded_channel();

        Subject::new(map, &[], tx, resume.map(|state| (state, KIND)))
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn without_resume_every_tick_is_emitted() {
        let mut subject = subject(None).await;

        assert_eq!(
            subject.transform(tick("2026-01-02T09:00:00Z", "1.0")).len(),
            1
        );
        assert_eq!(
            subject.transform(tick("2026-01-02T09:00:00Z", "1.0")).len(),
            1
        );
    }

    #[tokio::test]
    async fn with_resume_the_watermark_advances_on_emitted_ticks() {
        let state = Arc::new(LseResumeState::new());
        let mut subject = subject(Some(Arc::clone(&state))).await;

        subject.transform(tick("2026-01-02T09:00:00.000001Z", "1.0"));
        subject.transform(tick("2026-01-02T09:00:00.000001Z", "2.0"));

        let watermark = state.watermark(&key()).unwrap();
        assert_eq!(
            watermark.time_exchange,
            "2026-01-02T09:00:00.000001Z"
                .parse::<DateTime<Utc>>()
                .unwrap()
        );
        assert_eq!(watermark.count_at_time, 2);
    }

    /// The core of the mechanism: a reconnect replays the whole of the watermark's instant because
    /// `start` is inclusive, and exactly the already-delivered prefix is skipped.
    #[tokio::test]
    async fn a_replay_skips_exactly_the_ticks_already_delivered_at_the_resume_instant() {
        let state = Arc::new(LseResumeState::new());

        // First connection delivers three ticks sharing one instant.
        let mut first = subject(Some(Arc::clone(&state))).await;
        for price in ["1.0", "2.0", "3.0"] {
            first.transform(tick("2026-01-02T09:00:00.000001Z", price));
        }
        assert_eq!(state.watermark(&key()).unwrap().count_at_time, 3);

        // Reconnect: the provider replays all three, then a fourth that is genuinely new.
        let mut second = subject(Some(Arc::clone(&state))).await;
        for price in ["1.0", "2.0", "3.0"] {
            assert!(
                second
                    .transform(replayed_tick("2026-01-02T09:00:00.000001Z", price))
                    .is_empty(),
                "an already-delivered tick must not be emitted twice"
            );
        }

        assert_eq!(
            second
                .transform(tick("2026-01-02T09:00:00.000001Z", "4.0"))
                .len(),
            1,
            "a fourth tick at the same instant is new and must be emitted"
        );
        assert_eq!(state.watermark(&key()).unwrap().count_at_time, 4);
    }

    /// The gap half of the contract: everything after the resume instant is new, always.
    #[tokio::test]
    async fn a_replay_emits_every_tick_past_the_resume_instant() {
        let state = Arc::new(LseResumeState::new());

        let mut first = subject(Some(Arc::clone(&state))).await;
        first.transform(tick("2026-01-02T09:00:00.000001Z", "1.0"));

        let mut second = subject(Some(Arc::clone(&state))).await;
        assert!(
            second
                .transform(replayed_tick("2026-01-02T09:00:00.000001Z", "1.0"))
                .is_empty()
        );
        assert_eq!(
            second
                .transform(tick("2026-01-02T09:00:00.000002Z", "2.0"))
                .len(),
            1,
            "one microsecond later is past the resume instant and must be emitted"
        );
    }

    /// The drop window closes for good once the instant is passed, so a later tick that happens to
    /// carry the resume instant again cannot be swallowed.
    #[tokio::test]
    async fn the_drop_window_does_not_reopen_after_the_resume_instant_is_passed() {
        let state = Arc::new(LseResumeState::new());

        let mut first = subject(Some(Arc::clone(&state))).await;
        first.transform(tick("2026-01-02T09:00:00.000001Z", "1.0"));
        first.transform(tick("2026-01-02T09:00:00.000001Z", "2.0"));

        let mut second = subject(Some(Arc::clone(&state))).await;
        // Replay is short by one, then moves past the instant -- the window closes here.
        assert!(
            second
                .transform(replayed_tick("2026-01-02T09:00:00.000001Z", "1.0"))
                .is_empty()
        );
        assert_eq!(
            second
                .transform(tick("2026-01-02T09:00:00.000002Z", "2.0"))
                .len(),
            1
        );
        assert_eq!(
            second
                .transform(tick("2026-01-02T09:00:00.000001Z", "3.0"))
                .len(),
            1,
            "the window is closed; a tick at the old instant must be emitted, not dropped"
        );
    }

    /// A subscription with no history resumes as a fresh subscription would.
    #[tokio::test]
    async fn a_subscription_with_no_watermark_drops_nothing() {
        let state = Arc::new(LseResumeState::new());
        let mut subject = subject(Some(state)).await;

        assert_eq!(
            subject.transform(tick("2026-01-02T09:00:00Z", "1.0")).len(),
            1
        );
    }

    #[tokio::test]
    async fn a_control_frame_is_neither_emitted_nor_recorded() {
        let state = Arc::new(LseResumeState::new());
        let mut subject = subject(Some(Arc::clone(&state))).await;

        let frame: LseMessage =
            serde_json::from_str(r#"{"type":"replay_complete","symbol":"BTC/USD","rows":41}"#)
                .unwrap();

        assert!(subject.transform(frame).is_empty());
        assert_eq!(state.watermark(&key()), None);
    }

    /// The no-dedupe lock, restated at the transformer: identical repeats are genuine distinct
    /// prints and only the resume prefix may ever be skipped.
    #[tokio::test]
    async fn identical_live_ticks_are_never_deduplicated() {
        let state = Arc::new(LseResumeState::new());
        let mut subject = subject(Some(state)).await;

        let first = subject.transform(tick("2026-01-02T09:00:00.000001Z", "1.0"));
        let second = subject.transform(tick("2026-01-02T09:00:00.000001Z", "1.0"));

        assert_eq!(first.len(), 1);
        assert_eq!(second.len(), 1, "an identical repeat is a distinct print");
    }

    /// An inclusive `start` says this cannot happen, so it means the assumption behind positional
    /// skipping has been contradicted — most plausibly a `start` filter flooring to a coarser
    /// resolution. Emitting is the safe answer: dropping would discard a real event on the strength
    /// of an assumption already known to be wrong. A `warn!` carries the signal.
    #[tokio::test]
    async fn a_replayed_tick_older_than_the_resume_instant_is_emitted_rather_than_dropped() {
        let state = Arc::new(LseResumeState::new());

        let mut first = subject(Some(Arc::clone(&state))).await;
        first.transform(tick("2026-01-02T09:00:00.000002Z", "1.0"));

        let mut second = subject(Some(Arc::clone(&state))).await;
        assert_eq!(
            second
                .transform(replayed_tick("2026-01-02T09:00:00.000001Z", "0.5"))
                .len(),
            1,
            "a tick below the watermark must be emitted, not silently swallowed"
        );

        // The window itself is untouched by it: the tick at the watermark is still skipped.
        assert!(
            second
                .transform(replayed_tick("2026-01-02T09:00:00.000002Z", "1.0"))
                .is_empty()
        );
    }

    /// The skip stays positional rather than conditioning on `replay`, because the flag is absent
    /// on live ticks: a provider that stopped stamping replayed ones would otherwise turn every
    /// reconnect into a duplicate flood. The cost is this case, which is reported by a `warn!`
    /// rather than changed.
    #[tokio::test]
    async fn an_unstamped_tick_inside_the_resume_window_is_still_skipped() {
        let state = Arc::new(LseResumeState::new());

        let mut first = subject(Some(Arc::clone(&state))).await;
        first.transform(tick("2026-01-02T09:00:00.000001Z", "1.0"));

        let mut second = subject(Some(Arc::clone(&state))).await;
        assert!(
            second
                .transform(tick("2026-01-02T09:00:00.000001Z", "1.0"))
                .is_empty()
        );
    }

    /// A resumed subject whose `pending` holds a watermark at `instant`, as a reconnect's would.
    async fn resumed_at(state: &Arc<LseResumeState>, instant: &str) -> Subject {
        let mut first = subject(Some(Arc::clone(state))).await;
        first.transform(tick(instant, "1.0"));

        subject(Some(Arc::clone(state))).await
    }

    fn at(spelling: &str) -> DateTime<Utc> {
        spelling.parse::<DateTime<Utc>>().unwrap()
    }

    /// The boundary frame announcing a window opening at `from`.
    fn boundary(from: &str) -> LseMessage {
        serde_json::from_str(&format!(
            r#"{{"type":"replay_started","symbol":"BTC/USD","from":"{from}"}}"#
        ))
        .unwrap()
    }

    /// This subject's per-connection bookkeeping for the one subscription these tests use.
    fn pending_for(subject: &Subject) -> PendingDrop {
        *subject
            .resume
            .as_ref()
            .unwrap()
            .pending
            .get(&id())
            .unwrap_or_else(|| panic!("the resumed subject must hold an entry for {}", id()))
    }

    /// The provider retains 24 hours and moves an older `start` forward with no error, so the
    /// boundary frame's `from` is the only evidence the requested window was not the served one.
    #[tokio::test]
    async fn a_replay_window_opened_later_than_requested_is_reported() {
        let state = Arc::new(LseResumeState::new());
        let subject = resumed_at(&state, "2026-08-12T10:00:00Z").await;

        assert_eq!(
            subject
                .resume
                .as_ref()
                .unwrap()
                .clamped_from(&id(), at("2026-08-13T10:00:00Z")),
            Some(at("2026-08-12T10:00:00Z")),
        );
    }

    /// `start` is inclusive, so a window opened exactly where it was asked for is the window that
    /// was asked for — reporting it would make the check noise on every ordinary resume.
    #[tokio::test]
    async fn a_replay_window_opened_where_requested_is_not_a_clamp() {
        let state = Arc::new(LseResumeState::new());
        let subject = resumed_at(&state, "2026-08-14T10:16:55.161234Z").await;

        assert_eq!(
            subject
                .resume
                .as_ref()
                .unwrap()
                .clamped_from(&id(), at("2026-08-14T10:16:55.161234Z")),
            None,
        );
    }

    /// A subscription that asked for no window cannot have had one clamped.
    #[tokio::test]
    async fn a_replay_boundary_for_a_subscription_with_no_watermark_is_ignored() {
        let state = Arc::new(LseResumeState::new());
        let subject = subject(Some(state)).await;

        assert_eq!(
            subject
                .resume
                .as_ref()
                .unwrap()
                .clamped_from(&id(), at("2026-08-13T10:00:00Z")),
            None,
        );
    }

    /// A clamp explains the shortfall at the resume instant completely — the window does not reach
    /// it — so the shortfall must not *also* be reported as the provider's history changing under
    /// the subscription. Two reports with different causes for one event is worse than one, because
    /// only one of them is true.
    #[tokio::test]
    async fn a_clamped_window_is_not_also_reported_as_a_changed_history() {
        let state = Arc::new(LseResumeState::new());
        let mut subject = resumed_at(&state, "2026-08-12T10:00:00Z").await;

        // The provider answers with a window opening a day later than asked for.
        subject.transform(boundary("2026-08-13T10:00:00Z"));

        let pending = pending_for(&subject);
        assert!(
            pending.clamped,
            "the clamp must be recorded, not only logged"
        );
        assert_eq!(
            pending.remaining, 1,
            "the drop window is still open until a tick passes the resume instant",
        );

        // Every replayed tick under a clamp sits past the watermark by definition, so the first one
        // closes the window with the count unspent. That shortfall has one cause and it is the
        // clamp.
        assert_eq!(
            subject
                .transform(replayed_tick("2026-08-13T10:00:00.000001Z", "1.0"))
                .len(),
            1,
        );

        let pending = pending_for(&subject);
        assert_eq!(pending.remaining, 0, "the drop window must be closed");
        assert!(
            pending.clamped,
            "the clamp flag is what suppresses the wrong-cause report and must survive the frame \
             that closes the window",
        );
    }

    /// The boundary frame is measured to arrive first, but a single reordered frame must not lose
    /// the only evidence the window was cut short. The entry is therefore kept rather than removed
    /// once the replay passes the watermark, so the clamp is still detected when the first replayed
    /// tick beats its own boundary frame.
    #[tokio::test]
    async fn a_clamp_is_still_detected_when_a_replayed_tick_beats_the_boundary_frame() {
        let state = Arc::new(LseResumeState::new());
        let mut subject = resumed_at(&state, "2026-08-12T10:00:00Z").await;

        // The tick arrives first and closes the drop window, as every tick under a clamp does.
        assert_eq!(
            subject
                .transform(replayed_tick("2026-08-13T10:00:00.000001Z", "1.0"))
                .len(),
            1,
        );

        // The boundary frame follows. The requested instant must still be readable.
        assert_eq!(
            subject
                .resume
                .as_ref()
                .unwrap()
                .clamped_from(&id(), at("2026-08-13T10:00:00Z")),
            Some(at("2026-08-12T10:00:00Z")),
            "the requested instant is the only record of what was asked for and must outlive the \
             drop window",
        );
    }

    /// A rejection raised after the handshake stopped reading carries no instrument and no instant.
    /// It is consumed for its `warn!` and must not reach a decoder or the watermark.
    #[tokio::test]
    async fn a_rejection_yields_no_market_event_and_does_not_advance_the_watermark() {
        let state = Arc::new(LseResumeState::new());
        let mut subject = subject(Some(Arc::clone(&state))).await;

        let rejection: LseMessage = serde_json::from_str(
            r#"{"type":"error","code":"INVALID_START","message":"Invalid start"}"#,
        )
        .unwrap();

        assert!(subject.transform(rejection).is_empty());
        assert_eq!(state.watermark(&key()), None);
    }

    /// The frame is the only evidence a requested window was clamped, and it reaches the stream
    /// rather than the handshake. It must yield no market event of its own.
    #[tokio::test]
    async fn a_replay_boundary_is_consumed_without_emitting_an_event() {
        let state = Arc::new(LseResumeState::new());
        let mut subject = subject(Some(Arc::clone(&state))).await;

        let boundary: LseMessage = serde_json::from_str(
            r#"{"type":"replay_started","symbol":"BTC/USD","from":"2026-01-02T09:00:00Z"}"#,
        )
        .unwrap();

        assert!(subject.transform(boundary).is_empty());
        assert_eq!(
            state.watermark(&key()),
            None,
            "a boundary frame must not advance the watermark"
        );
    }
}
