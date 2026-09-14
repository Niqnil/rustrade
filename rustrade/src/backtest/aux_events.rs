use crate::{EngineEvent, Timed};
use rustrade_data::event::DataKind;
use rustrade_instrument::{
    asset::AssetIndex, exchange::ExchangeIndex, index::IndexedInstruments,
    instrument::InstrumentIndex,
};
use std::sync::Arc;

/// Source of auxiliary (non-market) [`EngineEvent`]s to interleave with the market-event replay
/// during a backtest, in simulated-time order.
///
/// The backtest harness pre-merges these events with the market stream into a single
/// time-ordered [`Stream`](futures::Stream) **before** the engine channel, so an injected
/// [`EngineEvent::CorporateAction`] (or [`EngineEvent::ContractExpiry`]) is processed at exactly
/// the right point in the timeline. Live trading injects the same events directly via
/// `System.feed_tx`; this trait is the backtest equivalent.
///
/// The default implementation, [`NoAuxEvents`], yields nothing — existing backtests opt out at
/// negligible per-event overhead.
///
/// # Caller obligations
/// - [`aux_events`](Self::aux_events) MUST yield events sorted ascending by [`Timed::time`]. The
///   merge with the market stream is a two-way merge that relies on both inputs already being
///   sorted; out-of-order aux events produce an out-of-order engine feed (and, in backtest, a
///   non-monotonic [`HistoricalClock`](crate::engine::clock::HistoricalClock)).
/// - For an [`EngineEvent::CorporateAction`], the wrapping [`Timed::time`] MUST equal the action's
///   `effective_time`. These are two independent knobs: the
///   merge **positions** the event in the stream by `Timed::time`, while the handler advances the
///   [`HistoricalClock`](crate::engine::clock::HistoricalClock) to `effective_time` and stamps the
///   adjustment there. A mismatch would *order* the action at one instant but make it *take effect*
///   at another. The backtest harness **enforces** this pre-merge via
///   `assert_aux_corporate_action_effective_times` (a hard panic naming the offending event) — the
///   handler itself cannot see the wrapping `Timed`, so keep the two equal to pass the check.
/// - For an [`EngineEvent::ContractExpiry`], the wrapping [`Timed::time`] MUST equal the target
///   instrument's own `expiry` (engine-side ground truth on
///   its `InstrumentKind`). Unlike `CorporateAction`, the instant is not on the payload — the merge
///   positions the event by `Timed::time`, while the handler advances the clock to the instrument's
///   `expiry` (see below). A mismatch would *order* the expiry at one instant but *settle* it at
///   another. Enforced pre-merge via `assert_aux_contract_expiry_times` (a hard panic naming the
///   offending event); non-expiring or unregistered targets are skipped.
///
/// # `ContractExpiry` clock advance
/// [`EngineEvent::ContractExpiry`] carries no timestamp on its payload (unlike
/// [`EngineEvent::CorporateAction`], which carries `effective_time`). Its effective instant is
/// instead engine-side ground truth: the
/// handler resolves the expiring instrument's `expiry` from its `InstrumentKind` and advances the
/// [`HistoricalClock`](crate::engine::clock::HistoricalClock) to it via
/// [`EngineClock::advance_to`](crate::engine::clock::EngineClock::advance_to), so the synthetic
/// settlement fill is stamped at the expiry instant rather than the prior market tick. As with
/// `CorporateAction`, position the injected expiry in the merged stream by its [`Timed::time`]; the
/// handler always settles at the instrument's own `expiry`, so keep the wrapping `Timed::time` equal
/// to that `expiry` (a mismatch would *order* the expiry at one instant but *settle* it at another).
/// The harness enforces this equality pre-merge — see the caller obligation above and
/// `assert_aux_contract_expiry_times`.
pub trait AuxEventSource<
    MarketKind = DataKind,
    ExchangeKey = ExchangeIndex,
    AssetKey = AssetIndex,
    InstrumentKey = InstrumentIndex,
>
{
    /// Return the auxiliary events to interleave, sorted ascending by [`Timed::time`].
    fn aux_events(
        &self,
    ) -> impl Iterator<Item = Timed<EngineEvent<MarketKind, ExchangeKey, AssetKey, InstrumentKey>>>;
}

/// Zero-size [`AuxEventSource`] that yields no events.
///
/// The default `AuxEvents` type for [`BacktestArgsConstant`](super::BacktestArgsConstant), so a
/// backtest that injects no corporate actions or expiries adds only negligible per-event overhead —
/// the two-way merge sees an empty aux side and forwards the market stream.
#[derive(Debug, Clone, Default)]
pub struct NoAuxEvents;

impl<MarketKind, ExchangeKey, AssetKey, InstrumentKey>
    AuxEventSource<MarketKind, ExchangeKey, AssetKey, InstrumentKey> for NoAuxEvents
{
    fn aux_events(
        &self,
    ) -> impl Iterator<Item = Timed<EngineEvent<MarketKind, ExchangeKey, AssetKey, InstrumentKey>>>
    {
        std::iter::empty()
    }
}

/// Panic if `events` are not sorted ascending by [`Timed::time`] (the [`AuxEventSource`] caller
/// obligation).
///
/// Shared by [`AuxEventsInMemory::new`] (early detection at construction, before any backtest runs)
/// and the backtest harness's pre-merge step (the load-bearing check for *any* [`AuxEventSource`]
/// impl — a custom source backed by a DB or file never goes through `AuxEventsInMemory`, so this is
/// the only guard on its events). A hard panic in all builds — not `debug_assert!` — because a
/// violation silently feeds the engine a non-monotonic timeline (and
/// [`HistoricalClock`](crate::engine::clock::HistoricalClock)) in release rather than failing. The
/// O(N) scan is negligible against the caller's own O(N log N) sort, and the message names the
/// offending pair so a failing custom source is debuggable without a rebuild.
pub(crate) fn assert_aux_events_sorted<MarketKind, ExchangeKey, AssetKey, InstrumentKey>(
    events: &[Timed<EngineEvent<MarketKind, ExchangeKey, AssetKey, InstrumentKey>>],
) {
    if let Some((i, w)) = events
        .windows(2)
        .enumerate()
        .find(|(_, w)| w[0].time > w[1].time)
    {
        panic!(
            "AuxEventSource events must be sorted ascending by Timed::time; \
             events[{i}].time={:?} > events[{}].time={:?}",
            w[0].time,
            i + 1,
            w[1].time,
        );
    }
}

/// Panic if any injected [`EngineEvent::CorporateAction`] carries an `effective_time` that differs
/// from its wrapping [`Timed::time`] (the second [`AuxEventSource`] caller obligation).
///
/// The merge **positions** a corporate action in the stream by `Timed::time`, while the handler
/// advances the [`HistoricalClock`](crate::engine::clock::HistoricalClock) to `effective_time` and
/// stamps the adjustment there. If the two disagree the action is *ordered* at one instant but
/// *takes effect* at another — a silent look-ahead / stale-stamp bug with no failure point in a
/// release build. This enforces them equal at the same pre-merge site as
/// [`assert_aux_events_sorted`] (a hard panic in all builds — the handler itself cannot see the
/// wrapping `Timed`, so the harness is the only place this can be checked). The O(N) scan is
/// negligible over the handful-sized aux set, and the message names the offending event so a failing
/// source is debuggable without a rebuild.
pub(crate) fn assert_aux_corporate_action_effective_times<
    MarketKind,
    ExchangeKey,
    AssetKey,
    InstrumentKey,
>(
    events: &[Timed<EngineEvent<MarketKind, ExchangeKey, AssetKey, InstrumentKey>>],
) {
    for (i, event) in events.iter().enumerate() {
        if let EngineEvent::CorporateAction {
            id, effective_time, ..
        } = &event.value
            && *effective_time != event.time
        {
            panic!(
                "AuxEventSource CorporateAction (id={id}) at events[{i}] must carry \
                 effective_time == Timed::time; effective_time={effective_time:?} != \
                 Timed::time={:?}",
                event.time,
            );
        }
    }
}

/// Panic if any injected [`EngineEvent::ContractExpiry`] carries a wrapping [`Timed::time`] that
/// differs from its target instrument's own `expiry` (the third [`AuxEventSource`] caller
/// obligation).
///
/// Unlike [`assert_aux_corporate_action_effective_times`], the effective instant is **not** on the
/// event payload — it is engine-side ground truth on the instrument's
/// [`InstrumentKind`](rustrade_instrument::instrument::kind::InstrumentKind). The merge **positions**
/// the expiry by `Timed::time`, while the handler advances the
/// [`HistoricalClock`](crate::engine::clock::HistoricalClock) to the resolved `expiry` and stamps the
/// synthetic settlement fill there. If the two disagree the expiry is *ordered* at one instant but
/// *settled* at another — a silent look-ahead / stale-stamp bug with no failure point in a release
/// build. This resolves each target via [`IndexedInstruments::find_instrument`] and enforces the
/// equality at the same pre-merge site as [`assert_aux_events_sorted`] (a hard panic in all builds —
/// the handler itself cannot see the wrapping `Timed`, so the harness is the only place this can be
/// checked). Events whose target is non-expiring (`expiry() == None`) or not registered are skipped
/// — those are not this check's concern (the engine surfaces an unregistered target on its own). The
/// O(N) scan over the handful-sized aux set is negligible, and the message names the offending event
/// so a failing source is debuggable without a rebuild.
pub(crate) fn assert_aux_contract_expiry_times<MarketKind, AssetKey>(
    events: &[Timed<EngineEvent<MarketKind, ExchangeIndex, AssetKey, InstrumentIndex>>],
    instruments: &IndexedInstruments,
) {
    for (i, event) in events.iter().enumerate() {
        let EngineEvent::ContractExpiry(key) = &event.value else {
            continue;
        };
        // Skip unregistered targets — the engine handler surfaces those separately; this check owns
        // only the time-vs-expiry equality.
        let Ok(instrument) = instruments.find_instrument(*key) else {
            continue;
        };
        if let Some(expiry) = instrument.kind.expiry()
            && expiry != event.time
        {
            panic!(
                "AuxEventSource ContractExpiry ({key}) at events[{i}] must carry Timed::time == \
                 the instrument's expiry; expiry={expiry:?} != Timed::time={:?}",
                event.time,
            );
        }
    }
}

/// In-memory [`AuxEventSource`] backed by an [`Arc`]'d, pre-sorted `Vec`.
///
/// Mirrors [`MarketDataInMemory`](super::market_data::MarketDataInMemory): cloning is O(1) (an
/// `Arc` clone), so the same source can be shared across a concurrent
/// [`run_backtests`](super::run_backtests) sweep without re-allocating per backtest.
#[derive(Debug, Clone)]
pub struct AuxEventsInMemory<
    MarketKind = DataKind,
    ExchangeKey = ExchangeIndex,
    AssetKey = AssetIndex,
    InstrumentKey = InstrumentIndex,
> {
    events: Arc<Vec<Timed<EngineEvent<MarketKind, ExchangeKey, AssetKey, InstrumentKey>>>>,
}

impl<MarketKind, ExchangeKey, AssetKey, InstrumentKey>
    AuxEventsInMemory<MarketKind, ExchangeKey, AssetKey, InstrumentKey>
{
    /// Create a new in-memory aux source from a pre-sorted `Vec` of [`Timed`] events.
    ///
    /// # Panics
    /// Panics if `events` is not sorted ascending by [`Timed::time`] (the [`AuxEventSource`] caller
    /// obligation). This is a hard assert in all builds: out-of-order aux events would silently
    /// produce a non-monotonic [`HistoricalClock`](crate::engine::clock::HistoricalClock) and wrong
    /// simulation results in release. Observable failure > silent corruption.
    pub fn new(
        events: Arc<Vec<Timed<EngineEvent<MarketKind, ExchangeKey, AssetKey, InstrumentKey>>>>,
    ) -> Self {
        // Enforce the caller's ascending-`Timed::time` obligation at construction, shared with the
        // backtest harness's pre-merge check. See [`assert_aux_events_sorted`].
        assert_aux_events_sorted(&events);
        Self { events }
    }
}

impl<MarketKind, ExchangeKey, AssetKey, InstrumentKey>
    AuxEventSource<MarketKind, ExchangeKey, AssetKey, InstrumentKey>
    for AuxEventsInMemory<MarketKind, ExchangeKey, AssetKey, InstrumentKey>
where
    MarketKind: Clone,
    ExchangeKey: Clone,
    AssetKey: Clone,
    InstrumentKey: Clone,
{
    fn aux_events(
        &self,
    ) -> impl Iterator<Item = Timed<EngineEvent<MarketKind, ExchangeKey, AssetKey, InstrumentKey>>>
    {
        self.events.iter().cloned()
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)] // Test code: panics on bad fixture input are acceptable
mod tests {
    use super::*;
    use crate::SplitRoundingPolicy;
    use crate::shutdown::Shutdown;
    use chrono::{DateTime, Utc};
    use rust_decimal::Decimal;
    use rustrade_instrument::corporate_action::{CorporateActionKind, SplitRatio};

    fn at(secs: i64) -> Timed<EngineEvent> {
        Timed::new(
            EngineEvent::Shutdown(Shutdown::Immediate),
            DateTime::from_timestamp(secs, 0).expect("valid timestamp"),
        )
    }

    /// Build a `CorporateAction` whose `effective_time` and wrapping `Timed::time` can differ, to
    /// exercise `assert_aux_corporate_action_effective_times`.
    fn corp_action(effective_secs: i64, time_secs: i64) -> Timed<EngineEvent> {
        Timed::new(
            EngineEvent::CorporateAction {
                id: "test-split".into(),
                instrument: InstrumentIndex::new(0),
                kind: CorporateActionKind::StockSplit {
                    ratio: SplitRatio::new(Decimal::from(2)).expect("valid ratio"),
                },
                policy: SplitRoundingPolicy::Fractional,
                effective_time: DateTime::from_timestamp(effective_secs, 0)
                    .expect("valid timestamp"),
            },
            DateTime::from_timestamp(time_secs, 0).expect("valid timestamp"),
        )
    }

    #[test]
    fn assert_aux_events_sorted_accepts_ascending_and_equal_timestamps() {
        // Ascending and *equal* adjacent timestamps both satisfy the contract (the check is `<=`).
        assert_aux_events_sorted::<DataKind, ExchangeIndex, AssetIndex, InstrumentIndex>(&[
            at(1_000),
            at(1_000),
            at(2_000),
        ]);
    }

    #[test]
    #[should_panic(expected = "events[1].time")]
    fn assert_aux_events_sorted_panics_on_unsorted_pair() {
        // The second pair (index 1) is out of order; the message must name that pair.
        assert_aux_events_sorted::<DataKind, ExchangeIndex, AssetIndex, InstrumentIndex>(&[
            at(1_000),
            at(3_000),
            at(2_000),
        ]);
    }

    #[test]
    #[should_panic(expected = "sorted ascending by Timed::time")]
    fn aux_events_in_memory_new_rejects_unsorted() {
        // `AuxEventsInMemory::new` must delegate to the shared check rather than accept unsorted input.
        let _ = AuxEventsInMemory::<DataKind>::new(Arc::new(vec![at(2_000), at(1_000)]));
    }

    #[test]
    fn assert_aux_corporate_action_effective_times_accepts_matching_times() {
        // effective_time == Timed::time satisfies the contract; non-CorporateAction events (the
        // trailing Shutdown) are ignored by the check. No panic.
        assert_aux_corporate_action_effective_times::<
            DataKind,
            ExchangeIndex,
            AssetIndex,
            InstrumentIndex,
        >(&[corp_action(1_000, 1_000), at(2_000)]);
    }

    #[test]
    #[should_panic(expected = "effective_time == Timed::time")]
    fn assert_aux_corporate_action_effective_times_panics_on_mismatch() {
        // A CorporateAction ordered at Timed::time=2000 but taking effect at 1000 is a silent
        // look-ahead; the hard assert (all builds) must panic naming the offending event.
        assert_aux_corporate_action_effective_times::<
            DataKind,
            ExchangeIndex,
            AssetIndex,
            InstrumentIndex,
        >(&[corp_action(1_000, 2_000)]);
    }

    fn time_at(secs: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(secs, 0).expect("valid timestamp")
    }

    /// A `ContractExpiry` event ordered at `time_secs`, targeting `InstrumentIndex(0)`.
    fn contract_expiry(time_secs: i64) -> Timed<EngineEvent> {
        Timed::new(
            EngineEvent::ContractExpiry(InstrumentIndex::new(0)),
            time_at(time_secs),
        )
    }

    /// `IndexedInstruments` holding one instrument at `InstrumentIndex(0)`, either a `Future`
    /// expiring at `expiry_secs` (`Some`) or a non-expiring `Spot` (`None`), so the expiry-vs-time
    /// check and its non-expiring skip branch can both be exercised.
    fn instruments_with(expiry_secs: Option<i64>) -> IndexedInstruments {
        use rustrade_instrument::{
            Underlying,
            exchange::ExchangeId,
            index::builder::IndexedInstrumentsBuilder,
            instrument::{
                Instrument, kind::InstrumentKind, kind::future::FutureContract,
                quote::InstrumentQuoteAsset,
            },
            test_utils::asset,
        };

        let kind = match expiry_secs {
            Some(secs) => InstrumentKind::Future(FutureContract {
                contract_size: Decimal::from(1),
                settlement_asset: asset("usdt"),
                expiry: time_at(secs),
            }),
            None => InstrumentKind::Spot,
        };
        IndexedInstrumentsBuilder::default()
            .add_instrument(Instrument::new(
                ExchangeId::BinanceSpot,
                "btc_usdt_future",
                "btc_usdt_future",
                Underlying::new(asset("btc"), asset("usdt")),
                InstrumentQuoteAsset::UnderlyingQuote,
                kind,
                None,
            ))
            .build()
    }

    #[test]
    fn assert_aux_contract_expiry_times_accepts_matching_time() {
        // Timed::time == the Future's own expiry satisfies the contract; the trailing non-expiry
        // event (Shutdown) is ignored. No panic.
        assert_aux_contract_expiry_times(
            &[contract_expiry(2_000), at(3_000)],
            &instruments_with(Some(2_000)),
        );
    }

    #[test]
    #[should_panic(expected = "Timed::time == the instrument's expiry")]
    fn assert_aux_contract_expiry_times_panics_on_mismatch() {
        // Ordered at Timed::time=1000 but the instrument expires at 2000 — a silent look-ahead the
        // hard assert (all builds) must catch, naming the offending event.
        assert_aux_contract_expiry_times(&[contract_expiry(1_000)], &instruments_with(Some(2_000)));
    }

    #[test]
    fn assert_aux_contract_expiry_times_skips_non_expiring_and_unregistered() {
        use rustrade_instrument::index::builder::IndexedInstrumentsBuilder;

        // A ContractExpiry targeting a non-expiring (Spot ⇒ expiry() == None) instrument is not
        // this check's concern — no expiry to compare against — so the time mismatch is ignored.
        assert_aux_contract_expiry_times(&[contract_expiry(1_000)], &instruments_with(None));
        // An empty registry ⇒ find_instrument errors ⇒ the target is skipped (the engine surfaces
        // an unregistered target on its own). No panic despite an arbitrary Timed::time.
        assert_aux_contract_expiry_times(
            &[contract_expiry(1_000)],
            &IndexedInstrumentsBuilder::default().build(),
        );
    }
}
