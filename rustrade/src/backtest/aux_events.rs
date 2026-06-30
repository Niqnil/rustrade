use crate::{EngineEvent, Timed};
use rustrade_data::event::DataKind;
use rustrade_instrument::{
    asset::AssetIndex, exchange::ExchangeIndex, instrument::InstrumentIndex,
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
/// zero cost.
///
/// # Caller obligations
/// - [`aux_events`](Self::aux_events) MUST yield events sorted ascending by [`Timed::time`]. The
///   merge with the market stream is a two-way merge that relies on both inputs already being
///   sorted; out-of-order aux events produce an out-of-order engine feed (and, in backtest, a
///   non-monotonic [`HistoricalClock`](crate::engine::clock::HistoricalClock)).
/// - For an [`EngineEvent::CorporateAction`](crate::EngineEvent::CorporateAction), the wrapping
///   [`Timed::time`] MUST equal the action's `effective_time`. These are two independent knobs: the
///   merge **positions** the event in the stream by `Timed::time`, while the handler advances the
///   [`HistoricalClock`](crate::engine::clock::HistoricalClock) to `effective_time` and stamps the
///   adjustment there. A mismatch is silently honored — the action would be *ordered* at one instant
///   but *take effect* at another — so the adjustment fires at the wrong simulated time. This is not
///   enforced (the handler cannot see the wrapping `Timed`); keep them equal.
///
/// # Limitation — `ContractExpiry` does not advance the backtest clock
/// [`EngineEvent::ContractExpiry`](crate::EngineEvent::ContractExpiry) carries no timestamp (unlike
/// [`EngineEvent::CorporateAction`](crate::EngineEvent::CorporateAction), which carries
/// `effective_time`). An injected expiry is therefore **ordered** correctly within the merged
/// stream but does **not** advance the
/// [`HistoricalClock`](crate::engine::clock::HistoricalClock): its synthetic settlement fill is
/// stamped at the prior market tick, not the expiry instant. `CorporateAction` is unaffected (it
/// advances the clock to its `effective_time`). If exact expiry-instant stamping matters, drive a
/// market event at the expiry time alongside the injected `ContractExpiry`.
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
/// backtest that injects no corporate actions or expiries pays nothing — the two-way merge sees an
/// empty aux side and reduces to the market stream unchanged.
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
    use crate::shutdown::Shutdown;
    use chrono::DateTime;

    fn at(secs: i64) -> Timed<EngineEvent> {
        Timed::new(
            EngineEvent::Shutdown(Shutdown),
            DateTime::from_timestamp(secs, 0).expect("valid timestamp"),
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
}
