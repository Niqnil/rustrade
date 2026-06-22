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
/// # Caller obligation
/// [`aux_events`](Self::aux_events) MUST yield events sorted ascending by [`Timed::time`]. The
/// merge with the market stream is a two-way merge that relies on both inputs already being
/// sorted; out-of-order aux events produce an out-of-order engine feed (and, in backtest, a
/// non-monotonic [`HistoricalClock`](crate::engine::clock::HistoricalClock)).
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
        // Hard assert (not `debug_assert!`): `events` ordering is a caller-supplied external
        // invariant, and a violation would silently corrupt the simulated timeline in release
        // rather than panic. The O(N) scan is negligible against the caller's own O(N log N) sort.
        assert!(
            events.windows(2).all(|w| w[0].time <= w[1].time),
            "AuxEventsInMemory events must be sorted ascending by Timed::time"
        );
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
