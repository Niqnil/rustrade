use crate::error::BarterError;
use chrono::{DateTime, Utc};
use futures::Stream;
use rustrade_data::streams::consumer::MarketStreamEvent;
use rustrade_instrument::instrument::InstrumentIndex;
use std::sync::Arc;

/// Interface that provides the backtest MarketStream and associated
/// [`HistoricalClock`](crate::engine::clock::HistoricalClock).
///
/// # Caller obligations
/// The backtest harness time-merges this market data with any
/// [`AuxEventSource`](super::aux_events::AuxEventSource) events into a single ordered engine feed,
/// so an implementation MUST uphold:
/// - [`stream`](Self::stream) yields events sorted ascending by `time_exchange`. The merge is a
///   two-way merge that assumes both inputs are already sorted; an unsorted stream produces an
///   out-of-order engine feed and a non-monotonic
///   [`HistoricalClock`](crate::engine::clock::HistoricalClock).
/// - [`time_first_event`](Self::time_first_event) and [`stream`](Self::stream) are *coherent*: they
///   describe the same dataset, and `time_first_event` equals the `time_exchange` of the first
///   [`MarketStreamEvent::Item`] that `stream` will yield. The harness calls them independently, so
///   an implementation backed by a single-pass cursor must not let one consume events the other
///   needs.
///
/// # Limitation
/// The harness currently `collect`s the entire `stream` into memory to perform the time-merge, so
/// lazy streaming is not preserved — sources too large to fit in memory are not yet supported.
pub trait BacktestMarketData {
    /// The type of market events provided by this data source.
    type Kind;

    /// Return the `DateTime<Utc>` of the first event in the market data `Stream`.
    ///
    /// Must be coherent with [`stream`](Self::stream) — see the trait-level caller obligations.
    fn time_first_event(&self) -> impl Future<Output = Result<DateTime<Utc>, BarterError>>;

    /// Return a `Stream` of `MarketStreamEvent`s, sorted ascending by `time_exchange`.
    ///
    /// See the trait-level caller obligations for the sort and coherence requirements.
    fn stream(
        &self,
    ) -> impl Future<
        Output = Result<
            impl Stream<Item = MarketStreamEvent<InstrumentIndex, Self::Kind>> + Send + 'static,
            BarterError,
        >,
    >;
}

/// In-memory market data.
///
/// Stores all market events in memory and generates a `Stream` of [`MarketStreamEvent`] by
/// lazy cloning the data as it's required.
#[derive(Debug, Clone)]
pub struct MarketDataInMemory<Kind> {
    time_first_event: DateTime<Utc>,
    events: Arc<Vec<MarketStreamEvent<InstrumentIndex, Kind>>>,
}

impl<Kind> BacktestMarketData for MarketDataInMemory<Kind>
where
    Kind: Clone + Sync + Send + 'static,
{
    type Kind = Kind;

    async fn time_first_event(&self) -> Result<DateTime<Utc>, BarterError> {
        Ok(self.time_first_event)
    }

    async fn stream(
        &self,
    ) -> Result<
        impl Stream<Item = MarketStreamEvent<InstrumentIndex, Self::Kind>> + Send + 'static,
        BarterError,
    > {
        let events = Arc::clone(&self.events);
        let lazy_clone_iter = (0..events.len()).map(move |index| events[index].clone());
        let stream = futures::stream::iter(lazy_clone_iter);
        Ok(stream)
    }
}

impl<Kind> MarketDataInMemory<Kind> {
    /// Create a new in-memory market data source from a pre-sorted vector of market events.
    ///
    /// # Panics
    /// - Panics if `events` contains no [`MarketStreamEvent::Item`] variant.
    /// - Panics if the [`Item`](MarketStreamEvent::Item) timestamps are not sorted ascending by
    ///   `time_exchange` (the [`BacktestMarketData`] caller obligation).
    #[allow(clippy::expect_used)] // Caller contract: events must contain at least one MarketStreamEvent::Item variant
    pub fn new(events: Arc<Vec<MarketStreamEvent<InstrumentIndex, Kind>>>) -> Self {
        let time_first_event = events
            .iter()
            .find_map(|event| match event {
                MarketStreamEvent::Item(event) => Some(event.time_exchange),
                _ => None,
            })
            .expect("cannot construct MarketDataInMemory using an empty Vec<MarketStreamEvent>");

        // Hard assert (not `debug_assert!`): event ordering is a caller-supplied external invariant
        // that the harness's time-merge with `AuxEventSource` events relies on; an unsorted stream
        // would silently produce a non-monotonic clock and wrong simulation results in release.
        // Mirrors `AuxEventsInMemory::new`. `Reconnecting` events carry no timestamp (the harness
        // carries the prior time forward), so only `Item` timestamps are checked.
        assert!(
            events
                .iter()
                .filter_map(|event| match event {
                    MarketStreamEvent::Item(event) => Some(event.time_exchange),
                    _ => None,
                })
                .is_sorted(),
            "MarketDataInMemory events must be sorted ascending by MarketEvent::time_exchange"
        );

        Self {
            time_first_event,
            events,
        }
    }
}
