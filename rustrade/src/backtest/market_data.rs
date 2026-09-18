use crate::error::BarterError;
use chrono::{DateTime, Utc};
use futures::{Stream, StreamExt};
use rustrade_data::streams::consumer::MarketStreamEvent;
use rustrade_instrument::instrument::InstrumentIndex;
use std::{marker::PhantomData, sync::Arc};

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
/// # Failure model
/// Each item is a `Result`, because a source that reads incrementally — a file, a decoder, a
/// paginated HTTP fetch — can fail **after** the stream has been successfully opened. An `Err`
/// **aborts the backtest**: [`backtest`](super::backtest) stops feeding the engine, shuts the
/// system down, and returns that error instead of a [`BacktestSummary`](super::BacktestSummary).
/// Truncating the stream instead would return statistics computed over however much of the dataset
/// happened to be read, with nothing distinguishing that from a complete run.
///
/// A source that cannot fail mid-stream (e.g. [`MarketDataInMemory`]) simply yields `Ok`.
///
/// # Memory model, and the obligation it puts on the implementation
/// The time-merge consumes this stream **lazily** — polled on demand, never collected — and buffers
/// at most one item per input. That is necessary for a dataset larger than memory, but on its own it
/// is **not sufficient**, and the difference matters:
///
/// The harness forwards this stream into the engine's feed channel, which is **unbounded**. Whatever
/// the engine has not yet processed sits in that channel. So peak memory tracks how far the source
/// runs ahead of the engine, not how lazily it is polled — and a source that never returns
/// `Poll::Pending` never lets the engine run at all until it is exhausted. A blocking iterator
/// wrapped in [`futures::stream::iter`] is exactly that shape: the entire artifact is decoded and
/// enqueued before the first event is handled, which is O(dataset), not O(1).
///
/// **An implementation is therefore responsible for bounding its own read-ahead** — for yielding
/// `Pending` rather than running to exhaustion. A network-paced source (a paginated fetch) does so
/// incidentally, because awaiting a response yields. A local decoder does not, and must be bridged:
/// [`stream_blocking_iter`](rustrade_data::streams::blocking::stream_blocking_iter) moves the decode
/// to a blocking thread and parks it whenever it gets a fixed number of events ahead of *that
/// channel's reader*.
///
/// That reader is the time-merge, **not the engine**, so this is not an end-to-end memory bound. The
/// harness forwards the merged stream into the unbounded feed channel with a synchronous send that
/// never waits on capacity, and no implementation of this trait can change that — the feed channel is
/// harness-side. What the bridge buys is real but narrower: the artifact is no longer decoded in one
/// uninterruptible burst before the engine runs, so decoding overlaps processing instead of preceding
/// it. Peak memory still tracks how far the engine lags the source. Bounding it outright needs the
/// feed channel itself to apply back-pressure, which it does not today — see
/// <https://github.com/Niqnil/rustrade/issues/220>.
pub trait BacktestMarketData {
    /// The type of market events provided by this data source.
    type Kind;

    /// Return the `DateTime<Utc>` of the first event in the market data `Stream`.
    ///
    /// Must be coherent with [`stream`](Self::stream) — see the trait-level caller obligations.
    fn time_first_event(&self) -> impl Future<Output = Result<DateTime<Utc>, BarterError>>;

    /// Return a `Stream` of `MarketStreamEvent`s, sorted ascending by `time_exchange`.
    ///
    /// See the trait-level caller obligations for the sort and coherence requirements, and the
    /// failure model for the meaning of an `Err` item.
    fn stream(
        &self,
    ) -> impl Future<
        Output = Result<
            impl Stream<Item = Result<MarketStreamEvent<InstrumentIndex, Self::Kind>, BarterError>>
            + Send
            + 'static,
            BarterError,
        >,
    >;
}

/// In-memory market data.
///
/// Stores all market events in memory and generates a `Stream` of [`MarketStreamEvent`] by
/// lazy cloning the data as it's required.
///
/// Cannot fail mid-stream, so every item is `Ok`.
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
        impl Stream<Item = Result<MarketStreamEvent<InstrumentIndex, Self::Kind>, BarterError>>
        + Send
        + 'static,
        BarterError,
    > {
        let events = Arc::clone(&self.events);
        let lazy_clone_iter = (0..events.len()).map(move |index| Ok(events[index].clone()));
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

/// Lazily streamed market data, produced on demand by a caller-supplied factory.
///
/// The counterpart to [`MarketDataInMemory`] for datasets that cannot be resident: a multi-gigabyte
/// Parquet export, a compressed tick archive, or a paginated provider fetch. The harness never
/// collects the stream, so nothing here scales with the dataset — but see
/// [the trait's memory model](BacktestMarketData#memory-model-and-the-obligation-it-puts-on-the-implementation):
/// bounding read-ahead is the **factory's** job, and a blocking decoder handed straight to
/// [`futures::stream::iter`] will still park the whole artifact in the engine's feed channel. Bridge
/// it with [`stream_blocking_iter`](rustrade_data::streams::blocking::stream_blocking_iter).
///
/// # The factory
/// `factory` is called **once per [`stream`](BacktestMarketData::stream) invocation** and returns a
/// fresh, independent stream each time — it is not a cursor being handed out repeatedly. It is
/// where all source-specific concern lives: opening files, decoding, resolving each source's
/// [`InstrumentIndex`], and merging several per-instrument sources into one time-ordered stream.
/// This crate therefore stays ignorant of file formats, compression and providers; see
/// `rustrade-data` for the k-way merge helper and the per-provider adapters that build such a
/// factory.
///
/// # Cost model — read this before using it with [`run_backtests`](super::run_backtests)
/// Construction calls the factory once (to resolve `time_first_event`), and **every backtest calls
/// it again**. [`run_backtests`](super::run_backtests) shares one `BacktestArgsConstant` across all
/// its runs, so N strategy configurations cost **1 + N** full source reads — where
/// [`MarketDataInMemory`] would cost one `Arc` clone each.
///
/// That is the deliberate price of streaming rather than holding the dataset, and it is the right
/// trade for a local file. It is
/// usually the **wrong** trade against a metered or rate-limited network source, where 1 + N reads
/// multiply both latency and quota consumption: fetch once to a local file (or, for a small enough
/// slice, into [`MarketDataInMemory`]) and stream from that instead.
///
/// # Coherence
/// The first event's timestamp is resolved **once, in the constructor**, and cached — so
/// [`time_first_event`](BacktestMarketData::time_first_event) never consumes the cursor that
/// [`stream`](BacktestMarketData::stream) needs. This costs one extra source open plus a first
/// record decode at construction.
///
/// # Sort obligation
/// Unlike [`MarketDataInMemory`], which can (and does) assert sortedness up front, a lazy source
/// cannot be checked without reading it. Upholding the ascending-`time_exchange` obligation is the
/// factory's responsibility.
///
/// A violation is nonetheless **observable rather than silent**: the harness's merge checks each
/// event against the last as it passes, in release as well as debug, and a backwards step aborts
/// the run with a [`BarterError::BacktestMarketData`] instead of producing statistics over a
/// non-monotonic clock. That is a backstop, not a licence to hand this an unsorted source: it
/// reports the first violation and stops, so it tells the factory it is wrong rather than repairing
/// anything.
#[derive(Debug, Clone)]
pub struct MarketDataStreamed<Factory, Kind> {
    time_first_event: DateTime<Utc>,
    factory: Factory,
    // `fn() -> Kind` rather than `Kind`, so the auto-trait implementations of this struct depend
    // only on `Factory`: the event type is a marker here, never held.
    phantom: PhantomData<fn() -> Kind>,
}

impl<Factory, Fut, St, Kind> MarketDataStreamed<Factory, Kind>
where
    Factory: Fn() -> Fut,
    Fut: Future<Output = Result<St, BarterError>>,
    St: Stream<Item = Result<MarketStreamEvent<InstrumentIndex, Kind>, BarterError>>
        + Send
        + 'static,
{
    /// Create a streaming market data source, resolving and caching the first event's timestamp.
    ///
    /// Opens the source once via `factory` and reads forward to the first
    /// [`MarketStreamEvent::Item`], discarding any leading
    /// [`Reconnecting`](MarketStreamEvent::Reconnecting) events (which carry no timestamp).
    ///
    /// # Errors
    /// - [`BarterError::BacktestMarketData`] if the source yields no `Item` at all — an empty
    ///   dataset has no clock start, so there is no backtest to run.
    /// - Whatever the factory or the source itself reports, propagated unchanged.
    pub async fn new(factory: Factory) -> Result<Self, BarterError> {
        let stream = factory().await?;
        futures::pin_mut!(stream);

        let mut time_first_event = None;
        while let Some(event) = stream.next().await {
            if let MarketStreamEvent::Item(event) = event? {
                time_first_event = Some(event.time_exchange);
                break;
            }
        }

        let Some(time_first_event) = time_first_event else {
            return Err(BarterError::BacktestMarketData(
                "source yielded no MarketStreamEvent::Item, so the backtest clock has no start"
                    .to_string(),
            ));
        };

        Ok(Self {
            time_first_event,
            factory,
            phantom: PhantomData,
        })
    }
}

impl<Factory, Fut, St, Kind> BacktestMarketData for MarketDataStreamed<Factory, Kind>
where
    Factory: Fn() -> Fut,
    Fut: Future<Output = Result<St, BarterError>>,
    St: Stream<Item = Result<MarketStreamEvent<InstrumentIndex, Kind>, BarterError>>
        + Send
        + 'static,
{
    type Kind = Kind;

    async fn time_first_event(&self) -> Result<DateTime<Utc>, BarterError> {
        Ok(self.time_first_event)
    }

    async fn stream(
        &self,
    ) -> Result<
        impl Stream<Item = Result<MarketStreamEvent<InstrumentIndex, Self::Kind>, BarterError>>
        + Send
        + 'static,
        BarterError,
    > {
        (self.factory)().await
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panicking on a bad fixture is acceptable
mod tests {
    use super::*;
    use rust_decimal_macros::dec;
    use rustrade_data::{
        event::{DataKind, MarketEvent},
        subscription::trade::PublicTrade,
    };
    use rustrade_instrument::{Side, exchange::ExchangeId};
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn at(secs: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(secs, 0).unwrap()
    }

    fn item(secs: i64) -> MarketStreamEvent<InstrumentIndex, DataKind> {
        MarketStreamEvent::Item(MarketEvent {
            time_exchange: at(secs),
            time_received: at(secs),
            exchange: ExchangeId::BinanceSpot,
            instrument: InstrumentIndex::new(0),
            kind: DataKind::Trade(PublicTrade {
                id: "1".into(),
                price: dec!(100),
                amount: dec!(1),
                side: Some(Side::Buy),
            }),
        })
    }

    /// Build a `MarketDataStreamed` over a fixed script of items, counting factory invocations.
    fn streamed(
        script: Vec<Result<MarketStreamEvent<InstrumentIndex, DataKind>, BarterError>>,
        calls: Arc<AtomicUsize>,
    ) -> impl Fn() -> std::future::Ready<
        Result<
            futures::stream::Iter<
                std::vec::IntoIter<
                    Result<MarketStreamEvent<InstrumentIndex, DataKind>, BarterError>,
                >,
            >,
            BarterError,
        >,
    > {
        move || {
            calls.fetch_add(1, Ordering::SeqCst);
            std::future::ready(Ok(futures::stream::iter(script.clone())))
        }
    }

    #[tokio::test]
    async fn time_first_event_is_cached_and_does_not_consume_the_stream() {
        let calls = Arc::new(AtomicUsize::new(0));
        let data = MarketDataStreamed::new(streamed(
            vec![Ok(item(10)), Ok(item(20))],
            Arc::clone(&calls),
        ))
        .await
        .unwrap();

        assert_eq!(data.time_first_event().await.unwrap(), at(10));
        assert_eq!(calls.load(Ordering::SeqCst), 1, "construction reads once");

        // The full dataset is still available — the constructor's read did not consume it.
        let events = data.stream().await.unwrap().collect::<Vec<_>>().await;
        assert_eq!(events.len(), 2);
    }

    #[tokio::test]
    async fn stream_is_re_callable_and_yields_a_fresh_stream_each_time() {
        let calls = Arc::new(AtomicUsize::new(0));
        let data = MarketDataStreamed::new(streamed(
            vec![Ok(item(10)), Ok(item(20))],
            Arc::clone(&calls),
        ))
        .await
        .unwrap();

        let first = data.stream().await.unwrap().collect::<Vec<_>>().await;
        let second = data.stream().await.unwrap().collect::<Vec<_>>().await;

        assert_eq!(first.len(), 2);
        assert_eq!(second.len(), 2);
        // Construction plus one read per `stream()` call — the documented 1 + N cost model.
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }

    /// A leading `Reconnecting` carries no timestamp, so the first `Item` is what seeds the clock.
    #[tokio::test]
    async fn leading_reconnecting_events_are_skipped_when_resolving_the_first_event() {
        let calls = Arc::new(AtomicUsize::new(0));
        let data = MarketDataStreamed::new(streamed(
            vec![
                Ok(MarketStreamEvent::Reconnecting(ExchangeId::BinanceSpot)),
                Ok(item(30)),
            ],
            calls,
        ))
        .await
        .unwrap();

        assert_eq!(data.time_first_event().await.unwrap(), at(30));
    }

    #[tokio::test]
    async fn empty_source_is_an_error_not_an_empty_backtest() {
        let calls = Arc::new(AtomicUsize::new(0));
        let result = MarketDataStreamed::new(streamed(vec![], calls)).await;

        assert!(matches!(
            result.err(),
            Some(BarterError::BacktestMarketData(_))
        ));
    }

    #[tokio::test]
    async fn source_error_before_the_first_item_propagates_from_the_constructor() {
        let calls = Arc::new(AtomicUsize::new(0));
        let result = MarketDataStreamed::new(streamed(
            vec![Err(BarterError::BacktestMarketData("boom".to_string()))],
            calls,
        ))
        .await;

        assert!(matches!(
            result.err(),
            Some(BarterError::BacktestMarketData(message)) if message == "boom"
        ));
    }

    #[tokio::test]
    async fn factory_failure_propagates_from_the_constructor() {
        let factory = || {
            std::future::ready(Err::<
                futures::stream::Iter<
                    std::vec::IntoIter<
                        Result<MarketStreamEvent<InstrumentIndex, DataKind>, BarterError>,
                    >,
                >,
                _,
            >(BarterError::BacktestMarketData(
                "cannot open".to_string(),
            )))
        };

        assert!(matches!(
            MarketDataStreamed::new(factory).await.err(),
            Some(BarterError::BacktestMarketData(message)) if message == "cannot open"
        ));
    }
}
