use crate::{event::MarketEvent, streams::consumer::MarketStreamEvent};
use chrono::{DateTime, Utc};
use futures::{
    Stream, StreamExt,
    stream::{FusedStream, Peekable},
};
use rustrade_instrument::exchange::ExchangeId;
use std::{
    pin::Pin,
    task::{Context, Poll},
};

/// Lazily merge N time-sorted market streams into one time-sorted stream.
///
/// Historical market data usually arrives one instrument (or one file) at a time, while consumers —
/// a backtest harness above all — need a single feed in simulated-time order. This performs that
/// k-way merge **lazily**: at most one buffered event per input is held, so memory is O(N) in the
/// number of inputs and O(1) in the size of the dataset. It is what lets a multi-instrument
/// backtest run over sources far larger than memory.
///
/// # Caller obligations
/// - **Every input MUST be sorted ascending by `time_exchange`.** A merge cannot recover ordering
///   its inputs do not have; an unsorted input yields an unsorted output, which in a backtest means
///   a non-monotonic clock and wrong results with no failure point. Violations trip a
///   `debug_assert!` naming the offending input — so tests fail loudly — but the check is compiled
///   out of release builds, where this stays an obligation rather than a guarantee.
/// - Inputs should already be tagged with the right instrument key. Resolving a source's key is the
///   producer's job, not the merge's.
///
/// # Ordering
/// The earliest `time_exchange` across all inputs wins. Ties are broken by **input order** — the
/// event from the earliest-listed stream is emitted first — which makes the merge deterministic for
/// a given input ordering, a property backtests depend on for reproducibility.
///
/// [`MarketStreamEvent::Reconnecting`] carries no timestamp. It is emitted as soon as it is seen,
/// ahead of any buffered `Item`, since it reports a transport condition at the point it occurred
/// rather than an event at a simulated instant.
///
/// # Pace
/// Nothing can be emitted until **every** input has either buffered an event or ended — an input
/// yet to produce might be about to yield something earlier, and an out-of-order output is
/// undetectable downstream. So the merge advances at the pace of its slowest input. Inputs still
/// make progress concurrently; it is only the emission that waits.
///
/// # ⚠️ Inputs must be able to progress independently
/// The corollary of the pace rule is that **an input which cannot make progress stalls the whole
/// merge, permanently and silently**. The inputs are also collected eagerly here, so every one of
/// them starts as soon as this function is called rather than when it is first polled.
///
/// The reachable case is
/// [`stream_blocking_iter`](super::blocking::stream_blocking_iter), which takes a blocking-pool
/// thread per input at construction and holds it for the whole decode. Tokio's pool defaults to
/// `max_blocking_threads: 512`, so beyond 512 inputs the surplus decoders never get a thread, stay
/// `Pending`, and hold the merge `Pending` — which stops the running decodes being drained, so none
/// of them releases a thread either. Nothing errors and nothing logs. See that function's
/// `# ⚠️ One blocking thread per call` section for the ways out.
///
/// # Errors
/// The item type is a `Result` so that fallible sources (file decoders, paginated fetches) compose
/// without collecting. An `Err` on any input is forwarded immediately, ahead of buffered events:
/// once a source has failed, the merged stream can no longer be complete, so the consumer should
/// see the failure before acting on more data. The merge does not end itself — that policy belongs
/// to the consumer.
pub fn merge_time_sorted<St, InstrumentKey, Kind, Error>(
    streams: impl IntoIterator<Item = St>,
) -> TimeSortedMerge<St, InstrumentKey, Kind, Error>
where
    St: Stream<Item = Result<MarketStreamEvent<InstrumentKey, Kind>, Error>>,
{
    let inputs = streams
        .into_iter()
        .map(|stream| Box::pin(stream.peekable()))
        .collect::<Vec<_>>();

    TimeSortedMerge {
        #[cfg(debug_assertions)]
        last_peeked: vec![None; inputs.len()],
        inputs,
    }
}

/// A lazy, time-ordered k-way merge of market streams. See [`merge_time_sorted`].
#[derive(Debug)]
pub struct TimeSortedMerge<St, InstrumentKey, Kind, Error>
where
    St: Stream<Item = Result<MarketStreamEvent<InstrumentKey, Kind>, Error>>,
{
    inputs: Vec<Pin<Box<Peekable<St>>>>,
    /// Last `time_exchange` peeked from each input, for the debug-only sortedness check.
    ///
    /// *Enforcing* the sort obligation would need buffering, which defeats the O(1) memory the
    /// merge exists for. *Detecting* a violation needs only the previous timestamp per input, which
    /// is O(N) in the input count and costs a release build nothing — the field does not exist
    /// there. So the obligation stays a caller obligation in production and becomes a loud failure
    /// in every test that runs this code.
    #[cfg(debug_assertions)]
    last_peeked: Vec<Option<DateTime<Utc>>>,
}

impl<St, InstrumentKey, Kind, Error> Stream for TimeSortedMerge<St, InstrumentKey, Kind, Error>
where
    St: Stream<Item = Result<MarketStreamEvent<InstrumentKey, Kind>, Error>>,
{
    type Item = Result<MarketStreamEvent<InstrumentKey, Kind>, Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        // Index of the earliest buffered `Item`, and its time.
        let mut earliest: Option<(usize, DateTime<Utc>)> = None;
        // Whether any input has yet to produce or end. See the check below the loop.
        let mut pending = false;

        for (index, input) in this.inputs.iter_mut().enumerate() {
            match input.as_mut().poll_peek(cx) {
                Poll::Pending => pending = true,
                // An ended input contributes nothing and stays ended.
                Poll::Ready(None) => {}
                // Forwarded ahead of buffered events: a failed source means the merged stream can
                // no longer be complete, and the consumer should learn that before acting on more
                // data.
                Poll::Ready(Some(Err(_))) => {
                    return input.as_mut().poll_next(cx);
                }
                // No timestamp to order on, so it is emitted where it occurred.
                Poll::Ready(Some(Ok(MarketStreamEvent::Reconnecting(_)))) => {
                    return input.as_mut().poll_next(cx);
                }
                Poll::Ready(Some(Ok(MarketStreamEvent::Item(event)))) => {
                    let time = event.time_exchange;

                    // `poll_peek` re-reports the same buffered event until it is consumed, so this
                    // must accept equality -- and ties across a genuine advance are legal anyway.
                    // Only a step backwards is the violation.
                    #[cfg(debug_assertions)]
                    {
                        debug_assert!(
                            this.last_peeked[index].is_none_or(|previous| previous <= time),
                            "merge_time_sorted input {index} is not sorted ascending by \
                             time_exchange: {:?} was followed by {time}. An unsorted input yields \
                             an unsorted output, which a backtest cannot detect downstream",
                            this.last_peeked[index]
                        );
                        this.last_peeked[index] = Some(time);
                    }

                    // Strictly `<`, so ties resolve to the earliest-listed input and the merge
                    // stays deterministic for a given input ordering.
                    if earliest.is_none_or(|(_, seen)| time < seen) {
                        earliest = Some((index, time));
                    }
                }
            }
        }

        // A single unresolved input holds the whole merge: it might be about to yield an event
        // earlier than anything currently buffered, and emitting the buffered one first would put
        // the output out of order — which no downstream consumer can detect or recover from.
        // Emitting is therefore only safe once EVERY input has either buffered an event or ended.
        // Each `poll_peek` above registered this task's waker, so the merge is re-polled when the
        // outstanding input resolves.
        if pending {
            return Poll::Pending;
        }

        match earliest {
            // The index came from the loop above, so this cannot be out of bounds.
            Some((index, _)) => this.inputs[index].as_mut().poll_next(cx),
            // Nothing pending and nothing buffered: every input has ended.
            None => Poll::Ready(None),
        }
    }
}

impl<St, InstrumentKey, Kind, Error> FusedStream for TimeSortedMerge<St, InstrumentKey, Kind, Error>
where
    St: Stream<Item = Result<MarketStreamEvent<InstrumentKey, Kind>, Error>>,
{
    /// Terminated exactly when every input is.
    ///
    /// `Peekable` is itself a [`FusedStream`], and `poll_next` above returns `Poll::Ready(None)`
    /// precisely when no input has either a buffered event or an outstanding poll — which is this
    /// condition. Implementing it means the normal backtest shape, a `select!` over the merged
    /// feed and a control channel, does not need a redundant `.fuse()` wrapper that would only
    /// re-derive a property the merge already has.
    fn is_terminated(&self) -> bool {
        self.inputs.iter().all(|input| input.is_terminated())
    }
}

/// Tag a stream of provider payloads as [`MarketStreamEvent::Item`]s for one instrument.
///
/// The N=1 building block for [`merge_time_sorted`]: historical fetches are typically per-symbol
/// and yield bare payloads, while consumers need [`MarketEvent`]s carrying the exchange and the
/// instrument key the caller resolved.
///
/// `time_exchange` is supplied by `time`, applied to each payload. For candles this MUST be the
/// bar's `close_time` — the period **end** — since stamping the open would enter a completed bar
/// into the timeline at the instant its period began, which is lookahead.
pub fn tag_events<St, Payload, InstrumentKey, Kind, Error>(
    stream: St,
    exchange: ExchangeId,
    instrument: InstrumentKey,
    time: impl Fn(&Payload) -> DateTime<Utc>,
    kind: impl Fn(Payload) -> Kind,
) -> impl Stream<Item = Result<MarketStreamEvent<InstrumentKey, Kind>, Error>>
where
    St: Stream<Item = Result<Payload, Error>>,
    InstrumentKey: Clone,
{
    stream.map(move |payload| {
        payload.map(|payload| {
            let time_exchange = time(&payload);
            MarketStreamEvent::Item(MarketEvent {
                time_exchange,
                // A historical replay has no real receipt instant; only `time_exchange` orders the
                // timeline, so mirroring it is the honest choice over a synthetic `Utc::now()`.
                time_received: time_exchange,
                exchange,
                instrument: instrument.clone(),
                kind: kind(payload),
            })
        })
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panicking on a bad fixture is acceptable
mod tests {
    use super::*;
    use crate::{event::DataKind, subscription::trade::PublicTrade};
    use rust_decimal_macros::dec;
    use rustrade_instrument::{Side, instrument::InstrumentIndex};

    #[derive(Debug, Clone, PartialEq)]
    struct TestError(&'static str);

    fn at(secs: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(secs, 0).unwrap()
    }

    fn item(
        instrument: usize,
        secs: i64,
    ) -> Result<MarketStreamEvent<InstrumentIndex, DataKind>, TestError> {
        Ok(MarketStreamEvent::Item(MarketEvent {
            time_exchange: at(secs),
            time_received: at(secs),
            exchange: ExchangeId::BinanceSpot,
            instrument: InstrumentIndex::new(instrument),
            kind: DataKind::Trade(PublicTrade {
                id: "1".into(),
                price: dec!(100),
                amount: dec!(1),
                side: Some(Side::Buy),
            }),
        }))
    }

    /// `(instrument index, time_exchange seconds)` of each merged `Item`.
    fn observed(
        events: &[Result<MarketStreamEvent<InstrumentIndex, DataKind>, TestError>],
    ) -> Vec<(usize, i64)> {
        events
            .iter()
            .filter_map(|event| match event {
                Ok(MarketStreamEvent::Item(event)) => {
                    Some((event.instrument.index(), event.time_exchange.timestamp()))
                }
                _ => None,
            })
            .collect()
    }

    async fn merge(
        inputs: Vec<Vec<Result<MarketStreamEvent<InstrumentIndex, DataKind>, TestError>>>,
    ) -> Vec<Result<MarketStreamEvent<InstrumentIndex, DataKind>, TestError>> {
        merge_time_sorted(inputs.into_iter().map(futures::stream::iter))
            .collect::<Vec<_>>()
            .await
    }

    #[tokio::test]
    async fn the_merge_reports_itself_terminated_once_every_input_has_ended() {
        // `FusedStream` is what lets a `select!` over the merged feed and a control channel -- the
        // normal backtest shape -- skip a redundant `.fuse()`. It has to be true before exhaustion
        // as well as after, or a `select!` would drop the merge on its first poll.
        let mut merged = merge_time_sorted(
            vec![vec![item(0, 10)], vec![item(1, 20)]]
                .into_iter()
                .map(futures::stream::iter),
        );

        assert!(!merged.is_terminated());

        while merged.next().await.is_some() {}

        assert!(merged.is_terminated());
    }

    #[tokio::test]
    async fn interleaves_three_streams_by_time() {
        let merged = merge(vec![
            vec![item(0, 10), item(0, 40)],
            vec![item(1, 20), item(1, 50)],
            vec![item(2, 30)],
        ])
        .await;

        assert_eq!(
            observed(&merged),
            vec![(0, 10), (1, 20), (2, 30), (0, 40), (1, 50)]
        );
    }

    #[tokio::test]
    async fn ties_resolve_to_the_earliest_listed_input() {
        let merged = merge(vec![
            vec![item(0, 10)],
            vec![item(1, 10)],
            vec![item(2, 10)],
        ])
        .await;

        assert_eq!(observed(&merged), vec![(0, 10), (1, 10), (2, 10)]);
    }

    #[tokio::test]
    async fn drains_remaining_inputs_after_others_end() {
        let merged = merge(vec![
            vec![item(0, 10)],
            vec![item(1, 20), item(1, 30), item(1, 40)],
        ])
        .await;

        assert_eq!(observed(&merged), vec![(0, 10), (1, 20), (1, 30), (1, 40)]);
    }

    /// The sort obligation cannot be *enforced* without buffering, so the debug-only check is the
    /// only thing that makes a violation loud — and a check nothing exercises is one that can be
    /// inverted or deleted without a single test noticing.
    // Neither the assert nor the state it reads exists in a release build.
    #[cfg(debug_assertions)]
    #[tokio::test]
    #[should_panic(expected = "is not sorted ascending")]
    async fn an_input_that_steps_backwards_trips_the_debug_assert() {
        let _merged = merge(vec![vec![item(0, 20), item(0, 10)]]).await;
    }

    /// An input that has not yet produced must not be overtaken by one that has.
    ///
    /// This is the merge's whole correctness condition, and the failure is silent: emitting a
    /// buffered event while another input is still `Pending` yields an out-of-order stream that no
    /// downstream consumer can detect — in a backtest, a non-monotonic clock and wrong results.
    /// Real inputs resolve at different times (independent HTTP fetches, files on different
    /// devices), so this is the normal case, not an edge one.
    #[tokio::test]
    async fn a_slow_input_is_not_overtaken_by_a_ready_one() {
        // Input 0 is immediately ready but holds LATER events; input 1 must yield to the executor
        // before producing its EARLIER ones.
        let fast = futures::stream::iter(vec![item(0, 20), item(0, 40)]);
        let slow = futures::stream::iter(vec![item(1, 10), item(1, 30)]).then(|event| async move {
            for _ in 0..4 {
                tokio::task::yield_now().await;
            }
            event
        });

        let merged = merge_time_sorted(vec![
            Box::pin(fast) as Pin<Box<dyn Stream<Item = _> + Send>>,
            Box::pin(slow),
        ])
        .collect::<Vec<_>>()
        .await;

        assert_eq!(
            observed(&merged),
            vec![(1, 10), (0, 20), (1, 30), (0, 40)],
            "merge must wait for a pending input rather than emitting a later buffered event"
        );
    }

    #[tokio::test]
    async fn empty_input_set_ends_immediately() {
        assert!(merge(vec![]).await.is_empty());
    }

    #[tokio::test]
    async fn empty_inputs_are_skipped() {
        let merged = merge(vec![vec![], vec![item(1, 20)], vec![]]).await;

        assert_eq!(observed(&merged), vec![(1, 20)]);
    }

    /// A failed source is forwarded ahead of buffered events, so the consumer learns the merged
    /// stream can no longer be complete before acting on more data.
    #[tokio::test]
    async fn error_is_forwarded_ahead_of_buffered_items() {
        let merged = merge(vec![
            vec![item(0, 10)],
            vec![Err(TestError("boom")), item(1, 20)],
        ])
        .await;

        assert_eq!(
            merged.first().unwrap().clone().err(),
            Some(TestError("boom"))
        );

        // The whole sequence, not just its head. Asserting only `first()` leaves the *other* half
        // of this contract unpinned: forwarding the error must not also discard the events behind
        // it. An implementation that reported the error and then ended -- the silent-truncation
        // failure this module exists to prevent -- would satisfy the assertion above.
        assert_eq!(
            observed(&merged),
            vec![(0, 10), (1, 20)],
            "the error is forwarded ahead of the buffered events, not instead of them"
        );
        assert_eq!(merged.len(), 3, "exactly one error and both events");
        assert_eq!(
            merged.iter().filter(|event| event.is_err()).count(),
            1,
            "the error must be reported once, not re-yielded"
        );
    }

    #[tokio::test]
    async fn reconnecting_is_emitted_where_it_occurs() {
        let merged = merge(vec![
            vec![item(0, 10), item(0, 90)],
            vec![
                Ok(MarketStreamEvent::Reconnecting(ExchangeId::BinanceSpot)),
                item(1, 20),
            ],
        ])
        .await;

        assert!(matches!(
            merged.first(),
            Some(Ok(MarketStreamEvent::Reconnecting(_)))
        ));
        assert_eq!(observed(&merged), vec![(0, 10), (1, 20), (0, 90)]);
    }

    #[tokio::test]
    async fn tag_events_stamps_exchange_instrument_and_time() {
        let payloads = futures::stream::iter(vec![Ok::<_, TestError>(at(42))]);
        let tagged = tag_events(
            payloads,
            ExchangeId::BinanceSpot,
            InstrumentIndex::new(7),
            |time: &DateTime<Utc>| *time,
            |time| {
                DataKind::Trade(PublicTrade {
                    id: time.timestamp().to_string().into(),
                    price: dec!(1),
                    amount: dec!(1),
                    side: None,
                })
            },
        )
        .collect::<Vec<_>>()
        .await;

        let Some(Ok(MarketStreamEvent::Item(event))) = tagged.first() else {
            panic!("expected a tagged Item")
        };
        assert_eq!(event.time_exchange, at(42));
        assert_eq!(event.time_received, at(42));
        assert_eq!(event.instrument, InstrumentIndex::new(7));
        assert_eq!(event.exchange, ExchangeId::BinanceSpot);
    }
}
