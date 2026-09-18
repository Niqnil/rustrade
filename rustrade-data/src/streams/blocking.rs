use futures::Stream;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::sync::mpsc::Receiver;
use tokio::task::JoinHandle;

/// Channel depth [`stream_blocking_iter`] is documented for, and a sensible default for a decoder
/// feeding a backtest.
///
/// Large enough that the producer is not woken per item, small enough that the buffered events are
/// an accounting rounding error next to the dataset. At ~160 bytes per market event this is ~160 KiB
/// in flight.
pub const DEFAULT_BLOCKING_CHANNEL_CAPACITY: usize = 1024;

/// Bridge a **blocking**, fallible iterator into a bounded [`Stream`], decoding on a blocking thread.
///
/// Historical data usually arrives as something that blocks: a Parquet artifact, a compressed
/// archive, a CSV on a slow disk. Two things then go wrong if such an iterator is simply wrapped in
/// [`futures::stream::iter`]:
///
/// 1. **It stalls a runtime worker for the whole decode.** Nothing else scheduled on that worker —
///    the engine, another instrument's decode, a heartbeat — makes progress until the file ends.
/// 2. **It never yields `Pending`, so nothing downstream can pace it.** A consumer that forwards
///    into an unbounded channel (the engine feed is one) therefore takes the *entire* artifact into
///    memory before processing the first event, which is the opposite of the streaming the caller
///    asked for. Laziness alone does not bound memory; the producer has to be made to wait.
///
/// This helper fixes both: `init` runs on a [`tokio::task::spawn_blocking`] thread — so opening the
/// source is blocking too, as it usually is — and each item is handed over a channel of `capacity`
/// items. When the channel is full the blocking thread parks, which is the back-pressure: the decoder
/// runs at most `capacity` items ahead of whatever polls this stream, whatever the dataset's size,
/// and decoding overlaps consumption instead of preceding it.
///
/// **That bound is local to this hand-off.** It constrains nothing further downstream: a caller that
/// forwards this stream into another *unbounded* queue — a backtest engine feed is exactly that shape
/// — reintroduces unbounded growth there whenever the far end is the slower side. What this helper
/// guarantees is "the decoder will not get more than `capacity` items ahead of its own consumer", not
/// an end-to-end memory bound for whatever pipeline it is embedded in.
///
/// # Failure model
/// The item type is `Result`, because a source that reads incrementally can fail *after* it opened
/// successfully. An `init` that fails yields exactly one `Err` and ends the stream, so a caller
/// handles open and mid-stream failures on one path.
///
/// A **panic** is not an `Err` and is not reported as one: `Error` is an unconstrained generic, so
/// there is no value this helper could construct to represent one. It is re-raised on the consumer
/// instead — see `# Panics`. What it must never do is end the stream quietly, which would leave a
/// truncated decode indistinguishable from a source that finished, and drive a backtest to a
/// normal-looking summary over partial data.
///
/// A [`tokio::task::JoinError`] that is *not* a panic does end the stream as a clean `None`. A
/// `spawn_blocking` task cannot be aborted and this one's handle is never exposed, so the only way
/// to reach that is runtime shutdown — where the consumer is being torn down for the same reason and
/// has nothing left to do with the news.
///
/// # Cancellation
/// Dropping the returned stream drops the receiver, the next hand-off fails, and the blocking task
/// returns at its next item boundary. A blocking task cannot be pre-empted, so an iterator that
/// blocks for a long time between items keeps its thread until it comes back — bounded by the
/// iterator's own step, not by the dataset.
///
/// # Panics
/// Panics if `capacity` is zero (a zero-capacity channel cannot deliver), and requires a Tokio
/// runtime with the `rt` feature, like any [`tokio::task::spawn_blocking`] caller.
///
/// **A panic in `init` or in the iterator is re-raised on the task that polls this stream**, with
/// the original payload, at the point the stream would otherwise have ended. This mirrors
/// [`futures::stream::iter`] over the same iterator, where the panic surfaces on the poller
/// directly: moving the decode to another thread changes where the work happens, not whether the
/// caller hears about it. Items already handed over are still yielded first, so the panic arrives
/// after the last successfully decoded item rather than discarding the batch.
///
/// # Polling past the end
/// `None` is terminal and repeatable: polling again yields `None` rather than panicking, so a
/// consumer that does not track termination itself needs no `.fuse()`. The returned stream is also
/// [`Unpin`], so awaiting `next()` needs no pinning.
///
/// Both are part of the contract, not accidents of the current body — a rewrite that broke either
/// (an `async_stream` generator breaks both) would break callers. [`Unpin`] is therefore in the
/// return type, so such a rewrite fails to compile here rather than at some downstream call site;
/// the terminal `None` cannot be spelled in a signature and is pinned by test instead. `FusedStream`
/// is deliberately *not* implemented: the terminal `None` above already gives a consumer what
/// fusing would, the intended consumer
/// [`merge_time_sorted`](super::merge::merge_time_sorted) fuses its inputs itself, and widening the
/// return type later is an additive change. That is a design judgement rather than an observation —
/// this helper has no in-tree production callers, so there is no consumer set to appeal to.
///
/// # ⚠️ One blocking thread per call, held for the whole decode
/// [`tokio::task::spawn_blocking`] fires **at call time, not at first poll**, so building N of
/// these starts N blocking tasks immediately, before anything is polled. Each holds its thread for
/// the whole decode: `blocking_send` parks the thread while the channel is full rather than
/// returning it to the pool, which is precisely the back-pressure described above.
///
/// Tokio's blocking pool defaults to `max_blocking_threads: 512`. Past that, further tasks are
/// queued and get no thread until a running one returns — and a decode only returns when its
/// consumer drains it.
///
/// **That combination deadlocks under a k-way merge.**
/// [`merge_time_sorted`](super::merge::merge_time_sorted) collects its inputs eagerly and cannot
/// emit until *every* input has buffered an event or ended (see its `# Pace` section). With more
/// than 512 inputs, the queued tasks never run, so their streams stay `Pending`, so the merge stays
/// `Pending`, so none of the *running* decodes is drained and none releases its thread. Nothing
/// times out and nothing logs: the backtest simply stops.
///
/// A 512-instrument universe is not exotic, so a caller merging more inputs than the pool has
/// threads must either raise
/// [`max_blocking_threads`](tokio::runtime::Builder::max_blocking_threads) above the input count,
/// or merge in batches whose size it does not exceed.
///
/// # Examples
/// ```no_run
/// # use rustrade_data::streams::blocking::{stream_blocking_iter, DEFAULT_BLOCKING_CHANNEL_CAPACITY};
/// # fn decode() -> Result<std::vec::IntoIter<Result<u64, std::io::Error>>, std::io::Error> {
/// #     unimplemented!()
/// # }
/// let _stream = stream_blocking_iter(DEFAULT_BLOCKING_CHANNEL_CAPACITY, decode);
/// ```
pub fn stream_blocking_iter<Init, Iter, Item, Error>(
    capacity: usize,
    init: Init,
) -> impl Stream<Item = Result<Item, Error>> + Send + Unpin + 'static
where
    Init: FnOnce() -> Result<Iter, Error> + Send + 'static,
    // No `Send`/`'static` on `Iter`: it is built and drained entirely inside the `spawn_blocking`
    // closure, never captured by it and never returned, so it does not cross a thread boundary. A
    // decoder with `!Send` internals is therefore usable here.
    Iter: Iterator<Item = Result<Item, Error>>,
    Item: Send + 'static,
    Error: Send + 'static,
{
    assert!(
        capacity > 0,
        "stream_blocking_iter requires a non-zero channel capacity"
    );

    let (tx, rx) = tokio::sync::mpsc::channel(capacity);

    // The handle is retained, not detached: it is the only thing that distinguishes an iterator
    // that finished from one that unwound. `spawn_blocking` tasks are not abortable, so it buys
    // nothing for cancellation -- dropping the receiver is still what ends the task -- but a
    // dropped handle would discard the panic along with it. See `BlockingIterStream::poll_next`.
    let handle = tokio::task::spawn_blocking(move || {
        let iter = match init() {
            Ok(iter) => iter,
            // The consumer is gone; there is nobody to report the failure to.
            Err(error) => {
                let _ = tx.blocking_send(Err(error));
                return;
            }
        };

        for item in iter {
            // Parks this thread while the channel is full -- the back-pressure the bound exists for.
            // An `Err` means the receiver was dropped, so the remaining items have no destination.
            if tx.blocking_send(item).is_err() {
                break;
            }
        }
    });

    BlockingIterStream {
        rx,
        handle: Some(handle),
    }
}

/// The [`Stream`] returned by [`stream_blocking_iter`].
///
/// Private: it exists to add one behaviour to a plain receiver stream — deciding *why* the channel
/// closed — and nothing about that needs to be nameable by a caller.
struct BlockingIterStream<Item, Error> {
    rx: Receiver<Result<Item, Error>>,
    /// Taken once resolved, so a stream polled past its end does not poll a completed handle.
    handle: Option<JoinHandle<()>>,
}

impl<Item, Error> Stream for BlockingIterStream<Item, Error> {
    type Item = Result<Item, Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // Every field is `Unpin`, so the struct is too and needs no projection.
        let this = self.get_mut();

        match this.rx.poll_recv(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Some(item)) => return Poll::Ready(Some(item)),
            // Every sender is gone. That is either the iterator finishing or the thread unwinding,
            // and the two are indistinguishable from here -- the join handle is what tells them
            // apart, so fall through rather than reporting an end the caller would have to trust.
            Poll::Ready(None) => {}
        }

        let Some(handle) = this.handle.as_mut() else {
            return Poll::Ready(None);
        };

        // The task drops its sender as it returns *or* as it unwinds, so the channel can close a
        // moment before the task is marked complete. Polling registers a waker for that gap rather
        // than spinning on it.
        let result = std::task::ready!(Pin::new(handle).poll(cx));
        this.handle = None;

        match result {
            Ok(()) => Poll::Ready(None),
            // Re-raised with the original payload, on the task that polls this stream. Losing it
            // here is the silent truncation the `# Failure model` exists to rule out.
            Err(error) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
            // `spawn_blocking` tasks cannot be aborted, so the only way to reach this is runtime
            // shutdown, where the consumer is going away too and has nothing to do with the news.
            Err(_) => Poll::Ready(None),
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use futures::StreamExt;
    use std::{
        io::{Error, ErrorKind},
        sync::{
            Arc, Mutex,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    fn ok_iter(count: usize) -> Result<std::vec::IntoIter<Result<usize, Error>>, Error> {
        Ok((0..count).map(Ok).collect::<Vec<_>>().into_iter())
    }

    #[tokio::test]
    async fn forwards_every_item_in_order() {
        let stream = stream_blocking_iter(4, || ok_iter(50));

        let items = stream.map(|item| item.unwrap()).collect::<Vec<_>>().await;

        assert_eq!(items, (0..50).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn an_init_failure_becomes_a_single_err_item() {
        let stream = stream_blocking_iter(4, || {
            Err::<std::vec::IntoIter<Result<usize, Error>>, _>(Error::new(
                ErrorKind::NotFound,
                "no such artifact",
            ))
        });

        let items = stream.collect::<Vec<_>>().await;
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].as_ref().unwrap_err().kind(), ErrorKind::NotFound);
    }

    #[tokio::test]
    async fn a_mid_stream_failure_is_surfaced_not_skipped() {
        let stream = stream_blocking_iter(4, || {
            Ok(vec![
                Ok(1_usize),
                Err(Error::new(ErrorKind::InvalidData, "bad row")),
                Ok(3),
            ]
            .into_iter())
        });

        let items = stream.collect::<Vec<_>>().await;
        assert_eq!(items.len(), 3);
        assert!(items[1].is_err());
    }

    /// A panicking decoder must not look like a decoder that reached the end of its data. Silently
    /// ending here would let a truncated artifact drive a backtest to a normal-looking summary.
    #[tokio::test]
    #[should_panic(expected = "corrupt page header")]
    async fn a_panic_in_the_iterator_reaches_the_consumer() {
        let stream = stream_blocking_iter(4, || {
            Ok((0..10).map(|index: usize| {
                assert!(index < 3, "corrupt page header");
                Ok::<usize, Error>(index)
            }))
        });

        let _items = stream.collect::<Vec<_>>().await;
    }

    #[tokio::test]
    #[should_panic(expected = "unreadable footer")]
    async fn a_panic_in_init_reaches_the_consumer() {
        let stream = stream_blocking_iter(4, || {
            panic!("unreadable footer");
            #[allow(unreachable_code)] // Pins the closure's return type for inference.
            ok_iter(0)
        });

        let _items = stream.collect::<Vec<_>>().await;
    }

    /// The items decoded before the panic are real data and are still delivered; the panic arrives
    /// after them, not instead of them.
    #[tokio::test]
    async fn items_before_a_panic_are_still_yielded() {
        // Collected through a handle that OUTLIVES the unwind. A `Vec` local to the caught future
        // is destroyed by the panic along with the rest of that frame, so nothing can be asserted
        // about it afterwards -- which left the "items before the panic are still yielded" half of
        // this contract untested, and only the "the panic escapes" half pinned.
        let delivered = Arc::new(Mutex::new(Vec::new()));

        let sink = Arc::clone(&delivered);
        let collect = std::panic::AssertUnwindSafe(async move {
            let mut stream = Box::pin(stream_blocking_iter(4, || {
                Ok((0..10).map(|index: usize| {
                    assert!(index < 3, "boom");
                    Ok::<usize, Error>(index)
                }))
            }));

            while let Some(item) = stream.next().await {
                // The lock is never held across the await, so the panic cannot poison it.
                sink.lock().unwrap().push(item.unwrap());
            }
        });

        let outcome = futures::FutureExt::catch_unwind(collect).await;

        assert!(outcome.is_err(), "the panic must not be swallowed");
        assert_eq!(
            *delivered.lock().unwrap(),
            vec![0, 1, 2],
            "every item decoded before the panic is real data and must still reach the consumer"
        );
    }

    /// The clean path must stay clean: a normal end is still a normal end, not a panic.
    #[tokio::test]
    async fn a_normal_end_of_stream_does_not_panic() {
        let mut stream = Box::pin(stream_blocking_iter(4, || ok_iter(3)));

        for expected in 0..3 {
            assert_eq!(stream.next().await.unwrap().unwrap(), expected);
        }

        // Polled past the end twice: the join handle is consumed on the first, and the second must
        // not poll it again.
        assert!(stream.next().await.is_none());
        assert!(stream.next().await.is_none());
    }

    /// The property the type exists for: the producer must not run ahead of the consumer without
    /// bound. With a capacity of 2 and nothing drained, no more than a handful of items can have
    /// been produced -- emphatically not all 10,000.
    #[tokio::test]
    async fn the_producer_is_bounded_by_the_channel_capacity() {
        const CAPACITY: usize = 2;
        let produced = Arc::new(AtomicUsize::new(0));

        let counter = Arc::clone(&produced);
        let mut stream = Box::pin(stream_blocking_iter(CAPACITY, move || {
            Ok((0..10_000).map(move |index| {
                counter.fetch_add(1, Ordering::SeqCst);
                Ok::<usize, Error>(index)
            }))
        }));

        // Take one item, then let the blocking thread run as far as it can.
        assert_eq!(stream.next().await.unwrap().unwrap(), 0);
        tokio::task::yield_now().await;

        // One consumed, `CAPACITY` buffered, and at most one more parked mid-hand-off.
        let produced = produced.load(Ordering::SeqCst);
        assert!(
            produced <= CAPACITY + 2,
            "producer ran {produced} items ahead of a {CAPACITY}-item channel"
        );
    }

    #[tokio::test]
    async fn dropping_the_stream_stops_the_producer() {
        const ITEMS: usize = 10_000;
        let produced = Arc::new(AtomicUsize::new(0));

        let counter = Arc::clone(&produced);
        let mut stream = Box::pin(stream_blocking_iter(2, move || {
            Ok((0..ITEMS).map(move |index| {
                counter.fetch_add(1, Ordering::SeqCst);
                Ok::<usize, Error>(index)
            }))
        }));

        assert!(stream.next().await.is_some());
        drop(stream);

        // Wait for the count to STOP climbing, rather than assuming it already has. The producer
        // runs on its own OS thread and stops at its next hand-off, so a `yield_now` on this side
        // proves nothing about where that thread has got to -- sampling twice around one yield can
        // catch it mid-iteration and fail spuriously. Polling until two samples separated by a real
        // delay agree is the same assertion made soundly.
        let mut settled = produced.load(Ordering::SeqCst);
        let mut stopped = false;
        for _ in 0..50 {
            tokio::time::sleep(Duration::from_millis(10)).await;
            let current = produced.load(Ordering::SeqCst);
            if current == settled {
                stopped = true;
                break;
            }
            settled = current;
        }

        assert!(
            stopped,
            "the producer was still running 500ms after the stream was dropped ({settled} items)"
        );
        // The property that matters, and the one a "count stopped changing" assertion alone does
        // not establish: it stopped *early*, rather than draining the whole iterator into a channel
        // nobody was left to read. The tight bound belongs to the sibling capacity test; here the
        // question is only whether the drop was observed at all.
        assert!(
            settled < ITEMS,
            "the producer ran to completion ({settled} items) despite the stream being dropped"
        );
    }
}
