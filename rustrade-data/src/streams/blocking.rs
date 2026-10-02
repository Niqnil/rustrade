use futures::Stream;
use std::any::Any;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::task::JoinHandle;
use tracing::warn;

/// Chunk size [`stream_blocking_iter`] is documented for, and a sensible default for a decoder
/// feeding a backtest.
///
/// Large enough that a blocking task is started once per thousand items rather than per item, small
/// enough that the buffered events are an accounting rounding error next to the dataset. At ~160
/// bytes per market event, the two chunks one stream can hold are ~320 KiB. That is per stream: a
/// merge of N streams holds up to N times as much, see [`stream_blocking_iter`].
pub const DEFAULT_BLOCKING_CHUNK_SIZE: usize = 1024;

/// Bridge a **blocking**, fallible iterator into a bounded [`Stream`], decoding on Tokio's blocking
/// pool.
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
/// This helper fixes both. The decode runs in chunks of `chunk_size` items, each on a
/// [`tokio::task::spawn_blocking`] task that decodes the chunk, returns it, and gives its thread
/// back. When a chunk arrives the stream starts the next one, so decoding overlaps consumption, and
/// it starts no further chunk until the consumer has drained the one it holds. That is the
/// back-pressure: the decoder runs at most two chunks, `2 × chunk_size` items, ahead of whatever
/// polls this stream, whatever the dataset's size. `init` runs in the first chunk's task, so opening
/// the source is off the runtime's workers too, as it usually needs to be.
///
/// **That bound is local to this hand-off.** It constrains nothing further downstream: a caller that
/// forwards this stream into another *unbounded* queue — a backtest engine feed is exactly that shape
/// — reintroduces unbounded growth there whenever the far end is the slower side. What this helper
/// guarantees is "the decoder will not get more than two chunks ahead of its own consumer", not an
/// end-to-end memory bound for whatever pipeline it is embedded in.
///
/// The bound is also **per stream**. A [`merge_time_sorted`](super::merge::merge_time_sorted) of N
/// of these polls every input until each has delivered a chunk, so it holds up to
/// `2 × N × chunk_size` decoded items. For a merge of thousands of inputs, lower `chunk_size`.
///
/// # No thread is held while waiting, so any number of these can be merged
/// A blocking thread is held only while a chunk is being decoded, never while the stream waits for
/// its consumer. Building a stream starts nothing: the first chunk is started on first poll. Any
/// number of these can therefore be driven together, for example by
/// [`merge_time_sorted`](super::merge::merge_time_sorted), which cannot emit until every input has
/// buffered an event. Past Tokio's `max_blocking_threads` (512 by default), chunks queue for a thread
/// and every one of them still finishes, so the merge is slower but always progresses.
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
/// A [`tokio::task::JoinError`] that is *not* a panic ends the stream with `None`, after a `warn!`.
/// This one's handle is never exposed, so nothing aborts the task, and the only way to reach that is
/// runtime shutdown cancelling a chunk still queued for a thread. The consumer is normally being torn
/// down for the same reason; the `warn!` says the decode was cut short in case it is not.
///
/// # Cancellation
/// Dropping the returned stream starts no further chunk. The chunk already started is not
/// cancelled, whether it is being decoded or still waiting for a thread: it runs to its end, at most
/// `chunk_size` items, and its result is discarded with the iterator. Letting it run keeps the
/// iterator, and whatever its `Drop` does, on the blocking pool.
///
/// # Panics
/// Panics if `chunk_size` is zero (a chunk must deliver at least one item), and, when first polled,
/// outside a Tokio runtime, like any [`tokio::task::spawn_blocking`] caller. `chunk_size` bounds the
/// items per chunk and is not allocated up front, so a large value such as `usize::MAX` is safe.
///
/// **A panic in `init` or in the iterator is re-raised on the task that polls this stream**, with
/// the original payload, at the point the stream would otherwise have ended. This mirrors
/// [`futures::stream::iter`] over the same iterator, where the panic surfaces on the poller
/// directly: moving the decode to another thread changes where the work happens, not whether the
/// caller hears about it. Items decoded before the panic, including those earlier in the same
/// chunk, are still yielded first, so the panic arrives after the last successfully decoded item
/// rather than discarding the batch.
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
/// # The iterator must be `Send + 'static`
/// The iterator moves from one chunk's task to the next, so it has to cross threads. A decoder with
/// `!Send` internals is not accepted. Holding one thread for the whole decode would allow it, and
/// is exactly what made a merge of more inputs than the pool has threads deadlock.
///
/// # Examples
/// ```no_run
/// # use rustrade_data::streams::blocking::{stream_blocking_iter, DEFAULT_BLOCKING_CHUNK_SIZE};
/// # fn decode() -> Result<std::vec::IntoIter<Result<u64, std::io::Error>>, std::io::Error> {
/// #     unimplemented!()
/// # }
/// let _stream = stream_blocking_iter(DEFAULT_BLOCKING_CHUNK_SIZE, decode);
/// ```
pub fn stream_blocking_iter<Init, Iter, Item, Error>(
    chunk_size: usize,
    init: Init,
) -> impl Stream<Item = Result<Item, Error>> + Send + Unpin + 'static
where
    Init: FnOnce() -> Result<Iter, Error> + Send + 'static,
    Iter: Iterator<Item = Result<Item, Error>> + Send + 'static,
    Item: Send + 'static,
    Error: Send + 'static,
{
    assert!(
        chunk_size > 0,
        "stream_blocking_iter requires a non-zero chunk size"
    );

    BlockingIterStream {
        chunk_size,
        init: Some(init),
        decoding: None,
        ready: Vec::new().into_iter(),
        panic: None,
    }
}

/// Where a chunk's task picks the decode up: the source not yet opened, or the iterator a previous
/// chunk returned.
enum Source<Init, Iter> {
    Init(Init),
    Iter(Iter),
}

/// What one chunk's task hands back.
struct Chunk<Iter, Item, Error> {
    /// The items decoded, in order.
    items: Vec<Result<Item, Error>>,
    /// The iterator, if it may have more items. `None` once it is exhausted, `init` failed, or the
    /// decode panicked.
    next: Option<Iter>,
    /// A panic caught in `init` or the iterator, re-raised once `items` have been yielded.
    panic: Option<Box<dyn Any + Send>>,
}

/// Decodes up to `chunk_size` items from `source`, catching a panic so that the items decoded
/// before it are still returned.
fn decode_chunk<Init, Iter, Item, Error>(
    source: Source<Init, Iter>,
    chunk_size: usize,
) -> Chunk<Iter, Item, Error>
where
    Init: FnOnce() -> Result<Iter, Error>,
    Iter: Iterator<Item = Result<Item, Error>>,
{
    // Capped: `chunk_size` bounds the chunk, but a source may yield far fewer items, and a huge
    // value would otherwise fail the allocation before decoding anything.
    let mut items = Vec::with_capacity(chunk_size.min(DEFAULT_BLOCKING_CHUNK_SIZE));

    // `AssertUnwindSafe`: after a panic, the only state used again is `items`, which holds whole
    // items pushed before it. The iterator, the one value a panic could leave half-updated, is
    // never touched again.
    let decoded = std::panic::catch_unwind(AssertUnwindSafe(|| {
        let mut iter = match source {
            Source::Iter(iter) => iter,
            Source::Init(init) => match init() {
                Ok(iter) => iter,
                Err(error) => {
                    items.push(Err(error));
                    return None;
                }
            },
        };

        while items.len() < chunk_size {
            match iter.next() {
                Some(item) => items.push(item),
                None => return None,
            }
        }
        Some(iter)
    }));

    match decoded {
        Ok(next) => Chunk {
            items,
            next,
            panic: None,
        },
        Err(payload) => Chunk {
            items,
            next: None,
            panic: Some(payload),
        },
    }
}

/// The [`Stream`] returned by [`stream_blocking_iter`].
///
/// Private: it exists to sequence the chunk tasks and re-raise a decode panic, and nothing about
/// that needs to be nameable by a caller.
struct BlockingIterStream<Init, Iter, Item, Error> {
    chunk_size: usize,
    /// The source, until the first poll starts the first chunk with it.
    init: Option<Init>,
    /// The chunk being decoded, if any. At most one is in flight.
    decoding: Option<JoinHandle<Chunk<Iter, Item, Error>>>,
    /// Items of the last chunk received, not yet yielded.
    ready: std::vec::IntoIter<Result<Item, Error>>,
    /// A decode panic, re-raised once `ready` is drained.
    panic: Option<Box<dyn Any + Send>>,
}

// No field is ever pinned: `poll_next` reaches them through `get_mut` and moves `init` out by value.
// Without this, `Unpin` would depend on `Init`, `Item` and `Error`, and a closure capturing a
// `!Unpin` value would make the stream `!Unpin`, breaking the `Unpin` the return type promises.
impl<Init, Iter, Item, Error> Unpin for BlockingIterStream<Init, Iter, Item, Error> {}

impl<Init, Iter, Item, Error> BlockingIterStream<Init, Iter, Item, Error>
where
    Init: FnOnce() -> Result<Iter, Error> + Send + 'static,
    Iter: Iterator<Item = Result<Item, Error>> + Send + 'static,
    Item: Send + 'static,
    Error: Send + 'static,
{
    fn start_chunk(&mut self, source: Source<Init, Iter>) {
        let chunk_size = self.chunk_size;
        self.decoding = Some(tokio::task::spawn_blocking(move || {
            decode_chunk(source, chunk_size)
        }));
    }
}

impl<Init, Iter, Item, Error> Stream for BlockingIterStream<Init, Iter, Item, Error>
where
    Init: FnOnce() -> Result<Iter, Error> + Send + 'static,
    Iter: Iterator<Item = Result<Item, Error>> + Send + 'static,
    Item: Send + 'static,
    Error: Send + 'static,
{
    type Item = Result<Item, Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        if let Some(init) = this.init.take() {
            this.start_chunk(Source::Init(init));
        }

        loop {
            if let Some(item) = this.ready.next() {
                return Poll::Ready(Some(item));
            }

            // Every item decoded before a panic has now been yielded, so it is re-raised here,
            // with the original payload, on the task polling this stream. Losing it would be the
            // silent truncation the `# Failure model` rules out.
            if let Some(payload) = this.panic.take() {
                std::panic::resume_unwind(payload);
            }

            let Some(handle) = this.decoding.as_mut() else {
                return Poll::Ready(None);
            };
            let result = std::task::ready!(Pin::new(handle).poll(cx));
            this.decoding = None;

            let chunk = match result {
                Ok(chunk) => chunk,
                // `decode_chunk` catches panics in the decode itself, so this is a panic outside
                // it, still re-raised rather than lost.
                Err(error) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
                // Nothing aborts the task, so this is runtime shutdown cancelling a chunk still
                // queued for a thread. The consumer is normally going away too.
                Err(error) => {
                    warn!(
                        %error,
                        "stream_blocking_iter: the runtime cancelled a decode chunk - ending the \
                         stream early"
                    );
                    return Poll::Ready(None);
                }
            };

            // Start the next chunk before yielding this one, so decoding overlaps consumption. No
            // chunk after that starts until this one is drained: the back-pressure.
            if let Some(iter) = chunk.next {
                this.start_chunk(Source::Iter(iter));
            }
            this.ready = chunk.items.into_iter();
            this.panic = chunk.panic;
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
    /// bound. With chunks of 2 and one item drained, at most two chunks can have been decoded --
    /// emphatically not all 10,000, however long the decoder is left to run.
    #[tokio::test]
    async fn the_producer_is_bounded_by_two_chunks() {
        const CHUNK: usize = 2;
        let produced = Arc::new(AtomicUsize::new(0));

        let counter = Arc::clone(&produced);
        let mut stream = Box::pin(stream_blocking_iter(CHUNK, move || {
            Ok((0..10_000).map(move |index| {
                counter.fetch_add(1, Ordering::SeqCst);
                Ok::<usize, Error>(index)
            }))
        }));

        // Take one item, then wait for the next chunk, which starts as soon as the first arrives
        // so that decoding overlaps consumption.
        assert_eq!(stream.next().await.unwrap().unwrap(), 0);
        let mut waited = 0;
        while produced.load(Ordering::SeqCst) < 2 * CHUNK {
            assert!(
                waited < 500,
                "the next chunk was not started ahead of the consumer"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
            waited += 1;
        }

        // Then give a third chunk every chance to appear: it must not.
        tokio::time::sleep(Duration::from_millis(50)).await;
        let produced = produced.load(Ordering::SeqCst);
        assert_eq!(
            produced,
            2 * CHUNK,
            "producer ran {produced} items ahead with chunks of {CHUNK}"
        );
    }

    #[test]
    #[should_panic(expected = "non-zero chunk size")]
    fn a_zero_chunk_size_is_rejected() {
        let _stream = stream_blocking_iter(0, || ok_iter(1));
    }

    /// Every chunk size delivers every item once, in order, including sizes that divide the item
    /// count exactly and a size far larger than the source.
    #[tokio::test]
    async fn every_chunk_size_forwards_every_item_in_order() {
        for chunk_size in [1, 2, 3, 5, 10, usize::MAX] {
            let items = stream_blocking_iter(chunk_size, || ok_iter(10))
                .map(|item| item.unwrap())
                .collect::<Vec<_>>()
                .await;
            assert_eq!(
                items,
                (0..10).collect::<Vec<_>>(),
                "chunk size {chunk_size}"
            );
        }

        let empty = stream_blocking_iter(4, || ok_iter(0))
            .collect::<Vec<_>>()
            .await;
        assert!(empty.is_empty());
    }

    /// A panic after earlier chunks were handed over arrives after all of their items, and the
    /// stream then reads as ended.
    #[tokio::test]
    async fn a_panic_in_a_later_chunk_follows_every_earlier_item() {
        let delivered = Arc::new(Mutex::new(Vec::new()));

        let sink = Arc::clone(&delivered);
        let mut stream = Box::pin(stream_blocking_iter(2, || {
            Ok((0..10).map(|index: usize| {
                assert!(index < 5, "boom");
                Ok::<usize, Error>(index)
            }))
        }));
        let collect = std::panic::AssertUnwindSafe(async {
            while let Some(item) = stream.next().await {
                sink.lock().unwrap().push(item.unwrap());
            }
        });

        let outcome = futures::FutureExt::catch_unwind(collect).await;

        assert!(outcome.is_err(), "the panic must not be swallowed");
        assert_eq!(*delivered.lock().unwrap(), vec![0, 1, 2, 3, 4]);
        assert!(
            stream.next().await.is_none(),
            "polled again after the panic, the stream reads as ended"
        );
    }

    /// Building a stream starts nothing: the first chunk, `init` included, starts on first poll.
    #[tokio::test]
    async fn nothing_is_decoded_before_the_first_poll() {
        let opened = Arc::new(AtomicUsize::new(0));

        let counter = Arc::clone(&opened);
        let mut stream = Box::pin(stream_blocking_iter(4, move || {
            counter.fetch_add(1, Ordering::SeqCst);
            ok_iter(3)
        }));

        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(opened.load(Ordering::SeqCst), 0, "init ran before any poll");

        assert_eq!(stream.next().await.unwrap().unwrap(), 0);
        assert_eq!(opened.load(Ordering::SeqCst), 1);
    }

    /// The items decoded earlier in the same chunk as a panic are real data too.
    #[tokio::test]
    async fn items_before_a_panic_in_the_same_chunk_are_still_yielded() {
        let delivered = Arc::new(Mutex::new(Vec::new()));

        let sink = Arc::clone(&delivered);
        let collect = std::panic::AssertUnwindSafe(async move {
            // One chunk of 8 covers items 0..=2 and the panic at 3.
            let mut stream = Box::pin(stream_blocking_iter(8, || {
                Ok((0..10).map(|index: usize| {
                    assert!(index < 3, "boom");
                    Ok::<usize, Error>(index)
                }))
            }));

            while let Some(item) = stream.next().await {
                sink.lock().unwrap().push(item.unwrap());
            }
        });

        let outcome = futures::FutureExt::catch_unwind(collect).await;

        assert!(outcome.is_err(), "the panic must not be swallowed");
        assert_eq!(*delivered.lock().unwrap(), vec![0, 1, 2]);
    }

    #[tokio::test]
    async fn dropping_the_stream_stops_the_producer() {
        const CHUNK: usize = 2;
        const ITEMS: usize = 10_000;
        let produced = Arc::new(AtomicUsize::new(0));

        let counter = Arc::clone(&produced);
        let mut stream = Box::pin(stream_blocking_iter(CHUNK, move || {
            Ok((0..ITEMS).map(move |index| {
                counter.fetch_add(1, Ordering::SeqCst);
                Ok::<usize, Error>(index)
            }))
        }));

        assert!(stream.next().await.is_some());
        drop(stream);

        // Wait for the count to STOP climbing, rather than assuming it already has. A chunk already
        // being decoded runs to its end on a blocking thread, so a `yield_now` on this side proves
        // nothing about where that thread has got to -- sampling twice around one yield can catch it
        // mid-chunk and fail spuriously. Polling until two samples separated by a real
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
        // not establish: it stopped *early*, rather than decoding the whole iterator for a consumer
        // that was gone. Checked on its own first so that a drop never observed at all fails with
        // that message, rather than as a bound exceeded.
        assert!(
            settled < ITEMS,
            "the producer ran to completion ({settled} items) despite the stream being dropped"
        );
        // Tighter: the first chunk and the one started ahead of it, and nothing after the drop.
        assert!(
            settled <= 2 * CHUNK,
            "{settled} items were decoded, more than the two chunks started before the drop"
        );
    }
}
