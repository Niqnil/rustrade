//! The feed is unbounded, so a live venue's read loop never waits on the `Engine`. The cost is
//! that an `Engine` slower than its inputs falls behind silently. [`FeedDepth`] makes that
//! observable without changing the delivery: what to do about a deep feed is the caller's policy.

use futures::{Sink, Stream};
use rustrade_integration::channel::{Tx, UnboundedRx, UnboundedTx, mpsc_unbounded};
use std::{
    fmt::Debug,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
};
use tokio::sync::mpsc::error::SendError;

/// Construct the engine feed: a [`FeedTx`] and the [`FeedRx`] the `Engine` consumes, sharing one
/// [`FeedDepth`].
pub fn feed<Event>() -> (FeedTx<Event>, FeedRx<Event>) {
    let (tx, rx) = mpsc_unbounded();
    let depth = FeedDepth::default();
    (
        FeedTx {
            tx,
            depth: depth.clone(),
        },
        FeedRx { rx, depth },
    )
}

/// Cloneable handle reading how many events wait in the engine feed.
///
/// A count, not an age. To see how far behind the `Engine` is in time, compare each audit tick's
/// [`EngineContext::time`](crate::engine::audit::context::EngineContext::time) with the processed
/// event's receive time. A market event carries one (`MarketEvent::time_received`); an account
/// event or a command does not. A caller driving
/// [`Engine::new`](crate::engine::Engine::new) directly feeds the events itself, so it already
/// knows its own queue.
///
/// A clone outlives the [`System`](super::System) it came from. Once the `Engine` stops, the value
/// no longer changes: events still queued are dropped with the feed, not counted out.
#[derive(Debug, Clone, Default)]
pub struct FeedDepth(Arc<AtomicUsize>);

impl FeedDepth {
    /// Events sent into the feed that the `Engine` has not yet taken.
    ///
    /// Approximate while senders and the `Engine` run: it is a snapshot that may be stale by the
    /// time it is read. An event the `Engine` is processing has already been taken, so it is not
    /// counted.
    pub fn current(&self) -> usize {
        self.0.load(Ordering::Relaxed)
    }
}

/// Transmitter into the engine feed, counting each event into its [`FeedDepth`].
///
/// Sends through either [`Tx`] or [`Sink`] are counted. The inner channel is private, so no send
/// bypasses the count.
#[derive(Debug, Clone)]
pub struct FeedTx<Event> {
    tx: UnboundedTx<Event>,
    depth: FeedDepth,
}

impl<Event> FeedTx<Event> {
    /// The handle reading this feed's depth.
    pub fn depth(&self) -> FeedDepth {
        self.depth.clone()
    }
}

impl<Event> Tx for FeedTx<Event>
where
    Event: Debug + Clone + Send,
{
    type Item = Event;
    type Error = SendError<Event>;

    fn send<Item: Into<Self::Item>>(&self, item: Item) -> Result<(), Self::Error> {
        // Counted before the send: the `Engine` can take the event the moment it is sent, and the
        // channel orders this increment before that decrement, so the count never wraps below zero.
        self.depth.0.fetch_add(1, Ordering::Relaxed);
        self.tx.send(item).inspect_err(|_| {
            self.depth.0.fetch_sub(1, Ordering::Relaxed);
        })
    }
}

impl<Event> Sink<Event> for FeedTx<Event> {
    type Error = SendError<Event>;

    fn poll_ready(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // Unbounded, so always ready.
        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, item: Event) -> Result<(), Self::Error> {
        // Same counting as `Tx::send`, which needs bounds this impl does not.
        self.depth.0.fetch_add(1, Ordering::Relaxed);
        self.tx.tx.send(item).inspect_err(|_| {
            self.depth.0.fetch_sub(1, Ordering::Relaxed);
        })
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // Nothing is buffered.
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
}

/// The `Engine`'s end of the feed, counting each event it yields out of its [`FeedDepth`].
///
/// Implements both [`Iterator`], for [`EngineFeedMode::Iterator`](super::builder::EngineFeedMode),
/// and [`Stream`], for [`EngineFeedMode::Stream`](super::builder::EngineFeedMode).
#[derive(Debug)]
pub struct FeedRx<Event> {
    rx: UnboundedRx<Event>,
    depth: FeedDepth,
}

impl<Event> FeedRx<Event> {
    fn taken(&self, event: Option<Event>) -> Option<Event> {
        if event.is_some() {
            self.depth.0.fetch_sub(1, Ordering::Relaxed);
        }
        event
    }
}

impl<Event> Iterator for FeedRx<Event> {
    type Item = Event;

    fn next(&mut self) -> Option<Self::Item> {
        let event = self.rx.next();
        self.taken(event)
    }
}

impl<Event> Stream for FeedRx<Event> {
    type Item = Event;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.rx)
            .poll_next(cx)
            .map(|event| self.taken(event))
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panicking on a failed send is the assertion
mod tests {
    use super::*;
    use futures::StreamExt;

    #[test]
    fn depth_counts_events_sent_and_not_yet_taken() {
        let (tx, mut rx) = feed::<u8>();
        let depth = tx.depth();
        assert_eq!(depth.current(), 0);

        tx.send(1).unwrap();
        tx.clone().send(2).unwrap();
        assert_eq!(depth.current(), 2);

        assert_eq!(Iterator::next(&mut rx), Some(1));
        assert_eq!(depth.current(), 1);
        assert_eq!(Iterator::next(&mut rx), Some(2));
        assert_eq!(depth.current(), 0);
    }

    #[tokio::test]
    async fn the_stream_end_counts_out_like_the_iterator_end() {
        let (tx, mut rx) = feed::<u8>();
        let depth = tx.depth();

        tx.send(1).unwrap();
        tx.send(2).unwrap();
        assert_eq!(StreamExt::next(&mut rx).await, Some(1));
        assert_eq!(depth.current(), 1);

        drop(tx);
        assert_eq!(StreamExt::next(&mut rx).await, Some(2));
        assert_eq!(StreamExt::next(&mut rx).await, None);
        assert_eq!(depth.current(), 0);
    }

    #[tokio::test]
    async fn a_sink_send_is_counted_like_a_tx_send() {
        use futures::SinkExt;

        let (mut tx, mut rx) = feed::<u8>();
        let depth = tx.depth();

        SinkExt::send(&mut tx, 1).await.unwrap();
        assert_eq!(depth.current(), 1);
        assert_eq!(StreamExt::next(&mut rx).await, Some(1));
        assert_eq!(depth.current(), 0);
    }

    /// Many senders on other threads while the receiver drains: the count never wraps below zero
    /// and returns to exactly zero once everything sent has been taken.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_senders_and_a_draining_receiver_settle_at_zero() {
        const SENDERS: usize = 4;
        const SENDS: usize = 10_000;

        let (tx, mut rx) = feed::<usize>();
        let depth = tx.depth();

        let senders: Vec<_> = (0..SENDERS)
            .map(|_| {
                let tx = tx.clone();
                tokio::spawn(async move {
                    for index in 0..SENDS {
                        Tx::send(&tx, index).unwrap();
                    }
                })
            })
            .collect();
        drop(tx);

        let mut taken = 0;
        while StreamExt::next(&mut rx).await.is_some() {
            taken += 1;
            // A wrap below zero would read as a value near `usize::MAX`.
            assert!(depth.current() <= SENDERS * SENDS);
        }
        for sender in senders {
            sender.await.unwrap();
        }

        assert_eq!(taken, SENDERS * SENDS);
        assert_eq!(depth.current(), 0);
    }

    #[test]
    fn a_failed_send_is_not_counted() {
        let (tx, rx) = feed::<u8>();
        drop(rx);

        assert!(tx.send(1).is_err());
        assert_eq!(tx.depth().current(), 0);
    }

    #[tokio::test]
    async fn a_failed_sink_send_is_not_counted() {
        use futures::SinkExt;

        let (mut tx, rx) = feed::<u8>();
        drop(rx);

        assert!(SinkExt::send(&mut tx, 1).await.is_err());
        assert_eq!(tx.depth().current(), 0);
    }
}
