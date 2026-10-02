//! The feed is unbounded, so a live venue's read loop never waits on the `Engine`. The cost is
//! that an `Engine` slower than its inputs falls behind silently. [`FeedDepth`] makes that
//! observable without changing the delivery: what to do about a deep feed is the caller's policy.

use futures::Stream;
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
/// [`EngineContext::time`](crate::engine::audit::context::EngineContext) with the processed
/// event's receive time (`MarketEvent::time_received` for market data). A caller driving
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
/// The inner channel is private, so every send is counted.
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

    #[test]
    fn a_failed_send_is_not_counted() {
        let (tx, rx) = feed::<u8>();
        drop(rx);

        assert!(tx.send(1).is_err());
        assert_eq!(tx.depth().current(), 0);
    }
}
