use crate::Unrecoverable;
use derive_more::{Constructor, Display};
use futures::{Sink, Stream};
use serde::{Deserialize, Serialize};
use std::{
    fmt::Debug,
    pin::Pin,
    task::{Context, Poll},
};
use tracing::warn;

pub trait Tx
where
    Self: Debug + Clone + Send,
{
    type Item;
    type Error: Unrecoverable + Debug;
    fn send<Item: Into<Self::Item>>(&self, item: Item) -> Result<(), Self::Error>;
}

/// Convenience type that holds the [`UnboundedTx`] and [`UnboundedRx`].
#[derive(Debug)]
pub struct Channel<T> {
    pub tx: UnboundedTx<T>,
    pub rx: UnboundedRx<T>,
}

impl<T> Channel<T> {
    /// Construct a new unbounded [`Channel`].
    pub fn new() -> Self {
        let (tx, rx) = mpsc_unbounded();
        Self { tx, rx }
    }
}

impl<T> Default for Channel<T> {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone)]
pub struct UnboundedTx<T> {
    pub tx: tokio::sync::mpsc::UnboundedSender<T>,
}

impl<T> UnboundedTx<T> {
    pub fn new(tx: tokio::sync::mpsc::UnboundedSender<T>) -> Self {
        Self { tx }
    }
}

impl<T> Tx for UnboundedTx<T>
where
    T: Debug + Clone + Send,
{
    type Item = T;
    type Error = tokio::sync::mpsc::error::SendError<T>;

    fn send<Item: Into<Self::Item>>(&self, item: Item) -> Result<(), Self::Error> {
        self.tx.send(item.into())
    }
}

impl<T> Unrecoverable for tokio::sync::mpsc::error::SendError<T> {
    fn is_unrecoverable(&self) -> bool {
        true
    }
}

impl<T> Sink<T> for UnboundedTx<T> {
    type Error = tokio::sync::mpsc::error::SendError<T>;

    fn poll_ready(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // UnboundedTx is always ready
        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, item: T) -> Result<(), Self::Error> {
        self.tx.send(item)
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // UnboundedTx does not buffer, so no flushing is required
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // UnboundedTx requires no closing logic
        Poll::Ready(Ok(()))
    }
}

#[derive(Debug, Constructor)]
pub struct UnboundedRx<T> {
    pub rx: tokio::sync::mpsc::UnboundedReceiver<T>,
}

/// Blocking iteration: `next` returns a waiting message at once, and otherwise blocks the thread
/// until one arrives, or returns `None` once every sender is dropped and the channel is drained.
///
/// # Panics
/// `next` panics if it has to wait while called from an asynchronous context, as
/// [`UnboundedReceiver::blocking_recv`](tokio::sync::mpsc::UnboundedReceiver::blocking_recv)
/// does. Iterate on a thread of its own, such as one from `spawn_blocking`, or use the
/// [`Stream`] impl instead.
impl<T> Iterator for UnboundedRx<T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        match self.rx.try_recv() {
            Ok(event) => Some(event),
            // Parks the thread rather than polling `try_recv` again, which would spin a core
            // for as long as the channel stays empty.
            Err(tokio::sync::mpsc::error::TryRecvError::Empty) => self.rx.blocking_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => None,
        }
    }
}

impl<T> UnboundedRx<T> {
    /// Number of messages waiting in the channel.
    ///
    /// Approximate while senders run: a snapshot that may be stale by the time it is read.
    pub fn len(&self) -> usize {
        self.rx.len()
    }

    /// Whether no message waits in the channel. Approximate, like [`len`](Self::len).
    pub fn is_empty(&self) -> bool {
        self.rx.is_empty()
    }

    pub fn into_stream(self) -> tokio_stream::wrappers::UnboundedReceiverStream<T> {
        tokio_stream::wrappers::UnboundedReceiverStream::new(self.rx)
    }
}

impl<T> Stream for UnboundedRx<T> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.rx.poll_recv(cx)
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Deserialize, Serialize)]
pub struct ChannelTxDroppable<ChannelTx> {
    pub state: ChannelState<ChannelTx>,
}

impl<ChannelTx> ChannelTxDroppable<ChannelTx> {
    pub fn new(tx: ChannelTx) -> Self {
        Self {
            state: ChannelState::Active(tx),
        }
    }

    pub fn new_disabled() -> Self {
        Self {
            state: ChannelState::Disabled,
        }
    }

    pub fn disable(&mut self) {
        self.state = ChannelState::Disabled
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Deserialize, Serialize, Display)]
pub enum ChannelState<Tx> {
    Active(Tx),
    Disabled,
}

impl<ChannelTx> ChannelTxDroppable<ChannelTx>
where
    ChannelTx: Tx,
{
    pub fn send(&mut self, item: ChannelTx::Item) {
        let ChannelState::Active(tx) = &self.state else {
            return;
        };

        if tx.send(item).is_err() {
            let name = std::any::type_name::<ChannelTx::Item>();
            warn!(
                name,
                "ChannelTxDroppable receiver dropped - items will no longer be sent"
            );
            self.state = ChannelState::Disabled
        }
    }
}
pub fn mpsc_unbounded<T>() -> (UnboundedTx<T>, UnboundedRx<T>) {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    (UnboundedTx::new(tx), UnboundedRx::new(rx))
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panicking on a failed send is the assertion
mod tests {
    use super::*;

    #[test]
    fn len_counts_messages_waiting_in_the_channel() {
        let (tx, mut rx) = mpsc_unbounded::<u8>();
        assert!(rx.is_empty());

        tx.send(1).unwrap();
        tx.send(2).unwrap();
        assert_eq!(rx.len(), 2);

        assert_eq!(Iterator::next(&mut rx), Some(1));
        assert_eq!(rx.len(), 1);
        assert!(!rx.is_empty());
    }

    #[test]
    fn next_waits_for_a_message_on_an_empty_channel() {
        let (tx, mut rx) = mpsc_unbounded::<u8>();

        let sender = std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(50));
            tx.send(7).unwrap();
            // `tx` drops here, disconnecting the channel once 7 is taken.
        });

        assert_eq!(Iterator::next(&mut rx), Some(7));
        assert_eq!(Iterator::next(&mut rx), None);
        sender.join().unwrap();
    }

    // A spinning `next` would hang here instead of panicking.
    #[tokio::test]
    #[should_panic(expected = "Cannot block the current thread")]
    async fn next_panics_if_it_must_wait_inside_an_async_context() {
        let (_tx, mut rx) = mpsc_unbounded::<u8>();
        let _ = Iterator::next(&mut rx);
    }

    #[test]
    fn next_drains_waiting_messages_after_the_senders_are_dropped() {
        let (tx, mut rx) = mpsc_unbounded::<u8>();
        tx.send(1).unwrap();
        drop(tx);

        assert_eq!(Iterator::next(&mut rx), Some(1));
        assert_eq!(Iterator::next(&mut rx), None);
    }
}
