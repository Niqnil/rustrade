//! Paper trading: the simulated venue a backtest uses, run on a live market and a live clock.
//!
//! [`PaperMarketFeed::tap`] wraps the market stream the `Engine` is fed, and
//! [`ExecutionBuilder::add_paper`](crate::execution::builder::ExecutionBuilder::add_paper) adds a
//! paper venue that trades on what the tap observes. Resting limit orders fill as the market
//! crosses them, and spot margin, interest, fees and the stale-market policies behave as they do in
//! a backtest of the same configuration.

use crate::execution::sim::{SimVenueOptions, market::VenueMarketUpdate};
use fnv::FnvHashMap;
use futures::Stream;
use rustrade_data::streams::{consumer::MarketStreamEvent, reconnect};
use rustrade_execution::market::MarketObservation;
use rustrade_instrument::instrument::InstrumentIndex;
use std::{
    pin::Pin,
    task::{Context, Poll, ready},
};
use tokio::sync::broadcast;

/// The market of one instrument, as a [`PaperMarketTap`] has folded it from the events seen so
/// far.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PaperMarketUpdate {
    pub instrument: InstrumentIndex,
    pub observation: MarketObservation,
}

/// The market a [`PaperMarketTap`] observes, for paper venues to subscribe to.
///
/// Made by [`tap`](Self::tap), and passed to
/// [`ExecutionBuilder::add_paper`](crate::execution::builder::ExecutionBuilder::add_paper), which
/// subscribes a venue to it. A venue sees only what the tap observes after it subscribed.
///
/// The feed ends only when the tap and every clone of this handle are dropped. A venue then keeps
/// serving requests on the last market it saw.
///
/// # A venue that falls behind
/// The feed holds a bounded number of updates for its subscribers. A venue that falls further
/// behind than that misses the oldest, which ends its account stream: see
/// [`MockExchange::new_market_driven`](rustrade_execution::exchange::mock::MockExchange::new_market_driven).
/// Booking an observation is in-memory work, so this needs a feed far faster than the venue, or a
/// venue starved of the CPU.
#[derive(Debug, Clone)]
pub struct PaperMarketFeed {
    tx: broadcast::Sender<PaperMarketUpdate>,
}

impl PaperMarketFeed {
    /// How many updates [`tap`](Self::tap) holds for a subscriber that has not yet read them.
    pub const DEFAULT_CAPACITY: usize = 8192;

    /// Wraps `stream`, returning a stream that yields exactly what `stream` does, and a feed of
    /// the market it observes.
    ///
    /// Feed the returned stream to the `Engine` in place of `stream`. Each market event it yields
    /// is first folded into the market of its instrument, with the same
    /// [`VenueMarketUpdate`] fold a backtest's venue uses, and that market is sent to every
    /// subscribed venue. So a venue has observed an event no later than the `Engine` sees it,
    /// and an event the stream does not yield, because nothing polls it, reaches no venue either.
    ///
    /// A [`Reconnecting`](reconnect::Event::Reconnecting) event is passed on and sends nothing. A
    /// venue sees no market while the stream reconnects, so a price that crossed a resting order
    /// during the gap fills nothing.
    ///
    /// Holds [`DEFAULT_CAPACITY`](Self::DEFAULT_CAPACITY) updates per subscriber; see
    /// [`tap_with_capacity`](Self::tap_with_capacity).
    pub fn tap<MarketStream, Kind>(
        stream: MarketStream,
    ) -> (PaperMarketTap<MarketStream, Kind>, Self)
    where
        MarketStream: Stream<Item = MarketStreamEvent<InstrumentIndex, Kind>>,
        Kind: VenueMarketUpdate,
    {
        Self::tap_with_capacity(stream, Self::DEFAULT_CAPACITY)
    }

    /// [`tap`](Self::tap), holding `capacity` updates for a subscriber that has not yet read them.
    ///
    /// # Panics
    /// If `capacity` is zero.
    pub fn tap_with_capacity<MarketStream, Kind>(
        stream: MarketStream,
        capacity: usize,
    ) -> (PaperMarketTap<MarketStream, Kind>, Self)
    where
        MarketStream: Stream<Item = MarketStreamEvent<InstrumentIndex, Kind>>,
        Kind: VenueMarketUpdate,
    {
        let (tx, _) = broadcast::channel(capacity);
        let tap = PaperMarketTap {
            stream,
            market: FnvHashMap::default(),
            tx: tx.clone(),
        };
        (tap, Self { tx })
    }

    /// A receiver of every update the tap observes from now on.
    pub(crate) fn subscribe(&self) -> broadcast::Receiver<PaperMarketUpdate> {
        self.tx.subscribe()
    }
}

/// A market stream that also sends the market it observes to a [`PaperMarketFeed`].
///
/// Made by [`PaperMarketFeed::tap`], which describes it.
#[pin_project::pin_project]
pub struct PaperMarketTap<MarketStream, Kind>
where
    Kind: VenueMarketUpdate,
{
    #[pin]
    stream: MarketStream,
    market: FnvHashMap<InstrumentIndex, Kind::State>,
    tx: broadcast::Sender<PaperMarketUpdate>,
}

/// Manual because the wrapped stream need not be `Debug`, and the fold state says nothing a reader
/// needs beyond how many instruments it holds.
impl<MarketStream, Kind> std::fmt::Debug for PaperMarketTap<MarketStream, Kind>
where
    Kind: VenueMarketUpdate,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PaperMarketTap")
            .field("instruments", &self.market.len())
            .field("subscribers", &self.tx.receiver_count())
            .finish_non_exhaustive()
    }
}

impl<MarketStream, Kind> Stream for PaperMarketTap<MarketStream, Kind>
where
    MarketStream: Stream<Item = MarketStreamEvent<InstrumentIndex, Kind>>,
    Kind: VenueMarketUpdate,
{
    type Item = MarketStreamEvent<InstrumentIndex, Kind>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        let item = ready!(this.stream.poll_next(cx));

        if let Some(reconnect::Event::Item(event)) = &item {
            let state = this.market.entry(event.instrument).or_default();
            // Asked before the fold, which is the only point at which the state can say whether
            // the bar is new, or whether it takes the event at all: as `SimRunner` routes it.
            let bar = Kind::bar(state, event);
            let fresh = Kind::fresh(state, event);
            Kind::apply(state, event);
            let observation = MarketObservation::new(Kind::snapshot(state))
                .with_depth(Kind::depth(state))
                .with_bar(bar)
                .with_fresh(fresh);

            // An error means no venue is subscribed, which leaves nobody to tell.
            let _ = this.tx.send(PaperMarketUpdate {
                instrument: event.instrument,
                observation,
            });
        }

        Poll::Ready(item)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.stream.size_hint()
    }
}

/// What a paper venue models beyond its
/// [`MockExecutionConfig`](rustrade_execution::client::mock::MockExecutionConfig), and how its
/// account stream is buffered.
///
/// Passed to [`ExecutionBuilder::add_paper`](crate::execution::builder::ExecutionBuilder::add_paper).
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct PaperOptions {
    /// What the venue models beyond its configuration, exactly as in a backtest: see
    /// [`SimVenueOptions`].
    pub venue: SimVenueOptions,
    /// How many account events the venue holds for its manager before the account stream lags and
    /// ends. Must be at least one.
    ///
    /// Defaults to [`DEFAULT_ACCOUNT_STREAM_CAPACITY`](Self::DEFAULT_ACCOUNT_STREAM_CAPACITY). An
    /// ended stream is reopened and the fills it lost are read back, so this bounds memory, not
    /// correctness.
    pub account_stream_capacity: usize,
}

impl PaperOptions {
    /// The default [`account_stream_capacity`](Self::account_stream_capacity).
    pub const DEFAULT_ACCOUNT_STREAM_CAPACITY: usize = 4096;

    /// These options, with the venue modelling what `venue` adds to it.
    #[must_use]
    pub fn with_venue(mut self, venue: SimVenueOptions) -> Self {
        self.venue = venue;
        self
    }

    /// These options, holding `capacity` account events.
    #[must_use]
    pub fn with_account_stream_capacity(mut self, capacity: usize) -> Self {
        self.account_stream_capacity = capacity;
        self
    }
}

impl Default for PaperOptions {
    fn default() -> Self {
        Self {
            venue: SimVenueOptions::default(),
            account_stream_capacity: Self::DEFAULT_ACCOUNT_STREAM_CAPACITY,
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use chrono::{DateTime, Utc};
    use futures::StreamExt;
    use rust_decimal_macros::dec;
    use rustrade_data::{
        event::{DataKind, MarketEvent},
        subscription::trade::PublicTrade,
    };
    use rustrade_execution::market::MarketSnapshot;
    use rustrade_instrument::exchange::ExchangeId;

    fn trade(
        instrument: usize,
        price: rust_decimal::Decimal,
    ) -> MarketStreamEvent<InstrumentIndex, DataKind> {
        let time = DateTime::<Utc>::from_timestamp(1_700_000_000, 0).unwrap();
        reconnect::Event::Item(MarketEvent {
            time_exchange: time,
            time_received: time,
            exchange: ExchangeId::BinanceSpot,
            instrument: InstrumentIndex(instrument),
            kind: DataKind::Trade(PublicTrade {
                id: "1".into(),
                price,
                amount: dec!(1),
                side: None,
            }),
        })
    }

    /// The tap yields what it wraps unchanged, sends each instrument's folded market, and sends
    /// nothing for a reconnection.
    #[tokio::test]
    async fn the_tap_passes_events_through_and_sends_each_instruments_market() {
        let events = vec![
            trade(0, dec!(100)),
            reconnect::Event::Reconnecting(ExchangeId::BinanceSpot),
            trade(1, dec!(7)),
        ];
        let (tap, feed) = PaperMarketFeed::tap(futures::stream::iter(events.clone()));
        let mut updates = feed.subscribe();

        assert_eq!(tap.collect::<Vec<_>>().await, events);

        let traded = |price| {
            MarketObservation::new(MarketSnapshot {
                best_bid: None,
                best_ask: None,
                last_price: Some(price),
            })
        };
        assert_eq!(
            updates.try_recv().unwrap(),
            PaperMarketUpdate {
                instrument: InstrumentIndex(0),
                observation: traded(dec!(100)),
            }
        );
        assert_eq!(
            updates.try_recv().unwrap(),
            PaperMarketUpdate {
                instrument: InstrumentIndex(1),
                observation: traded(dec!(7)),
            }
        );
        assert!(updates.try_recv().is_err(), "a reconnection sends nothing");
    }

    #[test]
    fn a_tap_reports_the_size_of_the_stream_it_wraps_and_names_itself() {
        let (tap, _feed) = PaperMarketFeed::tap(futures::stream::iter(vec![
            trade(0, dec!(1)),
            trade(0, dec!(2)),
            trade(0, dec!(3)),
        ]));
        assert_eq!(tap.size_hint(), (3, Some(3)));
        assert!(format!("{tap:?}").contains("PaperMarketTap"));
    }
}
