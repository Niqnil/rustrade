//! Paper trading: the simulated venue a backtest uses, run on a live market and a live clock.
//!
//! [`PaperMarketFeed::tap`] wraps the market stream the `Engine` is fed, and
//! [`ExecutionBuilder::add_paper`](crate::execution::builder::ExecutionBuilder::add_paper) adds a
//! paper venue that trades on what the tap observes. Resting limit orders fill as the market
//! crosses them, and spot margin, interest, fees and the stale-market policies behave as they do in
//! a backtest of the same configuration.

use crate::{
    engine::state::position::EngineCheckpoint,
    execution::sim::{SimVenueOptions, market::VenueMarketUpdate},
};
use fnv::FnvHashMap;
use futures::Stream;
use rustrade_data::streams::{consumer::MarketStreamEvent, reconnect};
use rustrade_execution::{
    borrow_capacity::BorrowCapacity,
    client::mock::{request_borrow_capacity, request_checkpoint},
    error::{ConnectivityError, UnindexedClientError},
    exchange::mock::{MockExchangeClock, request::MockExchangeRequest},
    market::MarketObservation,
};
use rustrade_instrument::{
    Side,
    exchange::ExchangeId,
    instrument::{InstrumentIndex, name::InstrumentNameExchange},
};
use std::{
    pin::Pin,
    task::{Context, Poll, ready},
};
use tokio::sync::{broadcast, mpsc};

/// The market of one instrument, as a [`PaperMarketTap`] has folded it from the events seen so
/// far.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PaperMarketUpdate {
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
    /// The fold runs inline in the task that polls the returned stream, ahead of the `Engine`.
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
    /// The checkpoint to restore the venue from, or `None` to start it from the configuration's
    /// `initial_state`. See [`with_checkpoint`](Self::with_checkpoint).
    pub checkpoint: Option<EngineCheckpoint>,
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

    /// These options, restoring the venue from `checkpoint`'s venue half, so it carries on where
    /// the venue that took it stopped, rather than starting from the configuration's
    /// `initial_state`.
    ///
    /// Restore the `Engine`'s positions from the same checkpoint, with
    /// [`EngineStateBuilder::restored_positions`](crate::engine::state::builder::EngineStateBuilder::restored_positions),
    /// so the two agree. The venue is given the same configuration and [`venue`](Self::venue)
    /// models as before: the checkpoint holds state, not configuration. See
    /// [`SimulatedVenueSnapshot`](rustrade_execution::exchange::mock::venue::SimulatedVenueSnapshot)
    /// for what is restored and what happens to the time the process was down.
    #[must_use]
    pub fn with_checkpoint(mut self, checkpoint: EngineCheckpoint) -> Self {
        self.checkpoint = Some(checkpoint);
        self
    }
}

impl Default for PaperOptions {
    fn default() -> Self {
        Self {
            venue: SimVenueOptions::default(),
            account_stream_capacity: Self::DEFAULT_ACCOUNT_STREAM_CAPACITY,
            checkpoint: None,
        }
    }
}

/// A handle to a paper venue, for asking it what an account query on a live venue asks, outside
/// the `Engine`'s order flow.
///
/// [`ExecutionBuilder::build`](crate::execution::builder::ExecutionBuilder::build) returns one for
/// each venue [`add_paper`](crate::execution::builder::ExecutionBuilder::add_paper) added, in
/// [`ExecutionBuild::paper_venues`](crate::execution::builder::ExecutionBuild::paper_venues), which
/// [`Execution`](crate::execution::Execution) carries on. Cheap to clone; every clone asks the same
/// venue.
///
/// It places and cancels nothing: orders go through the `Engine`, which tracks them.
///
/// # Ordering
/// A query travels on the same request channel as the orders the venue's `ExecutionManager` sends
/// it, after the same latency, and is answered from the venue's state when it arrives. An order the
/// `Engine` sent just before the query may not have reached the manager's channel yet, so the
/// answer can predate it.
///
/// # Before and after the venue runs
/// The handle never keeps a venue running: it holds the venue's request channel weakly, so a
/// system shuts down as it would without it. A query sent before
/// [`ExecutionBuild::init`](crate::execution::builder::ExecutionBuild::init) starts the venue
/// waits for it. Once the venue's `ExecutionManager` and the venue have stopped, or if their build
/// was dropped without being started, a query fails with [`ConnectivityError::ExchangeOffline`].
#[derive(Clone)]
pub struct PaperVenue {
    exchange: ExchangeId,
    request_tx: mpsc::WeakUnboundedSender<MockExchangeRequest>,
    clock: MockExchangeClock,
}

impl PaperVenue {
    pub(crate) fn new(
        exchange: ExchangeId,
        request_tx: &mpsc::UnboundedSender<MockExchangeRequest>,
        clock: MockExchangeClock,
    ) -> Self {
        Self {
            exchange,
            request_tx: request_tx.downgrade(),
            clock,
        }
    }

    /// The exchange the venue is added for.
    pub fn exchange(&self) -> ExchangeId {
        self.exchange
    }

    /// How much the paper account can borrow now for an order on `instrument` on `side`, as
    /// [`BorrowCapacityClient::fetch_borrow_capacity`](rustrade_execution::client::BorrowCapacityClient::fetch_borrow_capacity)
    /// asks a live margin venue.
    ///
    /// Answered by the simulated venue from its own ledger, its lending rules and the market it has
    /// observed, as
    /// [`SimulatedVenue::borrow_capacity`](rustrade_execution::exchange::mock::SimulatedVenue::borrow_capacity)
    /// is in a backtest; see there for when it answers `None` or an error. **Advisory**, as on a
    /// live venue: nothing is reserved, so an order sized to it can still be refused.
    ///
    /// # Errors
    /// As [`request_borrow_capacity`] says. A venue configured without spot margin lends against no
    /// instrument, so answers
    /// [`ApiError::InstrumentInvalid`](rustrade_execution::error::ApiError::InstrumentInvalid) for
    /// every one.
    pub async fn fetch_borrow_capacity(
        &self,
        instrument: &InstrumentNameExchange,
        side: Side,
    ) -> Result<BorrowCapacity, UnindexedClientError> {
        let Some(request_tx) = self.request_tx.upgrade() else {
            return Err(UnindexedClientError::Connectivity(
                ConnectivityError::ExchangeOffline(self.exchange),
            ));
        };
        request_borrow_capacity(&request_tx, self.exchange, (self.clock)(), instrument, side).await
    }
}

impl PaperVenue {
    /// Asks the venue for a checkpoint: it sends its state on its account stream, after every
    /// account event it sent before, and the `Engine` answers it with an
    /// [`EngineOutput::Checkpoint`](crate::engine::EngineOutput::Checkpoint) holding that state
    /// and the engine's positions at the same point. Keep that output to restore from, with
    /// [`PaperOptions::with_checkpoint`].
    ///
    /// Returns the checkpoint's [`id`](EngineCheckpoint::id) once the venue has sent it, so the
    /// output answering this call can be told from an earlier one. When the venue takes it is
    /// ordered as a query is: see `# Ordering` on this type.
    ///
    /// Sent is not processed. If the venue's account stream ends or lags before the checkpoint
    /// reaches the `Engine`, as it does when the manager reopens the stream, no output answers
    /// it: ask again once the stream is back.
    ///
    /// # Errors
    /// [`ConnectivityError::ExchangeOffline`] once the venue has stopped, as for
    /// [`fetch_borrow_capacity`](Self::fetch_borrow_capacity).
    pub async fn checkpoint(&self) -> Result<u64, UnindexedClientError> {
        let Some(request_tx) = self.request_tx.upgrade() else {
            return Err(UnindexedClientError::Connectivity(
                ConnectivityError::ExchangeOffline(self.exchange),
            ));
        };
        request_checkpoint(&request_tx, self.exchange, (self.clock)()).await
    }
}

impl std::fmt::Debug for PaperVenue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The clock is a closure, which has no `Debug`.
        f.debug_struct("PaperVenue")
            .field("exchange", &self.exchange)
            .finish_non_exhaustive()
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

    /// A venue handle's `Debug` names its exchange, and leaves out the clock, which has none.
    #[test]
    fn a_paper_venue_debugs_as_its_exchange() {
        let (request_tx, _request_rx) = tokio::sync::mpsc::unbounded_channel();
        let clock: MockExchangeClock =
            std::sync::Arc::new(|| DateTime::<Utc>::from_timestamp(0, 0).unwrap());
        let venue = PaperVenue::new(ExchangeId::BinanceSpot, &request_tx, clock);

        assert_eq!(
            format!("{venue:?}"),
            "PaperVenue { exchange: BinanceSpot, .. }"
        );
    }
}
