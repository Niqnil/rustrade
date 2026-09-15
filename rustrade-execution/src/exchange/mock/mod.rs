use crate::{
    UnindexedAccountEvent,
    client::mock::MockExecutionConfig,
    exchange::mock::request::{MockExchangeRequest, MockExchangeRequestKind},
    order::{Order, state::UnindexedOrderState},
};
use chrono::{DateTime, TimeDelta, Utc};
use fnv::FnvHashMap;
use futures::stream::BoxStream;
use rustrade_instrument::{
    asset::name::AssetNameExchange,
    exchange::ExchangeId,
    instrument::{Instrument, name::InstrumentNameExchange},
};
use std::fmt::Debug;
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio_stream::{StreamExt, wrappers::BroadcastStream};
use tracing::{error, info};

pub mod account;
pub mod request;
pub mod venue;

pub use venue::{CancelOutcome, OpenOutcome, SimulatedVenue, VenueOutcome};

/// Asynchronous driver for a [`SimulatedVenue`]: channels, simulated latency, emission ordering.
///
/// It owns no ledger and prices nothing. What the venue models, what it rejects and what it leaves
/// unaccounted for are documented on [`SimulatedVenue`]; this type decides only *when* the venue's
/// output reaches a client.
///
/// # Ordering
/// A filled open's account events and its response are queued together and drained by a single
/// task, so emission order equals booking order *across* fills, not merely within one. Closing
/// `request_rx` closes that queue in turn, and [`run`](Self::run) returns only once it has drained.
///
/// # ⚠️ Known limitation: fetch responses are not ordered against anything
/// Only opens go through that queue. Every `Fetch*` response is sent from a task of its own, so two
/// fetches issued together may be answered in either order, and a fetch issued after an open may be
/// answered before it. A client that reads a balance through `FetchBalances` while fills are in
/// flight can therefore observe one that is newer or older than the `BalanceSnapshot` it is about
/// to receive. Fills themselves are unaffected, and a backtest driving this venue
/// request-at-a-time never observes it; paper trading against a live feed can.
#[derive(Debug)]
pub struct MockExchange {
    /// The venue's state machine. Ledger, pricing and ordering obligations live here.
    pub venue: SimulatedVenue,
    /// Simulated round-trip delay. Transport policy, so it lives on the driver, not the venue.
    pub latency_ms: u64,
    pub request_rx: mpsc::UnboundedReceiver<MockExchangeRequest>,
    pub event_tx: broadcast::Sender<UnindexedAccountEvent>,
}

impl MockExchange {
    pub fn new(
        config: MockExecutionConfig,
        request_rx: mpsc::UnboundedReceiver<MockExchangeRequest>,
        event_tx: broadcast::Sender<UnindexedAccountEvent>,
        instruments: FnvHashMap<InstrumentNameExchange, Instrument<ExchangeId, AssetNameExchange>>,
    ) -> Self {
        Self {
            venue: SimulatedVenue::new(&config, instruments),
            latency_ms: config.latency_ms,
            request_rx,
            event_tx,
        }
    }

    /// Serves requests until the request channel closes, then lets queued fills finish.
    ///
    /// Filled opens are not emitted inline: they are queued and drained by a single task, so that
    /// emission order equals booking order *across* fills, not merely within one. Closing the
    /// request channel closes that queue in turn, and this method returns only once the queue has
    /// drained, so shutting the venue down cannot strand a fill the client is still waiting on.
    pub async fn run(mut self) {
        let (emit_tx, emit_rx) = mpsc::unbounded_channel();
        let emitter = tokio::spawn(Self::emit_queued_opens(
            emit_rx,
            self.event_tx.clone(),
            self.venue.exchange,
        ));

        while let Some(request) = self.request_rx.recv().await {
            self.advance_venue_time(request.time_request);

            match request.kind {
                MockExchangeRequestKind::FetchAccountSnapshot { response_tx } => {
                    let snapshot = self.venue.account_snapshot();
                    self.respond_with_latency(response_tx, snapshot);
                }
                MockExchangeRequestKind::FetchBalances {
                    response_tx,
                    assets,
                } => {
                    let balances = self.venue.balances(&assets);
                    self.respond_with_latency(response_tx, balances);
                }
                MockExchangeRequestKind::FetchOrdersOpen {
                    response_tx,
                    instruments,
                } => {
                    let orders_open = self.venue.orders_open(&instruments);
                    self.respond_with_latency(response_tx, orders_open);
                }
                MockExchangeRequestKind::FetchTrades {
                    response_tx,
                    time_since,
                } => {
                    let trades = self.venue.trades(time_since);
                    self.respond_with_latency(response_tx, trades);
                }
                MockExchangeRequestKind::CancelOrder {
                    response_tx,
                    request,
                } => {
                    error!(
                        exchange = %self.venue.exchange,
                        ?request,
                        "MockExchange received cancel request but only Market orders are supported"
                    );
                    let outcome = self.venue.cancel_order(request);
                    let _ = response_tx.send(outcome.response);
                }
                MockExchangeRequestKind::OpenOrder {
                    response_tx,
                    request,
                } => {
                    // The venue books the fill against its own ledger before returning, so a
                    // subsequent request on this loop already sees it.
                    let outcome = self.venue.open_order(request);
                    self.respond_open_with_latency(&emit_tx, response_tx, outcome);
                }
            }
        }

        // Nothing further will be queued, so close the queue and let the emitter finish draining
        // it. A fill booked on the final request would otherwise be dropped with the task, which is
        // precisely the truncation the drain exists to prevent.
        drop(emit_tx);
        if let Err(error) = emitter.await {
            error!(
                exchange = %self.venue.exchange,
                %error,
                "MockExchange emitter task did not shut down cleanly; queued fills may be lost"
            );
        }

        info!(exchange = %self.venue.exchange, "MockExchange shutting down");
    }

    /// Applies this driver's latency model, then advances the venue to the resulting instant.
    fn advance_venue_time(&mut self, time_request: DateTime<Utc>) {
        let client_to_exchange_latency = self.latency_ms / 2;

        let time_exchange = time_request
            .checked_add_signed(TimeDelta::milliseconds(client_to_exchange_latency as i64))
            .unwrap_or(time_request);

        self.venue.advance_time(time_exchange)
    }

    /// Sends the provided `Response` via the [`oneshot::Sender`] after waiting for the latency
    /// [`Duration`].
    ///
    /// Used to simulate network latency between the exchange and client.
    fn respond_with_latency<Response>(
        &self,
        response_tx: oneshot::Sender<Response>,
        response: Response,
    ) where
        Response: Send + 'static,
    {
        let exchange = self.venue.exchange;
        let latency = std::time::Duration::from_millis(self.latency_ms);

        tokio::spawn(async move {
            tokio::time::sleep(latency).await;
            if response_tx.send(response).is_err() {
                error!(
                    %exchange,
                    kind = std::any::type_name::<Response>(),
                    "MockExchange failed to send oneshot response to client"
                );
            }
        });
    }

    /// Queues everything one filled open owes the client, to be emitted in venue order.
    ///
    /// The venue has already decided both the events and their order; this only schedules their
    /// delivery. Putting the response last makes "the client has its response" imply "every
    /// account event for this order has already been sent".
    ///
    /// # Why a queue rather than a task per fill
    /// A balance is an **absolute snapshot**, not a delta: successive fills report `9_999_500`,
    /// then `9_999_000`, then `9_998_500`. Applying them out of order therefore does not merely
    /// reorder history, it yields the wrong balance.
    ///
    /// Each fill previously got its own [`tokio::spawn`]. Those tasks raced -- at `latency_ms: 0`
    /// they all become runnable at once -- so snapshots reached the client in arbitrary order and
    /// the last to *arrive* won. One queue, drained by one task, makes emission order equal
    /// booking order by construction.
    fn respond_open_with_latency(
        &self,
        emit_tx: &mpsc::UnboundedSender<PendingOpenEmission>,
        response_tx: oneshot::Sender<
            Order<ExchangeId, InstrumentNameExchange, UnindexedOrderState>,
        >,
        outcome: OpenOutcome,
    ) {
        // Stamped at booking time, not at emission time, so the delay each fill waits is measured
        // from when the venue booked *it*. `latency_ms` is fixed for the exchange, so these are
        // non-decreasing in queue order and the drain never sleeps away a fill's own latency twice.
        let ready_at =
            tokio::time::Instant::now() + std::time::Duration::from_millis(self.latency_ms);

        // Failure means the emitter is gone, which happens only once `run` has returned. There is
        // no client left to notify, so there is nothing to do but say so.
        if emit_tx
            .send(PendingOpenEmission {
                ready_at,
                events: outcome.events,
                response_tx,
                response: outcome.response,
            })
            .is_err()
        {
            error!(
                exchange = %self.venue.exchange,
                "MockExchange could not queue a filled open: the emitter has stopped"
            );
        }
    }

    /// Drains queued fills in order, emitting each one's account events before its response.
    ///
    /// Runs until `emit_rx` closes -- which happens when [`MockExchange::run`] returns -- and then
    /// finishes whatever is still queued, so a shutdown cannot strand a booked fill.
    async fn emit_queued_opens(
        mut emit_rx: mpsc::UnboundedReceiver<PendingOpenEmission>,
        event_tx: broadcast::Sender<UnindexedAccountEvent>,
        exchange: ExchangeId,
    ) {
        while let Some(emission) = emit_rx.recv().await {
            tokio::time::sleep_until(emission.ready_at).await;

            // Order is the venue's contract; this preserves it verbatim.
            for event in emission.events {
                if event_tx.send(event).is_err() {
                    error!(
                        %exchange,
                        "MockExchange failed to send AccountEvent notification to client"
                    );
                }
            }

            if emission.response_tx.send(emission.response).is_err() {
                error!(
                    %exchange,
                    kind = "OrderResponseOpen",
                    "MockExchange failed to send oneshot response to client"
                );
            }
        }
    }

    pub fn account_stream(&self) -> BoxStream<'static, UnindexedAccountEvent> {
        futures::StreamExt::boxed(BroadcastStream::new(self.event_tx.subscribe()).map_while(
            |result| match result {
                Ok(event) => Some(event),
                Err(error) => {
                    error!(
                        ?error,
                        "MockExchange Broadcast AccountStream lagged - terminating"
                    );
                    None
                }
            },
        ))
    }
}

/// One filled open awaiting emission, held in booking order by [`MockExchange`]'s emitter queue.
#[derive(Debug)]
struct PendingOpenEmission {
    /// When this fill's latency expires, measured from the instant the venue booked it.
    ready_at: tokio::time::Instant,
    /// The account events the fill produced, in the order the venue requires.
    events: Vec<UnindexedAccountEvent>,
    response_tx: oneshot::Sender<Order<ExchangeId, InstrumentNameExchange, UnindexedOrderState>>,
    response: Order<ExchangeId, InstrumentNameExchange, UnindexedOrderState>,
}

/// Fixtures shared by this module's driver tests and [`venue`]'s state-machine tests.
///
/// They live in the parent rather than beside either set of tests because a private item is visible
/// to descendant modules: `venue::tests` can reach `mock::fixtures`, whereas `mock::tests` could not
/// reach anything declared inside `venue`.
#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)] // Test code: panics on bad input are acceptable
pub(crate) mod fixtures {
    use crate::{
        UnindexedAccountSnapshot,
        balance::{AssetBalance, Balance},
        client::mock::MockExecutionConfig,
        fee::FeeModelConfig,
        fill::SimFillConfig,
        market::MarketSnapshot,
        order::{
            OrderEvent, OrderKey, OrderKind, TimeInForce,
            id::{ClientOrderId, StrategyId},
            request::{OrderRequestOpen, RequestOpen},
        },
    };
    use chrono::Utc;
    use fnv::FnvHashMap;
    use rust_decimal::Decimal;
    use rustrade_instrument::{
        Side, Underlying,
        asset::name::AssetNameExchange,
        exchange::ExchangeId,
        instrument::{
            Instrument,
            kind::InstrumentKind,
            name::{InstrumentNameExchange, InstrumentNameInternal},
            quote::InstrumentQuoteAsset,
        },
    };

    pub(super) const EXCHANGE: ExchangeId = ExchangeId::BinanceSpot;

    pub(super) fn d(s: &str) -> Decimal {
        s.parse().unwrap()
    }

    pub(super) fn base() -> AssetNameExchange {
        AssetNameExchange::new("BTC")
    }

    pub(super) fn quote() -> AssetNameExchange {
        AssetNameExchange::new("USDT")
    }

    pub(super) fn instrument_name() -> InstrumentNameExchange {
        InstrumentNameExchange::new("BTCUSDT")
    }

    /// A balance with nothing reserved, which is the only shape this venue can model.
    pub(super) fn funded(
        asset: AssetNameExchange,
        amount: Decimal,
    ) -> AssetBalance<AssetNameExchange> {
        AssetBalance {
            asset,
            balance: Balance::new(amount, amount),
            time_exchange: Utc::now(),
        }
    }

    /// Zero latency, so a driver's queue is exercised without any wall-clock sleeping.
    pub(super) fn config_from_balances(
        balances: Vec<AssetBalance<AssetNameExchange>>,
        fee_model: FeeModelConfig,
    ) -> MockExecutionConfig {
        MockExecutionConfig::new(
            EXCHANGE,
            UnindexedAccountSnapshot {
                exchange: EXCHANGE,
                balances,
                instruments: vec![],
            },
            0, // latency_ms
            fee_model,
            SimFillConfig::default(),
        )
    }

    /// The spot fixture: `btc` of the base asset and `usdt` of the quote, both fully free.
    pub(super) fn spot_config(
        btc: &str,
        usdt: &str,
        fee_model: FeeModelConfig,
    ) -> MockExecutionConfig {
        config_from_balances(
            vec![funded(base(), d(btc)), funded(quote(), d(usdt))],
            fee_model,
        )
    }

    pub(super) fn instruments_of(
        instrument: Instrument<ExchangeId, AssetNameExchange>,
    ) -> FnvHashMap<InstrumentNameExchange, Instrument<ExchangeId, AssetNameExchange>> {
        let mut instruments = FnvHashMap::default();
        instruments.insert(instrument.name_exchange.clone(), instrument);
        instruments
    }

    pub(super) fn spot_instruments()
    -> FnvHashMap<InstrumentNameExchange, Instrument<ExchangeId, AssetNameExchange>> {
        instruments_of(Instrument {
            exchange: EXCHANGE,
            name_internal: InstrumentNameInternal::new("btcusdt"),
            name_exchange: instrument_name(),
            underlying: Underlying {
                base: base(),
                quote: quote(),
            },
            quote: InstrumentQuoteAsset::UnderlyingQuote,
            kind: InstrumentKind::Spot,
            spec: None,
            data_venue: None,
        })
    }

    /// A Market order carrying the snapshot the venue will price it against.
    ///
    /// The snapshot is set on the request rather than passed alongside it, because
    /// [`RequestOpen::market`] is the venue's only price source and a second copy on the same path
    /// could disagree with it.
    pub(super) fn request(
        instrument: InstrumentNameExchange,
        side: Side,
        quantity: &str,
        market: Option<MarketSnapshot>,
    ) -> OrderRequestOpen<ExchangeId, InstrumentNameExchange> {
        OrderEvent {
            key: OrderKey {
                exchange: EXCHANGE,
                instrument,
                strategy: StrategyId::new("test"),
                cid: ClientOrderId::new("test-cid"),
            },
            state: RequestOpen {
                side,
                price: None, // Market orders don't have a limit price
                quantity: d(quantity),
                kind: OrderKind::Market,
                time_in_force: TimeInForce::ImmediateOrCancel,
                position_id: None,
                reduce_only: false,
                market,
            },
        }
    }

    pub(super) fn buy_request(
        quantity: &str,
        market: Option<MarketSnapshot>,
    ) -> OrderRequestOpen<ExchangeId, InstrumentNameExchange> {
        request(instrument_name(), Side::Buy, quantity, market)
    }

    pub(super) fn sell_request(
        quantity: &str,
        market: Option<MarketSnapshot>,
    ) -> OrderRequestOpen<ExchangeId, InstrumentNameExchange> {
        request(instrument_name(), Side::Sell, quantity, market)
    }

    /// A snapshot whose three prices are all `price`, wrapped as the venue receives it.
    pub(super) fn market_prices(price: &str) -> Option<MarketSnapshot> {
        let p = Some(d(price));
        Some(MarketSnapshot {
            best_bid: p,
            best_ask: p,
            last_price: p,
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::{fixtures::*, *};
    use crate::{
        AccountEventKind,
        fee::FeeModelConfig,
        order::{OrderEvent, id::ClientOrderId, request::RequestCancel, state::OrderState},
    };
    use rust_decimal::Decimal;

    /// A [`SimulatedVenue`] balance is an absolute restatement, so emission order *is* the answer.
    ///
    /// Each fill used to be emitted by its own spawned task. Those tasks raced, so the snapshots
    /// `9_999_500`, `9_999_000`, `9_998_500`, ... reached the client in arbitrary order and the
    /// last to arrive won — which is not the same thing as the last to be booked. That went
    /// unnoticed only because the timestamp each snapshot carries is derived from a clock
    /// contaminated with wall-clock time, and so happened to be unique and increasing, letting the
    /// engine discard the out-of-order ones as stale. Correcting that clock removes the accident.
    ///
    /// Asserted on the broadcast stream rather than on any downstream balance, because the ordering
    /// is the venue's contract to keep: the engine cannot repair snapshots it receives out of order.
    ///
    /// # Why this must stay multi-threaded
    /// The race is between spawned tasks. On a `current_thread` runtime they are serialised by the
    /// scheduler and this test passes against the unfixed code, detecting nothing.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn open_order_emissions_are_ordered_across_fills() {
        const FILLS: usize = 8;
        const DEBIT_PER_FILL: &str = "500";

        let usdt_start = "10000000";

        let (request_tx, request_rx) = mpsc::unbounded_channel();
        let (event_tx, mut events) = broadcast::channel(64);

        let exchange = MockExchange::new(
            spot_config("100", usdt_start, FeeModelConfig::default()),
            request_rx,
            event_tx,
            spot_instruments(),
        );

        let driver = tokio::spawn(exchange.run());

        let mut responses = Vec::with_capacity(FILLS);
        for nth in 0..FILLS {
            let (response_tx, response_rx) = oneshot::channel();
            let mut request = buy_request("0.01", market_prices("50000"));
            request.key.cid = ClientOrderId::new(format!("cid-{nth}"));

            request_tx
                .send(MockExchangeRequest::open_order(
                    Utc::now(),
                    response_tx,
                    request,
                ))
                .unwrap();
            responses.push(response_rx);
        }

        // Closing the request channel ends `run`, which drains the emitter before returning.
        drop(request_tx);
        driver.await.unwrap();

        for response in responses {
            let response = response.await.expect("every open is answered");
            assert!(
                matches!(
                    response.state,
                    OrderState::Active(_) | OrderState::Inactive(_)
                ),
                "the account is funded and the instrument priced, so every open fills: {response:?}"
            );
        }

        let balances = std::iter::from_fn(|| events.try_recv().ok())
            .filter_map(|event| match event.kind {
                AccountEventKind::BalanceSnapshot(snapshot) => Some(snapshot.0.balance.total),
                _ => None,
            })
            .collect::<Vec<_>>();

        // One balance snapshot per fill, each debiting the quote asset by the same notional.
        let expected = (1..=FILLS)
            .map(|nth| d(usdt_start) - d(DEBIT_PER_FILL) * Decimal::from(nth))
            .collect::<Vec<_>>();

        assert_eq!(
            balances, expected,
            "balance snapshots must reach the client in the order the venue booked them"
        );
    }

    /// A cancel is rejected by the venue, and the driver answers the channel it was asked on.
    ///
    /// The rejection itself is the venue's own; what this pins is that the driver
    /// forwards it rather than leaving the caller's `oneshot` to be dropped — which is what the
    /// previous, mistyped implementation did, since it could not produce the channel's response
    /// type at all.
    #[tokio::test]
    async fn a_cancel_request_is_answered_rather_than_dropped() {
        let (request_tx, request_rx) = mpsc::unbounded_channel();
        let (event_tx, _events) = broadcast::channel(16);

        let exchange = MockExchange::new(
            spot_config("100", "10000", FeeModelConfig::default()),
            request_rx,
            event_tx,
            spot_instruments(),
        );
        let driver = tokio::spawn(exchange.run());

        let (response_tx, response_rx) = oneshot::channel();
        let open = buy_request("1", market_prices("50000"));
        request_tx
            .send(MockExchangeRequest::cancel_order(
                Utc::now(),
                response_tx,
                OrderEvent {
                    key: open.key,
                    state: RequestCancel { id: None },
                },
            ))
            .unwrap();

        let response = response_rx.await.expect("a cancel must be answered");
        assert!(
            response.state.is_err(),
            "this venue rests no orders, so a cancel must be rejected: {response:?}"
        );

        drop(request_tx);
        driver.await.unwrap();
    }
}
