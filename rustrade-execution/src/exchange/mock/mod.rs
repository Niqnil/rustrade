use crate::{
    UnindexedAccountEvent,
    client::mock::MockExecutionConfig,
    exchange::mock::{
        request::{MockExchangeRequest, MockExchangeRequestKind},
    },
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


/// Simulated exchange: fills every market order immediately and keeps its own balance ledger.
///
/// # Which [`InstrumentKind`]s it accounts for
/// [`Spot`](InstrumentKind::Spot) and [`Cfd`](InstrumentKind::Cfd). A spot fill exchanges the two
/// underlying assets; a CFD fill is cash-settled against the quote asset in both directions, since
/// there is nothing to deliver and a short is a margin position rather than a stock loan. Both carry
/// the instrument's `contract_size` into the notional and the fee, so this ledger and the engine's
/// position accounting cannot disagree by that multiplier.
///
/// [`Perpetual`](InstrumentKind::Perpetual), [`Future`](InstrumentKind::Future) and
/// [`Option`](InstrumentKind::Option) need funding, margin and expiry settlement, none of which this
/// mock models. They are **rejected at [`open_order`](Self::open_order)** with
/// [`ApiError::InstrumentInvalid`] rather than filled. The check lives here, not only in the
/// `rustrade` builder, because this type and its `instruments` map are public: a consumer that
/// constructs one directly bypasses every upstream gate, and the alternative to rejecting is filling
/// a derivative as if it were deliverable stock.
///
/// # ⚠️ Caller obligations and known limitations
/// - **Fund the quote asset of every instrument traded.** Every debit is quote-denominated except a
///   spot sell, which debits base. A missing balance is a **panic**, not an error: the balances are
///   this mock's own fixture, so an absent one is a mis-specified test rather than a runtime
///   condition. A CFD settling in an account currency that is not the quote asset still needs the
///   **quote** asset funded — see below.
/// - **`CfdContract::settlement_asset` is not settled in.** A CFD routinely cash-settles in an
///   account currency that is not the quote asset (a GBP account trading a USD-quoted index), which
///   requires a quote→settlement conversion rate. This mock has no rate source and will not invent
///   one, so it debits and credits the quote asset and leaves the currency dimension unmodelled. The
///   field is carried on the instrument so its description stays faithful, and the engine's own
///   `InstrumentKind` — not this copy — drives PnL. A backtest whose result depends on the
///   settlement currency needs a real execution client.
/// - **The ledger debits the paying asset and does not credit the received one.** Pre-existing: a
///   spot buy debits quote without crediting base, and a spot sell the reverse. Balances therefore
///   track cash committed, not portfolio value; position-derived statistics come from the engine.
/// - **Only [`OrderKind::Market`] is accepted**; every other kind is rejected.
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
        outcome: crate::exchange::mock::venue::OpenOutcome,
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

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use crate::{
        UnindexedAccountSnapshot,
        balance::{AssetBalance, Balance},
        error::ApiError,
        fee::{FeeModelConfig, PercentageFeeModel},
        fill::{BidAskFillModel, SimFillConfig},
        order::{
            OrderEvent, OrderKey, OrderKind, TimeInForce,
            id::{ClientOrderId, StrategyId},
            request::RequestOpen,
            state::InactiveOrderState,
        },
    };
    use chrono::Utc;
    use rust_decimal::Decimal;
    use rustrade_instrument::{
        Side, Underlying,
        asset::name::AssetNameExchange,
        exchange::ExchangeId,
        instrument::{
            Instrument,
            kind::{InstrumentKind, cfd::CfdContract},
            name::{InstrumentNameExchange, InstrumentNameInternal},
            quote::InstrumentQuoteAsset,
        },
    };
    use tokio::sync::{broadcast, mpsc};

    fn d(s: &str) -> Decimal {
        s.parse().unwrap()
    }

    const EXCHANGE: ExchangeId = ExchangeId::BinanceSpot;

    fn base() -> AssetNameExchange {
        AssetNameExchange::new("BTC")
    }

    fn quote() -> AssetNameExchange {
        AssetNameExchange::new("USDT")
    }

    fn instrument_name() -> InstrumentNameExchange {
        InstrumentNameExchange::new("BTCUSDT")
    }

    fn make_exchange(btc: &str, usdt: &str) -> MockExchange {
        make_exchange_with_fee(btc, usdt, FeeModelConfig::default())
    }

    fn make_exchange_with_fee(btc: &str, usdt: &str, fee_model: FeeModelConfig) -> MockExchange {
        let btc = d(btc);
        let usdt = d(usdt);
        let initial_state = UnindexedAccountSnapshot {
            exchange: EXCHANGE,
            balances: vec![
                AssetBalance {
                    asset: base(),
                    balance: Balance::new(btc, btc),
                    time_exchange: Utc::now(),
                },
                AssetBalance {
                    asset: quote(),
                    balance: Balance::new(usdt, usdt),
                    time_exchange: Utc::now(),
                },
            ],
            instruments: vec![],
        };

        let config = MockExecutionConfig::new(
            EXCHANGE,
            initial_state,
            0, // latency_ms
            fee_model,
            SimFillConfig::default(),
        );

        let (_tx, request_rx) = mpsc::unbounded_channel();
        let (event_tx, _) = broadcast::channel(1);

        let mut instruments = FnvHashMap::default();
        instruments.insert(
            instrument_name(),
            Instrument {
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
            },
        );

        MockExchange::new(config, request_rx, event_tx, instruments)
    }

    /// A `MockExchange` balance is an absolute restatement, so emission order *is* the answer.
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
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn open_order_emissions_are_ordered_across_fills() {
        const FILLS: usize = 8;
        const DEBIT_PER_FILL: &str = "500";

        let usdt_start = "10000000";
        let mut exchange = make_exchange("100", usdt_start);

        // `make_exchange` keeps neither end of the channels; this test drives both.
        let (request_tx, request_rx) = mpsc::unbounded_channel();
        let (event_tx, mut events) = broadcast::channel(64);
        exchange.request_rx = request_rx;
        exchange.event_tx = event_tx;

        let venue = tokio::spawn(exchange.run());

        let mut responses = Vec::with_capacity(FILLS);
        for nth in 0..FILLS {
            let (response_tx, response_rx) = oneshot::channel();
            let mut request = buy_request("0.01");
            request.key.cid = ClientOrderId::new(format!("cid-{nth}"));
            request.state.market = market_prices("50000");

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
        venue.await.unwrap();

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

    fn buy_request(quantity: &str) -> OrderRequestOpen<ExchangeId, InstrumentNameExchange> {
        let quantity = d(quantity);
        OrderEvent {
            key: OrderKey {
                exchange: EXCHANGE,
                instrument: instrument_name(),
                strategy: StrategyId::new("test"),
                cid: ClientOrderId::new("test-cid"),
            },
            state: RequestOpen {
                side: Side::Buy,
                price: None, // Market orders don't have a limit price
                quantity,
                kind: OrderKind::Market,
                time_in_force: TimeInForce::ImmediateOrCancel,
                position_id: None,
                reduce_only: false,
                market: None,
            },
        }
    }

    fn sell_request(quantity: &str) -> OrderRequestOpen<ExchangeId, InstrumentNameExchange> {
        let quantity = d(quantity);
        OrderEvent {
            key: OrderKey {
                exchange: EXCHANGE,
                instrument: instrument_name(),
                strategy: StrategyId::new("test"),
                cid: ClientOrderId::new("test-cid"),
            },
            state: RequestOpen {
                side: Side::Sell,
                price: None, // Market orders don't have a limit price
                quantity,
                kind: OrderKind::Market,
                time_in_force: TimeInForce::ImmediateOrCancel,
                position_id: None,
                reduce_only: false,
                market: None,
            },
        }
    }

    /// A snapshot whose three prices are all `price`, wrapped as the venue receives it.
    fn market_prices(price: &str) -> Option<MarketSnapshot> {
        let p = Some(d(price));
        Some(MarketSnapshot {
            best_bid: p,
            best_ask: p,
            last_price: p,
        })
    }

    /// `contract_size` of the CFD fixture below, as a per-point multiplier a real index CFD carries.
    const CFD_CONTRACT_SIZE: &str = "25";

    fn cfd_instrument_name() -> InstrumentNameExchange {
        InstrumentNameExchange::new("spx500_usd")
    }

    /// A USD-quoted index CFD settling in **GBP** — the case that makes the settlement asset differ
    /// from the quote asset — with **only the quote asset funded** and no `spx500` balance at all.
    /// An index is not deliverable, so requiring base inventory to short one would be unfundable.
    fn make_cfd_exchange(usd: &str, fee_model: FeeModelConfig) -> MockExchange {
        let usd = d(usd);
        make_cfd_exchange_with_balances(
            vec![AssetBalance {
                asset: AssetNameExchange::new("usd"),
                balance: Balance::new(usd, usd),
                time_exchange: Utc::now(),
            }],
            fee_model,
        )
    }

    fn make_cfd_exchange_with_balances(
        balances: Vec<AssetBalance<AssetNameExchange>>,
        fee_model: FeeModelConfig,
    ) -> MockExchange {
        let initial_state = UnindexedAccountSnapshot {
            exchange: EXCHANGE,
            balances,
            instruments: vec![],
        };

        let config = MockExecutionConfig::new(
            EXCHANGE,
            initial_state,
            0, // latency_ms
            fee_model,
            SimFillConfig::default(),
        );

        let (_tx, request_rx) = mpsc::unbounded_channel();
        let (event_tx, _) = broadcast::channel(1);

        let mut instruments = FnvHashMap::default();
        instruments.insert(
            cfd_instrument_name(),
            Instrument {
                exchange: EXCHANGE,
                name_internal: InstrumentNameInternal::new("spx500_usd"),
                name_exchange: cfd_instrument_name(),
                underlying: Underlying {
                    base: AssetNameExchange::new("spx500"),
                    quote: AssetNameExchange::new("usd"),
                },
                quote: InstrumentQuoteAsset::UnderlyingQuote,
                kind: InstrumentKind::Cfd(CfdContract {
                    contract_size: d(CFD_CONTRACT_SIZE),
                    settlement_asset: AssetNameExchange::new("gbp"),
                }),
                spec: None,
                data_venue: None,
            },
        );

        MockExchange::new(config, request_rx, event_tx, instruments)
    }

    fn cfd_request(
        side: Side,
        quantity: &str,
    ) -> OrderRequestOpen<ExchangeId, InstrumentNameExchange> {
        OrderEvent {
            key: OrderKey {
                exchange: EXCHANGE,
                instrument: cfd_instrument_name(),
                strategy: StrategyId::new("test"),
                cid: ClientOrderId::new("test-cid"),
            },
            state: RequestOpen {
                side,
                price: None,
                quantity: d(quantity),
                kind: OrderKind::Market,
                time_in_force: TimeInForce::ImmediateOrCancel,
                position_id: None,
                reduce_only: false,
                market: None,
            },
        }
    }

    /// The multiplier must reach the ledger, or balances move 1x while the engine's PnL moves
    /// `contract_size`x — the same fill accounted two different ways.
    #[test]
    fn cfd_buy_debits_the_contract_size_scaled_notional() {
        let mut exchange = make_cfd_exchange("200000", FeeModelConfig::default());

        let (response, notifications) =
            exchange.open_order(cfd_request(Side::Buy, "1"), market_prices("5000"));

        assert!(
            response.state.is_accepted(),
            "cfd buy should fill: {:?}",
            response.state
        );
        assert!(notifications.is_some());

        // 1 contract * 5000 * 25 = 125,000 of true notional, not the unmultiplied 5,000.
        let usd = exchange
            .account
            .balance_mut(&AssetNameExchange::new("usd"))
            .unwrap();
        assert_eq!(usd.balance.free, d("200000") - d("125000"));
        assert_eq!(usd.balance.total, usd.balance.free);
    }

    /// A CFD short is a margin position, not a stock loan: it must not require inventory in an
    /// index that cannot be held. This fixture funds no `spx500` balance at all, so a base debit
    /// would panic on the missing balance rather than merely reject.
    #[test]
    fn cfd_sell_needs_no_base_inventory_and_debits_quote() {
        let mut exchange = make_cfd_exchange("200000", FeeModelConfig::default());

        let (response, notifications) =
            exchange.open_order(cfd_request(Side::Sell, "1"), market_prices("5000"));

        assert!(
            response.state.is_accepted(),
            "cfd short should fill without base inventory: {:?}",
            response.state
        );
        let notifications = notifications.expect("successful short must notify");
        assert_eq!(
            notifications.balance.0.asset,
            AssetNameExchange::new("usd"),
            "a cash-settled short debits quote, not base"
        );
        assert_eq!(notifications.balance.0.balance.free, d("75000"));
    }

    /// The scaled notional is what the balance check tests, so an account that could fund the
    /// unmultiplied order must still be rejected.
    #[test]
    fn cfd_buy_is_rejected_when_only_the_unscaled_notional_is_funded() {
        let mut exchange = make_cfd_exchange("10000", FeeModelConfig::default());

        let (response, notifications) =
            exchange.open_order(cfd_request(Side::Buy, "1"), market_prices("5000"));

        assert!(notifications.is_none());
        match response.state {
            OrderState::Inactive(InactiveOrderState::OpenFailed(
                crate::error::OrderError::Rejected(ApiError::BalanceInsufficient(ref asset, _)),
            )) => {
                assert_eq!(*asset, AssetNameExchange::new("usd"));
            }
            other => panic!("expected BalanceInsufficient, got: {other:?}"),
        }
    }

    /// Fees stay quote-denominated on both sides of a CFD, are computed on the `contract_size`
    /// scaled notional, and are debited on top of it.
    ///
    /// Every amount below is hard-coded rather than read back from the notification: an assertion
    /// that subtracts the reported fee from its own expected balance is satisfied by *any* fee,
    /// including the unscaled one this exists to rule out.
    #[test]
    fn cfd_fee_is_quote_denominated_on_both_sides() {
        for side in [Side::Buy, Side::Sell] {
            let mut exchange = make_cfd_exchange(
                "200000",
                // 0.1% of the scaled notional.
                FeeModelConfig::Percentage(PercentageFeeModel { rate: d("0.001") }),
            );

            let (response, notifications) =
                exchange.open_order(cfd_request(side, "1"), market_prices("5000"));

            assert!(
                response.state.is_accepted(),
                "{side:?} should fill: {:?}",
                response.state
            );
            let notifications = notifications.expect("successful fill must notify");
            assert_eq!(
                notifications.trade.fees.asset,
                AssetNameExchange::new("usd"),
                "{side:?} fee asset"
            );

            // 0.001 * 5000 * 1 * 25. The unscaled answer is 5, so this pins the multiplier.
            assert_eq!(
                notifications.trade.fees.fees,
                d("125"),
                "{side:?} fee must be 0.1% of the contract_size-scaled notional"
            );
            assert_eq!(
                notifications.balance.0.balance.free,
                d("200000") - d("125000") - d("125"),
                "{side:?} debit must be notional + fee"
            );
        }
    }

    /// The documented caller obligation: the **quote** asset must be funded, even when the CFD
    /// settles in another currency. This mock has no conversion rate, so it cannot fall back to the
    /// settlement asset, and an unfunded quote balance is a mis-specified fixture rather than a
    /// runtime condition.
    #[test]
    #[should_panic(expected = "MockExchange has Balance for all configured Instrument assets")]
    fn cfd_panics_when_the_quote_asset_is_unfunded() {
        // A realistically funded GBP account: the settlement asset is present, the quote asset is
        // not. The mock cannot convert between them, so this is a mis-specified fixture.
        let mut exchange = make_cfd_exchange_with_balances(
            vec![AssetBalance {
                asset: AssetNameExchange::new("gbp"),
                balance: Balance::new(d("200000"), d("200000")),
                time_exchange: Utc::now(),
            }],
            FeeModelConfig::default(),
        );

        let _ = exchange.open_order(cfd_request(Side::Buy, "1"), market_prices("5000"));
    }

    #[test]
    fn sell_order_decrements_base_balance_not_quote() {
        let mut exchange = make_exchange("1.0", "10000");
        let initial_usdt = d("10000");

        let (response, notifications) =
            exchange.open_order(sell_request("0.5"), market_prices("50000"));

        assert!(
            response.state.is_accepted(),
            "sell should succeed: {:?}",
            response.state
        );
        assert!(
            notifications.is_some(),
            "successful sell must produce notifications"
        );

        // Base (BTC) must be decremented by the quantity sold.
        let btc = exchange.account.balance_mut(&base()).unwrap();
        assert_eq!(
            btc.balance.free,
            d("0.5"),
            "base balance should decrease by quantity sold"
        );

        // Quote (USDT) must be unchanged (fees = 0 in this test).
        let usdt = exchange.account.balance_mut(&quote()).unwrap();
        assert_eq!(
            usdt.balance.free, initial_usdt,
            "quote balance should be unchanged on sell"
        );
    }

    /// A derivative must be rejected, not filled down the physically-settled spot path.
    ///
    /// The fixture is mutated through the public `instruments` map on purpose: that is exactly the
    /// route a consumer takes when it builds a `MockExchange` itself rather than through the
    /// `rustrade` builder, and it is the route that had no gate on it.
    #[test]
    fn unsupported_instrument_kinds_are_rejected_rather_than_filled_as_spot() {
        use rustrade_instrument::instrument::kind::{
            future::FutureContract,
            option::{OptionContract, OptionExercise, OptionKind},
            perpetual::PerpetualContract,
        };

        let settlement = AssetNameExchange::new("USDT");
        let expiry = Utc::now();

        let unsupported = [
            InstrumentKind::Perpetual(PerpetualContract {
                contract_size: d("10"),
                settlement_asset: settlement.clone(),
            }),
            InstrumentKind::Future(FutureContract {
                contract_size: d("10"),
                settlement_asset: settlement.clone(),
                expiry,
            }),
            InstrumentKind::Option(OptionContract {
                contract_size: d("10"),
                settlement_asset: settlement.clone(),
                kind: OptionKind::Call,
                exercise: OptionExercise::European,
                expiry,
                strike: d("50000"),
            }),
        ];

        for kind in unsupported {
            // Amply funded: the rejection must come from the kind, not from a balance shortfall.
            let mut exchange = make_exchange("10", "10000000");
            exchange
                .instruments
                .get_mut(&instrument_name())
                .unwrap()
                .kind = kind.clone();

            let (response, notifications) =
                exchange.open_order(buy_request("1.0"), market_prices("50000"));

            assert!(
                notifications.is_none(),
                "{kind:?} must produce no notifications"
            );
            assert!(
                matches!(
                    response.state,
                    OrderState::Inactive(InactiveOrderState::OpenFailed(
                        UnindexedOrderError::Connectivity(_) | UnindexedOrderError::Rejected(_)
                    ))
                ),
                "{kind:?} must be rejected, got {:?}",
                response.state
            );

            // And the ledger must be untouched -- a rejected order that still moved cash would be
            // worse than one that filled.
            let usdt = exchange.account.balance_mut(&quote()).unwrap();
            assert_eq!(
                usdt.balance.free,
                d("10000000"),
                "{kind:?} must not move the quote balance"
            );
        }
    }

    #[test]
    fn sell_order_insufficient_balance_names_base_asset() {
        // Regression guard for the sell-side balance bug fixed in this branch:
        // previously `balance_mut(&underlying.quote)` was called for sells, so
        // BalanceInsufficient would name the quote asset (USDT) instead of the base (BTC).
        let mut exchange = make_exchange("0.1", "10000");

        let (response, notifications) = exchange.open_order(
            sell_request("1.0"), // selling 1 BTC but only 0.1 available
            market_prices("50000"),
        );

        assert!(
            notifications.is_none(),
            "failed order must produce no notifications"
        );
        match response.state {
            OrderState::Inactive(InactiveOrderState::OpenFailed(
                crate::error::OrderError::Rejected(ApiError::BalanceInsufficient(ref asset, _)),
            )) => {
                assert_eq!(
                    *asset,
                    base(),
                    "BalanceInsufficient must name the base asset (BTC), not the quote (USDT)"
                );
            }
            other => panic!("expected BalanceInsufficient, got: {other:?}"),
        }
    }

    #[test]
    fn bid_ask_fill_model_fills_at_ask_price_and_deducts_correct_balance() {
        let mut exchange = make_exchange("0", "10000"); // 0 BTC, 10 000 USDT
        exchange.fill_model = SimFillConfig::BidAsk(BidAskFillModel);

        let market_prices = Some(MarketSnapshot {
            best_bid: Some(d("99.5")),
            best_ask: Some(d("100.5")),
            last_price: Some(d("100.0")),
        });

        // Market buy of 1 BTC; reference price 100 is only used as a fallback
        // when fill_model returns None — BidAsk returns best_ask so it is not used.
        let (response, notifications) = exchange.open_order(buy_request("1"), market_prices);

        assert!(
            response.state.is_accepted(),
            "buy should succeed: {:?}",
            response.state
        );
        let notifs = notifications.expect("successful buy must produce notifications");

        // BidAskFillModel: market buy fills at best_ask = 100.5, not last_price 100.0.
        assert_eq!(
            notifs.trade.price,
            d("100.5"),
            "fill price must be best_ask"
        );

        // Balance deduction: 1 * 100.5 = 100.5 USDT; fee_model = Zero.
        let usdt = exchange.account.balance_mut(&quote()).unwrap();
        assert_eq!(
            usdt.balance.free,
            d("9899.5"),
            "quote balance must decrease by fill_price * qty"
        );
    }

    #[test]
    fn percentage_fee_model_deducts_correct_fee_on_buy() {
        // 0.1% fee rate
        let fee_model = FeeModelConfig::Percentage(PercentageFeeModel { rate: d("0.001") });
        let mut exchange = make_exchange_with_fee("0", "10000", fee_model);

        // Buy 10 BTC at price 100 USDT each
        // Notional = 10 * 100 = 1000 USDT
        // Fee = 1000 * 0.001 = 1 USDT
        // Total deducted = 1000 + 1 = 1001 USDT
        let (response, notifications) =
            exchange.open_order(buy_request("10"), market_prices("100"));

        assert!(
            response.state.is_accepted(),
            "buy should succeed: {:?}",
            response.state
        );
        let notifs = notifications.expect("successful buy must produce notifications");

        // Trade must report fee in quote denomination
        assert_eq!(notifs.trade.fees.fees, d("1"), "trade fee must be 1 USDT");

        // Quote balance: 10000 - 1001 = 8999
        let usdt = exchange.account.balance_mut(&quote()).unwrap();
        assert_eq!(
            usdt.balance.free,
            d("8999"),
            "quote balance must decrease by notional + fee"
        );
    }

    #[test]
    fn percentage_fee_model_deducts_correct_fee_on_sell() {
        // 0.1% fee rate
        let fee_model = FeeModelConfig::Percentage(PercentageFeeModel { rate: d("0.001") });
        let mut exchange = make_exchange_with_fee("10", "0", fee_model);

        // Sell 1 BTC at price 100 USDT
        // Notional = 1 * 100 = 100 USDT
        // Fee (quote) = 100 * 0.001 = 0.1 USDT
        // Fee (base) = 0.1 / 100 = 0.001 BTC
        // Total base deducted = 1 + 0.001 = 1.001 BTC
        let (response, notifications) =
            exchange.open_order(sell_request("1"), market_prices("100"));

        assert!(
            response.state.is_accepted(),
            "sell should succeed: {:?}",
            response.state
        );
        let notifs = notifications.expect("successful sell must produce notifications");

        // Trade must report fee in quote denomination
        assert_eq!(
            notifs.trade.fees.fees,
            d("0.1"),
            "trade fee must be 0.1 USDT"
        );

        // Base balance: 10 - 1.001 = 8.999
        let btc = exchange.account.balance_mut(&base()).unwrap();
        assert_eq!(
            btc.balance.free,
            d("8.999"),
            "base balance must decrease by quantity + fee_in_base"
        );
    }

    #[test]
    fn percentage_fee_with_zero_price_returns_zero_fee() {
        // Edge case: if fill_price is zero, fee computation must not divide by zero
        let fee_model = FeeModelConfig::Percentage(PercentageFeeModel { rate: d("0.001") });
        let mut exchange = make_exchange_with_fee("10", "0", fee_model);

        // Sell 1 BTC at price 0 (degenerate case)
        // Fee (quote) = 0 * 0.001 * 1 = 0
        // Fee (base) = guarded by is_zero() check, returns 0
        let (response, notifications) = exchange.open_order(sell_request("1"), market_prices("0"));

        assert!(
            response.state.is_accepted(),
            "sell at zero price should succeed: {:?}",
            response.state
        );
        let notifs = notifications.expect("successful sell must produce notifications");

        // Fee must be zero (not NaN or panic from division by zero)
        assert_eq!(
            notifs.trade.fees.fees,
            Decimal::ZERO,
            "fee must be zero when price is zero"
        );

        // Base balance: 10 - 1 = 9 (no fee deducted)
        let btc = exchange.account.balance_mut(&base()).unwrap();
        assert_eq!(
            btc.balance.free,
            d("9"),
            "base balance must decrease by quantity only"
        );
    }

    /// A Market order the venue cannot price is rejected, not panicked on.
    ///
    /// A Market order carries no limit price of its own to fall back on, so with no usable
    /// snapshot there is no price anywhere. This used to hit an `expect`. Because
    /// `MockExchange::run` is a spawned task, that killed the exchange, dropped the request
    /// receiver, and turned every later order into `ExchangeOffline` -- so a backtest ran to
    /// completion over the whole dataset having filled nothing and surfaced only a `JoinError` at
    /// teardown.
    ///
    /// Both unpriceable cases are covered, because they mean different things: `None` is a caller
    /// that sampled no snapshot at all, `Some(empty)` is a cold start where one was sampled and the
    /// instrument had no price yet.
    #[test]
    fn a_market_order_that_cannot_be_priced_is_rejected_rather_than_panicking() {
        for market in [None, Some(MarketSnapshot::default())] {
            let mut exchange = make_exchange("10", "10000000");

            let (response, notifications) = exchange.open_order(buy_request("1.0"), market);

            assert!(
                notifications.is_none(),
                "a rejected order must not notify a fill (market={market:?})"
            );
            assert!(
                matches!(
                    response.state,
                    OrderState::Inactive(InactiveOrderState::OpenFailed(
                        UnindexedOrderError::Rejected(_)
                    ))
                ),
                "expected a rejection, got {:?} (market={market:?})",
                response.state
            );

            let usd = exchange.account.balance_mut(&quote()).unwrap();
            assert_eq!(
                usd.balance.free,
                d("10000000"),
                "a rejected order must not move the balance (market={market:?})"
            );
        }
    }

    /// The rejection names which of the two unpriceable causes occurred, because their fixes
    /// differ: an absent snapshot is a wiring problem in the caller, an empty one is a timing
    /// problem in the data.
    #[test]
    fn an_unpriceable_rejection_distinguishes_an_absent_snapshot_from_an_empty_one() {
        let reason_of = |market| {
            let mut exchange = make_exchange("10", "10000000");
            match exchange.open_order(buy_request("1.0"), market).0.state {
                OrderState::Inactive(InactiveOrderState::OpenFailed(
                    UnindexedOrderError::Rejected(ApiError::OrderRejected(reason)),
                )) => reason,
                other => panic!("expected an OrderRejected, got {other:?}"),
            }
        };

        assert!(
            reason_of(None).contains("no market snapshot"),
            "an absent snapshot must be named as such, got: {}",
            reason_of(None)
        );
        assert!(
            reason_of(Some(MarketSnapshot::default())).contains("no market price available yet"),
            "an empty snapshot must read as a cold start, got: {}",
            reason_of(Some(MarketSnapshot::default()))
        );
    }

    /// A Market order fills at the snapshot the request carried -- the property #279 was about.
    ///
    /// With the default `LastPriceFillModel` the fill price is the snapshot's `last_price`, so this
    /// pins that a market order priced purely from `RequestOpen::market` both fills and fills at
    /// the right price.
    #[test]
    fn a_market_order_fills_at_the_price_its_snapshot_carried() {
        let mut exchange = make_exchange("0", "10000");

        let (response, notifications) = exchange.open_order(
            buy_request("1.0"),
            Some(MarketSnapshot::from_last_price(Some(d("100")))),
        );

        let OrderState::Inactive(InactiveOrderState::FullyFilled(ref filled)) = response.state
        else {
            panic!("expected a filled order, got {:?}", response.state)
        };
        assert_eq!(
            filled.avg_price,
            Some(d("100")),
            "a market order must fill at its snapshot's last price"
        );
        assert_eq!(filled.filled_quantity, d("1.0"));

        let notifications = notifications.expect("a filled order must notify a trade");
        assert_eq!(notifications.trade.price, d("100"));

        let usd = exchange.account.balance_mut(&quote()).unwrap();
        assert_eq!(
            usd.balance.free,
            d("9900"),
            "a 1 BTC buy at 100 must debit 100 quote"
        );
    }

    /// A `total != free` balance is user configuration -- an `initial_state` copied from a live
    /// account with margin reserved -- not an internal invariant. It used to be an `assert_eq!`,
    /// which panicked the exchange task on the first order.
    #[test]
    fn a_reserved_quote_balance_is_rejected_rather_than_asserted() {
        let mut exchange = make_exchange("10", "10000000");
        exchange
            .account
            .balance_mut(&quote())
            .unwrap()
            .balance
            .total = d("20000000");

        let (response, notifications) =
            exchange.open_order(buy_request("1.0"), market_prices("50000"));

        assert!(notifications.is_none());
        assert!(
            matches!(
                response.state,
                OrderState::Inactive(InactiveOrderState::OpenFailed(
                    UnindexedOrderError::Rejected(_)
                ))
            ),
            "expected a rejection, got {:?}",
            response.state
        );
    }

    /// The sell path debits the base asset, so it carries its own copy of the reserve check.
    #[test]
    fn a_reserved_base_balance_is_rejected_rather_than_asserted() {
        let mut exchange = make_exchange("10", "10000000");
        exchange.account.balance_mut(&base()).unwrap().balance.total = d("20");

        let (response, notifications) =
            exchange.open_order(sell_request("1.0"), market_prices("50000"));

        assert!(notifications.is_none());
        assert!(
            matches!(
                response.state,
                OrderState::Inactive(InactiveOrderState::OpenFailed(
                    UnindexedOrderError::Rejected(_)
                ))
            ),
            "expected a rejection, got {:?}",
            response.state
        );
    }
}
