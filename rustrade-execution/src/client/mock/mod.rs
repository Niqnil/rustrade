use crate::{
    UnindexedAccountEvent, UnindexedAccountSnapshot,
    balance::AssetBalance,
    client::ExecutionClient,
    error::{
        ConnectivityError, OrderError, StreamTerminationReason, UnindexedClientError,
        UnindexedOrderError,
    },
    exchange::mock::request::{MarketPrices, MockExchangeRequest},
    fee::FeeModelConfig,
    fill::SimFillConfig,
    order::{
        Order, OrderEvent, OrderKey,
        request::{OrderRequestCancel, OrderRequestOpen, UnindexedOrderResponseCancel},
        state::{Open, OrderState, UnindexedOrderState},
    },
    trade::Trade,
};
use chrono::{DateTime, Utc};
use derive_more::Constructor;
use futures::{StreamExt, stream::BoxStream};
use rustrade_instrument::{
    asset::name::AssetNameExchange, exchange::ExchangeId, instrument::name::InstrumentNameExchange,
};
use serde::{Deserialize, Serialize};
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio_stream::wrappers::BroadcastStream;
use tracing::error;

#[derive(
    Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize, Constructor,
)]
pub struct MockExecutionConfig {
    pub mocked_exchange: ExchangeId,
    pub initial_state: UnindexedAccountSnapshot,
    pub latency_ms: u64,
    /// Fee model used by the mock exchange to compute trading fees.
    ///
    /// Defaults to [`FeeModelConfig::Zero`]. Use [`FeeModelConfig::Percentage`]
    /// for spot/futures simulation (e.g. 0.1% taker fee).
    #[serde(default)]
    pub fee_model: FeeModelConfig,
    /// Fill model used by the mock exchange to compute execution prices.
    ///
    /// Defaults to [`SimFillConfig::LastPrice`], which fills at the
    /// order price (identical to pre-FillModel behaviour). Switch to
    /// [`SimFillConfig::BidAsk`] or [`SimFillConfig::Midpoint`] for
    /// more realistic spread-cost simulation when market prices are injected
    /// alongside orders.
    #[serde(default)]
    pub fill_model: SimFillConfig,
}

#[derive(Debug, Constructor)]
pub struct MockExecutionClientConfig<FnTime> {
    pub mocked_exchange: ExchangeId,
    pub clock: FnTime,
    pub request_tx: mpsc::UnboundedSender<MockExchangeRequest>,
    pub event_rx: broadcast::Receiver<UnindexedAccountEvent>,
}

impl<FnTime> Clone for MockExecutionClientConfig<FnTime>
where
    FnTime: Clone,
{
    fn clone(&self) -> Self {
        Self {
            mocked_exchange: self.mocked_exchange,
            clock: self.clock.clone(),
            request_tx: self.request_tx.clone(),
            event_rx: self.event_rx.resubscribe(),
        }
    }
}

#[derive(Debug, Constructor)]
pub struct MockExecution<FnTime> {
    pub mocked_exchange: ExchangeId,
    pub clock: FnTime,
    pub request_tx: mpsc::UnboundedSender<MockExchangeRequest>,
    pub event_rx: broadcast::Receiver<UnindexedAccountEvent>,
}

impl<FnTime> Clone for MockExecution<FnTime>
where
    FnTime: Clone,
{
    fn clone(&self) -> Self {
        Self {
            mocked_exchange: self.mocked_exchange,
            clock: self.clock.clone(),
            request_tx: self.request_tx.clone(),
            event_rx: self.event_rx.resubscribe(),
        }
    }
}

impl<FnTime> MockExecution<FnTime>
where
    FnTime: Fn() -> DateTime<Utc>,
{
    pub fn time_request(&self) -> DateTime<Utc> {
        (self.clock)()
    }
}

impl<FnTime> ExecutionClient for MockExecution<FnTime>
where
    FnTime: Fn() -> DateTime<Utc> + Clone + Send + Sync,
{
    const EXCHANGE: ExchangeId = ExchangeId::Mock;
    type Config = MockExecutionClientConfig<FnTime>;
    type AccountStream = BoxStream<'static, UnindexedAccountEvent>;

    fn new(config: Self::Config) -> Self {
        Self {
            mocked_exchange: config.mocked_exchange,
            clock: config.clock,
            request_tx: config.request_tx,
            event_rx: config.event_rx,
        }
    }

    async fn account_snapshot(
        &self,
        _: &[AssetNameExchange],
        _: &[InstrumentNameExchange],
    ) -> Result<UnindexedAccountSnapshot, UnindexedClientError> {
        let (response_tx, response_rx) = oneshot::channel();

        self.request_tx
            .send(MockExchangeRequest::fetch_account_snapshot(
                self.time_request(),
                response_tx,
            ))
            .map_err(|_| {
                UnindexedClientError::Connectivity(ConnectivityError::ExchangeOffline(
                    self.mocked_exchange,
                ))
            })?;

        response_rx.await.map_err(|_| {
            UnindexedClientError::Connectivity(ConnectivityError::ExchangeOffline(
                self.mocked_exchange,
            ))
        })
    }

    async fn account_stream(
        &self,
        _: &[AssetNameExchange],
        _: &[InstrumentNameExchange],
    ) -> Result<Self::AccountStream, UnindexedClientError> {
        // `scan` (not `map_while`) so the broadcast-lag terminal is delivered in-band: on lag we
        // yield one StreamTerminated event, set `done`, and the next poll ends the stream — whereas
        // `map_while` would end on lag with a silent EOF and no terminal event.
        let exchange = self.mocked_exchange;
        Ok(BroadcastStream::new(self.event_rx.resubscribe())
            .scan(false, move |done, result| {
                // `futures::StreamExt::scan` ends the stream when the closure resolves to None.
                let item = if *done {
                    // Terminal already emitted on the prior poll — end the stream now.
                    None
                } else {
                    match result {
                        Ok(event) => Some(event),
                        Err(error) => {
                            error!(
                                ?error,
                                "MockExchange Broadcast AccountStream lagged - terminating"
                            );
                            *done = true;
                            Some(UnindexedAccountEvent::stream_terminated(
                                exchange,
                                StreamTerminationReason::Error(
                                    "mock broadcast stream lagged".to_string(),
                                ),
                            ))
                        }
                    }
                };
                futures::future::ready(item)
            })
            .boxed())
    }

    async fn cancel_order(
        &self,
        request: OrderRequestCancel<ExchangeId, &InstrumentNameExchange>,
    ) -> Option<UnindexedOrderResponseCancel> {
        let (response_tx, response_rx) = oneshot::channel();

        let key = OrderKey {
            exchange: request.key.exchange,
            instrument: request.key.instrument.clone(),
            strategy: request.key.strategy.clone(),
            cid: request.key.cid.clone(),
        };

        if self
            .request_tx
            .send(MockExchangeRequest::cancel_order(
                self.time_request(),
                response_tx,
                into_owned_request(request),
            ))
            .is_err()
        {
            return Some(UnindexedOrderResponseCancel {
                key,
                state: Err(UnindexedOrderError::Connectivity(
                    ConnectivityError::ExchangeOffline(self.mocked_exchange),
                )),
            });
        }

        Some(match response_rx.await {
            Ok(response) => response,
            Err(_) => UnindexedOrderResponseCancel {
                key,
                state: Err(UnindexedOrderError::Connectivity(
                    ConnectivityError::ExchangeOffline(self.mocked_exchange),
                )),
            },
        })
    }

    async fn open_order(
        &self,
        request: OrderRequestOpen<ExchangeId, &InstrumentNameExchange>,
    ) -> Option<Order<ExchangeId, InstrumentNameExchange, UnindexedOrderState>> {
        let (response_tx, response_rx) = oneshot::channel();

        let request = into_owned_request(request);

        if self
            .request_tx
            .send(MockExchangeRequest::open_order(
                self.time_request(),
                response_tx,
                request.clone(),
                MarketPrices::default(), // no market-data subscription; FillModel uses last_price=Some(request.state.price) as fallback, so fill equals request price
            ))
            .is_err()
        {
            return Some(Order {
                key: request.key,
                side: request.state.side,
                price: request.state.price,
                quantity: request.state.quantity,
                kind: request.state.kind,
                time_in_force: request.state.time_in_force,
                state: OrderState::inactive(OrderError::Connectivity(
                    ConnectivityError::ExchangeOffline(self.mocked_exchange),
                )),
            });
        }

        Some(match response_rx.await {
            Ok(response) => response,
            Err(_) => Order {
                key: request.key,
                side: request.state.side,
                price: request.state.price,
                quantity: request.state.quantity,
                kind: request.state.kind,
                time_in_force: request.state.time_in_force,
                state: OrderState::inactive(OrderError::Connectivity(
                    ConnectivityError::ExchangeOffline(self.mocked_exchange),
                )),
            },
        })
    }

    async fn fetch_balances(
        &self,
        assets: &[AssetNameExchange],
    ) -> Result<Vec<AssetBalance<AssetNameExchange>>, UnindexedClientError> {
        let (response_tx, response_rx) = oneshot::channel();

        self.request_tx
            .send(MockExchangeRequest::fetch_balances(
                self.time_request(),
                assets.to_vec(),
                response_tx,
            ))
            .map_err(|_| {
                UnindexedClientError::Connectivity(ConnectivityError::ExchangeOffline(
                    self.mocked_exchange,
                ))
            })?;

        response_rx.await.map_err(|_| {
            UnindexedClientError::Connectivity(ConnectivityError::ExchangeOffline(
                self.mocked_exchange,
            ))
        })
    }

    async fn fetch_open_orders(
        &self,
        instruments: &[InstrumentNameExchange],
    ) -> Result<Vec<Order<ExchangeId, InstrumentNameExchange, Open>>, UnindexedClientError> {
        let (response_tx, response_rx) = oneshot::channel();

        self.request_tx
            .send(MockExchangeRequest::fetch_orders_open(
                self.time_request(),
                instruments.to_vec(),
                response_tx,
            ))
            .map_err(|_| {
                UnindexedClientError::Connectivity(ConnectivityError::ExchangeOffline(
                    self.mocked_exchange,
                ))
            })?;

        response_rx.await.map_err(|_| {
            UnindexedClientError::Connectivity(ConnectivityError::ExchangeOffline(
                self.mocked_exchange,
            ))
        })
    }

    async fn fetch_trades(
        &self,
        time_since: DateTime<Utc>,
        // MockExchange fetch_trades doesn't filter by instrument
        _instruments: &[InstrumentNameExchange],
    ) -> Result<Vec<Trade<AssetNameExchange, InstrumentNameExchange>>, UnindexedClientError> {
        let (response_tx, response_rx) = oneshot::channel();

        self.request_tx
            .send(MockExchangeRequest::fetch_trades(
                self.time_request(),
                response_tx,
                time_since,
            ))
            .map_err(|_| {
                UnindexedClientError::Connectivity(ConnectivityError::ExchangeOffline(
                    self.mocked_exchange,
                ))
            })?;

        response_rx.await.map_err(|_| {
            UnindexedClientError::Connectivity(ConnectivityError::ExchangeOffline(
                self.mocked_exchange,
            ))
        })
    }
}

fn into_owned_request<Kind>(
    request: OrderEvent<Kind, ExchangeId, &InstrumentNameExchange>,
) -> OrderEvent<Kind, ExchangeId, InstrumentNameExchange> {
    let OrderEvent {
        key:
            OrderKey {
                exchange,
                instrument,
                strategy,
                cid,
            },
        state,
    } = request;

    OrderEvent {
        key: OrderKey {
            exchange,
            instrument: instrument.clone(),
            strategy,
            cid,
        },
        state,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use crate::{AccountEventKind, balance::Balance};
    use rust_decimal::Decimal;
    use rustrade_integration::collection::snapshot::Snapshot;

    fn filler_event() -> UnindexedAccountEvent {
        // Content is irrelevant — these events are overwritten by the lag before they can be
        // delivered; they exist only to overflow the broadcast buffer.
        UnindexedAccountEvent::new(
            ExchangeId::Mock,
            AccountEventKind::BalanceSnapshot(Snapshot::new(AssetBalance {
                asset: AssetNameExchange::new("btc"),
                balance: Balance::new(Decimal::ONE, Decimal::ONE),
                time_exchange: Utc::now(),
            })),
        )
    }

    /// Broadcast lag is a terminal stream death: the account stream must deliver an in-band
    /// `StreamTerminated(Error)` (not a silent EOF) and then end.
    #[tokio::test]
    async fn account_stream_emits_stream_terminated_on_broadcast_lag() {
        // Capacity 1 so a burst of sends with no reader immediately overflows → lag.
        let (event_tx, event_rx) = broadcast::channel::<UnindexedAccountEvent>(1);
        let (request_tx, _request_rx) = mpsc::unbounded_channel();
        // `MockExecution` derives `Constructor`, so its inherent `new` takes the four fields
        // directly (the `ExecutionClient::new(config)` trait method is shadowed here).
        let client = MockExecution::new(ExchangeId::Mock, Utc::now, request_tx, event_rx);

        let mut stream = client.account_stream(&[], &[]).await.unwrap();

        // Overflow the buffer before the stream's receiver reads anything → force a lag.
        for _ in 0..3 {
            event_tx.send(filler_event()).unwrap();
        }

        // The lag is surfaced in-band as the terminal event...
        let event = stream.next().await.expect("expected a terminal event");
        assert!(
            matches!(
                event.kind,
                AccountEventKind::StreamTerminated(StreamTerminationReason::Error(_))
            ),
            "expected StreamTerminated(Error) on broadcast lag, got {:?}",
            event.kind
        );
        assert_eq!(event.exchange, ExchangeId::Mock);

        // ...after which the stream ends.
        assert!(
            stream.next().await.is_none(),
            "stream must end after the terminal event"
        );
    }
}
