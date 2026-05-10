use crate::{
    AccountEventKind, InstrumentAccountSnapshot, UnindexedAccountEvent, UnindexedAccountSnapshot,
    balance::AssetBalance,
    client::mock::MockExecutionConfig,
    error::{ApiError, UnindexedApiError, UnindexedOrderError},
    exchange::mock::{
        account::AccountState,
        request::{MarketPrices, MockExchangeRequest, MockExchangeRequestKind},
    },
    fee::{FeeModel, FeeModelConfig},
    fill::{FillModel, SimFillConfig},
    order::{
        Order, OrderKey, OrderKind, UnindexedOrder,
        id::OrderId,
        request::{OrderRequestCancel, OrderRequestOpen, UnindexedOrderResponseCancel},
        state::{Cancelled, Filled, OrderState, UnindexedOrderState},
    },
    trade::{AssetFees, Trade, TradeId},
};
use chrono::{DateTime, TimeDelta, Utc};
use fnv::FnvHashMap;
use futures::stream::BoxStream;
use itertools::Itertools;
use rust_decimal::Decimal;
use rustrade_instrument::{
    Side,
    asset::name::AssetNameExchange,
    exchange::ExchangeId,
    instrument::{Instrument, name::InstrumentNameExchange},
};
use rustrade_integration::collection::snapshot::Snapshot;
use smol_str::ToSmolStr;
use std::fmt::Debug;
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio_stream::{StreamExt, wrappers::BroadcastStream};
use tracing::{error, info};

pub mod account;
pub mod request;

#[derive(Debug)]
pub struct MockExchange {
    pub exchange: ExchangeId,
    pub latency_ms: u64,
    pub fee_model: FeeModelConfig,
    pub fill_model: SimFillConfig,
    pub request_rx: mpsc::UnboundedReceiver<MockExchangeRequest>,
    pub event_tx: broadcast::Sender<UnindexedAccountEvent>,
    pub instruments: FnvHashMap<InstrumentNameExchange, Instrument<ExchangeId, AssetNameExchange>>,
    pub account: AccountState,
    pub order_sequence: u64,
    pub time_exchange_latest: DateTime<Utc>,
}

impl MockExchange {
    pub fn new(
        config: MockExecutionConfig,
        request_rx: mpsc::UnboundedReceiver<MockExchangeRequest>,
        event_tx: broadcast::Sender<UnindexedAccountEvent>,
        instruments: FnvHashMap<InstrumentNameExchange, Instrument<ExchangeId, AssetNameExchange>>,
    ) -> Self {
        Self {
            exchange: config.mocked_exchange,
            latency_ms: config.latency_ms,
            fee_model: config.fee_model,
            fill_model: config.fill_model,
            request_rx,
            event_tx,
            instruments,
            account: AccountState::from(config.initial_state),
            order_sequence: 0,
            time_exchange_latest: Default::default(),
        }
    }

    pub async fn run(mut self) {
        while let Some(request) = self.request_rx.recv().await {
            self.update_time_exchange(request.time_request);

            match request.kind {
                MockExchangeRequestKind::FetchAccountSnapshot { response_tx } => {
                    let snapshot = self.account_snapshot();
                    self.respond_with_latency(response_tx, snapshot);
                }
                MockExchangeRequestKind::FetchBalances {
                    response_tx,
                    assets,
                } => {
                    // Empty slice means "return all" (consistent with account_snapshot behavior).
                    let balances = self
                        .account
                        .balances()
                        .filter(|balance| assets.is_empty() || assets.contains(&balance.asset))
                        .cloned()
                        .collect();
                    self.respond_with_latency(response_tx, balances);
                }
                MockExchangeRequestKind::FetchOrdersOpen {
                    response_tx,
                    instruments,
                } => {
                    // Empty slice means "return all" (consistent with account_snapshot behavior).
                    let orders_open = self
                        .account
                        .orders_open()
                        .filter(|order| {
                            instruments.is_empty() || instruments.contains(&order.key.instrument)
                        })
                        .cloned()
                        .collect();
                    self.respond_with_latency(response_tx, orders_open);
                }
                MockExchangeRequestKind::FetchTrades {
                    response_tx,
                    time_since,
                } => {
                    let trades = self.account.trades(time_since).cloned().collect();
                    self.respond_with_latency(response_tx, trades);
                }
                MockExchangeRequestKind::CancelOrder {
                    response_tx,
                    request,
                } => {
                    // MockExchange only supports Market orders which fill immediately,
                    // so there are never any open orders to cancel. Send a rejection
                    // response so the caller doesn't hang waiting on the oneshot.
                    error!(
                        exchange = %self.exchange,
                        ?request,
                        "MockExchange received cancel request but only Market orders are supported"
                    );
                    let key = OrderKey {
                        exchange: request.key.exchange,
                        instrument: request.key.instrument,
                        strategy: request.key.strategy,
                        cid: request.key.cid,
                    };
                    let _ = response_tx.send(UnindexedOrderResponseCancel {
                        key,
                        state: Err(UnindexedOrderError::Rejected(ApiError::OrderRejected(
                            "MockExchange does not support CancelOrder (only Market orders which fill immediately)".into(),
                        ))),
                    });
                }
                MockExchangeRequestKind::OpenOrder {
                    response_tx,
                    request,
                    market_prices,
                } => {
                    let (response, notifications) = self.open_order(request, market_prices);
                    self.respond_with_latency(response_tx, response);

                    if let Some(notifications) = notifications {
                        self.account.ack_trade(notifications.trade.clone());
                        self.send_notifications_with_latency(notifications);
                    }
                }
            }
        }

        info!(exchange = %self.exchange, "MockExchange shutting down");
    }

    fn update_time_exchange(&mut self, time_request: DateTime<Utc>) {
        let client_to_exchange_latency = self.latency_ms / 2;

        self.time_exchange_latest = time_request
            .checked_add_signed(TimeDelta::milliseconds(client_to_exchange_latency as i64))
            .unwrap_or(time_request);

        self.account.update_time_exchange(self.time_exchange_latest)
    }

    pub fn time_exchange(&self) -> DateTime<Utc> {
        self.time_exchange_latest
    }

    pub fn account_snapshot(&self) -> UnindexedAccountSnapshot {
        let balances = self.account.balances().cloned().collect();

        let orders_open = self
            .account
            .orders_open()
            .cloned()
            .map(UnindexedOrder::from);

        let orders_cancelled = self
            .account
            .orders_cancelled()
            .cloned()
            .map(UnindexedOrder::from);

        let orders_all = orders_open.chain(orders_cancelled);
        let orders_all = orders_all.sorted_unstable_by_key(|order| order.key.instrument.clone());
        let orders_by_instrument = orders_all.chunk_by(|order| order.key.instrument.clone());

        let instruments = orders_by_instrument
            .into_iter()
            .map(|(instrument, orders)| InstrumentAccountSnapshot {
                instrument,
                orders: orders.into_iter().collect(),
                position: None,
            })
            .collect();

        UnindexedAccountSnapshot {
            exchange: self.exchange,
            balances,
            instruments,
        }
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
        let exchange = self.exchange;
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

    /// Sends the provided `OpenOrderNotifications` via the `MockExchanges`
    /// `broadcast::Sender<UnindexedAccountEvent>` after waiting for the latency
    /// [`Duration`].
    ///
    /// Used to simulate network latency between the exchange and client.
    fn send_notifications_with_latency(&self, notifications: OpenOrderNotifications) {
        let balance = self.build_account_event(notifications.balance);
        let trade = self.build_account_event(notifications.trade);

        let exchange = self.exchange;
        let latency = std::time::Duration::from_millis(self.latency_ms);
        let tx = self.event_tx.clone();
        tokio::spawn(async move {
            tokio::time::sleep(latency).await;

            if tx.send(balance).is_err() {
                error!(
                    %exchange,
                    kind = "Snapshot<AssetBalance<AssetNameExchange>",
                    "MockExchange failed to send AccountEvent notification to client"
                );
            }

            if tx.send(trade).is_err() {
                error!(
                    %exchange,
                    kind = "Trade<AssetNameExchange, InstrumentNameExchange>",
                    "MockExchange failed to send AccountEvent notification to client"
                );
            }
        });
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

    pub fn cancel_order(
        &mut self,
        _: OrderRequestCancel<ExchangeId, InstrumentNameExchange>,
    ) -> Order<ExchangeId, InstrumentNameExchange, Result<Cancelled, UnindexedOrderError>> {
        unimplemented!()
    }

    #[allow(clippy::expect_used)] // Mock exchange: panic if test data is incomplete
    pub fn open_order(
        &mut self,
        request: OrderRequestOpen<ExchangeId, InstrumentNameExchange>,
        market_prices: MarketPrices,
    ) -> (
        Order<ExchangeId, InstrumentNameExchange, UnindexedOrderState>,
        Option<OpenOrderNotifications>,
    ) {
        if let Err(error) = self.validate_order_kind_supported(request.state.kind) {
            return (build_open_order_err_response(request, error), None);
        }

        let underlying = match self.find_instrument_data(&request.key.instrument) {
            Ok(instrument) => instrument.underlying.clone(),
            Err(error) => return (build_open_order_err_response(request, error), None),
        };

        // Compute fill price via the configured FillModel.
        //
        // For limit orders, pass the limit price as `order_price`; for market orders pass `None`
        // so the model can select the best available market price (bid/ask/last).  When no market
        // data is present (standard MockExecution passes all-None MarketPrices), `request.state.price`
        // is used as the `last_price` fallback so behaviour is identical to the pre-FillModel path.
        //
        // Invariant: `fill_price` is only called for marketable orders. `validate_order_kind_supported`
        // (called above) currently rejects Limit orders, ensuring FillModel::fill_price never receives
        // a non-marketable limit order. If Limit support is added later, the fill model must enforce
        // limit-price semantics (e.g. a limit buy must not fill above the limit price).
        let fill_price = self
            .fill_model
            .fill_price(
                request.state.side,
                match request.state.kind {
                    // unreachable: validate_order_kind_supported (called above) already
                    // rejects non-Market orders with Err, so these arms are never reached.
                    // Kept for exhaustiveness; passes the limit/trigger price so fill models
                    // that gain support in future behave correctly without a separate change.
                    OrderKind::Market => None,
                    OrderKind::Limit
                    | OrderKind::StopLimit { .. }
                    | OrderKind::TrailingStopLimit { .. } => request.state.price,
                    OrderKind::Stop { trigger_price }
                    | OrderKind::TrailingStop {
                        offset: trigger_price,
                        ..
                    } => Some(trigger_price),
                },
                market_prices.best_bid,
                market_prices.best_ask,
                market_prices.last_price.or(request.state.price),
            )
            .or(request.state.price)
            .expect("fill_price must be available from market data or request price");

        let time_exchange = self.time_exchange();

        // Compute fee using the configured FeeModel. For spot, contract_size = 1.
        let order_fees_quote =
            self.fee_model
                .compute_fee(fill_price, request.state.quantity, Decimal::ONE);

        let balance_change_result = match request.state.side {
            Side::Buy => {
                // Buying Instrument requires sufficient QuoteAsset Balance
                #[allow(clippy::expect_used)]
                // Invariant: MockExchange - balances exist for all configured instruments
                let current = self
                    .account
                    .balance_mut(&underlying.quote)
                    .expect("MockExchange has Balance for all configured Instrument assets");

                // Currently we only supported MarketKind orders, so they should be identical
                assert_eq!(current.balance.total, current.balance.free);

                let order_value_quote = fill_price * request.state.quantity.abs();
                let quote_required = order_value_quote + order_fees_quote;

                let maybe_new_balance = current.balance.free - quote_required;

                if maybe_new_balance >= Decimal::ZERO {
                    current.balance.free = maybe_new_balance;
                    current.balance.total = maybe_new_balance;
                    current.time_exchange = time_exchange;

                    Ok((
                        current.clone(),
                        AssetFees::new(
                            underlying.quote.clone(),
                            order_fees_quote,
                            Some(order_fees_quote),
                        ),
                    ))
                } else {
                    Err(ApiError::BalanceInsufficient(
                        underlying.quote.clone(),
                        format!(
                            "Available Balance: {}, Required Balance inc. fees: {}",
                            current.balance.free, quote_required
                        ),
                    ))
                }
            }
            Side::Sell => {
                // Selling Instrument requires sufficient BaseAsset Balance
                #[allow(clippy::expect_used)]
                // Invariant: MockExchange - balances exist for all configured instruments
                let current = self
                    .account
                    .balance_mut(&underlying.base)
                    .expect("MockExchange has Balance for all configured Instrument assets");

                // Currently we only supported MarketKind orders, so they should be identical
                assert_eq!(current.balance.total, current.balance.free);

                let order_value_base = request.state.quantity.abs();
                // Fee is quote-denominated; convert to base for deduction.
                // Note: For PerContractFeeModel this conversion is nonsensical (flat USD / price),
                // but MockExchange is spot-only so PerContract isn't used in practice.
                debug_assert!(
                    !matches!(self.fee_model, FeeModelConfig::PerContract(_)),
                    "PerContractFeeModel produces nonsensical base-denominated fees on sell path"
                );
                let order_fees_base = if fill_price.is_zero() {
                    Decimal::ZERO
                } else {
                    order_fees_quote / fill_price
                };
                let base_required = order_value_base + order_fees_base;

                let maybe_new_balance = current.balance.free - base_required;

                if maybe_new_balance >= Decimal::ZERO {
                    current.balance.free = maybe_new_balance;
                    current.balance.total = maybe_new_balance;
                    current.time_exchange = time_exchange;

                    Ok((
                        current.clone(),
                        AssetFees::new(
                            underlying.quote.clone(),
                            order_fees_quote,
                            Some(order_fees_quote),
                        ),
                    ))
                } else {
                    Err(ApiError::BalanceInsufficient(
                        underlying.base,
                        format!(
                            "Available Balance: {}, Required Balance inc. fees: {}",
                            current.balance.free, base_required
                        ),
                    ))
                }
            }
        };

        let (balance_snapshot, fees) = match balance_change_result {
            Ok((balance_snapshot, fees)) => (Snapshot(balance_snapshot), fees),
            Err(error) => return (build_open_order_err_response(request, error), None),
        };

        let order_id = self.order_id_sequence_fetch_add();
        let trade_id = TradeId(order_id.0.clone());

        let order_response = Order {
            key: request.key.clone(),
            side: request.state.side,
            price: request.state.price,
            quantity: request.state.quantity,
            kind: request.state.kind,
            time_in_force: request.state.time_in_force,
            state: OrderState::fully_filled(Filled::new(
                order_id.clone(),
                self.time_exchange(),
                request.state.quantity,
                Some(fill_price),
            )),
        };

        let notifications = OpenOrderNotifications {
            balance: balance_snapshot,
            trade: Trade {
                id: trade_id,
                order_id: order_id.clone(),
                instrument: request.key.instrument,
                strategy: request.key.strategy,
                time_exchange: self.time_exchange(),
                side: request.state.side,
                price: fill_price,
                quantity: request.state.quantity,
                fees,
            },
        };

        (order_response, Some(notifications))
    }

    pub fn validate_order_kind_supported(
        &self,
        order_kind: OrderKind,
    ) -> Result<(), UnindexedOrderError> {
        if order_kind == OrderKind::Market {
            Ok(())
        } else {
            Err(UnindexedOrderError::Rejected(ApiError::OrderRejected(
                format!("MockExchange does not support OrderKind::{order_kind:?}"),
            )))
        }
    }

    pub fn find_instrument_data(
        &self,
        instrument: &InstrumentNameExchange,
    ) -> Result<&Instrument<ExchangeId, AssetNameExchange>, UnindexedApiError> {
        self.instruments.get(instrument).ok_or_else(|| {
            ApiError::InstrumentInvalid(
                instrument.clone(),
                format!("MockExchange is not set-up for managing: {instrument}"),
            )
        })
    }

    fn order_id_sequence_fetch_add(&mut self) -> OrderId {
        let sequence = self.order_sequence;
        self.order_sequence += 1;
        OrderId::new(sequence.to_smolstr())
    }

    fn build_account_event<Kind>(&self, kind: Kind) -> UnindexedAccountEvent
    where
        Kind: Into<AccountEventKind<ExchangeId, AssetNameExchange, InstrumentNameExchange>>,
    {
        UnindexedAccountEvent {
            exchange: self.exchange,
            kind: kind.into(),
        }
    }
}

fn build_open_order_err_response<E>(
    request: OrderRequestOpen<ExchangeId, InstrumentNameExchange>,
    error: E,
) -> Order<ExchangeId, InstrumentNameExchange, UnindexedOrderState>
where
    E: Into<UnindexedOrderError>,
{
    Order {
        key: request.key,
        side: request.state.side,
        price: request.state.price,
        quantity: request.state.quantity,
        kind: request.state.kind,
        time_in_force: request.state.time_in_force,
        state: OrderState::inactive(error.into()),
    }
}

#[derive(Debug)]
pub struct OpenOrderNotifications {
    pub balance: Snapshot<AssetBalance<AssetNameExchange>>,
    pub trade: Trade<AssetNameExchange, InstrumentNameExchange>,
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use crate::{
        UnindexedAccountSnapshot,
        balance::{AssetBalance, Balance},
        error::ApiError,
        exchange::mock::request::MarketPrices,
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
            kind::InstrumentKind,
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
                    balance: Balance {
                        total: btc,
                        free: btc,
                    },
                    time_exchange: Utc::now(),
                },
                AssetBalance {
                    asset: quote(),
                    balance: Balance {
                        total: usdt,
                        free: usdt,
                    },
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
            },
        );

        MockExchange::new(config, request_rx, event_tx, instruments)
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
            },
        }
    }

    fn market_prices(price: &str) -> MarketPrices {
        let p = Some(d(price));
        MarketPrices {
            best_bid: p,
            best_ask: p,
            last_price: p,
        }
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

        let market_prices = MarketPrices {
            best_bid: Some(d("99.5")),
            best_ask: Some(d("100.5")),
            last_price: Some(d("100.0")),
        };

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
}
