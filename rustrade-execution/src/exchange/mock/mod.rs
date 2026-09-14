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
    instrument::{Instrument, kind::InstrumentKind, name::InstrumentNameExchange},
};
use rustrade_integration::collection::snapshot::Snapshot;
use smol_str::ToSmolStr;
use std::fmt::Debug;
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio_stream::{StreamExt, wrappers::BroadcastStream};
use tracing::{error, info};

pub mod account;
pub mod request;

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
                isolated: None,
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

        // Cloned out before the `&mut self` balance borrow below.
        let (underlying, contract_size, cash_settled) =
            match self.find_instrument_data(&request.key.instrument) {
                Ok(instrument) => match Self::settlement_of_supported_kind(instrument) {
                    Ok(cash_settled) => (
                        instrument.underlying.clone(),
                        instrument.kind.contract_size(),
                        cash_settled,
                    ),
                    Err(error) => return (build_open_order_err_response(request, error), None),
                },
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
        let maybe_fill_price = self
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
                    | OrderKind::TakeProfitLimit { .. }
                    | OrderKind::TrailingStopLimit { .. } => request.state.price,
                    OrderKind::Stop { trigger_price }
                    | OrderKind::TakeProfit { trigger_price }
                    | OrderKind::TrailingStop {
                        offset: trigger_price,
                        ..
                    } => Some(trigger_price),
                },
                market_prices.best_bid,
                market_prices.best_ask,
                market_prices.last_price.or(request.state.price),
            )
            .or(request.state.price);

        // No price anywhere. That means no market data has been supplied for this instrument and
        // the order carries no limit price of its own to fall back on -- ordinary user data (a
        // first tick, a thin instrument, a subscription that was never made), not a violated
        // internal invariant, so it is rejected rather than panicked on.
        //
        // A panic here is also strictly less informative. `MockExchange::run` is a spawned task:
        // killing it drops the request receiver, every later order comes back as
        // `ConnectivityError::ExchangeOffline`, and the run completes over the whole dataset
        // having filled nothing, reporting only a `JoinError` at teardown.
        let Some(fill_price) = maybe_fill_price else {
            let reason = format!(
                "no market price available for {} and OrderKind::{:?} carries no limit price",
                request.key.instrument, request.state.kind
            );
            return (
                build_open_order_err_response(
                    request,
                    UnindexedOrderError::Rejected(ApiError::OrderRejected(reason)),
                ),
                None,
            );
        };

        let time_exchange = self.time_exchange();

        // Both the notional and the fee carry the instrument's `contract_size` multiplier, which is
        // `Decimal::ONE` for `Spot` and a real per-point multiplier for a `Cfd`. Dropping it here
        // while the engine applies it to PnL and fees
        // (`InstrumentState::update_from_trade`) would make this ledger and the engine's
        // position accounting disagree by exactly that factor -- balances moving 1x while PnL moves
        // 25x, silently, with every balance-derived return, drawdown and Sharpe wrong by the same
        // factor. Which models actually consult the multiplier is theirs to decide --
        // `PercentageFeeModel` scales by it, `PerContractFeeModel` deliberately does not -- and
        // passing it is what keeps that decision in the models rather than making this a second
        // place the multiplier can be lost.
        let order_notional_quote = fill_price * request.state.quantity.abs() * contract_size;
        let order_fees_quote =
            self.fee_model
                .compute_fee(fill_price, request.state.quantity, contract_size);

        let balance_change_result = match (cash_settled, request.state.side) {
            // Both directions of a CFD, and the buy side of a spot trade, post quote-denominated
            // cash -- so they are one arm, not two identical ones.
            //
            // A CFD is a cash-settled position on a price, not an exchange of the two underlying
            // assets: there is nothing to deliver in either direction. A CFD short is a margin
            // position rather than a stock loan, so -- unlike a spot sell -- it requires no base
            // inventory, which would otherwise force the caller to fund a phantom balance in an
            // index or a commodity to open one.
            //
            // For a CFD the notional stands in for a margin requirement: this mock models no
            // leverage, so "you must hold the full notional to open the position" is the
            // conservative reading, and it is the same requirement a spot buy already carries.
            (true, _) | (false, Side::Buy) => {
                #[allow(clippy::expect_used)]
                // Invariant: MockExchange - balances exist for all configured instruments
                let current = self
                    .account
                    .balance_mut(&underlying.quote)
                    .expect("MockExchange has Balance for all configured Instrument assets");

                let quote_required = order_notional_quote + order_fees_quote;
                let maybe_new_balance = current.balance.free - quote_required;

                // Every order this exchange supports fills immediately, so nothing is ever held on
                // reserve and `total` must equal `free`. An `initial_state` that says otherwise --
                // a snapshot copied from a live account with margin reserved, say -- cannot be
                // modelled here: the fill path below writes both fields from one number and would
                // erase the reserved portion without saying so. That is user configuration rather
                // than an internal invariant, so reject.
                if current.balance.total != current.balance.free {
                    Err(ApiError::OrderRejected(format!(
                        "MockExchange cannot model a reserved balance for {}: \
                         total {} != free {}",
                        underlying.quote, current.balance.total, current.balance.free
                    )))
                } else if maybe_new_balance >= Decimal::ZERO {
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
            (false, Side::Sell) => {
                // Selling Instrument requires sufficient BaseAsset Balance
                #[allow(clippy::expect_used)]
                // Invariant: MockExchange - balances exist for all configured instruments
                let current = self
                    .account
                    .balance_mut(&underlying.base)
                    .expect("MockExchange has Balance for all configured Instrument assets");

                let order_value_base = request.state.quantity.abs();
                // Fee is quote-denominated; convert to base for deduction.
                //
                // Note: for `PerContractFeeModel` this conversion is nonsensical (a flat commission
                // divided by a price). Only `Spot` reaches this branch -- a cash-settled kind is
                // handled above and never debits base -- and a per-contract commission on a spot
                // instrument is already a modelling error, so this stays an assert rather than a
                // conversion this mock pretends to do correctly.
                debug_assert!(
                    !matches!(self.fee_model, FeeModelConfig::PerContract(_)),
                    "PerContractFeeModel produces nonsensical base-denominated fees on the spot \
                     sell path"
                );
                let order_fees_base = if fill_price.is_zero() {
                    Decimal::ZERO
                } else {
                    order_fees_quote / fill_price
                };
                let base_required = order_value_base + order_fees_base;

                let maybe_new_balance = current.balance.free - base_required;

                // See the quote-side arm: immediate fills mean nothing is ever on reserve, so a
                // configured `total != free` cannot be modelled and is rejected rather than
                // asserted on.
                if current.balance.total != current.balance.free {
                    Err(ApiError::OrderRejected(format!(
                        "MockExchange cannot model a reserved balance for {}: \
                         total {} != free {}",
                        underlying.base, current.balance.total, current.balance.free
                    )))
                } else if maybe_new_balance >= Decimal::ZERO {
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

    /// Returns whether `instrument` is cash-settled, rejecting kinds this exchange cannot model.
    ///
    /// # Why this is enforced here and not only upstream
    /// [`MockExchange`] and its `instruments` map are both public, so a consumer can construct one
    /// directly and never pass through the `rustrade` builder that screens kinds today. Without
    /// this gate a [`InstrumentKind::Perpetual`], [`InstrumentKind::Future`] or
    /// [`InstrumentKind::Option`] falls to the physically-settled spot path and is filled as if it
    /// were deliverable stock — with its `contract_size` multiplier applied to a delivery that
    /// cannot happen, and with no funding, margin or expiry settlement anywhere. That is a wrong
    /// backtest, not a missing feature, and it is silent.
    ///
    /// # Errors
    /// Returns [`ApiError::InstrumentInvalid`] for any kind other than [`InstrumentKind::Spot`] or
    /// [`InstrumentKind::Cfd`].
    fn settlement_of_supported_kind(
        instrument: &Instrument<ExchangeId, AssetNameExchange>,
    ) -> Result<bool, UnindexedApiError> {
        match &instrument.kind {
            InstrumentKind::Spot => Ok(false),
            InstrumentKind::Cfd(_) => Ok(true),
            unsupported => Err(ApiError::InstrumentInvalid(
                instrument.name_exchange.clone(),
                format!(
                    "MockExchange does not support {}; only Spot and Cfd are modelled",
                    match unsupported {
                        InstrumentKind::Perpetual(_) => "InstrumentKind::Perpetual",
                        InstrumentKind::Future(_) => "InstrumentKind::Future",
                        InstrumentKind::Option(_) => "InstrumentKind::Option",
                        // Unreachable: both are matched above. Spelled out rather than `_` so a new
                        // kind is a compile error here instead of a mislabelled rejection.
                        InstrumentKind::Spot | InstrumentKind::Cfd(_) => "InstrumentKind",
                    }
                ),
            )),
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

    /// Reproduces the wiring `MockExecution` actually uses: an all-`None` [`MarketPrices`] and a
    /// Market order, which carries no limit price of its own to fall back on.
    ///
    /// This combination used to hit an `expect`. Because `MockExchange::run` is a spawned task,
    /// that killed the exchange, dropped the request receiver, and turned every later order into
    /// `ExchangeOffline` -- so a backtest ran to completion over the whole dataset having filled
    /// nothing and surfaced only a `JoinError` at teardown.
    #[test]
    fn a_market_order_with_no_market_data_is_rejected_rather_than_panicking() {
        let mut exchange = make_exchange("10", "10000000");

        let (response, notifications) =
            exchange.open_order(buy_request("1.0"), MarketPrices::default());

        assert!(
            notifications.is_none(),
            "a rejected order must not notify a fill"
        );
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

        let usd = exchange.account.balance_mut(&quote()).unwrap();
        assert_eq!(
            usd.balance.free,
            d("10000000"),
            "a rejected order must not move the balance"
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
