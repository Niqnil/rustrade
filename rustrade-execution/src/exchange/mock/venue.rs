//! The simulated venue's state machine, independent of any transport that drives it.

use crate::{
    AccountEventKind, InstrumentAccountSnapshot, UnindexedAccountEvent, UnindexedAccountSnapshot,
    balance::AssetBalance,
    client::mock::MockExecutionConfig,
    error::{ApiError, UnindexedApiError, UnindexedOrderError},
    exchange::mock::account::AccountState,
    fee::{FeeModel, FeeModelConfig},
    fill::{FillModel, SimFillConfig},
    market::MarketSnapshot,
    order::{
        Order, OrderKind, UnindexedOrder,
        id::OrderId,
        request::{OrderRequestCancel, OrderRequestOpen, UnindexedOrderResponseCancel},
        state::{Filled, Open, OrderState, UnindexedOrderState},
    },
    trade::{AssetFees, Trade, TradeId},
};
use chrono::{DateTime, Utc};
use fnv::FnvHashMap;
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

/// Everything one request obliges a driver to deliver, in the order it must deliver it.
///
/// # Ordering obligation
/// Every event in `events` must reach the client **before** `response`, and in the order given. A
/// [`SimulatedVenue`] balance is an absolute restatement, not a delta: successive fills report
/// `9_999_500`, then `9_999_000`, then `9_998_500`. Delivering two of them out of order therefore
/// does not merely reorder history, it leaves the client holding the wrong balance.
#[derive(Debug)]
pub struct VenueOutcome<Response> {
    /// Account events this request produced, in the order they must be delivered.
    pub events: Vec<UnindexedAccountEvent>,
    /// The response owed to whoever made the request.
    pub response: Response,
}

/// Response type of [`SimulatedVenue::open_order`].
pub type OpenOutcome = VenueOutcome<Order<ExchangeId, InstrumentNameExchange, UnindexedOrderState>>;

/// Response type of [`SimulatedVenue::cancel_order`].
pub type CancelOutcome = VenueOutcome<UnindexedOrderResponseCancel>;

/// Simulated venue state machine: synchronous, transport-free and latency-free.
///
/// Holds the ledger and prices fills. It has no channels and never sleeps, so a driver decides
/// *when* its output reaches a client while the venue decides *what* that output is and in which
/// order it must arrive.
#[derive(Debug)]
pub struct SimulatedVenue {
    pub exchange: ExchangeId,
    pub fee_model: FeeModelConfig,
    pub fill_model: SimFillConfig,
    pub instruments: FnvHashMap<InstrumentNameExchange, Instrument<ExchangeId, AssetNameExchange>>,
    pub account: AccountState,
    /// Monotone `OrderId` source. Private: resetting it mints duplicate ids, and
    /// `TradeId` is derived from it, so the duplicate would reach the trade ledger.
    order_sequence: u64,
    /// Read via [`time_exchange`](Self::time_exchange), advanced only by
    /// [`advance_time`](Self::advance_time).
    time_exchange_latest: DateTime<Utc>,
}

impl SimulatedVenue {
    pub fn new(
        config: &MockExecutionConfig,
        instruments: FnvHashMap<InstrumentNameExchange, Instrument<ExchangeId, AssetNameExchange>>,
    ) -> Self {
        Self {
            exchange: config.mocked_exchange,
            fee_model: config.fee_model,
            fill_model: config.fill_model,
            instruments,
            account: AccountState::from(config.initial_state.clone()),
            order_sequence: 0,
            time_exchange_latest: Default::default(),
        }
    }

    /// Sets the instant this venue stamps on everything it produces from now on.
    ///
    /// The caller owns the latency model: a driver simulating a network delay passes an instant
    /// already offset by it. The venue itself models no delay.
    pub fn advance_time(&mut self, time_exchange: DateTime<Utc>) {
        self.time_exchange_latest = time_exchange;
        self.account.update_time_exchange(time_exchange)
    }

    pub fn time_exchange(&self) -> DateTime<Utc> {
        self.time_exchange_latest
    }

    /// Number of orders this venue has booked, and so the next `OrderId` it will mint.
    pub fn order_sequence(&self) -> u64 {
        self.order_sequence
    }

    pub fn balances(&self, assets: &[AssetNameExchange]) -> Vec<AssetBalance<AssetNameExchange>> {
        // Empty slice means "return all" (consistent with account_snapshot behavior).
        self.account
            .balances()
            .filter(|balance| assets.is_empty() || assets.contains(&balance.asset))
            .cloned()
            .collect()
    }

    pub fn orders_open(
        &self,
        instruments: &[InstrumentNameExchange],
    ) -> Vec<Order<ExchangeId, InstrumentNameExchange, Open>> {
        // Empty slice means "return all" (consistent with account_snapshot behavior).
        self.account
            .orders_open()
            .filter(|order| instruments.is_empty() || instruments.contains(&order.key.instrument))
            .cloned()
            .collect()
    }

    pub fn trades(
        &self,
        time_since: DateTime<Utc>,
    ) -> Vec<Trade<AssetNameExchange, InstrumentNameExchange>> {
        self.account.trades(time_since).cloned().collect()
    }

    /// Books an open against the ledger and reports everything it owes the client.
    ///
    /// The venue books its own fill: the `Trade` is acknowledged into the ledger here, so a driver
    /// cannot forget to. Events come back as `[balance, trade]`, mirroring a real venue where the
    /// trade is booked before the order can be reported `FullyFilled`.
    ///
    /// A Market order carries no limit price, so the venue prices it against
    /// [`RequestOpen::market`](crate::order::request::RequestOpen::market) -- the snapshot its
    /// sender stamped at decision time, and this venue's only price source.
    pub fn open_order(
        &mut self,
        request: OrderRequestOpen<ExchangeId, InstrumentNameExchange>,
    ) -> OpenOutcome {
        // Single source: the snapshot the request carries. Passing it separately would allow two
        // copies on one path to disagree, with nothing to arbitrate.
        let market = request.state.market;
        let (response, notifications) = self.open_order_inner(request, market);

        let events = match notifications {
            Some(notifications) => {
                // Booked before the events are built, so a subsequent request on this venue sees
                // the trade regardless of when a driver gets around to delivering them.
                self.account.ack_trade(notifications.trade.clone());
                vec![
                    self.build_account_event(notifications.balance),
                    self.build_account_event(notifications.trade),
                ]
            }
            None => Vec::new(),
        };

        VenueOutcome { events, response }
    }

    /// Rejects a cancel: this venue accepts only Market orders, which fill on arrival.
    ///
    /// There is therefore never a resting order to cancel, and the rejection is a property of the
    /// venue rather than of any transport carrying it.
    pub fn cancel_order(
        &mut self,
        request: OrderRequestCancel<ExchangeId, InstrumentNameExchange>,
    ) -> CancelOutcome {
        VenueOutcome {
            events: Vec::new(),
            response: UnindexedOrderResponseCancel {
                key: request.key,
                state: Err(UnindexedOrderError::Rejected(ApiError::OrderRejected(
                    "SimulatedVenue does not support CancelOrder (only Market orders which fill immediately)".into(),
                ))),
            },
        }
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

    #[allow(clippy::expect_used)] // Mock exchange: panic if test data is incomplete
    fn open_order_inner(
        &mut self,
        request: OrderRequestOpen<ExchangeId, InstrumentNameExchange>,
        market: Option<MarketSnapshot>,
    ) -> (
        Order<ExchangeId, InstrumentNameExchange, UnindexedOrderState>,
        Option<OpenOrderNotifications>,
    ) {
        let market_supplied = market.is_some();

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
        // so the model can select the best available market price (bid/ask/last). `market` is the
        // snapshot the request's sender observed when it decided to trade -- for a market order it
        // is the only price source there is, since `request.state.price` is `None` by construction
        // for `OrderKind::Market`. A limit order additionally falls back to its own limit price.
        //
        // Invariant: `fill_price` is only called for marketable orders. `validate_order_kind_supported`
        // (called above) currently rejects Limit orders, ensuring FillModel::fill_price never receives
        // a non-marketable limit order. If Limit support is added later, the fill model must enforce
        // limit-price semantics (e.g. a limit buy must not fill above the limit price).
        let market = market.unwrap_or_default();
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
                market.best_bid,
                market.best_ask,
                market.last_price.or(request.state.price),
            )
            .or(request.state.price);

        // No price anywhere. Ordinary user data -- a cold start, a thin instrument, a subscription
        // that was never made, or a caller that supplied no snapshot at all -- not a violated
        // internal invariant, so it is rejected rather than panicked on.
        //
        // A panic here is also strictly less informative. `MockExchange::run` is a spawned task:
        // killing it drops the request receiver, every later order comes back as
        // `ConnectivityError::ExchangeOffline`, and the run completes over the whole dataset
        // having filled nothing, reporting only a `JoinError` at teardown.
        //
        // The two causes get different messages because they have different fixes: an absent
        // snapshot is a wiring problem in the caller, an empty one is a timing problem in the data.
        let Some(fill_price) = maybe_fill_price else {
            let cause = if market_supplied {
                "no market price available yet"
            } else {
                "the request carried no market snapshot (RequestOpen::market was None)"
            };
            let reason = format!(
                "cannot price {} for {}: {cause} and OrderKind::{:?} carries no limit price",
                request.key.instrument, request.key.exchange, request.state.kind
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
pub(super) struct OpenOrderNotifications {
    pub balance: Snapshot<AssetBalance<AssetNameExchange>>,
    pub trade: Trade<AssetNameExchange, InstrumentNameExchange>,
}
