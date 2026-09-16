//! The simulated venue's state machine, independent of any transport that drives it.

use crate::{
    AccountEventKind, InstrumentAccountSnapshot, UnindexedAccountEvent, UnindexedAccountSnapshot,
    balance::AssetBalance,
    client::mock::MockExecutionConfig,
    error::{ApiError, UnindexedApiError, UnindexedOrderError},
    exchange::mock::account::AccountState,
    fee::{FeeModel, FeeModelConfig, Liquidity},
    fill::{FillContext, FillModel, SimFillConfig},
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

/// A venue's view of one instrument, and when it last changed.
///
/// The instant is carried alongside the prices rather than inside them because a
/// [`MarketSnapshot`] says what the market is, not when it was observed — and a matching engine
/// needs both: an order resting since `T` may only be matched against a market at or after `T`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct VenueInstrumentMarket {
    /// Prices as of [`time_exchange`](Self::time_exchange).
    pub snapshot: MarketSnapshot,
    /// When this view was last replaced.
    pub time_exchange: DateTime<Utc>,
}

/// Simulated venue state machine: synchronous, transport-free and latency-free.
///
/// Fills every market order immediately and keeps its own balance ledger. It has no channels and
/// never sleeps, so a driver decides *when* its output reaches a client while the venue decides
/// *what* that output is and in which order it must arrive — see [`VenueOutcome`].
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
/// venue models. They are **rejected at [`open_order`](Self::open_order)** with
/// [`ApiError::InstrumentInvalid`] rather than filled. The check lives here, not only in the
/// `rustrade` builder, because this type and its [`instruments`](Self::instruments) map are public:
/// a consumer that constructs one directly bypasses every upstream gate, and the alternative to
/// rejecting is filling a derivative as if it were deliverable stock.
///
/// # ⚠️ Caller obligations and known limitations
/// - **Fund the quote asset of every instrument traded.** Every debit is quote-denominated except a
///   spot sell, which debits base. A missing balance is a **panic**, not an error: the balances are
///   this venue's own fixture, so an absent one is a mis-specified test rather than a runtime
///   condition. A CFD settling in an account currency that is not the quote asset still needs the
///   **quote** asset funded — see below.
/// - **`CfdContract::settlement_asset` is not settled in.** A CFD routinely cash-settles in an
///   account currency that is not the quote asset (a GBP account trading a USD-quoted index), which
///   requires a quote→settlement conversion rate. This venue has no rate source and will not invent
///   one, so it debits and credits the quote asset and leaves the currency dimension unmodelled. The
///   field is carried on the instrument so its description stays faithful, and the engine's own
///   `InstrumentKind` — not this copy — drives PnL. A backtest whose result depends on the
///   settlement currency needs a real execution client.
/// - **The ledger debits the paying asset and does not credit the received one.** Pre-existing: a
///   spot buy debits quote without crediting base, and a spot sell the reverse. Balances therefore
///   track cash committed, not portfolio value; position-derived statistics come from the engine.
/// - **Only [`OrderKind::Market`] is accepted**; every other kind is rejected.
///
/// # Reserved balances
/// The ledger distinguishes held from spendable: `free` is what an order may draw on, `total` is
/// what the account holds, and the difference is held against something. An `initial_state` copied
/// from a live account with margin reserved is therefore usable as configured — it was rejected
/// outright while every order filled on arrival and the ledger could not represent the state.
///
/// Every order this venue currently accepts still fills on arrival, so it reserves and settles in
/// one step ([`AccountState::debit_filled`]) and emits **one** balance restatement per fill. A
/// configured reservation is carried through untouched: settling lowers `total` by the settled
/// amount and leaves the rest held.
///
/// [`AccountState::debit_filled`]: crate::exchange::mock::account::AccountState::debit_filled
#[derive(Debug)]
pub struct SimulatedVenue {
    pub exchange: ExchangeId,
    pub fee_model: FeeModelConfig,
    pub fill_model: SimFillConfig,
    pub instruments: FnvHashMap<InstrumentNameExchange, Instrument<ExchangeId, AssetNameExchange>>,
    pub account: AccountState,
    /// This venue's own view of each instrument it trades, as of the last
    /// [`apply_market`](Self::apply_market).
    ///
    /// Empty unless a driver feeds it — see [`apply_market`](Self::apply_market) for which drivers
    /// do. Read through [`market`](Self::market).
    market: FnvHashMap<InstrumentNameExchange, VenueInstrumentMarket>,
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
            market: FnvHashMap::default(),
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

    /// Records this venue's own view of `instrument` as of `time_exchange`.
    ///
    /// `snapshot` **replaces** whatever was held: it is a complete view of the instrument, not a
    /// partial update, so a field that is `None` means the feed supplies no such price rather than
    /// "unchanged". Whoever calls this owns the state machine that folds a stream of market events
    /// into that view.
    ///
    /// # Not every driver supplies this
    /// A venue only has market state if something feeds it one. `SimRunner` does, routing each
    /// source market event to the venues trading that instrument before the `Engine` sees it.
    /// `MockExchange` has no market feed and cannot acquire one without re-creating the look-ahead
    /// hazard that motivated the split, so a venue driven by it reports [`market`](Self::market) as
    /// `None` forever. That difference is a property of this type, not an accident of wiring.
    ///
    /// # This is recorded, not yet priced against
    /// Nothing in this venue reads it to price a fill today. A market order is still priced from
    /// the snapshot its own request carried
    /// ([`RequestOpen::market`](crate::order::request::RequestOpen::market)), which is what keeps
    /// the venue's results identical whether or not a driver feeds it. It is the substrate resting
    /// orders need: an order that rests has no request-time snapshot to be matched against, because
    /// the market it must be matched against has not happened yet.
    pub fn apply_market(
        &mut self,
        instrument: &InstrumentNameExchange,
        snapshot: MarketSnapshot,
        time_exchange: DateTime<Utc>,
    ) {
        let entry = self.market.entry(instrument.clone()).or_default();
        entry.snapshot = snapshot;
        entry.time_exchange = time_exchange;
    }

    /// This venue's view of `instrument`, or `None` if nothing has fed it one.
    pub fn market(&self, instrument: &InstrumentNameExchange) -> Option<&VenueInstrumentMarket> {
        self.market.get(instrument)
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
        // Sorted on `(instrument, cid)` rather than `instrument` alone: the sort is unstable, so a
        // key shared by several orders leaves their relative order unspecified, and a snapshot that
        // lists the same account's orders in a different order on each run is not comparable
        // between runs. `cid` is unique per order, so the extended key is total.
        let orders_all = orders_all
            .sorted_unstable_by_key(|order| (order.key.instrument.clone(), order.key.cid.clone()));
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

    /// Prices one open and moves the balance it pays with, without committing anything else.
    ///
    /// Kept separate from [`open_order`](Self::open_order) rather than inlined: this body has six
    /// early returns and the ordering contract -- ack the trade, then emit balance before trade --
    /// is the part a reader needs to find. Splitting keeps that contract in a ten-line caller
    /// instead of at the end of a three-hundred-line one.
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
        // `market` is the snapshot the request's sender observed when it decided to trade -- for a
        // market order it is the only price source there is, since `request.state.price` is `None`
        // by construction for `OrderKind::Market`.
        //
        // The order's own price is deliberately NOT handed to the model. A limit constrains the
        // result, not the pricing: a model reads the book and returns where a taker prints, and
        // this venue is what bounds that by the order's terms. See `FillModel`'s contract.
        //
        // Invariant: `fill_price` is only called for an order that will fill on arrival.
        // `validate_order_kind_supported` (called above) rejects every kind but `Market`, so no
        // limit reaches this call yet and there is nothing to bound. When limits are accepted, the
        // clamp belongs here, next to the model call that needs bounding -- not inside the model.
        let market = market.unwrap_or_default();
        let maybe_fill_price = self
            .fill_model
            .fill_price(&FillContext::new(request.state.side, &market))
            // Inert for every order this venue currently accepts: `OrderKind::Market` carries no
            // price, so this is `.or(None)`. It survives as the last resort for a request whose
            // kind does carry one and whose market is empty.
            .or(request.state.price);

        // No price anywhere. Ordinary user data -- a cold start, a thin instrument, a subscription
        // that was never made, or a caller that supplied no snapshot at all -- not a violated
        // internal invariant, so it is rejected rather than panicked on.
        //
        // A panic here is also strictly less informative. The async driver runs as a spawned task:
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
        // Taker: every order this venue accepts is marketable on arrival, so every fill takes
        // liquidity. When resting orders land, an order matched while on the book is the maker.
        let order_fees_quote = self.fee_model.compute_fee(
            fill_price,
            request.state.quantity,
            contract_size,
            Liquidity::Taker,
        );

        // Which asset the order pays with, and how much of it.
        //
        // Both directions of a CFD, and the buy side of a spot trade, post quote-denominated cash
        // -- so they are one arm, not two identical ones.
        //
        // A CFD is a cash-settled position on a price, not an exchange of the two underlying
        // assets: there is nothing to deliver in either direction. A CFD short is a margin position
        // rather than a stock loan, so -- unlike a spot sell -- it requires no base inventory,
        // which would otherwise force the caller to fund a phantom balance in an index or a
        // commodity to open one.
        //
        // For a CFD the notional stands in for a margin requirement: this mock models no leverage,
        // so "you must hold the full notional to open the position" is the conservative reading,
        // and it is the same requirement a spot buy already carries.
        let (asset_debited, amount_required) = match (cash_settled, request.state.side) {
            (true, _) | (false, Side::Buy) => (
                underlying.quote.clone(),
                order_notional_quote + order_fees_quote,
            ),
            (false, Side::Sell) => {
                // Selling a spot instrument delivers the base asset, so the debit is denominated in
                // base and the quote-denominated fee is converted at the fill price.
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

                (
                    underlying.base.clone(),
                    request.state.quantity.abs() + order_fees_base,
                )
            }
        };

        // Reserve-then-settle in one step. A market order fills on arrival, so the two collapse
        // together and the client sees a single balance restatement -- see
        // `AccountState::debit_filled`. A configured `total != free` (an `initial_state` copied
        // from a live account with margin reserved) is carried through untouched rather than
        // rejected: the ledger now represents a held amount, so there is nothing left to refuse.
        let balance_change_result =
            self.account
                .debit_filled(&asset_debited, amount_required, time_exchange);

        let (balance_snapshot, fees) = match balance_change_result {
            Ok(balance_snapshot) => (
                Snapshot(balance_snapshot),
                AssetFees::new(
                    underlying.quote.clone(),
                    order_fees_quote,
                    Some(order_fees_quote),
                ),
            ),
            Err(insufficient) => {
                return (
                    build_open_order_err_response(
                        request,
                        ApiError::BalanceInsufficient(
                            asset_debited,
                            format!(
                                "Available Balance: {}, Required Balance inc. fees: {}",
                                insufficient.free, insufficient.required
                            ),
                        ),
                    ),
                    None,
                );
            }
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
                format!("SimulatedVenue does not support OrderKind::{order_kind:?}"),
            )))
        }
    }

    /// Returns whether `instrument` is cash-settled, rejecting kinds this venue cannot model.
    ///
    /// # Why this is enforced here and not only upstream
    /// This type and its [`instruments`](Self::instruments) map are both public, so a consumer can
    /// construct one directly and never pass through the `rustrade` builder that screens kinds
    /// today. Without this gate a [`InstrumentKind::Perpetual`], [`InstrumentKind::Future`] or
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
                    "SimulatedVenue does not support {}; only Spot and Cfd are modelled",
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
                format!("SimulatedVenue is not set-up for managing: {instrument}"),
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

/// What one filled open owes the client, before it is committed and packaged into events.
///
/// Internal to [`SimulatedVenue::open_order`]: it exists only to keep the ordering contract -- ack
/// the trade, then emit balance before trade -- visible in one short place rather than at the end
/// of the pricing body.
#[derive(Debug)]
struct OpenOrderNotifications {
    balance: Snapshot<AssetBalance<AssetNameExchange>>,
    trade: Trade<AssetNameExchange, InstrumentNameExchange>,
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use crate::{
        error::OrderError,
        exchange::mock::fixtures::*,
        fee::PercentageFeeModel,
        fill::BidAskFillModel,
        order::{
            OrderEvent, OrderKey, TimeInForce,
            id::{ClientOrderId, StrategyId},
            request::RequestCancel,
            state::InactiveOrderState,
        },
    };
    use rustrade_instrument::{
        Underlying,
        instrument::{
            kind::cfd::CfdContract, name::InstrumentNameInternal, quote::InstrumentQuoteAsset,
        },
    };

    fn make_venue(btc: &str, usdt: &str) -> SimulatedVenue {
        make_venue_with_fee(btc, usdt, FeeModelConfig::default())
    }

    fn make_venue_with_fee(btc: &str, usdt: &str, fee_model: FeeModelConfig) -> SimulatedVenue {
        SimulatedVenue::new(&spot_config(btc, usdt, fee_model), spot_instruments())
    }

    /// The balance snapshot a filled open owes its client, read back off the venue's own output.
    ///
    /// The assertions below go through [`VenueOutcome::events`] rather than any internal, so they
    /// pin what a driver actually delivers — and, by indexing, the order it must deliver it in.
    fn filled_balance(outcome: &OpenOutcome) -> &AssetBalance<AssetNameExchange> {
        match outcome.events.first().map(|event| &event.kind) {
            Some(AccountEventKind::BalanceSnapshot(balance)) => &balance.0,
            other => panic!("a filled open must emit its balance first, got: {other:?}"),
        }
    }

    /// The trade a filled open owes its client, which must follow the balance.
    fn filled_trade(outcome: &OpenOutcome) -> &Trade<AssetNameExchange, InstrumentNameExchange> {
        match outcome.events.get(1).map(|event| &event.kind) {
            Some(AccountEventKind::Trade(trade)) => trade,
            other => panic!("a filled open must emit its trade after its balance, got: {other:?}"),
        }
    }

    /// The venue's ordering obligation, asserted without any transport in the way.
    ///
    /// Two separate claims, because a driver needs both: each fill reports `[balance, trade]`, and
    /// successive fills report strictly decreasing balances. A balance here is an absolute
    /// restatement rather than a delta, so a driver that reorders them does not merely reorder
    /// history — it leaves the client holding the wrong number.
    #[test]
    fn a_filled_open_reports_its_balance_before_its_trade_and_debits_monotonically() {
        const FILLS: usize = 4;

        let mut venue = make_venue("100", "10000000");
        let mut balances = Vec::with_capacity(FILLS);

        for _ in 0..FILLS {
            let outcome = venue.open_order(buy_request("0.01", market_prices("50000")));

            assert_eq!(
                outcome.events.len(),
                2,
                "a fill owes exactly one balance and one trade"
            );
            // Both accessors assert their own position, so reaching here is the order assertion.
            let balance = filled_balance(&outcome);
            let trade = filled_trade(&outcome);

            assert_eq!(balance.asset, quote(), "a spot buy debits quote");
            assert_eq!(trade.price, d("50000"));
            balances.push(balance.balance.total);
        }

        // 0.01 * 50 000 = 500 per fill, from a 10 000 000 start.
        let expected = (1..=FILLS)
            .map(|nth| d("10000000") - d("500") * Decimal::from(nth))
            .collect::<Vec<_>>();
        assert_eq!(
            balances, expected,
            "successive fills must restate a strictly decreasing balance"
        );
    }

    /// The fill is in the ledger the moment `open_order` returns, not when its events are delivered.
    ///
    /// The venue acknowledges its own trade, so the ledger cannot disagree with the events a driver
    /// is still holding: a later request sees the fill regardless of when the driver gets around to
    /// emitting it. Splitting that across the seam — book here, acknowledge in the driver — is what
    /// let one driver forget.
    #[test]
    fn a_filled_open_is_in_the_ledger_before_its_events_are_delivered() {
        let mut venue = make_venue("100", "10000000");
        let before = venue.time_exchange();

        let outcome = venue.open_order(buy_request("0.01", market_prices("50000")));
        assert_eq!(outcome.events.len(), 2, "the fill must owe events");

        // Nothing has delivered `outcome`, yet the ledger already reflects it.
        let trades = venue.trades(before);
        assert_eq!(trades.len(), 1, "the venue must acknowledge its own trade");
        assert_eq!(trades[0].price, d("50000"));
        assert_eq!(
            trades[0].id.0, trades[0].order_id.0,
            "TradeId derives from OrderId"
        );
    }

    /// `advance_time` is the venue's only clock input, and it stamps everything produced after it.
    #[test]
    fn a_fill_is_stamped_with_the_instant_the_venue_was_last_advanced() {
        let mut venue = make_venue("100", "10000000");
        let time_exchange = "2025-01-01T00:00:00Z".parse::<DateTime<Utc>>().unwrap();

        venue.advance_time(time_exchange);
        assert_eq!(venue.time_exchange(), time_exchange);

        let outcome = venue.open_order(buy_request("0.01", market_prices("50000")));
        assert_eq!(filled_trade(&outcome).time_exchange, time_exchange);
        assert_eq!(filled_balance(&outcome).time_exchange, time_exchange);
    }

    /// A resting order's arrival stamp survives the clock moving past it.
    ///
    /// `advance_time` used to rewrite `time_exchange` on every open order. Nothing rested, so the
    /// only orders it could reach were those an `initial_state` seeded — and it moved them to
    /// whenever the clock last ticked. Arrival order is half of price-time priority, so an order
    /// whose arrival instant is rewritten on every event has no arrival instant at all.
    #[test]
    fn advancing_the_clock_does_not_restamp_a_resting_order() {
        let arrived = "2025-01-01T00:00:00Z".parse::<DateTime<Utc>>().unwrap();
        let much_later = "2025-06-01T00:00:00Z".parse::<DateTime<Utc>>().unwrap();

        let mut venue = make_venue("100", "10000000");
        venue.account = AccountState::new(
            venue
                .account
                .balances()
                .cloned()
                .map(|balance| (balance.asset.clone(), balance))
                .collect(),
            [Order {
                key: OrderKey {
                    exchange: EXCHANGE,
                    instrument: instrument_name(),
                    strategy: StrategyId::new("test"),
                    cid: ClientOrderId::new("resting"),
                },
                side: Side::Buy,
                price: Some(d("50000")),
                quantity: d("1"),
                kind: OrderKind::Limit,
                time_in_force: TimeInForce::GoodUntilCancelled { post_only: false },
                state: Open {
                    id: OrderId::new("resting"),
                    time_exchange: arrived,
                    filled_quantity: Decimal::ZERO,
                },
            }]
            .into_iter()
            .collect(),
            Default::default(),
            Vec::new(),
        );

        venue.advance_time(much_later);

        let resting = venue.orders_open(&[]);
        assert_eq!(resting.len(), 1, "the seeded order must still be open");
        assert_eq!(
            resting[0].state.time_exchange, arrived,
            "an order does not become a different order because time passed"
        );

        // Balances do move: a venue restates them as of now, and they carry no ordering role.
        let usdt = venue.balances(&[quote()]);
        assert_eq!(usdt[0].time_exchange, much_later);
    }

    /// A cancel is rejected, and the rejection is the venue's rather than any driver's.
    ///
    /// Only Market orders are accepted and they fill on arrival, so nothing ever rests to be
    /// cancelled. Answering here — rather than in each driver — is what stops two drivers from
    /// carrying two copies of the same policy, and the response is the one the request's channel
    /// asks for, which the previous implementation could not produce.
    #[test]
    fn a_cancel_is_rejected_because_this_venue_rests_no_orders() {
        let mut venue = make_venue("100", "10000000");
        let key = buy_request("1", market_prices("50000")).key;

        let outcome = venue.cancel_order(OrderEvent {
            key: key.clone(),
            state: RequestCancel { id: None },
        });

        assert!(
            outcome.events.is_empty(),
            "a rejected cancel moves nothing, so it owes no events"
        );
        assert_eq!(
            outcome.response.key, key,
            "the response must name its request"
        );
        match outcome.response.state {
            Err(OrderError::Rejected(ApiError::OrderRejected(ref reason))) => {
                assert!(
                    reason.contains("CancelOrder"),
                    "the rejection must say what is unsupported, got: {reason}"
                );
            }
            ref other => panic!("expected a rejected cancel, got: {other:?}"),
        }

        // Nothing was cancelled, so nothing may appear in the ledger either.
        assert!(
            venue.account_snapshot().instruments.is_empty(),
            "a rejected cancel must not enter the ledger"
        );
    }

    /// `contract_size` of the CFD fixture below, as a per-point multiplier a real index CFD carries.
    const CFD_CONTRACT_SIZE: &str = "25";

    fn cfd_instrument_name() -> InstrumentNameExchange {
        InstrumentNameExchange::new("spx500_usd")
    }

    /// A USD-quoted index CFD settling in **GBP** — the case that makes the settlement asset differ
    /// from the quote asset — with **only the quote asset funded** and no `spx500` balance at all.
    /// An index is not deliverable, so requiring base inventory to short one would be unfundable.
    fn make_cfd_venue(usd: &str, fee_model: FeeModelConfig) -> SimulatedVenue {
        make_cfd_venue_with_balances(
            vec![funded(AssetNameExchange::new("usd"), d(usd))],
            fee_model,
        )
    }

    fn make_cfd_venue_with_balances(
        balances: Vec<AssetBalance<AssetNameExchange>>,
        fee_model: FeeModelConfig,
    ) -> SimulatedVenue {
        SimulatedVenue::new(
            &config_from_balances(balances, fee_model),
            instruments_of(Instrument {
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
            }),
        )
    }

    fn cfd_request(
        side: Side,
        quantity: &str,
        market: Option<MarketSnapshot>,
    ) -> OrderRequestOpen<ExchangeId, InstrumentNameExchange> {
        request(cfd_instrument_name(), side, quantity, market)
    }

    /// The multiplier must reach the ledger, or balances move 1x while the engine's PnL moves
    /// `contract_size`x — the same fill accounted two different ways.
    #[test]
    fn cfd_buy_debits_the_contract_size_scaled_notional() {
        let mut venue = make_cfd_venue("200000", FeeModelConfig::default());

        let outcome = venue.open_order(cfd_request(Side::Buy, "1", market_prices("5000")));

        assert!(
            outcome.response.state.is_accepted(),
            "cfd buy should fill: {:?}",
            outcome.response.state
        );
        assert_eq!(outcome.events.len(), 2);

        // 1 contract * 5000 * 25 = 125,000 of true notional, not the unmultiplied 5,000.
        let usd = venue
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
        let mut venue = make_cfd_venue("200000", FeeModelConfig::default());

        let outcome = venue.open_order(cfd_request(Side::Sell, "1", market_prices("5000")));

        assert!(
            outcome.response.state.is_accepted(),
            "cfd short should fill without base inventory: {:?}",
            outcome.response.state
        );
        let balance = filled_balance(&outcome);
        assert_eq!(
            balance.asset,
            AssetNameExchange::new("usd"),
            "a cash-settled short debits quote, not base"
        );
        assert_eq!(balance.balance.free, d("75000"));
    }

    /// The scaled notional is what the balance check tests, so an account that could fund the
    /// unmultiplied order must still be rejected.
    #[test]
    fn cfd_buy_is_rejected_when_only_the_unscaled_notional_is_funded() {
        let mut venue = make_cfd_venue("10000", FeeModelConfig::default());

        let outcome = venue.open_order(cfd_request(Side::Buy, "1", market_prices("5000")));

        assert!(outcome.events.is_empty());
        match outcome.response.state {
            OrderState::Inactive(InactiveOrderState::OpenFailed(OrderError::Rejected(
                ApiError::BalanceInsufficient(ref asset, _),
            ))) => {
                assert_eq!(*asset, AssetNameExchange::new("usd"));
            }
            other => panic!("expected BalanceInsufficient, got: {other:?}"),
        }
    }

    /// Fees stay quote-denominated on both sides of a CFD, are computed on the `contract_size`
    /// scaled notional, and are debited on top of it.
    ///
    /// Every amount below is hard-coded rather than read back from the event: an assertion that
    /// subtracts the reported fee from its own expected balance is satisfied by *any* fee,
    /// including the unscaled one this exists to rule out.
    #[test]
    fn cfd_fee_is_quote_denominated_on_both_sides() {
        for side in [Side::Buy, Side::Sell] {
            let mut venue = make_cfd_venue(
                "200000",
                // 0.1% of the scaled notional.
                FeeModelConfig::Percentage(PercentageFeeModel::new(d("0.001"))),
            );

            let outcome = venue.open_order(cfd_request(side, "1", market_prices("5000")));

            assert!(
                outcome.response.state.is_accepted(),
                "{side:?} should fill: {:?}",
                outcome.response.state
            );
            assert_eq!(
                filled_trade(&outcome).fees.asset,
                AssetNameExchange::new("usd"),
                "{side:?} fee asset"
            );

            // 0.001 * 5000 * 1 * 25. The unscaled answer is 5, so this pins the multiplier.
            assert_eq!(
                filled_trade(&outcome).fees.fees,
                d("125"),
                "{side:?} fee must be 0.1% of the contract_size-scaled notional"
            );
            assert_eq!(
                filled_balance(&outcome).balance.free,
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
    #[should_panic(expected = "SimulatedVenue has Balance for all configured Instrument assets")]
    fn cfd_panics_when_the_quote_asset_is_unfunded() {
        // A realistically funded GBP account: the settlement asset is present, the quote asset is
        // not. The mock cannot convert between them, so this is a mis-specified fixture.
        let mut venue = make_cfd_venue_with_balances(
            vec![funded(AssetNameExchange::new("gbp"), d("200000"))],
            FeeModelConfig::default(),
        );

        let _ = venue.open_order(cfd_request(Side::Buy, "1", market_prices("5000")));
    }

    #[test]
    fn sell_order_decrements_base_balance_not_quote() {
        let mut venue = make_venue("1.0", "10000");
        let initial_usdt = d("10000");

        let outcome = venue.open_order(sell_request("0.5", market_prices("50000")));

        assert!(
            outcome.response.state.is_accepted(),
            "sell should succeed: {:?}",
            outcome.response.state
        );
        assert_eq!(outcome.events.len(), 2, "a successful sell must notify");

        // Base (BTC) must be decremented by the quantity sold.
        let btc = venue.account.balance_mut(&base()).unwrap();
        assert_eq!(
            btc.balance.free,
            d("0.5"),
            "base balance should decrease by quantity sold"
        );

        // Quote (USDT) must be unchanged (fees = 0 in this test).
        let usdt = venue.account.balance_mut(&quote()).unwrap();
        assert_eq!(
            usdt.balance.free, initial_usdt,
            "quote balance should be unchanged on sell"
        );
    }

    /// A derivative must be rejected, not filled down the physically-settled spot path.
    ///
    /// The fixture is mutated through the public `instruments` map on purpose: that is exactly the
    /// route a consumer takes when it builds a venue itself rather than through the `rustrade`
    /// builder, and it is the route that had no gate on it.
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
            let mut venue = make_venue("10", "10000000");
            venue.instruments.get_mut(&instrument_name()).unwrap().kind = kind.clone();

            let outcome = venue.open_order(buy_request("1.0", market_prices("50000")));

            assert!(
                outcome.events.is_empty(),
                "{kind:?} must produce no account events"
            );
            assert!(
                matches!(
                    outcome.response.state,
                    OrderState::Inactive(InactiveOrderState::OpenFailed(
                        UnindexedOrderError::Connectivity(_) | UnindexedOrderError::Rejected(_)
                    ))
                ),
                "{kind:?} must be rejected, got {:?}",
                outcome.response.state
            );

            // And the ledger must be untouched -- a rejected order that still moved cash would be
            // worse than one that filled.
            let usdt = venue.account.balance_mut(&quote()).unwrap();
            assert_eq!(
                usdt.balance.free,
                d("10000000"),
                "{kind:?} must not move the quote balance"
            );
        }
    }

    #[test]
    fn sell_order_insufficient_balance_names_base_asset() {
        // Regression guard for a sell-side balance bug: previously `balance_mut(&underlying.quote)`
        // was called for sells, so BalanceInsufficient would name the quote asset (USDT) instead of
        // the base (BTC).
        let mut venue = make_venue("0.1", "10000");

        // Selling 1 BTC but only 0.1 available.
        let outcome = venue.open_order(sell_request("1.0", market_prices("50000")));

        assert!(
            outcome.events.is_empty(),
            "failed order must produce no account events"
        );
        match outcome.response.state {
            OrderState::Inactive(InactiveOrderState::OpenFailed(OrderError::Rejected(
                ApiError::BalanceInsufficient(ref asset, _),
            ))) => {
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
        let mut venue = make_venue("0", "10000"); // 0 BTC, 10 000 USDT
        venue.fill_model = SimFillConfig::BidAsk(BidAskFillModel);

        let market = Some(MarketSnapshot {
            best_bid: Some(d("99.5")),
            best_ask: Some(d("100.5")),
            last_price: Some(d("100.0")),
        });

        // Market buy of 1 BTC; last_price 100 is only a fallback for when the fill model returns
        // None — BidAsk returns best_ask, so it is not used.
        let outcome = venue.open_order(buy_request("1", market));

        assert!(
            outcome.response.state.is_accepted(),
            "buy should succeed: {:?}",
            outcome.response.state
        );

        // BidAskFillModel: market buy fills at best_ask = 100.5, not last_price 100.0.
        assert_eq!(
            filled_trade(&outcome).price,
            d("100.5"),
            "fill price must be best_ask"
        );

        // Balance deduction: 1 * 100.5 = 100.5 USDT; fee_model = Zero.
        let usdt = venue.account.balance_mut(&quote()).unwrap();
        assert_eq!(
            usdt.balance.free,
            d("9899.5"),
            "quote balance must decrease by fill_price * qty"
        );
    }

    #[test]
    fn percentage_fee_model_deducts_correct_fee_on_buy() {
        // 0.1% fee rate
        let fee_model = FeeModelConfig::Percentage(PercentageFeeModel::new(d("0.001")));
        let mut venue = make_venue_with_fee("0", "10000", fee_model);

        // Buy 10 BTC at price 100 USDT each
        // Notional = 10 * 100 = 1000 USDT
        // Fee = 1000 * 0.001 = 1 USDT
        // Total deducted = 1000 + 1 = 1001 USDT
        let outcome = venue.open_order(buy_request("10", market_prices("100")));

        assert!(
            outcome.response.state.is_accepted(),
            "buy should succeed: {:?}",
            outcome.response.state
        );

        // Trade must report fee in quote denomination
        assert_eq!(
            filled_trade(&outcome).fees.fees,
            d("1"),
            "trade fee must be 1 USDT"
        );

        // Quote balance: 10000 - 1001 = 8999
        let usdt = venue.account.balance_mut(&quote()).unwrap();
        assert_eq!(
            usdt.balance.free,
            d("8999"),
            "quote balance must decrease by notional + fee"
        );
    }

    #[test]
    fn percentage_fee_model_deducts_correct_fee_on_sell() {
        // 0.1% fee rate
        let fee_model = FeeModelConfig::Percentage(PercentageFeeModel::new(d("0.001")));
        let mut venue = make_venue_with_fee("10", "0", fee_model);

        // Sell 1 BTC at price 100 USDT
        // Notional = 1 * 100 = 100 USDT
        // Fee (quote) = 100 * 0.001 = 0.1 USDT
        // Fee (base) = 0.1 / 100 = 0.001 BTC
        // Total base deducted = 1 + 0.001 = 1.001 BTC
        let outcome = venue.open_order(sell_request("1", market_prices("100")));

        assert!(
            outcome.response.state.is_accepted(),
            "sell should succeed: {:?}",
            outcome.response.state
        );

        // Trade must report fee in quote denomination
        assert_eq!(
            filled_trade(&outcome).fees.fees,
            d("0.1"),
            "trade fee must be 0.1 USDT"
        );

        // Base balance: 10 - 1.001 = 8.999
        let btc = venue.account.balance_mut(&base()).unwrap();
        assert_eq!(
            btc.balance.free,
            d("8.999"),
            "base balance must decrease by quantity + fee_in_base"
        );
    }

    #[test]
    fn percentage_fee_with_zero_price_returns_zero_fee() {
        // Edge case: if fill_price is zero, fee computation must not divide by zero
        let fee_model = FeeModelConfig::Percentage(PercentageFeeModel::new(d("0.001")));
        let mut venue = make_venue_with_fee("10", "0", fee_model);

        // Sell 1 BTC at price 0 (degenerate case)
        // Fee (quote) = 0 * 0.001 * 1 = 0
        // Fee (base) = guarded by is_zero() check, returns 0
        let outcome = venue.open_order(sell_request("1", market_prices("0")));

        assert!(
            outcome.response.state.is_accepted(),
            "sell at zero price should succeed: {:?}",
            outcome.response.state
        );

        // Fee must be zero (not NaN or panic from division by zero)
        assert_eq!(
            filled_trade(&outcome).fees.fees,
            Decimal::ZERO,
            "fee must be zero when price is zero"
        );

        // Base balance: 10 - 1 = 9 (no fee deducted)
        let btc = venue.account.balance_mut(&base()).unwrap();
        assert_eq!(
            btc.balance.free,
            d("9"),
            "base balance must decrease by quantity only"
        );
    }

    /// A Market order the venue cannot price is rejected, not panicked on.
    ///
    /// A Market order carries no limit price of its own to fall back on, so with no usable
    /// snapshot there is no price anywhere. This used to hit an `expect`. Because the async driver
    /// runs as a spawned task, that killed the exchange, dropped the request receiver, and turned
    /// every later order into `ExchangeOffline` -- so a backtest ran to completion over the whole
    /// dataset having filled nothing and surfaced only a `JoinError` at teardown.
    ///
    /// Both unpriceable cases are covered, because they mean different things: `None` is a caller
    /// that sampled no snapshot at all, `Some(empty)` is a cold start where one was sampled and the
    /// instrument had no price yet.
    #[test]
    fn a_market_order_that_cannot_be_priced_is_rejected_rather_than_panicking() {
        for market in [None, Some(MarketSnapshot::default())] {
            let mut venue = make_venue("10", "10000000");

            let outcome = venue.open_order(buy_request("1.0", market));

            assert!(
                outcome.events.is_empty(),
                "a rejected order must not notify a fill (market={market:?})"
            );
            assert!(
                matches!(
                    outcome.response.state,
                    OrderState::Inactive(InactiveOrderState::OpenFailed(
                        UnindexedOrderError::Rejected(_)
                    ))
                ),
                "expected a rejection, got {:?} (market={market:?})",
                outcome.response.state
            );

            let usd = venue.account.balance_mut(&quote()).unwrap();
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
            let mut venue = make_venue("10", "10000000");
            match venue.open_order(buy_request("1.0", market)).response.state {
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

    /// A Market order fills at the snapshot the request carried, and at nothing else.
    ///
    /// With the default `LastPriceFillModel` the fill price is the snapshot's `last_price`, so this
    /// pins that a market order priced purely from `RequestOpen::market` both fills and fills at
    /// the right price.
    #[test]
    fn a_market_order_fills_at_the_price_its_snapshot_carried() {
        let mut venue = make_venue("0", "10000");

        let outcome = venue.open_order(buy_request(
            "1.0",
            Some(MarketSnapshot::from_last_price(Some(d("100")))),
        ));

        let OrderState::Inactive(InactiveOrderState::FullyFilled(ref filled)) =
            outcome.response.state
        else {
            panic!("expected a filled order, got {:?}", outcome.response.state)
        };
        assert_eq!(
            filled.avg_price,
            Some(d("100")),
            "a market order must fill at its snapshot's last price"
        );
        assert_eq!(filled.filled_quantity, d("1.0"));

        assert_eq!(filled_trade(&outcome).price, d("100"));

        let usd = venue.account.balance_mut(&quote()).unwrap();
        assert_eq!(
            usd.balance.free,
            d("9900"),
            "a 1 BTC buy at 100 must debit 100 quote"
        );
    }

    /// A configured reservation is honoured, not refused.
    ///
    /// `total != free` is ordinary user configuration -- an `initial_state` copied from a live
    /// account with margin reserved. It was once an `assert_eq!` that panicked the exchange task on
    /// the first order, then a rejection, because a ledger where every order filled on arrival had
    /// no way to represent an amount held back. It has one now: the order draws on `free`, and the
    /// held amount is still held afterwards.
    #[test]
    fn a_configured_quote_reservation_survives_a_fill() {
        let mut venue = make_venue("10", "10000000");
        venue.account.balance_mut(&quote()).unwrap().balance.total = d("20000000");

        let outcome = venue.open_order(buy_request("1.0", market_prices("50000")));

        assert!(
            matches!(outcome.response.state, OrderState::Inactive(_)),
            "a market order fills on arrival, got {:?}",
            outcome.response.state
        );

        let usdt = venue.account.balance_mut(&quote()).unwrap().balance;
        assert_eq!(
            usdt.free,
            d("10000000") - d("50000"),
            "the fill draws the notional from the spendable side"
        );
        assert_eq!(
            usdt.total - usdt.free,
            d("10000000"),
            "the configured reservation must be untouched by an unrelated fill"
        );
    }

    /// The sell path debits the base asset, so the reservation must survive there too.
    #[test]
    fn a_configured_base_reservation_survives_a_fill() {
        let mut venue = make_venue("10", "10000000");
        venue.account.balance_mut(&base()).unwrap().balance.total = d("20");

        let outcome = venue.open_order(sell_request("1.0", market_prices("50000")));

        assert!(
            matches!(outcome.response.state, OrderState::Inactive(_)),
            "a market order fills on arrival, got {:?}",
            outcome.response.state
        );

        let btc = venue.account.balance_mut(&base()).unwrap().balance;
        assert_eq!(
            btc.free,
            d("10") - d("1"),
            "the fill delivers one base unit from the spendable side"
        );
        assert_eq!(
            btc.total - btc.free,
            d("10"),
            "the configured reservation must be untouched by an unrelated fill"
        );
    }

    /// An order larger than `free` is refused even when `total` would cover it.
    ///
    /// The whole point of a reservation is that the held portion is not spendable. Sizing against
    /// `total` would let one order spend what another is already holding.
    #[test]
    fn a_reservation_is_not_spendable() {
        let mut venue = make_venue("10", "100");
        // free 100, total 10_000_000: only 100 may be spent.
        venue.account.balance_mut(&quote()).unwrap().balance.total = d("10000000");

        let outcome = venue.open_order(buy_request("1.0", market_prices("50000")));

        assert!(
            matches!(
                outcome.response.state,
                OrderState::Inactive(InactiveOrderState::OpenFailed(
                    UnindexedOrderError::Rejected(ApiError::BalanceInsufficient(_, _))
                ))
            ),
            "expected an insufficient-balance rejection, got {:?}",
            outcome.response.state
        );
        assert!(
            outcome.events.is_empty(),
            "a refused order moves nothing, so it restates nothing"
        );

        let usdt = venue.account.balance_mut(&quote()).unwrap().balance;
        assert_eq!(usdt.free, d("100"), "a refused reserve must not move free");
        assert_eq!(
            usdt.total,
            d("10000000"),
            "a refused reserve must not move total"
        );
    }

    /// Orders come back in a total order, so two runs of one backtest produce one snapshot.
    ///
    /// The snapshot sorts with an *unstable* sort over a `FnvHashMap`'s values. Keyed on
    /// `instrument` alone every order on one instrument ties, leaving their relative order
    /// unspecified -- so a golden hash over the snapshot is a flake, and comparing two runs of the
    /// same backtest compares two different orderings. `cid` is unique per order, which makes the
    /// key total.
    #[test]
    fn an_account_snapshot_orders_one_instruments_orders_deterministically() {
        // Enough orders that a hash map's own iteration order is vanishingly unlikely to be sorted
        // by accident -- with three, a passing assertion would say nothing.
        const CIDS: [&str; 8] = ["h", "d", "a", "g", "c", "f", "b", "e"];

        let resting = |cid: &str| UnindexedOrder {
            key: OrderKey {
                exchange: EXCHANGE,
                instrument: instrument_name(),
                strategy: StrategyId::new("test"),
                cid: ClientOrderId::new(cid),
            },
            side: Side::Buy,
            price: Some(d("50000")),
            quantity: d("1"),
            kind: OrderKind::Limit,
            time_in_force: TimeInForce::GoodUntilCancelled { post_only: false },
            state: OrderState::active(Open {
                id: OrderId::new(cid),
                time_exchange: Default::default(),
                filled_quantity: Decimal::ZERO,
            }),
        };

        let mut config = spot_config("10", "10000000", FeeModelConfig::default());
        config.initial_state.instruments = vec![InstrumentAccountSnapshot {
            instrument: instrument_name(),
            orders: CIDS.into_iter().map(resting).collect(),
            position: None,
            isolated: None,
        }];

        let venue = SimulatedVenue::new(&config, spot_instruments());

        let cids = venue
            .account_snapshot()
            .instruments
            .into_iter()
            .flat_map(|instrument| instrument.orders)
            .map(|order| order.key.cid)
            .collect::<Vec<_>>();

        let mut expected = CIDS.map(ClientOrderId::new).to_vec();
        expected.sort();
        assert_eq!(
            cids, expected,
            "orders on one instrument must be ordered by a key no two of them share"
        );
    }

    /// A limit order is rejected before any fill model is consulted.
    ///
    /// This pins the vacuity that makes the current state safe. A [`FillModel`] no longer honours a
    /// limit price — bounding a fill by the order's own terms belongs to this venue — but the clamp
    /// that will do so is not written yet, because nothing can reach it:
    /// [`validate_order_kind_supported`](SimulatedVenue::validate_order_kind_supported) returns
    /// `Err` for every kind but `Market`, and `Market` carries no price.
    ///
    /// The snapshot is chosen so the omission would be visible if the guard ever stopped holding.
    /// `MidpointFillModel` would price this buy at 50,000 against a limit of 40,000 — a fill 25%
    /// above the order's own limit — so a `Filled` response here means the clamp is owed and
    /// missing, not merely that a rejection message changed.
    #[test]
    fn a_limit_order_is_rejected_before_a_fill_model_is_consulted() {
        let mut venue = make_venue("100", "10000000");
        venue.fill_model = SimFillConfig::Midpoint(crate::fill::MidpointFillModel);

        let mut request = buy_request("1", market_prices("50000"));
        request.state.kind = OrderKind::Limit;
        request.state.price = Some(d("40000"));

        let outcome = venue.open_order(request);

        assert!(
            outcome.events.is_empty(),
            "a rejected order moves no balance and books no trade"
        );
        match outcome.response.state {
            OrderState::Inactive(InactiveOrderState::OpenFailed(OrderError::Rejected(
                ApiError::OrderRejected(ref reason),
            ))) => assert!(
                reason.contains("does not support OrderKind::Limit"),
                "the rejection must name the unsupported kind, got: {reason}"
            ),
            other => panic!("a limit order must be rejected by kind, got: {other:?}"),
        }
    }
}
