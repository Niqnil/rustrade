//! The simulated venue's state machine, independent of any transport that drives it.

use crate::{
    AccountEventKind, InstrumentAccountSnapshot, UnindexedAccountEvent, UnindexedAccountSnapshot,
    balance::AssetBalance,
    client::mock::MockExecutionConfig,
    error::{ApiError, UnindexedApiError, UnindexedOrderError},
    exchange::mock::{
        account::AccountState,
        orders::{OpenOrder, Reservation, RestingOrder},
    },
    fee::{FeeModel, FeeModelConfig, Liquidity},
    fill::{FillContext, FillModel, SimFillConfig},
    market::MarketSnapshot,
    order::{
        Order, OrderKind, TimeInForce, UnindexedOrder,
        id::{ClientOrderId, OrderId},
        request::{OrderRequestCancel, OrderRequestOpen, UnindexedOrderResponseCancel},
        state::{Cancelled, Filled, Open, OrderState, UnindexedOrderState},
    },
    trade::{AssetFees, Trade, TradeId},
};
use chrono::{DateTime, Utc};
use fnv::FnvHashMap;
use itertools::Itertools;
use rust_decimal::Decimal;
use rustrade_instrument::{
    Side, Underlying,
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

/// Which price source a [`SimulatedVenue`] has, and therefore which orders it can accept.
///
/// A venue's regime is fixed at construction and is the gate on [`OrderKind::Limit`]. Without it a
/// limit order placed through a driver with no market feed would be accepted and then rest forever
/// with nothing that could ever match it — accepted-and-silently-never-filled, which is worse than
/// a flat rejection naming the reason.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VenueRegime {
    /// No market feed: a market order is priced from the snapshot its own request carries, and
    /// nothing can rest, so [`OrderKind::Limit`] is rejected.
    ///
    /// The regime of [`SimulatedVenue::new`], and what [`MockExchange`](super::MockExchange)
    /// drives.
    RequestPriced,

    /// A driver feeds [`apply_market`](SimulatedVenue::apply_market), so an order can rest and be
    /// matched against that feed later.
    ///
    /// The regime of [`SimulatedVenue::new_market_driven`], and what `SimRunner` drives.
    MarketDriven,
}

/// Simulated venue state machine: synchronous, transport-free and latency-free.
///
/// Fills every market order immediately, rests limit orders that the market has not reached, and
/// keeps its own balance ledger. It has no channels and never sleeps, so a driver decides *when*
/// its output reaches a client while the venue decides *what* that output is and in which order it
/// must arrive — see [`VenueOutcome`].
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
/// - **Which order kinds are accepted depends on the [`VenueRegime`].** [`OrderKind::Market`]
///   always; [`OrderKind::Limit`] only on a [`MarketDriven`](VenueRegime::MarketDriven) venue;
///   every other kind never.
/// - **A resting order seeded by `initial_state` reserves nothing, because this venue never took
///   it.** An account snapshot says an order is resting and says what the balances are, but not
///   which order any held portion belongs to. Such an order still matches, and its fill debits the
///   ledger then — so the seeded balances must cover it, exactly as they must cover a market order.
///   They cannot? That is a mis-specified fixture, and it **panics**, in the same class as an
///   absent balance. Cancelling one releases nothing and restates no balance.
///
/// # How a limit order is priced
/// - **Marketable on arrival** — priced by the [`FillModel`] against this venue's market, then
///   clamped to the order's limit: a buy never fills above it, a sell never below. Charged
///   [`Liquidity::Taker`].
/// - **Resting** — fills at its own limit price exactly, and never reaches the [`FillModel`].
///   Deriving a better price from the book would credit price improvement no resting order can
///   obtain: a maker is paid the price it quoted, and improvement accrues to the aggressor that
///   crossed it. Charged [`Liquidity::Maker`].
///
/// The two agree exactly at the crossing point — an order arriving when `best_ask == limit` fills
/// at the limit either way — and diverge only where the order genuinely is, or is not, marketable.
/// Marketability is judged on `best_ask`/`best_bid`, falling back to `last_price` when the feed
/// supplies no book: with the default `LastPriceFillModel` and a trades-only feed there is no book
/// at all, so a book-only rule would never match anything.
///
/// A market order is unaffected by any of this. It is still priced from the snapshot its own
/// request carried, which is what keeps a backtest's market-order fills identical whether or not a
/// driver feeds this venue a market.
///
/// # What matching does not model
/// - **No queue position.** An order that crosses fills in full, whatever size rests ahead of it.
///   [`OpenOrders`] ranks orders by price then arrival, so *which* of this account's orders fills
///   first is reproducible — but the book it is matched against has no sizes to be ahead in.
/// - **No size cap, and so no partial fills.** A crossing order fills its whole quantity at one
///   price. Every order is therefore either untouched or terminal, and
///   [`Cancelled::filled_quantity`] is always zero.
/// - **[`TimeInForce`] beyond [`GoodUntilCancelled`](TimeInForce::GoodUntilCancelled) with
///   `post_only: false` is rejected on a limit order**, rather than treated as good-until-cancelled
///   — which would silently leave an order working that its sender asked to have cancelled. A
///   market order fills in full on arrival, which honours every time in force, so the restriction
///   does not reach one.
///
/// [`OpenOrders`]: super::orders::OpenOrders
///
/// # Reserved balances
/// The ledger distinguishes held from spendable: `free` is what an order may draw on, `total` is
/// what the account holds, and the difference is held against something. An `initial_state` copied
/// from a live account with margin reserved is therefore usable as configured — it was rejected
/// outright while every order filled on arrival and the ledger could not represent the state.
///
/// An order that fills on arrival reserves and settles in one step
/// ([`AccountState::debit_filled`]), so it emits **one** balance restatement. An order that rests
/// reserves when it is booked and settles that same amount when it fills, so it emits one
/// restatement then and one more on the fill — never an intermediate balance the account did not
/// hold. A configured reservation is carried through untouched: settling lowers `total` by the
/// settled amount and leaves the rest held.
///
/// The reservation is **exactly** what the fill will settle, not a conservative over-estimate: both
/// come from one computation at the order's own limit and its own liquidity side. A conservative
/// reservation would have to be released and re-debited on the fill, producing two or three
/// restatements for one fill and reporting balances the account never held, while
/// [`Balance::used`](crate::balance::Balance::used) misreported for the order's whole life.
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
    /// Which price source this venue has. Private: it is fixed at construction, and a consumer
    /// flipping it on a venue already holding resting orders would leave them unmatchable with no
    /// way to find out. Read through [`regime`](Self::regime).
    regime: VenueRegime,
}

impl SimulatedVenue {
    /// A venue with no market feed, which prices each market order from the snapshot its own
    /// request carries and accepts no [`OrderKind::Limit`].
    ///
    /// See [`VenueRegime::RequestPriced`], and [`new_market_driven`](Self::new_market_driven) for
    /// the regime that accepts limits.
    pub fn new(
        config: &MockExecutionConfig,
        instruments: FnvHashMap<InstrumentNameExchange, Instrument<ExchangeId, AssetNameExchange>>,
    ) -> Self {
        Self::with_regime(config, instruments, VenueRegime::RequestPriced)
    }

    /// A venue whose driver feeds [`apply_market`](Self::apply_market), so limit orders are
    /// accepted and a resting one is matched against that feed.
    ///
    /// Constructing one and then never calling `apply_market` produces a venue that accepts limit
    /// orders and can never match them, which is the state the regime exists to prevent — so this
    /// constructor is a driver's assertion that it will feed the venue, not a configuration knob.
    ///
    /// See [`VenueRegime::MarketDriven`].
    pub fn new_market_driven(
        config: &MockExecutionConfig,
        instruments: FnvHashMap<InstrumentNameExchange, Instrument<ExchangeId, AssetNameExchange>>,
    ) -> Self {
        Self::with_regime(config, instruments, VenueRegime::MarketDriven)
    }

    fn with_regime(
        config: &MockExecutionConfig,
        instruments: FnvHashMap<InstrumentNameExchange, Instrument<ExchangeId, AssetNameExchange>>,
        regime: VenueRegime,
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
            regime,
        }
    }

    /// Which price source this venue has, and so which order kinds it accepts.
    pub fn regime(&self) -> VenueRegime {
        self.regime
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
    /// # What it prices, and what it does not
    /// This is the market a **resting** order is matched against, and the one a limit order's
    /// marketability is judged on — see the type's `# How a limit order is priced`.
    ///
    /// A market order is deliberately *not* priced from it. It is still priced from the snapshot
    /// its own request carried
    /// ([`RequestOpen::market`](crate::order::request::RequestOpen::market)), which is what keeps
    /// this venue's market-order results identical whether or not a driver feeds it.
    ///
    /// # Returns the fills it caused, which a driver must deliver
    /// Applying a market is what makes a resting order fill, so this returns every account event
    /// those fills produced, in the order they must be delivered — the same obligation
    /// [`VenueOutcome`] carries for a request. Per filled order the events are
    /// `[balance, trade, order]`: the trade precedes the terminal order snapshot, because a
    /// consumer routing a fill to a position needs the order still to be live when the trade
    /// arrives.
    ///
    /// `#[must_use]` because a driver that applies the market and drops the result has silently
    /// eaten the fills, leaving its client holding an order the venue no longer has.
    #[must_use]
    pub fn apply_market(
        &mut self,
        instrument: &InstrumentNameExchange,
        snapshot: MarketSnapshot,
        time_exchange: DateTime<Utc>,
    ) -> Vec<UnindexedAccountEvent> {
        let entry = self.market.entry(instrument.clone()).or_default();
        entry.snapshot = snapshot;
        entry.time_exchange = time_exchange;

        self.match_resting(instrument, snapshot, time_exchange)
    }

    /// Fills every resting order on `instrument` that `snapshot` has moved to or through.
    ///
    /// Walks each side best-first and stops at the first order that does not cross, which
    /// [`OpenOrders::resting`]'s price-time ordering makes correct: the crossing test is monotone
    /// in price down a side, so the first order that fails it is followed only by worse ones.
    ///
    /// [`OpenOrders::resting`]: super::orders::OpenOrders::resting
    fn match_resting(
        &mut self,
        instrument: &InstrumentNameExchange,
        snapshot: MarketSnapshot,
        time_exchange: DateTime<Utc>,
    ) -> Vec<UnindexedAccountEvent> {
        // Collected before anything is filled: matching removes from the very book it walks, and
        // each entry carries its own limit so the fill loop never re-reads an order it just took
        // off the book. `resting` only yields queued orders, and only a priced order is queued.
        let mut crossing: Vec<(ClientOrderId, Decimal)> = Vec::new();
        for side in [Side::Buy, Side::Sell] {
            crossing.extend(
                self.account
                    .orders()
                    .resting(instrument, side)
                    .take_while(|order| {
                        order
                            .price
                            .is_some_and(|limit| crosses(side, limit, &snapshot))
                    })
                    .filter_map(|order| Some((order.key.cid.clone(), order.price?))),
            );
        }

        if crossing.is_empty() {
            return Vec::new();
        }

        // One lookup for the whole instrument: every crossing order is on it by construction.
        let terms = match self.instrument_terms(instrument) {
            Ok(terms) => terms,
            // Unreachable: an order cannot rest on an instrument this venue rejected at open. A
            // venue whose `instruments` map was mutated afterwards would reach it, and dropping the
            // fills silently is the one thing this method must not do.
            Err(error) => panic!(
                "SimulatedVenue holds resting orders on an instrument it cannot price: {error}"
            ),
        };

        let mut events = Vec::with_capacity(crossing.len() * 3);

        for (cid, limit) in crossing {
            // Collected from this same book with nothing in between, so this always finds it.
            let Some(RestingOrder { order, reservation }) = self.account.orders_mut().remove(&cid)
            else {
                continue;
            };

            // A resting order fills at its own limit, never at a price derived from the book --
            // see the type's `# How a limit order is priced` -- and takes no liquidity, so it is
            // charged the maker rate.
            let settlement =
                self.settlement(&terms, order.side, order.quantity, limit, Liquidity::Maker);

            let balance = self.settle_resting(&order, reservation, &settlement, time_exchange);

            let order_id = order.state.id.clone();
            let trade = Trade {
                id: TradeId(order_id.0.clone()),
                order_id: order_id.clone(),
                instrument: order.key.instrument.clone(),
                strategy: order.key.strategy.clone(),
                time_exchange,
                side: order.side,
                price: limit,
                quantity: order.quantity,
                fees: settlement.fees,
            };

            self.account.ack_trade(trade.clone());
            self.account.ack_filled(cid);

            // The order is terminal, and says so as `Inactive(FullyFilled)` rather than as an
            // `Open` carrying a complete fill. Both denote the same fact, but only this one can
            // carry the price it filled at.
            let filled = Order {
                key: order.key,
                side: order.side,
                price: order.price,
                quantity: order.quantity,
                kind: order.kind,
                time_in_force: order.time_in_force,
                state: OrderState::fully_filled(Filled::new(
                    order_id,
                    time_exchange,
                    order.quantity,
                    Some(limit),
                )),
            };

            events.push(self.build_account_event(Snapshot(balance)));
            events.push(self.build_account_event(trade));
            events.push(self.build_account_event(Snapshot(filled)));
        }

        events
    }

    /// Moves the balance one resting fill pays with, and returns the restatement it owes.
    ///
    /// # Panics
    /// Panics if an order the venue holds nothing against cannot be afforded when it fills — see
    /// this type's caller obligations on `initial_state`.
    fn settle_resting(
        &mut self,
        order: &OpenOrder,
        reservation: Option<Reservation>,
        settlement: &Settlement,
        time_exchange: DateTime<Utc>,
    ) -> AssetBalance<AssetNameExchange> {
        let Some(Reservation { asset, amount }) = reservation else {
            // Seeded by a configured `initial_state`, so nothing is held against it and the fill
            // debits now. An account that cannot cover it is a mis-specified fixture, in the same
            // class as an absent balance, and gets the same treatment.
            #[allow(clippy::expect_used)] // Documented panic: a mis-specified `initial_state`.
            return self
                .account
                .debit_filled(&settlement.asset, settlement.amount, time_exchange)
                .unwrap_or_else(|insufficient| {
                    panic!(
                        "SimulatedVenue cannot afford the fill of resting order {}, which was \
                         seeded by `initial_state` and so reserved nothing: {} of {} free, {} \
                         required",
                        order.key.cid, insufficient.free, settlement.asset, insufficient.required
                    )
                });
        };

        // Reserve exactly what will be settled, then settle exactly what was reserved: the client
        // sees one balance restatement per fill, and never a balance the account did not hold.
        debug_assert!(
            asset == settlement.asset && amount == settlement.amount,
            "resting order {} reserved {amount} of {asset} but its fill settles {} of {}: the \
             reservation and the fill disagree, so a balance the account never held would be \
             reported",
            order.key.cid,
            settlement.amount,
            settlement.asset
        );

        self.account.settle(&asset, amount, time_exchange)
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
    /// cannot forget to. A filled open reports `[balance, trade]`, mirroring a real venue where the
    /// trade is booked before the order can be reported `FullyFilled`; an open that rests reports
    /// `[balance]` alone -- the reservation now held against it -- and no trade, because nothing
    /// traded.
    ///
    /// A Market order carries no limit price, so the venue prices it against
    /// [`RequestOpen::market`](crate::order::request::RequestOpen::market) -- the snapshot its
    /// sender stamped at decision time. A Limit order is judged and priced against this venue's own
    /// market instead; see the type's `# How a limit order is priced`.
    pub fn open_order(
        &mut self,
        request: OrderRequestOpen<ExchangeId, InstrumentNameExchange>,
    ) -> OpenOutcome {
        // Single source: the snapshot the request carries. Passing it separately would allow two
        // copies on one path to disagree, with nothing to arbitrate.
        let market = request.state.market;
        let (response, notifications) = self.open_order_inner(request, market);

        let events = match notifications {
            Some(OpenOrderNotifications::Filled { balance, trade }) => {
                // Booked before the events are built, so a subsequent request on this venue sees
                // the trade regardless of when a driver gets around to delivering them.
                self.account.ack_trade(trade.clone());
                self.account.ack_filled(response.key.cid.clone());
                vec![
                    self.build_account_event(balance),
                    self.build_account_event(trade),
                ]
            }
            // Already on the book: `open_order_inner` put it there, holding the reservation this
            // balance restates.
            Some(OpenOrderNotifications::Rested { balance }) => {
                vec![self.build_account_event(balance)]
            }
            None => Vec::new(),
        };

        VenueOutcome { events, response }
    }

    /// Takes a resting order off the book, releasing whatever is held against it.
    ///
    /// Returns `[balance]` and a [`Cancelled`] carrying the quantity filled before the cancel
    /// arrived -- zero for every order this venue can currently rest, since it models no partial
    /// fills. An order the venue holds nothing against releases nothing and reports no balance;
    /// see this type's note on `initial_state`.
    ///
    /// # A cancel that finds nothing says which nothing it found
    /// A cancel racing its own order's fill is the reason this distinguishes three cases rather
    /// than rejecting flatly. With a non-zero `to_venue` latency the fill can win, and
    /// [`ApiError::OrderAlreadyFullyFilled`] is what tells the caller to reconcile against the fill
    /// rather than retry. An order cancelled twice gets [`ApiError::OrderAlreadyCancelled`], and
    /// only an id this venue has never booked is rejected as unknown.
    pub fn cancel_order(
        &mut self,
        request: OrderRequestCancel<ExchangeId, InstrumentNameExchange>,
    ) -> CancelOutcome {
        let time_exchange = self.time_exchange();

        let Some(RestingOrder { order, reservation }) =
            self.account.orders_mut().remove(&request.key.cid)
        else {
            return VenueOutcome {
                events: Vec::new(),
                response: UnindexedOrderResponseCancel {
                    state: Err(self.cancel_missing_reason(&request.key.cid)),
                    key: request.key,
                },
            };
        };

        // Released before the response is built, so a request arriving after this one sees the
        // freed balance regardless of when a driver delivers the restatement.
        let events = match reservation {
            Some(Reservation { asset, amount }) => {
                let balance = self.account.release(&asset, amount, time_exchange);
                vec![self.build_account_event(Snapshot(balance))]
            }
            None => Vec::new(),
        };

        let cancelled = Cancelled {
            id: order.state.id.clone(),
            time_exchange,
            filled_quantity: order.state.filled_quantity,
        };

        self.account.ack_cancelled(Order {
            key: order.key,
            side: order.side,
            price: order.price,
            quantity: order.quantity,
            kind: order.kind,
            time_in_force: order.time_in_force,
            state: cancelled.clone(),
        });

        VenueOutcome {
            events,
            response: UnindexedOrderResponseCancel {
                key: request.key,
                state: Ok(cancelled),
            },
        }
    }

    /// Why a cancel found no resting order under `cid`.
    fn cancel_missing_reason(&self, cid: &ClientOrderId) -> UnindexedOrderError {
        if self.account.is_filled(cid) {
            ApiError::OrderAlreadyFullyFilled.into()
        } else if self.account.is_cancelled(cid) {
            ApiError::OrderAlreadyCancelled.into()
        } else {
            UnindexedOrderError::Rejected(ApiError::OrderRejected(format!(
                "SimulatedVenue is not holding an open order with {cid}"
            )))
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

    /// Decides one open's fate, moves the balance it pays with, and commits nothing else.
    ///
    /// Kept separate from [`open_order`](Self::open_order) rather than inlined: this body has
    /// several early returns and the ordering contract -- ack the trade, then emit balance before
    /// trade -- is the part a reader needs to find. Splitting keeps that contract in a short caller
    /// instead of at the end of a long one.
    ///
    /// `market` is the snapshot the request carried, and is used only by an order that is priced
    /// from it -- see the type's `# How a limit order is priced`.
    fn open_order_inner(
        &mut self,
        request: OrderRequestOpen<ExchangeId, InstrumentNameExchange>,
        market: Option<MarketSnapshot>,
    ) -> (
        Order<ExchangeId, InstrumentNameExchange, UnindexedOrderState>,
        Option<OpenOrderNotifications>,
    ) {
        if let Err(error) = self.validate_order_kind_supported(request.state.kind) {
            return (build_open_order_err_response(request, error), None);
        }

        if let Err(error) =
            validate_time_in_force_supported(request.state.kind, request.state.time_in_force)
        {
            return (build_open_order_err_response(request, error), None);
        }

        // Read out before the `&mut self` balance borrows below.
        let terms = match self.instrument_terms(&request.key.instrument) {
            Ok(terms) => terms,
            Err(error) => return (build_open_order_err_response(request, error), None),
        };

        if request.state.kind != OrderKind::Limit {
            // Every other kind this venue accepts is marketable on arrival by definition.
            return self.fill_on_arrival(request, market, &terms);
        }

        let Some(limit) = request.state.price else {
            let reason = format!(
                "cannot open {} for {}: OrderKind::Limit carries no limit price",
                request.key.instrument, request.key.exchange
            );
            return (
                build_open_order_err_response(
                    request,
                    UnindexedOrderError::Rejected(ApiError::OrderRejected(reason)),
                ),
                None,
            );
        };

        // A limit order is judged against this venue's own market, not the requester's snapshot:
        // the venue is what owns the book an order rests on, and a resting order has no
        // request-time snapshot to be matched against at all. `VenueInstrumentMarket` is `Copy`, so
        // this leaves no borrow outstanding against the `&mut self` below.
        let venue_market = self
            .market(&request.key.instrument)
            .map(|market| market.snapshot);

        if venue_market.is_some_and(|market| crosses(request.state.side, limit, &market)) {
            self.fill_on_arrival(request, venue_market, &terms)
        } else {
            self.rest_order(request, limit, &terms)
        }
    }

    /// Fills an order that is marketable the moment it arrives, debiting what it pays with.
    fn fill_on_arrival(
        &mut self,
        request: OrderRequestOpen<ExchangeId, InstrumentNameExchange>,
        market: Option<MarketSnapshot>,
        terms: &InstrumentTerms,
    ) -> (
        Order<ExchangeId, InstrumentNameExchange, UnindexedOrderState>,
        Option<OpenOrderNotifications>,
    ) {
        let market_supplied = market.is_some();
        let market = market.unwrap_or_default();

        // The order's own price is deliberately NOT handed to the model. A limit constrains the
        // result, not the pricing: a model reads the book and returns where a taker prints, and
        // this venue is what bounds that by the order's terms. See `FillModel`'s contract.
        let maybe_fill_price = self
            .fill_model
            .fill_price(&FillContext::new(request.state.side, &market))
            // For `OrderKind::Market` this is `.or(None)`, since a market order carries no price.
            // For a marketable limit it is the last resort that makes the rejection below
            // unreachable: an order whose limit the market crossed necessarily has one.
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

        // The clamp, and the only place a limit price bounds a fill. A taker crossing the spread
        // can be filled better than its limit but never worse, so a buy never prints above it and
        // a sell never below. Applied here rather than inside the model because it is a property of
        // the order's terms, not of how the market is read.
        let fill_price = match request.state.price {
            Some(limit) if request.state.kind == OrderKind::Limit => match request.state.side {
                Side::Buy => fill_price.min(limit),
                Side::Sell => fill_price.max(limit),
            },
            _ => fill_price,
        };

        let time_exchange = self.time_exchange();

        // Marketable on arrival, so this fill takes liquidity and is charged the taker rate.
        let settlement = self.settlement(
            terms,
            request.state.side,
            request.state.quantity,
            fill_price,
            Liquidity::Taker,
        );

        // Reserve-then-settle in one step. An order that fills on arrival collapses the two
        // together and the client sees a single balance restatement -- see
        // `AccountState::debit_filled`. A configured `total != free` (an `initial_state` copied
        // from a live account with margin reserved) is carried through untouched rather than
        // rejected: the ledger represents a held amount, so there is nothing left to refuse.
        let balance_snapshot =
            match self
                .account
                .debit_filled(&settlement.asset, settlement.amount, time_exchange)
            {
                Ok(balance_snapshot) => balance_snapshot,
                Err(insufficient) => {
                    return (
                        build_open_order_err_response(
                            request,
                            ApiError::BalanceInsufficient(
                                settlement.asset,
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
                time_exchange,
                request.state.quantity,
                Some(fill_price),
            )),
        };

        let notifications = OpenOrderNotifications::Filled {
            balance: Snapshot(balance_snapshot),
            trade: Trade {
                id: trade_id,
                order_id,
                instrument: request.key.instrument,
                strategy: request.key.strategy,
                time_exchange,
                side: request.state.side,
                price: fill_price,
                quantity: request.state.quantity,
                fees: settlement.fees,
            },
        };

        (order_response, Some(notifications))
    }

    /// Puts an order that is not marketable onto the book, holding what its fill will cost.
    ///
    /// The reservation is what the fill will settle, computed once here and settled unchanged --
    /// never a conservative over-estimate. A conservative reservation would have to be released and
    /// re-debited on the fill, and `debit_filled`'s contract is that a fill produces exactly **one**
    /// balance restatement; the extra ones would report balances the account never held, and
    /// [`Balance::used`](crate::balance::Balance::used) would misreport for the order's whole life.
    fn rest_order(
        &mut self,
        request: OrderRequestOpen<ExchangeId, InstrumentNameExchange>,
        limit: Decimal,
        terms: &InstrumentTerms,
    ) -> (
        Order<ExchangeId, InstrumentNameExchange, UnindexedOrderState>,
        Option<OpenOrderNotifications>,
    ) {
        let time_exchange = self.time_exchange();

        // Priced and charged exactly as the fill will be: at the order's own limit, as the maker.
        let settlement = self.settlement(
            terms,
            request.state.side,
            request.state.quantity,
            limit,
            Liquidity::Maker,
        );

        let balance_snapshot =
            match self
                .account
                .reserve(&settlement.asset, settlement.amount, time_exchange)
            {
                Ok(balance_snapshot) => balance_snapshot,
                Err(insufficient) => {
                    return (
                        build_open_order_err_response(
                            request,
                            ApiError::BalanceInsufficient(
                                settlement.asset,
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

        // Nothing has filled, so the order rests with none of its quantity done.
        let open = Open::new(
            self.order_id_sequence_fetch_add(),
            time_exchange,
            Decimal::ZERO,
        );

        self.account.orders_mut().insert(
            Order {
                key: request.key.clone(),
                side: request.state.side,
                price: request.state.price,
                quantity: request.state.quantity,
                kind: request.state.kind,
                time_in_force: request.state.time_in_force,
                state: open.clone(),
            },
            Some(Reservation {
                asset: settlement.asset,
                amount: settlement.amount,
            }),
        );

        let order_response = Order {
            key: request.key,
            side: request.state.side,
            price: request.state.price,
            quantity: request.state.quantity,
            kind: request.state.kind,
            time_in_force: request.state.time_in_force,
            state: OrderState::active(open),
        };

        (
            order_response,
            Some(OpenOrderNotifications::Rested {
                balance: Snapshot(balance_snapshot),
            }),
        )
    }

    /// The instrument facts one order's settlement is computed from, read out once.
    fn instrument_terms(
        &self,
        instrument: &InstrumentNameExchange,
    ) -> Result<InstrumentTerms, UnindexedApiError> {
        let instrument = self.find_instrument_data(instrument)?;
        let cash_settled = Self::settlement_of_supported_kind(instrument)?;

        Ok(InstrumentTerms {
            underlying: instrument.underlying.clone(),
            contract_size: instrument.kind.contract_size(),
            cash_settled,
        })
    }

    /// What one order pays with at `price`, and how much of it including the fee.
    ///
    /// The single place this venue decides an order's cost. A reservation and the fill that settles
    /// it both come from here, with the same `price` and `liquidity`, which is what makes "reserve
    /// exactly what will be settled" true by construction rather than by two formulas agreeing.
    ///
    /// Both the notional and the fee carry the instrument's `contract_size` multiplier, which is
    /// `Decimal::ONE` for `Spot` and a real per-point multiplier for a `Cfd`. Dropping it here while
    /// the engine applies it to PnL and fees (`InstrumentState::update_from_trade`) would make this
    /// ledger and the engine's position accounting disagree by exactly that factor -- balances
    /// moving 1x while PnL moves 25x, silently, with every balance-derived return, drawdown and
    /// Sharpe wrong by the same factor. Which models actually consult the multiplier is theirs to
    /// decide -- `PercentageFeeModel` scales by it, `PerContractFeeModel` deliberately does not --
    /// and passing it is what keeps that decision in the models rather than making this a second
    /// place the multiplier can be lost.
    fn settlement(
        &self,
        terms: &InstrumentTerms,
        side: Side,
        quantity: Decimal,
        price: Decimal,
        liquidity: Liquidity,
    ) -> Settlement {
        let notional_quote = price * quantity.abs() * terms.contract_size;
        let fees_quote =
            self.fee_model
                .compute_fee(price, quantity, terms.contract_size, liquidity);

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
        let (asset, amount) = match (terms.cash_settled, side) {
            (true, _) | (false, Side::Buy) => {
                (terms.underlying.quote.clone(), notional_quote + fees_quote)
            }
            (false, Side::Sell) => {
                // Selling a spot instrument delivers the base asset, so the debit is denominated in
                // base and the quote-denominated fee is converted at `price`.
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
                let fees_base = if price.is_zero() {
                    Decimal::ZERO
                } else {
                    fees_quote / price
                };

                (terms.underlying.base.clone(), quantity.abs() + fees_base)
            }
        };

        Settlement {
            asset,
            amount,
            fees: AssetFees::new(terms.underlying.quote.clone(), fees_quote, Some(fees_quote)),
        }
    }

    /// Whether this venue accepts `order_kind`, which depends on its [`VenueRegime`].
    ///
    /// [`OrderKind::Market`] is always accepted. [`OrderKind::Limit`] needs somewhere to rest and
    /// something to be matched against, so it is accepted only by a
    /// [`MarketDriven`](VenueRegime::MarketDriven) venue. Every other kind is rejected outright.
    ///
    /// # Errors
    /// Returns [`ApiError::OrderRejected`] naming the kind, and -- for a limit order refused by the
    /// regime -- why the venue cannot hold one.
    pub fn validate_order_kind_supported(
        &self,
        order_kind: OrderKind,
    ) -> Result<(), UnindexedOrderError> {
        let reason = match (order_kind, self.regime) {
            (OrderKind::Market, _) | (OrderKind::Limit, VenueRegime::MarketDriven) => return Ok(()),
            (OrderKind::Limit, VenueRegime::RequestPriced) => format!(
                "SimulatedVenue does not support OrderKind::{order_kind:?} in \
                 VenueRegime::RequestPriced: it has no market feed, so an order that rested could \
                 never be matched. Construct it with SimulatedVenue::new_market_driven, and feed \
                 it with apply_market"
            ),
            _ => format!("SimulatedVenue does not support OrderKind::{order_kind:?}"),
        };

        Err(UnindexedOrderError::Rejected(ApiError::OrderRejected(
            reason,
        )))
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

/// Whether the market has moved to or through `limit` for an order resting on `side`.
///
/// Judged on the far side of the book -- an offer for a buy, a bid for a sell -- because that is
/// what an order on this side trades against. A feed supplying no book at all falls back to
/// `last_price`, which is what makes a trades-only feed able to match anything; a feed that does
/// supply a book is never second-guessed by it, since a price *between* the two sides crosses
/// neither.
fn crosses(side: Side, limit: Decimal, market: &MarketSnapshot) -> bool {
    match side {
        Side::Buy => match market.best_ask {
            Some(best_ask) => best_ask <= limit,
            None => market.last_price.is_some_and(|last| last <= limit),
        },
        Side::Sell => match market.best_bid {
            Some(best_bid) => best_bid >= limit,
            None => market.last_price.is_some_and(|last| last >= limit),
        },
    }
}

/// Whether this venue can honour `time_in_force` for an order of `order_kind`.
///
/// # Every [`TimeInForce`] is honoured for an order that fills on arrival
/// A [`OrderKind::Market`] order fills in full the instant it arrives, which satisfies every time
/// in force there is -- there is no remainder to cancel, and no later instant at which the order
/// could still be working. So the gate applies only to an order that can rest.
///
/// # Errors
/// Returns [`ApiError::OrderRejected`] for a [`OrderKind::Limit`] carrying any time in force other
/// than a non-post-only [`TimeInForce::GoodUntilCancelled`], naming what it would take to honour
/// it. Rejecting is the point: treating an unsupported time in force as `GoodUntilCancelled` would
/// silently leave an order working that its sender asked to have cancelled.
fn validate_time_in_force_supported(
    order_kind: OrderKind,
    time_in_force: TimeInForce,
) -> Result<(), UnindexedOrderError> {
    if order_kind != OrderKind::Limit {
        return Ok(());
    }

    let missing = match time_in_force {
        TimeInForce::GoodUntilCancelled { post_only: false } => return Ok(()),
        TimeInForce::GoodUntilCancelled { post_only: true } => {
            "rejecting an order that would take liquidity on arrival"
        }
        TimeInForce::ImmediateOrCancel | TimeInForce::FillOrKill => {
            "cancelling an order that is not marketable on arrival"
        }
        TimeInForce::GoodTillDate { .. } => "expiring a resting order at a stated instant",
        TimeInForce::GoodUntilEndOfDay | TimeInForce::AtOpen | TimeInForce::AtClose => {
            "a session calendar, which this venue does not have"
        }
    };

    Err(UnindexedOrderError::Rejected(ApiError::OrderRejected(
        format!(
            "SimulatedVenue does not support TimeInForce::{time_in_force} on \
             OrderKind::{order_kind:?}: it does not model {missing}"
        ),
    )))
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

/// What one open owes the client, before it is committed and packaged into events.
///
/// Internal to [`SimulatedVenue::open_order`]: it exists only to keep the ordering contract -- ack
/// the trade, then emit balance before trade -- visible in one short place rather than at the end
/// of the pricing body.
///
/// An enum rather than an optional trade because the two outcomes owe different things and a
/// missing trade is not an absent field: a rested open has a balance and *no* trade, and nothing
/// about it is incomplete.
// `large_enum_variant`: boxing the `Trade` would trade one move of ~300 bytes for a heap
// allocation on every fill, which is the wrong way round for a backtest that does millions of
// them. The value is built once per order and destructured immediately, and the struct this
// replaced moved the same bytes on both paths -- the enum only makes the rested one smaller.
#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
enum OpenOrderNotifications {
    /// The open filled on arrival: the balance it paid with, and the trade it printed.
    Filled {
        balance: Snapshot<AssetBalance<AssetNameExchange>>,
        trade: Trade<AssetNameExchange, InstrumentNameExchange>,
    },
    /// The open rests on the book: the balance now held against it, and nothing traded.
    Rested {
        balance: Snapshot<AssetBalance<AssetNameExchange>>,
    },
}

/// The instrument facts one order's settlement is computed from.
///
/// Read out of [`SimulatedVenue::instruments`] once per request, before any `&mut self` borrow of
/// the ledger, so pricing never holds a borrow the balance moves need.
#[derive(Debug, Clone)]
struct InstrumentTerms {
    underlying: Underlying<AssetNameExchange>,
    contract_size: Decimal,
    cash_settled: bool,
}

/// What one order pays with, how much of it, and the fee inside that amount.
#[derive(Debug, Clone)]
struct Settlement {
    /// Quote for a buy or a CFD, base for a spot sell.
    asset: AssetNameExchange,
    /// Denominated in [`asset`](Self::asset), inclusive of the fee.
    amount: Decimal,
    /// Always quote-denominated, whatever [`asset`](Self::asset) is.
    fees: AssetFees<AssetNameExchange>,
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
            state::{ActiveOrderState, InactiveOrderState},
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

    /// Maker 2bp against taker 10bp, so a fill charged the wrong side is off by 5x rather than by a
    /// rounding difference.
    fn maker_taker_fee() -> FeeModelConfig {
        FeeModelConfig::Percentage(PercentageFeeModel::maker_taker(d("0.0002"), d("0.001")))
    }

    /// A venue fed by a driver, and so the only regime that accepts limit orders.
    fn make_market_venue(btc: &str, usdt: &str) -> SimulatedVenue {
        make_market_venue_with(btc, usdt, maker_taker_fee(), SimFillConfig::default())
    }

    fn make_market_venue_with(
        btc: &str,
        usdt: &str,
        fee_model: FeeModelConfig,
        fill_model: SimFillConfig,
    ) -> SimulatedVenue {
        SimulatedVenue::new_market_driven(
            &spot_config_with_fill(btc, usdt, fee_model, fill_model),
            spot_instruments(),
        )
    }

    /// The balance restatement a venue owes, read off the event it emitted.
    fn balance_of(event: &UnindexedAccountEvent) -> &AssetBalance<AssetNameExchange> {
        match &event.kind {
            AccountEventKind::BalanceSnapshot(balance) => &balance.0,
            other => panic!("expected a balance restatement, got: {other:?}"),
        }
    }

    fn trade_of(
        event: &UnindexedAccountEvent,
    ) -> &Trade<AssetNameExchange, InstrumentNameExchange> {
        match &event.kind {
            AccountEventKind::Trade(trade) => trade,
            other => panic!("expected a trade, got: {other:?}"),
        }
    }

    fn order_of(
        event: &UnindexedAccountEvent,
    ) -> &Order<ExchangeId, InstrumentNameExchange, UnindexedOrderState> {
        match &event.kind {
            AccountEventKind::OrderSnapshot(order) => &order.0,
            other => panic!("expected an order snapshot, got: {other:?}"),
        }
    }

    /// The `Open` a rested order reports, or a panic naming what it reported instead.
    fn rested_open(outcome: &OpenOutcome) -> &Open {
        match &outcome.response.state {
            OrderState::Active(ActiveOrderState::Open(open)) => open,
            other => panic!("expected a resting open order, got: {other:?}"),
        }
    }

    /// The price a filled order reports having filled at.
    fn filled_price(
        order: &Order<ExchangeId, InstrumentNameExchange, UnindexedOrderState>,
    ) -> Decimal {
        match &order.state {
            OrderState::Inactive(InactiveOrderState::FullyFilled(filled)) => filled
                .avg_price
                .expect("a filled order reports the price it filled at"),
            other => panic!("expected a fully filled order, got: {other:?}"),
        }
    }

    /// Rests one buy limit, returning the venue holding it and what it told its client.
    ///
    /// The market is set first and does not cross the order, which is what makes it rest rather
    /// than fill on arrival.
    fn venue_resting_one_buy(limit: &str) -> (SimulatedVenue, OpenOutcome) {
        let mut venue = make_market_venue("10", "1000000");
        venue.advance_time(time(1));

        assert!(
            venue
                .apply_market(&instrument_name(), book("49000", "49100"), time(1))
                .is_empty(),
            "an empty book fills nothing"
        );

        let outcome = venue.open_order(limit_request("resting", Side::Buy, "1", limit, gtc()));
        assert_eq!(
            outcome.events.len(),
            1,
            "a rested order restates one balance and books no trade"
        );

        (venue, outcome)
    }

    fn time(seconds: i64) -> DateTime<Utc> {
        DateTime::<Utc>::from_timestamp(1_700_000_000 + seconds, 0).expect("representable instant")
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

    /// A cancel for an id the venue never booked is rejected as unknown, and the rejection is the
    /// venue's rather than any driver's.
    ///
    /// Answering here — rather than in each driver — is what stops two drivers from carrying two
    /// copies of the same policy, and the response is the one the request's channel asks for.
    #[test]
    fn a_cancel_for_an_unknown_order_is_rejected_as_unknown() {
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
                    reason.contains("test-cid"),
                    "the rejection must name the order it could not find, got: {reason}"
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

    /// A [`RequestPriced`](VenueRegime::RequestPriced) venue rejects a limit order on its kind,
    /// before any fill model is consulted.
    ///
    /// The clamp that bounds a fill by the order's own terms lives on the market-driven path, so
    /// this venue must never reach a pricing decision for an order it cannot hold. The snapshot is
    /// chosen so a lapse would be unmistakable: `MidpointFillModel` would price this buy at 50,000
    /// against a limit of 40,000 — a fill 25% above the order's own limit — so a `Filled` response
    /// here means the gate stopped holding, not merely that a rejection message changed.
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

    // --- Stage 3b: limit orders -------------------------------------------------------------

    /// The regime gate, and the reason it exists: a venue with no feed could never match a resting
    /// order, so accepting one would be accepted-and-silently-never-filled.
    #[test]
    fn a_request_priced_venue_rejects_a_limit_order_and_says_what_would_accept_one() {
        let mut venue = make_venue("10", "1000000");
        assert_eq!(venue.regime(), VenueRegime::RequestPriced);

        let outcome = venue.open_order(limit_request("a", Side::Buy, "1", "48000", gtc()));

        assert!(
            outcome.events.is_empty(),
            "a rejected order moves no balance"
        );
        match outcome.response.state {
            OrderState::Inactive(InactiveOrderState::OpenFailed(OrderError::Rejected(
                ApiError::OrderRejected(ref reason),
            ))) => {
                assert!(
                    reason.contains("new_market_driven"),
                    "the rejection must name the constructor that would accept it, got: {reason}"
                );
            }
            ref other => panic!("a limit order must be rejected by the regime, got: {other:?}"),
        }
    }

    /// A limit order that the market has not reached rests, and the venue holds exactly what its
    /// fill will cost — no more, so no release-and-re-debit is ever needed.
    #[test]
    fn a_limit_order_that_does_not_cross_rests_and_reserves_what_its_fill_will_cost() {
        let (venue, rested) = venue_resting_one_buy("48000");

        // 48000 notional + 2bp maker fee on it.
        let reserved = d("48009.6");

        let balance = venue.balances(&[quote()]).remove(0);
        assert_eq!(
            balance.balance.total,
            d("1000000"),
            "resting an order spends nothing: `total` moves only on a fill"
        );
        assert_eq!(
            balance.balance.free,
            d("1000000") - reserved,
            "`free` falls by the notional plus the MAKER fee the fill will charge"
        );
        assert_eq!(
            balance.balance.used(),
            reserved,
            "the held portion is what a client sees as committed to the order"
        );

        assert_eq!(
            rested_open(&rested).filled_quantity,
            Decimal::ZERO,
            "the client is told an order is working with none of its quantity done"
        );

        let open = venue
            .orders_open(&[instrument_name()])
            .pop()
            .expect("the order is on the book");
        assert_eq!(open.price, Some(d("48000")));
        assert_eq!(
            open.state,
            *rested_open(&rested),
            "the book and the response describe the same order"
        );
    }

    /// A resting order is the passive side, so it is paid the price it quoted. Price improvement
    /// accrues to the aggressor that crossed it, never to the maker.
    #[test]
    fn a_resting_order_fills_at_its_own_limit_and_is_never_improved_by_the_book() {
        let (mut venue, _) = venue_resting_one_buy("48000");
        venue.advance_time(time(2));

        // Straight through the limit: the offer is 100 better than the order asked for.
        let events = venue.apply_market(&instrument_name(), book("47800", "47900"), time(2));

        assert_eq!(
            events.len(),
            3,
            "one filled order owes a balance, a trade and a terminal order snapshot"
        );

        let trade = trade_of(&events[1]);
        assert_eq!(
            trade.price,
            d("48000"),
            "a resting order fills at its own limit; 47900 would credit it improvement no maker \
             can obtain"
        );
        assert_eq!(
            trade.fees.fees,
            d("9.6"),
            "2bp MAKER on 48000, not 10bp taker: the resting side supplied liquidity"
        );

        assert_eq!(
            filled_price(order_of(&events[2])),
            d("48000"),
            "the terminal snapshot reports the same price as the trade"
        );
    }

    /// The trade precedes the terminal order snapshot, which is not merely cosmetic: a consumer
    /// routing a fill to a position needs the order still to be live when the trade arrives.
    #[test]
    fn a_resting_fill_reports_its_trade_before_the_order_that_produced_it() {
        let (mut venue, _) = venue_resting_one_buy("48000");
        venue.advance_time(time(2));

        let events = venue.apply_market(&instrument_name(), book("47800", "47900"), time(2));

        assert!(
            matches!(events[0].kind, AccountEventKind::BalanceSnapshot(_)),
            "the balance the fill paid with comes first, got: {:?}",
            events[0].kind
        );
        assert!(
            matches!(events[1].kind, AccountEventKind::Trade(_)),
            "then the trade, got: {:?}",
            events[1].kind
        );
        assert!(
            matches!(events[2].kind, AccountEventKind::OrderSnapshot(_)),
            "and only then the order it terminated, got: {:?}",
            events[2].kind
        );
    }

    /// A completed order is reported as `Inactive(FullyFilled)` rather than as an `Open` carrying a
    /// complete fill. Both denote the same fact; only this one can carry the price it filled at.
    #[test]
    fn a_resting_fill_reports_its_order_as_terminal_rather_than_as_a_complete_open() {
        let (mut venue, _) = venue_resting_one_buy("48000");
        venue.advance_time(time(2));

        let events = venue.apply_market(&instrument_name(), book("47800", "47900"), time(2));

        assert!(
            matches!(
                order_of(&events[2]).state,
                OrderState::Inactive(InactiveOrderState::FullyFilled(_))
            ),
            "got: {:?}",
            order_of(&events[2]).state
        );
        assert!(
            venue.orders_open(&[]).is_empty(),
            "a filled order leaves the book"
        );
    }

    /// Reserve exactly what will be settled: the client sees one balance restatement per fill, and
    /// never a balance the account did not hold.
    #[test]
    fn a_resting_order_settles_exactly_what_it_reserved() {
        let (mut venue, _) = venue_resting_one_buy("48000");
        venue.advance_time(time(2));

        let reserved = d("48009.6");
        let free_while_resting = venue.balances(&[quote()]).remove(0).balance.free;

        let events = venue.apply_market(&instrument_name(), book("47800", "47900"), time(2));

        let balance = balance_of(&events[0]);
        assert_eq!(
            balance.balance.total,
            d("1000000") - reserved,
            "`total` falls by exactly what was held"
        );
        assert_eq!(
            balance.balance.free, free_while_resting,
            "`free` was already reduced when the order rested, so the fill does not move it again"
        );
        assert_eq!(
            balance.balance.used(),
            Decimal::ZERO,
            "nothing is held any more: the order that held it is gone"
        );
    }

    /// A limit order the market has already reached is the aggressor, so it takes the book's price
    /// bounded by its own limit, and pays the taker rate.
    #[test]
    fn a_marketable_limit_order_fills_on_arrival_at_the_book_bounded_by_its_limit() {
        let mut venue = make_market_venue_with(
            "10",
            "1000000",
            maker_taker_fee(),
            SimFillConfig::BidAsk(BidAskFillModel),
        );
        venue.advance_time(time(1));
        assert!(
            venue
                .apply_market(&instrument_name(), book("47800", "47900"), time(1))
                .is_empty()
        );

        // Willing to pay 48000; the offer is 47900, so it prints at the offer.
        let outcome = venue.open_order(limit_request("taker", Side::Buy, "1", "48000", gtc()));

        assert_eq!(outcome.events.len(), 2, "a filled open owes balance, trade");
        let trade = trade_of(&outcome.events[1]);
        assert_eq!(
            trade.price,
            d("47900"),
            "the aggressor takes the offer, which is better than its limit"
        );
        assert_eq!(
            trade.fees.fees,
            d("47.9"),
            "10bp TAKER on 47900: it crossed the spread rather than resting"
        );
        assert!(
            venue.orders_open(&[]).is_empty(),
            "it filled on arrival, so it never rested"
        );
    }

    /// The clamp, on the **default** fill model. Marketability is judged on the book while the
    /// price comes from the model, and the two are separate readings of a sampled snapshot — so the
    /// model can return a price the order never agreed to pay. The order's own terms bound it.
    ///
    /// A clamp against [`MidpointFillModel`] or [`BidAskFillModel`] would be vacuous: both derive
    /// the price from the very side of the book that decided the order was marketable, so neither
    /// can exceed the limit that side already crossed.
    #[test]
    fn a_marketable_limit_order_never_fills_worse_than_its_limit() {
        let mut venue = make_market_venue_with(
            "10",
            "1000000",
            FeeModelConfig::default(),
            // `LastPriceFillModel`, which reads `last_price` and never consults the book.
            SimFillConfig::default(),
        );
        venue.advance_time(time(1));

        // The offer is inside the limit, so the order is marketable -- but the trade price the
        // model reads is well above it.
        let stale = MarketSnapshot {
            best_bid: Some(d("46900")),
            best_ask: Some(d("47000")),
            last_price: Some(d("48000")),
        };
        assert!(
            venue
                .apply_market(&instrument_name(), stale, time(1))
                .is_empty()
        );

        let outcome = venue.open_order(limit_request("clamped", Side::Buy, "1", "47100", gtc()));

        assert_eq!(
            trade_of(&outcome.events[1]).price,
            d("47100"),
            "the model priced it at 48000, which the order never agreed to pay"
        );
    }

    /// The two pricing rules are different rules, and this is the one point at which they must give
    /// the same answer. If they diverge here, one of them is wrong.
    #[test]
    fn the_two_pricing_rules_agree_where_the_book_meets_the_limit() {
        let limit = "48000";
        let touching = || book("47900", "48000");

        // Rule 1: marketable on arrival. The book is already at the limit when the order arrives.
        let mut aggressor = make_market_venue_with(
            "10",
            "1000000",
            maker_taker_fee(),
            SimFillConfig::BidAsk(BidAskFillModel),
        );
        aggressor.advance_time(time(1));
        assert!(
            aggressor
                .apply_market(&instrument_name(), touching(), time(1))
                .is_empty()
        );
        let on_arrival = aggressor.open_order(limit_request("a", Side::Buy, "1", limit, gtc()));
        let arrival_price = trade_of(&on_arrival.events[1]).price;

        // Rule 2: rested first, crossed later by a book that reaches exactly the same price.
        let mut maker = make_market_venue_with(
            "10",
            "1000000",
            maker_taker_fee(),
            SimFillConfig::BidAsk(BidAskFillModel),
        );
        maker.advance_time(time(1));
        assert!(
            maker
                .apply_market(&instrument_name(), book("49000", "49100"), time(1))
                .is_empty()
        );
        let rested = maker.open_order(limit_request("b", Side::Buy, "1", limit, gtc()));
        assert_eq!(rested.events.len(), 1, "it rested rather than filling");
        maker.advance_time(time(2));
        let resting_events = maker.apply_market(&instrument_name(), touching(), time(2));
        let resting_price = trade_of(&resting_events[1]).price;

        assert_eq!(
            arrival_price,
            d(limit),
            "an order arriving when the book is at its limit fills at the limit"
        );
        assert_eq!(
            resting_price, arrival_price,
            "the two pricing rules must agree exactly at the crossing point; they may differ only \
             where the order genuinely is, or is not, marketable"
        );
    }

    /// Matching takes orders in the order the book holds them and stops at the first that does not
    /// cross — not at the first it happens to iterate over.
    #[test]
    fn matching_fills_in_price_time_order_and_stops_at_the_first_order_that_does_not_cross() {
        let mut venue = make_market_venue("10", "1000000");
        venue.advance_time(time(1));
        assert!(
            venue
                .apply_market(&instrument_name(), book("49000", "49100"), time(1))
                .is_empty()
        );

        // Best bid last, to prove the fill order is the book's rather than the insertion order.
        for (cid, price) in [("worst", "47000"), ("mid", "48000"), ("best", "48500")] {
            let outcome = venue.open_order(limit_request(cid, Side::Buy, "1", price, gtc()));
            assert_eq!(outcome.events.len(), 1, "{cid} must rest");
        }

        venue.advance_time(time(2));
        // Crosses `best` and `mid`, leaves `worst` alone.
        let events = venue.apply_market(&instrument_name(), book("47900", "48000"), time(2));

        let filled: Vec<_> = events
            .iter()
            .filter_map(|event| match &event.kind {
                AccountEventKind::Trade(trade) => Some(trade.price),
                _ => None,
            })
            .collect();

        assert_eq!(
            filled,
            vec![d("48500"), d("48000")],
            "best bid first, and each at its own limit"
        );
        assert_eq!(
            venue
                .orders_open(&[])
                .into_iter()
                .map(|order| order.key.cid)
                .collect::<Vec<_>>(),
            vec![ClientOrderId::new("worst")],
            "the order the market did not reach is untouched"
        );
    }

    /// A trades-only feed has no book at all, so marketability falls back to the trade price. A
    /// book-only rule would never match anything on such a feed.
    #[test]
    fn a_feed_with_no_book_matches_on_the_trade_price() {
        let mut venue = make_market_venue("10", "1000000");
        venue.advance_time(time(1));
        assert!(
            venue
                .apply_market(&instrument_name(), traded("49000"), time(1))
                .is_empty()
        );

        let outcome = venue.open_order(limit_request("t", Side::Buy, "1", "48000", gtc()));
        assert_eq!(outcome.events.len(), 1, "49000 traded does not reach 48000");

        venue.advance_time(time(2));
        let events = venue.apply_market(&instrument_name(), traded("47500"), time(2));

        assert_eq!(events.len(), 3, "a print through the limit fills it");
        assert_eq!(trade_of(&events[1]).price, d("48000"));
    }

    /// A price between the two sides of a book crosses neither, so a supplied book is never
    /// second-guessed by `last_price`.
    #[test]
    fn a_price_inside_the_spread_does_not_cross_a_resting_order() {
        let (mut venue, _) = venue_resting_one_buy("48000");
        venue.advance_time(time(2));

        // The microprice sits inside the spread and below the limit; the offer does not.
        let inside = MarketSnapshot {
            best_bid: Some(d("47900")),
            best_ask: Some(d("48100")),
            last_price: Some(d("47950")),
        };

        assert!(
            venue
                .apply_market(&instrument_name(), inside, time(2))
                .is_empty(),
            "the offer is 48100, so a 48000 bid is not crossed however the trade price reads"
        );
        assert_eq!(
            venue.orders_open(&[]).len(),
            1,
            "the order is still resting"
        );
    }

    /// A sell rests against the bid, pays with base, and settles the base it committed.
    #[test]
    fn a_resting_sell_reserves_base_and_settles_it() {
        let mut venue = make_market_venue("10", "1000000");
        venue.advance_time(time(1));
        assert!(
            venue
                .apply_market(&instrument_name(), book("47000", "47100"), time(1))
                .is_empty()
        );

        let outcome = venue.open_order(limit_request("s", Side::Sell, "1", "48000", gtc()));
        assert_eq!(outcome.events.len(), 1, "47000 bid does not reach 48000");
        assert_eq!(
            balance_of(&outcome.events[0]).asset,
            base(),
            "a spot sell delivers the base asset, so that is what is held"
        );

        venue.advance_time(time(2));
        let events = venue.apply_market(&instrument_name(), book("48100", "48200"), time(2));

        assert_eq!(trade_of(&events[1]).price, d("48000"), "at its own limit");
        let balance = balance_of(&events[0]);
        assert_eq!(balance.asset, base());
        // 1 BTC delivered, plus the quote fee converted at the limit: 9.6 / 48000.
        assert_eq!(balance.balance.total, d("10") - d("1") - d("0.0002"));
    }

    /// A maker rebate is a negative fee, so the amount held is less than the notional and settling
    /// it leaves more behind than the notional alone would.
    #[test]
    fn a_maker_rebate_is_held_and_settled_as_a_smaller_amount_than_the_notional() {
        let rebate =
            FeeModelConfig::Percentage(PercentageFeeModel::maker_taker(d("-0.0001"), d("0.001")));
        let mut venue = make_market_venue_with("10", "1000000", rebate, SimFillConfig::default());
        venue.advance_time(time(1));
        assert!(
            venue
                .apply_market(&instrument_name(), book("49000", "49100"), time(1))
                .is_empty()
        );

        let outcome = venue.open_order(limit_request("r", Side::Buy, "1", "48000", gtc()));

        // 48000 notional less a 1bp rebate on it.
        let held = d("48000") - d("4.8");
        assert_eq!(
            balance_of(&outcome.events[0]).balance.used(),
            held,
            "a rebate means less is held than the notional"
        );

        venue.advance_time(time(2));
        let events = venue.apply_market(&instrument_name(), book("47800", "47900"), time(2));

        assert_eq!(
            balance_of(&events[0]).balance.total,
            d("1000000") - held,
            "and exactly that less is settled, so the rebate is kept"
        );
        assert_eq!(trade_of(&events[1]).fees.fees, d("-4.8"));
    }

    /// An order the account cannot afford never reaches the book, and the ledger is untouched.
    #[test]
    fn a_limit_order_that_cannot_be_afforded_is_rejected_without_resting() {
        let mut venue = make_market_venue("10", "1000");
        venue.advance_time(time(1));
        assert!(
            venue
                .apply_market(&instrument_name(), book("49000", "49100"), time(1))
                .is_empty()
        );

        let outcome = venue.open_order(limit_request("poor", Side::Buy, "1", "48000", gtc()));

        assert!(
            outcome.events.is_empty(),
            "a refused reservation moves nothing"
        );
        assert!(matches!(
            outcome.response.state,
            OrderState::Inactive(InactiveOrderState::OpenFailed(OrderError::Rejected(
                ApiError::BalanceInsufficient(..)
            )))
        ));
        assert!(venue.orders_open(&[]).is_empty(), "it never rested");
        assert_eq!(
            venue.balances(&[quote()]).remove(0).balance.free,
            d("1000"),
            "the ledger is left exactly as it was"
        );
    }

    // --- Stage 3b: cancel -------------------------------------------------------------------

    /// Cancelling gives the held balance back and reports the quantity done, which this venue can
    /// only ever report as zero because it models no partial fills.
    #[test]
    fn a_cancel_releases_the_reservation_and_restates_the_balance() {
        let (mut venue, _) = venue_resting_one_buy("48000");
        venue.advance_time(time(2));

        let outcome = venue.cancel_order(OrderEvent {
            key: OrderKey {
                exchange: EXCHANGE,
                instrument: instrument_name(),
                strategy: StrategyId::new("test"),
                cid: ClientOrderId::new("resting"),
            },
            state: RequestCancel { id: None },
        });

        assert_eq!(outcome.events.len(), 1, "a cancel restates one balance");
        let balance = balance_of(&outcome.events[0]);
        assert_eq!(
            balance.balance.free,
            d("1000000"),
            "everything held against the order is spendable again"
        );
        assert_eq!(
            balance.balance.total,
            d("1000000"),
            "nothing ever left the account, so `total` never moved"
        );

        let cancelled = outcome.response.state.expect("the order was on the book");
        assert_eq!(cancelled.filled_quantity, Decimal::ZERO);
        assert_eq!(cancelled.time_exchange, time(2));
        assert!(venue.orders_open(&[]).is_empty());
    }

    /// The cancel-vs-fill race, which a non-zero `to_venue` latency makes reachable. The caller is
    /// told to reconcile against the fill rather than to retry.
    #[test]
    fn a_cancel_that_loses_the_race_to_its_own_fill_reports_the_fill() {
        let (mut venue, _) = venue_resting_one_buy("48000");
        venue.advance_time(time(2));
        let events = venue.apply_market(&instrument_name(), book("47800", "47900"), time(2));
        assert_eq!(events.len(), 3, "the order filled first");

        let outcome = venue.cancel_order(OrderEvent {
            key: OrderKey {
                exchange: EXCHANGE,
                instrument: instrument_name(),
                strategy: StrategyId::new("test"),
                cid: ClientOrderId::new("resting"),
            },
            state: RequestCancel { id: None },
        });

        assert!(outcome.events.is_empty(), "a failed cancel moves nothing");
        assert!(
            matches!(
                outcome.response.state,
                Err(OrderError::Rejected(ApiError::OrderAlreadyFullyFilled))
            ),
            "got: {:?}",
            outcome.response.state
        );
    }

    /// A second cancel is a state conflict rather than an unknown order, and says so.
    #[test]
    fn a_second_cancel_reports_the_order_as_already_cancelled() {
        let (mut venue, _) = venue_resting_one_buy("48000");
        let cancel = || OrderEvent {
            key: OrderKey {
                exchange: EXCHANGE,
                instrument: instrument_name(),
                strategy: StrategyId::new("test"),
                cid: ClientOrderId::new("resting"),
            },
            state: RequestCancel { id: None },
        };

        assert!(venue.cancel_order(cancel()).response.state.is_ok());
        let second = venue.cancel_order(cancel());

        assert!(
            matches!(
                second.response.state,
                Err(OrderError::Rejected(ApiError::OrderAlreadyCancelled))
            ),
            "got: {:?}",
            second.response.state
        );
    }

    /// A cancelled order is reported by a later snapshot, so a client reconciling against the venue
    /// sees the terminal state rather than an order that merely vanished.
    #[test]
    fn a_cancelled_order_enters_the_account_snapshot() {
        let (mut venue, _) = venue_resting_one_buy("48000");
        venue.cancel_order(OrderEvent {
            key: OrderKey {
                exchange: EXCHANGE,
                instrument: instrument_name(),
                strategy: StrategyId::new("test"),
                cid: ClientOrderId::new("resting"),
            },
            state: RequestCancel { id: None },
        });

        let snapshot = venue.account_snapshot();
        let orders = &snapshot.instruments[0].orders;
        assert_eq!(orders.len(), 1);
        assert!(matches!(
            orders[0].state,
            OrderState::Inactive(InactiveOrderState::Cancelled(_))
        ));
    }

    // --- Stage 3b: time in force ------------------------------------------------------------

    /// Every time in force this venue cannot honour is rejected rather than treated as
    /// `GoodUntilCancelled`, which would silently leave an order working that its sender asked to
    /// have cancelled.
    #[test]
    fn a_limit_order_is_rejected_for_every_time_in_force_this_venue_cannot_honour() {
        let unsupported = [
            TimeInForce::GoodUntilCancelled { post_only: true },
            TimeInForce::ImmediateOrCancel,
            TimeInForce::FillOrKill,
            TimeInForce::GoodTillDate { expiry: time(9) },
            TimeInForce::GoodUntilEndOfDay,
            TimeInForce::AtOpen,
            TimeInForce::AtClose,
        ];

        for time_in_force in unsupported {
            let mut venue = make_market_venue("10", "1000000");
            venue.advance_time(time(1));
            assert!(
                venue
                    .apply_market(&instrument_name(), book("49000", "49100"), time(1))
                    .is_empty()
            );

            let outcome =
                venue.open_order(limit_request("tif", Side::Buy, "1", "48000", time_in_force));

            assert!(
                outcome.events.is_empty(),
                "{time_in_force} must move no balance"
            );
            assert!(
                venue.orders_open(&[]).is_empty(),
                "{time_in_force} must not rest"
            );
            match outcome.response.state {
                OrderState::Inactive(InactiveOrderState::OpenFailed(OrderError::Rejected(
                    ApiError::OrderRejected(ref reason),
                ))) => assert!(
                    reason.contains("does not model"),
                    "the rejection must say what is missing, got: {reason}"
                ),
                ref other => panic!("{time_in_force} must be rejected, got: {other:?}"),
            }
        }
    }

    /// The time-in-force gate must not reach a market order. One fills in full on arrival, which
    /// satisfies every time in force there is — and rejecting one would be a regression.
    #[test]
    fn a_market_order_is_accepted_whatever_its_time_in_force() {
        for time_in_force in [
            TimeInForce::GoodUntilCancelled { post_only: false },
            TimeInForce::ImmediateOrCancel,
            TimeInForce::FillOrKill,
            TimeInForce::GoodTillDate { expiry: time(9) },
            TimeInForce::GoodUntilEndOfDay,
            TimeInForce::AtOpen,
            TimeInForce::AtClose,
        ] {
            let mut venue = make_venue("10", "1000000");
            let mut request = buy_request("1", market_prices("48000"));
            request.state.time_in_force = time_in_force;

            let outcome = venue.open_order(request);

            assert!(
                matches!(
                    outcome.response.state,
                    OrderState::Inactive(InactiveOrderState::FullyFilled(_))
                ),
                "{time_in_force} on a market order must still fill, got: {:?}",
                outcome.response.state
            );
        }
    }
}
