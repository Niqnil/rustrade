//! The simulated venue's state machine, independent of any transport that drives it.

use crate::{
    AccountEventKind, InstrumentAccountSnapshot, UnindexedAccountEvent, UnindexedAccountSnapshot,
    balance::AssetBalance,
    client::mock::MockExecutionConfig,
    error::{ApiError, UnindexedApiError, UnindexedOrderError},
    exchange::mock::{
        account::{AccountState, Debit},
        orders::{OpenOrder, Reservation, RestingOrder},
    },
    fee::{FeeModel, FeeModelConfig, Liquidity},
    fill::{FillContext, FillModel, SimFillConfig},
    market::{MarketDepth, MarketSnapshot},
    order::{
        Order, OrderKind, TimeInForce, UnindexedOrder,
        id::{ClientOrderId, OrderId},
        request::{OrderRequestCancel, OrderRequestOpen, UnindexedOrderResponseCancel},
        state::{Cancelled, Expired, Filled, Open, OrderState, UnindexedOrderState},
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

/// A venue's view of one instrument: what it is worth, how much of it is left to take, and when
/// that view was last replaced.
///
/// The instant is carried alongside the prices rather than inside them because a
/// [`MarketSnapshot`] says what the market is, not when it was observed — and a matching engine
/// needs both: an order resting since `T` may only be matched against a market at or after `T`.
/// The remaining depth is here for the same reason and one more: it is not even a property of the
/// market, but of what this account has already taken out of it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct VenueInstrumentMarket {
    /// Prices as of [`time_exchange`](Self::time_exchange).
    pub snapshot: MarketSnapshot,

    /// What is **left** of the size reported with [`snapshot`](Self::snapshot), after every taker
    /// fill this venue has struck against it.
    ///
    /// Reset wholesale by [`apply_market`](SimulatedVenue::apply_market), because a new
    /// observation is a new statement of what is on offer rather than an increment to the old
    /// one. Between two such observations it only ever falls, which is what stops two market
    /// orders arriving in the same tick from each taking the whole of a size that was only ever
    /// displayed once. Reading a size does not consume it; filling against it does.
    pub depth: MarketDepth,

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
    /// matched against that feed later — and **every** fill, market orders included, is priced
    /// from that feed rather than from the snapshot a request carried.
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
///   absent balance. Cancelling one releases nothing and restates no balance, and neither does
///   retiring one at its deadline. Such an order may also arrive **part-filled**, carrying an
///   [`Open::filled_quantity`] this venue did not produce: only the remainder ever trades here,
///   and a fill that completes it reports no `avg_price`, because the price of the part that
///   traded elsewhere is not something this venue can know. An order **this** venue part-filled
///   before resting is the same shape, and reports no `avg_price` for the same reason — the two
///   fills were struck at different prices and this state carries one of them.
///
/// # How an order is priced
///
/// Every fill is priced from **one** market, and which market that is depends only on the
/// [`VenueRegime`]: this venue's own on a [`MarketDriven`](VenueRegime::MarketDriven) one, and the
/// snapshot the request carried
/// ([`RequestOpen::market`](crate::order::request::RequestOpen::market)) on a
/// [`RequestPriced`](VenueRegime::RequestPriced) one. A limit order is only ever accepted on a
/// `MarketDriven` venue, so it is always priced from the venue's own market; a market order follows
/// the regime.
///
/// That a `MarketDriven` venue prices a market order from its own book — rather than from the
/// sender's view at decision time — is what makes a driver's request latency price-relevant instead
/// of a pure delivery delay. The market may move between an order being decided and arriving, and a
/// fill is struck against the book it arrives to. `RequestOpen::market` is then decision-time
/// provenance and nothing else, which is what it is documented to be, and the difference between it
/// and the fill is implementation shortfall — a quantity that is identically zero when the two are
/// the same snapshot.
///
/// A `MarketDriven` venue that has not been fed an instrument **rejects** a market order in it as
/// unpriceable rather than falling back to the requester's snapshot: a venue with no market for an
/// instrument cannot fill a market order in it, and saying so is the observable failure. Silently
/// pricing from the requester's view would make the fill depend on which of the two happened to
/// hold a price, and would re-admit the very `RequestPriced` behaviour the regime exists to
/// distinguish.
///
/// Given that market, the three cases are:
/// - **A market order**, and **a limit order marketable on arrival** — priced by the [`FillModel`].
///   A limit order is then *clamped* to its own limit: a buy never fills above it, a sell never
///   below. Both are charged [`Liquidity::Taker`].
/// - **A resting limit order** — fills at its own limit price exactly, and never reaches the
///   [`FillModel`]. Deriving a better price from the book would credit price improvement no resting
///   order can obtain: a maker is paid the price it quoted, and improvement accrues to the
///   aggressor that crossed it. Charged [`Liquidity::Maker`].
///
/// The marketable and resting rules agree exactly at the crossing point — an order arriving when
/// `best_ask == limit` fills at the limit either way — and diverge only where the order genuinely
/// is, or is not, marketable. Marketability is judged on `best_ask`/`best_bid`, falling back to
/// `last_price` when the feed supplies no book: with the default `LastPriceFillModel` and a
/// trades-only feed there is no book at all, so a book-only rule would never match anything.
///
/// # What matching does not model
/// - **No queue position.** Nothing rests ahead of this account's orders. [`OpenOrders`] ranks
///   them by price then arrival, so *which* of them fills first is reproducible, but they are
///   matched against a book with no other participants in it — an order that crosses is never
///   waiting behind anyone.
/// - **Only a taker is capped by size.** An order aggressing on arrival fills at most what
///   [`VenueInstrumentMarket::depth`] says is on offer, and draws that down as it takes it. A
///   **resting** order is not capped: when the market reaches it, it fills its whole remaining
///   quantity at its limit however little traded there. Capping it honestly would need the volume
///   that actually printed through the limit, which this venue's feed does not carry — a
///   [`MarketSnapshot`] is a view of the market, not a record of what traded — and capping it by
///   the size on the *opposite* side would be a number shaped like a cap that measures nothing a
///   cap should.
/// - **A capped taker's remainder is a maker, and makers are not capped.** So an order the book
///   could only partly fill rests the rest, and the very next tick that still crosses it fills
///   that remainder in full. The cap bounds what one order takes from one observation; it does not
///   make liquidity scarce for longer than that.
/// - **Absent size means unlimited, not unfillable.** A feed with no sizes — trades only, candles,
///   a price-only export, or a [`RequestPriced`](VenueRegime::RequestPriced) venue, which has no
///   market at all — caps nothing. See [`MarketDepth`].
/// - **Depth is this account's own bound, not the market's.** It is reset by each
///   [`apply_market`](Self::apply_market) rather than accumulated, so it stops two of this
///   account's orders double-spending one observation's size and claims nothing about anyone
///   else's.
///
/// # Which [`TimeInForce`] this venue honours
/// An unsupported one is **rejected**, never quietly treated as good-until-cancelled: that would
/// leave an order working that its sender asked to have cancelled.
///
/// | [`TimeInForce`] | [`OrderKind::Limit`] | [`OrderKind::Market`] |
/// |---|---|---|
/// | `GoodUntilCancelled { post_only: false }` | rests, or fills if marketable | fills |
/// | `GoodUntilCancelled { post_only: true }` | rests, or **cancels** if marketable | **rejected** |
/// | `ImmediateOrCancel` / `FillOrKill` | fills if marketable, else **cancels** | fills |
/// | `GoodTillDate { expiry }` | rests until `expiry`, then **expires** | fills |
/// | `GoodUntilEndOfDay` | **rejected** | fills |
/// | `AtOpen` / `AtClose` | **rejected** | **rejected** |
///
/// A market order works only on arrival, which honours every time in force that bounds *how long*
/// an order works — so most of the table is moot for one. (It fills in full wherever the book has
/// the size; what it cannot fill is cancelled, since it has no limit price to rest at.) The two
/// exceptions are real:
/// `post_only` on a market order is a promise never to take liquidity, which is all a market order
/// does; and `AtOpen`/`AtClose` say *when* an order executes rather than how long it lasts, so
/// honouring them needs a session calendar this venue has not got. Filling one on arrival would be
/// a wrong fill rather than an ignored flag.
///
/// ## A deadline is swept, not scheduled
/// This venue has no timer. A [`GoodTillDate`](TimeInForce::GoodTillDate) order is retired when a
/// driver next calls [`advance_time`](Self::advance_time) or [`apply_market`](Self::apply_market),
/// whichever comes first, and the sweep spans every instrument rather than only the one that
/// ticked. A deadline falling after the last such call is never reached at all — see
/// [`advance_time`](Self::advance_time) for what that means for a caller.
///
/// A deadline is an unconditional cutoff. An order reaching it is retired even if the very tick
/// that reached it would have crossed it, and an order that arrives already past its deadline is
/// retired without resting and without trading — otherwise whether an expired order traded would
/// depend on where the book happened to be.
///
/// [`OpenOrders`]: super::orders::OpenOrders
///
/// # Reserved balances
/// The ledger distinguishes held from spendable: `free` is what an order may draw on, `total` is
/// what the account holds, and the difference is held against something. An `initial_state` copied
/// from a live account with margin reserved is therefore usable as configured — it was rejected
/// outright while every order filled on arrival and the ledger could not represent the state.
///
/// Every arrival is **one** commitment against the ledger ([`AccountState::commit`]) and therefore
/// **one** balance restatement, whichever of the three shapes it has: an order that fills outright
/// settles and holds nothing; one that rests holds and settles nothing; one the book fills in part
/// does both at once, settling what traded and holding against the remainder it leaves resting. An
/// order that rests emits one more restatement later, when the market reaches it and the hold is
/// settled — never an intermediate balance the account did not hold. A configured reservation is
/// carried through untouched: settling lowers `total` by the settled amount and leaves the rest
/// held.
///
/// **An order is funded whole or refused whole.** The two legs of a part-filled arrival are asked
/// for together, so an account that cannot cover both has the order rejected with nothing moved.
/// It is not filled for the part it could afford: choosing that would be this venue substituting a
/// disposition for the one the sender asked for. Note the consequence, which is easy to be
/// surprised by — a capped order costs *more* than the same order filling outright, because its
/// remainder is held at the order's own limit while the fill struck a better price, so an order
/// can be refused for a size the book was willing to sell it.
///
/// The reservation is **exactly** what the fill will settle, not a conservative over-estimate: both
/// come from one computation at the order's own limit and its own liquidity side. A conservative
/// reservation would have to be released and re-debited on the fill, producing two or three
/// restatements for one fill and reporting balances the account never held, while
/// [`Balance::used`](crate::balance::Balance::used) misreported for the order's whole life.
///
/// [`AccountState::commit`]: crate::exchange::mock::account::AccountState::commit
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
    /// Monotone `OrderId` source. Private: resetting it mints duplicate ids, which a consumer
    /// keying a position on an order would silently merge.
    ///
    /// Counts every order this venue booked, including those that retired without trading, so it
    /// is also what [`order_sequence`](Self::order_sequence) reports.
    order_sequence: u64,
    /// Monotone `TradeId` source, independent of [`order_sequence`](Self::order_sequence).
    ///
    /// Separate because one order can print more than once — a taker the book fills in part rests
    /// the remainder and prints again when it is crossed — so an id derived from the order's would
    /// collide between the two. A real venue mints trade ids independently for the same reason;
    /// the link back to the order is [`Trade::order_id`], which is typed rather than parsed.
    trade_sequence: u64,
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
            trade_sequence: 0,
            time_exchange_latest: Default::default(),
            regime,
        }
    }

    /// Which price source this venue has, and so which order kinds it accepts.
    pub fn regime(&self) -> VenueRegime {
        self.regime
    }

    /// Sets the instant this venue stamps on everything it produces from now on, retiring any
    /// order whose deadline that instant has reached.
    ///
    /// The caller owns the latency model: a driver simulating a network delay passes an instant
    /// already offset by it. The venue itself models no delay.
    ///
    /// # Deadlines are swept from here and from [`apply_market`](Self::apply_market), never on a
    /// clock of their own
    /// A [`TimeInForce::GoodTillDate`] order must stop working at the instant it says, not at
    /// whenever the next market tick happens to arrive -- otherwise a cancel sent after the
    /// nominal deadline would succeed against an order that should already have been retired, and
    /// an instrument that stops ticking would hold its expired orders open indefinitely. So both
    /// of this venue's inputs sweep.
    ///
    /// The sweep is **not** scoped to any instrument: a deadline is a property of the clock, not of
    /// a book. An order on a quiet instrument is retired by activity anywhere on the venue.
    ///
    /// ## The residual limitation, stated plainly
    /// This venue has no timer. Nothing retires an order until a driver next advances the clock or
    /// feeds a market, so a deadline falling after the last such call is never reached at all. A
    /// driver that stops driving leaves orders working past their deadline, and that is a property
    /// of the simulation rather than something a caller can configure away.
    ///
    /// # Returns the expiries it caused, which a driver must deliver
    /// Per retired order the events are `[balance, order]` -- the released reservation, then the
    /// terminal [`Expired`] snapshot -- or `[order]` alone for an order this venue never took a
    /// reservation for, exactly as [`cancel_order`](Self::cancel_order) reports one.
    ///
    /// `#[must_use]` because a driver that advances the clock and drops the result has silently
    /// eaten the expiries, leaving its client holding orders the venue no longer has and a balance
    /// it has already released.
    #[must_use]
    pub fn advance_time(&mut self, time_exchange: DateTime<Utc>) -> Vec<UnindexedAccountEvent> {
        self.time_exchange_latest = time_exchange;
        self.account.update_time_exchange(time_exchange);
        self.sweep_expired(time_exchange)
    }

    /// Retires every order whose deadline `time_exchange` has reached, releasing what it held.
    ///
    /// Shared by this venue's two inputs so that a deadline means the same thing whichever one
    /// reaches it first -- see [`advance_time`](Self::advance_time).
    ///
    /// Deadlines are collected before anything is retired, because retiring removes from the very
    /// index the sweep walks. [`OpenOrders::expired_as_of`] yields them by deadline then
    /// [`ClientOrderId`], so the events are in the same order on every run.
    ///
    /// [`OpenOrders::expired_as_of`]: super::orders::OpenOrders::expired_as_of
    fn sweep_expired(&mut self, time_exchange: DateTime<Utc>) -> Vec<UnindexedAccountEvent> {
        let expired = self.account.orders().expired_as_of(time_exchange);

        if expired.is_empty() {
            return Vec::new();
        }

        let mut events = Vec::with_capacity(expired.len() * 2);

        for cid in expired {
            // Collected from this same index with nothing in between, so this always finds it.
            let Some(RestingOrder { order, reservation }) = self.account.orders_mut().remove(&cid)
            else {
                continue;
            };

            // Released before the terminal snapshot, so a consumer applying them in order never
            // sees an order retired against a balance that still holds its reservation. An order
            // the venue took nothing for releases nothing -- see `OpenOrders`.
            if let Some(Reservation { asset, amount }) = reservation {
                let balance = self.account.release(&asset, amount, time_exchange);
                events.push(self.build_account_event(Snapshot(balance)));
            }

            let expired_order = Order {
                key: order.key,
                side: order.side,
                price: order.price,
                quantity: order.quantity,
                kind: order.kind,
                time_in_force: order.time_in_force,
                state: Expired {
                    id: order.state.id,
                    time_exchange,
                    filled_quantity: order.state.filled_quantity,
                },
            };

            self.account.ack_expired(expired_order.clone());

            events.push(self.build_account_event(Snapshot(UnindexedOrder::from(expired_order))));
        }

        events
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
    /// # `depth` replaces what is left, it does not add to it
    /// [`MarketDepth`] is the size on offer as of this observation, and it is stored as the size
    /// still *available* — drawn down by every taker fill struck against it until the next call
    /// replaces it. That is the only thing that stops two market orders arriving between two
    /// observations from each taking the whole of a size that was displayed once: reading a
    /// stored size does not consume it, so without the draw-down both would see it undiminished.
    ///
    /// It bounds this account's own aggression within one observation and claims nothing more. It
    /// is not queue position against the rest of the market, which stays unmodelled — see the
    /// type's `# What matching does not model`.
    ///
    /// A feed with no sizes passes [`MarketDepth::UNKNOWN`], which caps nothing. See
    /// [`MarketDepth`] for why that, and not "nothing is available", is what absent size means.
    ///
    /// # Not every driver supplies this
    /// A venue only has market state if something feeds it one. `SimRunner` does, routing each
    /// source market event to the venues trading that instrument before the `Engine` sees it.
    /// `MockExchange` has no market feed and cannot acquire one without re-creating the look-ahead
    /// hazard that motivated the split, so a venue driven by it reports [`market`](Self::market) as
    /// `None` forever. That difference is a property of this type, not an accident of wiring.
    ///
    /// # What it prices
    /// Everything this venue fills. It is the market a **resting** order is matched against, the
    /// one a limit order's marketability is judged on, and — on a
    /// [`MarketDriven`](VenueRegime::MarketDriven) venue, which is the only kind that has one — the
    /// one a **market** order is priced from. See the type's `# How an order is priced`.
    ///
    /// So feeding this is not optional for a `MarketDriven` venue: an instrument it has never been
    /// fed cannot fill a market order, and says so rather than reaching for the requester's
    /// snapshot.
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
        depth: MarketDepth,
        time_exchange: DateTime<Utc>,
    ) -> Vec<UnindexedAccountEvent> {
        let entry = self.market.entry(instrument.clone()).or_default();
        entry.snapshot = snapshot;
        entry.depth = depth;
        entry.time_exchange = time_exchange;

        // Swept before matching, so an order whose deadline this tick has reached cannot trade on
        // the very tick that retires it -- see `advance_time`.
        let mut events = self.sweep_expired(time_exchange);
        events.append(&mut self.match_resting(instrument, snapshot, time_exchange));
        events
    }

    /// Fills every resting order on `instrument` that `snapshot` has moved to or through.
    ///
    /// Walks each side best-first and stops at the first order that does not cross, which
    /// [`OpenOrders::resting`]'s price-time ordering makes correct: the crossing test is monotone
    /// in price down a side, so the first order that fails it is followed only by worse ones.
    ///
    /// # What crosses is the remainder
    /// A crossing order fills what is **left** of it: its quantity less whatever
    /// [`Open::filled_quantity`] says has already traded. That is its whole quantity for every
    /// order this venue books itself, which fills nothing before it rests -- but not for one a
    /// configured `initial_state` seeds, which may arrive carrying a live account's partial fill.
    /// Settling and printing the whole quantity of such an order would double-count the part that
    /// traded elsewhere, in the balance ledger and in the trade ledger alike.
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

            // Only what has not already traded crosses. An order can reach this book carrying a
            // filled quantity -- a configured `initial_state` seeds one straight out of a live
            // account -- and settling or printing the whole of it again would charge the ledger a
            // second time for a portion that traded before this venue ever held the order.
            let remaining = order.state.quantity_remaining(order.quantity);

            debug_assert!(
                remaining > Decimal::ZERO,
                "resting order {cid} arrived with {} of {} already filled, so crossing it trades \
                 nothing: an order with no quantity left to trade does not belong on the book",
                order.state.filled_quantity,
                order.quantity
            );

            // A resting order fills at its own limit, never at a price derived from the book --
            // see the type's `# How an order is priced` -- and takes no liquidity, so it is
            // charged the maker rate.
            let settlement =
                self.settlement(&terms, order.side, remaining, limit, Liquidity::Maker);

            let balance = self.settle_resting(&order, reservation, &settlement, time_exchange);

            let order_id = order.state.id.clone();
            let trade = Trade {
                id: self.trade_id_sequence_fetch_add(),
                order_id: order_id.clone(),
                instrument: order.key.instrument.clone(),
                strategy: order.key.strategy.clone(),
                time_exchange,
                side: order.side,
                price: limit,
                quantity: remaining,
                fees: settlement.fees,
            };

            self.account.ack_trade(trade.clone());
            self.account.ack_filled(cid);

            // The order is terminal, and says so as `Inactive(FullyFilled)` rather than as an
            // `Open` carrying a complete fill. Both denote the same fact, but only this one can
            // carry the price it filled at.
            //
            // `filled_quantity` is the order's whole quantity -- what it has done in total, not
            // what this cross did -- while `avg_price` is reported only when this venue struck
            // every fill behind it, which is exactly an order that reached the book with nothing
            // done. An order that arrived part-filled took that part at a price this venue never
            // saw, so it reports no average rather than passing one fill's price off as the mean
            // of two. That is what `Filled::avg_price` being optional is for.
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
                    order.state.filled_quantity.is_zero().then_some(limit),
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
                .commit(&settlement.settled(), time_exchange)
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
        // Both sides are the unfilled remainder, priced at the order's own limit as the maker --
        // `rest_order` holds for it and this settles it, so the two agree by construction rather
        // than by two formulas coinciding.
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
    ///
    /// Counts every order it gave an id to, including those that retired without ever trading. It
    /// is **not** a count of trades — one order can print more than once, and trade ids are minted
    /// from a sequence of their own.
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
    /// A Market order carries no limit price, so the venue prices it from whichever market its
    /// [`VenueRegime`] gives it -- its own on a [`MarketDriven`](VenueRegime::MarketDriven) venue,
    /// and [`RequestOpen::market`](crate::order::request::RequestOpen::market) on a
    /// [`RequestPriced`](VenueRegime::RequestPriced) one. A Limit order is always judged and priced
    /// against this venue's own market; see the type's `# How an order is priced`.
    pub fn open_order(
        &mut self,
        request: OrderRequestOpen<ExchangeId, InstrumentNameExchange>,
    ) -> OpenOutcome {
        // Chosen here so one path holds one snapshot: threading both through would allow two copies
        // to disagree, with nothing to arbitrate.
        let market = self.pricing_market(&request);
        let (response, notifications) = self.open_order_inner(request, market);

        // Everything is already committed -- the trade acknowledged, the order recorded or booked,
        // the ledger moved. All that is left is to package it in the order a driver must deliver
        // it: the balance that pays for a fill before the fill that spent it.
        let events = match notifications {
            Some(OpenOrderNotifications::Filled { balance, trade }) => vec![
                self.build_account_event(balance),
                self.build_account_event(trade),
            ],
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
    /// arrived -- which is whatever the order had done: nothing for one that rested untouched, the
    /// taker part for one the book could only partly fill on arrival, and whatever a configured
    /// `initial_state` seeded, carried through rather than added to. An order the venue holds
    /// nothing against releases nothing and reports no balance; see this type's note on
    /// `initial_state`.
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
        } else if self.account.is_expired(cid) {
            ApiError::OrderAlreadyExpired.into()
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

        let orders_expired = self
            .account
            .orders_expired()
            .cloned()
            .map(UnindexedOrder::from);

        let orders_all = orders_open.chain(orders_cancelled).chain(orders_expired);
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
    /// several early returns, and the ordering contract -- emit the balance before the trade it
    /// paid for -- is the part a reader needs to find. Splitting keeps that contract in a short
    /// caller instead of at the end of a long one.
    ///
    /// Everything an arrival commits is committed *here*, by whichever function decides the
    /// order's fate: [`fill_on_arrival`](Self::fill_on_arrival) acknowledges its own trade and
    /// records what the order became, exactly as [`cancel_on_arrival`](Self::cancel_on_arrival),
    /// [`expire_on_arrival`](Self::expire_on_arrival) and [`rest_order`](Self::rest_order) already
    /// do. The caller is left owning event order and nothing else.
    ///
    /// `market` is the snapshot the request carried, and is used only by an order that is priced
    /// from it -- see the type's `# How an order is priced`.
    /// The market an arriving order is priced against, which is the venue's own wherever it has
    /// one.
    ///
    /// # Why the regime decides, and why there is no fallback
    /// A [`MarketDriven`](VenueRegime::MarketDriven) venue owns a book, and a fill is priced from
    /// the book the order reaches -- that is what a venue *is*. Its driver routes each market event
    /// to it before the client that will react sees it, and books the request at the instant it
    /// arrives, so this snapshot is the market as of that arrival: neither stale, nor from a future
    /// the sender could not have seen.
    ///
    /// [`RequestPriced`](VenueRegime::RequestPriced) has no book at all, so the snapshot the
    /// request carried is its only price source. That is the whole of the difference, and it is why
    /// `MockExchange`'s results do not move.
    ///
    /// A `MarketDriven` venue that has not been fed this instrument answers `None` rather than
    /// falling back to the request's snapshot. A venue with no market for an instrument cannot fill
    /// a market order in it, and saying so is the observable failure; silently pricing from the
    /// requester's view would make the fill depend on which of the two happened to hold a price,
    /// and would re-admit exactly the request-priced behaviour the regime exists to distinguish.
    /// [`fill_on_arrival`](Self::fill_on_arrival) reports it as an unpriceable order, naming the
    /// absent snapshot.
    fn pricing_market(
        &self,
        request: &OrderRequestOpen<ExchangeId, InstrumentNameExchange>,
    ) -> Option<MarketSnapshot> {
        match self.regime {
            VenueRegime::RequestPriced => request.state.market,
            // `VenueInstrumentMarket` is `Copy`, so this leaves no borrow outstanding.
            VenueRegime::MarketDriven => self
                .market(&request.key.instrument)
                .map(|market| market.snapshot),
        }
    }

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

        let now = self.time_exchange();

        if request.state.kind != OrderKind::Limit {
            // Every other kind this venue accepts is marketable on arrival by definition, so only
            // the two aggressing arms an order with no limit price can reach, and `Expire`, are
            // reachable: `Rest` and `FillAndRest` need somewhere to rest, which such an order has
            // not got, and `CancelUnfilled` needs `post_only`, which the static gate already
            // refused on a market order. The remaining arms retire the order unfilled rather than
            // asserting, because filling an order this function could not account for is the one
            // wrong answer.
            //
            // The two aggressing arms differ in what becomes of a remainder the book could not
            // fill: `ImmediateOrCancel` keeps what it got and retires the rest, `FillOrKill`
            // would rather trade nothing -- see `Remainder`.
            return match disposition(request.state.kind, request.state.time_in_force, true, now) {
                Disposition::FillAndCancel => {
                    self.fill_on_arrival(request, market, &terms, Remainder::Cancel)
                }
                Disposition::FillOrNothing => {
                    self.fill_on_arrival(request, market, &terms, Remainder::Kill)
                }
                Disposition::Expire => self.expire_on_arrival(request, now),
                Disposition::FillAndRest | Disposition::CancelUnfilled | Disposition::Rest => {
                    self.cancel_on_arrival(request, now)
                }
            };
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

        // The same snapshot the order will be priced from, so marketability and price cannot be
        // decided against two different views of the book. `validate_order_kind_supported` has
        // already refused a limit order on a `RequestPriced` venue, so on this path
        // `pricing_market` is this venue's own market by construction.
        let marketable = market.is_some_and(|market| crosses(request.state.side, limit, &market));

        match disposition(
            request.state.kind,
            request.state.time_in_force,
            marketable,
            now,
        ) {
            // The three differ only in what becomes of a remainder the book could not fill, which
            // is exactly what they are asked here -- see `Remainder`.
            Disposition::FillAndRest => {
                self.fill_on_arrival(request, market, &terms, Remainder::Rest { limit })
            }
            Disposition::FillAndCancel => {
                self.fill_on_arrival(request, market, &terms, Remainder::Cancel)
            }
            Disposition::FillOrNothing => {
                self.fill_on_arrival(request, market, &terms, Remainder::Kill)
            }
            // Nothing has traded, so the whole quantity is what rests.
            Disposition::Rest => self.rest_order(request, limit, &terms, Decimal::ZERO),
            Disposition::CancelUnfilled => self.cancel_on_arrival(request, now),
            Disposition::Expire => self.expire_on_arrival(request, now),
        }
    }

    /// Retires an order that asked to work only on arrival and could not, having traded nothing.
    ///
    /// Reached by a non-marketable [`TimeInForce::ImmediateOrCancel`] or
    /// [`TimeInForce::FillOrKill`], and by a `post_only` order that would have taken liquidity.
    /// Nothing was reserved and nothing traded, so this owes no balance restatement -- only the
    /// terminal response.
    fn cancel_on_arrival(
        &mut self,
        request: OrderRequestOpen<ExchangeId, InstrumentNameExchange>,
        now: DateTime<Utc>,
    ) -> (
        Order<ExchangeId, InstrumentNameExchange, UnindexedOrderState>,
        Option<OpenOrderNotifications>,
    ) {
        let cancelled = Cancelled {
            id: self.order_id_sequence_fetch_add(),
            time_exchange: now,
            filled_quantity: Decimal::ZERO,
        };

        self.account.ack_cancelled(Order {
            key: request.key.clone(),
            side: request.state.side,
            price: request.state.price,
            quantity: request.state.quantity,
            kind: request.state.kind,
            time_in_force: request.state.time_in_force,
            state: cancelled.clone(),
        });

        (
            Order {
                key: request.key,
                side: request.state.side,
                price: request.state.price,
                quantity: request.state.quantity,
                kind: request.state.kind,
                time_in_force: request.state.time_in_force,
                state: OrderState::inactive(cancelled),
            },
            None,
        )
    }

    /// Retires an order whose [`TimeInForce::GoodTillDate`] deadline had already passed when it
    /// arrived.
    ///
    /// Such an order is never rested, and never traded -- not even when the book would have filled
    /// it, since an order past its deadline has stopped working and whether it *could* have traded
    /// is not a question this venue asks. With no independent clock, deadlines are swept only when
    /// a driver advances it or feeds it a market, so an order accepted past its own deadline would
    /// work until something else happened to touch the venue -- and if nothing did, for the rest of
    /// the run. Retiring it here makes the deadline mean what it says.
    ///
    /// Reported as expired rather than rejected because nothing was wrong with the request: its
    /// price, size and funding were never examined, and calling it a rejection would tell a
    /// consumer's statistics the venue refused an order it merely found too late.
    fn expire_on_arrival(
        &mut self,
        request: OrderRequestOpen<ExchangeId, InstrumentNameExchange>,
        now: DateTime<Utc>,
    ) -> (
        Order<ExchangeId, InstrumentNameExchange, UnindexedOrderState>,
        Option<OpenOrderNotifications>,
    ) {
        let expired = Expired {
            id: self.order_id_sequence_fetch_add(),
            time_exchange: now,
            filled_quantity: Decimal::ZERO,
        };

        self.account.ack_expired(Order {
            key: request.key.clone(),
            side: request.state.side,
            price: request.state.price,
            quantity: request.state.quantity,
            kind: request.state.kind,
            time_in_force: request.state.time_in_force,
            state: expired.clone(),
        });

        (
            Order {
                key: request.key,
                side: request.state.side,
                price: request.state.price,
                quantity: request.state.quantity,
                kind: request.state.kind,
                time_in_force: request.state.time_in_force,
                state: OrderState::expired(expired),
            },
            None,
        )
    }

    /// Trades an order against the book the moment it arrives, for as much of it as the book has.
    ///
    /// # What the book has is what it fills
    /// The fill is capped at the size on offer to this side
    /// ([`VenueInstrumentMarket::depth`]), and that size is drawn down by what this fill takes, so
    /// a second order arriving before the next [`apply_market`](Self::apply_market) sees only what
    /// the first left. A feed supplying no size caps nothing — see [`MarketDepth`].
    ///
    /// `remainder` decides what becomes of the quantity the book could not fill, and it is the
    /// order's own time in force that chose it — see [`Remainder`] and [`Disposition`].
    ///
    /// # An order is funded whole or refused whole
    /// An order that fills in part and rests the rest owes the ledger two things at one instant: a
    /// settlement for what traded and a hold against what did not. They are asked for as one
    /// requirement ([`AccountState::commit`]), so an account that cannot cover both has the order
    /// refused with nothing moved — rather than filled for the part it could afford, which would
    /// be this venue choosing a disposition the sender did not ask for.
    ///
    /// That makes a capped order *dearer* than the same order filling outright, because the
    /// remainder is held at the order's own limit while the fill struck a better price. An order
    /// can therefore be refused for a size the book was willing to sell it.
    ///
    /// [`VenueInstrumentMarket::depth`]: VenueInstrumentMarket::depth
    /// [`AccountState::commit`]: crate::exchange::mock::account::AccountState::commit
    fn fill_on_arrival(
        &mut self,
        request: OrderRequestOpen<ExchangeId, InstrumentNameExchange>,
        market: Option<MarketSnapshot>,
        terms: &InstrumentTerms,
        remainder: Remainder,
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
        let side = request.state.side;

        // What the book has to give this order, which is all of it wherever no size is known.
        let filled_quantity = match self.depth_available(&request.key.instrument, side) {
            Some(available) => request.state.quantity.min(available),
            None => request.state.quantity,
        };
        let unfilled = request.state.quantity - filled_quantity;

        // Nothing trades: either the book has no size left to give, or the order would rather
        // trade nothing than trade in part. `FillOrKill` is the second case and the only one --
        // this is where it stops coinciding with `ImmediateOrCancel`, which takes what it can get.
        // The whole order then meets the fate its remainder would have.
        if filled_quantity <= Decimal::ZERO || (unfilled > Decimal::ZERO && remainder.is_kill()) {
            return match remainder {
                // Nothing has traded, so the whole quantity is what rests.
                Remainder::Rest { limit } => self.rest_order(request, limit, terms, Decimal::ZERO),
                Remainder::Cancel | Remainder::Kill => {
                    self.cancel_on_arrival(request, time_exchange)
                }
            };
        }

        // Marketable on arrival, so this fill takes liquidity and is charged the taker rate.
        let fill = self.settlement(terms, side, filled_quantity, fill_price, Liquidity::Taker);

        // What holding the remainder will cost, priced exactly as the fill that settles it will be
        // -- the unfilled quantity, at the order's own limit, as the maker. Computed before
        // anything moves, so both legs are asked of the ledger as one requirement.
        let held = match remainder {
            Remainder::Rest { limit } if unfilled > Decimal::ZERO => {
                Some(self.settlement(terms, side, unfilled, limit, Liquidity::Maker))
            }
            _ => None,
        };

        // One commitment for the whole arrival. A configured `total != free` (an `initial_state`
        // copied from a live account with margin reserved) is carried through untouched rather
        // than rejected: the ledger represents a held amount, so there is nothing left to refuse.
        let debit = match &held {
            Some(held) => fill.settled_holding(held),
            None => fill.settled(),
        };

        let balance = match self.account.commit(&debit, time_exchange) {
            Ok(balance) => balance,
            Err(insufficient) => {
                return (
                    build_open_order_err_response(
                        request,
                        ApiError::BalanceInsufficient(
                            debit.asset,
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

        // Taken off the book only now that it is paid for, so a refused order consumes nothing.
        self.consume_depth(&request.key.instrument, side, filled_quantity);

        let order_id = self.order_id_sequence_fetch_add();
        let trade = Trade {
            id: self.trade_id_sequence_fetch_add(),
            order_id: order_id.clone(),
            instrument: request.key.instrument.clone(),
            strategy: request.key.strategy.clone(),
            time_exchange,
            side,
            price: fill_price,
            quantity: filled_quantity,
            fees: fill.fees,
        };

        // Booked here rather than by the caller, so that whatever this order became is decided in
        // one place: every other arrival already acknowledges its own outcome.
        self.account.ack_trade(trade.clone());

        let (state, released) = self.retire_filled(
            &request,
            order_id,
            Fill {
                quantity: filled_quantity,
                price: fill_price,
            },
            held,
            time_exchange,
        );

        (
            Order {
                key: request.key,
                side,
                price: request.state.price,
                quantity: request.state.quantity,
                kind: request.state.kind,
                time_in_force: request.state.time_in_force,
                state,
            },
            Some(OpenOrderNotifications::Filled {
                // A displaced order's hold is given back after this order's own is taken, so the
                // later restatement is the one that is true.
                balance: Snapshot(released.unwrap_or(balance)),
                trade,
            }),
        )
    }

    /// Records what an order that traded on arrival became, and reports the state it answers with.
    ///
    /// The three outcomes are the three things a taker fill can leave behind: nothing, a remainder
    /// with nowhere to wait, or a remainder on the book. Returns the balance a displaced order's
    /// released hold restated, if this order replaced one.
    fn retire_filled(
        &mut self,
        request: &OrderRequestOpen<ExchangeId, InstrumentNameExchange>,
        order_id: OrderId,
        fill: Fill,
        held: Option<Settlement>,
        time_exchange: DateTime<Utc>,
    ) -> (UnindexedOrderState, Option<AssetBalance<AssetNameExchange>>) {
        // Spelled out per arm rather than shared: a closure would be monomorphic in the state
        // type, and the two arms that record an order record two different ones.
        match held {
            // The book could not fill all of it and the remainder waits at the order's own limit,
            // carrying what has already traded so `Open::quantity_remaining` stays true for the
            // rest of its life -- which is what `match_resting` settles and prints.
            Some(held) => {
                let open = Open::new(order_id, time_exchange, fill.quantity);
                let released = self.book_rested(
                    Order {
                        key: request.key.clone(),
                        side: request.state.side,
                        price: request.state.price,
                        quantity: request.state.quantity,
                        kind: request.state.kind,
                        time_in_force: request.state.time_in_force,
                        state: open.clone(),
                    },
                    Reservation {
                        asset: held.asset,
                        amount: held.amount,
                    },
                    time_exchange,
                );

                (OrderState::active(open), released)
            }

            // Either it filled outright, or what is left has nowhere to wait.
            None if fill.quantity == request.state.quantity => {
                // One fill is the whole of this order, so its price is also the mean of every
                // fill behind the total -- which is exactly when `avg_price` may be reported.
                let filled = Filled::new(
                    order_id,
                    time_exchange,
                    request.state.quantity,
                    Some(fill.price),
                );
                self.account.ack_filled(request.key.cid.clone());

                (OrderState::fully_filled(filled), None)
            }

            // A market order carries no price, so its remainder has nothing to rest at and retires
            // carrying what did trade -- which is what `Cancelled::filled_quantity` is for, and the
            // first time this venue makes it non-zero.
            None => {
                let cancelled = Cancelled {
                    id: order_id,
                    time_exchange,
                    filled_quantity: fill.quantity,
                };
                self.account.ack_cancelled(Order {
                    key: request.key.clone(),
                    side: request.state.side,
                    price: request.state.price,
                    quantity: request.state.quantity,
                    kind: request.state.kind,
                    time_in_force: request.state.time_in_force,
                    state: cancelled.clone(),
                });

                (OrderState::inactive(cancelled), None)
            }
        }
    }

    /// What is left to take at the best price on the far side of `side`, where a feed says.
    fn depth_available(&self, instrument: &InstrumentNameExchange, side: Side) -> Option<Decimal> {
        self.market
            .get(instrument)
            .and_then(|market| market.depth.available_to(side))
    }

    /// Draws down what an aggressor on `side` can still take, by what this one just took.
    fn consume_depth(
        &mut self,
        instrument: &InstrumentNameExchange,
        side: Side,
        quantity: Decimal,
    ) {
        if let Some(market) = self.market.get_mut(instrument) {
            market.depth.consume(side, quantity);
        }
    }

    /// Puts `order` on the book holding `reservation`, and gives back whatever it displaced.
    ///
    /// Re-opening a [`ClientOrderId`] that is already resting replaces the order under it.
    /// [`OpenOrders::insert`] hands the displaced order's own reservation back rather than dropping
    /// it, because it is still held against this account and nothing else will ever release it --
    /// so it is released here, and the balance that restates is returned. Dropping it instead
    /// leaves `free` permanently short and
    /// [`Balance::used`](crate::balance::Balance::used) permanently overstated.
    ///
    /// Nothing is displaced until the replacement has been paid for, so an order this account
    /// could not fund leaves the one already resting exactly where it was.
    ///
    /// [`OpenOrders::insert`]: super::orders::OpenOrders::insert
    fn book_rested(
        &mut self,
        order: OpenOrder,
        reservation: Reservation,
        time_exchange: DateTime<Utc>,
    ) -> Option<AssetBalance<AssetNameExchange>> {
        let Reservation { asset, amount } =
            self.account.orders_mut().insert(order, Some(reservation))?;

        Some(self.account.release(&asset, amount, time_exchange))
    }

    /// Puts what is left of an order onto the book, holding what that remainder's fill will cost.
    ///
    /// `filled_quantity` is how much of `request` has already traded, and everything here is
    /// computed from what is **left** of it: the reservation covers the unfilled remainder alone,
    /// and the [`Open`] state carries the filled part so that [`Open::quantity_remaining`] stays
    /// true for the rest of the order's life -- which is what
    /// [`match_resting`](Self::match_resting) settles and prints when the book reaches it.
    /// Reserving for the whole quantity would hold balance against a portion that has already
    /// settled, and the fill that eventually released it would settle less than was held.
    ///
    /// The reservation is what the fill will settle, computed once here and settled unchanged --
    /// never a conservative over-estimate. A conservative reservation would have to be released and
    /// re-debited on the fill, and [`AccountState::commit`]'s contract is that an arrival produces
    /// exactly **one** balance restatement; the extra ones would report balances the account never
    /// held, and [`Balance::used`](crate::balance::Balance::used) would misreport for the order's
    /// whole life.
    ///
    /// [`AccountState::commit`]: crate::exchange::mock::account::AccountState::commit
    fn rest_order(
        &mut self,
        request: OrderRequestOpen<ExchangeId, InstrumentNameExchange>,
        limit: Decimal,
        terms: &InstrumentTerms,
        filled_quantity: Decimal,
    ) -> (
        Order<ExchangeId, InstrumentNameExchange, UnindexedOrderState>,
        Option<OpenOrderNotifications>,
    ) {
        let time_exchange = self.time_exchange();

        // Priced and charged exactly as the fill will be: the unfilled remainder, at the order's
        // own limit, as the maker.
        let settlement = self.settlement(
            terms,
            request.state.side,
            request.state.quantity - filled_quantity,
            limit,
            Liquidity::Maker,
        );

        let balance_snapshot = match self.account.commit(&settlement.reserved(), time_exchange) {
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

        // The order rests carrying whatever it has already done, so the book, the client and this
        // venue's own later arithmetic all read the same remainder off it.
        let open = Open::new(
            self.order_id_sequence_fetch_add(),
            time_exchange,
            filled_quantity,
        );

        let released = self.book_rested(
            Order {
                key: request.key.clone(),
                side: request.state.side,
                price: request.state.price,
                quantity: request.state.quantity,
                kind: request.state.kind,
                time_in_force: request.state.time_in_force,
                state: open.clone(),
            },
            Reservation {
                asset: settlement.asset,
                amount: settlement.amount,
            },
            time_exchange,
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
                // A displaced order's hold is given back after this order's own is taken, so the
                // later restatement is the one that is true.
                balance: Snapshot(released.unwrap_or(balance_snapshot)),
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
        OrderId(sequence.to_smolstr())
    }

    /// Mints the next `TradeId`, which no other trade from this venue instance carries.
    ///
    /// Not derived from the order's id: one order can print twice — filling in part on arrival and
    /// again when its remainder is crossed — and two trades under one id would be indistinguishable
    /// to anything reconciling them. See [`TradeId`] for what a consumer may assume of one.
    fn trade_id_sequence_fetch_add(&mut self) -> TradeId {
        let sequence = self.trade_sequence;
        self.trade_sequence += 1;
        TradeId(sequence.to_smolstr())
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

/// Whether this venue can honour `time_in_force` for an order of `order_kind` **at all**.
///
/// This is the static half of the time-in-force decision: it asks only whether the pair could ever
/// be honoured, never what the order should then do. What a supported time in force *does* depends
/// on whether the order is marketable when it arrives, which is not known here -- see
/// [`Disposition`].
///
/// # Most of a time in force is moot for an order that works only on arrival
/// A [`OrderKind::Market`] order trades the instant it arrives and never afterwards, which
/// satisfies every time in force that governs *how long* an order works: there is no later instant
/// at which it could still be working, and a remainder the book could not fill has no limit price
/// to wait at, so it is cancelled rather than kept.
///
/// [`TimeInForce::AtOpen`] and [`TimeInForce::AtClose`] are not of that kind. They govern *when* an
/// order executes, not how long it lasts, and honouring them needs a session calendar this venue
/// has not got. Accepting one would fill it immediately, at a price that is not the auction it
/// asked for -- a wrong fill rather than an ignored flag. This library takes them seriously
/// elsewhere: its IBKR client turns them into real market-on-close and limit-on-close orders, which
/// is correct only because a real venue has the calendar. So they are refused here for the same
/// reason on both order kinds.
///
/// [`TimeInForce::GoodUntilCancelled`] with `post_only` is likewise refused on a market order, as
/// a promise never to take liquidity is one a market order cannot keep.
///
/// # Errors
/// Returns [`ApiError::OrderRejected`] naming what it would take to honour the pair. Rejecting is
/// the point: treating an unsupported time in force as `GoodUntilCancelled` would silently leave an
/// order working that its sender asked to have cancelled, and treating an unsupported one as
/// "fill now" answers a question the sender did not ask.
fn validate_time_in_force_supported(
    order_kind: OrderKind,
    time_in_force: TimeInForce,
) -> Result<(), UnindexedOrderError> {
    const NO_CALENDAR: &str = "a session calendar, which this venue does not have";

    let missing = match (order_kind, time_in_force) {
        // Governs *when* an order executes, not how long it works, whatever the kind.
        (_, TimeInForce::AtOpen | TimeInForce::AtClose) => NO_CALENDAR,

        // A market order takes liquidity by definition, so it cannot promise not to.
        (OrderKind::Market, TimeInForce::GoodUntilCancelled { post_only: true }) => {
            "an order that both takes liquidity and refuses to"
        }

        // Every other time in force bounds how long an order works, which is moot for one that
        // fills in full on arrival.
        (OrderKind::Market, _) => return Ok(()),

        (OrderKind::Limit, TimeInForce::GoodUntilEndOfDay) => NO_CALENDAR,

        // Honoured -- what each one then does is `Disposition`'s decision, not this one.
        (
            OrderKind::Limit,
            TimeInForce::GoodUntilCancelled { .. }
            | TimeInForce::ImmediateOrCancel
            | TimeInForce::FillOrKill
            | TimeInForce::GoodTillDate { .. },
        ) => return Ok(()),

        // Every other kind is rejected before this gate is reached.
        (_, _) => return Ok(()),
    };

    Err(UnindexedOrderError::Rejected(ApiError::OrderRejected(
        format!(
            "SimulatedVenue does not support TimeInForce::{time_in_force} on \
             OrderKind::{order_kind:?}: it does not model {missing}"
        ),
    )))
}

/// What a supported time in force makes one arriving order do.
///
/// The marketability-dependent half of the time-in-force decision, split from
/// [`validate_time_in_force_supported`] because three of the four time in forces this venue
/// honours on a limit order need context that gate cannot see: `post_only`, `ImmediateOrCancel`
/// and `FillOrKill` all turn on whether the order is marketable when it arrives, and
/// `GoodTillDate` turns on the clock.
///
/// # Three ways to aggress, which differ only over a remainder
/// What an aggressing order's time in force really decides is the fate of the quantity the book
/// could not fill: it rests, or it retires, or -- for [`TimeInForce::FillOrKill`] -- nothing trades
/// at all. Which of them an order asked for is a property of the order rather than of what the
/// book happened to hold, so all three are decided here and the answer is handed to
/// [`fill_on_arrival`](SimulatedVenue::fill_on_arrival) as a [`Remainder`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Disposition {
    /// Trade against the book now as the aggressor, resting whatever does not fill.
    FillAndRest,
    /// Trade against the book now as the aggressor, retiring whatever does not fill.
    FillAndCancel,
    /// Trade against the book now as the aggressor, but only if it fills in full: an order that
    /// could fill only in part trades nothing at all.
    FillOrNothing,
    /// Join the book and wait to be crossed.
    Rest,
    /// Retire immediately having traded nothing, because the order asked to work only now and
    /// could not: a non-marketable `ImmediateOrCancel` or `FillOrKill`, or a `post_only` order
    /// that would have taken liquidity.
    CancelUnfilled,
    /// Retire immediately having traded nothing, because the order's own deadline has passed.
    Expire,
}

/// Decide what one order does, given whether it is marketable and what time it is.
///
/// `marketable` is [`crosses`] for a limit order, and `true` for every other kind this venue
/// accepts -- each of those is marketable on arrival by definition, which is what lets one function
/// answer for both.
///
/// `order_kind` decides one thing only: whether an unfilled remainder has anywhere to go. An order
/// carrying no limit price has nothing to wait at, so a market order aggresses and retires whatever
/// is left of it, whichever time in force it carries.
///
/// # Immediate-or-cancel and fill-or-kill differ only when the book is too thin
/// Both aggress on arrival and neither waits, so while the book can fill an order whole they are
/// the same instruction. They part exactly where a fill is capped by the size on offer:
/// [`Disposition::FillAndCancel`] takes what there is and retires the rest, while
/// [`Disposition::FillOrNothing`] would rather trade nothing than trade in part, and so trades
/// nothing. Which one an order gets is decided here; what they then do with the remainder is
/// [`Remainder`]'s.
fn disposition(
    order_kind: OrderKind,
    time_in_force: TimeInForce,
    marketable: bool,
    now: DateTime<Utc>,
) -> Disposition {
    match time_in_force {
        // Post-only means "never take liquidity", so being marketable is what disqualifies it.
        TimeInForce::GoodUntilCancelled { post_only: true } if marketable => {
            Disposition::CancelUnfilled
        }

        // A deadline already reached retires the order whatever the book is doing. Checked before
        // marketability so that a deadline is an unconditional cutoff rather than one an order can
        // trade its way past at the very instant it expires.
        TimeInForce::GoodTillDate { expiry } if now >= expiry => Disposition::Expire,

        // The one time in force that would rather trade nothing than trade in part, whatever the
        // order kind.
        TimeInForce::FillOrKill if marketable => Disposition::FillOrNothing,

        // Every other aggressing order keeps what it could not fill only if it has somewhere to
        // keep it: a limit price to wait at, and a time in force that asked to wait. `post_only`
        // is not among them here -- a marketable one was disqualified above.
        _ if marketable => match (order_kind, time_in_force) {
            (
                OrderKind::Limit,
                TimeInForce::GoodUntilCancelled { .. } | TimeInForce::GoodTillDate { .. },
            ) => Disposition::FillAndRest,
            _ => Disposition::FillAndCancel,
        },

        TimeInForce::ImmediateOrCancel | TimeInForce::FillOrKill => Disposition::CancelUnfilled,

        _ => Disposition::Rest,
    }
}

/// What becomes of the quantity an arriving order's book could not fill.
///
/// The half of a [`Disposition`] that [`fill_on_arrival`](SimulatedVenue::fill_on_arrival) needs,
/// derived at the two call sites that can reach it. Carrying the limit price *here* rather than
/// re-reading [`RequestOpen::price`](crate::order::request::RequestOpen::price) is what makes
/// "rest what is left" unrepresentable for an order that has nowhere to rest it: a market order
/// carries no price, so it cannot construct [`Rest`](Self::Rest) at all.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Remainder {
    /// Joins the book at this limit and waits to be crossed.
    Rest { limit: Decimal },
    /// Retires as [`Cancelled`], carrying whatever did trade.
    Cancel,
    /// Nothing trades at all. An order that cannot be filled in full would rather be filled not at
    /// all, so there is never a remainder for this arm to dispose of -- only a whole order to
    /// retire untouched.
    Kill,
}

impl Remainder {
    /// Whether a remainder is refused rather than disposed of, which is what makes
    /// [`TimeInForce::FillOrKill`] differ from [`TimeInForce::ImmediateOrCancel`].
    fn is_kill(self) -> bool {
        matches!(self, Self::Kill)
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

/// What one open owes the client, before it is committed and packaged into events.
///
/// Internal to [`SimulatedVenue::open_order`]: it exists only to keep the ordering contract --
/// emit the balance before the trade it paid for -- visible in one short place rather than at the
/// end of the pricing body. Nothing here is uncommitted; it is what to *say*, not what to do.
///
/// An enum rather than an optional trade because the two outcomes owe different things and a
/// missing trade is not an absent field: a rested open has a balance and *no* trade, and nothing
/// about it is incomplete.
///
/// # One arrival owes one balance
/// Including the arrival that both settles a fill and holds against the remainder it leaves
/// resting. Those are one event at the venue and one commitment in the ledger
/// ([`AccountState::commit`]), so they restate the balance once — see that method for why an
/// intermediate would report a balance the account never held.
///
/// [`AccountState::commit`]: crate::exchange::mock::account::AccountState::commit
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

/// How much of an arriving order traded, and at what price.
///
/// One value because the two are decided together and used together: the quantity is what the
/// book had to give, and the price is what it gave it at.
#[derive(Debug, Clone, Copy)]
struct Fill {
    quantity: Decimal,
    price: Decimal,
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

impl Settlement {
    /// The whole of this settlement leaves the account, because the whole of it traded.
    fn settled(&self) -> Debit {
        Debit {
            asset: self.asset.clone(),
            settled: self.amount,
            reserved: Decimal::ZERO,
        }
    }

    /// The whole of this settlement is held, because the order it belongs to is still working.
    fn reserved(&self) -> Debit {
        Debit {
            asset: self.asset.clone(),
            settled: Decimal::ZERO,
            reserved: self.amount,
        }
    }

    /// This settlement traded and `remainder` is held against what is still working: one order
    /// that the book filled in part and that rested the rest.
    ///
    /// # Panics
    /// Debug-asserts that both settlements pay with the same asset. They come from one order, so
    /// they share its side and its instrument, and [`SimulatedVenue::settlement`] decides the
    /// asset from nothing else — a disagreement would mean one order paying with two assets.
    fn settled_holding(&self, remainder: &Settlement) -> Debit {
        debug_assert!(
            self.asset == remainder.asset,
            "one order settled {} of {} and reserved {} of {}: an order pays with one asset, so \
             two settlements of it cannot name two",
            self.amount,
            self.asset,
            remainder.amount,
            remainder.asset
        );

        Debit {
            asset: self.asset.clone(),
            settled: self.amount,
            reserved: remainder.amount,
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use crate::{
        error::OrderError,
        exchange::mock::{fixtures::*, orders::as_open},
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
        advance(&mut venue, time(1));

        assert!(
            venue
                .apply_market(
                    &instrument_name(),
                    book("49000", "49100"),
                    MarketDepth::UNKNOWN,
                    time(1)
                )
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

    /// Advances `venue`, asserting the advance retired nothing.
    ///
    /// Every existing test predates deadlines and holds only `GoodUntilCancelled` orders, so a
    /// sweep that produced anything would mean an order retired that nothing asked to expire.
    /// Asserting that beats discarding the value: it is the same one line, and it pins the
    /// property instead of hiding it.
    fn advance(venue: &mut SimulatedVenue, time_exchange: DateTime<Utc>) {
        assert!(
            venue.advance_time(time_exchange).is_empty(),
            "advancing to {time_exchange} retired an order this test never gave a deadline"
        );
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
    }

    /// `advance_time` is the venue's only clock input, and it stamps everything produced after it.
    #[test]
    fn a_fill_is_stamped_with_the_instant_the_venue_was_last_advanced() {
        let mut venue = make_venue("100", "10000000");
        let time_exchange = "2025-01-01T00:00:00Z".parse::<DateTime<Utc>>().unwrap();

        advance(&mut venue, time_exchange);
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

        advance(&mut venue, much_later);

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
        advance(&mut venue, time(2));

        // Straight through the limit: the offer is 100 better than the order asked for.
        let events = venue.apply_market(
            &instrument_name(),
            book("47800", "47900"),
            MarketDepth::UNKNOWN,
            time(2),
        );

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
        advance(&mut venue, time(2));

        let events = venue.apply_market(
            &instrument_name(),
            book("47800", "47900"),
            MarketDepth::UNKNOWN,
            time(2),
        );

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
        advance(&mut venue, time(2));

        let events = venue.apply_market(
            &instrument_name(),
            book("47800", "47900"),
            MarketDepth::UNKNOWN,
            time(2),
        );

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
        advance(&mut venue, time(2));

        let reserved = d("48009.6");
        let free_while_resting = venue.balances(&[quote()]).remove(0).balance.free;

        let events = venue.apply_market(
            &instrument_name(),
            book("47800", "47900"),
            MarketDepth::UNKNOWN,
            time(2),
        );

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

    // --- An order trades only what is left of it ---------------------------------------------

    /// An open buy at `price` that reached the book with `filled` of `quantity` already done, as a
    /// configured `initial_state` copied from a live account may seed one.
    fn seeded_part_filled(price: &str, quantity: &str, filled: &str) -> OpenOrder {
        as_open(Order {
            key: OrderKey {
                exchange: EXCHANGE,
                instrument: instrument_name(),
                strategy: StrategyId::new("test"),
                cid: ClientOrderId::new("part_filled"),
            },
            side: Side::Buy,
            price: Some(d(price)),
            quantity: d(quantity),
            kind: OrderKind::Limit,
            time_in_force: gtc(),
            state: OrderState::active(Open::new(OrderId::new("part_filled"), time(1), d(filled))),
        })
        .expect("the seeded order is Open")
    }

    /// An order that reached the book part-filled trades only the rest of it.
    ///
    /// Settling and printing its whole quantity would charge the ledger a second time for a
    /// portion that traded before this venue ever held the order, and report a trade of a size
    /// that never happened. The two readings are far enough apart to be unmistakable: the whole
    /// order costs 48,009.6 of quote and its remainder 28,805.76.
    #[test]
    fn a_part_filled_order_on_the_book_settles_and_prints_only_its_remainder() {
        let mut venue = make_market_venue("10", "1000000");
        advance(&mut venue, time(1));

        venue.account.orders_mut().insert(
            seeded_part_filled("48000", "1", "0.4"),
            // As `initial_state` seeds one: the venue never took anything for it.
            None,
        );

        advance(&mut venue, time(2));
        let events = venue.apply_market(
            &instrument_name(),
            book("47800", "47900"),
            MarketDepth::UNKNOWN,
            time(2),
        );

        assert_eq!(
            events.len(),
            3,
            "one filled order owes a balance, a trade and a terminal order snapshot"
        );

        let trade = trade_of(&events[1]);
        assert_eq!(
            trade.quantity,
            d("0.6"),
            "0.6 is what was left to trade; printing 1 would report a fill that did not happen"
        );
        assert_eq!(
            trade.fees.fees,
            d("5.76"),
            "2bp maker on 0.6 at 48000, not on the whole quantity"
        );

        assert_eq!(
            balance_of(&events[0]).balance.total,
            d("1000000") - d("28805.76"),
            "the fill costs what the remainder costs; 48009.6 would pay a second time for the 0.4 \
             that traded elsewhere"
        );
    }

    /// A fill that completes a part-filled order reports no average price.
    ///
    /// This venue struck one of the two fills behind it and never saw the other, so the limit it
    /// filled at is this fill's price rather than the mean of both — and
    /// [`Filled::avg_price`](crate::order::state::Filled::avg_price) is optional for exactly that.
    /// A consumer needing the mean has the trades to compute it from. An order that reached the
    /// book with nothing done still reports the price it filled at, which
    /// `a_resting_order_fills_at_its_own_limit_and_is_never_improved_by_the_book` pins.
    #[test]
    fn a_fill_completing_a_part_filled_order_reports_no_average_price() {
        let mut venue = make_market_venue("10", "1000000");
        advance(&mut venue, time(1));

        venue
            .account
            .orders_mut()
            .insert(seeded_part_filled("48000", "1", "0.4"), None);

        advance(&mut venue, time(2));
        let events = venue.apply_market(
            &instrument_name(),
            book("47800", "47900"),
            MarketDepth::UNKNOWN,
            time(2),
        );

        let filled = match &order_of(&events[2]).state {
            OrderState::Inactive(InactiveOrderState::FullyFilled(filled)) => filled,
            other => panic!("expected a fully filled order, got: {other:?}"),
        };

        assert_eq!(
            filled.filled_quantity,
            d("1"),
            "the order has now done its whole quantity, which is what this field reports"
        );
        assert_eq!(
            filled.avg_price, None,
            "48000 is one of the two prices behind that quantity, not their mean"
        );
    }

    /// Resting the remainder of a part-filled order holds what is left of it, and the cross that
    /// completes it settles exactly that.
    ///
    /// `rest_order` is reached directly because nothing routes a part-filled order to it yet: this
    /// venue caps no fill, so an order that aggresses on arrival fills in full and never rests a
    /// remainder. The arithmetic is pinned all the same, because the two halves have to agree —
    /// holding against the whole quantity would reserve balance for a portion that has already
    /// settled, and the fill that released it would then settle less than was held.
    #[test]
    fn resting_a_part_filled_order_reserves_and_settles_exactly_its_remainder() {
        let mut venue = make_market_venue("10", "1000000");
        advance(&mut venue, time(1));

        let terms = venue
            .instrument_terms(&instrument_name())
            .expect("the spot fixture is priceable");

        let (response, notifications) = venue.rest_order(
            limit_request("part_filled", Side::Buy, "1", "48000", gtc()),
            d("48000"),
            &terms,
            d("0.4"),
        );

        match &response.state {
            OrderState::Active(ActiveOrderState::Open(open)) => assert_eq!(
                open.filled_quantity,
                d("0.4"),
                "the order rests carrying what it has already done"
            ),
            other => panic!("expected a resting open order, got: {other:?}"),
        }

        match notifications {
            Some(OpenOrderNotifications::Rested { balance }) => assert_eq!(
                balance.0.balance.free,
                d("1000000") - d("28805.76"),
                "0.6 at 48000 plus 2bp maker is held; 48009.6 would hold against the 0.4 that has \
                 already settled"
            ),
            other => panic!("a rested order restates one balance, got: {other:?}"),
        }

        advance(&mut venue, time(2));
        let events = venue.apply_market(
            &instrument_name(),
            book("47800", "47900"),
            MarketDepth::UNKNOWN,
            time(2),
        );

        assert_eq!(events.len(), 3);
        assert_eq!(
            trade_of(&events[1]).quantity,
            d("0.6"),
            "the cross prints the remainder that was held for"
        );
        assert_eq!(
            balance_of(&events[0]).balance.total,
            d("1000000") - d("28805.76"),
            "and settles exactly that: one restatement, with nothing left over to release"
        );
        assert_eq!(
            balance_of(&events[0]).balance.used(),
            Decimal::ZERO,
            "nothing is held any more: the order that held it is gone"
        );
    }

    /// Which of the three ways to aggress an order's terms asked for.
    ///
    /// Asserted on [`Disposition`] directly because nothing observable separates them yet: this
    /// venue caps no fill, so every arm below fills in full and leaves no remainder to dispose of.
    /// What an order asked for is a property of the order rather than of what the book held, and a
    /// size cap is what would make the difference visible — fill-or-kill refusing the partial fill
    /// immediate-or-cancel accepts, and a good-until-cancelled limit resting what neither keeps.
    #[test]
    fn a_marketable_order_aggresses_the_way_its_own_terms_asked_for() {
        let now = time(1);
        let gtd = TimeInForce::GoodTillDate { expiry: time(9) };

        assert_eq!(
            disposition(OrderKind::Limit, gtc(), true, now),
            Disposition::FillAndRest,
            "a limit order with somewhere to wait keeps whatever it could not fill"
        );
        assert_eq!(
            disposition(OrderKind::Limit, gtd, true, now),
            Disposition::FillAndRest,
            "a deadline it has not reached is still somewhere to wait"
        );
        assert_eq!(
            disposition(OrderKind::Limit, TimeInForce::ImmediateOrCancel, true, now),
            Disposition::FillAndCancel,
            "immediate-or-cancel asked not to wait"
        );
        assert_eq!(
            disposition(OrderKind::Limit, TimeInForce::FillOrKill, true, now),
            Disposition::FillOrNothing,
            "fill-or-kill would rather trade nothing than trade in part"
        );
        assert_eq!(
            disposition(OrderKind::Market, gtc(), true, now),
            Disposition::FillAndCancel,
            "an order carrying no limit price has nothing to wait at, whatever its time in force"
        );
        assert_eq!(
            disposition(OrderKind::Market, gtd, true, now),
            Disposition::FillAndCancel,
            "including one whose deadline is still ahead of it"
        );
        assert_eq!(
            disposition(OrderKind::Market, TimeInForce::FillOrKill, true, now),
            Disposition::FillOrNothing,
            "and all-or-nothing stays all-or-nothing whatever the kind"
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
        advance(&mut venue, time(1));
        assert!(
            venue
                .apply_market(
                    &instrument_name(),
                    book("47800", "47900"),
                    MarketDepth::UNKNOWN,
                    time(1)
                )
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
        advance(&mut venue, time(1));

        // The offer is inside the limit, so the order is marketable -- but the trade price the
        // model reads is well above it.
        let stale = MarketSnapshot {
            best_bid: Some(d("46900")),
            best_ask: Some(d("47000")),
            last_price: Some(d("48000")),
        };
        assert!(
            venue
                .apply_market(&instrument_name(), stale, MarketDepth::UNKNOWN, time(1))
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
        advance(&mut aggressor, time(1));
        assert!(
            aggressor
                .apply_market(
                    &instrument_name(),
                    touching(),
                    MarketDepth::UNKNOWN,
                    time(1)
                )
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
        advance(&mut maker, time(1));
        assert!(
            maker
                .apply_market(
                    &instrument_name(),
                    book("49000", "49100"),
                    MarketDepth::UNKNOWN,
                    time(1)
                )
                .is_empty()
        );
        let rested = maker.open_order(limit_request("b", Side::Buy, "1", limit, gtc()));
        assert_eq!(rested.events.len(), 1, "it rested rather than filling");
        advance(&mut maker, time(2));
        let resting_events = maker.apply_market(
            &instrument_name(),
            touching(),
            MarketDepth::UNKNOWN,
            time(2),
        );
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
        advance(&mut venue, time(1));
        assert!(
            venue
                .apply_market(
                    &instrument_name(),
                    book("49000", "49100"),
                    MarketDepth::UNKNOWN,
                    time(1)
                )
                .is_empty()
        );

        // Best bid last, to prove the fill order is the book's rather than the insertion order.
        for (cid, price) in [("worst", "47000"), ("mid", "48000"), ("best", "48500")] {
            let outcome = venue.open_order(limit_request(cid, Side::Buy, "1", price, gtc()));
            assert_eq!(outcome.events.len(), 1, "{cid} must rest");
        }

        advance(&mut venue, time(2));
        // Crosses `best` and `mid`, leaves `worst` alone.
        let events = venue.apply_market(
            &instrument_name(),
            book("47900", "48000"),
            MarketDepth::UNKNOWN,
            time(2),
        );

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
        advance(&mut venue, time(1));
        assert!(
            venue
                .apply_market(
                    &instrument_name(),
                    traded("49000"),
                    MarketDepth::UNKNOWN,
                    time(1)
                )
                .is_empty()
        );

        let outcome = venue.open_order(limit_request("t", Side::Buy, "1", "48000", gtc()));
        assert_eq!(outcome.events.len(), 1, "49000 traded does not reach 48000");

        advance(&mut venue, time(2));
        let events = venue.apply_market(
            &instrument_name(),
            traded("47500"),
            MarketDepth::UNKNOWN,
            time(2),
        );

        assert_eq!(events.len(), 3, "a print through the limit fills it");
        assert_eq!(trade_of(&events[1]).price, d("48000"));
    }

    /// A price between the two sides of a book crosses neither, so a supplied book is never
    /// second-guessed by `last_price`.
    #[test]
    fn a_price_inside_the_spread_does_not_cross_a_resting_order() {
        let (mut venue, _) = venue_resting_one_buy("48000");
        advance(&mut venue, time(2));

        // The microprice sits inside the spread and below the limit; the offer does not.
        let inside = MarketSnapshot {
            best_bid: Some(d("47900")),
            best_ask: Some(d("48100")),
            last_price: Some(d("47950")),
        };

        assert!(
            venue
                .apply_market(&instrument_name(), inside, MarketDepth::UNKNOWN, time(2))
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
        advance(&mut venue, time(1));
        assert!(
            venue
                .apply_market(
                    &instrument_name(),
                    book("47000", "47100"),
                    MarketDepth::UNKNOWN,
                    time(1)
                )
                .is_empty()
        );

        let outcome = venue.open_order(limit_request("s", Side::Sell, "1", "48000", gtc()));
        assert_eq!(outcome.events.len(), 1, "47000 bid does not reach 48000");
        assert_eq!(
            balance_of(&outcome.events[0]).asset,
            base(),
            "a spot sell delivers the base asset, so that is what is held"
        );

        advance(&mut venue, time(2));
        let events = venue.apply_market(
            &instrument_name(),
            book("48100", "48200"),
            MarketDepth::UNKNOWN,
            time(2),
        );

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
        advance(&mut venue, time(1));
        assert!(
            venue
                .apply_market(
                    &instrument_name(),
                    book("49000", "49100"),
                    MarketDepth::UNKNOWN,
                    time(1)
                )
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

        advance(&mut venue, time(2));
        let events = venue.apply_market(
            &instrument_name(),
            book("47800", "47900"),
            MarketDepth::UNKNOWN,
            time(2),
        );

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
        advance(&mut venue, time(1));
        assert!(
            venue
                .apply_market(
                    &instrument_name(),
                    book("49000", "49100"),
                    MarketDepth::UNKNOWN,
                    time(1)
                )
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
        advance(&mut venue, time(2));

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
        advance(&mut venue, time(2));
        let events = venue.apply_market(
            &instrument_name(),
            book("47800", "47900"),
            MarketDepth::UNKNOWN,
            time(2),
        );
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
            TimeInForce::GoodUntilEndOfDay,
            TimeInForce::AtOpen,
            TimeInForce::AtClose,
        ];

        for time_in_force in unsupported {
            let mut venue = make_market_venue("10", "1000000");
            advance(&mut venue, time(1));
            assert!(
                venue
                    .apply_market(
                        &instrument_name(),
                        book("49000", "49100"),
                        MarketDepth::UNKNOWN,
                        time(1)
                    )
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

    /// A time in force that only bounds *how long* an order works is moot for one that fills in
    /// full on arrival, and must not stop a market order filling.
    #[test]
    fn a_market_order_is_accepted_for_every_time_in_force_that_only_bounds_how_long_it_works() {
        for time_in_force in [
            TimeInForce::GoodUntilCancelled { post_only: false },
            TimeInForce::ImmediateOrCancel,
            TimeInForce::FillOrKill,
            TimeInForce::GoodTillDate { expiry: time(9) },
            TimeInForce::GoodUntilEndOfDay,
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

    /// The two kinds of time in force a market order cannot keep, as opposed to the many it makes
    /// moot.
    ///
    /// `AtOpen` and `AtClose` say *when* an order executes rather than how long it works, and
    /// honouring them needs a session calendar. Filling one on arrival would answer a question its
    /// sender did not ask, at a price that is not the auction it wanted — this library's own IBKR
    /// client turns them into real market-on-close orders, so they are not a flag to be ignored.
    /// `post_only` is refused because a market order takes liquidity by definition.
    #[test]
    fn a_market_order_is_rejected_for_a_time_in_force_it_cannot_keep() {
        for time_in_force in [
            TimeInForce::GoodUntilCancelled { post_only: true },
            TimeInForce::AtOpen,
            TimeInForce::AtClose,
        ] {
            let mut venue = make_venue("10", "1000000");
            let mut request = buy_request("1", market_prices("48000"));
            request.state.time_in_force = time_in_force;

            let outcome = venue.open_order(request);

            assert!(
                outcome.events.is_empty(),
                "{time_in_force} must move no balance"
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

    // --- Stage 3b-ii: post-only -------------------------------------------------------------

    /// Post-only means "never take liquidity", so being marketable is what disqualifies the order.
    ///
    /// It retires having traded nothing rather than being rejected: the request was valid, and the
    /// venue did exactly what `post_only` asked of it.
    #[test]
    fn a_post_only_order_that_would_take_liquidity_is_cancelled_having_traded_nothing() {
        let mut venue = make_market_venue("10", "1000000");
        advance(&mut venue, time(1));
        assert!(
            venue
                .apply_market(
                    &instrument_name(),
                    book("49000", "49100"),
                    MarketDepth::UNKNOWN,
                    time(1)
                )
                .is_empty()
        );

        // Buying at 50000 crosses an ask of 49100, so this order would be the aggressor.
        let outcome = venue.open_order(limit_request(
            "post",
            Side::Buy,
            "1",
            "50000",
            TimeInForce::GoodUntilCancelled { post_only: true },
        ));

        assert!(outcome.events.is_empty(), "nothing was reserved or traded");
        assert!(venue.orders_open(&[]).is_empty(), "it must not rest");
        assert!(venue.trades(time(0)).is_empty(), "it must not trade");

        match outcome.response.state {
            OrderState::Inactive(InactiveOrderState::Cancelled(ref cancelled)) => {
                assert_eq!(cancelled.filled_quantity, Decimal::ZERO)
            }
            ref other => panic!("a marketable post-only order must be cancelled, got: {other:?}"),
        }
    }

    /// The same order that does *not* cross is exactly what post-only is for, and rests normally.
    #[test]
    fn a_post_only_order_that_would_rest_is_accepted_and_reserves() {
        let mut venue = make_market_venue("10", "1000000");
        advance(&mut venue, time(1));
        assert!(
            venue
                .apply_market(
                    &instrument_name(),
                    book("49000", "49100"),
                    MarketDepth::UNKNOWN,
                    time(1)
                )
                .is_empty()
        );

        let outcome = venue.open_order(limit_request(
            "post",
            Side::Buy,
            "1",
            "48000",
            TimeInForce::GoodUntilCancelled { post_only: true },
        ));

        assert_eq!(rested_open(&outcome).filled_quantity, Decimal::ZERO);
        assert_eq!(venue.orders_open(&[]).len(), 1);
        assert_eq!(
            balance_of(&outcome.events[0]).balance.free,
            d("1000000") - d("48000") - d("9.6"),
            "resting post-only reserves the notional plus the 2bp maker fee its fill will charge"
        );
    }

    // --- Stage 3b-ii: immediate-or-cancel and fill-or-kill -----------------------------------

    /// Both retire unfilled when the book is not there to trade against, and neither rests.
    ///
    /// They coincide because this venue models no order size — see `disposition`.
    #[test]
    fn a_non_marketable_immediate_order_is_cancelled_having_traded_nothing() {
        for time_in_force in [TimeInForce::ImmediateOrCancel, TimeInForce::FillOrKill] {
            let mut venue = make_market_venue("10", "1000000");
            advance(&mut venue, time(1));
            assert!(
                venue
                    .apply_market(
                        &instrument_name(),
                        book("49000", "49100"),
                        MarketDepth::UNKNOWN,
                        time(1)
                    )
                    .is_empty()
            );

            let outcome =
                venue.open_order(limit_request("ioc", Side::Buy, "1", "48000", time_in_force));

            assert!(
                outcome.events.is_empty(),
                "{time_in_force} must move no balance"
            );
            assert!(
                venue.orders_open(&[]).is_empty(),
                "{time_in_force} must not rest"
            );
            match outcome.response.state {
                OrderState::Inactive(InactiveOrderState::Cancelled(ref cancelled)) => assert_eq!(
                    cancelled.filled_quantity,
                    Decimal::ZERO,
                    "{time_in_force} traded nothing"
                ),
                ref other => panic!("{time_in_force} must be cancelled, got: {other:?}"),
            }
        }
    }

    /// Marketable, and both fill in full against the book as the aggressor.
    #[test]
    fn a_marketable_immediate_order_fills_in_full() {
        for time_in_force in [TimeInForce::ImmediateOrCancel, TimeInForce::FillOrKill] {
            let mut venue = make_market_venue("10", "1000000");
            advance(&mut venue, time(1));
            assert!(
                venue
                    .apply_market(
                        &instrument_name(),
                        traded("49000"),
                        MarketDepth::UNKNOWN,
                        time(1)
                    )
                    .is_empty()
            );

            let outcome =
                venue.open_order(limit_request("ioc", Side::Buy, "1", "50000", time_in_force));

            assert!(
                matches!(
                    outcome.response.state,
                    OrderState::Inactive(InactiveOrderState::FullyFilled(_))
                ),
                "{time_in_force} must fill, got: {:?}",
                outcome.response.state
            );
        }
    }

    // --- Stage 3b-ii: deadlines -------------------------------------------------------------

    /// A deadline in the future rests like any other order, and nothing retires it early.
    #[test]
    fn a_good_till_date_order_rests_until_its_deadline() {
        let mut venue = make_market_venue("10", "1000000");
        advance(&mut venue, time(1));

        let outcome = venue.open_order(limit_request(
            "gtd",
            Side::Buy,
            "1",
            "48000",
            TimeInForce::GoodTillDate { expiry: time(9) },
        ));

        assert_eq!(rested_open(&outcome).filled_quantity, Decimal::ZERO);

        // Short of the deadline, so the sweep must find nothing.
        advance(&mut venue, time(8));
        assert_eq!(venue.orders_open(&[]).len(), 1, "still working at time(8)");
    }

    /// Advancing to the deadline retires the order, releases what it held, and says so in order.
    #[test]
    fn advancing_to_a_deadline_releases_the_reservation_then_reports_the_order() {
        let mut venue = make_market_venue("10", "1000000");
        advance(&mut venue, time(1));

        let outcome = venue.open_order(limit_request(
            "gtd",
            Side::Buy,
            "1",
            "48000",
            TimeInForce::GoodTillDate { expiry: time(9) },
        ));
        let reserved = balance_of(&outcome.events[0]).balance.free;

        let events = venue.advance_time(time(9));

        assert_eq!(events.len(), 2, "one release and one terminal snapshot");
        assert_eq!(
            balance_of(&events[0]).balance.free,
            reserved + d("48000") + d("9.6"),
            "the release restates exactly what resting took"
        );

        let order = order_of(&events[1]);
        match order.state {
            OrderState::Inactive(InactiveOrderState::Expired(ref expired)) => {
                assert_eq!(expired.time_exchange, time(9));
                assert_eq!(expired.filled_quantity, Decimal::ZERO);
            }
            ref other => panic!("the order must be reported expired, got: {other:?}"),
        }

        assert!(venue.orders_open(&[]).is_empty(), "it must leave the book");
    }

    /// The deadline is reached *at* its instant, not after it.
    ///
    /// The boundary is the whole contract: a caller reading "good till `T`" must be able to say
    /// whether the order is working at `T`, and this pins the answer to "no".
    #[test]
    fn a_deadline_is_reached_at_its_instant_not_after_it() {
        let mut venue = make_market_venue("10", "1000000");
        advance(&mut venue, time(1));

        let _ = venue.open_order(limit_request(
            "gtd",
            Side::Buy,
            "1",
            "48000",
            TimeInForce::GoodTillDate { expiry: time(5) },
        ));

        // One second short: still working.
        advance(&mut venue, time(4));
        assert_eq!(venue.orders_open(&[]).len(), 1);

        // Exactly the stated instant: retired.
        assert_eq!(venue.advance_time(time(5)).len(), 2);
        assert!(venue.orders_open(&[]).is_empty());
    }

    /// A tick that both reaches a deadline and crosses the order retires it rather than filling it.
    ///
    /// The deadline is an unconditional cutoff: whether an order trades at the very instant it
    /// expires must not depend on whether the market happened to cross there.
    #[test]
    fn a_deadline_is_swept_before_the_tick_that_would_have_filled_it() {
        let mut venue = make_market_venue("10", "1000000");
        advance(&mut venue, time(1));

        let _ = venue.open_order(limit_request(
            "gtd",
            Side::Buy,
            "1",
            "48000",
            TimeInForce::GoodTillDate { expiry: time(5) },
        ));

        // An ask of 47000 crosses a limit of 48000, so this tick would have filled the order — but
        // it also reaches the deadline.
        let events = venue.apply_market(
            &instrument_name(),
            book("46900", "47000"),
            MarketDepth::UNKNOWN,
            time(5),
        );

        assert_eq!(events.len(), 2, "a release and an expiry, not a fill");
        assert!(
            venue.trades(time(0)).is_empty(),
            "an expired order must not trade on the tick that retired it"
        );
        assert!(matches!(
            order_of(&events[1]).state,
            OrderState::Inactive(InactiveOrderState::Expired(_))
        ));
    }

    /// An order whose deadline has already passed on arrival never reaches the book.
    ///
    /// With no independent clock, an order accepted past its own deadline would work until
    /// something else happened to touch the venue — and if nothing did, for the rest of the run.
    #[test]
    fn a_good_till_date_order_that_has_already_expired_is_never_rested() {
        let mut venue = make_market_venue("10", "1000000");
        advance(&mut venue, time(9));

        let outcome = venue.open_order(limit_request(
            "stale",
            Side::Buy,
            "1",
            "48000",
            TimeInForce::GoodTillDate { expiry: time(5) },
        ));

        assert!(outcome.events.is_empty(), "nothing was reserved");
        assert!(venue.orders_open(&[]).is_empty(), "it must not rest");
        match outcome.response.state {
            OrderState::Inactive(InactiveOrderState::Expired(ref expired)) => {
                assert_eq!(expired.time_exchange, time(9), "stamped when it was found");
                assert_eq!(expired.filled_quantity, Decimal::ZERO);
            }
            ref other => panic!("a stale deadline must expire, not {other:?}"),
        }
    }

    /// A stale deadline retires the order even when the book would have filled it.
    ///
    /// The same unconditional cutoff as on the tick path: an order whose deadline has elapsed has
    /// stopped working, so whether it *could* have traded is not a question the venue asks. The
    /// alternative would execute an order its sender had already asked to be finished with, and
    /// make the outcome depend on where the book happened to be.
    #[test]
    fn a_stale_deadline_retires_an_order_the_book_would_have_filled() {
        let mut venue = make_market_venue("10", "1000000");
        advance(&mut venue, time(9));
        assert!(
            venue
                .apply_market(
                    &instrument_name(),
                    traded("47000"),
                    MarketDepth::UNKNOWN,
                    time(9)
                )
                .is_empty()
        );

        let outcome = venue.open_order(limit_request(
            "stale",
            Side::Buy,
            "1",
            "48000",
            TimeInForce::GoodTillDate { expiry: time(5) },
        ));

        assert!(
            matches!(
                outcome.response.state,
                OrderState::Inactive(InactiveOrderState::Expired(_))
            ),
            "a marketable order past its deadline must expire, not fill: {:?}",
            outcome.response.state
        );
        assert!(
            venue.trades(time(0)).is_empty(),
            "it must not trade on its way out"
        );
    }

    /// A cancel arriving after its order's deadline is told *why* it found nothing.
    #[test]
    fn a_cancel_after_a_deadline_is_answered_already_expired() {
        let mut venue = make_market_venue("10", "1000000");
        advance(&mut venue, time(1));

        let _ = venue.open_order(limit_request(
            "gtd",
            Side::Buy,
            "1",
            "48000",
            TimeInForce::GoodTillDate { expiry: time(5) },
        ));
        assert_eq!(venue.advance_time(time(6)).len(), 2);

        let outcome = venue.cancel_order(OrderEvent {
            key: OrderKey {
                exchange: EXCHANGE,
                instrument: instrument_name(),
                strategy: StrategyId::new("test"),
                cid: ClientOrderId::new("gtd"),
            },
            state: RequestCancel { id: None },
        });

        assert_eq!(
            outcome.response.state,
            Err(OrderError::Rejected(ApiError::OrderAlreadyExpired)),
            "a cancel that lost to a deadline must not read as an unknown order"
        );
    }

    /// An expired order is still part of the account, and a later snapshot reports it.
    #[test]
    fn an_expired_order_enters_the_account_snapshot() {
        let mut venue = make_market_venue("10", "1000000");
        advance(&mut venue, time(1));

        let _ = venue.open_order(limit_request(
            "gtd",
            Side::Buy,
            "1",
            "48000",
            TimeInForce::GoodTillDate { expiry: time(5) },
        ));
        assert_eq!(venue.advance_time(time(5)).len(), 2);

        let snapshot = venue.account_snapshot();
        let orders = &snapshot.instruments[0].orders;
        assert_eq!(orders.len(), 1);
        assert!(matches!(
            orders[0].state,
            OrderState::Inactive(InactiveOrderState::Expired(_))
        ));
    }

    /// Deadlines are swept in deadline order, so a run's events do not depend on map iteration.
    #[test]
    fn deadlines_are_swept_earliest_first() {
        let mut venue = make_market_venue("10", "1000000");
        advance(&mut venue, time(1));

        // Booked latest-deadline first, so insertion order is the opposite of sweep order.
        for (cid, expiry) in [("late", time(7)), ("early", time(5)), ("middle", time(6))] {
            let _ = venue.open_order(limit_request(
                cid,
                Side::Buy,
                "1",
                "48000",
                TimeInForce::GoodTillDate { expiry },
            ));
        }

        let events = venue.advance_time(time(9));

        // Each retirement is a release then a snapshot, so the orders are at the odd indices.
        let swept: Vec<_> = events
            .iter()
            .skip(1)
            .step_by(2)
            .map(|event| order_of(event).key.cid.0.to_string())
            .collect();

        assert_eq!(swept, ["early", "middle", "late"]);
    }

    /// A deadline is a property of the clock, not of a book: an order on an instrument that never
    /// ticks again is still retired by activity elsewhere on the venue.
    #[test]
    fn a_deadline_on_a_quiet_instrument_is_swept_by_a_tick_on_another() {
        let mut venue = make_market_venue("10", "1000000");
        advance(&mut venue, time(1));

        let _ = venue.open_order(limit_request(
            "gtd",
            Side::Buy,
            "1",
            "48000",
            TimeInForce::GoodTillDate { expiry: time(5) },
        ));

        // A market for an instrument this venue holds no order on.
        let elsewhere = InstrumentNameExchange::new("eth_usdt");
        let events = venue.apply_market(
            &elsewhere,
            book("2000", "2001"),
            MarketDepth::UNKNOWN,
            time(6),
        );

        assert_eq!(
            events.len(),
            2,
            "the quiet instrument's order still retires"
        );
        assert!(venue.orders_open(&[]).is_empty());
    }

    /// An order the venue took nothing for releases nothing, exactly as cancelling one does.
    #[test]
    fn expiring_an_order_seeded_with_no_reservation_restates_no_balance() {
        let expiry = time(5);
        let seeded = Order {
            key: OrderKey {
                exchange: EXCHANGE,
                instrument: instrument_name(),
                strategy: StrategyId::new("test"),
                cid: ClientOrderId::new("seeded"),
            },
            side: Side::Buy,
            price: Some(d("48000")),
            quantity: d("1"),
            kind: OrderKind::Limit,
            time_in_force: TimeInForce::GoodTillDate { expiry },
            state: OrderState::active(Open::new(OrderId::new("seeded"), time(1), Decimal::ZERO)),
        };

        let mut venue = make_market_venue("10", "1000000");
        venue.account.orders_mut().insert(
            as_open(seeded).expect("the seeded order is Open"),
            // As `initial_state` seeds one: the venue never took anything for it.
            None,
        );

        let events = venue.advance_time(expiry);

        assert_eq!(events.len(), 1, "the terminal snapshot alone, no release");
        assert!(matches!(
            order_of(&events[0]).state,
            OrderState::Inactive(InactiveOrderState::Expired(_))
        ));
    }

    // --- Stage 4: market orders are priced from the book they arrive to ---------------------

    /// A market-driven venue prices a market order from its own book, ignoring the requester's.
    ///
    /// The request carries a deliberately different snapshot, so a fill at the venue's price can
    /// only have come from the venue's own market — the two are far enough apart that no fill
    /// model or rounding could confuse them.
    #[test]
    fn a_market_driven_venue_prices_a_market_order_from_its_own_book() {
        let mut venue = make_market_venue("10", "1000000");
        advance(&mut venue, time(1));
        assert!(
            venue
                .apply_market(
                    &instrument_name(),
                    traded("49000"),
                    MarketDepth::UNKNOWN,
                    time(1)
                )
                .is_empty()
        );

        // What the sender was looking at when it decided, which is not what the venue holds.
        let outcome = venue.open_order(buy_request("1.0", Some(traded("40000"))));

        assert_eq!(
            filled_price(&outcome.response),
            d("49000"),
            "the fill is struck against the venue's book, not the snapshot the request carried"
        );
    }

    /// The same request on a request-priced venue still fills at the snapshot it carried.
    ///
    /// The regime is the whole of the difference, which is why `MockExchange`'s results do not
    /// move: it drives a `RequestPriced` venue and reads `market()` as `None` forever.
    #[test]
    fn a_request_priced_venue_still_prices_a_market_order_from_the_request() {
        let mut venue = make_venue("10", "1000000");

        let outcome = venue.open_order(buy_request("1.0", Some(traded("40000"))));

        assert_eq!(
            filled_price(&outcome.response),
            d("40000"),
            "with no book of its own, the request's snapshot is still the only price source"
        );
    }

    /// A market-driven venue that has never been fed an instrument rejects a market order in it
    /// rather than reaching for the requester's snapshot.
    ///
    /// Falling back would make the fill depend on which of the two happened to hold a price, and
    /// would re-admit the request-priced behaviour the regime exists to distinguish. The request
    /// here carries a perfectly good snapshot, and is refused anyway.
    #[test]
    fn a_market_driven_venue_with_no_book_rejects_a_market_order_rather_than_using_the_request() {
        let mut venue = make_market_venue("10", "1000000");
        advance(&mut venue, time(1));

        let outcome = venue.open_order(buy_request("1.0", Some(traded("49000"))));

        assert!(outcome.events.is_empty(), "nothing was debited or traded");
        match outcome.response.state {
            OrderState::Inactive(InactiveOrderState::OpenFailed(
                UnindexedOrderError::Rejected(ApiError::OrderRejected(ref reason)),
            )) => assert!(
                reason.contains("no market snapshot"),
                "the rejection must name the venue's absent market, got: {reason}"
            ),
            ref other => panic!("an unpriceable market order must be rejected, got: {other:?}"),
        }
    }

    // ---------------------------------------------------------------------------------------
    // A taker takes only what is on offer.
    // ---------------------------------------------------------------------------------------

    /// The same size on both sides, so a test need only say how much.
    fn sized(amount: &str) -> MarketDepth {
        MarketDepth::new(Some(d(amount)), Some(d(amount)))
    }

    /// A venue fed one book carrying `depth`, priced off that book rather than off a last trade.
    ///
    /// The offer is 47900 throughout, so a buy limited at 48000 is marketable and prints at 47900.
    fn venue_offering(depth: MarketDepth) -> SimulatedVenue {
        venue_offering_funded("1000000", depth)
    }

    fn venue_offering_funded(usdt: &str, depth: MarketDepth) -> SimulatedVenue {
        let mut venue = make_market_venue_with(
            "10",
            usdt,
            maker_taker_fee(),
            SimFillConfig::BidAsk(BidAskFillModel),
        );
        advance(&mut venue, time(1));
        assert!(
            venue
                .apply_market(&instrument_name(), book("47800", "47900"), depth, time(1))
                .is_empty(),
            "an empty book fills nothing"
        );
        venue
    }

    fn free_quote(venue: &SimulatedVenue) -> Decimal {
        venue.balances(&[quote()])[0].balance.free
    }

    fn total_quote(venue: &SimulatedVenue) -> Decimal {
        venue.balances(&[quote()])[0].balance.total
    }

    /// A market order has no limit price, so what the book could not fill has nowhere to wait.
    #[test]
    fn a_market_order_fills_what_the_book_has_and_cancels_what_it_has_not() {
        let mut venue = venue_offering(sized("0.4"));

        let outcome = venue.open_order(buy_request("1", None));

        assert_eq!(outcome.events.len(), 2, "a fill owes balance, then trade");
        let trade = trade_of(&outcome.events[1]);
        assert_eq!(
            trade.quantity,
            d("0.4"),
            "it took the whole offer and no more"
        );
        assert_eq!(trade.price, d("47900"));

        match &outcome.response.state {
            OrderState::Inactive(InactiveOrderState::Cancelled(cancelled)) => assert_eq!(
                cancelled.filled_quantity,
                d("0.4"),
                "the remainder retires carrying what did trade"
            ),
            other => panic!("a market order's unfillable remainder is cancelled, got: {other:?}"),
        }
        assert!(
            venue.orders_open(&[]).is_empty(),
            "a market order has no price to rest the remainder at"
        );
    }

    /// The condition under which the two coincided has expired, which is this test.
    #[test]
    fn fill_or_kill_refuses_the_partial_fill_immediate_or_cancel_accepts() {
        // One book, one size, one order -- only the time in force differs.
        let mut immediate = venue_offering(sized("0.4"));
        let mut all_or_nothing = venue_offering(sized("0.4"));

        let ioc = immediate.open_order(limit_request(
            "ioc",
            Side::Buy,
            "1",
            "48000",
            TimeInForce::ImmediateOrCancel,
        ));
        let fok = all_or_nothing.open_order(limit_request(
            "fok",
            Side::Buy,
            "1",
            "48000",
            TimeInForce::FillOrKill,
        ));

        assert_eq!(
            trade_of(&ioc.events[1]).quantity,
            d("0.4"),
            "immediate-or-cancel keeps what the book could give it"
        );

        assert!(
            fok.events.is_empty(),
            "fill-or-kill would rather trade nothing than trade in part"
        );
        assert!(
            all_or_nothing.trades(time(0)).is_empty(),
            "and nothing reached the ledger either"
        );
        assert_eq!(
            free_quote(&all_or_nothing),
            d("1000000"),
            "a killed order moves no balance"
        );
    }

    /// One arrival, one balance -- whatever it did with the part the book could not fill.
    #[test]
    fn a_capped_limit_order_fills_what_it_can_and_rests_the_remainder() {
        let mut venue = venue_offering(sized("0.4"));

        let outcome = venue.open_order(limit_request("split", Side::Buy, "1", "48000", gtc()));

        assert_eq!(
            outcome.events.len(),
            2,
            "one arrival restates one balance and prints one trade, even settling and holding at \
             once"
        );
        let trade = trade_of(&outcome.events[1]);
        assert_eq!(trade.quantity, d("0.4"));
        assert_eq!(
            trade.price,
            d("47900"),
            "the part that traded took the offer"
        );

        let open = rested_open(&outcome);
        assert_eq!(
            open.filled_quantity,
            d("0.4"),
            "the remainder rests carrying what has already traded"
        );
        assert_eq!(open.quantity_remaining(d("1")), d("0.6"));
        assert_eq!(venue.orders_open(&[]).len(), 1);

        // Settled the taker leg, still holding the maker one: 19179.16 spent, 28805.76 held.
        assert_eq!(total_quote(&venue), d("1000000") - d("19179.16"));
        assert_eq!(
            free_quote(&venue),
            d("1000000") - d("19179.16") - d("28805.76")
        );
    }

    /// An order that fills in part, rests, and is later crossed settles exactly the remainder --
    /// twice in total, summing to the original, with no third restatement.
    #[test]
    fn an_order_filled_in_part_then_crossed_settles_the_remainder_and_no_more() {
        let mut venue = venue_offering(sized("0.4"));

        let arrival = venue.open_order(limit_request("split", Side::Buy, "1", "48000", gtc()));
        let taker = trade_of(&arrival.events[1]).clone();

        // The market reaches what is resting.
        let fills = venue.apply_market(
            &instrument_name(),
            book("48000", "48000"),
            MarketDepth::UNKNOWN,
            time(2),
        );
        assert_eq!(fills.len(), 3, "a resting fill owes balance, trade, order");
        let maker = trade_of(&fills[1]);

        assert_eq!(maker.quantity, d("0.6"), "only the remainder crosses");
        assert_eq!(
            maker.price,
            d("48000"),
            "a maker is paid the price it quoted, not the one the book moved to"
        );
        assert_eq!(
            taker.quantity + maker.quantity,
            d("1"),
            "the two fills are the whole order, and no more than it"
        );

        assert_ne!(
            taker.id, maker.id,
            "one order printing twice must print two distinguishable trades"
        );
        assert_eq!(
            taker.order_id, maker.order_id,
            "and both must still say which order they belong to"
        );

        // 0.4 taken at 47900 plus 10bp, then 0.6 quoted at 48000 plus 2bp. Nothing else moved.
        let spent = d("19179.16") + d("28805.76");
        assert_eq!(total_quote(&venue), d("1000000") - spent);
        assert_eq!(
            free_quote(&venue),
            d("1000000") - spent,
            "the order is done, so nothing is still held against it"
        );
    }

    /// Reading a stored size does not consume it -- only filling against it does.
    #[test]
    fn a_second_order_on_one_observation_sees_only_what_the_first_left() {
        let mut venue = venue_offering(sized("1"));

        let first = venue.open_order(limit_request(
            "first",
            Side::Buy,
            "0.6",
            "48000",
            TimeInForce::ImmediateOrCancel,
        ));
        assert_eq!(trade_of(&first.events[1]).quantity, d("0.6"));

        let second = venue.open_order(limit_request(
            "second",
            Side::Buy,
            "0.6",
            "48000",
            TimeInForce::ImmediateOrCancel,
        ));
        assert_eq!(
            trade_of(&second.events[1]).quantity,
            d("0.4"),
            "the offer was displayed once, so the two of them share it"
        );

        let third = venue.open_order(limit_request(
            "third",
            Side::Buy,
            "0.6",
            "48000",
            TimeInForce::ImmediateOrCancel,
        ));
        assert!(
            third.events.is_empty(),
            "the offer is exhausted until the next observation replaces it"
        );
    }

    /// A new observation states what is on offer now; it is not an increment on what was.
    #[test]
    fn a_new_observation_replaces_the_remaining_size_rather_than_adding_to_it() {
        let mut venue = venue_offering(sized("1"));

        let taken = venue.open_order(limit_request(
            "first",
            Side::Buy,
            "0.6",
            "48000",
            TimeInForce::ImmediateOrCancel,
        ));
        assert_eq!(trade_of(&taken.events[1]).quantity, d("0.6"));

        assert!(
            venue
                .apply_market(
                    &instrument_name(),
                    book("47800", "47900"),
                    sized("1"),
                    time(2)
                )
                .is_empty()
        );

        let after = venue.open_order(limit_request(
            "second",
            Side::Buy,
            "1",
            "48000",
            TimeInForce::ImmediateOrCancel,
        ));
        assert_eq!(
            trade_of(&after.events[1]).quantity,
            d("1"),
            "the fresh observation offers a whole unit again"
        );
    }

    /// No size information is not no size. A price-only feed must keep filling exactly as it did.
    #[test]
    fn a_feed_supplying_no_size_caps_nothing() {
        let mut venue = venue_offering(MarketDepth::UNKNOWN);

        let outcome = venue.open_order(limit_request(
            "uncapped",
            Side::Buy,
            "5",
            "48000",
            TimeInForce::ImmediateOrCancel,
        ));

        assert_eq!(
            trade_of(&outcome.events[1]).quantity,
            d("5"),
            "an absent size caps nothing, however large the order"
        );
    }

    /// A reported *absence* of size is the other thing, and it does stop a fill.
    #[test]
    fn a_marketable_order_with_nothing_on_offer_rests_rather_than_filling() {
        let mut venue = venue_offering(MarketDepth::new(Some(d("1")), Some(Decimal::ZERO)));

        let outcome = venue.open_order(limit_request("nothing", Side::Buy, "1", "48000", gtc()));

        assert_eq!(
            outcome.events.len(),
            1,
            "nothing traded, so it owes only the hold it took"
        );
        assert_eq!(rested_open(&outcome).filled_quantity, Decimal::ZERO);
    }

    /// A capped order costs **more** than the same order filling outright: its remainder is held at
    /// the order's own limit while the fill struck a better price. So an account can afford the
    /// whole order and still not afford the split -- and it is refused whole rather than filled for
    /// the part it could pay for, which would be this venue choosing a disposition nobody asked for.
    #[test]
    fn an_order_that_cannot_fund_both_legs_is_refused_with_nothing_moved() {
        // 47947.90 buys the whole of it at the offer; the split wants 47984.92.
        const FUNDED: &str = "47950";

        let mut uncapped = venue_offering_funded(FUNDED, MarketDepth::UNKNOWN);
        let affordable =
            uncapped.open_order(limit_request("whole", Side::Buy, "1", "48000", gtc()));
        assert_eq!(
            trade_of(&affordable.events[1]).quantity,
            d("1"),
            "uncapped, this very order is affordable and fills outright"
        );

        let mut venue = venue_offering_funded(FUNDED, sized("0.4"));
        let outcome = venue.open_order(limit_request("split", Side::Buy, "1", "48000", gtc()));

        assert!(outcome.events.is_empty(), "a refused order moves nothing");
        assert!(
            matches!(
                outcome.response.state,
                OrderState::Inactive(InactiveOrderState::OpenFailed(_))
            ),
            "and says so, rather than filling the part it could afford: {:?}",
            outcome.response.state
        );
        assert_eq!(free_quote(&venue), d(FUNDED), "the ledger is as it was");
        assert_eq!(total_quote(&venue), d(FUNDED));
        assert!(venue.trades(time(0)).is_empty(), "and nothing traded");
        assert!(venue.orders_open(&[]).is_empty());
    }

    /// Re-opening a resting `ClientOrderId` replaces the order under it, and what was held against
    /// the one displaced is given back rather than left held against nothing.
    #[test]
    fn replacing_a_resting_order_releases_what_was_held_against_it() {
        let (mut venue, first) = venue_resting_one_buy("48000");
        assert_eq!(
            free_quote(&venue),
            d("1000000") - d("48009.6"),
            "the first order holds its whole notional plus the maker fee"
        );

        // Same cid, half the size.
        let second = venue.open_order(limit_request("resting", Side::Buy, "0.5", "48000", gtc()));

        assert_eq!(
            venue.orders_open(&[]).len(),
            1,
            "one cid, one resting order"
        );
        assert_eq!(
            free_quote(&venue),
            d("1000000") - d("24004.8"),
            "only the replacement's own reservation is still held"
        );
        assert_eq!(
            balance_of(&second.events[0]).balance.free,
            free_quote(&venue),
            "and the balance it restated is the one that is true afterwards"
        );
        assert_eq!(
            total_quote(&venue),
            d("1000000"),
            "nothing settled: a replaced order traded nothing"
        );
        drop(first);
    }
}
