use crate::{
    UnindexedAccountSnapshot,
    balance::{AssetBalance, Balance, MarginDetails},
    exchange::mock::orders::{
        AlreadyResting, OpenOrder, OpenOrders, Reservation, RestingOrder, as_open,
    },
    margin::MarginSideEffect,
    order::{
        Order, UnindexedInactiveOrder,
        id::ClientOrderId,
        state::{Cancelled, Expired, Filled, InactiveOrderState, Open, OrderState},
    },
    trade::Trade,
};
use chrono::{DateTime, Utc};
use fnv::FnvHashMap;
use rust_decimal::Decimal;
use rustrade_instrument::{
    asset::name::AssetNameExchange, exchange::ExchangeId, instrument::name::InstrumentNameExchange,
};
use thiserror::Error;

#[derive(Debug)]
pub struct AccountState {
    balances: FnvHashMap<AssetNameExchange, AssetBalance<AssetNameExchange>>,
    orders_open: OpenOrders,
    orders_cancelled:
        FnvHashMap<ClientOrderId, Order<ExchangeId, InstrumentNameExchange, Cancelled>>,
    /// Orders that filled, kept so a cancel arriving after one can say *why* it failed, and so an
    /// order-state lookup can report how it ended.
    ///
    /// Whole orders, like `orders_cancelled`: the fill's total and average price are not
    /// recoverable from [`trades`](Self::trades) alone, which records each cross separately. Grows
    /// with the run, like `trades` and `orders_cancelled` — a simulated venue's ledgers are bounded
    /// by the dataset, not reaped.
    orders_filled: FnvHashMap<ClientOrderId, Order<ExchangeId, InstrumentNameExchange, Filled>>,
    /// Orders retired by their own deadline, kept so they reach a later account snapshot and so a
    /// cancel arriving after one can say *why* it failed.
    ///
    /// Whole orders rather than ids alone, unlike [`orders_filled`](Self::orders_filled): nothing
    /// else records an expiry, so a snapshot has no other source for it — whereas a fill is already
    /// in [`trades`](Self::trades). Grows with the run, like `trades` and `orders_cancelled`.
    orders_expired: FnvHashMap<ClientOrderId, Order<ExchangeId, InstrumentNameExchange, Expired>>,
    trades: Vec<Trade<AssetNameExchange, InstrumentNameExchange>>,
    /// The policy of a margin account, or `None` for a cash account, which reports no loans and
    /// borrows nothing. Set by [`enable_margin`](Self::enable_margin).
    margin: Option<MarginSideEffect>,
}

impl AccountState {
    pub fn new(
        balances: FnvHashMap<AssetNameExchange, AssetBalance<AssetNameExchange>>,
        orders_open: OpenOrders,
        orders_cancelled: FnvHashMap<
            ClientOrderId,
            Order<ExchangeId, InstrumentNameExchange, Cancelled>,
        >,
        trades: Vec<Trade<AssetNameExchange, InstrumentNameExchange>>,
    ) -> Self {
        Self {
            balances,
            orders_open,
            orders_cancelled,
            orders_filled: FnvHashMap::default(),
            orders_expired: FnvHashMap::default(),
            trades,
            margin: None,
        }
    }

    /// Makes this a margin account applying `side_effect`: every balance reports its loan, and
    /// under [`MarginSideEffect::AutoBorrowRepay`] the account borrows what an arrival lacks and
    /// repays its loans from what it receives — see
    /// [`SpotMarginConfig`](crate::margin::SpotMarginConfig).
    ///
    /// A balance that already reports a loan, as one seeded from a live margin account does, keeps
    /// it; every other balance starts owing nothing.
    pub fn enable_margin(&mut self, side_effect: MarginSideEffect) {
        self.margin = Some(side_effect);
        for balance in self.balances.values_mut() {
            balance
                .balance
                .margin
                .get_or_insert_with(MarginDetails::default);
        }
    }

    /// Whether this account borrows what an arrival lacks and repays loans from what it receives.
    pub fn lends(&self) -> bool {
        self.margin == Some(MarginSideEffect::AutoBorrowRepay)
    }

    /// The free balance of `asset`, zero for an asset the account has never held.
    pub fn free(&self, asset: &AssetNameExchange) -> Decimal {
        self.balances
            .get(asset)
            .map_or(Decimal::ZERO, |balance| balance.balance.free)
    }

    /// This account's open orders, indexed for lookup and ordered for matching.
    pub fn orders(&self) -> &OpenOrders {
        &self.orders_open
    }

    /// Takes the order resting under `cid` off the book, with whatever is held against it.
    ///
    /// The caller records how it ended, with [`ack_cancelled`](Self::ack_cancelled),
    /// [`ack_filled`](Self::ack_filled) or [`ack_expired`](Self::ack_expired). Booking is only
    /// through [`book`](Self::book), which keeps an id in at most one place.
    pub fn remove_order(&mut self, cid: &ClientOrderId) -> Option<RestingOrder> {
        self.orders_open.remove(cid)
    }

    /// Puts `order` on the book holding `reservation`, unless an order is already resting under
    /// its [`ClientOrderId`].
    ///
    /// A client order id names one order at a time, not one for good: once an order has ended its
    /// id may name a new one. So booking forgets how an earlier order under the same id ended,
    /// which keeps an id in at most one place, the book or one ledger, and a snapshot from listing
    /// it twice.
    ///
    /// # Errors
    /// [`AlreadyResting`], with `order` and `reservation` handed back and nothing changed, if an
    /// order is already resting under its id. See [`OpenOrders::insert`].
    pub fn book(
        &mut self,
        order: OpenOrder,
        reservation: Option<Reservation>,
    ) -> Result<(), AlreadyResting> {
        let cid = order.key.cid.clone();
        self.orders_open.insert(order, reservation)?;
        self.forget_ended(&cid);
        Ok(())
    }

    /// Holds `order`, a market order, for its instrument's next bar with `reservation` held
    /// against it, unless an order is already resting under its [`ClientOrderId`]. Forgets how an
    /// earlier order under the id ended, as [`book`](Self::book) does.
    ///
    /// # Errors
    /// [`AlreadyResting`], as for [`book`](Self::book). See [`OpenOrders::hold`].
    pub fn hold(
        &mut self,
        order: OpenOrder,
        reservation: Option<Reservation>,
    ) -> Result<(), AlreadyResting> {
        let cid = order.key.cid.clone();
        self.orders_open.hold(order, reservation)?;
        self.forget_ended(&cid);
        Ok(())
    }

    /// The orders held for `instrument`'s next bar, earliest arrival first, no longer held. See
    /// [`OpenOrders::take_held`].
    pub fn take_held(&mut self, instrument: &InstrumentNameExchange) -> Vec<ClientOrderId> {
        self.orders_open.take_held(instrument)
    }

    /// Drops what the ledgers remember of how an order under `cid` ended, so that the order about
    /// to be recorded under it is the only one they hold.
    fn forget_ended(&mut self, cid: &ClientOrderId) {
        self.orders_cancelled.remove(cid);
        self.orders_filled.remove(cid);
        self.orders_expired.remove(cid);
    }

    /// Records that `order` was cancelled, so it is reported by a later account snapshot and a
    /// second cancel for it can be told apart from a cancel for an order that never existed.
    ///
    /// Replaces whatever the ledgers held for an earlier order under the same id.
    pub fn ack_cancelled(&mut self, order: Order<ExchangeId, InstrumentNameExchange, Cancelled>) {
        self.forget_ended(&order.key.cid);
        self.orders_cancelled.insert(order.key.cid.clone(), order);
    }

    /// Whether `cid` names an order this account cancelled.
    pub fn is_cancelled(&self, cid: &ClientOrderId) -> bool {
        self.orders_cancelled.contains_key(cid)
    }

    /// Records that `order` filled, so a cancel that loses the race to it can say so and an
    /// order-state lookup can report it.
    ///
    /// Replaces whatever the ledgers held for an earlier order under the same id.
    pub fn ack_filled(&mut self, order: Order<ExchangeId, InstrumentNameExchange, Filled>) {
        self.forget_ended(&order.key.cid);
        self.orders_filled.insert(order.key.cid.clone(), order);
    }

    /// Whether `cid` names an order this account filled.
    pub fn is_filled(&self, cid: &ClientOrderId) -> bool {
        self.orders_filled.contains_key(cid)
    }

    /// How the order `cid` ended, or `None` if it is still open or this account never held it.
    ///
    /// An order a configured `initial_state` reported as cancelled is found too; one it reported
    /// in any other inactive state is not, because a snapshot seeds only open and cancelled orders.
    ///
    /// An id names at most one order at a time, and an id is in at most one place: the book, or
    /// one of the ledgers an order can end in, holding the latest order under it (see
    /// [`book`](Self::book)). So the answer is how the latest order under `cid` ended.
    pub fn order_ended(&self, cid: &ClientOrderId) -> Option<UnindexedInactiveOrder> {
        if self.orders_open.contains(cid) {
            None
        } else if let Some(filled) = self.orders_filled.get(cid) {
            Some(filled.clone().map_state(InactiveOrderState::FullyFilled))
        } else if let Some(cancelled) = self.orders_cancelled.get(cid) {
            Some(cancelled.clone().map_state(InactiveOrderState::Cancelled))
        } else {
            self.orders_expired
                .get(cid)
                .map(|expired| expired.clone().map_state(InactiveOrderState::Expired))
        }
    }

    /// Records that `order` reached its own deadline, so it is reported by a later account snapshot
    /// and a cancel arriving after it can be told apart from a cancel for an unknown order.
    ///
    /// Replaces whatever the ledgers held for an earlier order under the same id.
    pub fn ack_expired(&mut self, order: Order<ExchangeId, InstrumentNameExchange, Expired>) {
        self.forget_ended(&order.key.cid);
        self.orders_expired.insert(order.key.cid.clone(), order);
    }

    /// Whether `cid` names an order that reached its deadline while this account held it.
    pub fn is_expired(&self, cid: &ClientOrderId) -> bool {
        self.orders_expired.contains_key(cid)
    }

    /// Restates every balance as of `time_exchange`.
    ///
    /// # Open orders keep their own stamps
    /// [`Open::time_exchange`](crate::order::state::Open::time_exchange) is the instant the venue
    /// accepted the order, and an order does not become a different order because time passed. This
    /// used to rewrite it on every advance, which was invisible only because nothing rested: the
    /// venue filled every order on arrival, so `orders_open` held at most what an `initial_state`
    /// seeded.
    ///
    /// It is not invisible once orders rest. Arrival order is half of price-time priority, so
    /// rewriting it would make a matching engine's tie-break depend on when the clock last moved
    /// rather than on when each order arrived — and since every order would be rewritten to the
    /// same instant, there would be no tie-break left at all. It would also make every open order
    /// look newer than the engine's copy on each advance, which is exactly what the `rustrade`
    /// engine's `OrderManager` recency guard reads to decide whether an update is stale.
    pub fn update_time_exchange(&mut self, time_exchange: DateTime<Utc>) {
        for balance in self.balances.values_mut() {
            balance.time_exchange = time_exchange;
        }
    }

    pub fn balances(&self) -> impl Iterator<Item = &AssetBalance<AssetNameExchange>> + '_ {
        self.balances.values()
    }

    /// Every open order, in no particular order — see [`OpenOrders::resting`] for the order a
    /// venue matches in.
    pub fn orders_open(
        &self,
    ) -> impl Iterator<Item = &Order<ExchangeId, InstrumentNameExchange, Open>> + '_ {
        self.orders_open.iter()
    }

    pub fn orders_cancelled(
        &self,
    ) -> impl Iterator<Item = &Order<ExchangeId, InstrumentNameExchange, Cancelled>> + '_ {
        self.orders_cancelled.values()
    }

    pub fn orders_expired(
        &self,
    ) -> impl Iterator<Item = &Order<ExchangeId, InstrumentNameExchange, Expired>> + '_ {
        self.orders_expired.values()
    }

    pub fn trades(
        &self,
        time_since: DateTime<Utc>,
    ) -> impl Iterator<Item = &Trade<AssetNameExchange, InstrumentNameExchange>> + '_ {
        self.trades
            .iter()
            .filter(move |trade| trade.time_exchange >= time_since)
    }

    pub fn balance_mut(
        &mut self,
        asset: &AssetNameExchange,
    ) -> Option<&mut AssetBalance<AssetNameExchange>> {
        self.balances.get_mut(asset)
    }

    pub fn ack_trade(&mut self, trade: Trade<AssetNameExchange, InstrumentNameExchange>) {
        self.trades.push(trade);
    }

    /// Holds `amount` of `asset` against an order, moving it out of `free`.
    ///
    /// `total` is untouched: the asset is still held, it is merely no longer spendable. The held
    /// portion is `total - free`, which is what [`Balance::used`](crate::balance::Balance::used)
    /// reports and what a venue restates to its client.
    ///
    /// # Errors
    /// [`BalanceInsufficient`] if `free` does not cover `amount`. The ledger is left untouched, so
    /// a caller may reject the order and carry on.
    ///
    /// # Panics
    /// Panics if `asset` has no balance. The balances are the venue's own fixture, so an absent one
    /// is a mis-specified account rather than a runtime condition — see [`SimulatedVenue`]'s
    /// caller obligations.
    ///
    /// [`SimulatedVenue`]: crate::exchange::mock::SimulatedVenue
    pub fn reserve(
        &mut self,
        asset: &AssetNameExchange,
        amount: Decimal,
        time_exchange: DateTime<Utc>,
    ) -> Result<AssetBalance<AssetNameExchange>, BalanceInsufficient> {
        let balance = self.balance_expect(asset);

        if balance.balance.free < amount {
            return Err(BalanceInsufficient {
                asset: asset.clone(),
                free: balance.balance.free,
                required: amount,
                borrowable: None,
            });
        }

        balance.balance.free -= amount;
        balance.time_exchange = time_exchange;

        Ok(balance.clone())
    }

    /// Settles `amount` of `asset` already held by [`reserve`](Self::reserve) out of the account.
    ///
    /// Only `total` moves: `free` was reduced when the amount was reserved. Reserving and then
    /// settling the same amount therefore lowers both by it, which is what an order that fills on
    /// arrival does.
    ///
    /// # Panics
    /// Panics if `asset` has no balance — see [`reserve`](Self::reserve). Settling more than is
    /// held would take `total` below `free` and break the ledger's invariant, so it panics in debug
    /// builds; callers settle an amount they reserved.
    pub fn settle(
        &mut self,
        asset: &AssetNameExchange,
        amount: Decimal,
        time_exchange: DateTime<Utc>,
    ) -> AssetBalance<AssetNameExchange> {
        let balance = self.balance_expect(asset);

        debug_assert!(
            balance.balance.total - amount >= balance.balance.free,
            "settling {amount} of {asset} would take total {} below free {}: only a reserved \
             amount may be settled",
            balance.balance.total,
            balance.balance.free
        );

        balance.balance.total -= amount;
        balance.time_exchange = time_exchange;

        balance.clone()
    }

    /// Gives `amount` of `asset` back to `free`, undoing a [`reserve`](Self::reserve) that will
    /// never be settled.
    ///
    /// Only `free` moves: nothing left the account, so `total` is unchanged. This is what a
    /// cancelled, expired or rejected order does to the balance held against it.
    ///
    /// # Panics
    /// Panics if `asset` has no balance — see [`reserve`](Self::reserve). Releasing more than is
    /// held would take `free` above `total` and break the ledger's invariant, so it panics in debug
    /// builds; callers release an amount they reserved.
    pub fn release(
        &mut self,
        asset: &AssetNameExchange,
        amount: Decimal,
        time_exchange: DateTime<Utc>,
    ) -> AssetBalance<AssetNameExchange> {
        let balance = self.balance_expect(asset);

        debug_assert!(
            balance.balance.free + amount <= balance.balance.total,
            "releasing {amount} of {asset} would take free {} above total {}: only a reserved \
             amount may be released",
            balance.balance.free,
            balance.balance.total
        );

        balance.balance.free += amount;
        balance.time_exchange = time_exchange;

        balance.clone()
    }

    /// Moves `amount` of `asset` into the account, or out of it when negative, whatever `free`
    /// holds.
    ///
    /// For a charge the account cannot refuse, such as a holding cost: a venue posts carry whether
    /// or not the account can cover it. `total` and `free` move together, so what is held stays
    /// held, and a debit larger than `free` takes `free` below zero rather than being refused. The
    /// caller reports that, since only it knows what the charge was for.
    ///
    /// # Panics
    /// Panics if `asset` has no balance — see [`reserve`](Self::reserve).
    pub fn charge(
        &mut self,
        asset: &AssetNameExchange,
        amount: Decimal,
        time_exchange: DateTime<Utc>,
    ) -> AssetBalance<AssetNameExchange> {
        let balance = self.balance_expect(asset);

        balance.balance.total += amount;
        balance.balance.free += amount;
        balance.time_exchange = time_exchange;

        balance.clone()
    }

    /// Moves a [`Delivery`] into the account, or out of it when negative, creating the balance at
    /// zero if the account has never held its asset.
    ///
    /// What a fill delivers: the base a spot buy receives, or the proceeds of a spot sell. A
    /// venue credits the asset an order receives whether or not the account held it before, so
    /// unlike every debit, an absent balance here is not a mis-specified fixture. `total` and
    /// `free` move together, as for [`charge`](Self::charge).
    ///
    /// **Unchecked**, like [`charge`](Self::charge): a negative delivery larger than `free` takes
    /// `free` below zero rather than being refused. [`commit`](Self::commit) is the checked path,
    /// and a fill that can be refused goes through it.
    ///
    /// On an account that [`lends`](Self::lends), what is delivered into an asset the account owes
    /// repays that loan first, interest then principal, and only the rest is added to `free`.
    pub fn deliver(
        &mut self,
        delivery: &Delivery,
        time_exchange: DateTime<Utc>,
    ) -> AssetBalance<AssetNameExchange> {
        let lends = self.lends();
        let balance = self.balance_or_open(&delivery.asset, time_exchange);

        balance.balance.total += delivery.amount;
        balance.balance.free += delivery.amount;
        if lends {
            repay(&mut balance.balance, delivery.amount);
        }
        balance.time_exchange = time_exchange;

        balance.clone()
    }

    /// Gives `reservation` back to `free`, as [`release`](Self::release) does, then repays the
    /// loan its order took when it was placed out of what came back, interest first.
    ///
    /// What a cancelled, expired or rejected order does to the balance held against it on a margin
    /// account, which repays an unused loan when the order it was taken for ends. A reservation
    /// that borrowed nothing is simply released.
    ///
    /// # Panics
    /// As [`release`](Self::release).
    pub fn release_reservation(
        &mut self,
        reservation: &Reservation,
        time_exchange: DateTime<Utc>,
    ) -> AssetBalance<AssetNameExchange> {
        let released = self.release(&reservation.asset, reservation.amount, time_exchange);
        match reservation.borrowed.is_zero() {
            true => released,
            false => self.repay_loan(&reservation.asset, reservation.borrowed, time_exchange),
        }
    }

    /// Repays up to `amount` of `asset`'s loan out of its free balance, interest first, and never
    /// more than `free` holds.
    ///
    /// # Panics
    /// Panics if `asset` has no balance — see [`reserve`](Self::reserve).
    pub fn repay_loan(
        &mut self,
        asset: &AssetNameExchange,
        amount: Decimal,
        time_exchange: DateTime<Utc>,
    ) -> AssetBalance<AssetNameExchange> {
        let balance = self.balance_expect(asset);
        let available = amount.min(balance.balance.free);
        repay(&mut balance.balance, available);
        balance.time_exchange = time_exchange;

        balance.clone()
    }

    /// Lends `amount` of `asset`: `total` and `free` rise by it, and so does the loan. An asset the
    /// account has never held is opened at zero first, as a delivery opens one.
    fn borrow(&mut self, asset: &AssetNameExchange, amount: Decimal, time_exchange: DateTime<Utc>) {
        let balance = &mut self.balance_or_open(asset, time_exchange).balance;
        balance.total += amount;
        balance.free += amount;
        balance
            .margin
            .get_or_insert_with(MarginDetails::default)
            .borrowed += amount;
    }

    /// `asset`'s balance, opened at zero if the account has never held it, reporting a loan from
    /// the start on a margin account.
    fn balance_or_open(
        &mut self,
        asset: &AssetNameExchange,
        time_exchange: DateTime<Utc>,
    ) -> &mut AssetBalance<AssetNameExchange> {
        let margin = self.margin.map(|_| MarginDetails::default());
        // `SmolStr` clones without allocating for an asset name, so the entry API costs nothing
        // on the common path where the balance exists.
        self.balances
            .entry(asset.clone())
            .or_insert_with(|| AssetBalance {
                asset: asset.clone(),
                balance: Balance {
                    margin,
                    ..Balance::new(Decimal::ZERO, Decimal::ZERO)
                },
                time_exchange,
            })
    }

    /// Commits one arriving order's whole ledger effect, or none of it.
    ///
    /// One order's arrival can both settle a fill and take a hold: a taker that the book could
    /// only partly fill settles what traded and reserves against the remainder it leaves resting.
    /// Those are one event at the venue, so they are one operation here — and because every
    /// requirement is tested before anything moves, an arrival this account cannot afford leaves
    /// the ledger exactly as it found it.
    ///
    /// # Why this is not two calls
    /// Settling first and reserving second is the same two moves in the order that can fail
    /// halfway: the settle commits, the reserve is refused, and nothing puts back what `total`
    /// has already lost — [`release`](Self::release) moves only `free`, and its own assertion
    /// fires if it is used as a rollback. The venue would then hold a balance its client never
    /// hears about, silently and for the rest of the run. Asking for the whole requirement up
    /// front is what makes that state unreachable rather than merely avoided.
    ///
    /// # One restatement per asset moved
    /// A balance is an absolute restatement rather than a delta, and a fill is atomic at the
    /// venue, so emitting the state between the settle and the hold would report a balance the
    /// account never held — for the same reason a conservative reservation that had to be released
    /// and re-debited would. [`Committed`] therefore carries one balance per asset the arrival
    /// moved: the one it paid with, and the one it received when that is another asset.
    ///
    /// # What the fill pays back
    /// `credit` is what the fill gives the account, and which [`Credit`] it is decides what it
    /// does:
    /// - **[`PaidBack`](Credit::PaidBack)**, into the paying asset — the margin and realised PnL
    ///   of the part of a CFD fill that closes a position. It counts towards what `free` must
    ///   cover, because one fill can carry both: a CFD flip, closing a long and opening a short in
    ///   one order, is paid back for the long and posts margin for the short, so the flip is
    ///   funded by the long it closes.
    /// - **[`Delivery`](Credit::Delivery)**, into another asset — what a spot fill receives: base
    ///   for a buy, the proceeds for a sell, each net of any fee charged in it. It funds nothing,
    ///   since it is not the asset paid with, and it is [delivered](Self::deliver) whether or not
    ///   the account held that asset before. It is negative only for a fill whose fee exceeds
    ///   what it receives, and then that asset's `free` must cover the excess too.
    ///
    /// Nothing is credited until every requirement is known to be covered, so a refusal still
    /// leaves the ledger exactly as it was. `None` is an arrival with nothing to pay back: an
    /// order that rests having traded nothing.
    ///
    /// # What a margin account lends
    /// On an account that [`lends`](Self::lends), a requirement `free` and the credit do not cover
    /// borrows the shortfall of the paying asset, if it is at most `borrowable`, the most the
    /// venue will lend of it now. The loan is taken with the rest of the arrival, or not at all.
    /// [`Committed::borrowed`] says how much was lent. A paying asset the account has never held
    /// has nothing free, and is opened when the loan is taken. A cash account ignores
    /// `borrowable`, and a negative one lends nothing.
    ///
    /// # Errors
    /// [`BalanceInsufficient`], naming the asset short, if `free` plus a credit into the paying
    /// asset, plus what may be borrowed, does not cover [`settled`](Debit::settled) +
    /// [`reserved`](Debit::reserved), or if a negative credit into another asset exceeds that
    /// asset's `free`. The ledger is left untouched.
    ///
    /// # Panics
    /// Panics if the paying asset has no balance and the arrival borrows none of it — see
    /// [`reserve`](Self::reserve). Debug-asserts that a [`PaidBack`](Credit::PaidBack) credit is
    /// not negative.
    pub fn commit(
        &mut self,
        debit: &Debit,
        credit: Option<&Credit>,
        borrowable: Decimal,
        time_exchange: DateTime<Utc>,
    ) -> Result<Committed, BalanceInsufficient> {
        let (funding, delivery) = match credit {
            Some(Credit::PaidBack(amount)) => {
                debug_assert!(
                    !amount.is_sign_negative(),
                    "a credit of {amount} {} is negative: a loss beyond the margin is debited",
                    debit.asset
                );
                (*amount, None)
            }
            Some(Credit::Delivery(delivery)) => (Decimal::ZERO, Some(delivery)),
            None => (Decimal::ZERO, None),
        };

        // Asked for as one requirement, so a refusal refuses the arrival rather than half of it.
        // A margin account lends the shortfall, within what the venue allows.
        // A margin account borrows even an asset it has never held, which it opens on lending.
        let required = debit.settled + debit.reserved;
        let lends = self.lends();
        let free = match lends {
            true => self.free(&debit.asset),
            false => self.balance_expect(&debit.asset).balance.free,
        };
        let shortfall = required - (free + funding);
        let borrowed = match shortfall > Decimal::ZERO {
            false => Decimal::ZERO,
            true if lends && shortfall <= borrowable => shortfall,
            true => {
                return Err(BalanceInsufficient {
                    asset: debit.asset.clone(),
                    free: free + funding,
                    required,
                    borrowable: lends.then_some(borrowable.max(Decimal::ZERO)),
                });
            }
        };

        // A delivery that takes rather than gives -- a fill whose fee exceeds what it receives --
        // must be covered before anything moves too. An asset never held has nothing free.
        if let Some(delivery) = delivery.filter(|delivery| delivery.amount.is_sign_negative()) {
            let free = self
                .balances
                .get(&delivery.asset)
                .map_or(Decimal::ZERO, |balance| balance.balance.free);
            if free + delivery.amount < Decimal::ZERO {
                return Err(BalanceInsufficient {
                    asset: delivery.asset.clone(),
                    free,
                    required: -delivery.amount,
                    borrowable: None,
                });
            }
        }

        // Applied only now that every requirement is covered, so a refusal moves nothing.
        if !borrowed.is_zero() {
            self.borrow(&debit.asset, borrowed, time_exchange);
        }
        if !funding.is_zero() {
            let balance = self.balance_expect(&debit.asset);
            balance.balance.total += funding;
            balance.balance.free += funding;
        }

        // Infallible: `free` now covers the whole requirement. `total` cannot fall below `free`,
        // which the reserve takes the whole requirement out of, and whatever was reserved and not
        // settled stays held.
        self.reserve(&debit.asset, required, time_exchange)?;
        let paid = self.settle(&debit.asset, debit.settled, time_exchange);
        let received = delivery.map(|delivery| self.deliver(delivery, time_exchange));

        Ok(Committed {
            paid,
            received,
            borrowed,
        })
    }

    #[allow(clippy::expect_used)] // Documented panic: an absent balance is a mis-specified fixture.
    fn balance_expect(
        &mut self,
        asset: &AssetNameExchange,
    ) -> &mut AssetBalance<AssetNameExchange> {
        self.balances
            .get_mut(asset)
            .expect("SimulatedVenue has Balance for all configured Instrument assets")
    }
}

/// Repays up to `available` of `balance`'s loan, interest first, then principal, out of its
/// `total` and `free`, and returns how much it repaid. Repays nothing from a `available` that is
/// not positive, or from a balance that owes nothing.
fn repay(balance: &mut Balance, available: Decimal) -> Decimal {
    let Some(margin) = balance.margin.as_mut() else {
        return Decimal::ZERO;
    };
    let available = available.max(Decimal::ZERO);
    let interest = margin.interest.min(available);
    let principal = margin.borrowed.min(available - interest);
    margin.interest -= interest;
    margin.borrowed -= principal;

    let repaid = interest + principal;
    balance.total -= repaid;
    balance.free -= repaid;
    repaid
}

/// What one arriving order does to one asset's balance, as a single commitment.
///
/// Both fields are denominated in [`asset`](Self::asset) and inclusive of fees. They are one
/// asset, not two, because they come from one order: which asset an order pays with is decided by
/// its instrument and its side, and an order has one of each. That is what makes an arrival a
/// single ledger operation rather than two that must be kept in step.
///
/// The three arrivals a [`SimulatedVenue`] can have are the three shapes of this type:
///
/// | arrival | `settled` | `reserved` |
/// |---|---|---|
/// | fills in full | the fill | zero |
/// | rests, having traded nothing | zero | the reservation |
/// | fills in part and rests the remainder | the fill | the remainder's reservation |
///
/// [`SimulatedVenue`]: crate::exchange::mock::SimulatedVenue
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Debit {
    /// Which asset the order pays with: quote for a buy or a CFD, base for a spot sell.
    pub asset: AssetNameExchange,

    /// Left the account outright, because it traded. Lowers `free` and `total` alike.
    pub settled: Decimal,

    /// Held against a remainder that is still working. Lowers `free` and leaves `total`, so it is
    /// still the account's until whatever it is held against settles or is released.
    pub reserved: Decimal,
}

/// What one fill gives the account. See [`AccountState::commit`].
///
/// Two kinds, because where a credit goes decides what it does: one paid back into the asset the
/// order paid with funds that order, and one delivered in another asset does not.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Credit {
    /// Paid back into the asset the order paid with: the margin and realised PnL of the part of a
    /// CFD fill that closes a position. Zero for a fill that closes nothing, and never negative,
    /// since a loss beyond the margin is part of the debit.
    PaidBack(Decimal),
    /// Delivered in another asset: what a spot fill receives.
    Delivery(Delivery),
}

/// What a spot fill delivers, in the asset it receives.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Delivery {
    /// The asset received: base for a buy, quote for a sell.
    pub asset: AssetNameExchange,
    /// Denominated in [`asset`](Self::asset), net of any fee charged in it. Negative only for a
    /// fill whose fee exceeds what it receives.
    pub amount: Decimal,
}

/// The balances one [`AccountState::commit`] restated, one per asset it moved.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Committed {
    /// The asset the order paid with, or held against it.
    pub paid: AssetBalance<AssetNameExchange>,
    /// The asset a spot fill delivered, which is never the one paid with. `None` for an arrival
    /// that moved one asset: a CFD fill, or an order that rests having traded nothing.
    pub received: Option<AssetBalance<AssetNameExchange>>,
    /// How much of the paying asset a margin account lent the arrival. Zero when it lent nothing.
    pub borrowed: Decimal,
}

/// A ledger operation asked for more of an asset than `free` covers.
///
/// Carries the asset and both sides of the comparison so the caller can report them without
/// re-reading the ledger it has just been refused by.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BalanceInsufficient {
    /// The asset short of what was asked for.
    pub asset: AssetNameExchange,
    /// Freely-spendable amount at the moment of refusal.
    pub free: Decimal,
    /// Amount the operation asked for.
    pub required: Decimal,
    /// How much of the asset a margin account would have lent, or `None` when the account lends
    /// nothing, as a cash account does.
    pub borrowable: Option<Decimal>,
}

/// An account snapshot lists one [`ClientOrderId`] more than once among the orders it seeds.
///
/// A snapshot seeds open and cancelled orders, and a client order id names one order at a time,
/// so it can be listed once among them. Two entries under one id would leave the venue to choose
/// which order it holds, silently.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("account snapshot lists {0} more than once among its open and cancelled orders")]
pub struct DuplicateSeededOrder(pub ClientOrderId);

/// Seeds an account from a snapshot, as a [`SimulatedVenue`] does from its `initial_state`.
///
/// # Errors
/// [`DuplicateSeededOrder`] if the snapshot lists one [`ClientOrderId`] more than once among its
/// open and cancelled orders.
///
/// [`SimulatedVenue`]: crate::exchange::mock::SimulatedVenue
impl TryFrom<UnindexedAccountSnapshot> for AccountState {
    type Error = DuplicateSeededOrder;

    fn try_from(value: UnindexedAccountSnapshot) -> Result<Self, Self::Error> {
        let UnindexedAccountSnapshot {
            exchange: _,
            balances,
            instruments,
        } = value;

        let balances = balances
            .into_iter()
            .map(|asset_balance| (asset_balance.asset.clone(), asset_balance))
            .collect();

        let mut orders_open = OpenOrders::default();
        let mut orders_cancelled = FnvHashMap::default();
        for order in instruments.into_iter().flat_map(|snapshot| snapshot.orders) {
            match &order.state {
                OrderState::Active(_) => {
                    // `as_open` yields `None` for an active order that is not yet open — an
                    // `OpenInFlight`, which has no venue-side existence to record.
                    let Some(open) = as_open(order) else {
                        continue;
                    };
                    let cid = open.key.cid.clone();
                    if orders_cancelled.contains_key(&cid) {
                        return Err(DuplicateSeededOrder(cid));
                    }
                    // Seeded, not booked here: the venue holds nothing against it.
                    orders_open
                        .insert(open, None)
                        .map_err(|_| DuplicateSeededOrder(cid))?;
                }
                OrderState::Inactive(InactiveOrderState::Cancelled(cancelled)) => {
                    let cid = order.key.cid.clone();
                    if orders_open.contains(&cid) || orders_cancelled.contains_key(&cid) {
                        return Err(DuplicateSeededOrder(cid));
                    }
                    let cancelled = cancelled.clone();
                    orders_cancelled.insert(
                        cid,
                        Order {
                            key: order.key,
                            side: order.side,
                            price: order.price,
                            quantity: order.quantity,
                            kind: order.kind,
                            time_in_force: order.time_in_force,
                            state: cancelled,
                        },
                    );
                }
                // Not seeded: see `order_ended`.
                OrderState::Inactive(_) => {}
            }
        }

        Ok(Self {
            balances,
            orders_open,
            orders_cancelled,
            orders_filled: FnvHashMap::default(),
            orders_expired: FnvHashMap::default(),
            trades: vec![],
            margin: None,
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics are the correct failure mode
mod tests {
    use super::*;
    use crate::{
        balance::Balance,
        exchange::mock::fixtures::{funded, seeded_gtd, spot_config_holding},
        order::{
            UnindexedOrder,
            id::OrderId,
            state::{Cancelled, Filled},
        },
    };
    use rust_decimal_macros::dec;

    fn usd() -> AssetNameExchange {
        AssetNameExchange::new("usd")
    }

    fn account_with_usd(amount: Decimal) -> AccountState {
        AccountState::new(
            FnvHashMap::from_iter([(usd(), funded(usd(), amount))]),
            OpenOrders::default(),
            FnvHashMap::default(),
            vec![],
        )
    }

    fn usd_balance(account: &AccountState) -> Balance {
        let Some(balance) = account.balances().find(|balance| balance.asset == usd()) else {
            panic!("the account holds usd");
        };
        balance.balance
    }

    /// A CFD flip: what closing the old position pays back funds the margin the new one posts,
    /// so a requirement above `free` alone is met, and the remainder's hold stays held.
    #[test]
    fn a_credit_counts_towards_the_requirement_it_arrives_with() {
        let mut account = account_with_usd(dec!(100));
        let debit = Debit {
            asset: usd(),
            settled: dec!(250),
            reserved: dec!(40),
        };

        let credit = Credit::PaidBack(dec!(200));

        let Ok(committed) = account.commit(
            &debit,
            Some(&credit),
            Decimal::ZERO,
            DateTime::<Utc>::MIN_UTC,
        ) else {
            panic!("100 free plus a 200 credit covers 290");
        };

        // total: 100 + 200 credited - 250 settled; free: that, less the 40 still held.
        assert_eq!(committed.paid.balance, Balance::new(dec!(50), dec!(10)));
        assert_eq!(
            committed.received, None,
            "a CFD credit restates the asset it paid with"
        );
        assert_eq!(usd_balance(&account), Balance::new(dec!(50), dec!(10)));
    }

    #[test]
    fn a_refused_commit_applies_none_of_its_credit() {
        let mut account = account_with_usd(dec!(100));
        let debit = Debit {
            asset: usd(),
            settled: dec!(250),
            reserved: dec!(60),
        };

        let credit = Credit::PaidBack(dec!(200));

        let Err(insufficient) = account.commit(
            &debit,
            Some(&credit),
            Decimal::ZERO,
            DateTime::<Utc>::MIN_UTC,
        ) else {
            panic!("100 free plus a 200 credit does not cover 310");
        };

        assert_eq!(
            insufficient,
            BalanceInsufficient {
                asset: usd(),
                free: dec!(300),
                required: dec!(310),
                borrowable: None,
            }
        );
        assert_eq!(usd_balance(&account), Balance::new(dec!(100), dec!(100)));
    }

    fn btc() -> AssetNameExchange {
        AssetNameExchange::new("btc")
    }

    fn balance_of(account: &AccountState, asset: &AssetNameExchange) -> Option<Balance> {
        account
            .balances()
            .find(|balance| &balance.asset == asset)
            .map(|balance| balance.balance)
    }

    /// A spot buy pays quote and receives base: the base funds nothing, is restated as its own
    /// balance, and is created at zero when the account never held it.
    #[test]
    fn a_credit_into_another_asset_is_delivered_and_restated_separately() {
        let mut account = account_with_usd(dec!(100));
        let debit = Debit {
            asset: usd(),
            settled: dec!(100),
            reserved: dec!(0),
        };
        let credit = Credit::Delivery(Delivery {
            asset: btc(),
            amount: dec!(2),
        });

        let Ok(committed) = account.commit(
            &debit,
            Some(&credit),
            Decimal::ZERO,
            DateTime::<Utc>::MIN_UTC,
        ) else {
            panic!("100 free covers 100");
        };

        assert_eq!(committed.paid.balance, Balance::new(dec!(0), dec!(0)));
        let Some(received) = committed.received else {
            panic!("a credit into another asset restates that asset");
        };
        assert_eq!(received.asset, btc());
        assert_eq!(received.balance, Balance::new(dec!(2), dec!(2)));
        assert_eq!(
            balance_of(&account, &btc()),
            Some(Balance::new(dec!(2), dec!(2)))
        );
    }

    /// A credit into another asset is not the asset paid with, so it cannot fund the debit.
    #[test]
    fn a_credit_into_another_asset_does_not_fund_the_debit() {
        let mut account = account_with_usd(dec!(100));
        let debit = Debit {
            asset: usd(),
            settled: dec!(101),
            reserved: dec!(0),
        };
        let credit = Credit::Delivery(Delivery {
            asset: btc(),
            amount: dec!(1000),
        });

        let Err(insufficient) = account.commit(
            &debit,
            Some(&credit),
            Decimal::ZERO,
            DateTime::<Utc>::MIN_UTC,
        ) else {
            panic!("100 free does not cover 101, whatever another asset is credited");
        };

        assert_eq!(
            insufficient,
            BalanceInsufficient {
                asset: usd(),
                free: dec!(100),
                required: dec!(101),
                borrowable: None,
            }
        );
        assert_eq!(usd_balance(&account), Balance::new(dec!(100), dec!(100)));
        assert_eq!(
            balance_of(&account, &btc()),
            None,
            "a refusal creates nothing"
        );
    }

    /// A fill whose fee exceeds what it receives takes the excess from the asset it receives, and
    /// is refused, moving nothing, when that asset cannot cover it.
    #[test]
    fn a_negative_delivery_must_be_covered_by_its_own_asset() {
        let mut account = AccountState::new(
            FnvHashMap::from_iter([
                (usd(), funded(usd(), dec!(3))),
                (btc(), funded(btc(), dec!(1))),
            ]),
            OpenOrders::default(),
            FnvHashMap::default(),
            vec![],
        );
        let debit = Debit {
            asset: btc(),
            settled: dec!(1),
            reserved: dec!(0),
        };
        let short = Credit::Delivery(Delivery {
            asset: usd(),
            amount: dec!(-4),
        });

        let Err(insufficient) = account.commit(
            &debit,
            Some(&short),
            Decimal::ZERO,
            DateTime::<Utc>::MIN_UTC,
        ) else {
            panic!("3 usd free does not cover a 4 usd excess fee");
        };
        assert_eq!(
            insufficient,
            BalanceInsufficient {
                asset: usd(),
                free: dec!(3),
                required: dec!(4),
                borrowable: None,
            }
        );
        assert_eq!(
            balance_of(&account, &btc()),
            Some(Balance::new(dec!(1), dec!(1)))
        );
        assert_eq!(usd_balance(&account), Balance::new(dec!(3), dec!(3)));

        let covered = Credit::Delivery(Delivery {
            asset: usd(),
            amount: dec!(-2),
        });
        let Ok(committed) = account.commit(
            &debit,
            Some(&covered),
            Decimal::ZERO,
            DateTime::<Utc>::MIN_UTC,
        ) else {
            panic!("3 usd free covers a 2 usd excess fee");
        };
        assert_eq!(committed.paid.balance, Balance::new(dec!(0), dec!(0)));
        assert_eq!(
            committed.received.map(|balance| balance.balance),
            Some(Balance::new(dec!(1), dec!(1)))
        );
    }

    #[test]
    fn a_charge_moves_total_and_free_together_and_may_take_free_negative() {
        let mut account = account_with_usd(dec!(100));
        account
            .reserve(&usd(), dec!(60), DateTime::<Utc>::MIN_UTC)
            .unwrap();

        let charged = account.charge(&usd(), dec!(-50), DateTime::<Utc>::MIN_UTC);
        assert_eq!(charged.balance, Balance::new(dec!(50), dec!(-10)));
        assert_eq!(usd_balance(&account), Balance::new(dec!(50), dec!(-10)));

        let credited = account.charge(&usd(), dec!(15), DateTime::<Utc>::MIN_UTC);
        assert_eq!(credited.balance, Balance::new(dec!(65), dec!(5)));
    }

    /// A snapshot holding `orders`, as a configured `initial_state` would.
    fn snapshot_of(orders: impl IntoIterator<Item = UnindexedOrder>) -> UnindexedAccountSnapshot {
        let mut orders = orders.into_iter();
        let first = orders.next().unwrap();
        let mut snapshot = spot_config_holding("1", "1000", first).initial_state;
        snapshot.instruments[0].orders.extend(orders);
        snapshot
    }

    fn open(cid: &str) -> UnindexedOrder {
        let at = DateTime::<Utc>::MIN_UTC;
        seeded_gtd(cid, "100", at, at + chrono::TimeDelta::days(1))
    }

    fn ended(
        cid: &str,
        state: OrderState<AssetNameExchange, InstrumentNameExchange>,
    ) -> UnindexedOrder {
        UnindexedOrder { state, ..open(cid) }
    }

    fn cancelled(cid: &str) -> UnindexedOrder {
        let at = DateTime::<Utc>::MIN_UTC;
        ended(
            cid,
            OrderState::inactive(Cancelled::new(OrderId::new(cid), at, None)),
        )
    }

    /// One id listed twice among the orders a snapshot seeds is refused, rather than one of them
    /// silently winning, whichever two of open and cancelled they are.
    #[test]
    fn a_snapshot_seeding_one_id_twice_is_refused() {
        for (orders, case) in [
            ([open("x"), open("x")], "open twice"),
            ([open("x"), cancelled("x")], "open, then cancelled"),
            ([cancelled("x"), open("x")], "cancelled, then open"),
            ([cancelled("x"), cancelled("x")], "cancelled twice"),
        ] {
            assert_eq!(
                AccountState::try_from(snapshot_of(orders)).err(),
                Some(DuplicateSeededOrder(ClientOrderId::new("x"))),
                "{case}"
            );
        }
    }

    /// An order the snapshot does not seed holds no id, so it cannot clash with one it does.
    #[test]
    fn an_order_a_snapshot_does_not_seed_does_not_clash() {
        let at = DateTime::<Utc>::MIN_UTC;
        let filled = ended(
            "x",
            OrderState::fully_filled(Filled::new(OrderId::new("x"), at, dec!(1), None)),
        );

        let Ok(account) = AccountState::try_from(snapshot_of([filled, open("x")])) else {
            panic!("a filled order is not seeded");
        };
        assert!(account.orders().contains(&ClientOrderId::new("x")));
    }
}
