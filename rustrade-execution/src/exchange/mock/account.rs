use crate::{
    UnindexedAccountSnapshot,
    balance::AssetBalance,
    exchange::mock::orders::{
        AlreadyResting, OpenOrder, OpenOrders, Reservation, RestingOrder, as_open,
    },
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
        }
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
                free: balance.balance.free,
                required: amount,
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

    /// Commits one arriving order's whole ledger effect, or none of it.
    ///
    /// One order's arrival can both settle a fill and take a hold: a taker that the book could
    /// only partly fill settles what traded and reserves against the remainder it leaves resting.
    /// Those are one event at the venue, so they are one operation here — and because
    /// [`reserve`](Self::reserve) tests `free` before it moves anything, an arrival this account
    /// cannot afford leaves the ledger exactly as it found it.
    ///
    /// # Why this is not two calls
    /// Settling first and reserving second is the same two moves in the order that can fail
    /// halfway: the settle commits, the reserve is refused, and nothing puts back what `total`
    /// has already lost — [`release`](Self::release) moves only `free`, and its own assertion
    /// fires if it is used as a rollback. The venue would then hold a balance its client never
    /// hears about, silently and for the rest of the run. Asking for the whole requirement up
    /// front is what makes that state unreachable rather than merely avoided.
    ///
    /// # One arrival, one restatement
    /// The returned balance is the only one an arrival owes. A balance is an absolute restatement
    /// rather than a delta, and a fill is atomic at the venue, so emitting the state between the
    /// settle and the hold would report a balance the account never held — for the same reason a
    /// conservative reservation that had to be released and re-debited would.
    ///
    /// # A credit funds the debit it arrives with
    /// A fill that closes a cash-settled position pays proceeds back: `credit`, the margin and
    /// realised PnL of what it closes, paid into the debit's own asset, since one fill moves one
    /// asset. A spot fill, or a CFD fill that closes nothing, passes zero. The credit is separate
    /// from the debit rather than netted into it, because one fill can carry both: a CFD flip,
    /// closing a long and opening a short in one order, is paid back for the long and posts margin
    /// for the short. Both are part of the same fill, so the credit counts towards what `free` must
    /// cover, and the flip is funded by the long it closes. The credit is applied only once the
    /// whole requirement is known to be covered, so a refusal still leaves the ledger exactly as it
    /// was.
    ///
    /// # Errors
    /// [`BalanceInsufficient`] if `free` plus the credit does not cover
    /// [`settled`](Debit::settled) + [`reserved`](Debit::reserved), leaving the ledger untouched.
    ///
    /// # Panics
    /// Panics if the asset has no balance — see [`reserve`](Self::reserve). Debug-asserts that
    /// `credit` is not negative: a loss beyond the margin is part of the debit, not a credit.
    pub fn commit(
        &mut self,
        debit: &Debit,
        credit: Decimal,
        time_exchange: DateTime<Utc>,
    ) -> Result<AssetBalance<AssetNameExchange>, BalanceInsufficient> {
        debug_assert!(
            !credit.is_sign_negative(),
            "a credit of {credit} {} is negative: a loss beyond the margin is debited",
            debit.asset
        );

        // Asked for as one requirement, so a refusal refuses the arrival rather than half of it.
        let required = debit.settled + debit.reserved;
        let free = self.balance_expect(&debit.asset).balance.free;
        if free + credit < required {
            return Err(BalanceInsufficient {
                free: free + credit,
                required,
            });
        }

        // Applied only now that the whole requirement is covered, so a refusal moves nothing.
        if !credit.is_zero() {
            let balance = self.balance_expect(&debit.asset);
            balance.balance.total += credit;
            balance.balance.free += credit;
        }

        // Infallible: `free` now covers the whole requirement. `total` cannot fall below `free`,
        // which the reserve takes the whole requirement out of, and whatever was reserved and not
        // settled stays held.
        self.reserve(&debit.asset, required, time_exchange)?;
        Ok(self.settle(&debit.asset, debit.settled, time_exchange))
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

/// A ledger operation asked for more of an asset than `free` covers.
///
/// Carries both sides of the comparison so the caller can report them without re-reading the
/// ledger it has just been refused by.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct BalanceInsufficient {
    /// Freely-spendable amount at the moment of refusal.
    pub free: Decimal,
    /// Amount the operation asked for.
    pub required: Decimal,
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

        let Ok(balance) = account.commit(&debit, dec!(200), DateTime::<Utc>::MIN_UTC) else {
            panic!("100 free plus a 200 credit covers 290");
        };

        // total: 100 + 200 credited - 250 settled; free: that, less the 40 still held.
        assert_eq!(balance.balance, Balance::new(dec!(50), dec!(10)));
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

        let Err(insufficient) = account.commit(&debit, dec!(200), DateTime::<Utc>::MIN_UTC) else {
            panic!("100 free plus a 200 credit does not cover 310");
        };

        assert_eq!(
            insufficient,
            BalanceInsufficient {
                free: dec!(300),
                required: dec!(310),
            }
        );
        assert_eq!(usd_balance(&account), Balance::new(dec!(100), dec!(100)));
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
