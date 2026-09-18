use crate::{
    UnindexedAccountSnapshot,
    balance::AssetBalance,
    exchange::mock::orders::{OpenOrders, as_open},
    order::{
        Order,
        id::ClientOrderId,
        state::{Cancelled, Expired, InactiveOrderState, Open, OrderState},
    },
    trade::Trade,
};
use chrono::{DateTime, Utc};
use fnv::{FnvHashMap, FnvHashSet};
use rust_decimal::Decimal;
use rustrade_instrument::{
    asset::name::AssetNameExchange, exchange::ExchangeId, instrument::name::InstrumentNameExchange,
};

#[derive(Debug)]
pub struct AccountState {
    balances: FnvHashMap<AssetNameExchange, AssetBalance<AssetNameExchange>>,
    orders_open: OpenOrders,
    orders_cancelled:
        FnvHashMap<ClientOrderId, Order<ExchangeId, InstrumentNameExchange, Cancelled>>,
    /// Orders that filled, kept only so a cancel arriving after one can say *why* it failed.
    ///
    /// Ids alone: nothing reads the order back, and the fill itself is already in
    /// [`trades`](Self::trades). Grows with the run, like `trades` and `orders_cancelled` — a
    /// simulated venue's ledgers are bounded by the dataset, not reaped.
    orders_filled: FnvHashSet<ClientOrderId>,
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
            orders_filled: FnvHashSet::default(),
            orders_expired: FnvHashMap::default(),
            trades,
        }
    }

    /// This account's open orders, indexed for lookup and ordered for matching.
    pub fn orders(&self) -> &OpenOrders {
        &self.orders_open
    }

    /// This account's open orders, for a venue booking, matching or cancelling one.
    pub fn orders_mut(&mut self) -> &mut OpenOrders {
        &mut self.orders_open
    }

    /// Records that `order` was cancelled, so it is reported by a later account snapshot and a
    /// second cancel for it can be told apart from a cancel for an order that never existed.
    pub fn ack_cancelled(&mut self, order: Order<ExchangeId, InstrumentNameExchange, Cancelled>) {
        self.orders_cancelled.insert(order.key.cid.clone(), order);
    }

    /// Whether `cid` names an order this account cancelled.
    pub fn is_cancelled(&self, cid: &ClientOrderId) -> bool {
        self.orders_cancelled.contains_key(cid)
    }

    /// Records that `cid` filled, so a cancel that loses the race to it can say so.
    pub fn ack_filled(&mut self, cid: ClientOrderId) {
        self.orders_filled.insert(cid);
    }

    /// Whether `cid` names an order this account filled.
    pub fn is_filled(&self, cid: &ClientOrderId) -> bool {
        self.orders_filled.contains(cid)
    }

    /// Records that `order` reached its own deadline, so it is reported by a later account snapshot
    /// and a cancel arriving after it can be told apart from a cancel for an unknown order.
    pub fn ack_expired(&mut self, order: Order<ExchangeId, InstrumentNameExchange, Expired>) {
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
    /// # Errors
    /// [`BalanceInsufficient`] if `free` does not cover
    /// [`settled`](Debit::settled) + [`reserved`](Debit::reserved), leaving the ledger untouched.
    ///
    /// # Panics
    /// Panics if the asset has no balance — see [`reserve`](Self::reserve).
    pub fn commit(
        &mut self,
        debit: &Debit,
        time_exchange: DateTime<Utc>,
    ) -> Result<AssetBalance<AssetNameExchange>, BalanceInsufficient> {
        // Asked for as one requirement, so a refusal refuses the arrival rather than half of it.
        self.reserve(&debit.asset, debit.settled + debit.reserved, time_exchange)?;

        // Infallible: `total` cannot fall below `free`, which the reserve above has already taken
        // the whole requirement out of. Whatever was reserved and not settled stays held.
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

impl From<UnindexedAccountSnapshot> for AccountState {
    fn from(value: UnindexedAccountSnapshot) -> Self {
        let UnindexedAccountSnapshot {
            exchange: _,
            balances,
            instruments,
        } = value;

        let balances = balances
            .into_iter()
            .map(|asset_balance| (asset_balance.asset.clone(), asset_balance))
            .collect();

        let (orders_open, orders_cancelled) = instruments.into_iter().fold(
            (OpenOrders::default(), FnvHashMap::default()),
            |(mut orders_open, mut orders_cancelled), snapshot| {
                for order in snapshot.orders {
                    match &order.state {
                        OrderState::Active(_) => {
                            // `as_open` yields `None` for an active order that is not yet open —
                            // an `OpenInFlight`, which has no venue-side existence to record.
                            if let Some(open) = as_open(order) {
                                // Seeded, not booked here: the venue holds nothing against it, so
                                // a displaced entry can leak nothing. A snapshot listing one
                                // `ClientOrderId` twice is a mis-specified fixture rather than a
                                // runtime condition, and the later order silently replacing the
                                // earlier is what it would otherwise get.
                                let cid = open.key.cid.clone();
                                let displaced = orders_open.insert(open, None);
                                debug_assert!(
                                    displaced.is_none(),
                                    "account snapshot lists {cid} as open more than once, so only \
                                     the last of them reaches the book"
                                );
                            }
                        }
                        OrderState::Inactive(InactiveOrderState::Cancelled(cancelled)) => {
                            let cancelled = cancelled.clone();
                            orders_cancelled.insert(
                                order.key.cid.clone(),
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
                        _ => {}
                    }
                }

                (orders_open, orders_cancelled)
            },
        );

        Self {
            balances,
            orders_open,
            orders_cancelled,
            orders_filled: FnvHashSet::default(),
            orders_expired: FnvHashMap::default(),
            trades: vec![],
        }
    }
}
