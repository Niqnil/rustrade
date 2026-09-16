use crate::{
    UnindexedAccountSnapshot,
    balance::AssetBalance,
    order::{
        Order,
        id::ClientOrderId,
        state::{ActiveOrderState, Cancelled, InactiveOrderState, Open, OrderState},
    },
    trade::Trade,
};
use chrono::{DateTime, Utc};
use derive_more::Constructor;
use fnv::FnvHashMap;
use rust_decimal::Decimal;
use rustrade_instrument::{
    asset::name::AssetNameExchange, exchange::ExchangeId, instrument::name::InstrumentNameExchange,
};

#[derive(Debug, Constructor)]
pub struct AccountState {
    balances: FnvHashMap<AssetNameExchange, AssetBalance<AssetNameExchange>>,
    orders_open: FnvHashMap<ClientOrderId, Order<ExchangeId, InstrumentNameExchange, Open>>,
    orders_cancelled:
        FnvHashMap<ClientOrderId, Order<ExchangeId, InstrumentNameExchange, Cancelled>>,
    trades: Vec<Trade<AssetNameExchange, InstrumentNameExchange>>,
}

impl AccountState {
    pub fn update_time_exchange(&mut self, time_exchange: DateTime<Utc>) {
        for balance in self.balances.values_mut() {
            balance.time_exchange = time_exchange;
        }

        for order in self.orders_open.values_mut() {
            order.state.time_exchange = time_exchange;
        }
    }

    pub fn balances(&self) -> impl Iterator<Item = &AssetBalance<AssetNameExchange>> + '_ {
        self.balances.values()
    }

    pub fn orders_open(
        &self,
    ) -> impl Iterator<Item = &Order<ExchangeId, InstrumentNameExchange, Open>> + '_ {
        self.orders_open.values()
    }

    pub fn orders_cancelled(
        &self,
    ) -> impl Iterator<Item = &Order<ExchangeId, InstrumentNameExchange, Cancelled>> + '_ {
        self.orders_cancelled.values()
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

    /// Reserves and immediately settles `amount` of `asset`: the ledger move an order that fills on
    /// arrival makes.
    ///
    /// Expressed as the two steps rather than one subtraction because it is the same path a resting
    /// order takes, with the interval between them collapsed to nothing. The client sees **one**
    /// balance restatement, not two — a fill is atomic at the venue, and a balance is an absolute
    /// restatement rather than a delta, so emitting the intermediate state would report a balance
    /// the account never had.
    ///
    /// # Errors
    /// [`BalanceInsufficient`] if `free` does not cover `amount`, leaving the ledger untouched.
    ///
    /// # Panics
    /// Panics if `asset` has no balance — see [`reserve`](Self::reserve).
    pub fn debit_filled(
        &mut self,
        asset: &AssetNameExchange,
        amount: Decimal,
        time_exchange: DateTime<Utc>,
    ) -> Result<AssetBalance<AssetNameExchange>, BalanceInsufficient> {
        self.reserve(asset, amount, time_exchange)?;
        Ok(self.settle(asset, amount, time_exchange))
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
            (FnvHashMap::default(), FnvHashMap::default()),
            |(mut orders_open, mut orders_cancelled), snapshot| {
                for order in snapshot.orders {
                    match order.state {
                        OrderState::Active(ActiveOrderState::Open(open)) => {
                            orders_open.insert(
                                order.key.cid.clone(),
                                Order {
                                    key: order.key,
                                    side: order.side,
                                    price: order.price,
                                    quantity: order.quantity,
                                    kind: order.kind,
                                    time_in_force: order.time_in_force,
                                    state: open,
                                },
                            );
                        }
                        OrderState::Inactive(InactiveOrderState::Cancelled(cancelled)) => {
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
            trades: vec![],
        }
    }
}
