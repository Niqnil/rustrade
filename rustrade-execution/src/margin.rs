//! Margin borrowing: the borrow and repay policy a margin account applies to its orders, the
//! simulated venue's spot margin configuration, and the interest it charges on its loans.
//!
//! [`MarginEffect`] is what an order does to a margin account's loans: borrow what it lacks, repay
//! from what it receives, or neither. A live margin client sends it with each order, and the
//! simulated venue applies it when [`SpotMarginConfig`] turns its spot margin on. A
//! [`MarginInterestModel`] prices those loans over time; [`PeriodicMarginInterest`] is the
//! built-in one. A margin client that learns what an order borrowed reports it as a
//! [`MarginBorrow`], in a [`MarginPlacement`].

use crate::cash_flow::CashFlowLoan;
use crate::holding_cost::{DayCount, RateSeries, next_whole_interval};
use crate::order::{
    Order,
    state::{ActiveOrderState, InactiveOrderState, OrderState, UnindexedOrderState},
};
use chrono::{DateTime, TimeDelta, Utc};
use derive_more::Constructor;
use fnv::FnvHashMap;
use rust_decimal::Decimal;
use rustrade_instrument::{
    asset::name::AssetNameExchange, exchange::ExchangeId, instrument::name::InstrumentNameExchange,
};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, fmt::Debug};
use thiserror::Error;

/// What an order does to a margin account's loans: borrow what it lacks, repay from what it
/// receives, or touch neither.
///
/// A margin client has one as its default and a request may carry its own
/// (`RequestOpen::margin_effect`, where `None` takes the client's default). The three values are
/// the ones a margin venue takes per order (Binance's `NO_SIDE_EFFECT`, `MARGIN_BUY` and
/// `AUTO_REPAY`). Whether an order opens or closes a position decides which it should be, and
/// that is position state the library deliberately does not track, so the caller says.
///
/// A client or simulated venue that cannot honour an effect refuses the order unsent with
/// [`OrderError::UnsupportedMarginEffect`](crate::error::OrderError::UnsupportedMarginEffect),
/// rather than place it without the effect.
///
/// The `Serialize`/`Deserialize` wire form is the config-file value (`"no_side_effect"`,
/// `"borrow"`, `"repay"`), which is not any venue's API value.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum MarginEffect {
    /// Neither borrow nor repay: an order that would need a loan is refused, and what it receives
    /// stays free, as Binance's `NO_SIDE_EFFECT` does. The default, so that no order takes on
    /// debt unless the caller asks.
    #[default]
    NoSideEffect,
    /// Borrow what the order lacks, as Binance's `MARGIN_BUY` does, in the asset it pays with
    /// (base for a sell, which opens or extends a short).
    ///
    /// Binance lends the shortfall, or the asset's minimum borrow if that is more: **loan =
    /// max(shortfall, minimum borrow)**, where the minimum is the asset's `userMinBorrow` in
    /// `GET /sapi/v1/margin/allAssets`. A sell's shortfall is its quantity less the free base; a
    /// limit buy's is its quantity times its limit price less the free quote. Nothing caps the
    /// loan at the order's size, so a small order on an empty balance can borrow more than it
    /// pays, and what it does not pay stays free balance, owed.
    ///
    /// The loan is taken when the order is placed, not when it fills, and nothing repays it from
    /// the order's proceeds: close it with a [`Repay`](Self::Repay) order. Whether a loan
    /// is repaid when the order is cancelled before it fills is the client's `auto_repay_at_cancel`
    /// option.
    ///
    /// # Warning
    /// A **mis-sized order takes on debt**: one larger than the free balance borrows the shortfall,
    /// or the asset's minimum borrow, without any further opt-in, and interest accrues on it.
    Borrow,
    /// Repay the loan of the asset the order receives from what it receives, interest first, as
    /// Binance's `AUTO_REPAY` does. It borrows nothing: an order the free balance cannot pay for
    /// is refused.
    Repay,
}

/// A loan a margin order took out when it was placed: `amount` of `asset`.
///
/// `amount` is the loan's principal, as the venue reports lending it: a reported zero is kept as
/// zero, not read as no loan, and the amount need not equal what the order lacked. It is not the
/// order's net
/// effect on the balance: a fee the order pays in `asset` is not in it, and interest accrues on
/// it from the moment it is lent. `asset` is the asset the order pays with, which may be the base
/// or the quote asset of the pair.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Deserialize, Serialize, Constructor)]
pub struct MarginBorrow {
    pub asset: AssetNameExchange,
    pub amount: Decimal,
}

/// The answer to placing a margin order: the order, and what it borrowed.
///
/// Returned by a margin client's own placement method (for example `BinanceMargin`'s
/// `open_margin_order`), whose venue reports the loan in its answer to the order.
/// `ExecutionClient::open_order` returns only the order.
#[derive(Debug, Clone, PartialEq)]
pub struct MarginPlacement {
    /// The order, as [`ExecutionClient::open_order`](crate::client::ExecutionClient::open_order)
    /// returns it.
    pub order: Order<ExchangeId, InstrumentNameExchange, UnindexedOrderState>,
    /// What the order borrowed when it was placed.
    ///
    /// - `Some` when the venue's answer reports a loan.
    /// - `None` when the answer reports none: the order borrowed nothing (its
    ///   [`MarginEffect`] is not [`Borrow`](MarginEffect::Borrow), or its free balance covered
    ///   it), or the order was refused.
    /// - When [`order.state`](Order::state) is an unknown outcome
    ///   ([`OrderState::is_outcome_unknown`](crate::order::state::OrderState::is_outcome_unknown)),
    ///   the venue may have placed the order and lent to it without the client learning how
    ///   much. `None` then says nothing about a loan: read the account's balances.
    pub borrowed: Option<MarginBorrow>,
}

impl MarginPlacement {
    /// The loan the order took, with what identifies it, when the answer reports one: what
    /// [`borrowed`](Self::borrowed) says, the order's instrument, its venue and client ids, and
    /// the venue's time for the order's state, which for a new order is when the venue accepted
    /// it and lent.
    ///
    /// `None` when [`borrowed`](Self::borrowed) is, or when the order's state has no venue order
    /// id: one the venue acknowledged without assigning its own, or one still in flight or
    /// refused.
    pub fn loan(&self) -> Option<OrderLoan> {
        let borrowed = self.borrowed.clone()?;
        let (order_id, time) = match &self.order.state {
            OrderState::Active(ActiveOrderState::Open(open)) => {
                (open.id.assigned()?.clone(), open.time_exchange)
            }
            OrderState::Inactive(InactiveOrderState::FullyFilled(filled)) => {
                (filled.id.clone(), filled.time_exchange)
            }
            OrderState::Inactive(InactiveOrderState::Cancelled(cancelled)) => {
                (cancelled.id.clone(), cancelled.time_exchange)
            }
            OrderState::Inactive(InactiveOrderState::Expired(expired)) => {
                (expired.id.clone(), expired.time_exchange)
            }
            OrderState::Active(
                ActiveOrderState::OpenInFlight(_)
                | ActiveOrderState::CancelInFlight(_)
                | ActiveOrderState::AmendInFlight(_),
            )
            | OrderState::Inactive(InactiveOrderState::OpenFailed(_)) => return None,
        };
        Some(OrderLoan::new(
            self.order.key.instrument.clone(),
            borrowed,
            CashFlowLoan::new(time, order_id, self.order.key.cid.clone()),
        ))
    }
}

/// A loan an order took when it was placed, and what identifies it: the order's instrument, what
/// it borrowed, and the order and time it borrowed at.
///
/// A margin client that keeps the loans its orders took can link the venue's charge on a new loan
/// to the order that took it, through [`CashFlow::loan`](crate::cash_flow::CashFlow::loan). Built
/// by [`MarginPlacement::loan`], and handed back to a client after a restart by, for example,
/// `BinanceMargin::recall_loans`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Deserialize, Serialize, Constructor)]
#[non_exhaustive]
pub struct OrderLoan {
    /// The instrument the order traded.
    pub instrument: InstrumentNameExchange,
    /// What the order borrowed: the asset and the loan's principal.
    pub borrowed: MarginBorrow,
    /// When the venue lent, and the order that borrowed.
    pub loan: CashFlowLoan,
}

/// Spot margin on the simulated venue: what it lends a spot order, how much, and what it repays.
///
/// Off by default: `MockExecutionConfig::spot_margin` is `None`, and the venue is a cash account
/// whose spot orders must be paid for with what it holds. With a configuration, it is a cross
/// margin account, with one pool of balances shared by every instrument, which lends a spot order
/// what it lacks and reports each balance's loan as Binance's margin account does.
///
/// # What it lends, and when
/// Each spot order has a [`MarginEffect`]: its request's `margin_effect`, else this
/// configuration's [`margin_effect`](Self::margin_effect). An order with [`MarginEffect::Borrow`]
/// that the account cannot pay for with its free balance borrows the shortfall of the asset it
/// pays with (base for a sell, quote for a buy), or that asset's
/// [`min_borrow`](Self::min_borrow) if it is more, as Binance does. It borrows when the venue
/// accepts the order, as Binance does, so a resting limit order or a held market order is lent its
/// shortfall at once, and the venue holds it against the order as it holds any other balance. What
/// a minimum borrow lends beyond the shortfall is not held: it is free balance, owed. An asset the
/// account has never held is opened with the loan.
///
/// An order that is cancelled, expires or is refused at its fill before it fills repays what it
/// borrowed, interest first, and a held market order that fills for less than was held repays the
/// part of its loan it no longer needs, if [`auto_repay_at_cancel`](Self::auto_repay_at_cancel)
/// is on (the default, as on Binance). One that fills in part and rests the remainder pays for the
/// part that filled with the account's own balance first, so the loan is held against the
/// remainder and repaid if the remainder is cancelled. What a minimum borrow lent beyond the
/// shortfall is repaid with the rest if the order ends before it fills in full, and stays owed
/// once it does.
///
/// An order with [`MarginEffect::NoSideEffect`] or [`MarginEffect::Repay`] lends nothing: one it
/// cannot pay for is rejected as on a cash account. A loan already in the venue's initial state
/// is left as it is, and balances still report their loans.
///
/// Only spot orders borrow. A CFD's margin is posted from the quote asset's free balance, as
/// without this configuration, and a CFD order carrying [`MarginEffect::Borrow`] or
/// [`MarginEffect::Repay`] is refused with
/// [`OrderError::UnsupportedMarginEffect`](crate::error::OrderError::UnsupportedMarginEffect).
///
/// # How much it lends
/// A borrow is allowed while the account's debt, after it, is at most `max_leverage − 1` times
/// the account's equity. Both are valued in [`valuation_asset`](Self::valuation_asset):
/// - Each asset is valued at the mid of an instrument on the venue that pairs it with the
///   valuation asset, else that market's last price, in either direction (`BTC/USDT`, or the
///   inverse of `USDT/BTC`). The valuation asset is worth one. A market priced at zero values
///   nothing. A [borrow price window](Self::with_borrow_price_window) values each asset over a
///   recent window of that price instead: see `# Borrow price window`.
/// - **Equity** is the sum over assets of `total − borrowed − interest`, valued that way, where an
///   asset whose net is positive counts at its
///   [`borrow_collateral_ratio`](Self::borrow_collateral_ratio): see `# Collateral ratios`. An
///   asset the venue cannot value counts as no equity, which understates what the account can
///   borrow rather than overstating it.
/// - **Debt** is the sum over assets of `borrowed + interest`, valued the same way. The venue
///   lends nothing while it cannot value a debt, and never lends an asset it cannot value.
///
/// An asset given a [`max_borrow`](Self::max_borrow) is lent no more than that cap allows as
/// well: what the account already owes of it, principal plus interest, plus the new loan, is at
/// most the cap. Both limits apply, and the tighter one decides.
///
/// An order that needs more than the leverage limit allows is rejected as unaffordable, with a
/// reason that says how much the venue would lend. So is one whose asset's
/// [`min_borrow`](Self::min_borrow) is more than that, since the venue never lends less than the
/// minimum, and its reason names the minimum too. One that needs more than an asset's cap allows,
/// where the cap is the tighter limit, is rejected with
/// [`ApiError::BorrowRejected`](crate::error::ApiError::BorrowRejected) instead: see
/// [`with_max_borrow`](Self::with_max_borrow). A `max_leverage` of one lends nothing, but still
/// reports each balance's loan.
///
/// It lends nothing while a liquidation is under way: see [`SpotMarginLiquidation`].
///
/// # Collateral ratios
/// An asset can count for less than its full value as collateral, as Binance's cross margin
/// weights each asset by a collateral ratio. Each asset has two, both one by default:
/// - a [`borrow_collateral_ratio`](Self::borrow_collateral_ratio), which the leverage limit above
///   uses, and so what the venue lends;
/// - a [`liquidation_collateral_ratio`](Self::liquidation_collateral_ratio), which the margin level
///   a [`SpotMarginLiquidation`] checks uses.
///
/// A ratio applies only to the part of an asset the account holds beyond what it owes of it,
/// its positive net: `ratio × (total − borrowed − interest)`. What it holds up to what it owes
/// counts in full, and so do the holdings of an asset it owes more of than it holds. So an
/// account that holds 2 BTC and owes 1, at a ratio of 0.95, counts the first BTC in full and the
/// second at 0.95. Debt is never weighted. A ratio of zero counts the asset for nothing beyond
/// its debt.
///
/// In margin-level terms, the account's collateral is its debt plus its weighted equity, and the
/// margin level is collateral over debt, as Binance defines it: a borrow is allowed while that
/// level, with borrow ratios, stays at or above `max_leverage / (max_leverage − 1)`.
///
/// An asset's liquidation ratio may not be below its borrow ratio, so that the borrow ratios are
/// the stricter ones, as on Binance. That keeps a borrow the leverage limit allows from leaving
/// the account at its liquidation level.
///
/// # Borrow price window
/// Off by default: the leverage limit values each asset at its current price, as the liquidation
/// check does. Binance's borrow check is stricter. It values each asset over a recent window of
/// its price, at the highest for an asset the account owes and the lowest for every other, so a
/// price that just jumped lends no more at once, and one that just fell lends less at once. Set
/// with [`with_borrow_price_window`](Self::with_borrow_price_window), the leverage limit does the
/// same over the window given:
/// - Each market observation the venue is fed adds its price to the window of that instrument:
///   the mid, else the last price, at which the venue values the instrument, and the high and low
///   of a bar the observation carries. The current price always counts.
/// - What counts is what was observed in the window up to the latest instant the venue's clock
///   has reached, from that instant less the window, inclusive.
/// - An asset the account owes is valued at its highest price in the window, for every unit it
///   holds as well as for its debt. So is the asset being borrowed, since the loan makes it one
///   the account owes: an order's or a [`borrow_capacity`] query's. Every other asset is valued
///   at its lowest. The valuation asset is worth one.
/// - The venue lends the lesser of what the leverage limit allows at those prices and at the
///   current ones. The window can only make the venue lend less. Valuing an asset the account owes
///   at its highest raises the value of its holdings as well as its debt, so on its own it could
///   lend more than the current prices would, and leave the account at its liquidation level the
///   moment it borrowed.
///
/// The venue keeps, per spot instrument, every observed price that could still be the window's
/// lowest or highest: in a market that only rises or only falls, every observation inside the
/// window, so a long window over a fast feed holds that many.
///
/// The window changes only how much the venue lends: the margin level a [`SpotMarginLiquidation`]
/// checks, and any interest, are valued at current prices. A venue no driver feeds the market,
/// one priced by each request's own snapshot, has no window to look back on, and lends as if
/// there were none.
///
/// [`borrow_capacity`]: crate::exchange::mock::venue::SimulatedVenue::borrow_capacity
///
/// # What it repays
/// Under [`MarginEffect::Repay`], whatever a fill delivers into an asset the account owes repays
/// that asset's loan, interest first, then principal, and only the rest becomes free balance. What
/// a buy delivers is its quantity less any fee charged in base, so a buy of exactly what is owed,
/// with a base fee, leaves a fee-sized loan. Gross the buy up by the fee to clear it. Under the
/// other two effects a fill repays nothing: as on Binance, a repay is per order, and nothing
/// repays a loan automatically.
///
/// # Liquidation
/// Off by default: an account whose debt grows past its equity keeps trading, and nothing closes
/// its positions. Configured with [`with_liquidation`](Self::with_liquidation), the venue repays
/// every debt once the account's margin level falls to the configured level, selling what the
/// account holds as needed. See [`SpotMarginLiquidation`].
///
/// # What it does not model
/// - **Isolated margin.** One pool of balances backs every instrument.
/// - **Tiered collateral ratios.** An asset has one ratio per check, whatever the size of its
///   holding. Binance publishes each ratio in progressive tiers by the holding's value in USD:
///   each slice of the value is weighted at its own tier's ratio. An asset with a single tier, as
///   every asset had on 2026-11-04, is modelled exactly.
/// - **Index prices.** Every check values an asset at a market's own mid or last price, where
///   Binance values at an index over several venues. The borrow price window above stands in for
///   the window Binance takes over that index.
/// - **The venue's lending pool.** The venue has whatever it lends: no shared pool runs short, as
///   one can on a real venue whatever the account's own limits allow.
/// - **Interest**, by this configuration alone. A venue given a [`MarginInterestModel`] charges
///   it; see that trait.
///
/// [`new`](Self::new) refuses a `max_leverage` below one, [`with_min_borrow`](Self::with_min_borrow)
/// a negative minimum, [`with_max_borrow`](Self::with_max_borrow) a negative cap, the collateral
/// ratio builders a ratio outside `[0, 1]` or a liquidation ratio below the borrow ratio,
/// [`with_borrow_price_window`](Self::with_borrow_price_window) a window that is not a positive
/// whole number of milliseconds, [`with_liquidation`](Self::with_liquidation) a liquidation the
/// leverage limit could reach, and so does deserialising any of them, so a configuration that
/// exists is one the venue can apply.
/// Serialised, `max_leverage`, each minimum borrow, each cap and each ratio are decimal strings
/// (`"3"`), as the crate writes decimals elsewhere, so no precision is lost to a float.
/// `min_borrow`, `max_borrow`, `borrow_collateral_ratio` and `liquidation_collateral_ratio` are
/// maps from asset to amount, each left out when empty. `borrow_price_window_ms` is the window
/// in whole milliseconds, an integer, and `liquidation` is left out when off, as is the window.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize)]
#[serde(try_from = "SpotMarginRepr", into = "SpotMarginRepr")]
pub struct SpotMarginConfig(SpotMarginRepr);

/// The serialised form of a [`SpotMarginConfig`], before validation.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize)]
struct SpotMarginRepr {
    valuation_asset: AssetNameExchange,
    #[serde(with = "rust_decimal::serde::str")]
    max_leverage: Decimal,
    margin_effect: MarginEffect,
    #[serde(default = "default_auto_repay_at_cancel")]
    auto_repay_at_cancel: bool,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    min_borrow: BTreeMap<AssetNameExchange, Decimal>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    max_borrow: BTreeMap<AssetNameExchange, Decimal>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    borrow_collateral_ratio: BTreeMap<AssetNameExchange, Decimal>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    liquidation_collateral_ratio: BTreeMap<AssetNameExchange, Decimal>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    borrow_price_window_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    liquidation: Option<SpotMarginLiquidation>,
}

/// Serde default for `auto_repay_at_cancel`, here and on `BinanceMarginConfig`: on, as Binance's
/// `autoRepayAtCancel` is.
pub(crate) fn default_auto_repay_at_cancel() -> bool {
    true
}

impl SpotMarginConfig {
    /// Spot margin applying `margin_effect` to each order that does not carry its own, up to
    /// `max_leverage` over the account's equity, valued in `valuation_asset`.
    ///
    /// The effect has no default, so that a configuration says whether the venue may take on debt
    /// for an order. Unused loans are repaid when their order ends unfilled; see
    /// [`with_auto_repay_at_cancel`](Self::with_auto_repay_at_cancel).
    ///
    /// # Errors
    /// Returns [`SpotMarginConfigError`] if `max_leverage` is below one.
    pub fn new(
        valuation_asset: AssetNameExchange,
        max_leverage: Decimal,
        margin_effect: MarginEffect,
    ) -> Result<Self, SpotMarginConfigError> {
        Self::try_from(SpotMarginRepr {
            valuation_asset,
            max_leverage,
            margin_effect,
            auto_repay_at_cancel: default_auto_repay_at_cancel(),
            min_borrow: BTreeMap::new(),
            max_borrow: BTreeMap::new(),
            borrow_collateral_ratio: BTreeMap::new(),
            liquidation_collateral_ratio: BTreeMap::new(),
            borrow_price_window_ms: None,
            liquidation: None,
        })
    }

    /// This configuration, repaying a loan its order no longer needs (the order was cancelled,
    /// expired or refused at its fill, or a held market order filled for less than was held) when
    /// `auto_repay_at_cancel` is true, and leaving it owed when false.
    ///
    /// On by default, as Binance's `autoRepayAtCancel` is. With it off, an unused loan stays owed
    /// until a [`MarginEffect::Repay`] order repays it.
    #[must_use]
    pub fn with_auto_repay_at_cancel(mut self, auto_repay_at_cancel: bool) -> Self {
        self.0.auto_repay_at_cancel = auto_repay_at_cancel;
        self
    }

    /// The asset equity and debt are valued in.
    pub fn valuation_asset(&self) -> &AssetNameExchange {
        &self.0.valuation_asset
    }

    /// The most the account may hold, as a multiple of its equity: its debt may reach
    /// `max_leverage − 1` times its equity.
    pub fn max_leverage(&self) -> Decimal {
        self.0.max_leverage
    }

    /// The effect of an order whose request carries none.
    pub fn margin_effect(&self) -> MarginEffect {
        self.0.margin_effect
    }

    /// Whether a loan its order no longer needs is repaid. See
    /// [`with_auto_repay_at_cancel`](Self::with_auto_repay_at_cancel).
    pub fn auto_repay_at_cancel(&self) -> bool {
        self.0.auto_repay_at_cancel
    }

    /// This configuration, never lending less than `min_borrow` of `asset`: a loan of `asset`
    /// below it is raised to it, as Binance raises a `MARGIN_BUY` loan to the asset's
    /// `userMinBorrow`. A minimum of zero removes it. Replaces any minimum `asset` had.
    ///
    /// No asset has one by default, so each loan is exactly its order's shortfall. An order its
    /// free balance covers borrows nothing, whatever the minimum.
    ///
    /// # Errors
    /// Returns [`SpotMarginConfigError::NegativeMinBorrow`] if `min_borrow` is negative.
    pub fn with_min_borrow(
        mut self,
        asset: AssetNameExchange,
        min_borrow: Decimal,
    ) -> Result<Self, SpotMarginConfigError> {
        self.0.min_borrow.insert(asset, min_borrow);
        Self::try_from(self.0)
    }

    /// The least the venue lends of `asset` in one loan, zero if it has no minimum. See
    /// [`with_min_borrow`](Self::with_min_borrow).
    pub fn min_borrow(&self, asset: &AssetNameExchange) -> Decimal {
        self.0
            .min_borrow
            .get(asset)
            .copied()
            .unwrap_or(Decimal::ZERO)
    }

    /// This configuration, lending `asset` only while what the account owes of it, principal plus
    /// interest, and the new loan come to at most `max_borrow`: a fixed cap on the account's debt
    /// in `asset`, as Binance caps what a margin account may borrow of each asset
    /// (`maxBorrowable`'s `borrowLimit`). Replaces any cap `asset` had.
    ///
    /// No asset has one by default, so only the leverage limit bounds a loan. A cap of zero lends
    /// none of `asset`. Nor is `asset` lent once what the cap leaves is below its
    /// [`min_borrow`](Self::min_borrow), since the venue never lends less than the minimum.
    ///
    /// # How the cap counts
    /// A borrow is allowed while what the account already owes of `asset`, loan plus interest,
    /// plus the new loan is at most the cap. Interest the new loan is charged when it is made is
    /// not counted, as the leverage limit does not count it, so it can take the debt just past the
    /// cap. Counting interest is this venue's rule: Binance does not document whether its limit
    /// counts it.
    ///
    /// The cap is the account's own limit, not the venue's supply: Binance can also refuse a
    /// borrow its limit allows because it has too little of the asset to lend, which this venue
    /// does not model.
    ///
    /// The mock client reports the cap as
    /// [`BorrowCapacity::account_limit`](crate::BorrowCapacity::account_limit), from
    /// [`BorrowCapacityClient::fetch_borrow_capacity`](crate::client::BorrowCapacityClient::fetch_borrow_capacity).
    ///
    /// # Rejection
    /// An order that would borrow past the cap is rejected, never lent less than it needs. Where
    /// the cap allows less than the leverage limit, the rejection is
    /// [`ApiError::BorrowRejected`](crate::error::ApiError::BorrowRejected) with
    /// reason [`BorrowLimitExceeded`](crate::error::BorrowRejectReason::BorrowLimitExceeded),
    /// as a live Binance margin client reports Binance's `-3006`. Its message names the cap, and
    /// for a sell its [`available`](crate::error::BorrowReject::available) is how much more of the
    /// base asset the venue would lend: zero when that is below the minimum borrow. A buy borrows
    /// the quote asset, in units other than the order's, so it leaves `available` unset. Where
    /// the leverage limit is the tighter, the order is rejected as unaffordable, as without a cap.
    ///
    /// # Errors
    /// Returns [`SpotMarginConfigError::NegativeMaxBorrow`] if `max_borrow` is negative.
    pub fn with_max_borrow(
        mut self,
        asset: AssetNameExchange,
        max_borrow: Decimal,
    ) -> Result<Self, SpotMarginConfigError> {
        self.0.max_borrow.insert(asset, max_borrow);
        Self::try_from(self.0)
    }

    /// The most the account may owe of `asset`, principal plus interest, or `None` if it has no
    /// cap. See [`with_max_borrow`](Self::with_max_borrow).
    pub fn max_borrow(&self, asset: &AssetNameExchange) -> Option<Decimal> {
        self.0.max_borrow.get(asset).copied()
    }

    /// This configuration, counting `ratio` of what the account holds of `asset` beyond what it
    /// owes of it as equity when the venue decides how much it lends: the asset's borrow
    /// collateral ratio, as Binance's Margin Data page lists one per asset. Replaces any borrow
    /// ratio `asset` had. See [`SpotMarginConfig`]'s `# Collateral ratios`.
    ///
    /// Every asset's is one by default. A ratio of one removes it.
    ///
    /// # Errors
    /// [`SpotMarginConfigError::CollateralRatioOutOfRange`] if `ratio` is outside `[0, 1]`, and
    /// [`SpotMarginConfigError::LiquidationRatioBelowBorrowRatio`] if it is above `asset`'s
    /// [`liquidation_collateral_ratio`](Self::liquidation_collateral_ratio).
    pub fn with_borrow_collateral_ratio(
        mut self,
        asset: AssetNameExchange,
        ratio: Decimal,
    ) -> Result<Self, SpotMarginConfigError> {
        self.0.borrow_collateral_ratio.insert(asset, ratio);
        Self::try_from(self.0)
    }

    /// The fraction of what the account holds of `asset` beyond what it owes of it that counts
    /// as equity when the venue decides how much it lends, one if it has none. See
    /// [`with_borrow_collateral_ratio`](Self::with_borrow_collateral_ratio).
    pub fn borrow_collateral_ratio(&self, asset: &AssetNameExchange) -> Decimal {
        self.0
            .borrow_collateral_ratio
            .get(asset)
            .copied()
            .unwrap_or(Decimal::ONE)
    }

    /// This configuration, counting `ratio` of what the account holds of `asset` beyond what it
    /// owes of it as equity in the margin level a [`SpotMarginLiquidation`] checks: the asset's
    /// liquidation collateral ratio, as Binance's Margin Data page lists one per asset. Replaces
    /// any liquidation ratio `asset` had. See [`SpotMarginConfig`]'s `# Collateral ratios`.
    ///
    /// Every asset's is one by default. A ratio of one removes it. It has no effect without a
    /// [`liquidation`](Self::with_liquidation).
    ///
    /// # Errors
    /// [`SpotMarginConfigError::CollateralRatioOutOfRange`] if `ratio` is outside `[0, 1]`, and
    /// [`SpotMarginConfigError::LiquidationRatioBelowBorrowRatio`] if it is below `asset`'s
    /// [`borrow_collateral_ratio`](Self::borrow_collateral_ratio). To set a liquidation ratio
    /// below the asset's current borrow ratio, lower the borrow ratio first, or set both at once
    /// with [`with_collateral_ratios`](Self::with_collateral_ratios).
    pub fn with_liquidation_collateral_ratio(
        mut self,
        asset: AssetNameExchange,
        ratio: Decimal,
    ) -> Result<Self, SpotMarginConfigError> {
        self.0.liquidation_collateral_ratio.insert(asset, ratio);
        Self::try_from(self.0)
    }

    /// The fraction of what the account holds of `asset` beyond what it owes of it that counts
    /// as equity in the margin level a liquidation checks, one if it has none. See
    /// [`with_liquidation_collateral_ratio`](Self::with_liquidation_collateral_ratio).
    pub fn liquidation_collateral_ratio(&self, asset: &AssetNameExchange) -> Decimal {
        self.0
            .liquidation_collateral_ratio
            .get(asset)
            .copied()
            .unwrap_or(Decimal::ONE)
    }

    /// This configuration, with `asset`'s
    /// [`borrow_collateral_ratio`](Self::with_borrow_collateral_ratio) and
    /// [`liquidation_collateral_ratio`](Self::with_liquidation_collateral_ratio) both replaced,
    /// checked together once both are set.
    ///
    /// Setting the two one after the other checks the pair after each step, against the other
    /// ratio's old value, so it can refuse a pair that is valid as a whole: raising a 0.5 borrow
    /// ratio and a 0.6 liquidation ratio to 0.8 and 0.9 fails if the borrow ratio goes first, and
    /// lowering them back fails the other way round. This setter does not depend on the order.
    ///
    /// A ratio of one removes it, as either setter does.
    ///
    /// # Errors
    /// [`SpotMarginConfigError::CollateralRatioOutOfRange`] if either ratio is outside `[0, 1]`,
    /// and [`SpotMarginConfigError::LiquidationRatioBelowBorrowRatio`] if `liquidation` is below
    /// `borrow`.
    pub fn with_collateral_ratios(
        mut self,
        asset: AssetNameExchange,
        borrow: Decimal,
        liquidation: Decimal,
    ) -> Result<Self, SpotMarginConfigError> {
        self.0.borrow_collateral_ratio.insert(asset.clone(), borrow);
        self.0
            .liquidation_collateral_ratio
            .insert(asset, liquidation);
        Self::try_from(self.0)
    }

    /// This configuration, valuing each asset over the trailing `window` of its price when the
    /// venue decides how much it lends. Replaces any window it had. See [`SpotMarginConfig`]'s
    /// `# Borrow price window`.
    ///
    /// Off by default, when each asset is valued at its current price.
    ///
    /// # Errors
    /// [`SpotMarginConfigError::BorrowPriceWindowInvalid`] if `window` is not a positive whole
    /// number of milliseconds, the unit it is serialised in.
    pub fn with_borrow_price_window(
        mut self,
        window: TimeDelta,
    ) -> Result<Self, SpotMarginConfigError> {
        let millis = window.num_milliseconds();
        if window <= TimeDelta::zero() || TimeDelta::milliseconds(millis) != window {
            return Err(SpotMarginConfigError::BorrowPriceWindowInvalid(window));
        }
        self.0.borrow_price_window_ms = Some(millis);
        Self::try_from(self.0)
    }

    /// How far back the venue looks at each asset's price when it decides how much it lends, or
    /// `None` if it values each at its current price. See
    /// [`with_borrow_price_window`](Self::with_borrow_price_window).
    pub fn borrow_price_window(&self) -> Option<TimeDelta> {
        // Validated positive, so within what a `TimeDelta` holds.
        self.0.borrow_price_window_ms.map(TimeDelta::milliseconds)
    }

    /// This configuration, liquidating the account as `liquidation` says. Replaces any
    /// liquidation it had.
    ///
    /// # Errors
    /// Returns [`SpotMarginConfigError::LiquidationWithinLeverage`] if a borrow the leverage limit
    /// allows could leave the account at the liquidation level: if `max_leverage` is above one
    /// and the level is at least `max_leverage / (max_leverage − 1)`, the margin level of an
    /// account borrowed to the limit.
    pub fn with_liquidation(
        mut self,
        liquidation: SpotMarginLiquidation,
    ) -> Result<Self, SpotMarginConfigError> {
        self.0.liquidation = Some(liquidation);
        Self::try_from(self.0)
    }

    /// When the venue liquidates the account, or `None` if it never does. See
    /// [`with_liquidation`](Self::with_liquidation).
    pub fn liquidation(&self) -> Option<SpotMarginLiquidation> {
        self.0.liquidation
    }
}

impl TryFrom<SpotMarginRepr> for SpotMarginConfig {
    type Error = SpotMarginConfigError;

    fn try_from(mut repr: SpotMarginRepr) -> Result<Self, Self::Error> {
        if repr.max_leverage < Decimal::ONE {
            return Err(SpotMarginConfigError::LeverageBelowOne(repr.max_leverage));
        }
        if let Some((asset, min_borrow)) = repr
            .min_borrow
            .iter()
            .find(|(_, min_borrow)| **min_borrow < Decimal::ZERO)
        {
            return Err(SpotMarginConfigError::NegativeMinBorrow {
                asset: asset.clone(),
                min_borrow: *min_borrow,
            });
        }
        if let Some((asset, max_borrow)) = repr
            .max_borrow
            .iter()
            .find(|(_, max_borrow)| **max_borrow < Decimal::ZERO)
        {
            return Err(SpotMarginConfigError::NegativeMaxBorrow {
                asset: asset.clone(),
                max_borrow: *max_borrow,
            });
        }
        if let Some((asset, ratio)) = repr
            .borrow_collateral_ratio
            .iter()
            .chain(&repr.liquidation_collateral_ratio)
            .find(|(_, ratio)| !(Decimal::ZERO..=Decimal::ONE).contains(*ratio))
        {
            return Err(SpotMarginConfigError::CollateralRatioOutOfRange {
                asset: asset.clone(),
                ratio: *ratio,
            });
        }
        let ratio_of = |ratios: &BTreeMap<AssetNameExchange, Decimal>, asset| {
            ratios.get(asset).copied().unwrap_or(Decimal::ONE)
        };
        if let Some(asset) = repr
            .borrow_collateral_ratio
            .keys()
            .chain(repr.liquidation_collateral_ratio.keys())
            .find(|asset| {
                ratio_of(&repr.liquidation_collateral_ratio, *asset)
                    < ratio_of(&repr.borrow_collateral_ratio, *asset)
            })
        {
            return Err(SpotMarginConfigError::LiquidationRatioBelowBorrowRatio {
                asset: asset.clone(),
                borrow: ratio_of(&repr.borrow_collateral_ratio, asset),
                liquidation: ratio_of(&repr.liquidation_collateral_ratio, asset),
            });
        }
        if let Some(millis) = repr.borrow_price_window_ms.filter(|millis| *millis <= 0) {
            // Only `i64::MIN` milliseconds is beyond a `TimeDelta`, whose range is symmetric; it
            // is reported as the most negative one there is.
            let window = TimeDelta::try_milliseconds(millis).unwrap_or(TimeDelta::MIN);
            return Err(SpotMarginConfigError::BorrowPriceWindowInvalid(window));
        }
        if let Some(liquidation) = &repr.liquidation {
            // An account borrowed to the limit has `debt = (max_leverage − 1) × equity`, so a
            // margin level of `max_leverage / (max_leverage − 1)`. One lending nothing, at a
            // `max_leverage` of one, has no limit to reach, and passes: the product is zero.
            if liquidation.level() * (repr.max_leverage - Decimal::ONE) >= repr.max_leverage {
                return Err(SpotMarginConfigError::LiquidationWithinLeverage {
                    level: liquidation.level(),
                    max_leverage: repr.max_leverage,
                });
            }
        }
        // A zero minimum is no minimum, so it is not kept: two configurations that lend alike
        // compare equal.
        repr.min_borrow
            .retain(|_, min_borrow| !min_borrow.is_zero());
        // Likewise a ratio of one, which counts the asset in full.
        repr.borrow_collateral_ratio
            .retain(|_, ratio| *ratio != Decimal::ONE);
        repr.liquidation_collateral_ratio
            .retain(|_, ratio| *ratio != Decimal::ONE);
        Ok(Self(repr))
    }
}

impl From<SpotMarginConfig> for SpotMarginRepr {
    fn from(config: SpotMarginConfig) -> Self {
        config.0
    }
}

/// Why a [`SpotMarginConfig`] was refused.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum SpotMarginConfigError {
    /// A `max_leverage` below one, which would allow less than the account's own equity.
    #[error("spot margin max_leverage {0} is below 1; a leverage of 1 lends nothing")]
    LeverageBelowOne(Decimal),
    /// A negative minimum borrow for `asset`.
    #[error("spot margin min_borrow {min_borrow} of {asset} is negative")]
    #[non_exhaustive]
    NegativeMinBorrow {
        /// The asset given the minimum.
        asset: AssetNameExchange,
        /// The minimum given, below zero.
        min_borrow: Decimal,
    },
    /// A negative borrow cap for `asset`.
    #[error("spot margin max_borrow {max_borrow} of {asset} is negative")]
    #[non_exhaustive]
    NegativeMaxBorrow {
        /// The asset given the cap.
        asset: AssetNameExchange,
        /// The cap given, below zero.
        max_borrow: Decimal,
    },
    /// A collateral ratio for `asset` outside `[0, 1]`.
    #[error("spot margin collateral ratio {ratio} of {asset} is outside [0, 1]")]
    #[non_exhaustive]
    CollateralRatioOutOfRange {
        /// The asset given the ratio.
        asset: AssetNameExchange,
        /// The ratio given.
        ratio: Decimal,
    },
    /// A liquidation collateral ratio for `asset` below its borrow collateral ratio, which would
    /// count the asset for less when liquidating than when lending.
    #[error(
        "spot margin liquidation collateral ratio {liquidation} of {asset} is below its borrow \
         collateral ratio {borrow}"
    )]
    #[non_exhaustive]
    LiquidationRatioBelowBorrowRatio {
        /// The asset whose ratios disagree.
        asset: AssetNameExchange,
        /// Its borrow collateral ratio.
        borrow: Decimal,
        /// Its liquidation collateral ratio, below `borrow`.
        liquidation: Decimal,
    },
    /// A borrow price window that is not a positive whole number of milliseconds, the unit it is
    /// serialised in.
    #[error("spot margin borrow price window {0} is not a positive whole number of milliseconds")]
    BorrowPriceWindowInvalid(TimeDelta),
    /// A liquidation level below one, at which an account would already owe more than it holds.
    #[error("spot margin liquidation level {0} is below 1")]
    LiquidationLevelBelowOne(Decimal),
    /// A liquidation fee rate outside `[0, 1)`.
    #[error("spot margin liquidation fee rate {0} is outside [0, 1)")]
    LiquidationFeeOutOfRange(Decimal),
    /// A margin-call level not above the liquidation level, so it could never be reached first.
    #[error(
        "spot margin margin-call level {margin_call} is not above the liquidation level {level}"
    )]
    #[non_exhaustive]
    MarginCallNotAboveLiquidation {
        /// The margin-call level given.
        margin_call: Decimal,
        /// The liquidation level it must be above.
        level: Decimal,
    },
    /// A liquidation level an account borrowed to the leverage limit would already be at.
    #[error(
        "spot margin liquidation level {level} is reached by a borrow max_leverage {max_leverage} \
         allows; it must be below max_leverage / (max_leverage - 1)"
    )]
    #[non_exhaustive]
    LiquidationWithinLeverage {
        /// The liquidation level given.
        level: Decimal,
        /// The configuration's leverage limit.
        max_leverage: Decimal,
    },
}

/// When the simulated venue liquidates a spot margin account, what it charges for it, and whether
/// it warns first. Set by [`SpotMarginConfig::with_liquidation`].
///
/// # When
/// The venue checks the account's **margin level** after each of its inputs, a market tick or a
/// move of its clock, since those are what change it: prices move, and interest falls due. The
/// level is the value of everything the account holds over the value of what it owes, loans plus
/// interest, both valued as the leverage limit values them (see [`SpotMarginConfig`]'s
/// `# How much it lends`): at the mid of a spot instrument pairing each asset with the valuation
/// asset, else its last price. What it holds of an asset beyond what it owes of it counts at the
/// asset's [`liquidation_collateral_ratio`](SpotMarginConfig::liquidation_collateral_ratio), one
/// by default (see [`SpotMarginConfig`]'s `# Collateral ratios`). There is no index price, so a
/// market's own mid stands in for the one a venue liquidates on. An asset the venue cannot value counts for nothing, which can bring
/// a liquidation forward but never put one off. While it cannot value a debt it does not check,
/// and warns once.
///
/// At or below [`level`](Self::level) it liquidates.
///
/// # How
/// It sends an [`AccountNotice`](crate::notice::AccountNotice) of
/// [`NoticeKind::Liquidation`](crate::notice::NoticeKind::Liquidation), then closes every debt:
/// 1. It cancels every open order, resting or held, releasing what each held.
/// 2. It repays each debt from the asset's own free balance, interest first.
/// 3. It repays what is still owed of the valuation asset with the proceeds of selling the
///    account's other assets, each on a spot instrument pairing it with the valuation asset, in
///    name order, as much of each as is needed.
/// 4. It buys back what is still owed of each other asset, in name order, on a spot instrument
///    pairing it with the valuation asset, selling other assets for the valuation asset first as
///    in step 3 when the account holds too little of it. What a buy delivers repays the debt.
/// 5. It charges the fee: [`fee_rate`](Self::fee_rate) times the value of the debt this repaid,
///    in the valuation asset, raised as in step 3 if the account holds too little of it.
///
/// Each forced trade fills as a market order would on arrival, as a taker: priced by the venue's
/// fill model at its current market, slipped, limited by the depth on offer, and charged the
/// venue's fee. It is reported as `[balance.., trade]`, the [`Trade`](crate::trade::Trade)
/// carrying [`TradeOrigin::Liquidation`](crate::trade::TradeOrigin::Liquidation), no client order
/// id and an unknown strategy, with no order snapshot, since no client placed it. It is never
/// held for a bar or a fresh market. Once priced it has drawn from the venue's slippage stream,
/// even if it then trades nothing, so a liquidation that runs over several inputs draws on each.
/// The fee is reported as `[balance, cash flow]`, a
/// [`CashFlowKind::LiquidationFee`](crate::cash_flow::CashFlowKind::LiquidationFee) with no
/// instrument.
///
/// # When it cannot close every debt
/// A debt can outlast an input when the depth on offer is too thin, when an asset has no market
/// to trade on, or when the account holds too little to repay it. The venue then warns once, and
/// leaves the rest owed: it has no insurance fund, and nothing floors the account's balances.
///
/// The liquidation continues on each later input until no debt is left, without another notice:
/// each input cancels whatever the account has since placed, repays what it can, and charges the
/// fee on what it repaid.
///
/// Each pass's fee is charged out of the account's remaining assets after that pass's
/// repayments, selling for it as in step 3, as a venue takes its fee out of the proceeds of the
/// liquidation's trades. So a debt a pass leaves owed has that much less to be repaid from on the
/// next pass. A fee the account cannot pay takes the valuation asset's free balance below zero.
///
/// While the liquidation is under way the venue lends nothing. An order that would need a loan is
/// rejected as unaffordable, its reason showing nothing borrowable, and so is the fill of a held
/// order that would need one. One the account can pay for from its own balance is accepted, and
/// cancelled by the next input if it is still open. A debt the venue cannot trade out of, for want
/// of a market or of anything left to sell, keeps lending off until the account repays it.
///
/// # Margin call
/// Off by default. With [`with_margin_call`](Self::with_margin_call), the venue sends
/// [`NoticeKind::MarginCall`](crate::notice::NoticeKind::MarginCall) when the margin level falls
/// to or below that level, and
/// [`NoticeKind::MarginRestored`](crate::notice::NoticeKind::MarginRestored) when it rises back
/// above it or the account no longer owes anything, once per crossing. A liquidation ends a margin
/// call without a notice.
///
/// Each notice carries the margin level that caused it, `None` for an account that owes nothing,
/// and its kind's name as its status.
///
/// # Not liquidated
/// A CFD position: its margin is posted from the quote asset, outside the spot margin account.
///
/// # Serialised form
/// `level`, `fee_rate` and `margin_call_level` are decimal strings, and `margin_call_level` is
/// left out when off. Deserialising refuses what the constructors refuse.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize)]
#[serde(
    try_from = "SpotMarginLiquidationRepr",
    into = "SpotMarginLiquidationRepr"
)]
pub struct SpotMarginLiquidation(SpotMarginLiquidationRepr);

/// The serialised form of a [`SpotMarginLiquidation`], before validation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize)]
struct SpotMarginLiquidationRepr {
    #[serde(with = "rust_decimal::serde::str")]
    level: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    fee_rate: Decimal,
    #[serde(
        default,
        with = "rust_decimal::serde::str_option",
        skip_serializing_if = "Option::is_none"
    )]
    margin_call_level: Option<Decimal>,
}

impl SpotMarginLiquidation {
    /// Liquidation at a margin level of `level` or below, charging `fee_rate` of the debt it
    /// repays, with no margin call.
    ///
    /// For example, a level of 1.1 and a fee rate of 0.02 liquidate an account whose holdings are
    /// worth 1.1 times its debt or less, and charge 2 % of what it repays.
    ///
    /// # Errors
    /// [`SpotMarginConfigError::LiquidationLevelBelowOne`] for a `level` below one, and
    /// [`SpotMarginConfigError::LiquidationFeeOutOfRange`] for a `fee_rate` below zero or not
    /// below one.
    pub fn new(level: Decimal, fee_rate: Decimal) -> Result<Self, SpotMarginConfigError> {
        Self::try_from(SpotMarginLiquidationRepr {
            level,
            fee_rate,
            margin_call_level: None,
        })
    }

    /// This liquidation, with a margin call at `margin_call_level`. See `# Margin call`.
    ///
    /// # Errors
    /// [`SpotMarginConfigError::MarginCallNotAboveLiquidation`] if `margin_call_level` is not
    /// above the liquidation [`level`](Self::level).
    pub fn with_margin_call(
        mut self,
        margin_call_level: Decimal,
    ) -> Result<Self, SpotMarginConfigError> {
        self.0.margin_call_level = Some(margin_call_level);
        Self::try_from(self.0)
    }

    /// The margin level at or below which the venue liquidates.
    pub fn level(&self) -> Decimal {
        self.0.level
    }

    /// The fee, as a fraction of the value of the debt a liquidation repays.
    pub fn fee_rate(&self) -> Decimal {
        self.0.fee_rate
    }

    /// The margin level at or below which the venue sends a margin call, or `None` for none.
    pub fn margin_call_level(&self) -> Option<Decimal> {
        self.0.margin_call_level
    }
}

impl TryFrom<SpotMarginLiquidationRepr> for SpotMarginLiquidation {
    type Error = SpotMarginConfigError;

    fn try_from(repr: SpotMarginLiquidationRepr) -> Result<Self, Self::Error> {
        if repr.level < Decimal::ONE {
            return Err(SpotMarginConfigError::LiquidationLevelBelowOne(repr.level));
        }
        if repr.fee_rate < Decimal::ZERO || repr.fee_rate >= Decimal::ONE {
            return Err(SpotMarginConfigError::LiquidationFeeOutOfRange(
                repr.fee_rate,
            ));
        }
        if let Some(margin_call) = repr.margin_call_level
            && margin_call <= repr.level
        {
            return Err(SpotMarginConfigError::MarginCallNotAboveLiquidation {
                margin_call,
                level: repr.level,
            });
        }
        Ok(Self(repr))
    }
}

impl From<SpotMarginLiquidation> for SpotMarginLiquidationRepr {
    fn from(liquidation: SpotMarginLiquidation) -> Self {
        liquidation.0
    }
}

/// Charges interest on a simulated margin loan: once when the loan is taken, and at boundaries in
/// time while it stays open.
///
/// The simulated venue asks for a charge on each loan its spot margin takes
/// ([`InterestDue::Borrow`], on the amount just lent), and, through
/// [`next_boundary`](Self::next_boundary), at each boundary its clock reaches while the account
/// owes the asset ([`InterestDue::Boundary`], on the principal owed when it falls). Each charge is
/// added to the asset's loan as interest, which the account repays before principal, and is
/// reported as a [`CashFlowKind::MarginInterest`](crate::cash_flow::CashFlowKind::MarginInterest)
/// in the borrowed asset.
///
/// Interest is charged on principal only: interest already owed earns none.
///
/// # Contract
/// - `next_boundary` must return an instant strictly after `after`, or `None` when the model
///   never charges the asset at a boundary. A boundary at or before `after` would charge one
///   instant forever, so the venue **panics** on one.
/// - A charge's [`interest`](InterestCharge::interest) must not be negative: interest is owed,
///   never paid out. The venue **panics** on a negative one.
/// - Both methods must be pure functions of their arguments: the venue may ask for the same
///   boundary more than once, and charges it once.
///
/// In return, the venue asks [`charge`](Self::charge) only about a positive `principal`: an order
/// that lends nothing, and an asset that owes nothing, is never charged.
///
/// `Send + Sync` because a venue holds its model behind an `Arc`, so that one model, and the
/// rate history it carries, can be shared by every run of a backtest sweep without being copied.
pub trait MarginInterestModel: Debug + Send + Sync {
    /// The first boundary strictly after `after` at which this model charges a loan of `asset`,
    /// or `None` if it never does.
    fn next_boundary(
        &self,
        asset: &AssetNameExchange,
        after: DateTime<Utc>,
    ) -> Option<DateTime<Utc>>;

    /// What a loan of `principal` of `asset` is charged when `due`, or `None` for nothing.
    fn charge(
        &self,
        asset: &AssetNameExchange,
        principal: Decimal,
        due: InterestDue,
    ) -> Option<InterestCharge>;
}

/// When a [`MarginInterestModel`] is asked to charge a loan.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum InterestDue {
    /// The loan was taken at this instant, and the principal is the amount just lent.
    Borrow(DateTime<Utc>),
    /// A boundary the model named, and the principal is what the account owed when it fell.
    Boundary(DateTime<Utc>),
}

impl InterestDue {
    /// The instant the charge is due.
    pub fn time(self) -> DateTime<Utc> {
        match self {
            Self::Borrow(time) | Self::Boundary(time) => time,
        }
    }
}

/// One charge a [`MarginInterestModel`] makes on one loan.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InterestCharge {
    /// The interest owed, in the borrowed asset. Never negative; zero charges nothing.
    pub interest: Decimal,
    /// The rate it was charged at, reported as the cash flow's `rate` and used for nothing else,
    /// so a model may quote it annualised or per its own period. [`PeriodicMarginInterest`]
    /// reports the annualised rate.
    pub rate: Option<Decimal>,
}

impl InterestCharge {
    /// A charge of `interest`, in the borrowed asset, at `rate`.
    pub fn new(interest: Decimal, rate: Option<Decimal>) -> Self {
        Self { interest, rate }
    }
}

/// When a [`PeriodicMarginInterest`] charges a loan.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum InterestSchedule {
    /// One period when the loan is taken, then one at every boundary while it is open.
    #[default]
    AtBorrowAndEachBoundary,
    /// One period when the loan is taken, and nothing afterwards however long it is held.
    AtBorrowOnly,
}

/// Margin interest charged a period at a time, at an annualised rate per asset: a crypto margin
/// account's hourly interest.
///
/// - **When.** One period when a loan is taken, whenever in the period that is, then one at every
///   boundary while the loan is open, unless [`InterestSchedule::AtBorrowOnly`]. Boundaries fall
///   at whole multiples of the period since the Unix epoch, so an hourly period charges on the
///   hour, UTC. Each charge pays in advance for the period it falls in: the one a borrow is taken
///   in, or the one a boundary begins. So a loan repaid mid-period has already paid for that
///   period, and one repaid just before a boundary is not charged there.
/// - **How much.** `principal × rate × period / basis`, where the rate is annualised, read from
///   the asset's [`RateSeries`] at the instant charged, and the basis is the [`DayCount`]'s year.
///   Each charge is rounded **up** to the asset's unit, `1e-8` unless set by
///   [`with_unit`](Self::with_unit), so a small loan is charged at least one unit, as a venue
///   that books interest in whole units charges it.
/// - **Which assets.** Only those given rates. A loan of any other asset, or one charged before its
///   series begins, is charged nothing.
///
/// A venue that quotes a daily rate is reproduced by an annual rate of `daily × 365` on
/// [`DayCount::Act365`].
#[derive(Debug, Clone)]
pub struct PeriodicMarginInterest {
    period: TimeDelta,
    day_count: DayCount,
    schedule: InterestSchedule,
    rates: FnvHashMap<AssetNameExchange, RateSeries>,
    units: FnvHashMap<AssetNameExchange, Decimal>,
}

/// The unit a [`PeriodicMarginInterest`] rounds an asset's charge up to unless told otherwise: one
/// satoshi, the unit crypto venues book interest in.
const DEFAULT_INTEREST_UNIT: Decimal = Decimal::from_parts(1, 0, 0, false, 8);

impl PeriodicMarginInterest {
    /// Interest charged every `period` on the `day_count` basis, at borrow and each boundary, with
    /// no asset's rates yet.
    ///
    /// # Panics
    /// If `period` is not positive.
    pub fn new(period: TimeDelta, day_count: DayCount) -> Self {
        assert!(
            period > TimeDelta::zero(),
            "PeriodicMarginInterest period must be positive, got {period}"
        );
        Self {
            period,
            day_count,
            schedule: InterestSchedule::default(),
            rates: FnvHashMap::default(),
            units: FnvHashMap::default(),
        }
    }

    /// Charges a loan of `asset` at the annualised rates in `rates`.
    ///
    /// # Panics
    /// If any rate in `rates` is negative: a borrower is never paid interest, so a negative rate is
    /// a fault in the data, refused here rather than when the venue first charges it.
    #[must_use]
    pub fn with_rates(mut self, asset: AssetNameExchange, rates: RateSeries) -> Self {
        if let Some(rate) = rates.rates().find(|rate| *rate < Decimal::ZERO) {
            panic!("PeriodicMarginInterest rate for {asset} must not be negative, got {rate}");
        }
        self.rates.insert(asset, rates);
        self
    }

    /// Rounds each charge on `asset` up to a multiple of `unit` instead of `1e-8`.
    ///
    /// # Panics
    /// If `unit` is not positive.
    #[must_use]
    pub fn with_unit(mut self, asset: AssetNameExchange, unit: Decimal) -> Self {
        assert!(
            unit > Decimal::ZERO,
            "PeriodicMarginInterest unit for {asset} must be positive, got {unit}"
        );
        self.units.insert(asset, unit);
        self
    }

    /// Charges on `schedule` instead of at borrow and each boundary.
    #[must_use]
    pub fn with_schedule(mut self, schedule: InterestSchedule) -> Self {
        self.schedule = schedule;
        self
    }
}

impl MarginInterestModel for PeriodicMarginInterest {
    fn next_boundary(
        &self,
        asset: &AssetNameExchange,
        after: DateTime<Utc>,
    ) -> Option<DateTime<Utc>> {
        match self.schedule {
            InterestSchedule::AtBorrowOnly => None,
            InterestSchedule::AtBorrowAndEachBoundary if !self.rates.contains_key(asset) => None,
            InterestSchedule::AtBorrowAndEachBoundary => next_whole_interval(self.period, after),
        }
    }

    fn charge(
        &self,
        asset: &AssetNameExchange,
        principal: Decimal,
        due: InterestDue,
    ) -> Option<InterestCharge> {
        let rate = self.rates.get(asset)?.at(due.time())?;
        let unit = self
            .units
            .get(asset)
            .copied()
            .unwrap_or(DEFAULT_INTEREST_UNIT);
        let exact = self.day_count.accrue_span(principal * rate, self.period);

        Some(InterestCharge::new(
            (exact / unit).ceil() * unit,
            Some(rate),
        ))
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics are the correct failure mode
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    /// A placement's loan is its order's, at its state's time, when the answer reports a loan and
    /// the state names a venue order id, and none otherwise.
    #[test]
    fn a_placement_s_loan_is_its_order_s_when_its_state_names_a_venue_id() {
        use crate::{
            error::{ApiError, OrderError},
            order::{
                OrderKey, OrderKind, TimeInForce,
                id::{ClientOrderId, OrderId, StrategyId, VenueOrderId},
                state::{Cancelled, Expired, Filled, Open, OpenInFlight},
            },
        };
        use chrono::TimeZone;
        use rustrade_instrument::Side;

        let time = Utc.with_ymd_and_hms(2026, 11, 2, 10, 5, 0).unwrap();
        let id = OrderId::new("7");
        let borrowed = MarginBorrow::new(AssetNameExchange::new("USDT"), dec!(6.23292076));
        let placement =
            |state: UnindexedOrderState, borrowed: Option<MarginBorrow>| MarginPlacement {
                order: Order {
                    key: OrderKey {
                        exchange: ExchangeId::BinanceMargin,
                        instrument: InstrumentNameExchange::new("BTCUSDT"),
                        strategy: StrategyId::new("strategy"),
                        cid: ClientOrderId::new("borrow"),
                    },
                    side: Side::Buy,
                    price: None,
                    quantity: dec!(1),
                    kind: OrderKind::Market,
                    time_in_force: TimeInForce::ImmediateOrCancel,
                    state,
                },
                borrowed,
            };
        let loan = Some(OrderLoan::new(
            InstrumentNameExchange::new("BTCUSDT"),
            borrowed.clone(),
            CashFlowLoan::new(time, id.clone(), ClientOrderId::new("borrow")),
        ));

        for (case, state, expected) in [
            (
                "open",
                OrderState::active(Open::new(
                    VenueOrderId::Assigned(id.clone()),
                    time,
                    Decimal::ZERO,
                )),
                loan.clone(),
            ),
            (
                "filled",
                OrderState::fully_filled(Filled::new(id.clone(), time, dec!(1), None)),
                loan.clone(),
            ),
            (
                "cancelled",
                OrderState::inactive(Cancelled::new(id.clone(), time, None)),
                loan.clone(),
            ),
            (
                "expired",
                OrderState::expired(Expired::new(id.clone(), time, None)),
                loan.clone(),
            ),
            (
                "open without a venue id",
                OrderState::active(Open::new(VenueOrderId::ClientAssigned, time, Decimal::ZERO)),
                None,
            ),
            (
                "in flight",
                OrderState::active(OpenInFlight::new(time)),
                None,
            ),
            (
                "refused",
                OrderState::inactive(OrderError::Rejected(ApiError::OrderRejected(
                    "refused".to_owned(),
                ))),
                None,
            ),
        ] {
            assert_eq!(
                placement(state.clone(), Some(borrowed.clone())).loan(),
                expected,
                "{case}"
            );
            assert_eq!(
                placement(state, None).loan(),
                None,
                "{case}, no loan reported"
            );
        }
    }

    #[test]
    fn a_leverage_below_one_is_refused_by_the_constructor_and_by_deserialising() {
        assert_eq!(
            SpotMarginConfig::new(
                AssetNameExchange::new("usdt"),
                dec!(0.5),
                MarginEffect::NoSideEffect
            ),
            Err(SpotMarginConfigError::LeverageBelowOne(dec!(0.5)))
        );

        let json = r#"{"valuation_asset":"usdt","max_leverage":"0.5","margin_effect":"borrow"}"#;
        assert!(serde_json::from_str::<SpotMarginConfig>(json).is_err());
    }

    #[test]
    fn the_margin_effect_has_no_default_and_a_retired_side_effect_is_refused() {
        let omitted = r#"{"valuation_asset":"usdt","max_leverage":"3"}"#;
        assert!(serde_json::from_str::<SpotMarginConfig>(omitted).is_err());

        // The retired key alone is no substitute for the effect.
        let retired =
            r#"{"valuation_asset":"usdt","max_leverage":"3","side_effect":"auto_borrow_repay"}"#;
        assert!(serde_json::from_str::<SpotMarginConfig>(retired).is_err());
    }

    #[test]
    fn auto_repay_at_cancel_defaults_to_true_when_omitted() {
        let json = r#"{"valuation_asset":"usdt","max_leverage":"3","margin_effect":"repay"}"#;
        let config: SpotMarginConfig = serde_json::from_str(json).unwrap();

        assert_eq!(config.margin_effect(), MarginEffect::Repay);
        assert!(config.auto_repay_at_cancel());
        assert_eq!(config.max_leverage(), dec!(3));
        assert_eq!(config.valuation_asset(), &AssetNameExchange::new("usdt"));
    }

    #[test]
    fn a_minimum_borrow_is_per_asset_and_zero_where_none_is_set() {
        let config = SpotMarginConfig::new(
            AssetNameExchange::new("usdt"),
            dec!(3),
            MarginEffect::Borrow,
        )
        .unwrap()
        .with_min_borrow(AssetNameExchange::new("btc"), dec!(0.00006))
        .unwrap();

        assert_eq!(
            config.min_borrow(&AssetNameExchange::new("btc")),
            dec!(0.00006)
        );
        assert_eq!(
            config.min_borrow(&AssetNameExchange::new("usdt")),
            Decimal::ZERO
        );
    }

    #[test]
    fn a_negative_minimum_borrow_is_refused_by_the_builder_and_by_deserialising() {
        let config = SpotMarginConfig::new(
            AssetNameExchange::new("usdt"),
            dec!(3),
            MarginEffect::Borrow,
        )
        .unwrap();
        assert_eq!(
            config.with_min_borrow(AssetNameExchange::new("btc"), dec!(-0.1)),
            Err(SpotMarginConfigError::NegativeMinBorrow {
                asset: AssetNameExchange::new("btc"),
                min_borrow: dec!(-0.1),
            })
        );

        let json = r#"{"valuation_asset":"usdt","max_leverage":"3","margin_effect":"borrow","min_borrow":{"btc":"-0.1"}}"#;
        assert!(serde_json::from_str::<SpotMarginConfig>(json).is_err());
    }

    #[test]
    fn a_zero_minimum_borrow_removes_the_minimum() {
        let plain = SpotMarginConfig::new(
            AssetNameExchange::new("usdt"),
            dec!(3),
            MarginEffect::Borrow,
        )
        .unwrap();
        let cleared = plain
            .clone()
            .with_min_borrow(AssetNameExchange::new("btc"), dec!(5))
            .unwrap()
            .with_min_borrow(AssetNameExchange::new("btc"), Decimal::ZERO)
            .unwrap();

        assert_eq!(cleared, plain);
        assert_eq!(
            cleared.min_borrow(&AssetNameExchange::new("btc")),
            Decimal::ZERO
        );
    }

    #[test]
    fn a_minimum_borrow_round_trips_as_a_map_of_decimal_strings() {
        let config = SpotMarginConfig::new(
            AssetNameExchange::new("usdt"),
            dec!(3),
            MarginEffect::Borrow,
        )
        .unwrap()
        .with_min_borrow(AssetNameExchange::new("btc"), dec!(0.00006))
        .unwrap();

        let json = serde_json::to_string(&config).unwrap();
        assert_eq!(
            json,
            r#"{"valuation_asset":"usdt","max_leverage":"3","margin_effect":"borrow","auto_repay_at_cancel":true,"min_borrow":{"btc":"0.00006"}}"#
        );
        assert_eq!(
            serde_json::from_str::<SpotMarginConfig>(&json).unwrap(),
            config
        );
    }

    fn usdt_margin() -> SpotMarginConfig {
        SpotMarginConfig::new(
            AssetNameExchange::new("usdt"),
            dec!(3),
            MarginEffect::Borrow,
        )
        .unwrap()
    }

    #[test]
    fn a_borrow_cap_is_per_asset_and_none_where_none_is_set() {
        let config = usdt_margin()
            .with_max_borrow(AssetNameExchange::new("btc"), dec!(100))
            .unwrap();

        assert_eq!(
            config.max_borrow(&AssetNameExchange::new("btc")),
            Some(dec!(100))
        );
        assert_eq!(config.max_borrow(&AssetNameExchange::new("usdt")), None);
    }

    #[test]
    fn a_borrow_cap_replaces_the_asset_s_last_and_a_zero_cap_is_kept() {
        let config = usdt_margin()
            .with_max_borrow(AssetNameExchange::new("btc"), dec!(100))
            .unwrap()
            .with_max_borrow(AssetNameExchange::new("btc"), Decimal::ZERO)
            .unwrap();

        assert_eq!(
            config.max_borrow(&AssetNameExchange::new("btc")),
            Some(Decimal::ZERO),
            "a cap of zero lends none of the asset, which no cap would not say"
        );
        assert_ne!(config, usdt_margin());
    }

    #[test]
    fn a_negative_borrow_cap_is_refused_by_the_builder_and_by_deserialising() {
        assert_eq!(
            usdt_margin().with_max_borrow(AssetNameExchange::new("btc"), dec!(-0.1)),
            Err(SpotMarginConfigError::NegativeMaxBorrow {
                asset: AssetNameExchange::new("btc"),
                max_borrow: dec!(-0.1),
            })
        );

        let json = r#"{"valuation_asset":"usdt","max_leverage":"3","margin_effect":"borrow","max_borrow":{"btc":"-0.1"}}"#;
        assert!(serde_json::from_str::<SpotMarginConfig>(json).is_err());
    }

    #[test]
    fn a_borrow_cap_round_trips_as_a_map_of_decimal_strings_left_out_when_empty() {
        let config = usdt_margin()
            .with_max_borrow(AssetNameExchange::new("btc"), dec!(150.5))
            .unwrap();

        let json = serde_json::to_string(&config).unwrap();
        assert_eq!(
            json,
            r#"{"valuation_asset":"usdt","max_leverage":"3","margin_effect":"borrow","auto_repay_at_cancel":true,"max_borrow":{"btc":"150.5"}}"#
        );
        assert_eq!(
            serde_json::from_str::<SpotMarginConfig>(&json).unwrap(),
            config
        );
        assert!(
            !serde_json::to_string(&usdt_margin())
                .unwrap()
                .contains("max_borrow")
        );
    }

    #[test]
    fn collateral_ratios_are_per_asset_and_one_where_none_is_set() {
        let config = usdt_margin()
            .with_borrow_collateral_ratio(btc(), dec!(0.95))
            .unwrap()
            .with_liquidation_collateral_ratio(btc(), dec!(0.97))
            .unwrap();

        assert_eq!(config.borrow_collateral_ratio(&btc()), dec!(0.95));
        assert_eq!(config.liquidation_collateral_ratio(&btc()), dec!(0.97));
        let usdt = AssetNameExchange::new("usdt");
        assert_eq!(config.borrow_collateral_ratio(&usdt), Decimal::ONE);
        assert_eq!(config.liquidation_collateral_ratio(&usdt), Decimal::ONE);
    }

    #[test]
    fn a_ratio_of_one_is_no_ratio_and_zero_is_kept() {
        let config = usdt_margin()
            .with_borrow_collateral_ratio(btc(), dec!(0.5))
            .unwrap()
            .with_borrow_collateral_ratio(btc(), dec!(1.0))
            .unwrap();
        assert_eq!(
            config,
            usdt_margin(),
            "a ratio of one counts the asset in full"
        );

        let zero = usdt_margin()
            .with_borrow_collateral_ratio(btc(), Decimal::ZERO)
            .unwrap()
            .with_liquidation_collateral_ratio(btc(), Decimal::ZERO)
            .unwrap();
        assert_eq!(zero.borrow_collateral_ratio(&btc()), Decimal::ZERO);
        assert_eq!(zero.liquidation_collateral_ratio(&btc()), Decimal::ZERO);
    }

    #[test]
    fn a_ratio_outside_zero_to_one_is_refused_by_the_builders_and_by_deserialising() {
        for ratio in [dec!(-0.01), dec!(1.01)] {
            let refused = SpotMarginConfigError::CollateralRatioOutOfRange {
                asset: btc(),
                ratio,
            };
            assert_eq!(
                usdt_margin().with_borrow_collateral_ratio(btc(), ratio),
                Err(refused.clone())
            );
            assert_eq!(
                usdt_margin().with_liquidation_collateral_ratio(btc(), ratio),
                Err(refused)
            );
        }

        for key in ["borrow_collateral_ratio", "liquidation_collateral_ratio"] {
            let json = format!(
                r#"{{"valuation_asset":"usdt","max_leverage":"3","margin_effect":"borrow","{key}":{{"btc":"1.5"}}}}"#
            );
            assert!(
                serde_json::from_str::<SpotMarginConfig>(&json).is_err(),
                "{key}"
            );
        }
    }

    #[test]
    fn a_liquidation_ratio_below_the_borrow_ratio_is_refused_whichever_is_set_last() {
        let below = SpotMarginConfigError::LiquidationRatioBelowBorrowRatio {
            asset: btc(),
            borrow: Decimal::ONE,
            liquidation: dec!(0.9),
        };
        assert_eq!(
            usdt_margin().with_liquidation_collateral_ratio(btc(), dec!(0.9)),
            Err(below),
            "the borrow ratio is one until set"
        );

        let config = usdt_margin()
            .with_borrow_collateral_ratio(btc(), dec!(0.9))
            .unwrap()
            .with_liquidation_collateral_ratio(btc(), dec!(0.9))
            .unwrap();
        assert_eq!(
            config.with_borrow_collateral_ratio(btc(), dec!(0.95)),
            Err(SpotMarginConfigError::LiquidationRatioBelowBorrowRatio {
                asset: btc(),
                borrow: dec!(0.95),
                liquidation: dec!(0.9),
            })
        );

        let json = r#"{"valuation_asset":"usdt","max_leverage":"3","margin_effect":"borrow","liquidation_collateral_ratio":{"btc":"0.9"}}"#;
        assert!(serde_json::from_str::<SpotMarginConfig>(json).is_err());
    }

    #[test]
    fn with_collateral_ratios_sets_both_where_setting_them_in_turn_fails_in_one_order() {
        let low = usdt_margin()
            .with_collateral_ratios(btc(), dec!(0.5), dec!(0.6))
            .unwrap();
        assert_eq!(low.borrow_collateral_ratio(&btc()), dec!(0.5));
        assert_eq!(low.liquidation_collateral_ratio(&btc()), dec!(0.6));
        assert!(
            low.clone()
                .with_borrow_collateral_ratio(btc(), dec!(0.8))
                .is_err(),
            "raising the borrow ratio first passes the old liquidation ratio"
        );

        let high = low
            .with_collateral_ratios(btc(), dec!(0.8), dec!(0.9))
            .unwrap();
        assert_eq!(high.borrow_collateral_ratio(&btc()), dec!(0.8));
        assert_eq!(high.liquidation_collateral_ratio(&btc()), dec!(0.9));
        assert!(
            high.clone()
                .with_liquidation_collateral_ratio(btc(), dec!(0.6))
                .is_err(),
            "lowering the liquidation ratio first passes the old borrow ratio"
        );

        let back = high
            .with_collateral_ratios(btc(), dec!(0.5), dec!(0.6))
            .unwrap();
        assert_eq!(back.borrow_collateral_ratio(&btc()), dec!(0.5));
        assert_eq!(back.liquidation_collateral_ratio(&btc()), dec!(0.6));
        assert_eq!(
            back.with_collateral_ratios(btc(), Decimal::ONE, dec!(1.0)),
            Ok(usdt_margin()),
            "ratios of one count the asset in full"
        );
    }

    #[test]
    fn with_collateral_ratios_refuses_what_either_setter_refuses() {
        for (borrow, liquidation, ratio) in [
            (dec!(-0.01), dec!(0.5), dec!(-0.01)),
            (dec!(0.5), dec!(1.01), dec!(1.01)),
        ] {
            assert_eq!(
                usdt_margin().with_collateral_ratios(btc(), borrow, liquidation),
                Err(SpotMarginConfigError::CollateralRatioOutOfRange {
                    asset: btc(),
                    ratio,
                })
            );
        }
        assert_eq!(
            usdt_margin().with_collateral_ratios(btc(), dec!(0.9), dec!(0.8)),
            Err(SpotMarginConfigError::LiquidationRatioBelowBorrowRatio {
                asset: btc(),
                borrow: dec!(0.9),
                liquidation: dec!(0.8),
            })
        );
    }

    #[test]
    fn collateral_ratios_round_trip_as_maps_of_decimal_strings_left_out_when_empty() {
        let config = usdt_margin()
            .with_borrow_collateral_ratio(btc(), dec!(0.95))
            .unwrap()
            .with_borrow_collateral_ratio(AssetNameExchange::new("eth"), dec!(0.8))
            .unwrap()
            .with_liquidation_collateral_ratio(btc(), dec!(0.99))
            .unwrap();

        let json = serde_json::to_string(&config).unwrap();
        assert_eq!(
            json,
            r#"{"valuation_asset":"usdt","max_leverage":"3","margin_effect":"borrow","auto_repay_at_cancel":true,"borrow_collateral_ratio":{"btc":"0.95","eth":"0.8"},"liquidation_collateral_ratio":{"btc":"0.99"}}"#
        );
        assert_eq!(
            serde_json::from_str::<SpotMarginConfig>(&json).unwrap(),
            config
        );
        let plain = serde_json::to_string(&usdt_margin()).unwrap();
        assert!(!plain.contains("collateral_ratio"), "{plain}");
    }

    #[test]
    fn a_borrow_price_window_is_off_by_default_and_replaced_by_the_builder() {
        assert_eq!(usdt_margin().borrow_price_window(), None);

        let config = usdt_margin()
            .with_borrow_price_window(TimeDelta::hours(2))
            .unwrap()
            .with_borrow_price_window(TimeDelta::milliseconds(1))
            .unwrap();

        assert_eq!(
            config.borrow_price_window(),
            Some(TimeDelta::milliseconds(1))
        );
    }

    #[test]
    fn a_borrow_price_window_not_a_positive_whole_number_of_milliseconds_is_refused() {
        for window in [
            TimeDelta::zero(),
            TimeDelta::milliseconds(-1),
            TimeDelta::microseconds(1),
            TimeDelta::microseconds(1_500),
            TimeDelta::MIN,
        ] {
            assert_eq!(
                usdt_margin().with_borrow_price_window(window),
                Err(SpotMarginConfigError::BorrowPriceWindowInvalid(window)),
                "{window}"
            );
        }
        assert!(
            usdt_margin()
                .with_borrow_price_window(TimeDelta::MAX)
                .is_ok()
        );

        let json = |millis: &str| {
            format!(
                r#"{{"valuation_asset":"usdt","max_leverage":"3","margin_effect":"borrow","borrow_price_window_ms":{millis}}}"#
            )
        };
        for (millis, window) in [
            ("0", TimeDelta::zero()),
            ("-5", TimeDelta::milliseconds(-5)),
            // One past the most negative `TimeDelta`, reported as that.
            (&i64::MIN.to_string(), TimeDelta::MIN),
        ] {
            let refused = serde_json::from_str::<SpotMarginConfig>(&json(millis))
                .unwrap_err()
                .to_string();
            let expected = SpotMarginConfigError::BorrowPriceWindowInvalid(window).to_string();
            assert!(refused.contains(&expected), "{millis}: {refused}");
        }
    }

    #[test]
    fn a_borrow_price_window_round_trips_as_whole_milliseconds_left_out_when_off() {
        let config = usdt_margin()
            .with_borrow_price_window(TimeDelta::hours(1))
            .unwrap();

        let json = serde_json::to_string(&config).unwrap();
        assert_eq!(
            json,
            r#"{"valuation_asset":"usdt","max_leverage":"3","margin_effect":"borrow","auto_repay_at_cancel":true,"borrow_price_window_ms":3600000}"#
        );
        assert_eq!(
            serde_json::from_str::<SpotMarginConfig>(&json).unwrap(),
            config
        );
        let plain = serde_json::to_string(&usdt_margin()).unwrap();
        assert!(!plain.contains("borrow_price_window"), "{plain}");
    }

    #[test]
    fn a_configuration_round_trips_through_its_serialised_form() {
        let config = SpotMarginConfig::new(
            AssetNameExchange::new("usdt"),
            dec!(3),
            MarginEffect::Borrow,
        )
        .unwrap()
        .with_auto_repay_at_cancel(false);

        let json = serde_json::to_string(&config).unwrap();
        assert_eq!(
            json,
            r#"{"valuation_asset":"usdt","max_leverage":"3","margin_effect":"borrow","auto_repay_at_cancel":false}"#
        );
        assert_eq!(
            serde_json::from_str::<SpotMarginConfig>(&json).unwrap(),
            config
        );
    }

    #[test]
    fn a_liquidation_round_trips_and_is_left_out_when_off() {
        let config = SpotMarginConfig::new(
            AssetNameExchange::new("usdt"),
            dec!(3),
            MarginEffect::Borrow,
        )
        .unwrap()
        .with_liquidation(
            SpotMarginLiquidation::new(dec!(1.1), dec!(0.02))
                .unwrap()
                .with_margin_call(dec!(1.3))
                .unwrap(),
        )
        .unwrap();

        let json = serde_json::to_string(&config).unwrap();
        assert_eq!(
            json,
            r#"{"valuation_asset":"usdt","max_leverage":"3","margin_effect":"borrow","auto_repay_at_cancel":true,"liquidation":{"level":"1.1","fee_rate":"0.02","margin_call_level":"1.3"}}"#
        );
        assert_eq!(
            serde_json::from_str::<SpotMarginConfig>(&json).unwrap(),
            config
        );

        let without_call = r#"{"level":"1.1","fee_rate":"0"}"#;
        let liquidation = serde_json::from_str::<SpotMarginLiquidation>(without_call).unwrap();
        assert_eq!(liquidation.margin_call_level(), None);
        assert_eq!(serde_json::to_string(&liquidation).unwrap(), without_call);
    }

    #[test]
    fn a_liquidation_out_of_range_is_refused_by_its_constructors_and_by_deserialising() {
        assert_eq!(
            SpotMarginLiquidation::new(dec!(0.99), dec!(0)),
            Err(SpotMarginConfigError::LiquidationLevelBelowOne(dec!(0.99)))
        );
        assert!(SpotMarginLiquidation::new(dec!(1), dec!(0)).is_ok());
        for fee_rate in [dec!(-0.01), dec!(1)] {
            assert_eq!(
                SpotMarginLiquidation::new(dec!(1.1), fee_rate),
                Err(SpotMarginConfigError::LiquidationFeeOutOfRange(fee_rate))
            );
        }
        assert_eq!(
            SpotMarginLiquidation::new(dec!(1.1), dec!(0))
                .unwrap()
                .with_margin_call(dec!(1.1)),
            Err(SpotMarginConfigError::MarginCallNotAboveLiquidation {
                margin_call: dec!(1.1),
                level: dec!(1.1),
            })
        );

        for json in [
            r#"{"level":"0.5","fee_rate":"0"}"#,
            r#"{"level":"1.1","fee_rate":"1"}"#,
            r#"{"level":"1.1","fee_rate":"0","margin_call_level":"1"}"#,
        ] {
            assert!(
                serde_json::from_str::<SpotMarginLiquidation>(json).is_err(),
                "{json}"
            );
        }
        let within_leverage = r#"{"valuation_asset":"usdt","max_leverage":"3","margin_effect":"borrow","liquidation":{"level":"1.5","fee_rate":"0"}}"#;
        assert!(serde_json::from_str::<SpotMarginConfig>(within_leverage).is_err());
    }

    #[test]
    fn the_effect_wire_names_are_snake_case_and_the_default_borrows_nothing() {
        assert_eq!(MarginEffect::default(), MarginEffect::NoSideEffect);
        for (effect, name) in [
            (MarginEffect::NoSideEffect, r#""no_side_effect""#),
            (MarginEffect::Borrow, r#""borrow""#),
            (MarginEffect::Repay, r#""repay""#),
        ] {
            assert_eq!(serde_json::to_string(&effect).unwrap(), name);
            assert_eq!(serde_json::from_str::<MarginEffect>(name).unwrap(), effect);
        }
    }

    fn btc() -> AssetNameExchange {
        AssetNameExchange::new("btc")
    }

    fn usdt() -> AssetNameExchange {
        AssetNameExchange::new("usdt")
    }

    fn utc(text: &str) -> DateTime<Utc> {
        text.parse().unwrap()
    }

    /// Hourly interest on BTC at `annual`, on the Act365 basis.
    fn hourly_btc(annual: Decimal) -> PeriodicMarginInterest {
        PeriodicMarginInterest::new(TimeDelta::hours(1), DayCount::Act365)
            .with_rates(btc(), RateSeries::constant(annual))
    }

    #[test]
    fn a_small_loan_is_charged_one_unit_for_its_first_period() {
        // 0.00063 BTC at 0.39% a year is about 2.8e-10 an hour, rounded up to one satoshi.
        let model = hourly_btc(dec!(0.0039));

        let charge = model.charge(
            &btc(),
            dec!(0.00063),
            InterestDue::Borrow(utc("2026-10-15T01:02:33Z")),
        );

        assert_eq!(
            charge,
            Some(InterestCharge::new(dec!(0.00000001), Some(dec!(0.0039))))
        );
    }

    #[test]
    fn a_daily_rate_times_365_on_act365_charges_a_24th_of_it_an_hour_without_rounding_up() {
        // 0.01% a day on 24 BTC is 0.0024 a day, exactly 0.0001 an hour.
        let model = hourly_btc(dec!(0.0001) * dec!(365));

        let charge = model
            .charge(
                &btc(),
                dec!(24),
                InterestDue::Boundary(utc("2026-10-15T02:00:00Z")),
            )
            .unwrap();

        assert_eq!(charge.interest, dec!(0.0001));
    }

    #[test]
    fn a_charge_is_rounded_up_to_the_asset_s_own_unit() {
        let model = PeriodicMarginInterest::new(TimeDelta::hours(1), DayCount::Act365)
            .with_rates(usdt(), RateSeries::constant(dec!(0.0876)))
            .with_unit(usdt(), dec!(0.01));
        let at = InterestDue::Borrow(utc("2026-10-15T01:00:00Z"));

        // 8.76% a year is 0.001% an hour: 0.01 on 1,000, and 0.01001 on 1,001.
        let charged = |principal| model.charge(&usdt(), principal, at).unwrap().interest;
        assert_eq!(charged(dec!(1000)), dec!(0.01));
        assert_eq!(charged(dec!(1001)), dec!(0.02));
    }

    #[test]
    fn boundaries_fall_on_whole_periods_and_one_on_a_boundary_steps_to_the_next() {
        let model = hourly_btc(dec!(0.01));

        assert_eq!(
            model.next_boundary(&btc(), utc("2026-10-15T01:02:33Z")),
            Some(utc("2026-10-15T02:00:00Z"))
        );
        assert_eq!(
            model.next_boundary(&btc(), utc("2026-10-15T02:00:00Z")),
            Some(utc("2026-10-15T03:00:00Z"))
        );
    }

    #[test]
    fn at_borrow_only_has_no_boundaries_but_still_charges_the_borrow() {
        let model = hourly_btc(dec!(0.01)).with_schedule(InterestSchedule::AtBorrowOnly);
        let at = utc("2026-10-15T01:02:33Z");

        assert_eq!(model.next_boundary(&btc(), at), None);
        assert!(
            model
                .charge(&btc(), dec!(1), InterestDue::Borrow(at))
                .is_some()
        );
    }

    #[test]
    fn an_asset_without_rates_or_before_its_rates_begin_is_charged_nothing() {
        let start = utc("2026-10-15T00:00:00Z");
        let model = PeriodicMarginInterest::new(TimeDelta::hours(1), DayCount::Act365)
            .with_rates(btc(), RateSeries::new(vec![(start, dec!(0.01))]).unwrap());
        let before = InterestDue::Borrow(start - TimeDelta::seconds(1));

        assert_eq!(model.next_boundary(&usdt(), start), None);
        assert_eq!(
            model.charge(&usdt(), dec!(1), InterestDue::Borrow(start)),
            None
        );
        assert_eq!(model.charge(&btc(), dec!(1), before), None);
        assert!(
            model
                .charge(&btc(), dec!(1), InterestDue::Borrow(start))
                .is_some()
        );
    }

    #[test]
    #[should_panic(expected = "period must be positive")]
    fn a_period_that_is_not_positive_panics() {
        let _ = PeriodicMarginInterest::new(TimeDelta::zero(), DayCount::Act365);
    }

    #[test]
    #[should_panic(expected = "unit for btc must be positive")]
    fn a_unit_that_is_not_positive_panics() {
        let _ = hourly_btc(dec!(0.01)).with_unit(btc(), Decimal::ZERO);
    }

    #[test]
    #[should_panic(expected = "rate for btc must not be negative, got -0.01")]
    fn a_negative_rate_anywhere_in_the_series_panics() {
        let rates = RateSeries::new(vec![
            (utc("2026-10-01T00:00:00Z"), dec!(0.02)),
            (utc("2026-10-02T00:00:00Z"), dec!(-0.01)),
        ])
        .unwrap();
        let _ = PeriodicMarginInterest::new(TimeDelta::hours(1), DayCount::Act365)
            .with_rates(btc(), rates);
    }

    #[test]
    fn a_zero_rate_is_accepted_and_charges_nothing() {
        let model = hourly_btc(Decimal::ZERO);

        let charge = model.charge(
            &btc(),
            dec!(1),
            InterestDue::Borrow(utc("2026-10-15T01:00:00Z")),
        );

        assert_eq!(charge.map(|charge| charge.interest), Some(Decimal::ZERO));
    }
}
