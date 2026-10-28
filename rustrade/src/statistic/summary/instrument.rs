use crate::{
    Timed,
    engine::state::position::PositionExited,
    statistic::{
        metric::{
            calmar::CalmarRatio,
            drawdown::{
                Drawdown, DrawdownGenerator,
                max::{MaxDrawdown, MaxDrawdownGenerator},
                mean::{MeanDrawdown, MeanDrawdownGenerator},
            },
            profit_factor::ProfitFactor,
            rate_of_return::RateOfReturn,
            sharpe::SharpeRatio,
            sortino::SortinoRatio,
            win_rate::WinRate,
        },
        summary::pnl::PnLReturns,
        time::TimeInterval,
    },
};
use chrono::{DateTime, TimeDelta, Utc};
use rust_decimal::Decimal;
use rustrade_execution::order::id::PositionId;
use serde::{Deserialize, Serialize};
use tracing::warn;

/// How many distinct fallback-keyed [`PositionId`]s a tear sheet retains.
///
/// The counters are authoritative for *how many* fills were misrouted; this list exists so a
/// consumer can go and look the positions up, and a handful is enough to start a reconciliation.
/// Bounding it keeps a pathological session — a venue replaying thousands of unmatched fills —
/// from growing the tear sheet without limit.
pub const MAX_FALLBACK_POSITIONS: usize = 16;

/// TearSheet summarising the trading performance related to an instrument.
#[derive(Debug, Clone, PartialEq, PartialOrd, Deserialize, Serialize)]
pub struct TearSheet<Interval> {
    /// PnL of the closed positions, net of carry: each one's realised PnL plus its
    /// [`carry`](PositionExited::carry), less [`Self::fees_unattributed`]. The drawdowns below
    /// follow this total. The returns and ratios are computed per closed position from the same
    /// net figures, so they leave the unattributed fees out.
    pub pnl: Decimal,

    /// Carry of the closed positions, already included in `pnl`: funding, interest and borrow
    /// fees, signed (positive = received). Base-asset carry is valued at each position's exit
    /// price; carry in any other asset is left out.
    ///
    /// `#[serde(default)]` so tear sheets serialised before this field existed still load.
    #[serde(default)]
    pub carry: Decimal,

    /// How many closed positions had base-asset carry and no exit price, so counted in `carry`
    /// and `pnl` with their quote carry alone. See [`PnLReturns::positions_carry_unpriced`].
    ///
    /// `#[serde(default)]` so tear sheets serialised before this field existed still load.
    #[serde(default)]
    pub positions_carry_unpriced: u64,

    /// Fees of fills that opened no position, already subtracted from `pnl`: positive = paid.
    /// In the instrument's quote asset, as each fee's quote equivalent.
    ///
    /// A spot buy from flat whose base-asset fee takes all it bought is one: it moves no holding,
    /// so no position carries its fee. See
    /// [`PnLReturns::fees_unattributed`](super::pnl::PnLReturns::fees_unattributed).
    ///
    /// `#[serde(default)]` so tear sheets serialised before this field existed still load.
    #[serde(default)]
    pub fees_unattributed: Decimal,

    /// The dust base-fee netting left out of the positions, in the instrument's base asset,
    /// signed as a position is: positive is base the account holds that no long counts, negative
    /// base still owed on a short's loan that no short counts. Summed over the fills that left it;
    /// see [`Self::fills_dust`].
    ///
    /// The dust is held, not a cost: it stays in the account's balance, and `pnl` charges neither
    /// it nor the quote it cost. A sale of it later, by an order sent with
    /// [`ReduceOnly::PositionAndFreeBase`](rustrade_execution::order::request::ReduceOnly::PositionAndFreeBase)
    /// or by hand, does not reduce this total, which counts what the fills left.
    ///
    /// `#[serde(default)]` so tear sheets serialised before this field existed still load.
    #[serde(default)]
    pub dust_quantity: Decimal,

    /// [`Self::dust_quantity`] valued at each fill's price, in the instrument's quote asset,
    /// signed as it is: what the dust cost, or what the owed dust was worth, when it was left.
    ///
    /// Each closed position's PnL agrees with the account at its fills' prices. What the dust has
    /// gained or lost since, `dust_quantity × price − dust_value`, is the account's and no
    /// position's.
    ///
    /// `#[serde(default)]` so tear sheets serialised before this field existed still load, and
    /// read from a `dust` field, which held a long's dust written off as a fee before the engine
    /// held it instead. A total loaded that way was charged to `pnl` and has no quantity beside
    /// it, so the gain above does not hold for it.
    #[serde(default, alias = "dust")]
    pub dust_value: Decimal,

    /// The excess of reduce-only fills the engine cut, valued at each fill's price, in the
    /// instrument's quote asset, as a size: the part of each fill no position took. See
    /// [`Self::fills_reduce_only_capped`].
    ///
    /// Reported only, not charged to `pnl`: the account still holds the excess, or the venue
    /// spent it, as on a margin repayment sized to cover interest, and only the caller knows
    /// which.
    ///
    /// `#[serde(default)]` so tear sheets serialised before this field existed still load.
    #[serde(default)]
    pub reduce_only_excess: Decimal,

    /// The free base that fills of orders sent
    /// [`ReduceOnly::PositionAndFreeBase`](rustrade_execution::order::request::ReduceOnly::PositionAndFreeBase)
    /// traded past the position they reduce, valued at each fill's price, in the instrument's
    /// quote asset, as a size. See [`Self::fills_free_base_traded`].
    ///
    /// Reported only, not charged to `pnl`: the base was the account's, outside every position,
    /// and the quote a sale of it brings in is the account's too.
    ///
    /// `#[serde(default)]` so tear sheets serialised before this field existed still load.
    #[serde(default)]
    pub free_base_traded: Decimal,

    pub pnl_return: RateOfReturn<Interval>,
    pub sharpe_ratio: SharpeRatio<Interval>,
    pub sortino_ratio: SortinoRatio<Interval>,
    pub calmar_ratio: CalmarRatio<Interval>,
    pub pnl_drawdown: Option<Drawdown>,
    pub pnl_drawdown_mean: Option<MeanDrawdown>,
    pub pnl_drawdown_max: Option<MaxDrawdown>,
    pub win_rate: Option<WinRate>,
    pub profit_factor: Option<ProfitFactor>,

    /// Open requests the engine sent to the exchange for this instrument.
    ///
    /// `#[serde(default)]` so tear sheets serialised before this field existed still load.
    #[serde(default)]
    pub orders_opened: usize,

    /// Of those, how many the exchange rejected.
    ///
    /// A session where this equals `orders_opened` traded nothing, however healthy the rest of
    /// the sheet looks: every ratio below is computed over zero fills. Without this field that
    /// case is indistinguishable from a strategy that chose not to trade.
    #[serde(default)]
    pub orders_rejected: usize,

    /// Why the first rejection happened, verbatim from the exchange.
    ///
    /// Kept so diagnosing an empty session does not require re-running with logging enabled.
    #[serde(default)]
    pub first_rejection_reason: Option<String>,

    /// Fills that reached a position slot keyed by their raw exchange `OrderId` because the order
    /// they belong to carried no `PositionId` mapping — `OmsMode::Hedging` only.
    ///
    /// Each one **splits that order's PnL across two position slots**: the fill opens a second
    /// position instead of joining the one its own order opened. Every statistic above is computed
    /// per position, so a non-zero count here means they are computed over a partition of the
    /// instrument's activity that the strategy never chose. Treat any non-zero value as a
    /// reconciliation signal, not a warning.
    ///
    /// Reachable by design as well as by defect: the corporate-action split path deliberately drops
    /// these mappings while leaving the order resting, so a late fill on a split instrument lands
    /// here.
    #[serde(default)]
    pub fills_routed_by_fallback: usize,

    /// Fills for which no order was tracked at all, also keyed by raw exchange `OrderId` —
    /// `OmsMode::Hedging` only.
    ///
    /// Counted apart from [`Self::fills_routed_by_fallback`] because the cause is different and so
    /// is the remedy. These are orders this engine never submitted, or that snapshot reconciliation
    /// removed; one position per external order is a defensible reading rather than a split of
    /// something that should have been whole. A consumer trading the same account from elsewhere
    /// should expect this to be non-zero.
    #[serde(default)]
    pub fills_unmatched: usize,

    /// Fills the venue forced on this instrument's positions: liquidations, auto-deleveraging
    /// and option assignments, as
    /// [`TradeOrigin::is_forced`](rustrade_execution::trade::TradeOrigin::is_forced) defines them.
    ///
    /// Counted as the venues reported them, and most mark no forced fill at all, so zero is not
    /// proof that none happened.
    /// [`TradeOrigin`](rustrade_execution::trade::TradeOrigin) says what each venue reports.
    #[serde(default)]
    pub fills_forced: usize,

    /// Venue settlement fills, or parts of one, applied to no position — `OmsMode::Hedging` only.
    ///
    /// A settlement fill, such as a liquidation or an option's expiry, exercise or assignment,
    /// matches none of this engine's orders, so in Hedging mode it reduces the one open position
    /// on the side it closes. When there is none, or several, the engine cannot tell which
    /// position the venue closed, and opens nothing rather than guess. The part of a fill larger
    /// than the position it reduces is counted too. Each is logged at ERROR: the engine's
    /// positions no longer match the venue's, so reconcile them. See
    /// [`InstrumentState::update_from_trade`](crate::engine::state::instrument::InstrumentState::update_from_trade)
    /// for the share leg of an exercise or assignment, which may open a position instead.
    #[serde(default)]
    pub fills_unallocated: usize,

    /// Spot fills whose base-asset fee left the holding off the instrument's quantity step, so
    /// the position was rounded toward zero to the step and the remainder left out of it.
    ///
    /// The remainder stays in the account's balance, too small for the venue to sell alone, and
    /// is held, not charged to `pnl`: a long's is base the account holds, a short's base still
    /// owed on the loan. Its quantity is summed in [`Self::dust_quantity`] and its value in
    /// [`Self::dust_value`]. Each is logged at INFO. See the base-asset fee section of
    /// [`InstrumentState::update_from_trade`](crate::engine::state::instrument::InstrumentState::update_from_trade).
    #[serde(default)]
    pub fills_dust: usize,

    /// Fills of reduce-only orders the engine cut so they would not grow or flip the position
    /// they reduce, or applied none of, because the position was flat or on the fill's side.
    ///
    /// The excess is applied to no position, its value summed in [`Self::reduce_only_excess`],
    /// and its share of the fill's fees charged to [`Self::fees_unattributed`]. Each is logged at
    /// WARN. Unlike [`Self::fills_unallocated`], this is no sign the engine's positions differ
    /// from the venue's: an order sized above its position on purpose, such as a margin
    /// repayment covering interest, is cut every time. See the reduce-only section of
    /// [`InstrumentState::update_from_trade`](crate::engine::state::instrument::InstrumentState::update_from_trade).
    #[serde(default)]
    pub fills_reduce_only_capped: usize,

    /// Fills of orders sent
    /// [`ReduceOnly::PositionAndFreeBase`](rustrade_execution::order::request::ReduceOnly::PositionAndFreeBase)
    /// on a spot instrument that traded base past the position they reduce, as they were sized
    /// to: from flat, or past a long for a sell, past a short for a buy.
    ///
    /// The engine cuts such a fill to land the position flat, as it does any reduce-only fill,
    /// but reads the excess as intended: it is logged at INFO, counted here rather than on
    /// [`Self::fills_reduce_only_capped`], and its value summed in [`Self::free_base_traded`].
    /// Its share of the fill's fees is charged to [`Self::fees_unattributed`]. See the
    /// reduce-only section of
    /// [`InstrumentState::update_from_trade`](crate::engine::state::instrument::InstrumentState::update_from_trade).
    #[serde(default)]
    pub fills_free_base_traded: usize,

    /// Fills whose fee was charged in a third asset, neither the instrument's base nor its quote,
    /// that the engine could not value in quote, so their fees are left out of `pnl`.
    ///
    /// A fee such as BNB on a `BTCUSDT` fill is valued with the price of a spot instrument
    /// pairing the fee asset with the quote, when the engine has one with a price. Each fill
    /// counted here was reported in an
    /// [`EngineOutput::FeeConversion`](crate::engine::EngineOutput::FeeConversion) saying why.
    /// A non-zero count means `pnl` is gross of those fees. A fill is counted when it arrives,
    /// valued with the prices held then, including one held for its order's acknowledgement
    /// before it reaches a position. See
    /// [`EngineState::update_from_account`](crate::engine::state::EngineState::update_from_account).
    #[serde(default)]
    pub fills_fee_unconverted: usize,

    /// Why the first fill of either kind above could not be routed.
    ///
    /// Shared by both counters, for the same reason [`Self::first_rejection_reason`] exists: the
    /// first occurrence is what identifies the cause, and keeping it means diagnosing a split
    /// position does not require re-running with logging enabled.
    #[serde(default)]
    pub first_fallback_detail: Option<String>,

    /// The distinct positions opened by either fallback above, so a consumer reconciling this
    /// instrument can look them up rather than parse [`Self::first_fallback_detail`].
    ///
    /// Each entry is a `PositionId` built from a raw exchange `OrderId`, which is what makes the
    /// position identifiable as unrouted rather than strategy-chosen. Deduplicated — an order
    /// filling ten times through the fallback lands in one position, not ten — and capped at
    /// [`MAX_FALLBACK_POSITIONS`]. When the counters exceed the length of this list, the list is
    /// the first `MAX_FALLBACK_POSITIONS` and the counters remain exact.
    #[serde(default)]
    pub fallback_positions: Vec<PositionId>,
}

/// Generator for a [`TearSheet`].
#[derive(Debug, Clone, PartialEq, PartialOrd, Deserialize, Serialize)]
pub struct TearSheetGenerator {
    /// Trading session start time defined by the [`Engine`](crate::engine::Engine) clock.
    pub time_engine_start: DateTime<Utc>,

    /// Trading session end time defined by the [`Engine`](crate::engine::Engine) clock.
    pub time_engine_now: DateTime<Utc>,

    pub pnl_returns: PnLReturns,
    pub pnl_drawdown: DrawdownGenerator,
    pub pnl_drawdown_mean: MeanDrawdownGenerator,
    pub pnl_drawdown_max: MaxDrawdownGenerator,

    /// Open requests sent for this instrument. See [`TearSheet::orders_opened`].
    #[serde(default)]
    pub orders_opened: usize,

    /// Open requests the exchange rejected. See [`TearSheet::orders_rejected`].
    #[serde(default)]
    pub orders_rejected: usize,

    /// First rejection reason seen. See [`TearSheet::first_rejection_reason`].
    #[serde(default)]
    pub first_rejection_reason: Option<String>,

    /// Fills routed by the raw-`OrderId` fallback despite a known order.
    /// See [`TearSheet::fills_routed_by_fallback`].
    #[serde(default)]
    pub fills_routed_by_fallback: usize,

    /// Fills routed by the raw-`OrderId` fallback with no order tracked at all.
    /// See [`TearSheet::fills_unmatched`].
    #[serde(default)]
    pub fills_unmatched: usize,

    /// Fills the venue forced. See [`TearSheet::fills_forced`].
    #[serde(default)]
    pub fills_forced: usize,

    /// Settlement fills applied to no position. See [`TearSheet::fills_unallocated`].
    #[serde(default)]
    pub fills_unallocated: usize,

    /// Fills whose base-fee netting left dust out of the position. See [`TearSheet::fills_dust`].
    #[serde(default)]
    pub fills_dust: usize,

    /// Dust held, in base. See [`TearSheet::dust_quantity`].
    #[serde(default)]
    pub dust_quantity: Decimal,

    /// Dust held, valued at each fill's price. See [`TearSheet::dust_value`].
    #[serde(default, alias = "dust")]
    pub dust_value: Decimal,

    /// Reduce-only fills cut. See [`TearSheet::fills_reduce_only_capped`].
    #[serde(default)]
    pub fills_reduce_only_capped: usize,

    /// Value of the reduce-only fills' excess. See [`TearSheet::reduce_only_excess`].
    #[serde(default)]
    pub reduce_only_excess: Decimal,

    /// Fills that traded free base past their position. See
    /// [`TearSheet::fills_free_base_traded`].
    #[serde(default)]
    pub fills_free_base_traded: usize,

    /// Value of the free base those fills traded. See [`TearSheet::free_base_traded`].
    #[serde(default)]
    pub free_base_traded: Decimal,

    /// Fills whose third-asset fee was left out of PnL. See [`TearSheet::fills_fee_unconverted`].
    #[serde(default)]
    pub fills_fee_unconverted: usize,

    /// First unroutable-fill detail seen. See [`TearSheet::first_fallback_detail`].
    #[serde(default)]
    pub first_fallback_detail: Option<String>,

    /// Distinct positions opened by a fallback routing. See [`TearSheet::fallback_positions`].
    #[serde(default)]
    pub fallback_positions: Vec<PositionId>,
}

impl TearSheetGenerator {
    /// Initialise a [`TearSheetGenerator`] with an initial timestamp.
    pub fn init(time_engine_start: DateTime<Utc>) -> Self {
        Self {
            time_engine_start,
            time_engine_now: time_engine_start,
            pnl_returns: PnLReturns::default(),
            pnl_drawdown: DrawdownGenerator::default(),
            pnl_drawdown_mean: MeanDrawdownGenerator::default(),
            pnl_drawdown_max: MaxDrawdownGenerator::default(),
            orders_opened: 0,
            orders_rejected: 0,
            first_rejection_reason: None,
            fills_routed_by_fallback: 0,
            fills_unmatched: 0,
            fills_forced: 0,
            fills_unallocated: 0,
            fills_dust: 0,
            dust_quantity: Decimal::ZERO,
            dust_value: Decimal::ZERO,
            fills_reduce_only_capped: 0,
            reduce_only_excess: Decimal::ZERO,
            fills_free_base_traded: 0,
            free_base_traded: Decimal::ZERO,
            fills_fee_unconverted: 0,
            first_fallback_detail: None,
            fallback_positions: Vec::new(),
        }
    }

    /// Record that the engine sent an open request for this instrument.
    pub fn record_open_requested(&mut self) {
        self.orders_opened = self.orders_opened.saturating_add(1);
    }

    /// Record that the exchange rejected an open request, keeping the first reason given.
    pub fn record_open_rejected(&mut self, reason: impl Into<String>) {
        self.orders_rejected = self.orders_rejected.saturating_add(1);
        if self.first_rejection_reason.is_none() {
            self.first_rejection_reason = Some(reason.into());
        }
    }

    /// Record a fill that opened `position_id` under its raw exchange `OrderId` because the order
    /// it belongs to had no `PositionId` mapping. See [`TearSheet::fills_routed_by_fallback`].
    ///
    /// `detail` is called only if this is the first unroutable fill of the session, which is why
    /// it is a closure rather than a `String`: the caller would otherwise format one per misrouted
    /// fill to discard all but the first.
    pub fn record_fill_routed_by_fallback(
        &mut self,
        position_id: &PositionId,
        detail: impl FnOnce() -> String,
    ) {
        self.fills_routed_by_fallback = self.fills_routed_by_fallback.saturating_add(1);
        self.record_fallback_position(position_id, detail);
    }

    /// Record a fill that opened `position_id` under its raw exchange `OrderId` because no order
    /// matched it at all. See [`TearSheet::fills_unmatched`].
    ///
    /// `detail` is called only if this is the first unroutable fill of the session.
    pub fn record_fill_unmatched(
        &mut self,
        position_id: &PositionId,
        detail: impl FnOnce() -> String,
    ) {
        self.fills_unmatched = self.fills_unmatched.saturating_add(1);
        self.record_fallback_position(position_id, detail);
    }

    /// Record a fill the venue forced. See [`TearSheet::fills_forced`].
    pub fn record_fill_forced(&mut self) {
        self.fills_forced = self.fills_forced.saturating_add(1);
    }

    /// Record a settlement fill, or the part of one, applied to no position. See
    /// [`TearSheet::fills_unallocated`].
    pub fn record_fill_unallocated(&mut self) {
        self.fills_unallocated = self.fills_unallocated.saturating_add(1);
    }

    /// Record a fill whose base-fee netting left dust out of the position: its `quantity`, signed
    /// as a position is, and its `value` at the fill's price, signed the same. See
    /// [`TearSheet::fills_dust`].
    ///
    /// Only counted here: the dust is held, not charged to PnL. Holds both last-good totals when
    /// either would overflow, and emits a `warn!`, as the PnL totals do.
    pub fn record_fill_dust(&mut self, quantity: Decimal, value: Decimal) {
        self.fills_dust = self.fills_dust.saturating_add(1);
        match (
            self.dust_quantity.checked_add(quantity),
            self.dust_value.checked_add(value),
        ) {
            (Some(new_quantity), Some(new_value)) => {
                self.dust_quantity = new_quantity;
                self.dust_value = new_value;
            }
            _ => warn!(
                dust_quantity = %self.dust_quantity,
                dust_value = %self.dust_value,
                %quantity,
                %value,
                "dust accumulation overflowed Decimal; holding last-good values"
            ),
        }
    }

    /// Record a fill whose third-asset fee the engine could not value in quote. See
    /// [`TearSheet::fills_fee_unconverted`].
    pub fn record_fill_fee_unconverted(&mut self) {
        self.fills_fee_unconverted = self.fills_fee_unconverted.saturating_add(1);
    }

    /// Record a fill that traded free base past the position it reduces, and the value of that
    /// excess, as a size. See [`TearSheet::fills_free_base_traded`].
    ///
    /// Holds its last-good total on overflow and emits a `warn!`, as the PnL totals do.
    pub fn record_fill_free_base_traded(&mut self, excess_value: Decimal) {
        self.fills_free_base_traded = self.fills_free_base_traded.saturating_add(1);
        match self.free_base_traded.checked_add(excess_value) {
            Some(new_total) => self.free_base_traded = new_total,
            None => warn!(
                free_base_traded = %self.free_base_traded,
                %excess_value,
                "free base traded accumulation overflowed Decimal; holding last-good value"
            ),
        }
    }

    /// Record a reduce-only fill the engine cut, and its excess's value, as a size. See
    /// [`TearSheet::fills_reduce_only_capped`].
    ///
    /// Holds its last-good total on overflow and emits a `warn!`, as the PnL totals do.
    pub fn record_fill_reduce_only_capped(&mut self, excess_value: Decimal) {
        self.fills_reduce_only_capped = self.fills_reduce_only_capped.saturating_add(1);
        match self.reduce_only_excess.checked_add(excess_value) {
            Some(new_total) => self.reduce_only_excess = new_total,
            None => warn!(
                reduce_only_excess_held = %self.reduce_only_excess,
                excess_value = %excess_value,
                "reduce-only excess accumulation overflowed Decimal; holding last-good value"
            ),
        }
    }

    /// Note the position a fallback routing opened, and the reason if it is the first one.
    ///
    /// `detail` is a closure because only the first caller's string is ever kept, while the
    /// callers are on a path that a corporate-action split can drive repeatedly — formatting
    /// eagerly would allocate once per misrouted fill to discard all but one. This mirrors what
    /// `tracing` does for a disabled log level at the same call sites.
    fn record_fallback_position(
        &mut self,
        position_id: &PositionId,
        detail: impl FnOnce() -> String,
    ) {
        if self.first_fallback_detail.is_none() {
            self.first_fallback_detail = Some(detail());
        }

        // Deduplicated rather than appended: one order filling repeatedly through the fallback is
        // one suspect position, and the counters already carry the number of fills.
        if self.fallback_positions.len() < MAX_FALLBACK_POSITIONS
            && !self.fallback_positions.contains(position_id)
        {
            self.fallback_positions.push(position_id.clone());
        }
    }

    /// Update the [`TearSheetGenerator`] from the next [`PositionExited`].
    pub fn update_from_position<AssetKey, InstrumentKey>(
        &mut self,
        position: &PositionExited<AssetKey, InstrumentKey>,
    ) {
        self.pnl_returns.update(position);
        self.record_pnl_at(position.time_exit);
    }

    /// Charge the fee of a fill that opened no position, so it reaches
    /// [`TearSheet::pnl`] and the drawdowns but no per-position statistic. `fee` is the fee's
    /// quote equivalent, positive = paid; `time` is the fill's.
    ///
    /// See [`TearSheet::fees_unattributed`].
    pub fn record_fee_unattributed(&mut self, fee: Decimal, time: DateTime<Utc>) {
        self.pnl_returns.update_from_fee_unattributed(fee);
        self.record_pnl_at(time);
    }

    /// Advance the clock to `time`, and feed the drawdowns the PnL total as it now stands.
    fn record_pnl_at(&mut self, time: DateTime<Utc>) {
        self.time_engine_now = time;

        if let Some(next_drawdown) = self
            .pnl_drawdown
            .update(Timed::new(self.pnl_returns.pnl_raw, self.time_engine_now))
        {
            self.pnl_drawdown_mean.update(&next_drawdown);
            self.pnl_drawdown_max.update(&next_drawdown);
        }
    }

    /// Generate the latest [`TearSheet`] at the specific [`TimeInterval`].
    ///
    /// For example, pass [`Annual365`](super::super::time::Annual365) to generate a crypto-centric
    /// (24/7 trading) annualised [`TearSheet`].
    pub fn generate<Interval>(
        &mut self,
        risk_free_return: Decimal,
        interval: Interval,
    ) -> TearSheet<Interval>
    where
        Interval: TimeInterval,
    {
        let trading_period = self
            .time_engine_now
            .signed_duration_since(self.time_engine_start)
            .max(TimeDelta::seconds(1));

        let sharpe_ratio = SharpeRatio::calculate(
            risk_free_return,
            self.pnl_returns.total.mean,
            self.pnl_returns.total.dispersion.std_dev,
            trading_period,
        )
        .scale(interval);

        let sortino_ratio = SortinoRatio::calculate(
            risk_free_return,
            self.pnl_returns.total.mean,
            self.pnl_returns.losses.dispersion.std_dev,
            trading_period,
        )
        .scale(interval);

        let current_pnl_drawdown = self.pnl_drawdown.generate();
        if let Some(current_pnl_drawdown) = &current_pnl_drawdown {
            self.pnl_drawdown_mean.update(current_pnl_drawdown);
            self.pnl_drawdown_max.update(current_pnl_drawdown);
        }
        let pnl_drawdown_mean = self.pnl_drawdown_mean.generate();
        let pnl_drawdown_max = self.pnl_drawdown_max.generate();

        let calmar_ratio = CalmarRatio::calculate(
            risk_free_return,
            self.pnl_returns.total.mean,
            // Zero drawdown risk handled by CalmarRatio::calculate
            pnl_drawdown_max
                .as_ref()
                .unwrap_or(&MaxDrawdown(Drawdown::default()))
                .0
                .value,
            trading_period,
        )
        .scale(interval);

        let pnl_return =
            RateOfReturn::calculate(self.pnl_returns.total.mean, trading_period).scale(interval);

        let win_rate =
            WinRate::calculate(self.pnl_returns.losses.count, self.pnl_returns.total.count);

        let profit_factor =
            ProfitFactor::calculate(self.pnl_returns.total.sum, self.pnl_returns.losses.sum);

        TearSheet {
            sharpe_ratio,
            sortino_ratio,
            calmar_ratio,
            pnl: self.pnl_returns.pnl_raw,
            carry: self.pnl_returns.carry,
            positions_carry_unpriced: self.pnl_returns.positions_carry_unpriced,
            fees_unattributed: self.pnl_returns.fees_unattributed,
            dust_quantity: self.dust_quantity,
            dust_value: self.dust_value,
            reduce_only_excess: self.reduce_only_excess,
            free_base_traded: self.free_base_traded,
            pnl_return,
            pnl_drawdown: current_pnl_drawdown,
            pnl_drawdown_mean,
            pnl_drawdown_max,
            win_rate,
            profit_factor,
            orders_opened: self.orders_opened,
            orders_rejected: self.orders_rejected,
            first_rejection_reason: self.first_rejection_reason.clone(),
            fills_routed_by_fallback: self.fills_routed_by_fallback,
            fills_unmatched: self.fills_unmatched,
            fills_forced: self.fills_forced,
            fills_unallocated: self.fills_unallocated,
            fills_dust: self.fills_dust,
            fills_reduce_only_capped: self.fills_reduce_only_capped,
            fills_free_base_traded: self.fills_free_base_traded,
            fills_fee_unconverted: self.fills_fee_unconverted,
            first_fallback_detail: self.first_fallback_detail.clone(),
            fallback_positions: self.fallback_positions.clone(),
        }
    }

    /// Reset the internal state, using a new starting `DateTime<Utc>` as seed.
    pub fn reset(&mut self, time_engine_start: DateTime<Utc>) {
        *self = Self::init(time_engine_start);
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use crate::statistic::time::Annual365;
    use rust_decimal_macros::dec;

    #[test]
    fn a_tear_sheet_serialised_before_fees_unattributed_loads_with_zero() {
        let mut generator = TearSheetGenerator::init(DateTime::<Utc>::MIN_UTC);
        generator.record_fee_unattributed(dec!(2), DateTime::<Utc>::MIN_UTC);
        let mut value = serde_json::to_value(generator.generate(Decimal::ZERO, Annual365)).unwrap();
        value
            .as_object_mut()
            .unwrap()
            .remove("fees_unattributed")
            .unwrap();

        let loaded: TearSheet<Annual365> = serde_json::from_value(value).unwrap();
        assert_eq!(loaded.pnl, dec!(-2));
        assert_eq!(loaded.fees_unattributed, Decimal::ZERO);
    }

    #[test]
    fn a_tear_sheet_serialised_before_dust_loads_with_zero() {
        let mut generator = TearSheetGenerator::init(DateTime::<Utc>::MIN_UTC);
        generator.record_fill_dust(dec!(0.009), dec!(0.9));
        let mut value = serde_json::to_value(generator.generate(Decimal::ZERO, Annual365)).unwrap();
        let fields = value.as_object_mut().unwrap();
        fields.remove("dust_quantity").unwrap();
        fields.remove("dust_value").unwrap();
        fields.remove("fills_dust").unwrap();

        let loaded: TearSheet<Annual365> = serde_json::from_value(value).unwrap();
        assert_eq!(
            (loaded.fills_dust, loaded.dust_quantity, loaded.dust_value),
            (0, Decimal::ZERO, Decimal::ZERO)
        );
    }

    /// A tear sheet from before the dust was held names its value `dust`, which loads as the
    /// value, with no quantity.
    #[test]
    fn a_tear_sheet_with_the_dust_written_off_loads_it_as_the_value() {
        let mut generator = TearSheetGenerator::init(DateTime::<Utc>::MIN_UTC);
        generator.record_fill_dust(dec!(0.009), dec!(0.9));
        let mut value = serde_json::to_value(generator.generate(Decimal::ZERO, Annual365)).unwrap();
        let fields = value.as_object_mut().unwrap();
        fields.remove("dust_quantity").unwrap();
        let written_off = fields.remove("dust_value").unwrap();
        fields.insert("dust".to_owned(), written_off);

        let loaded: TearSheet<Annual365> = serde_json::from_value(value).unwrap();
        assert_eq!(
            (loaded.fills_dust, loaded.dust_quantity, loaded.dust_value),
            (1, Decimal::ZERO, dec!(0.9))
        );
    }

    #[test]
    fn a_tear_sheet_serialised_before_fills_fee_unconverted_loads_with_zero() {
        let mut generator = TearSheetGenerator::init(DateTime::<Utc>::MIN_UTC);
        generator.record_fill_fee_unconverted();
        let mut value = serde_json::to_value(generator.generate(Decimal::ZERO, Annual365)).unwrap();
        assert_eq!(value["fills_fee_unconverted"], 1);
        value
            .as_object_mut()
            .unwrap()
            .remove("fills_fee_unconverted")
            .unwrap();

        let loaded: TearSheet<Annual365> = serde_json::from_value(value).unwrap();
        assert_eq!(loaded.fills_fee_unconverted, 0);
    }

    #[test]
    fn a_tear_sheet_serialised_before_reduce_only_excess_loads_with_zero() {
        let mut generator = TearSheetGenerator::init(DateTime::<Utc>::MIN_UTC);
        generator.record_fill_reduce_only_capped(dec!(49));
        let mut value = serde_json::to_value(generator.generate(Decimal::ZERO, Annual365)).unwrap();
        let fields = value.as_object_mut().unwrap();
        fields.remove("reduce_only_excess").unwrap();
        fields.remove("fills_reduce_only_capped").unwrap();

        let loaded: TearSheet<Annual365> = serde_json::from_value(value).unwrap();
        assert_eq!(
            (loaded.fills_reduce_only_capped, loaded.reduce_only_excess),
            (0, Decimal::ZERO)
        );
    }

    #[test]
    fn a_tear_sheet_serialised_before_free_base_traded_loads_with_zero() {
        let mut generator = TearSheetGenerator::init(DateTime::<Utc>::MIN_UTC);
        generator.record_fill_free_base_traded(dec!(0.5));
        let mut value = serde_json::to_value(generator.generate(Decimal::ZERO, Annual365)).unwrap();
        let fields = value.as_object_mut().unwrap();
        fields.remove("free_base_traded").unwrap();
        fields.remove("fills_free_base_traded").unwrap();

        let loaded: TearSheet<Annual365> = serde_json::from_value(value).unwrap();
        assert_eq!(
            (loaded.fills_free_base_traded, loaded.free_base_traded),
            (0, Decimal::ZERO)
        );
    }

    /// A free base traded total that would overflow holds its last-good value, and the fill is
    /// still counted, apart from the reduce-only fills cut.
    #[test]
    fn free_base_traded_holds_its_last_good_total_on_overflow() {
        let mut generator = TearSheetGenerator::init(DateTime::<Utc>::MIN_UTC);
        generator.record_fill_free_base_traded(Decimal::MAX);

        generator.record_fill_free_base_traded(Decimal::ONE);

        assert_eq!(
            (generator.fills_free_base_traded, generator.free_base_traded),
            (2, Decimal::MAX)
        );
        assert_eq!(
            (
                generator.fills_reduce_only_capped,
                generator.reduce_only_excess
            ),
            (0, Decimal::ZERO)
        );
    }

    /// A reduce-only excess total that would overflow holds its last-good value, and the fill is
    /// still counted.
    #[test]
    fn reduce_only_excess_holds_its_last_good_total_on_overflow() {
        let mut generator = TearSheetGenerator::init(DateTime::<Utc>::MIN_UTC);
        generator.record_fill_reduce_only_capped(Decimal::MAX);

        generator.record_fill_reduce_only_capped(Decimal::ONE);

        assert_eq!(
            (
                generator.fills_reduce_only_capped,
                generator.reduce_only_excess
            ),
            (2, Decimal::MAX)
        );
    }

    /// A dust total that would overflow holds both last-good values, whichever overflows, and
    /// the fill is still counted.
    #[test]
    fn dust_holds_its_last_good_totals_on_overflow() {
        for (quantity, value) in [(Decimal::ONE, Decimal::MAX), (Decimal::MAX, Decimal::ONE)] {
            let mut generator = TearSheetGenerator::init(DateTime::<Utc>::MIN_UTC);
            generator.record_fill_dust(quantity, value);

            generator.record_fill_dust(Decimal::ONE, Decimal::ONE);

            assert_eq!(
                (
                    generator.fills_dust,
                    generator.dust_quantity,
                    generator.dust_value
                ),
                (2, quantity, value)
            );
        }
    }

    /// Dust totals are signed: a short's owed dust takes from a long's held dust.
    #[test]
    fn dust_totals_are_signed() {
        let mut generator = TearSheetGenerator::init(DateTime::<Utc>::MIN_UTC);
        generator.record_fill_dust(dec!(0.009), dec!(0.9));
        generator.record_fill_dust(dec!(-0.001), dec!(-0.11));

        assert_eq!(
            (
                generator.fills_dust,
                generator.dust_quantity,
                generator.dust_value
            ),
            (2, dec!(0.008), dec!(0.79))
        );
    }
}
