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
    pub pnl: Decimal,
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
        self.time_engine_now = position.time_exit;
        self.pnl_returns.update(position);

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
            first_fallback_detail: self.first_fallback_detail.clone(),
            fallback_positions: self.fallback_positions.clone(),
        }
    }

    /// Reset the internal state, using a new starting `DateTime<Utc>` as seed.
    pub fn reset(&mut self, time_engine_start: DateTime<Utc>) {
        *self = Self::init(time_engine_start);
    }
}
