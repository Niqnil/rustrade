use crate::statistic::summary::TradingSummary;
use rust_decimal::Decimal;
use smol_str::SmolStr;
use std::time::Duration;

/// Container for multiple [`BacktestSummary`]s and associated multi backtest metadata.
#[derive(Debug)]
pub struct MultiBacktestSummary<Interval> {
    /// Number of backtests run in this batch.
    pub num_backtests: usize,
    /// Total execution time for all backtests.
    pub duration: Duration,
    /// Collection of `BacktestSummary`s.
    pub summaries: Vec<BacktestSummary<Interval>>,
}

impl<Interval> MultiBacktestSummary<Interval> {
    /// Create a new `MultiBacktestSummary` with the provided data.
    pub fn new<SummaryIter>(duration: Duration, summary_iter: SummaryIter) -> Self
    where
        SummaryIter: IntoIterator<Item = BacktestSummary<Interval>>,
    {
        let summaries = summary_iter.into_iter().collect::<Vec<_>>();

        Self {
            num_backtests: summaries.len(),
            duration,
            summaries,
        }
    }
}

/// Full result of a single [`backtest`](super::backtest) run: the aggregate [`BacktestSummary`]
/// **and** the terminal engine state.
///
/// [`BacktestSummary::trading_summary`] aggregates statistics derived from **closed** positions only,
/// so it cannot answer questions about state left at the end of the run — e.g. a position still
/// **open** at shutdown, or the effect of a notional-preserving corporate action that moves no
/// aggregate metric. `engine_state` exposes that terminal [`EngineState`](crate::engine::state::EngineState):
/// callers can inspect open positions, balances, and instrument state directly (e.g. assert a stock
/// split rescaled an open position's `quantity_abs` / `price_entry_average`).
///
/// `State` is the engine's state type (`EngineState<GlobalData, InstrumentData>` for the standard
/// [`backtest`](super::backtest) path); it is left generic so this module stays decoupled from the
/// engine-state internals.
#[derive(Debug, PartialEq)]
pub struct BacktestResult<Interval, State> {
    /// Aggregate performance summary derived from closed positions.
    pub summary: BacktestSummary<Interval>,
    /// Terminal engine state after the run completes (open positions, balances, instrument state).
    pub engine_state: State,
}

/// Single backtest `TradingSummary` and associated metadata.
#[derive(Debug, PartialEq)]
pub struct BacktestSummary<Interval> {
    /// [`BacktestArgsDynamic`](super::BacktestArgsDynamic) unique identifier that was input for the backtest.
    pub id: SmolStr,
    /// Risk-free return rate used for performance metrics.
    pub risk_free_return: Decimal,
    /// Performance metrics and statistics from the backtest simulated trading.
    pub trading_summary: TradingSummary<Interval>,
}
