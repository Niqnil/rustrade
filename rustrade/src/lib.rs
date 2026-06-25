#![forbid(unsafe_code)]
#![deny(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]
#![warn(
    unused,
    clippy::cognitive_complexity,
    unused_crate_dependencies,
    unused_extern_crates,
    clippy::unused_self,
    clippy::useless_let_if_seq,
    missing_debug_implementations,
    rust_2018_idioms
)]
#![allow(clippy::type_complexity, clippy::too_many_arguments, type_alias_bounds)]

//! # Barter
//! Barter core is a Rust framework for building high-performance live-trading, paper-trading and back-testing systems.
//! * **Fast**: Written in native Rust. Minimal allocations. Data-oriented state management system with direct index lookups.
//! * **Robust**: Strongly typed. Thread safe. Extensive test coverage.
//! * **Customisable**: Plug and play Strategy and RiskManager components that facilitates most trading strategies (MarketMaking, StatArb, HFT, etc.).
//! * **Scalable**: Multithreaded architecture with modular design. Leverages Tokio for I/O. Memory efficient data structures.
//!
//! ## Overview
//! Barter core is a Rust framework for building professional grade live-trading, paper-trading and back-testing systems. The
//! central Engine facilitates executing on many exchanges simultaneously, and offers the flexibility to run most types of
//! trading strategies.  It allows turning algorithmic order generation on/off and can action Commands issued from external
//! processes (eg/ CloseAllPositions, OpenOrders, CancelOrders, etc.)
//!
//! At a high-level, it provides a few major components:
//! * `Engine` with plug and play `Strategy` and `RiskManager` components.
//! * Centralised cache friendly `EngineState` management with O(1) constant lookups using indexed data structures.
//! * `Strategy` interfaces for customising Engine behavior (AlgoStrategy, ClosePositionsStrategy, OnDisconnectStrategy, etc.).
//! * `RiskManager` interface for defining custom risk logic which checking generated algorithmic orders.
//! * Event-driven system that allows for Commands to be issued from external processes (eg/ CloseAllPositions, OpenOrders, CancelOrders, etc.),
//!   as well as turning algorithmic trading on/off.
//! * Comprehensive statistics package that provides a summary of key performance metrics (PnL, Sharpe, Sortino, Drawdown, etc.).
//!
//! ## Getting Started Via Engine Examples
//! [See Engine Examples](https://github.com/Niqnil/rustrade/tree/feat/docs_tests_readmes_examples/rustrade/examples)

// Silence unused_crate_dependencies for dev-dependencies used only in tests
#[cfg(test)]
use criterion as _;
#[cfg(test)]
use serde_json as _;

use crate::{
    engine::{command::Command, state::trading::TradingState},
    execution::AccountStreamEvent,
};
use chrono::{DateTime, Utc};
use derive_more::{Constructor, From};
use rustrade_data::{
    event::{DataKind, MarketEvent},
    streams::consumer::MarketStreamEvent,
};
use rustrade_execution::AccountEvent;
use rustrade_instrument::{
    asset::AssetIndex, corporate_action::CorporateActionKind, exchange::ExchangeIndex,
    instrument::InstrumentIndex,
};
use rustrade_integration::Terminal;
use serde::{Deserialize, Serialize};
use shutdown::Shutdown;
use smol_str::SmolStr;

/// Re-export of [`SplitRoundingPolicy`] so callers can construct
/// [`EngineEvent::CorporateAction`] without depending on the engine's internal module layout.
pub use crate::engine::state::position::SplitRoundingPolicy;

/// Algorithmic trading `Engine`, and entry points for processing input `Events`.
///
/// eg/ `Engine`, `run`, `process_with_audit`, etc.
pub mod engine;

/// Defines all possible errors in Barter core.
pub mod error;

/// Components for initialising multi-exchange execution, routing `ExecutionRequest`s and other
/// execution logic.
pub mod execution;

/// Provides default Barter core Tracing logging initialisers.
pub mod logging;

/// RiskManager interface for reviewing and optionally filtering algorithmic cancel and open
/// order requests.
pub mod risk;

/// Statistical algorithms for analysing datasets, financial metrics and financial summaries.
///
/// eg/ `TradingSummary`, `TearSheet`, `SharpeRatio`, etc.
pub mod statistic;

/// Strategy interfaces for generating algorithmic orders, closing positions, and performing
/// `Engine` actions on disconnect / trading disabled.
pub mod strategy;

/// Utilities for initialising and interacting with a full trading system.
pub mod system;

/// Backtesting utilities.
pub mod backtest;

/// Traits and types related to component shutdowns.
pub mod shutdown;

/// A timed value.
#[derive(
    Debug,
    Copy,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Default,
    Deserialize,
    Serialize,
    Constructor,
)]
pub struct Timed<T> {
    pub value: T,
    pub time: DateTime<Utc>,
}

/// Default [`Engine`](engine::Engine) event that encompasses market events, account/execution
/// events, and `Engine` commands.
///
/// Note that the `Engine` can be configured to process custom events.
///
/// `#[non_exhaustive]`: new engine-driven event variants (e.g. corporate actions beyond stock
/// splits) can be added without breaking downstream exhaustive matches. External matchers must
/// carry a wildcard arm.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize, From)]
#[non_exhaustive]
pub enum EngineEvent<
    MarketKind = DataKind,
    ExchangeKey = ExchangeIndex,
    AssetKey = AssetIndex,
    InstrumentKey = InstrumentIndex,
> {
    Shutdown(Shutdown),
    Command(Command<ExchangeKey, AssetKey, InstrumentKey>),
    TradingStateUpdate(TradingState),
    Account(AccountStreamEvent<ExchangeKey, AssetKey, InstrumentKey>),
    Market(MarketStreamEvent<InstrumentKey, MarketKind>),

    /// Signal that an option contract has expired and settlement should be computed.
    ///
    /// The library handles: cancelling open orders, computing intrinsic-value settlement,
    /// synthesising a closing fill, and setting the `expiration_processed` flag.
    ///
    /// **Caller obligation**: inject this event when `Utc::now() >= option.expiry`.
    /// The handler is idempotent — duplicate events for the same instrument are safe.
    ///
    /// Note: `From` is skipped here because the generic `InstrumentKey` parameter
    /// would conflict with the `From<Shutdown>` impl when `InstrumentKey = Shutdown`.
    /// Construct this variant directly: `EngineEvent::ContractExpiry(key)`.
    #[from(skip)]
    ContractExpiry(InstrumentKey),

    /// Signal that a corporate action (e.g. a stock split) has taken effect on an instrument and
    /// the engine's internal position(s) must be adjusted to match.
    ///
    /// Positions are fill-derived: even in live trading a broker applying a split overnight does
    /// **not** fix the engine's internal `quantity`/`price_entry_average`/`pnl_unrealised`, which
    /// stay on the pre-split scale until something explicitly adjusts them. This event is that
    /// explicit adjustment, used identically in backtest and live.
    ///
    /// The handler is idempotent, keyed on `id` alone (see below), and adjusts **every** open
    /// position for `instrument` (one in netting mode, N in hedging mode). It does not mutate
    /// resting orders — a real broker price-adjusts them, so cancelling engine-side would diverge;
    /// the open orders are surfaced as an observable output instead.
    ///
    /// # Fields
    /// - `id`: a caller-assigned **unique** action identifier (e.g. `"AAPL-2026-06-20-split"`).
    ///   This is the **sole** idempotency key — the handler records processed `id`s per instrument
    ///   and skips (with a warning) any `id` it has already seen.
    /// - `instrument`: the key whose position(s) are adjusted. For a stock split this is the
    ///   underlying equity key; option positions on that underlying are **not** adjusted here and
    ///   are surfaced via a separate observable output.
    /// - `kind`: the market fact (e.g. [`CorporateActionKind::StockSplit`] carrying the ratio).
    /// - `policy`: how to round a fractional resulting **equity** share count
    ///   ([`SplitRoundingPolicy`]) — broker-specific, no default. Governs the **equity** leg only;
    ///   option contract counts are whole integers and are never floored, so a standard option
    ///   adjustment is exact regardless of this `policy`.
    /// - `effective_time`: the resolved instant the adjustment takes effect. In backtest the
    ///   [`HistoricalClock`](engine::clock::HistoricalClock) advances to this instant so the
    ///   adjustment is stamped and ordered exactly (no look-ahead); in live it is honest metadata.
    ///
    /// # Caller obligations
    /// - Assign a unique `id` per action.
    /// - Resolve the ticker to a **valid** engine `InstrumentKey`. The engine indexes it directly
    ///   and **panics on an unknown key** (consistent with the rest of the `EngineEvent` API), so
    ///   validate the key before constructing the event.
    /// - Supply the `policy` (matching the broker's rounding behaviour) and a resolved
    ///   `effective_time` (see [`split_effective_instant`](rustrade_instrument::corporate_action::split_effective_instant)).
    /// - **Inject once**, after the broker has applied the action, before processing new fills on
    ///   the post-split scale.
    /// - A same-day **correction** is expressed as two distinct events with distinct `id`s — a
    ///   reversal (`ratio = 1 / old`) followed by the corrected split — so neither is suppressed
    ///   by the idempotency guard.
    /// - Do **not** inject a `ratio == 1` no-op "split". It is a non-event: the engine applies it as
    ///   a no-op, yet it still classifies as non-standard and emits
    ///   `OptionPositionsRequireIdentityChange` for any option positions on the underlying —
    ///   misleading noise for what changed nothing.
    ///
    /// # Missing last price
    /// Unlike [`ContractExpiry`](Self::ContractExpiry) — which bails and is retryable when the
    /// underlying price is unavailable — a split needs no price for its quantity/basis arithmetic.
    /// The adjustment is therefore **applied** and the `id` **recorded** even when the instrument's
    /// last price is unavailable; `pnl_unrealised` is set to zero (with a warning) and corrected on
    /// the next market tick. This event is **not** retryable.
    ///
    /// Note: `From` is skipped here so the variant is always constructed explicitly — the caller
    /// must consciously supply `id`, `policy`, and `effective_time` (it is not derivable from any
    /// single field).
    #[from(skip)]
    CorporateAction {
        /// Caller-assigned unique action identifier; the sole idempotency key.
        id: SmolStr,
        /// The instrument whose open position(s) are adjusted.
        instrument: InstrumentKey,
        /// The corporate-action market fact (e.g. a stock split ratio).
        kind: CorporateActionKind,
        /// How to round a fractional resulting **equity** share count (broker-specific; no
        /// default). Governs the equity leg only — option contract counts are whole and never floored.
        policy: SplitRoundingPolicy,
        /// The resolved instant the adjustment takes effect (drives the backtest clock).
        effective_time: DateTime<Utc>,
    },
}

impl<MarketKind, ExchangeKey, AssetKey, InstrumentKey> Terminal
    for EngineEvent<MarketKind, ExchangeKey, AssetKey, InstrumentKey>
{
    fn is_terminal(&self) -> bool {
        matches!(self, Self::Shutdown(_))
    }
}

impl<MarketKind, ExchangeKey, AssetKey, InstrumentKey>
    EngineEvent<MarketKind, ExchangeKey, AssetKey, InstrumentKey>
{
    pub fn shutdown() -> Self {
        Self::Shutdown(Shutdown)
    }
}

impl<MarketKind, ExchangeKey, AssetKey, InstrumentKey>
    From<AccountEvent<ExchangeKey, AssetKey, InstrumentKey>>
    for EngineEvent<MarketKind, ExchangeKey, AssetKey, InstrumentKey>
{
    fn from(value: AccountEvent<ExchangeKey, AssetKey, InstrumentKey>) -> Self {
        Self::Account(AccountStreamEvent::Item(value))
    }
}

impl<MarketKind, ExchangeKey, AssetKey, InstrumentKey> From<MarketEvent<InstrumentKey, MarketKind>>
    for EngineEvent<MarketKind, ExchangeKey, AssetKey, InstrumentKey>
{
    fn from(value: MarketEvent<InstrumentKey, MarketKind>) -> Self {
        Self::Market(MarketStreamEvent::Item(value))
    }
}

/// Monotonically increasing event sequence. Used to track `Engine` event processing sequence.
#[derive(
    Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize, Constructor,
)]
pub struct Sequence(pub u64);

impl Sequence {
    pub fn value(&self) -> u64 {
        self.0
    }

    pub fn fetch_add(&mut self) -> Sequence {
        let sequence = *self;
        self.0 += 1;
        sequence
    }
}

/// Barter core test utilities.
#[allow(clippy::unwrap_used)] // Test utilities: callers provide valid inputs
pub mod test_utils {
    use crate::{
        Timed, engine::state::asset::AssetState, statistic::summary::asset::TearSheetAssetGenerator,
    };
    use chrono::{DateTime, Days, TimeDelta, Utc};
    use rust_decimal::Decimal;
    use rustrade_execution::{
        balance::Balance,
        order::id::{OrderId, StrategyId},
        trade::{AssetFees, Trade, TradeId},
    };
    use rustrade_instrument::{
        Side, asset::QuoteAsset, instrument::name::InstrumentNameInternal, test_utils::asset,
    };

    pub fn f64_is_eq(actual: f64, expected: f64, epsilon: f64) -> bool {
        if actual.is_nan() && expected.is_nan() {
            true
        } else if actual.is_infinite() && expected.is_infinite() {
            actual.is_sign_positive() == expected.is_sign_positive()
        } else if actual.is_nan()
            || expected.is_nan()
            || actual.is_infinite()
            || expected.is_infinite()
        {
            false
        } else {
            (actual - expected).abs() < epsilon
        }
    }

    pub fn time_plus_days(base: DateTime<Utc>, plus: u64) -> DateTime<Utc> {
        base.checked_add_days(Days::new(plus)).unwrap()
    }

    pub fn time_plus_secs(base: DateTime<Utc>, plus: i64) -> DateTime<Utc> {
        base.checked_add_signed(TimeDelta::seconds(plus)).unwrap()
    }

    pub fn time_plus_millis(base: DateTime<Utc>, plus: i64) -> DateTime<Utc> {
        base.checked_add_signed(TimeDelta::milliseconds(plus))
            .unwrap()
    }

    pub fn time_plus_micros(base: DateTime<Utc>, plus: i64) -> DateTime<Utc> {
        base.checked_add_signed(TimeDelta::microseconds(plus))
            .unwrap()
    }

    pub fn trade(
        time_exchange: DateTime<Utc>,
        side: Side,
        price: f64,
        quantity: f64,
        fees: f64,
    ) -> Trade<QuoteAsset, InstrumentNameInternal> {
        Trade {
            id: TradeId::new("trade_id"),
            order_id: OrderId::new("order_id"),
            instrument: InstrumentNameInternal::new("instrument"),
            strategy: StrategyId::new("strategy"),
            time_exchange,
            side,
            price: price.try_into().unwrap(),
            quantity: quantity.try_into().unwrap(),
            fees: AssetFees {
                asset: QuoteAsset,
                fees: fees.try_into().unwrap(),
                fees_quote: Some(fees.try_into().unwrap()),
            },
        }
    }

    pub fn asset_state(
        symbol: &str,
        balance_total: f64,
        balance_free: f64,
        time_exchange: DateTime<Utc>,
    ) -> AssetState {
        let balance = Timed::new(
            Balance::new(
                Decimal::try_from(balance_total).unwrap(),
                Decimal::try_from(balance_free).unwrap(),
            ),
            time_exchange,
        );

        AssetState {
            asset: asset(symbol),
            balance: Some(balance),
            statistics: TearSheetAssetGenerator::init(&balance),
        }
    }
}
