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

//! # Barter-Execution
//! Stream private account data from financial venues, and execute (live or mock) orders. Also provides
//! a feature rich MockExchange and MockExecutionClient to assist with backtesting and paper-trading.
//!
//! **It is:**
//! * **Easy**: ExecutionClient trait provides a unified and simple language for interacting with exchanges.
//! * **Normalised**: Allow your strategy to communicate with every real or MockExchange using the same interface.
//! * **Extensible**: Barter-Execution is highly extensible, making it easy to contribute by adding new exchange integrations!
//!
//! See `README.md` for more information and examples.

// Silence unused_crate_dependencies for dev-dependencies used only in tests
#[cfg(test)]
use serial_test as _;
#[cfg(test)]
use tracing_subscriber as _;
#[cfg(test)]
use wiremock as _;

use crate::{
    balance::{AssetBalance, AssetBalanceUpdate},
    error::StreamTerminationReason,
    order::{Order, OrderSnapshot, request::OrderResponseCancel},
    position::Position,
    trade::Trade,
};
use chrono::{DateTime, Utc};
use derive_more::{Constructor, From};
use order::state::OrderState;
use rust_decimal::Decimal;
use rustrade_instrument::{
    asset::{AssetIndex, name::AssetNameExchange},
    exchange::{ExchangeId, ExchangeIndex},
    instrument::{InstrumentIndex, name::InstrumentNameExchange},
};
use rustrade_integration::collection::snapshot::Snapshot;
use serde::{Deserialize, Serialize};

pub mod balance;
pub mod client;
pub mod error;
pub mod exchange;
pub mod fee;
pub use fee::{FeeModel, FeeModelConfig, PerContractFeeModel, PercentageFeeModel, ZeroFeeModel};
pub mod fill;
pub use fill::{BidAskFillModel, FillModel, LastPriceFillModel, MidpointFillModel, SimFillConfig};
pub mod indexer;
pub mod map;
pub mod order;
pub mod position;
pub mod trade;

/// Convenient type alias for an [`AccountEvent`] keyed with [`ExchangeId`],
/// [`AssetNameExchange`], and [`InstrumentNameExchange`].
pub type UnindexedAccountEvent =
    AccountEvent<ExchangeId, AssetNameExchange, InstrumentNameExchange>;

/// Convenient type alias for an [`AccountSnapshot`] keyed with [`ExchangeId`],
/// [`AssetNameExchange`], and [`InstrumentNameExchange`].
pub type UnindexedAccountSnapshot =
    AccountSnapshot<ExchangeId, AssetNameExchange, InstrumentNameExchange>;

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct AccountEvent<
    ExchangeKey = ExchangeIndex,
    AssetKey = AssetIndex,
    InstrumentKey = InstrumentIndex,
> {
    pub exchange: ExchangeKey,
    pub kind: AccountEventKind<ExchangeKey, AssetKey, InstrumentKey>,
}

impl<ExchangeKey, AssetKey, InstrumentKey> AccountEvent<ExchangeKey, AssetKey, InstrumentKey> {
    pub fn new<K>(exchange: ExchangeKey, kind: K) -> Self
    where
        K: Into<AccountEventKind<ExchangeKey, AssetKey, InstrumentKey>>,
    {
        Self {
            exchange,
            kind: kind.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize, From)]
#[non_exhaustive]
pub enum AccountEventKind<ExchangeKey, AssetKey, InstrumentKey> {
    /// Full [`AccountSnapshot`] - replaces all existing state.
    Snapshot(AccountSnapshot<ExchangeKey, AssetKey, InstrumentKey>),

    /// Single [`AssetBalance`] snapshot - replaces existing balance state.
    ///
    /// Sourced from a REST account snapshot: carries the **full** balance including any per-asset
    /// margin debt (`borrowed`/`interest`). This is the authoritative source for debt totals.
    BalanceSnapshot(Snapshot<AssetBalance<AssetKey>>),

    /// Single [`AssetBalanceUpdate`] - applies a WS partial (`free`/`locked` only).
    ///
    /// Sourced from an exchange WS user-data stream (e.g. Binance `outboundAccountPosition`). It
    /// carries **no** margin debt, so applying it updates `free`/`locked` while **preserving** any
    /// existing [`MarginDetails`](balance::MarginDetails) — debt cannot be silently clobbered by a
    /// stream update. Debt totals remain as fresh as the last [`BalanceSnapshot`](Self::BalanceSnapshot).
    BalanceStreamUpdate(Snapshot<AssetBalanceUpdate<AssetKey>>),

    /// Live per-instrument isolated-margin balance update (`free`/`locked` per side).
    ///
    /// The stream counterpart of [`InstrumentAccountSnapshot::isolated`] for venues with per-pair
    /// isolated sub-accounts (e.g. Binance isolated margin). Carries a point-in-time `free`/`locked`
    /// **snapshot** for the pair's `base` and `quote` assets — NOT a delta — keyed by instrument
    /// rather than asset, because isolated balances are per-`(pair, asset)` and cannot be folded
    /// into the asset-keyed balance state without collision (see [`InstrumentBalanceUpdate`]).
    ///
    /// The engine deliberately does **not** store this (the per-asset balance state is informational
    /// only and the engine never reads it for sizing/gating); a consumer reads it off the account
    /// event feed. Debt totals stay as fresh as the last
    /// [`BalanceSnapshot`](Self::BalanceSnapshot) per the debt-freshness contract.
    InstrumentBalanceUpdate(InstrumentBalanceUpdate<AssetKey, InstrumentKey>),

    /// Single [`Order`] snapshot - used to upsert existing order state if it's more recent.
    ///
    /// This variant covers general order updates, and open order responses.
    OrderSnapshot(Snapshot<Order<ExchangeKey, InstrumentKey, OrderState<AssetKey, InstrumentKey>>>),

    /// Response to an [`OrderRequestCancel<ExchangeKey, InstrumentKey>`](order::request::OrderRequestOpen).
    OrderCancelled(OrderResponseCancel<ExchangeKey, AssetKey, InstrumentKey>),

    /// [`Order<ExchangeKey, InstrumentKey, Open>`] partial or full-fill.
    ///
    /// The fee asset (`AssetKey`) may be the quote asset, base asset, or a third-party
    /// asset (e.g., BNB on Binance). Use `fees.fees_quote` for quote-equivalent value
    /// when available.
    Trade(Trade<AssetKey, InstrumentKey>),

    /// WebSocket-level error from exchange. Connection may have dropped.
    ///
    /// Implementations send this when the underlying stream encounters an error.
    /// Consumers should treat this as a signal that events may have been missed
    /// and consider re-syncing via REST (e.g., `fetch_trades`, `account_snapshot`).
    StreamError(String),

    /// The account event stream has ended; no further events will arrive on it.
    ///
    /// This is the in-band, programmatic signal that a stream died — delivered on the **same**
    /// account feed as every other event rather than being inferred from channel EOF or read from
    /// logs. The [`StreamTerminationReason`] distinguishes a venue that exhausted its reconnect
    /// budget from an unrecoverable error, a consumer-side drop, or a graceful shutdown, so the
    /// consumer can apply its own recovery policy (re-establish the stream, re-sync via REST, halt
    /// trading). The library reports *that* and *why* the stream ended; it does not prescribe the
    /// response.
    StreamTerminated(StreamTerminationReason),
}

impl<ExchangeKey, AssetKey, InstrumentKey> AccountEvent<ExchangeKey, AssetKey, InstrumentKey>
where
    AssetKey: Eq,
    InstrumentKey: Eq,
{
    pub fn snapshot(self) -> Option<AccountSnapshot<ExchangeKey, AssetKey, InstrumentKey>> {
        match self.kind {
            AccountEventKind::Snapshot(snapshot) => Some(snapshot),
            _ => None,
        }
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize, Constructor,
)]
pub struct AccountSnapshot<
    ExchangeKey = ExchangeIndex,
    AssetKey = AssetIndex,
    InstrumentKey = InstrumentIndex,
> {
    pub exchange: ExchangeKey,
    pub balances: Vec<AssetBalance<AssetKey>>,
    pub instruments: Vec<InstrumentAccountSnapshot<ExchangeKey, AssetKey, InstrumentKey>>,
}

/// serde `default` for an `Option` field whose inner type is generic — returns `None` without
/// requiring the inner type to be `Default` (see the `isolated` field below).
fn none_option<T>() -> Option<T> {
    None
}

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize, Constructor,
)]
pub struct InstrumentAccountSnapshot<
    ExchangeKey = ExchangeIndex,
    AssetKey = AssetIndex,
    InstrumentKey = InstrumentIndex,
> {
    pub instrument: InstrumentKey,
    #[serde(default = "Vec::new")]
    pub orders: Vec<OrderSnapshot<ExchangeKey, AssetKey, InstrumentKey>>,
    /// Open position for derivative instruments (perpetuals, futures, margin).
    /// `None` for spot instruments where position is implicit in balances.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub position: Option<Position>,
    /// Per-pair isolated-margin balances and risk, for venues with isolated sub-accounts
    /// (e.g. Binance isolated margin). `None` for cross margin, spot, and all other contexts.
    ///
    /// Surfaced here — attached to the instrument — rather than folded into the asset-keyed
    /// [`AccountSnapshot::balances`] because isolated sub-accounts are per-`(pair, asset)`: the same
    /// asset (e.g. `USDT`) in two isolated pairs is a separate pool, which the asset-keyed balance
    /// model cannot represent without collision. The engine does not store this; a consumer reads
    /// it off the snapshot to compose per-pair risk. See [`IsolatedInstrumentState`].
    ///
    // `default = "none_option"` (not a bare `#[serde(default)]`) avoids serde inferring a spurious
    // `AssetKey: Default` bound: a bare default on a generic-typed field conservatively requires the
    // field type to be `Default` (the `position` field escapes this only because `Position` is
    // concrete). Naming a function makes serde *call* it instead, requiring only `AssetKey: Deserialize`.
    #[serde(default = "none_option", skip_serializing_if = "Option::is_none")]
    pub isolated: Option<IsolatedInstrumentState<AssetKey>>,
}

/// Per-pair isolated-margin state attached to an [`InstrumentAccountSnapshot`].
///
/// Binance isolated margin (and other CEX isolated/per-pair sub-accounts) hold balances
/// per-`(pair, asset)`, not per-asset, so they are surfaced attached to the instrument rather than
/// in the asset-keyed [`AccountSnapshot::balances`]. A consumer composes per-pair risk from `base`,
/// `quote`, and `risk`.
#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize, Constructor,
)]
pub struct IsolatedInstrumentState<AssetKey = AssetIndex> {
    /// Base-asset balance of the pair's isolated sub-account (carries per-asset debt).
    pub base: AssetBalance<AssetKey>,
    /// Quote-asset balance of the pair's isolated sub-account (carries per-asset debt).
    pub quote: AssetBalance<AssetKey>,
    /// Per-pair risk metrics — snapshot-fresh, not live (see [`IsolatedMarginRisk`]).
    pub risk: IsolatedMarginRisk,
}

/// Per-pair isolated-margin risk metrics, surfaced on [`IsolatedInstrumentState`].
///
/// Every field is `Option` — a venue may omit any given metric, and a missing metric must not
/// drop the surrounding balance snapshot.
///
/// # Freshness
/// These are **snapshot-only**: authoritative as of the last `account_snapshot` and refreshed on
/// snapshot. Unlike balances, there is **no live-stream twin** — the WS `outboundAccountPosition`
/// frame carries no margin-level / liquidation data. The live signal for risk crossing a threshold
/// is the venue's `marginLevelStatusChange` event (surfaced observably, not accumulated here).
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
pub struct IsolatedMarginRisk {
    /// Margin level of the isolated pair (collateral-to-debt ratio); higher is safer.
    pub margin_level: Option<Decimal>,
    /// Margin ratio of the isolated pair.
    pub margin_ratio: Option<Decimal>,
    /// Estimated liquidation price for the isolated pair.
    pub liquidation_price: Option<Decimal>,
}

/// Live per-instrument isolated-margin balance update payload (the
/// [`AccountEventKind::InstrumentBalanceUpdate`] counterpart of [`IsolatedInstrumentState`]).
///
/// Carries a point-in-time `free`/`locked` **snapshot** for the pair's `base` and `quote` assets —
/// NOT a delta — keyed by instrument. Structurally analogous to [`AssetBalanceUpdate`] but
/// per-instrument: it carries no debt, so applying it keeps `free`/`locked` live while preserving
/// any known per-asset debt (use [`Balance::apply_stream_update`](balance::Balance::apply_stream_update)).
#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize, Constructor,
)]
pub struct InstrumentBalanceUpdate<AssetKey = AssetIndex, InstrumentKey = InstrumentIndex> {
    /// Instrument (isolated pair) the update applies to.
    pub instrument: InstrumentKey,
    /// Base-asset `free`/`locked` update for the pair's isolated sub-account.
    pub base: AssetBalanceUpdate<AssetKey>,
    /// Quote-asset `free`/`locked` update for the pair's isolated sub-account.
    pub quote: AssetBalanceUpdate<AssetKey>,
}

impl<ExchangeKey, AssetKey, InstrumentKey> AccountSnapshot<ExchangeKey, AssetKey, InstrumentKey> {
    pub fn time_most_recent(&self) -> Option<DateTime<Utc>> {
        let order_times = self.instruments.iter().flat_map(|instrument| {
            instrument
                .orders
                .iter()
                .filter_map(|order| order.state.time_exchange())
        });
        let balance_times = self.balances.iter().map(|balance| balance.time_exchange);

        order_times.chain(balance_times).max()
    }

    pub fn assets(&self) -> impl Iterator<Item = &AssetKey> {
        self.balances.iter().map(|balance| &balance.asset)
    }

    pub fn instruments(&self) -> impl Iterator<Item = &InstrumentKey> {
        self.instruments.iter().map(|snapshot| &snapshot.instrument)
    }
}
