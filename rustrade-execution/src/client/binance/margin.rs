//! Binance Margin execution client (cross and isolated).
//!
//! [`BinanceMargin`] is a margin counterpart to [`super::spot::BinanceSpot`]. It shares the
//! exchange-agnostic infrastructure in [`super::shared`] (rate-limit tracking, reconnect/backoff,
//! event deduplication, error parsing) and implements the same `ExecutionClient` trait so callers
//! do not branch on spot-vs-margin transport.
//!
//! ## Scope
//! This module provides the client's identity and configuration ([`BinanceMargin`],
//! [`BinanceMarginConfig`], [`MarginEffect`]) and a full [`ExecutionClient`] implementation:
//! order submission/cancel and account snapshot / balance / open-order / trade queries over REST,
//! plus a live account event stream ([`ExecutionClient::account_stream`]) over Binance's margin
//! user-data stream (a listen key from `POST /sapi/v2/margin/listen-key`), which carries fills,
//! orders, balances, margin debt and margin notices, and after a reconnect recovers missed fills
//! and reports how orders ended meanwhile. It also
//! implements [`OrderStatusClient`], looking orders up by client order id, and reports a pair's
//! borrow terms ([`ShortabilityClient`]) and how much this account can borrow for it
//! ([`BorrowCapacityClient`]). Two reads beyond the traits cover what the account stream does not
//! carry: the interest charged ([`BinanceMargin::fetch_margin_interest`]) and forced liquidations
//! ([`BinanceMargin::fetch_forced_liquidations`]).
//!
//! The client supports both **cross** (`isIsolated = "FALSE"`, account-wide collateral) and
//! **isolated** (`isIsolated = "TRUE"`, per-pair sub-accounts) margin, selected by
//! [`BinanceMarginConfig::is_isolated`].
//!
//! ## Cross vs. isolated
//! Cross is account-wide, with per-asset balances surfaced in the top-level `balances`. Isolated
//! is per-pair: the caller declares the [`BinanceMarginConfig::isolated_symbols`] universe.
//! Binance's one listen key covers the cross account and every isolated pair alike, and the
//! account stream keeps only the frames of the account the client is configured for. Isolated
//! per-pair balances and risk are attached per-instrument (never folded into the asset-keyed
//! model, which cannot represent `(pair, asset)` pools). See [`BinanceMargin`] for the per-method
//! contract.
//!
//! ## Borrow/repay
//! Margin orders carry a `sideEffectType` controlling whether the venue borrows when the order is
//! placed and whether it repays from the order's proceeds. A client has a default
//! [`MarginEffect`] ([`BinanceMarginConfig::margin_effect`], `NoSideEffect` unless set), and a
//! request may carry its own (`RequestOpen::margin_effect`). Binance retired the combined
//! `AUTO_BORROW_REPAY` on 2026-10-14, so a position is opened with `Borrow` (`MARGIN_BUY`) and
//! closed with `Repay` (`AUTO_REPAY`), each per order.
//!
//! ## No testnet
//! Binance margin/SAPI exists on **no** testnet — the SDK exposes only
//! `MarginTradingRestApi::production`. A `testnet: true` config is therefore inert for margin and
//! always resolves to production endpoints; the constructor logs a warning so this is observable
//! rather than silent. See [`BinanceMarginConfig::testnet`].
//!
//! ## Known limitations
//! - `balanceUpdate` events (deposits/withdrawals) are not forwarded. Callers reconcile balances
//!   after external transfers via [`ExecutionClient::fetch_balances`] or
//!   [`ExecutionClient::account_snapshot`].
//! - A `TRADE` report whose order status (`X`) is neither `PARTIALLY_FILLED` nor `FILLED` emits
//!   the execution but no order snapshot, so a fill arriving after its order's terminal report
//!   cannot resurrect a retired order as a resting one. That order's filled quantity is settled
//!   instead by the terminal report's own `z`, or by
//!   [`ExecutionClient::fetch_open_orders`].

mod records;
mod stream;

pub use records::MarginForcedLiquidation;

use super::restatement::{SharedStreamClock, restatement_time};
use super::rules::RulesLoader;
use super::shared::{
    AbortOnDropStream, BINANCE_MAX_TRADES, BinanceOrderType, BinanceTimeInForce,
    FILL_RECOVERY_TIMEOUT_SECS, MyTradesFrom, ORDER_EXECUTIONS_BUDGET, OpenOrderListing,
    PlacementResponse, RateLimitTracker, RequestKind, SharedDedupCache, TrailingOrderError,
    UnrecoveredFills, WeightPool, WireInt, answer_time, balance_read_time, binance_filled_qty,
    classify_order_kind_tif, classify_rest_order_error, classify_rest_query_error,
    convert_ended_order, convert_open_order_listing, convert_open_order_owned_symbol,
    dedup_key_from_event, drop_after, gap_failed, gap_time, is_duplicate, is_unknown_order,
    new_dedup_cache, placed_order_state, recovered_order_cids, recovered_order_totals,
    response_decode_error, rest_call_with_retry, rest_read_with_retry, trailing_params,
    unreadable_answer,
};
use crate::rules::{OrderRules, PreSendRules};
use crate::{
    AccountEventKind, AccountSnapshot, FillRecoveryFailure, InstrumentAccountSnapshot,
    InstrumentBalanceSnapshot, IsolatedInstrumentState, IsolatedMarginRisk, UnindexedAccountEvent,
    UnindexedAccountSnapshot,
    balance::{AssetBalance, Balance},
    borrow_capacity::BorrowCapacity,
    client::{
        BorrowCapacityClient, ExecutionClient, OrderStatusClient, ShortabilityClient,
        order_recovery::{
            KnownLiveOrders, MAX_KNOWN_LIVE_ORDERS, OpenListing, OrderLookup,
            SharedKnownLiveOrders, UncheckedOrders, fetch_ended_by_key, recover_ended_orders,
        },
    },
    error::{ApiError, OrderError, UnindexedClientError},
    margin::{MarginBorrow, MarginEffect, MarginPlacement},
    order::{
        Order, OrderKey, OrderKind, TimeInForce, UnindexedInactiveOrder, UnindexedOrderKey,
        id::{ClientOrderId, OrderId, StrategyId, VenueOrderId},
        request::{OrderRequestCancel, OrderRequestOpen, UnindexedOrderResponseCancel},
        state::{Cancelled, Open, OrderState, UnindexedOrderState},
    },
    position::PositionReport,
    shortability::Shortability,
    trade::{AssetFees, TimeSource, Trade, TradeId, TradesRead},
};
use binance_sdk::{
    common::{config::ConfigurationRestApi, constants::MARGIN_TRADING_REST_API_PROD_URL},
    margin_trading::{
        MarginTradingRestApi,
        rest_api::{
            GetAllCrossMarginPairsParams, GetAllIsolatedMarginSymbolParams,
            GetAllMarginAssetsParams, GetDelistScheduleParams,
            GetFutureHourlyInterestRateIsIsolatedEnum, GetFutureHourlyInterestRateParams,
            MarginAccountCancelOrderIsIsolatedEnum, MarginAccountCancelOrderParams,
            MarginAccountNewOrderIsIsolatedEnum, MarginAccountNewOrderNewOrderRespTypeEnum,
            MarginAccountNewOrderParams, MarginAccountNewOrderSideEffectTypeEnum,
            MarginAccountNewOrderSideEnum, MarginAccountNewOrderTimeInForceEnum,
            MarginAccountNewOrderTypeEnum, QueryCrossMarginAccountDetailsParams,
            QueryCrossMarginAccountDetailsResponseUserAssetsInner,
            QueryIsolatedMarginAccountInfoParams,
            QueryIsolatedMarginAccountInfoResponseAssetsInner,
            QueryMarginAccountsOpenOrdersIsIsolatedEnum, QueryMarginAccountsOpenOrdersParams,
            QueryMarginAccountsOrderIsIsolatedEnum, QueryMarginAccountsOrderParams,
            QueryMarginAccountsTradeListIsIsolatedEnum, QueryMarginAccountsTradeListParams,
            QueryMarginAccountsTradeListResponseInner, QueryMaxBorrowParams, RestApi,
        },
    },
};
use chrono::{DateTime, TimeZone, Utc};
use fnv::FnvHashSet;
use futures::stream::BoxStream;
use rust_decimal::Decimal;
use rustrade_instrument::{
    Side,
    asset::name::AssetNameExchange,
    exchange::ExchangeId,
    instrument::{kind::InstrumentKindDiscriminant, name::InstrumentNameExchange},
};
use serde::Deserialize;
use smol_str::format_smolstr;
use std::{
    collections::{BTreeMap, HashMap},
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use stream::{MarginStream, STREAM_URL, SapiListenKeys, StreamAccount};
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

// ---------------------------------------------------------------------------
// Margin sideEffectType
// ---------------------------------------------------------------------------

/// `effect` as the margin SDK's `sideEffectType` parameter enum: `NO_SIDE_EFFECT`, `MARGIN_BUY`
/// or `AUTO_REPAY`.
///
/// The SDK also has `AUTO_BORROW_REPAY`, which Binance retired on 2026-10-14 by downgrading it to
/// `MARGIN_BUY`; it is never sent. Exhaustive, so an effect added to [`MarginEffect`] must say
/// here what it sends.
fn binance_side_effect(effect: MarginEffect) -> MarginAccountNewOrderSideEffectTypeEnum {
    match effect {
        MarginEffect::NoSideEffect => MarginAccountNewOrderSideEffectTypeEnum::NoSideEffect,
        MarginEffect::Borrow => MarginAccountNewOrderSideEffectTypeEnum::MarginBuy,
        MarginEffect::Repay => MarginAccountNewOrderSideEffectTypeEnum::AutoRepay,
    }
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for the [`BinanceMargin`] execution client.
///
/// Mirrors [`BinanceSpotConfig`](super::spot::BinanceSpotConfig)'s credential handling (private
/// keys, secret-redacting [`Debug`], `Deserialize`-only) and adds margin-specific knobs.
// Serialize intentionally omitted — would expose secret_key in plaintext
#[derive(Clone, Deserialize)]
pub struct BinanceMarginConfig {
    // not pub — prevents accidental credential exposure via struct access.
    // Use BinanceMarginConfig::new() to construct, or deserialize from config file.
    api_key: String,
    secret_key: String,
    /// Use testnet endpoints instead of production.
    ///
    /// **Inert for margin:** Binance margin/SAPI has no testnet, so this field is always treated
    /// as production. Retained to mirror the spot config shape; [`BinanceMargin::new`] warns if it
    /// is set to `true`.
    pub testnet: bool,
    /// Isolated margin (per-pair sub-accounts) when `true`; cross margin (account-wide) when
    /// `false`.
    ///
    /// Defaults to `false` (cross). When `true`, [`isolated_symbols`](Self::isolated_symbols) must
    /// be non-empty — it declares the pairs to snapshot/stream; [`BinanceMargin::new`] panics
    /// otherwise.
    #[serde(default)]
    pub is_isolated: bool,
    /// The isolated-margin pairs this client snapshots and streams when
    /// [`is_isolated`](Self::is_isolated) is `true`.
    ///
    /// This is the authoritative symbol universe for the isolated account stream, which keeps the
    /// frames of these pairs and drops those of any other. Each pair's base and quote asset are
    /// read when the stream opens, so the set is fixed for the stream's lifetime: a pair added
    /// *after* `account_stream` is called is **not** picked up — reconfigure and restart the
    /// stream for it.
    ///
    /// Ignored for cross margin (`is_isolated = false`). Empty by default; a `true` +
    /// empty-set combination is a misconfiguration that panics in [`BinanceMargin::new`].
    #[serde(default)]
    pub isolated_symbols: Vec<InstrumentNameExchange>,
    /// The [`MarginEffect`] of an order whose request carries none.
    ///
    /// Defaults to [`MarginEffect::NoSideEffect`], Binance's own default, so that no order takes
    /// on debt unless the caller asks. A `side_effect` key from an earlier version is ignored.
    #[serde(default)]
    pub margin_effect: MarginEffect,
    /// Whether the debt of an order that is cancelled before it fills is repaid, as Binance's
    /// `autoRepayAtCancel`. Sent with every [`MarginEffect::Borrow`] order, and with no other.
    ///
    /// Defaults to `true`, Binance's default. Binance suggests `false` for a strategy that places
    /// and cancels at high frequency: the repay is a request of its own. With it off, a cancelled
    /// order's loan stays owed until a [`MarginEffect::Repay`] order repays it.
    #[serde(default = "crate::margin::default_auto_repay_at_cancel")]
    pub auto_repay_at_cancel: bool,
    /// Whether an order is checked against its pair's tick, lot and notional rules before it is
    /// sent. Defaults to [`PreSendRules::Enforce`].
    ///
    /// Binance margin publishes no rules of its own: the check reads the pair's spot rules, which
    /// a margin order obeys.
    #[serde(default)]
    pub pre_send_rules: PreSendRules,
}

// custom Debug to avoid leaking credentials in logs
impl std::fmt::Debug for BinanceMarginConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BinanceMarginConfig")
            .field("api_key", &"***")
            .field("secret_key", &"***")
            .field("testnet", &self.testnet)
            .field("is_isolated", &self.is_isolated)
            .field("isolated_symbols", &self.isolated_symbols)
            .field("margin_effect", &self.margin_effect)
            .field("auto_repay_at_cancel", &self.auto_repay_at_cancel)
            .field("pre_send_rules", &self.pre_send_rules)
            .finish()
    }
}

impl BinanceMarginConfig {
    /// Construct a [`BinanceMarginConfig`] with explicit control over every field.
    ///
    /// Prefer [`BinanceMarginConfig::cross_margin`] / [`BinanceMarginConfig::isolated`] for the
    /// common cases, or deserialize from a config file to keep credentials out of source. The
    /// positional args expose every field for full control; defaultable knobs (`is_isolated`,
    /// `isolated_symbols`, `margin_effect`, `auto_repay_at_cancel`) also default via
    /// `#[serde(default)]` on the file path.
    ///
    /// Note: this does not itself validate `is_isolated` against `isolated_symbols`; that gate
    /// lives in [`BinanceMargin::new`] (it must also cover the `Deserialize`-only path).
    pub fn new(
        api_key: String,
        secret_key: String,
        testnet: bool,
        is_isolated: bool,
        isolated_symbols: Vec<InstrumentNameExchange>,
        margin_effect: MarginEffect,
        auto_repay_at_cancel: bool,
    ) -> Self {
        Self {
            api_key,
            secret_key,
            testnet,
            is_isolated,
            isolated_symbols,
            margin_effect,
            auto_repay_at_cancel,
            pre_send_rules: PreSendRules::default(),
        }
    }

    /// Set whether an order is checked against its pair's rules before it is sent.
    #[must_use]
    pub fn with_pre_send_rules(mut self, pre_send_rules: PreSendRules) -> Self {
        self.pre_send_rules = pre_send_rules;
        self
    }

    /// Convenience constructor for the common case: cross margin, production endpoints, and the
    /// default [`MarginEffect::NoSideEffect`].
    ///
    /// Equivalent to [`new`](Self::new) with `testnet = false`, `is_isolated = false`, no
    /// `isolated_symbols`, the default `margin_effect` and `auto_repay_at_cancel = true`. Use
    /// [`new`](Self::new) when any of those need to differ.
    pub fn cross_margin(api_key: String, secret_key: String) -> Self {
        Self::new(
            api_key,
            secret_key,
            false,
            false,
            Vec::new(),
            MarginEffect::default(),
            true,
        )
    }

    /// Convenience constructor for isolated margin: production endpoints, the
    /// default [`MarginEffect::NoSideEffect`], and the given per-pair symbol universe (see
    /// [`isolated_symbols`](Self::isolated_symbols)).
    ///
    /// `symbols` should be non-empty: an isolated client with no symbols has nothing to
    /// snapshot or stream and [`BinanceMargin::new`] panics on it. Use [`new`](Self::new) to
    /// override `margin_effect`.
    pub fn isolated(
        api_key: String,
        secret_key: String,
        symbols: Vec<InstrumentNameExchange>,
    ) -> Self {
        Self::new(
            api_key,
            secret_key,
            false,
            true,
            symbols,
            MarginEffect::default(),
            true,
        )
    }

    /// Read-only access to the API key (e.g. for logging or header construction).
    pub fn api_key(&self) -> &str {
        &self.api_key
    }
}

// ---------------------------------------------------------------------------
// BinanceMargin client
// ---------------------------------------------------------------------------

/// Binance Margin (cross and isolated) execution client using the official binance-sdk.
///
/// Places orders and queries account state over the margin REST API
/// (`margin_trading::rest_api`), and streams live account events over Binance's margin
/// user-data stream, read with a listen key from `POST /sapi/v2/margin/listen-key`.
///
/// See [`BinanceMarginConfig::testnet`] for the no-testnet caveat. The behaviour a caller most needs to know
/// is summarised below, with links to the authoritative detail.
///
/// # Borrow/repay (`sideEffectType`)
/// A client has a default [`MarginEffect`] ([`BinanceMarginConfig::margin_effect`], by default
/// `NoSideEffect`, so no order takes on debt unless asked), and each request may carry its own
/// (`RequestOpen::margin_effect`). Open a short or a margin long with `Borrow` (`MARGIN_BUY`),
/// which borrows what the order lacks **when it is placed**, and close it with `Repay`
/// (`AUTO_REPAY`), which repays from the proceeds. Binance retired `AUTO_BORROW_REPAY` on
/// 2026-10-14, so there is no longer one value that does both. A **mis-sized `Borrow` order
/// borrows the shortfall**, or the asset's minimum borrow if that is more, without further opt-in:
/// see [`MarginEffect::Borrow`] for the rule, and [`open_order`](Self::open_order).
/// [`open_margin_order`](Self::open_margin_order) also reports what the order borrowed.
///
/// # Cross vs. isolated margin
/// The mode is fixed per client via [`BinanceMarginConfig::is_isolated`]:
/// - **Cross** (`is_isolated = false`, `isIsolated = "FALSE"`, account-wide collateral): per-asset
///   balances (incl. debt) are surfaced account-wide in the top-level `balances`.
/// - **Isolated** (`is_isolated = true`): per-pair sub-accounts. Balances + risk are attached
///   **per-instrument** via [`InstrumentAccountSnapshot::isolated`] (the asset-keyed top-level
///   `balances` is left empty, since `(pair, asset)` slots would collide), and per-symbol queries
///   are scoped to the configured [`BinanceMarginConfig::isolated_symbols`]. See
///   [`account_snapshot`](Self::account_snapshot) for the full per-method semantics.
///
/// # Trailing stops
/// As on spot, a `TrailingStop` is placed as a `STOP_LOSS` with a `trailingDelta`, and a
/// `TrailingStopFixedLimit` as a `STOP_LOSS_LIMIT` with a `trailingDelta` and its fixed limit as
/// `price`, for `BasisPoints` and `Percentage` offsets that come to a whole number of basis
/// points. An activation price is sent as `stopPrice`. Absolute offsets and `TrailingStopLimit`
/// return [`OrderError::UnsupportedOrderType`]. See [`open_order`](Self::open_order).
///
/// The account mode matters for `TrailingStop`. A **cross** account refuses a trailing
/// `STOP_LOSS` with `-3111` ("Trailing stop order only supports limit type"), which this client
/// returns as [`OrderError::UnsupportedOrderType`]; an isolated account has accepted it.
/// `TrailingStopFixedLimit` is accepted on both, so it is the form to place on a cross account.
/// The client does not refuse a `TrailingStop` unsent: Binance's answer decides, so the client
/// carries no per-mode rule that could fall out of date.
///
/// # Stop and take-profit orders without a limit
/// Since 2026-10-14 Binance has refused `STOP_LOSS` and `TAKE_PROFIT` orders on margin with
/// `-3116` ("not supported for this account"), and has not said when it will take them again.
/// That covers a `Stop`, a `TakeProfit` and a `TrailingStop`, which this client places as those
/// types. The refusal comes back as [`OrderError::UnsupportedOrderType`], after the round trip:
/// place the limit form instead, `StopLimit`, `TakeProfitLimit` or `TrailingStopFixedLimit`,
/// which is not affected. The client does not refuse these orders unsent, so they go through
/// again once Binance takes them.
///
/// # Conditional orders Binance does not describe
/// Binance margin accepts a `trailingDelta` on its conditional types (`STOP_LOSS`,
/// `STOP_LOSS_LIMIT`, `TAKE_PROFIT`, `TAKE_PROFIT_LIMIT`), on cross and isolated accounts, though
/// a cross account refuses a trailing `STOP_LOSS` (see "Trailing stops" above). Two of its reports
/// never say whether such an order trails:
/// - the REST order queries this client reads (`openOrders`, `order`, `allOrders`) return neither
///   `trailingDelta` nor `trailingTime` (#541), on either account;
/// - an isolated account's stream sends an empty trailing delta (`"d": ""`) for every order,
///   trailing or not. A cross account's stream sends the delta of a trailing order, and an empty
///   one only for an order that does not trail.
///
/// A trailing order placed without an activation price reads `stopPrice` `"0"` in both.
///
/// Such an order is reported with the kind this client knows it by: the kind it was placed with,
/// known from the moment the order is sent, since the stream may report it before the answer to
/// placing it arrives, or the one a report that did describe it gave, for every order the client
/// placed, listed or saw while live, or else a kind the caller supplied with
/// [`recall_order_kinds`](Self::recall_order_kinds). An order it knows no kind for, such as one
/// placed elsewhere or before a restart, is reported as [`OrderKind::ConditionalUnresolved`], with
/// its type's family and its `stopPrice` as `trigger_price`, logged at `warn`, rather than
/// described as a fixed or a trailing order it may not be. So
/// [`fetch_open_orders`](Self::fetch_open_orders), [`account_snapshot`](Self::account_snapshot),
/// the [`account_stream`](Self::account_stream) and
/// [`fetch_ended_orders`](crate::client::OrderStatusClient::fetch_ended_orders) list every live
/// conditional order, and report every ended one as ended, so that a stop that fired is not held
/// as live.
///
/// # User-data stream
/// [`account_stream`](Self::account_stream) reads Binance's margin user-data stream with a listen
/// key from `POST /sapi/v2/margin/listen-key`, kept alive as each connection opens and every 30
/// minutes after. Since 2026-10-14
/// Binance refuses the earlier margin streams' credentials (`POST /sapi/v1/userListenToken` and
/// `POST /sapi/v1/margin/listen-key`) with `-3086` for new accounts and for accounts it has
/// migrated, and the v2 key is open to every account. One stream carries fills, orders, balances,
/// margin debt and margin notices, for the cross account and every isolated pair alike.
///
/// # Margin balances & debt-freshness
/// Balances carry per-asset margin debt: [`Balance::net_asset`](crate::balance::Balance::net_asset)
/// returns `total - borrowed`, with `borrowed`/`interest` exposed via
/// [`MarginDetails`](crate::balance::MarginDetails). Authoritative debt totals come from the REST
/// [`account_snapshot`](Self::account_snapshot) (`BalanceSnapshot`). The WS stream keeps
/// `free`/`locked` live via `BalanceStreamUpdate`, which **never** clobbers debt, and keeps debt
/// live via [`AccountEventKind::MarginDebt`], one per asset a `liabilityChange` lists, on a cross
/// account: Binance has not been seen to send that event for an isolated pair. A debt change sent
/// while the stream is down is not recovered, so call `account_snapshot` at startup and after a
/// reconnect (see [`account_stream`](Self::account_stream)'s cold-start note).
///
/// As Binance documents it, margin interest is charged hourly on what is borrowed and accrues as
/// the asset's unpaid `interest`, which comes out of `total` only when it is repaid. `net_asset`
/// deducts `borrowed` alone, so until then `interest` is the only figure that holds it, as fresh
/// as the rest of the debt. Fills carry no interest, so PnL computed from them excludes it.
///
/// # Rate limits
/// Once a `/sapi` response reports at least 90% of the documented per-IP weight limit (12000 per
/// minute) used, queries wait for the next minute, so the rest is left for orders and cancels,
/// which never wait for it. A query, including a [`fetch_open_orders`](Self::fetch_open_orders)
/// after a reconnect, can therefore take up to about a minute longer. A reconnect's fill recovery
/// and its listen-key request do not wait for it either, since a pause could outlast the
/// recovery's 30 s and delay its fills to a retry. The per-UID `/sapi` limit is not tracked.
/// After Binance answers with a rate-limit error, every REST call waits out a cooldown as long as
/// its `Retry-After` asked, or 10 s without one. A call facing more than 10 s of it is not sent:
/// it fails at once with [`ApiError::RateLimit`] carrying the time left, rather than hanging
/// through a ban that can last days. A 418 (an IP ban) is never retried in place.
///
/// The account stream follows the same rule. It does not open, or reconnect, during a cooldown:
/// it waits out one of 10 s or less, without spending a reconnect attempt, and a longer one fails
/// the open with [`ApiError::RateLimit`], unsent, or ends a running stream with
/// [`StreamTerminationReason::RateLimited`](crate::error::StreamTerminationReason::RateLimited),
/// carrying the time left. A connect or subscribe Binance refuses for a rate limit starts a
/// cooldown, as a REST refusal does. A refused handshake names no wait, so a 429 starts the 10 s
/// default, and handshakes refused that way spend the
/// [reconnect budget](super#account-stream-reconnection) as any failure does; a 418, an IP ban,
/// starts Binance's shortest ban, 2 minutes, and so ends the stream.
///
/// # One client per engine (`ExchangeId`)
/// All emitted events — cross and isolated alike — are stamped [`ExchangeId::BinanceMargin`], and
/// the engine routes `AssetStates` / `ConnectivityStates` by `ExchangeId`. Running **two**
/// `BinanceMargin` clients in a single engine (e.g. one cross, one isolated) therefore collides
/// their exchange identity. A single engine should run **at most one** `BinanceMargin` client; a
/// consumer needing cross and isolated concurrently runs them in separate engines.
///
/// # Fills of other clients' orders
/// The account stream, fill recovery and `fetch_trades` report every fill on the account, whoever
/// placed the order, and Binance does not say who did. A consumer that also trades the account
/// outside the engine filters those fills itself, for example by
/// [`Trade::client_order_id`](crate::trade::Trade::client_order_id). See the
/// [module docs](super#every-fill-on-the-account-is-delivered).
#[derive(Clone)]
pub struct BinanceMargin {
    config: Arc<BinanceMarginConfig>,
    // REST client for cancels and queries (account snapshots, balances, trades).
    rest: Arc<RestApi>,
    // Production-configured REST config (credentials + base path) for the new-order and cancel
    // requests, sent through the SDK's `common::utils::send_request` rather than the typed
    // `RestApi` surface, since the SDK's response types cannot decode their answers (see
    // `NewOrderAnswer` and `CancelOrderAnswer`).
    rest_config: Arc<ConfigurationRestApi>,
    // shared rate-limit tracker across all REST calls
    rate_limiter: Arc<RateLimitTracker>,
    // The orders seen live and not yet seen end, which a reconnect asks about. Shared by every
    // clone and every account stream, since an order placed through one ends on any of them.
    known_live: SharedKnownLiveOrders,
    // The pairs' tick, lot and notional rules, loaded on each pair's first order from the spot
    // `exchangeInfo`. Shared by every clone.
    rules: Arc<RulesLoader>,
}

impl std::fmt::Debug for BinanceMargin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BinanceMargin")
            .field("testnet", &self.config.testnet)
            .field("is_isolated", &self.config.is_isolated)
            .field("margin_effect", &self.config.margin_effect)
            .finish_non_exhaustive()
    }
}

impl BinanceMargin {
    /// Tell the client the kinds of orders it may not know, by client order id, such as after a
    /// restart, so that it reports them with these kinds where Binance's reports cannot describe
    /// them.
    ///
    /// Binance's margin REST order queries never say whether a conditional order (`STOP_LOSS`,
    /// `STOP_LOSS_LIMIT`, `TAKE_PROFIT`, `TAKE_PROFIT_LIMIT`) trails, and neither does an
    /// isolated account's stream. The client reports such an order with the kind it knows it by,
    /// from placing it or from a report that described it, and otherwise as
    /// [`OrderKind::ConditionalUnresolved`]. A kind recalled here fills that gap: it is reported
    /// for its order when Binance's report leaves the kind unresolved and the client has learned
    /// no resolved kind itself. Call it before [`account_snapshot`](ExecutionClient::account_snapshot),
    /// [`fetch_open_orders`](ExecutionClient::fetch_open_orders) or
    /// [`account_stream`](ExecutionClient::account_stream) to have their first reports use it.
    /// It can be called again at any time, and adds to what earlier calls supplied.
    ///
    /// Each kind must be the one its order was placed with: it is reported as given, unchecked
    /// against the order's Binance type.
    ///
    /// A kind is kept until the client sees its order end: on its account stream, on cancelling
    /// it, or in the check the stream runs after a reconnect.
    /// [`fetch_ended_orders`](OrderStatusClient::fetch_ended_orders) does not drop it. One for an
    /// order this client has recently seen end is ignored. So is an unresolved kind, and anything
    /// past 4096 held kinds, with a warning. The client keeps nothing across restarts: persisting
    /// the kinds of the orders it places is the caller's.
    ///
    /// `kinds` is collected before the client's order set is locked, so it may call this client.
    pub fn recall_order_kinds(&self, kinds: impl IntoIterator<Item = (ClientOrderId, OrderKind)>) {
        let kinds: Vec<_> = kinds.into_iter().collect();
        // The guard is dropped at the end of the statement, so the warning waits for no lock.
        let ignored = self.known_live.lock().recall(kinds);
        if ignored > 0 {
            warn!(
                ignored,
                limit = MAX_KNOWN_LIVE_ORDERS,
                "Binance margin: recalled order kinds past the limit are ignored"
            );
        }
    }

    /// Load the order rules of `instruments` from Binance now, and hold them for
    /// [`open_order`](ExecutionClient::open_order) to check against.
    ///
    /// Rules otherwise load on each pair's first order, which adds a request to that order.
    /// Calling this ahead takes it off the first order, and shows a bad symbol or a failed load
    /// here. Binance refuses the whole request if one symbol is not listed. The rules are the
    /// pair's spot rules, which Binance margin has no endpoint for.
    pub async fn load_rules(
        &self,
        instruments: &[InstrumentNameExchange],
    ) -> Result<HashMap<InstrumentNameExchange, OrderRules>, UnindexedClientError> {
        self.rules.load(instruments).await
    }

    /// Submit a margin order over the SAPI `POST /sapi/v1/margin/order` endpoint, and report
    /// what it borrowed.
    ///
    /// [`ExecutionClient::open_order`] returns this method's [`MarginPlacement::order`]. Call
    /// this one to learn what an order placed with [`MarginEffect::Borrow`] borrowed, which
    /// Binance reports only in its answer to the order (`marginBuyBorrowAmount`,
    /// `marginBuyBorrowAsset`): see [`MarginPlacement::borrowed`]. Binance leaves them out when
    /// the free balance covered the order. The amount is what Binance reports lending, which need
    /// not equal the order's shortfall: it is at least the asset's minimum borrow (see
    /// [`MarginEffect::Borrow`]), so read it rather than compute it from the balance.
    ///
    /// Mirrors [`BinanceSpot::open_order`](super::spot::BinanceSpot)'s contract: every failure is
    /// folded into the returned order's state as [`OrderState::inactive`], so the engine always
    /// sees an outcome.
    ///
    /// Margin specifics:
    /// - `sideEffectType` is the request's [`MarginEffect`], else the client's
    ///   [`BinanceMarginConfig::margin_effect`]: `NO_SIDE_EFFECT`, `MARGIN_BUY` for `Borrow` and
    ///   `AUTO_REPAY` for `Repay`.
    /// - A `Borrow` borrows when the order is **placed**, not when it fills, and nothing repays
    ///   it from the order's proceeds: close the position with a `Repay` order.
    /// - `autoRepayAtCancel` is sent with `Borrow` only, from
    ///   [`BinanceMarginConfig::auto_repay_at_cancel`]: Binance applies it to `MARGIN_BUY` alone.
    /// - An order with `Borrow` has a request weight of 1500 against the account's UID limit,
    ///   against 6 for the other effects. Place them accordingly.
    /// - `isIsolated` is config-driven (`"TRUE"` for isolated, `"FALSE"` for cross).
    /// - A `TrailingStop` is sent as a `STOP_LOSS` with `trailingDelta` in basis points, and a
    ///   `TrailingStopFixedLimit` as a `STOP_LOSS_LIMIT` with `trailingDelta` and its fixed limit
    ///   as `price`; an activation price goes as `stopPrice`. An Absolute offset, and
    ///   `TrailingStopLimit`, return [`OrderError::UnsupportedOrderType`]; an offset that is not a
    ///   positive whole number of basis points returns [`OrderError::InvalidPrecision`] (see the
    ///   type-level docs). A non-positive activation price, or a `TrailingStopFixedLimit` without
    ///   a positive `price`, is refused unsent as [`OrderError::Rejected`].
    /// - A **cross** account refuses the `STOP_LOSS` form of a `TrailingStop` with `-3111`, which
    ///   comes back as [`OrderError::UnsupportedOrderType`] after the round trip. Place a
    ///   `TrailingStopFixedLimit` there instead: it is accepted on both account modes.
    /// - A successful answer that cannot be read (one that does not decode, has no `orderId`, or
    ///   reports a borrowed amount without its asset or the other way round) returns
    ///   [`OrderError::Connectivity`]: the venue acted on the request, so the order may be live,
    ///   and its outcome is unknown rather than refused.
    /// - The order's time is the answer's `transactTime`. An answer without a readable one is
    ///   still read, at the local time, and logged with a `warn!`.
    /// - The order's kind is known to the client from the moment its request is sent, so a stream
    ///   report that arrives before the answer is described by it. If the future is dropped after
    ///   the request was sent, the order may be live: its kind is kept until the client sees the
    ///   order end, as one from [`recall_order_kinds`](Self::recall_order_kinds) is, and counts
    ///   toward the same limit. That includes a drop while waiting to retry a rate-limited
    ///   request, which Binance refused, since the client cannot tell the two apart.
    pub async fn open_margin_order(
        &self,
        request: OrderRequestOpen<ExchangeId, &InstrumentNameExchange>,
    ) -> MarginPlacement {
        let instrument = request.key.instrument.clone();
        let side = request.state.side;
        let price = request.state.price;
        let quantity = request.state.quantity;
        let kind = request.state.kind;
        let time_in_force = request.state.time_in_force;
        let cid = request.key.cid.clone();

        let order_key = OrderKey::new(
            ExchangeId::BinanceMargin,
            instrument.clone(),
            request.key.strategy.clone(),
            cid.clone(),
        );

        // Build the returned placement with a given inactive (failure) state and no loan,
        // preserving the request fields — keeps the many early-return error paths to one line each.
        let inactive = |state: UnindexedOrderState| MarginPlacement {
            order: Order {
                key: order_key.clone(),
                side,
                price,
                quantity,
                kind,
                time_in_force,
                state,
            },
            borrowed: None,
        };

        let params = match build_new_order_params(
            instrument.name().to_string(),
            side,
            price,
            quantity,
            kind,
            time_in_force,
            cid.0.to_string(),
            request
                .state
                .margin_effect
                .unwrap_or(self.config.margin_effect),
            self.config.auto_repay_at_cancel,
            self.config.is_isolated,
        ) {
            Ok(params) => params,
            Err(BuildOrderError::Unsupported) => {
                return inactive(OrderState::inactive(OrderError::UnsupportedOrderType(
                    format!(
                        "BinanceMargin does not support OrderKind::{kind:?} with {time_in_force:?}"
                    ),
                )));
            }
            Err(BuildOrderError::Trailing(refused)) => {
                return inactive(OrderState::inactive(refused.into_order_error()));
            }
            Err(BuildOrderError::Build(msg)) => {
                error!(%msg, "BinanceMargin failed to build new order params");
                return inactive(OrderState::inactive(OrderError::Rejected(
                    ApiError::OrderRejected(msg),
                )));
            }
        };

        if let Err(refused) = self
            .rules
            .check(instrument.name(), kind, price, quantity)
            .await
        {
            return inactive(OrderState::inactive(refused));
        }

        // The stream may report the order before the answer to placing it arrives, and an
        // isolated account's report cannot say whether a conditional order trails: the kind it
        // is placed with is held from the request's first dispatch, after any wait on the rate
        // limiter, for such a report to take. A placement dropped before then sent nothing.
        let mut sending = None;
        // Every `return` below leaves this block, so a held kind is released whatever the answer.
        let placement = async {
            // Sent through `send_request` with the SDK's own query, so that the answer decodes into
            // `NewOrderAnswer` (see there) rather than the SDK's response type.
            let query = new_order_query(params);
            let response = match rest_call_with_retry(
                &self.rest_config,
                &self.rate_limiter,
                RequestKind::Order,
                |cfg| {
                    sending.get_or_insert_with(|| self.known_live.sending(&cid, kind));
                    let query = query.clone();
                    Box::pin(async move {
                        binance_sdk::common::utils::send_request::<NewOrderAnswer>(
                            &cfg,
                            ORDER_PATH,
                            reqwest::Method::POST,
                            query,
                            BTreeMap::new(),
                            None,
                            true,
                        )
                        .await
                    })
                },
            )
            .await
            {
                Ok(response) => response,
                Err(e) => {
                    return inactive(OrderState::inactive(classify_rest_order_error(
                        &e,
                        &instrument,
                    )));
                }
            };

            let data = match response.data().await {
                Ok(data) => data,
                Err(e) => {
                    return inactive(OrderState::inactive(unreadable_answer(
                        ExchangeId::BinanceMargin,
                        &e.to_string(),
                    )));
                }
            };

            let time_exchange = answer_time(
                ExchangeId::BinanceMargin,
                &instrument,
                "open_order",
                data.transact_time,
            );

            let exchange_order_id = match data.order_id {
                Some(id) => OrderId(format_smolstr!("{id}")),
                None => {
                    return inactive(OrderState::inactive(unreadable_answer(
                        ExchangeId::BinanceMargin,
                        "open_order answer has no orderId",
                    )));
                }
            };

            let borrowed = match (data.margin_buy_borrow_amount, data.margin_buy_borrow_asset) {
                (Some(amount), Some(asset)) => Some(MarginBorrow::new(asset, amount)),
                (None, None) => None,
                (Some(_), None) | (None, Some(_)) => {
                    return inactive(OrderState::inactive(unreadable_answer(
                        ExchangeId::BinanceMargin,
                        "open_order answer has a borrowed amount or asset without the other",
                    )));
                }
            };

            // Read from the response's status, so an order that ended in it (an IOC or FOK order
            // that expired) is not reported, or held, as live.
            let state = placed_order_state(
                ExchangeId::BinanceMargin,
                &instrument,
                quantity,
                PlacementResponse {
                    status: data.status.as_deref(),
                    order_id: exchange_order_id,
                    time_exchange,
                    executed_qty: data.executed_qty.as_deref(),
                    cumulative_quote_qty: data.cummulative_quote_qty.as_deref(),
                },
            );
            self.known_live
                .lock()
                .placed(&order_key, quantity, kind, &state);

            MarginPlacement {
                order: Order {
                    // Cloned: the block holds `inactive`, which borrows the key, to its end.
                    key: order_key.clone(),
                    side,
                    price,
                    quantity,
                    kind,
                    time_in_force,
                    state,
                },
                borrowed,
            }
        }
        .await;
        if let Some(sending) = sending {
            sending.sent(&placement.order.state);
        }
        placement
    }

    /// Hold each of `orders` as live, for a reconnect to ask about if the stream misses its end.
    fn remember_live<'a>(
        &self,
        orders: impl IntoIterator<Item = &'a Order<ExchangeId, InstrumentNameExchange, Open>>,
    ) {
        let mut known = self.known_live.lock();
        for order in orders {
            known.live(&order.key, order.quantity, order.kind, &order.state);
        }
    }

    /// Build the production-configured margin REST configuration (credentials + base path).
    ///
    /// The base path is pinned to production (`https://api.binance.com`): margin/SAPI has no
    /// testnet, and the config is reused both for the SDK `RestApi` and the requests sent through
    /// `send_request` (the new-order and cancel requests, which join their paths onto this base).
    ///
    /// # Panics
    /// Panics if the binance-sdk configuration builder fails (invalid credentials format).
    #[allow(clippy::expect_used)] // Documented panic: invalid credentials detected at startup
    fn build_rest_config(config: &BinanceMarginConfig) -> ConfigurationRestApi {
        ConfigurationRestApi::builder()
            .api_key(config.api_key.clone())
            .api_secret(config.secret_key.clone())
            .base_path(MARGIN_TRADING_REST_API_PROD_URL)
            .build()
            .expect("failed to build Binance margin REST configuration")
    }

    /// Resolve the per-call effective isolated symbol set (isolated mode only).
    ///
    /// Isolated margin queries are per-symbol on the venue (there is no account-wide "no symbol =
    /// all" affordance), so every isolated snapshot/query iterates an explicit symbol set:
    /// - empty `instruments` (the "return all" sentinel) → the full configured
    ///   [`isolated_symbols`](BinanceMarginConfig::isolated_symbols);
    /// - non-empty → `instruments ∩ isolated_symbols`; any requested instrument NOT in
    ///   `isolated_symbols` is skipped with a `warn!` (there is no configured isolated
    ///   token/stream/snapshot for it — returning it would be a silent mismatch).
    ///
    /// Shared by `account_snapshot`, `fetch_open_orders`, and `fetch_trades` so their instrument,
    /// open-order, and trade sets cannot drift apart.
    fn effective_isolated_set(
        &self,
        instruments: &[InstrumentNameExchange],
    ) -> Vec<InstrumentNameExchange> {
        if instruments.is_empty() {
            return self.config.isolated_symbols.clone();
        }
        instruments
            .iter()
            .filter(|inst| {
                let in_set = self.config.isolated_symbols.contains(*inst);
                if !in_set {
                    warn!(
                        instrument = %inst.name(),
                        "BinanceMargin isolated: requested instrument not in configured isolated_symbols — skipping"
                    );
                }
                in_set
            })
            .cloned()
            .collect()
    }

    /// Isolated-margin `account_snapshot`: per-pair balances + risk attached per-instrument, with
    /// the asset-keyed top-level `balances` left empty. Open orders and
    /// isolated balances cover the same effective isolated set so they cannot diverge.
    async fn isolated_account_snapshot(
        &self,
        instruments: &[InstrumentNameExchange],
    ) -> Result<UnindexedAccountSnapshot, UnindexedClientError> {
        use futures::{StreamExt as _, TryStreamExt as _};

        let effective = self.effective_isolated_set(instruments);
        if effective.is_empty() {
            // No configured isolated pair matched: a no-symbol isolated call is invalid, so there is
            // nothing to query — return an empty snapshot rather than an account-wide one.
            return Ok(AccountSnapshot::new(
                ExchangeId::BinanceMargin,
                Vec::new(),
                Vec::new(),
            ));
        }

        // Per-pair isolated balances + risk (chunked ≤5 symbols/request), keyed by instrument,
        // each chunk's stamped by when its request was sent.
        let mut balances_by_symbol = HashMap::with_capacity(effective.len());
        for read in fetch_isolated_margin_account_info(
            self.rest.clone(),
            self.rate_limiter.clone(),
            effective.clone(),
        )
        .await?
        {
            balances_by_symbol.extend(convert_isolated_margin_assets(
                read.assets,
                balance_read_time(read.sent),
            ));
        }

        // Open orders per instrument over the same effective set, concurrently (bounded to avoid
        // bursting request-weight limits).
        let instrument_snapshots: Vec<_> =
            futures::stream::iter(effective.into_iter().map(|instrument| {
                fetch_margin_open_orders_for_instrument(
                    self.rest.clone(),
                    self.rate_limiter.clone(),
                    self.known_live.clone(),
                    instrument,
                    true,
                    RequestKind::Query,
                )
            }))
            .buffer_unordered(8)
            .map(|result| {
                let (inst, listing) = result?;
                self.remember_live(&listing.orders);
                let wrapped = listing
                    .orders
                    .into_iter()
                    .map(active_order_snapshot)
                    .collect();
                let isolated = balances_by_symbol.remove(&inst);
                if isolated.is_none() {
                    // The same effective set drives both balances and open orders, so a miss here
                    // means the venue's isolated-account response omitted (or mis-keyed) a requested
                    // pair. Surface it rather than silently emitting `isolated: None`.
                    warn!(
                        instrument = %inst.name(),
                        "BinanceMargin isolated: no isolated balance entry returned for instrument — snapshot will carry isolated: None"
                    );
                }
                Ok::<_, UnindexedClientError>(InstrumentAccountSnapshot::new(
                    inst,
                    wrapped,
                    listing.complete,
                    PositionReport::Unreported,
                    isolated,
                ))
            })
            .try_collect()
            .await?;

        // Isolated balances live on the instrument snapshots; the asset-keyed top-level vec stays
        // empty so the engine's `update_from_account` never writes colliding per-asset slots.
        Ok(AccountSnapshot::new(
            ExchangeId::BinanceMargin,
            Vec::new(),
            instrument_snapshots,
        ))
    }
}

impl ExecutionClient for BinanceMargin {
    const EXCHANGE: ExchangeId = ExchangeId::BinanceMargin;

    // Margin is leverage applied to spot pairs, not a distinct instrument kind: a cross or
    // isolated margin order is still an order on a `Spot` symbol.
    const SUPPORTED_KINDS: &'static [InstrumentKindDiscriminant] =
        &[InstrumentKindDiscriminant::Spot];
    type Config = BinanceMarginConfig;
    type AccountStream = BoxStream<'static, UnindexedAccountEvent>;

    /// Construct a `BinanceMargin` client from its configuration.
    ///
    /// # Panics
    /// Panics if:
    /// - the binance-sdk configuration builder fails (e.g. empty or malformed API key/secret),
    ///   matching [`BinanceSpot`](super::spot::BinanceSpot)'s startup contract; or
    /// - `is_isolated = true` but [`isolated_symbols`](BinanceMarginConfig::isolated_symbols) is
    ///   empty — an isolated client with no configured pairs has nothing to snapshot or stream.
    ///   This gate lives here (not only in [`BinanceMarginConfig::isolated`]) because the config is
    ///   `Deserialize`-only and a deserialized config bypasses the named constructor. The
    ///   `ExecutionClient::new` signature returns `Self`, not `Result`, so an unusable config is a
    ///   fail-fast panic (consistent with the credential `expect`s above), not a recoverable error.
    fn new(config: Self::Config) -> Self {
        if config.testnet {
            warn!(
                "BinanceMarginConfig.testnet = true is ignored: Binance margin has no testnet; \
                 using production endpoints"
            );
        }
        assert!(
            !(config.is_isolated && config.isolated_symbols.is_empty()),
            "BinanceMarginConfig: is_isolated = true requires a non-empty isolated_symbols"
        );
        // Built once and shared: one clone is consumed by the SDK `RestApi`, the original is retained
        // for the new-order and cancel requests via `send_request`. `ConfigurationRestApi: Clone`
        // and is cheap to copy (its `reqwest::Client` is `Arc`-backed), avoiding redundant credential
        // clones from building it twice.
        let rest_config = Self::build_rest_config(&config);
        let rest = Arc::new(MarginTradingRestApi::production(rest_config.clone()));
        // The rules come from the spot `exchangeInfo`, a public endpoint on the spot weight pool.
        // Its tracker is this client's own: a `BinanceSpot` in the same process keeps another for
        // the same IP pool, which is cheap, as rules load once per pair an hour.
        let rules = Arc::new(RulesLoader::new(
            config.pre_send_rules,
            Arc::new(binance_sdk::spot::SpotRestApi::production(
                rest_config.clone(),
            )),
            Arc::new(RateLimitTracker::new(WeightPool::Spot)),
        ));
        let rest_config = Arc::new(rest_config);
        Self {
            config: Arc::new(config),
            rest,
            rest_config,
            rate_limiter: Arc::new(RateLimitTracker::new(WeightPool::Sapi)),
            known_live: KnownLiveOrders::shared(ExchangeId::BinanceMargin),
            rules,
        }
    }

    /// Submit a margin order over the SAPI `POST /sapi/v1/margin/order` endpoint.
    ///
    /// [`BinanceMargin::open_margin_order`]'s order, without what it borrowed: see there for the
    /// margin specifics.
    async fn open_order(
        &self,
        request: OrderRequestOpen<ExchangeId, &InstrumentNameExchange>,
    ) -> Order<ExchangeId, InstrumentNameExchange, UnindexedOrderState> {
        self.open_margin_order(request).await.order
    }

    /// Cancel a resting margin order via `DELETE /sapi/v1/margin/order`.
    ///
    /// Cancels by exchange `orderId` when present and parseable, otherwise by the originating
    /// client order id. `isIsolated` is config-driven. Mirrors
    /// [`BinanceSpot::cancel_order`](super::spot::BinanceSpot): every failure is folded into the
    /// returned response's `state` as an `Err`.
    ///
    /// The cancellation's time is the answer's `transactTime`. An answer without a readable one is
    /// read at the local time, with a `warn!`.
    ///
    /// A successful answer that cannot be read (one that does not decode, or has no readable `orderId`)
    /// returns [`OrderError::Connectivity`]: the venue acted on the cancel, so whether the order
    /// is still working is unknown.
    async fn cancel_order(
        &self,
        request: OrderRequestCancel<ExchangeId, &InstrumentNameExchange>,
    ) -> UnindexedOrderResponseCancel {
        let instrument = request.key.instrument.clone();
        let key = OrderKey {
            exchange: request.key.exchange,
            instrument: instrument.clone(),
            strategy: request.key.strategy.clone(),
            cid: request.key.cid.clone(),
        };

        let params = match build_cancel_order_params(
            instrument.name().to_string(),
            request.state.id.as_ref().and_then(VenueOrderId::assigned),
            &request.key.cid,
            self.config.is_isolated,
        ) {
            Ok(p) => p,
            Err(e) => {
                error!(%e, "BinanceMargin failed to build cancel order params");
                return UnindexedOrderResponseCancel {
                    key,
                    state: Err(OrderError::Rejected(ApiError::OrderRejected(e))),
                };
            }
        };

        // Sent through `send_request` with the SDK's own query, so that the answer decodes into
        // `CancelOrderAnswer` (see there) rather than the SDK's response type.
        let query = cancel_order_query(params);
        let response = match rest_call_with_retry(
            &self.rest_config,
            &self.rate_limiter,
            RequestKind::Order,
            |cfg| {
                let query = query.clone();
                Box::pin(async move {
                    binance_sdk::common::utils::send_request::<CancelOrderAnswer>(
                        &cfg,
                        ORDER_PATH,
                        reqwest::Method::DELETE,
                        query,
                        BTreeMap::new(),
                        None,
                        true,
                    )
                    .await
                })
            },
        )
        .await
        {
            Ok(response) => response,
            Err(e) => {
                return UnindexedOrderResponseCancel {
                    key,
                    state: Err(classify_rest_order_error(&e, &instrument)),
                };
            }
        };

        let data = match response.data().await {
            Ok(data) => data,
            Err(e) => {
                return UnindexedOrderResponseCancel {
                    key,
                    state: Err(unreadable_answer(ExchangeId::BinanceMargin, &e.to_string())),
                };
            }
        };

        let time_exchange = answer_time(
            ExchangeId::BinanceMargin,
            &instrument,
            "cancel_order",
            data.transact_time,
        );

        let exchange_order_id = match data.order_id {
            Some(WireInt::Int(id)) => OrderId(format_smolstr!("{id}")),
            Some(WireInt::Unreadable(raw)) => {
                return UnindexedOrderResponseCancel {
                    key,
                    state: Err(unreadable_answer(
                        ExchangeId::BinanceMargin,
                        &format!("cancel answer has an unreadable orderId: {raw}"),
                    )),
                };
            }
            None => {
                return UnindexedOrderResponseCancel {
                    key,
                    state: Err(unreadable_answer(
                        ExchangeId::BinanceMargin,
                        "cancel answer has no orderId",
                    )),
                };
            }
        };

        let filled_qty = binance_filled_qty(
            ExchangeId::BinanceMargin,
            &exchange_order_id,
            data.executed_qty.as_deref(),
        );

        self.known_live.lock().ended(&key.cid);
        UnindexedOrderResponseCancel {
            key,
            state: Ok(Cancelled::new(exchange_order_id, time_exchange, filled_qty)),
        }
    }

    /// Fetch a full margin account snapshot: balances plus open orders per instrument.
    ///
    /// **Cross** (`is_isolated = false`): account-wide per-asset balances from
    /// `query_cross_margin_account_details` (carrying `borrowed`/`interest` → [`Balance::new_margin`])
    /// in the top-level `balances`, plus open orders per requested instrument — mirroring
    /// [`BinanceSpot::account_snapshot`](super::spot::BinanceSpot).
    ///
    /// **Isolated** (`is_isolated = true`): per-pair balances + risk from
    /// `query_isolated_margin_account_info` (chunked ≤5 symbols/request), attached **per-instrument**
    /// via [`InstrumentAccountSnapshot::isolated`] rather than folded into the asset-keyed
    /// top-level `balances` (which is left **empty**) — isolated sub-accounts are per-`(pair, asset)`
    /// and would collide in the asset-keyed model. The instrument set is the effective isolated set
    /// (`isolated_symbols`, or `instruments ∩ isolated_symbols` with out-of-set instruments skipped);
    /// each instrument's open orders and isolated balances are fetched over that same set.
    ///
    /// # Known limitation: balances can lag a fill
    /// Binance's REST account view can trail its matching engine. A cross account read straight
    /// after a fill has returned the pre-trade balances, and was current about 2 s later. A caller
    /// that needs post-fill balances should apply the
    /// [`account_stream`](Self::account_stream)'s balance events, or read again after a short
    /// delay. The client adds no retry or delay of its own: when and how to reconcile is the
    /// caller's policy.
    ///
    /// So each balance is stamped 5 s before its request was sent: a stream balance or debt
    /// event the read may not yet show is then still more recent than the read, and applies over
    /// it.
    async fn account_snapshot(
        &self,
        assets: &[AssetNameExchange],
        instruments: &[InstrumentNameExchange],
    ) -> Result<UnindexedAccountSnapshot, UnindexedClientError> {
        if self.config.is_isolated {
            return self.isolated_account_snapshot(instruments).await;
        }
        let (response, sent) =
            rest_read_with_retry(&self.rest, &self.rate_limiter, RequestKind::Query, |rest| {
                Box::pin(async move {
                    let params = QueryCrossMarginAccountDetailsParams::builder().build()?;
                    rest.query_cross_margin_account_details(params).await
                })
            })
            .await
            .map_err(|e| classify_rest_query_error(&e, None))?;

        let account = response.data().await.map_err(response_decode_error)?;

        let balances = filter_and_convert_margin_balances(
            account.user_assets.unwrap_or_default(),
            assets,
            balance_read_time(sent),
        );

        // Fetch open orders for all instruments concurrently (with retry), limiting concurrency
        // to avoid bursting Binance's request weight limits. account_snapshot wraps Open orders
        // in OrderState::active(); fetch_open_orders returns them without the wrapper — both use
        // fetch_margin_open_orders_for_instrument.
        use futures::{StreamExt as _, TryStreamExt as _};
        let instrument_snapshots: Vec<_> =
            futures::stream::iter(instruments.iter().cloned().map(|instrument| {
                fetch_margin_open_orders_for_instrument(
                    self.rest.clone(),
                    self.rate_limiter.clone(),
                    self.known_live.clone(),
                    instrument,
                    // Cross path only: isolated returns early via `isolated_account_snapshot`.
                    false,
                    RequestKind::Query,
                )
            }))
            .buffer_unordered(8)
            .map(|result| {
                let (inst, listing) = result?;
                self.remember_live(&listing.orders);
                let wrapped = listing
                    .orders
                    .into_iter()
                    .map(active_order_snapshot)
                    .collect();
                Ok::<_, UnindexedClientError>(InstrumentAccountSnapshot::new(
                    inst,
                    wrapped,
                    listing.complete,
                    PositionReport::Unreported,
                    None,
                ))
            })
            .try_collect()
            .await?;

        Ok(AccountSnapshot::new(
            ExchangeId::BinanceMargin,
            balances,
            instrument_snapshots,
        ))
    }

    /// Fetch current margin balances (incl. `borrowed`/`interest` debt) for the requested assets.
    ///
    /// **Cross**: account-wide per-asset balances; an empty `assets` slice is the "return all"
    /// sentinel.
    ///
    /// **Isolated**: returns an empty `Vec`. Isolated balances are per-`(pair, asset)` and the
    /// asset-keyed return type cannot carry them without collision — they are surfaced per-instrument
    /// via [`account_snapshot`](Self::account_snapshot)'s [`InstrumentAccountSnapshot::isolated`]
    /// instead.
    ///
    /// Reads the same REST account view as [`account_snapshot`](Self::account_snapshot), so its
    /// known limitation applies: balances read straight after a fill can still be the pre-trade
    /// ones.
    async fn fetch_balances(
        &self,
        assets: &[AssetNameExchange],
    ) -> Result<Vec<AssetBalance<AssetNameExchange>>, UnindexedClientError> {
        if self.config.is_isolated {
            return Ok(Vec::new());
        }
        let (response, sent) =
            rest_read_with_retry(&self.rest, &self.rate_limiter, RequestKind::Query, |rest| {
                Box::pin(async move {
                    let params = QueryCrossMarginAccountDetailsParams::builder().build()?;
                    rest.query_cross_margin_account_details(params).await
                })
            })
            .await
            .map_err(|e| classify_rest_query_error(&e, None))?;

        let account = response.data().await.map_err(response_decode_error)?;

        Ok(filter_and_convert_margin_balances(
            account.user_assets.unwrap_or_default(),
            assets,
            balance_read_time(sent),
        ))
    }

    /// Fetch currently open margin orders, optionally filtered by instrument.
    ///
    /// **Cross**: honours the `ExecutionClient::fetch_open_orders` "return all" sentinel — an empty
    /// `instruments` slice is served by a single no-symbol `query_margin_accounts_open_orders` call
    /// (each order's instrument taken from its own `symbol`); a non-empty slice fetches the listed
    /// instruments concurrently, per-symbol.
    ///
    /// **Isolated**: per-symbol on the venue — always iterates the effective isolated set (empty
    /// `instruments` → configured `isolated_symbols`; out-of-set instruments skipped with a warning);
    /// never issues a no-symbol isolated call.
    ///
    /// **A conditional order is listed with the kind this client knows it by**, or else as
    /// [`OrderKind::ConditionalUnresolved`]: the REST row cannot tell a fixed from a trailing one
    /// (#541). See the type-level docs and [`recall_order_kinds`](Self::recall_order_kinds).
    async fn fetch_open_orders(
        &self,
        instruments: &[InstrumentNameExchange],
    ) -> Result<Vec<Order<ExchangeId, InstrumentNameExchange, Open>>, UnindexedClientError> {
        use futures::{StreamExt as _, TryStreamExt as _};

        // Isolated: per-symbol on the venue — always iterate the effective set (never a no-symbol
        // isolated call, which would error). The empty-`instruments` sentinel resolves to the
        // configured isolated_symbols.
        if self.config.is_isolated {
            let effective = self.effective_isolated_set(instruments);
            let capacity = effective.len();
            return futures::stream::iter(effective.into_iter().map(|instrument| {
                fetch_margin_open_orders_for_instrument(
                    self.rest.clone(),
                    self.rate_limiter.clone(),
                    self.known_live.clone(),
                    instrument,
                    true,
                    RequestKind::Query,
                )
            }))
            .buffer_unordered(8)
            .try_fold(
                Vec::with_capacity(capacity),
                |mut acc: Vec<Order<ExchangeId, InstrumentNameExchange, Open>>,
                 (_, listing)| async move {
                    acc.extend(listing.orders);
                    Ok(acc)
                },
            )
            .await
            .inspect(|orders| self.remember_live(orders));
        }

        // Cross: empty slice = "return all" sentinel — a single no-symbol query is both correct
        // (the contract requires all instruments) and far cheaper than enumerating every symbol.
        if instruments.is_empty() {
            let orders = fetch_margin_all_open_orders(
                self.rest.clone(),
                self.rate_limiter.clone(),
                self.known_live.clone(),
                false,
            )
            .await?;
            self.remember_live(&orders);
            return Ok(orders);
        }
        futures::stream::iter(instruments.iter().cloned().map(|instrument| {
            fetch_margin_open_orders_for_instrument(
                self.rest.clone(),
                self.rate_limiter.clone(),
                self.known_live.clone(),
                instrument,
                false,
                RequestKind::Query,
            )
        }))
        .buffer_unordered(8)
        .try_fold(
            Vec::with_capacity(instruments.len()),
            |mut acc: Vec<Order<ExchangeId, InstrumentNameExchange, Open>>, (_, listing)| async move {
                acc.extend(listing.orders);
                Ok(acc)
            },
        )
        .await
        .inspect(|orders| self.remember_live(orders))
    }

    /// Read margin trades (fills) from `start` to `end`, optionally filtered by instrument.
    ///
    /// **Documented deviation from the `ExecutionClient::fetch_trades` "return all" contract:**
    /// Binance's margin trade-list endpoint (`myTrades`) requires a symbol — there is no no-symbol
    /// "all trades" query (unlike open orders).
    ///
    /// **Cross**: an empty `instruments` slice has nothing to query and reads nothing; callers
    /// wanting all trades must enumerate instruments explicitly.
    ///
    /// **Isolated**: the empty sentinel resolves to the configured `isolated_symbols` (the effective
    /// isolated set; out-of-set instruments skipped with a warning), iterated per-symbol (Design
    /// decision #4).
    ///
    /// **Complete:** each instrument is read to the span's end within the call, with no page cap,
    /// so the read is always complete (`resume: None`).
    ///
    /// **Lookback cost:** Binance serves margin trades by time only in spans under 24 hours, so
    /// each instrument is read in 23-hour windows from `start` until its first trade, one
    /// request (weight 10) per window, then by trade id. The cost grows with the lookback and
    /// has no cap: a `start` 30 days back with no trades costs about 32 requests per
    /// instrument, and one at the Unix epoch about 21,000. Pass the most recent `start` that
    /// covers what you need.
    ///
    /// **Upper bound:** trades are read up to `end`, or up to the local clock at the call if that
    /// is earlier. A trade stamped after the local clock, as one inside any skew between it and
    /// Binance's can be, is left to the next read, unless a window before the last one held a
    /// trade (or the last one read a full page) and the walk went on to page by id, which reads
    /// on to `end`.
    async fn fetch_trades(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        instruments: &[InstrumentNameExchange],
    ) -> Result<TradesRead<AssetNameExchange, InstrumentNameExchange>, UnindexedClientError> {
        use futures::StreamExt as _;

        if start > end {
            return Ok(TradesRead::complete(Vec::new()));
        }

        // Resolve the effective instrument set. Isolated resolves the empty sentinel to the
        // configured isolated_symbols; cross keeps the slice verbatim (no no-symbol "all" form).
        let effective: Vec<InstrumentNameExchange> = if self.config.is_isolated {
            self.effective_isolated_set(instruments)
        } else {
            instruments.to_vec()
        };
        if effective.is_empty() {
            // Distinguish the two empty causes: cross with an empty `instruments` slice (the
            // documented no-op sentinel) vs. isolated with no configured `isolated_symbols` matched.
            debug!(
                is_isolated = self.config.is_isolated,
                "BinanceMargin fetch_trades: empty effective instrument set — returning empty result"
            );
            return Ok(TradesRead::complete(Vec::new()));
        }
        // Binance stamps trades in whole milliseconds; the exact span is applied after the read.
        let span = MyTradesFrom::Span {
            start: start.timestamp_millis(),
            end: end.timestamp_millis(),
        };
        let is_isolated = self.config.is_isolated;
        let mut all_trades = Vec::new();

        // Binance requires per-symbol queries for trade history. Limit concurrency to avoid
        // bursting Binance's request weight limits.
        let mut stream = futures::stream::iter(effective.into_iter().map(|inst| {
            let rest = self.rest.clone();
            let rate_limiter = self.rate_limiter.clone();
            async move {
                let pages = paginate_margin_my_trades(
                    &rest,
                    &rate_limiter,
                    &inst,
                    span,
                    is_isolated,
                    RequestKind::Query,
                )
                .await?;
                Ok::<_, UnindexedClientError>((inst, pages))
            }
        }))
        .buffer_unordered(8);
        while let Some(result) = stream.next().await {
            let (instrument, trades_data) = result?;
            for t in trades_data {
                if let Some(trade) = convert_margin_trade(&t, &instrument)
                    && (start..=end).contains(&trade.time_exchange)
                {
                    all_trades.push(trade);
                }
            }
        }

        Ok(TradesRead::complete(all_trades))
    }

    /// Live stream of account events (fills, order updates, balances, margin debt and notices)
    /// over Binance's margin user-data stream.
    ///
    /// Gets a listen key (`POST /sapi/v2/margin/listen-key`, weight 1), connects to
    /// `wss://fstream.binance.com/private/ws` and subscribes the key, then keeps the stream live
    /// with auto-reconnect, exponential backoff, heartbeat monitoring and fill recovery (mirroring
    /// [`BinanceSpot`](super::spot::BinanceSpot)). The key is kept alive as each connection
    /// opens and every 30 minutes after (`PUT`, weight 1), since Binance hands back a key that is
    /// still live without saying how long it has left; a failed keepalive is logged at `warn`,
    /// and if the key lapses anyway Binance says so on the stream, and the stream reconnects with
    /// a new one. The key is not closed when the stream ends: it lapses 60 minutes after its last
    /// keepalive. The stream ends once 10 reconnects in a row have failed or a rate-limit cooldown
    /// outlasts what it waits through: see the [module docs](super#account-stream-reconnection).
    /// After each reconnect it restates the account's balances, debt included: see
    /// [there](super#balance-restatement-after-a-reconnect).
    ///
    /// Binance sends on this stream only when the account changes, so the client pings it at each
    /// heartbeat check, and the pong is what proves a quiet connection alive.
    ///
    /// As on spot, each instrument's gap after a reconnect is kept until its fills are forwarded:
    /// a gap whose read failed or timed out is retried after 1, 2, 4, 8 and 16 minutes, connected
    /// or not in between, reading only the gap. After five failed retries it is given up, logged
    /// at `error` and reported as an [`AccountEventKind::FillRecoveryGaveUp`];
    /// [`ExecutionClient::fetch_trades`] can still read its fills.
    ///
    /// # One key for the whole margin account
    /// Binance's listen key covers the cross account and every isolated pair alike, and each frame
    /// says which it is about. A cross client keeps the cross account's frames, and an isolated
    /// client those of its [`isolated_symbols`](BinanceMarginConfig::isolated_symbols); the rest
    /// are dropped at `debug`. So a cross and an isolated client on one account each see only
    /// their own, though each opens a stream of its own.
    ///
    /// An `executionReport` that does not say which it is about is kept by its symbol, on cross
    /// whatever the symbol and on isolated when it is a configured pair, and the ambiguity is
    /// logged at `warn` once: an order on the same symbol in the other kind of account could then
    /// reach both clients. A balance or debt frame that does not say is dropped with a `warn`.
    ///
    /// # Debt cold-start
    /// This method does **not** seed balances. Margin debt (`borrowed`/`interest`) is correct only
    /// if the caller invokes [`ExecutionClient::account_snapshot`] at startup (the `BalanceSnapshot`
    /// that populates `margin`). The stream thereafter keeps `free`/`locked` live via
    /// `BalanceStreamUpdate`, and debt via [`AccountEventKind::MarginDebt`] (see "Margin debt"
    /// below), but only for the assets whose debt changes; a change sent while the stream was down
    /// is not recovered.
    ///
    /// # Margin notices
    /// Binance's `marginLevelChange` is delivered as an [`AccountEventKind::Notice`]. Its status
    /// maps to a [`NoticeKind`](crate::NoticeKind): `MARGIN_CALL` → `MarginCall`,
    /// `PRE_LIQUIDATION` → `LiquidationWarning`, `FORCE_LIQUIDATION` → `Liquidation`, `NORMAL` and
    /// `EXCESSIVE` → `MarginRestored`, anything else → `Other`, with Binance's status verbatim in
    /// [`AccountNotice::status`](crate::AccountNotice::status) and its margin level (`l`) in
    /// [`margin_level`](crate::AccountNotice::margin_level).
    ///
    /// The event's `s` is its status, so it names no pair. A cross account's notice names no
    /// instrument. An isolated pair's is delivered to an isolated client naming no instrument,
    /// with a `warn`, whichever pair it is about, since losing a margin call costs more than losing
    /// its attribution; a cross client does not receive it. So an isolated client's notice can be
    /// about any isolated pair of the account, one it is not configured for included, and two
    /// isolated clients on one account each receive it.
    ///
    /// A notice with no time is stamped on arrival. Each arrival is logged at `info`, before
    /// dedup. A notice sent while the stream is disconnected is not recovered.
    ///
    /// # Margin debt
    /// Binance's `liabilityChange` is delivered as one [`AccountEventKind::MarginDebt`] per asset
    /// it lists. Each asset's principal (`p`) and interest (`i`) are its totals after the change,
    /// as observed live (Binance's docs do not say), so each update states the whole debt. Its
    /// type (`t`) maps to a [`MarginDebtKind`](crate::MarginDebtKind): `BORROW` → `Borrow`,
    /// `REPAY` → `Repay`, `INTEREST` (the periodic charge) → `Interest`, anything else, such as
    /// `DEBT_CHANGE`, → `Other`, with Binance's type verbatim in
    /// [`venue_kind`](crate::MarginDebtUpdate::venue_kind).
    /// - **Cross** updates name no instrument: they are the account's debt in that asset.
    /// - **Isolated** debt is not known to be streamed. Binance has not been seen to send
    ///   `liabilityChange` for an isolated pair: a borrow and a repay there came with balance
    ///   updates alone. One that did arrive would name its pair, from the frame. An isolated
    ///   client reads its pairs' debt from [`ExecutionClient::account_snapshot`].
    ///
    /// Binance posts an interest charge some time after the period it is for; when is not
    /// documented. An asset with no name, or whose `p` or `i` cannot be read, is dropped with a
    /// `warn`, as is a change with no readable time, which could not be ordered against the debt
    /// held. Each arrival is logged at `info`.
    ///
    /// # Fills around the start
    /// Frames the venue sends before it confirms the subscription are kept and delivered first. A
    /// fill before the subscription is not on the stream: callers requiring startup fill
    /// completeness call [`ExecutionClient::fetch_trades`] with a ~1s lookback after this returns.
    ///
    /// A recovered fill advances the order too. A live fill carries the order's cumulative filled
    /// quantity in [`Trade::order_filled_quantity`] (`executionReport`'s `z`); REST `myTrades`
    /// carries none, so recovery rebuilds the same figure by reading each recovered order's
    /// executions from its first: one extra request per order (another per further 1,000
    /// executions), up to four orders at a time per instrument. Those lookups have their own
    /// time budget inside the recovery timeout, so they can never cost a fill: a fill whose order
    /// was not looked up in time, or whose lookup failed, goes out with `order_filled_quantity:
    /// None`, logged at `warn`, and advances the position but not the order.
    ///
    /// # Orders that ended while disconnected
    ///
    /// A reconnect also reports how each order the client holds as live on one of the stream's
    /// instruments ended, where it did, as an [`AccountEventKind::OrderSnapshot`] of its inactive
    /// state: filled, cancelled with what filled before, expired, or rejected. The stream's
    /// instruments are `instruments` on cross and
    /// [`isolated_symbols`](BinanceMarginConfig::isolated_symbols) on isolated, as for fill
    /// recovery. Binance's order history filters on when an order was created, so such an order is
    /// found by asking about it, not by time.
    ///
    /// - **Which orders.** The client holds an order as live from the response to placing it,
    ///   from a listing of open orders ([`account_snapshot`](ExecutionClient::account_snapshot),
    ///   [`fetch_open_orders`](ExecutionClient::fetch_open_orders)), and from its live reports on
    ///   any of its account streams, until it sees the order end. It holds up to 4,096 orders and
    ///   forgets the oldest past that, logged at `warn`. An order placed outside this client and
    ///   never listed or reported to it is not covered.
    /// - **Cost.** One `GET /sapi/v1/margin/openOrders` per instrument with an order held (weight
    ///   10), then one `GET /sapi/v1/margin/order` (weight 10) for each held order the listing no
    ///   longer shows, all in the configured cross or isolated account.
    /// - **Fills first.** An instrument is checked only once its fill gap is recovered or given up,
    ///   so an order's recovered fills arrive before how it ended. A fill that brings an order to
    ///   its full quantity ends it, and that order is not reported again.
    /// - **Keys.** Each snapshot carries [`StrategyId::unknown`], since Binance records no
    ///   strategy. The engine matches it to the order it tracks by client order id.
    /// - **Failures.** Each order's lookup is settled as it ends, and each instrument as soon as its
    ///   check ends. One whose listing or any lookup fails, or whose check is still running when a
    ///   pass reaches 30 s, is retried on the fill gaps' schedule (1, 2, 4, 8 and 16 minutes),
    ///   asking only about the orders still held, then given up, logged at `error`. Its orders are
    ///   asked about again at the next reconnect. An order Binance does not know (`-2013`) stops being held, logged
    ///   at `warn`; one it still reports live, or in a state this version cannot read, stays held.
    ///
    /// The same lookup is public as [`OrderStatusClient::fetch_ended_orders`].
    ///
    /// # Isolated mode
    /// Under `is_isolated = true` the stream covers exactly
    /// [`isolated_symbols`](BinanceMarginConfig::isolated_symbols); the `instruments` argument does
    /// not choose them. Each pair's base and quote asset are read from the isolated account info
    /// before the stream opens, and a failed read fails the call.
    ///
    /// ## Live per-pair balances — [`InstrumentBalanceUpdate`](crate::AccountEventKind::InstrumentBalanceUpdate)
    /// The isolated stream delivers live fills and order updates **and** live per-pair
    /// `free`/`locked` balances, emitted as [`AccountEventKind::InstrumentBalanceUpdate`], one per
    /// asset whose balance changed: base, quote or both. A side that did not change is not
    /// reported. A pair whose base and quote could not be read at the start has its balance frames
    /// dropped with a `warn`; its fills and orders are unaffected.
    ///
    /// Debt totals (`borrowed`/`interest`) are not known to be streamed for an isolated pair (see
    /// "Margin debt" above), so they come from
    /// [`account_snapshot`](ExecutionClient::account_snapshot), and from the
    /// [`InstrumentBalanceSnapshot`](AccountEventKind::InstrumentBalanceSnapshot)s the stream
    /// restates after each reconnect (see the
    /// [module docs](super#balance-restatement-after-a-reconnect)).
    ///
    /// **Consumption contract (important):** the engine deliberately does **not** store
    /// `InstrumentBalanceUpdate` — it falls through `update_from_account`'s `_ => None` wildcard,
    /// mirroring the snapshot's
    /// [`InstrumentAccountSnapshot::isolated`](crate::InstrumentAccountSnapshot::isolated). It is
    /// therefore **never in `EngineState`**, and a `StateReplicaManager` replica replays the same
    /// wildcard so it is **not** in replica state either. A consumer obtains live per-pair balances
    /// **only** by inspecting the raw account event off the stream (`AccountStreamEvent::Item(..)` /
    /// `audit_tick.event`) and matching the variant itself — a consumer reading solely replica
    /// `EngineState` will see nothing and wrongly conclude the feature is broken. A wrapper wanting
    /// engine-queryable per-instrument balances uses the `InstrumentData` extension point instead.
    async fn account_stream(
        &self,
        // Only a reconnect's balance restatement uses `assets`: Binance pushes
        // outboundAccountPosition for every asset of the account, whatever is asked for (mirrors
        // spot).
        assets: &[AssetNameExchange],
        instruments: &[InstrumentNameExchange],
    ) -> Result<Self::AccountStream, UnindexedClientError> {
        let (tx, rx) = mpsc::unbounded_channel::<UnindexedAccountEvent>();

        let (account, instruments) = if self.config.is_isolated {
            let symbols = self.config.isolated_symbols.clone();
            // Each pair's base and quote, which route its balance frames: a pair's never change,
            // so they are read once. A REST failure here fails the call before anything spawns.
            let assets: Vec<_> = fetch_isolated_margin_account_info(
                self.rest.clone(),
                self.rate_limiter.clone(),
                symbols.clone(),
            )
            .await?
            .into_iter()
            .flat_map(|read| read.assets)
            .collect();
            let mut base_quote = build_base_quote_map(&assets);
            let pairs = symbols
                .iter()
                .map(|symbol| {
                    let assets = base_quote.remove(symbol);
                    if assets.is_none() {
                        warn!(
                            symbol = %symbol.name(),
                            "BinanceMargin isolated: account info returned no base/quote for \
                             configured symbol — its live per-pair balance frames will be dropped \
                             (fills/orders unaffected; balances available via account_snapshot)"
                        );
                    }
                    (symbol.clone(), assets)
                })
                .collect();
            (StreamAccount::Isolated { pairs }, symbols)
        } else {
            (StreamAccount::Cross, instruments.to_vec())
        };

        let stream = MarginStream {
            keys: SapiListenKeys {
                rest: self.rest.clone(),
                rate_limiter: self.rate_limiter.clone(),
            },
            url: STREAM_URL.to_owned(),
            rest: self.rest.clone(),
            rate_limiter: self.rate_limiter.clone(),
            known: self.known_live.clone(),
            account,
            instruments,
            assets: assets.to_vec(),
            clock: SharedStreamClock::default(),
            tx,
            dedup: new_dedup_cache(),
        };
        // Opened before returning, so the caller can tell "can't start at all" from "started then
        // disconnected", which the stream handles itself.
        let session = stream.open().await?;
        let handle = tokio::spawn(stream.run(session));

        let rx_stream = tokio_stream::wrappers::UnboundedReceiverStream::new(rx);
        let guarded_stream = AbortOnDropStream::new(rx_stream, handle);
        Ok(futures::StreamExt::boxed(guarded_stream))
    }
}

/// Looks each order up with `GET /sapi/v1/margin/order` by symbol and client order id, in the
/// cross or isolated account as [`BinanceMarginConfig::is_isolated`] says, weight 10 each, up to
/// eight at a time. Binance's `FILLED` is reported with its average price, `CANCELED` with what
/// filled before it, `EXPIRED` and `EXPIRED_IN_MATCH` (self-trade prevention) as expired, and
/// `REJECTED` as [`OpenFailed`](crate::order::state::InactiveOrderState::OpenFailed). An order
/// Binance does not know under the key's symbol (`-2013`, or `-1121` for a symbol that does not
/// exist) is omitted. A row whose status this version does not know is omitted too, with a
/// warning, as is a `PENDING_CANCEL` one.
///
/// In isolated mode a key's symbol is asked about whether or not it is one of
/// [`isolated_symbols`](BinanceMarginConfig::isolated_symbols), since an order can be placed on any
/// pair. Any error other than those two codes fails the call, including one Binance gives for a
/// pair with no isolated account.
///
/// The account stream runs the same lookup itself after a reconnect; see
/// [`account_stream`](ExecutionClient::account_stream).
impl OrderStatusClient for BinanceMargin {
    async fn fetch_ended_orders(
        &self,
        orders: &[UnindexedOrderKey],
    ) -> Result<Vec<UnindexedInactiveOrder>, UnindexedClientError> {
        fetch_ended_by_key(orders, |key| {
            let recorded_kind = self.known_live.lock().kind_of(&key.cid);
            fetch_margin_order_lookup(
                self.rest.clone(),
                self.rate_limiter.clone(),
                key,
                self.config.is_isolated,
                RequestKind::Query,
                recorded_kind,
            )
        })
        .await
    }
}

// ---------------------------------------------------------------------------
// Shortability and borrow capacity
// ---------------------------------------------------------------------------

/// Hours in the 365-day year a Binance hourly interest rate is annualised over.
const HOURS_PER_YEAR: u32 = 24 * 365;

impl ShortabilityClient for BinanceMargin {
    /// Reads the pair, its base asset, the base asset's next hourly interest rate and Binance's
    /// margin delisting schedule, which cost 202 of Binance's per-IP request weight on cross
    /// margin and 211 on isolated, almost all of it the interest rate and the schedule.
    ///
    /// - `shortable` is whether the pair is traded on margin and may be sold
    ///   (`/sapi/v1/margin/allPairs`, or `isolated/allPairs` on isolated margin), its base asset
    ///   may be borrowed (`/sapi/v1/margin/allAssets`), and its delisting time, if any, is still
    ///   ahead. Any of them false makes it false.
    /// - `delisting_at` is when Binance will delist the pair from this client's margin, from
    ///   `/sapi/v1/margin/delist-schedule`, which lists only what is due to be delisted. On cross
    ///   margin Binance schedules assets, so the pair goes with the first of its base and quote
    ///   assets to be delisted. On isolated margin it schedules the pair itself.
    /// - `fee_rate` is the base asset's next hourly interest rate
    ///   (`/sapi/v1/margin/next-hourly-interest-rate`, for this client's cross or isolated margin)
    ///   times 24 × 365. It is this account's rate, at its VIP level, for the coming hour only.
    /// - `easy_to_borrow` is unknown: Binance has no such distinction.
    /// - `available` is unknown: Binance does not say how much it has to lend in total, only how
    ///   much this account can borrow, which
    ///   [`fetch_borrow_capacity`](BorrowCapacityClient::fetch_borrow_capacity) reports.
    ///
    /// # Errors
    ///
    /// - [`ApiError::InstrumentInvalid`] for a pair Binance does not trade on this client's cross
    ///   or isolated margin.
    /// - Whatever error any of the requests returns, the delisting schedule's included: an unread
    ///   schedule is not reported as no delisting scheduled.
    async fn fetch_shortability(
        &self,
        instrument: &InstrumentNameExchange,
    ) -> Result<Shortability, UnindexedClientError> {
        let pair = self.fetch_margin_pair(instrument).await?;
        let (borrowable, hourly_rate, delisting_at) = tokio::try_join!(
            self.fetch_asset_borrowable(&pair.base),
            self.fetch_next_hourly_interest_rate(&pair.base),
            self.fetch_delisting_at(instrument, &pair),
        )?;

        let mut shortability = Shortability::new();
        if let Some(delisting_at) = delisting_at {
            shortability = shortability.with_delisting_at(delisting_at);
        }
        // Any flag false is enough to forbid the short; all must be known true to allow it. The
        // schedule lists everything due to be delisted, so a pair absent from it is known not to
        // be, and this flag is never unknown.
        let not_delisted = Some(delisting_at.is_none_or(|delisting_at| delisting_at > Utc::now()));
        let flags = [
            pair.is_margin_trade,
            pair.is_sell_allowed,
            borrowable,
            not_delisted,
        ];
        if flags.contains(&Some(false)) {
            shortability = shortability.with_shortable(false);
        } else if flags.iter().all(|flag| *flag == Some(true)) {
            shortability = shortability.with_shortable(true);
        }
        if let Some(hourly_rate) = hourly_rate {
            shortability = shortability.with_fee_rate(hourly_rate * Decimal::from(HOURS_PER_YEAR));
        }
        Ok(shortability)
    }
}

impl BorrowCapacityClient for BinanceMargin {
    /// Reads the pair, then `/sapi/v1/margin/maxBorrowable` for the asset an order on `side`
    /// borrows, on this client's cross margin or on the pair's isolated account. They cost 51 of
    /// Binance's per-IP request weight on cross margin and 60 on isolated.
    ///
    /// `borrowable_now` is Binance's `amount`, which it describes as the account's "max
    /// borrowable amount with sufficient system availability", so it is also capped by what
    /// Binance has to lend. `account_limit` is its `borrowLimit`, the most the account's VIP level
    /// allows.
    ///
    /// # Errors
    ///
    /// - [`ApiError::InstrumentInvalid`] for a pair Binance does not trade on this client's cross
    ///   or isolated margin.
    /// - [`ApiError::RequestRejected`] with Binance's `-11001` on isolated margin, for a pair this
    ///   account has no isolated margin account for. Open one on Binance, or ask a cross-margin
    ///   client.
    async fn fetch_borrow_capacity(
        &self,
        instrument: &InstrumentNameExchange,
        side: Side,
    ) -> Result<BorrowCapacity, UnindexedClientError> {
        let pair = self.fetch_margin_pair(instrument).await?;
        let asset = match side {
            Side::Buy => pair.quote,
            Side::Sell => pair.base,
        };
        let isolated_symbol = self
            .config
            .is_isolated
            .then(|| instrument.name().to_string());
        let response =
            rest_call_with_retry(&self.rest, &self.rate_limiter, RequestKind::Query, |rest| {
                let asset = asset.name().to_string();
                let isolated_symbol = isolated_symbol.clone();
                Box::pin(async move {
                    let params = QueryMaxBorrowParams::builder(asset)
                        .isolated_symbol(isolated_symbol)
                        .build()?;
                    rest.query_max_borrow(params).await
                })
            })
            .await
            .map_err(|e| classify_rest_query_error(&e, Some(instrument)))?;
        let max_borrow = response.data().await.map_err(response_decode_error)?;

        let mut capacity = BorrowCapacity::new(asset);
        if let Some(amount) = optional_decimal("amount", max_borrow.amount.as_deref())? {
            capacity = capacity.with_borrowable_now(amount);
        }
        if let Some(limit) = optional_decimal("borrowLimit", max_borrow.borrow_limit.as_deref())? {
            capacity = capacity.with_account_limit(limit);
        }
        Ok(capacity)
    }
}

/// A margin pair as Binance lists it, for this client's cross or isolated margin.
#[derive(Debug)]
struct MarginPair {
    base: AssetNameExchange,
    quote: AssetNameExchange,
    is_margin_trade: Option<bool>,
    is_sell_allowed: Option<bool>,
}

impl BinanceMargin {
    /// `instrument` as Binance lists it for this client's margin: `/sapi/v1/margin/allPairs`
    /// (weight 1) on cross, `/sapi/v1/margin/isolated/allPairs` (weight 10) on isolated, both
    /// filtered to the one symbol.
    async fn fetch_margin_pair(
        &self,
        instrument: &InstrumentNameExchange,
    ) -> Result<MarginPair, UnindexedClientError> {
        let symbol = instrument.name().to_string();
        let listed = if self.config.is_isolated {
            let response =
                rest_call_with_retry(&self.rest, &self.rate_limiter, RequestKind::Query, |rest| {
                    let symbol = symbol.clone();
                    Box::pin(async move {
                        let params = GetAllIsolatedMarginSymbolParams::builder()
                            .symbol(symbol)
                            .build()?;
                        rest.get_all_isolated_margin_symbol(params).await
                    })
                })
                .await
                .map_err(|e| classify_rest_query_error(&e, Some(instrument)))?;
            response
                .data()
                .await
                .map_err(response_decode_error)?
                .into_iter()
                .find(|pair| pair.symbol.as_deref() == Some(symbol.as_str()))
                .map(|pair| {
                    (
                        pair.base,
                        pair.quote,
                        pair.is_margin_trade,
                        pair.is_sell_allowed,
                    )
                })
        } else {
            let response =
                rest_call_with_retry(&self.rest, &self.rate_limiter, RequestKind::Query, |rest| {
                    let symbol = symbol.clone();
                    Box::pin(async move {
                        let params = GetAllCrossMarginPairsParams::builder()
                            .symbol(symbol)
                            .build()?;
                        rest.get_all_cross_margin_pairs(params).await
                    })
                })
                .await
                .map_err(|e| classify_rest_query_error(&e, Some(instrument)))?;
            response
                .data()
                .await
                .map_err(response_decode_error)?
                .into_iter()
                .find(|pair| pair.symbol.as_deref() == Some(symbol.as_str()))
                .map(|pair| {
                    (
                        pair.base,
                        pair.quote,
                        pair.is_margin_trade,
                        pair.is_sell_allowed,
                    )
                })
        };
        let margin = if self.config.is_isolated {
            "isolated"
        } else {
            "cross"
        };
        let Some((base, quote, is_margin_trade, is_sell_allowed)) = listed else {
            return Err(UnindexedClientError::Api(ApiError::InstrumentInvalid(
                instrument.clone(),
                format!("Binance lists no {margin} margin pair {symbol}"),
            )));
        };
        let (Some(base), Some(quote)) = (base, quote) else {
            return Err(UnindexedClientError::Internal(format!(
                "Binance lists {margin} margin pair {symbol} without its base or quote asset"
            )));
        };
        Ok(MarginPair {
            base: AssetNameExchange::new(base),
            quote: AssetNameExchange::new(quote),
            is_margin_trade,
            is_sell_allowed,
        })
    }

    /// Whether Binance lends `asset` on margin: its `isBorrowable` in `/sapi/v1/margin/allAssets`
    /// (weight 1). Unknown if Binance does not list it.
    async fn fetch_asset_borrowable(
        &self,
        asset: &AssetNameExchange,
    ) -> Result<Option<bool>, UnindexedClientError> {
        let name = asset.name().to_string();
        let response =
            rest_call_with_retry(&self.rest, &self.rate_limiter, RequestKind::Query, |rest| {
                let name = name.clone();
                Box::pin(async move {
                    let params = GetAllMarginAssetsParams::builder().asset(name).build()?;
                    rest.get_all_margin_assets(params).await
                })
            })
            .await
            .map_err(|e| classify_rest_query_error(&e, None))?;
        Ok(response
            .data()
            .await
            .map_err(response_decode_error)?
            .into_iter()
            .find(|listed| listed.asset_name.as_deref() == Some(name.as_str()))
            .and_then(|listed| listed.is_borrowable))
    }

    /// When Binance will delist `instrument` from this client's margin, from
    /// `/sapi/v1/margin/delist-schedule` (weight 100). On cross margin it is the earliest time
    /// either of the pair's assets is listed under, and on isolated margin the earliest the pair
    /// is listed under. `None` if neither is scheduled.
    async fn fetch_delisting_at(
        &self,
        instrument: &InstrumentNameExchange,
        pair: &MarginPair,
    ) -> Result<Option<DateTime<Utc>>, UnindexedClientError> {
        let response =
            rest_call_with_retry(&self.rest, &self.rate_limiter, RequestKind::Query, |rest| {
                Box::pin(async move {
                    let params = GetDelistScheduleParams::builder().build()?;
                    rest.get_delist_schedule(params).await
                })
            })
            .await
            .map_err(|e| classify_rest_query_error(&e, None))?;
        let schedule = response.data().await.map_err(response_decode_error)?;

        let symbol = instrument.name().as_str();
        let (base, quote) = (pair.base.name().as_str(), pair.quote.name().as_str());
        let mut earliest: Option<DateTime<Utc>> = None;
        for entry in schedule {
            let scheduled = if self.config.is_isolated {
                entry
                    .isolated_margin_symbols
                    .iter()
                    .flatten()
                    .any(|listed| listed == symbol)
            } else {
                entry
                    .cross_margin_assets
                    .iter()
                    .flatten()
                    .any(|listed| listed == base || listed == quote)
            };
            if !scheduled {
                continue;
            }
            // Milliseconds, as Binance confirmed for every margin endpoint carrying `delistTime`.
            let delisting_at = entry
                .delist_time
                .and_then(DateTime::from_timestamp_millis)
                .ok_or_else(|| {
                    UnindexedClientError::Internal(format!(
                        "decoding Binance response: delist-schedule lists {symbol} with delistTime \
                         {:?}",
                        entry.delist_time
                    ))
                })?;
            earliest = Some(earliest.map_or(delisting_at, |earliest| earliest.min(delisting_at)));
        }
        Ok(earliest)
    }

    /// `asset`'s next hourly interest rate on this client's cross or isolated margin, a fraction
    /// per hour: `/sapi/v1/margin/next-hourly-interest-rate` (weight 100). Unknown if Binance does
    /// not answer for it.
    async fn fetch_next_hourly_interest_rate(
        &self,
        asset: &AssetNameExchange,
    ) -> Result<Option<Decimal>, UnindexedClientError> {
        let name = asset.name().to_string();
        let is_isolated = self.config.is_isolated;
        let response =
            rest_call_with_retry(&self.rest, &self.rate_limiter, RequestKind::Query, |rest| {
                let name = name.clone();
                Box::pin(async move {
                    let params = GetFutureHourlyInterestRateParams::builder(
                        name,
                        IsolatedFlag::from_flag(is_isolated),
                    )
                    .build()?;
                    rest.get_future_hourly_interest_rate(params).await
                })
            })
            .await
            .map_err(|e| classify_rest_query_error(&e, None))?;
        let rate = response
            .data()
            .await
            .map_err(response_decode_error)?
            .into_iter()
            .find(|listed| listed.asset.as_deref() == Some(name.as_str()))
            .and_then(|listed| listed.next_hourly_interest_rate);
        optional_decimal("nextHourlyInterestRate", rate.as_deref())
    }
}

/// A decimal Binance may leave out: absent is `None`, but present and unreadable is an error, so a
/// malformed amount is not mistaken for an unreported one.
fn optional_decimal(
    field: &str,
    value: Option<&str>,
) -> Result<Option<Decimal>, UnindexedClientError> {
    value
        .map(|value| {
            Decimal::from_str(value).map_err(|error| {
                UnindexedClientError::Internal(format!(
                    "decoding Binance response: {field} {value:?}: {error}"
                ))
            })
        })
        .transpose()
}

// ---------------------------------------------------------------------------
// User-data stream: recovery after a reconnect (the stream itself is in `stream`)
// ---------------------------------------------------------------------------

/// Recover the fills of every gap in `unrecovered` that is due, via REST `myTrades`, routed
/// through the dedup cache. Mirrors `BinanceSpot::recover_fills`, which documents how gaps are
/// settled and retried.
///
/// Only fills are recovered here; how the orders held as live ended is checked afterwards by
/// [`recover_margin_ended_orders`]. A recovered fill that completes its order ends it in `known`.
///
/// `myTrades` reports executions only, with no cumulative, so each recovered trade's
/// `order_filled_quantity` is rebuilt from its order's executions by
/// [`recovered_order_totals`]. Nor does it carry a client order id, so each recovered trade's
/// `client_order_id` is then read from its order by [`recovered_order_cids`], with whatever time
/// the walks left. A trade whose order could not be looked up keeps `None` in either.
///
/// Margin `myTrades` is read by time in windows under 24 hours (see
/// [`paginate_margin_my_trades`]), so each day of a gap before its first fill costs one sequential
/// request. A gap of weeks can therefore run into [`FILL_RECOVERY_TIMEOUT_SECS`], which bounds the
/// whole recovery; it is then retried, and given up once it has failed every retry.
///
/// Fills forwarded before the deadline stay delivered. Its reads are [`RequestKind::Essential`]:
/// they wait out a short rate-limit cooldown, and fail at once on a longer one, but not the pause
/// near the weight limit, which could outlast the budget.
async fn recover_margin_fills(
    rest: &Arc<RestApi>,
    rate_limiter: &Arc<RateLimitTracker>,
    unrecovered: &mut UnrecoveredFills,
    tx: &mpsc::UnboundedSender<UnindexedAccountEvent>,
    dedup: &SharedDedupCache,
    known: &SharedKnownLiveOrders,
    is_isolated: bool,
) {
    use futures::StreamExt as _;

    let due = unrecovered.due(tokio::time::Instant::now());
    let Some(oldest) = due.iter().map(|(_, gap)| gap.start_ms).min() else {
        return;
    };
    info!(
        gaps = due.len(),
        oldest = %gap_time(oldest),
        is_isolated,
        "BinanceMargin recovering fills missed while disconnected"
    );

    // Which gaps of `due` have been settled, recovered or failed, and how many have started: the
    // stream starts them in order, eight at a time, so those from `started` on were never read.
    let mut settled = vec![false; due.len()];
    let started = AtomicUsize::new(0);
    let recovery = async {
        let order_executions_deadline = tokio::time::Instant::now() + ORDER_EXECUTIONS_BUDGET;
        let mut recovered = 0u32;
        let mut duplicates = 0u32;

        let mut stream =
            futures::stream::iter(due.iter().cloned().enumerate().map(|(index, (inst, gap))| {
                started.store(index + 1, Ordering::Relaxed);
                let rest = rest.clone();
                let rl = rate_limiter.clone();
                async move {
                    let span = MyTradesFrom::Span {
                        start: gap.start_ms,
                        end: gap.end_ms,
                    };
                    let raw = match paginate_margin_my_trades(
                        &rest,
                        &rl,
                        &inst,
                        span,
                        is_isolated,
                        RequestKind::Essential,
                    )
                    .await
                    {
                        Ok(pages) => pages,
                        Err(e) => return (index, Err(e)),
                    };
                    // `myTrades` carries no cumulative, so each recovered fill's is rebuilt from
                    // its order's executions; without it the fill advances the position but not
                    // the order.
                    let totals = recovered_order_totals(
                        ExchangeId::BinanceMargin,
                        &inst,
                        &raw,
                        order_executions_deadline,
                        |order_id| {
                            paginate_margin_my_trades(
                                &rest,
                                &rl,
                                &inst,
                                MyTradesFrom::Order(order_id),
                                is_isolated,
                                RequestKind::Essential,
                            )
                        },
                    )
                    .await;
                    // After the walks, so these reads only use time the walks left: the walks
                    // matter more, since a fill without a cumulative leaves its order behind.
                    let cids = recovered_order_cids(
                        ExchangeId::BinanceMargin,
                        &inst,
                        &raw,
                        order_executions_deadline,
                        |order_id| fetch_margin_order_cid(&rest, &rl, &inst, order_id, is_isolated),
                    )
                    .await;
                    let trades = raw
                        .iter()
                        .filter_map(|t| {
                            let mut trade = convert_margin_trade(t, &inst)?;
                            trade.order_filled_quantity =
                                t.id.and_then(|id| totals.get(&id).copied());
                            trade.client_order_id =
                                t.order_id.and_then(|id| cids.get(&id).cloned());
                            Some(trade)
                        })
                        .collect::<Vec<_>>();
                    (index, Ok(trades))
                }
            }))
            .buffer_unordered(8);
        while let Some((index, result)) = stream.next().await {
            let (inst, gap) = &due[index];
            settled[index] = true;
            let trades = match result {
                Ok(trades) => trades,
                Err(e) => {
                    gap_failed(
                        unrecovered,
                        tx,
                        ExchangeId::BinanceMargin,
                        inst,
                        gap,
                        FillRecoveryFailure::Request(e.to_string()),
                    );
                    continue;
                }
            };
            for trade in trades {
                let event = UnindexedAccountEvent::new(
                    ExchangeId::BinanceMargin,
                    AccountEventKind::Trade(trade),
                );
                if let Some(key) = dedup_key_from_event(&event)
                    && is_duplicate(dedup, key)
                {
                    duplicates += 1;
                    continue;
                }
                // A recovered fill that completes its order ends it, so a later check of how the
                // orders held as live ended does not report it again.
                let sent = {
                    let mut known = known.lock();
                    known.observe(&event);
                    tx.send(event)
                };
                if sent.is_err() {
                    debug!("BinanceMargin fill recovery: consumer dropped during recovery");
                    return;
                }
                recovered += 1;
            }
            unrecovered.recovered(inst, gap);
        }
        info!(
            recovered,
            duplicates, is_isolated, "BinanceMargin fill recovery complete"
        );
    };
    // A timeout drops `recovery` at an await, between gaps: one gap's fills are sent without
    // awaiting, so each is either fully forwarded and settled, or not forwarded at all. A gap
    // whose read started and did not finish has failed; one never started stays due as it was.
    if tokio::time::timeout(Duration::from_secs(FILL_RECOVERY_TIMEOUT_SECS), recovery)
        .await
        .is_err()
    {
        let started = started.load(Ordering::Relaxed);
        for ((inst, gap), _) in due[..started]
            .iter()
            .zip(&settled)
            .filter(|(_, settled)| !**settled)
        {
            gap_failed(
                unrecovered,
                tx,
                ExchangeId::BinanceMargin,
                inst,
                gap,
                FillRecoveryFailure::TimedOut {
                    timeout_secs: FILL_RECOVERY_TIMEOUT_SECS,
                },
            );
        }
    }
}

/// Open what a reconnect after a disconnect at `disconnected` recovers: a fill gap on each of the
/// stream's `instruments`, and a check of how the orders held as live ended on each of them that
/// has one. Only the stream's instruments are checked, since only their fills are recovered, and
/// an instrument is checked once its fills are.
fn open_reconnect_recovery(
    unrecovered: &mut UnrecoveredFills,
    unchecked: &mut UncheckedOrders,
    known: &SharedKnownLiveOrders,
    instruments: &[InstrumentNameExchange],
    disconnected: DateTime<Utc>,
) {
    unrecovered.open(instruments, disconnected, Utc::now());
    let held = known.lock().instruments_among(instruments);
    unchecked.open(held);
}

/// Read the margin account's balances to restate after a reconnect, debt included, stamped by
/// [`restatement_time`] for a read started with `floor` (see
/// [`restate`](super::restatement::restate)).
///
/// A cross account's are each of `assets` (all of them when empty), as a
/// [`BalanceSnapshot`](AccountEventKind::BalanceSnapshot). An isolated account's are the base and
/// quote of each pair in `pairs`, in that order, as an
/// [`InstrumentBalanceSnapshot`](AccountEventKind::InstrumentBalanceSnapshot); the pairs come in
/// no particular order.
async fn read_margin_restatement(
    rest: &Arc<RestApi>,
    rate_limiter: &Arc<RateLimitTracker>,
    is_isolated: bool,
    pairs: &[InstrumentNameExchange],
    assets: &[AssetNameExchange],
    clock: &SharedStreamClock,
    floor: Option<DateTime<Utc>>,
) -> Result<Vec<UnindexedAccountEvent>, UnindexedClientError> {
    let event = |kind| UnindexedAccountEvent::new(ExchangeId::BinanceMargin, kind);
    let sent_at = tokio::time::Instant::now();
    if is_isolated {
        let reads =
            fetch_isolated_margin_account_info(rest.clone(), rate_limiter.clone(), pairs.to_vec())
                .await?;
        let clock = clock.lock();
        return Ok(reads
            .into_iter()
            .flat_map(|read| {
                let time = restatement_time(&clock, read.sent, sent_at, floor);
                convert_isolated_margin_assets(read.assets, time)
            })
            .flat_map(|(pair, state)| {
                [state.base, state.quote].map(|balance| {
                    event(AccountEventKind::InstrumentBalanceSnapshot(
                        InstrumentBalanceSnapshot::new(pair.clone(), balance),
                    ))
                })
            })
            .collect());
    }
    let (response, sent) = rest_read_with_retry(rest, rate_limiter, RequestKind::Query, |rest| {
        Box::pin(async move {
            let params = QueryCrossMarginAccountDetailsParams::builder().build()?;
            rest.query_cross_margin_account_details(params).await
        })
    })
    .await
    .map_err(|e| classify_rest_query_error(&e, None))?;
    let account = response.data().await.map_err(response_decode_error)?;
    let time = restatement_time(&clock.lock(), sent, sent_at, floor);
    Ok(
        filter_and_convert_margin_balances(account.user_assets.unwrap_or_default(), assets, time)
            .into_iter()
            .map(|balance| {
                event(AccountEventKind::BalanceSnapshot(
                    rustrade_integration::collection::snapshot::Snapshot(balance),
                ))
            })
            .collect(),
    )
}

/// Extract the invariant `instrument → (base_asset, quote_asset)` map from an isolated
/// account-info response.
///
/// The authoritative base/quote split that picks a pair's own assets out of its
/// `outboundAccountPosition` frames — base/quote never change for a pair, so this is built once at
/// stream start and reused across reconnects. Entries missing a symbol or either asset name are
/// skipped (their balance frames then drop with a `warn!`).
fn build_base_quote_map(
    assets: &[QueryIsolatedMarginAccountInfoResponseAssetsInner],
) -> HashMap<InstrumentNameExchange, (AssetNameExchange, AssetNameExchange)> {
    let mut map = HashMap::with_capacity(assets.len());
    for entry in assets {
        let (Some(symbol), Some(base), Some(quote)) = (
            entry.symbol.as_deref(),
            entry.base_asset.as_deref().and_then(|b| b.asset.as_deref()),
            entry
                .quote_asset
                .as_deref()
                .and_then(|q| q.asset.as_deref()),
        ) else {
            warn!(
                "BinanceMargin isolated: account-info entry missing symbol/base/quote asset — \
                 skipping base/quote map entry"
            );
            continue;
        };
        map.insert(
            InstrumentNameExchange::new(symbol),
            (AssetNameExchange::new(base), AssetNameExchange::new(quote)),
        );
    }
    map
}

// ---------------------------------------------------------------------------
// Account query helpers (margin-specific)
// ---------------------------------------------------------------------------

/// Fetch open orders for a single instrument via `query_margin_accounts_open_orders`
/// (`isIsolated` config-driven). Mirrors `BinanceSpot::fetch_open_orders_for_instrument`.
async fn fetch_margin_open_orders_for_instrument(
    rest: Arc<RestApi>,
    rate_limiter: Arc<RateLimitTracker>,
    known: SharedKnownLiveOrders,
    instrument: InstrumentNameExchange,
    is_isolated: bool,
    kind: RequestKind,
) -> Result<(InstrumentNameExchange, OpenOrderListing), UnindexedClientError> {
    // Convert once before the retry closure to avoid a String allocation on every retry.
    let symbol_str = instrument.name().to_string();
    let isolated = QueryMarginAccountsOpenOrdersIsIsolatedEnum::from_flag(is_isolated);
    let response = rest_call_with_retry(&rest, &rate_limiter, kind, |rest| {
        let sym = symbol_str.clone();
        let isolated = isolated.clone();
        Box::pin(async move {
            let params = QueryMarginAccountsOpenOrdersParams::builder()
                .symbol(sym)
                .is_isolated(isolated)
                .build()?;
            rest.query_margin_accounts_open_orders(params).await
        })
    })
    .await
    .map_err(|e| classify_rest_query_error(&e, Some(&instrument)))?;

    let orders_data = response.data().await.map_err(response_decode_error)?;

    let listing = convert_open_order_listing(
        &orders_data,
        ExchangeId::BinanceMargin,
        &instrument,
        &|cid| known.lock().kind_of(cid),
    );

    Ok((instrument, listing))
}

/// Fetch *all* open margin orders in a single no-symbol `query_margin_accounts_open_orders`
/// call. Backs the [`fetch_open_orders`](BinanceMargin::fetch_open_orders) "return all" sentinel for
/// **cross** (the no-symbol affordance is cross-only): with no symbol the venue returns orders
/// across every instrument, so each order's instrument is recovered from its own `symbol` field
/// (orders missing it are dropped). `isIsolated` is config-driven, but under isolated the caller
/// iterates the configured symbol set per-symbol rather than using this no-symbol path.
async fn fetch_margin_all_open_orders(
    rest: Arc<RestApi>,
    rate_limiter: Arc<RateLimitTracker>,
    known: SharedKnownLiveOrders,
    is_isolated: bool,
) -> Result<Vec<Order<ExchangeId, InstrumentNameExchange, Open>>, UnindexedClientError> {
    let isolated = QueryMarginAccountsOpenOrdersIsIsolatedEnum::from_flag(is_isolated);
    let response = rest_call_with_retry(&rest, &rate_limiter, RequestKind::Query, |rest| {
        let isolated = isolated.clone();
        Box::pin(async move {
            let params = QueryMarginAccountsOpenOrdersParams::builder()
                .is_isolated(isolated)
                .build()?;
            rest.query_margin_accounts_open_orders(params).await
        })
    })
    .await
    .map_err(|e| classify_rest_query_error(&e, None))?;

    let orders_data = response.data().await.map_err(response_decode_error)?;

    let orders = orders_data
        .into_iter()
        .filter_map(|o| {
            convert_open_order_owned_symbol(&o, ExchangeId::BinanceMargin, &|cid| {
                known.lock().kind_of(cid)
            })
        })
        .collect();

    Ok(orders)
}

/// Look the order under `key` up with `GET /sapi/v1/margin/order` by its client order id
/// (weight 10), in the cross or isolated account as `is_isolated` says.
///
/// `recorded_kind` is the kind the client holds the order with, if it does. The row cannot say
/// whether a conditional order trailed (#541), so an ended one is reported with that kind, or
/// failing it as an unresolved kind (see [`convert_ended_order`]).
async fn fetch_margin_order_lookup(
    rest: Arc<RestApi>,
    rate_limiter: Arc<RateLimitTracker>,
    key: UnindexedOrderKey,
    is_isolated: bool,
    kind: RequestKind,
    recorded_kind: Option<OrderKind>,
) -> Result<OrderLookup, UnindexedClientError> {
    // Convert once before the retry closure to avoid a String allocation on every retry.
    let symbol = key.instrument.name().to_string();
    let cid = key.cid.0.to_string();
    let isolated = QueryMarginAccountsOrderIsIsolatedEnum::from_flag(is_isolated);
    let response = match rest_call_with_retry(&rest, &rate_limiter, kind, |rest| {
        let (symbol, cid, isolated) = (symbol.clone(), cid.clone(), isolated.clone());
        Box::pin(async move {
            let params = QueryMarginAccountsOrderParams::builder(symbol)
                .orig_client_order_id(cid)
                .is_isolated(isolated)
                .build()?;
            rest.query_margin_accounts_order(params).await
        })
    })
    .await
    {
        Ok(response) => response,
        Err(e) if is_unknown_order(&e) => {
            debug!(instrument = %key.instrument, cid = %key.cid, error = %e, "BinanceMargin does not know this order");
            return Ok(OrderLookup::Unknown);
        }
        Err(e) => return Err(classify_rest_query_error(&e, Some(&key.instrument))),
    };

    let row = response.data().await.map_err(response_decode_error)?;

    Ok(
        convert_ended_order(&row, ExchangeId::BinanceMargin, &key, recorded_kind)
            .map_or(OrderLookup::NotEnded, |order| {
                OrderLookup::Ended(Box::new(order))
            }),
    )
}

/// The client order id of the order `order_id` on `instrument`, read with
/// `GET /sapi/v1/margin/order` (weight 10) in the cross or isolated account as `is_isolated`
/// says, for fill recovery: `myTrades` carries none. `None` if Binance reports none, or does not
/// know the order.
async fn fetch_margin_order_cid(
    rest: &Arc<RestApi>,
    rate_limiter: &RateLimitTracker,
    instrument: &InstrumentNameExchange,
    order_id: i64,
    is_isolated: bool,
) -> Result<Option<ClientOrderId>, UnindexedClientError> {
    let symbol = instrument.name().to_string();
    let isolated = QueryMarginAccountsOrderIsIsolatedEnum::from_flag(is_isolated);
    let response = match rest_call_with_retry(rest, rate_limiter, RequestKind::Essential, |rest| {
        let (symbol, isolated) = (symbol.clone(), isolated.clone());
        Box::pin(async move {
            let params = QueryMarginAccountsOrderParams::builder(symbol)
                .order_id(order_id)
                .is_isolated(isolated)
                .build()?;
            rest.query_margin_accounts_order(params).await
        })
    })
    .await
    {
        Ok(response) => response,
        Err(e) if is_unknown_order(&e) => {
            debug!(%instrument, order_id, error = %e, "BinanceMargin does not know this order");
            return Ok(None);
        }
        Err(e) => return Err(classify_rest_query_error(&e, Some(instrument))),
    };
    let row = response.data().await.map_err(response_decode_error)?;
    Ok(row
        .client_order_id
        .filter(|cid| !cid.is_empty())
        .map(ClientOrderId::new))
}

/// The client order ids `GET /sapi/v1/margin/openOrders` lists on `instruments` (weight 10 each,
/// one at a time), for a reconnect's check of the orders held as live.
async fn listed_margin_open_cids(
    rest: Arc<RestApi>,
    rate_limiter: Arc<RateLimitTracker>,
    known: SharedKnownLiveOrders,
    instruments: Vec<InstrumentNameExchange>,
    is_isolated: bool,
) -> Result<FnvHashSet<ClientOrderId>, UnindexedClientError> {
    let mut listed = FnvHashSet::default();
    for instrument in instruments {
        let (_, listing) = fetch_margin_open_orders_for_instrument(
            rest.clone(),
            rate_limiter.clone(),
            known.clone(),
            instrument,
            is_isolated,
            RequestKind::Essential,
        )
        .await?;
        listed.extend(listing.orders.into_iter().map(|order| order.key.cid));
    }
    Ok(listed)
}

/// [`recover_ended_orders`] on Binance Margin: listings by [`listed_margin_open_cids`] and lookups
/// by [`fetch_margin_order_lookup`], both [`RequestKind::Essential`], as fill recovery's reads are.
async fn recover_margin_ended_orders(
    rest: &Arc<RestApi>,
    rate_limiter: &Arc<RateLimitTracker>,
    known: &SharedKnownLiveOrders,
    unchecked: &mut UncheckedOrders,
    unrecovered: &UnrecoveredFills,
    tx: &mpsc::UnboundedSender<UnindexedAccountEvent>,
    is_isolated: bool,
) {
    recover_ended_orders(
        ExchangeId::BinanceMargin,
        known,
        unchecked,
        unrecovered,
        tx,
        OpenListing::PerInstrument,
        |instruments| {
            listed_margin_open_cids(
                rest.clone(),
                rate_limiter.clone(),
                known.clone(),
                instruments,
                is_isolated,
            )
        },
        |key| {
            // Held as live, so the client knows its kind, which the row may not show.
            let recorded_kind = known.lock().kind_of(&key.cid);
            fetch_margin_order_lookup(
                rest.clone(),
                rate_limiter.clone(),
                key,
                is_isolated,
                RequestKind::Essential,
                recorded_kind,
            )
        },
    )
    .await;
}

/// Length of each time window [`paginate_margin_my_trades`] walks before it finds a trade.
///
/// Binance requires "less than 24 hours between startTime and endTime" on margin `myTrades`,
/// and returns only 24 hours of trades to a query without `fromId`. Kept well under that
/// limit so no reading of the bound (inclusive ends, clock rounding) can make a window too long.
const MARGIN_MY_TRADES_WINDOW_MS: i64 = 23 * 60 * 60 * 1000;

/// One margin `myTrades` request in a [`paginate_margin_my_trades`] walk. The walk's state is the
/// next request to send.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MarginTradesQuery {
    /// Trades stamped from `start` to `end`, in epoch milliseconds.
    Window { start: i64, end: i64 },
    /// Trades from this id onwards, whatever their time.
    FromId(i64),
    /// One order's executions, from its first.
    Order(i64),
    /// One order's executions, from this id onwards.
    OrderFromId { order_id: i64, from_id: i64 },
}

impl MarginTradesQuery {
    /// The first request of a walk from `from`, or `None` when a walk by time starts after
    /// `now_ms` and so has nothing to read. `now_ms` is where a walk by time stops stepping by
    /// window: a span's end, cut to one window past the call's start (see
    /// [`paginate_margin_my_trades`]).
    fn first(from: MyTradesFrom, now_ms: i64) -> Option<Self> {
        match from {
            MyTradesFrom::Span { start, .. } => {
                (start <= now_ms).then(|| Self::window(start, now_ms))
            }
            MyTradesFrom::Order(order_id) => Some(Self::Order(order_id)),
        }
    }

    /// The window from `start`, ending [`MARGIN_MY_TRADES_WINDOW_MS`] later or at `now_ms`.
    fn window(start: i64, now_ms: i64) -> Self {
        Self::Window {
            start,
            end: start.saturating_add(MARGIN_MY_TRADES_WINDOW_MS).min(now_ms),
        }
    }

    /// The request after this one returned `page_len` trades, the last with id `last_id`, or
    /// `None` when the walk is complete.
    fn next(self, page_len: usize, last_id: Option<i64>, now_ms: i64) -> Option<Self> {
        let full = page_len >= BINANCE_MAX_TRADES;
        let after_last = || last_id?.checked_add(1);
        match self {
            // An empty window steps on until it reaches `now_ms`. The next window starts at this
            // one's end, so a trade stamped exactly on the boundary is read whether Binance treats
            // `endTime` as inclusive or not. That cannot duplicate a trade: windows only follow
            // one another while every window so far was empty.
            Self::Window { end, .. } if page_len == 0 => {
                (end < now_ms).then(|| Self::window(end, now_ms))
            }
            // The final window read short holds everything up to `now_ms`: done.
            Self::Window { end, .. } if end >= now_ms && !full => None,
            // Any other window holding a trade: later trades may lie beyond its end, so read on
            // by id, which Binance serves whatever the time.
            Self::Window { .. } => after_last().map(Self::FromId),
            Self::FromId(_) if full => after_last().map(Self::FromId),
            Self::Order(order_id) | Self::OrderFromId { order_id, .. } if full => {
                after_last().map(|from_id| Self::OrderFromId { order_id, from_id })
            }
            Self::FromId(_) | Self::Order(_) | Self::OrderFromId { .. } => None,
        }
    }
}

/// Paginate the margin trade list for a single instrument from `from` (`isIsolated`
/// config-driven), returning every trade it covers with no gap.
///
/// - [`MyTradesFrom::Order`]: first page by `order_id`, then `from_id = last_id + 1` alongside it,
///   as `BinanceSpot::paginate_my_trades` does.
/// - [`MyTradesFrom::Span`]: unlike spot, margin `myTrades` does not return every trade since a
///   bare `startTime` — without `fromId` it returns only 24 hours of trades, and it rejects a
///   `startTime`..`endTime` span of 24 hours or more. So the walk first steps forward in windows
///   of [`MARGIN_MY_TRADES_WINDOW_MS`], each bounded by `startTime` and `endTime`, from the span's
///   start to its end. An end past the local clock is kept, because the venue's clock can be
///   ahead of it, but cut to one window past the call's start, so an empty walk does not step
///   on through windows no fill can be stamped in. At the first
///   window holding a trade it switches to `from_id = last_id + 1` and reads on until a short
///   page, unless that window was the last and read a short page, and stops at the first page
///   reaching past the span's end, without the executions after it.
///
/// **Cost:** each window before the first trade is one request (weight 10), so the cost grows
/// with the lookback: about one request per day, and about 21,000 from the Unix epoch. An
/// ordinary reconnect gap fits in one window.
///
/// **Isolated correctness:** this also backs reconnect fill-recovery (`recover_margin_fills`), so a
/// missed `isIsolated` flip would silently query *cross* trades on an isolated client — hence the
/// value is threaded from config rather than hardcoded.
async fn paginate_margin_my_trades(
    rest: &Arc<RestApi>,
    rate_limiter: &Arc<RateLimitTracker>,
    instrument: &InstrumentNameExchange,
    from: MyTradesFrom,
    is_isolated: bool,
    kind: RequestKind,
) -> Result<Vec<QueryMarginAccountsTradeListResponseInner>, UnindexedClientError> {
    // Convert once before the retry closure to avoid a String allocation on every retry.
    let symbol_str = instrument.name().to_string();
    let isolated = QueryMarginAccountsTradeListIsIsolatedEnum::from_flag(is_isolated);
    // The const_assert! in shared bounds BINANCE_MAX_TRADES <= i32::MAX, which trivially fits in
    // i64, so this cast is always lossless. clippy::cast_possible_wrap fires only because usize is
    // the same width as i64 on 64-bit targets (a hypothetical >64-bit usize could wrap); the bound
    // rules that out.
    #[allow(clippy::cast_possible_wrap)]
    let limit = BINANCE_MAX_TRADES as i64;
    // A walk by time reads up to here. A trade after it is left to the caller's next read (or, on
    // reconnect, to the stream), unless the walk is already paging by id.
    let now_ms = match from {
        MyTradesFrom::Span { end, .. } => end.min(
            Utc::now()
                .timestamp_millis()
                .saturating_add(MARGIN_MY_TRADES_WINDOW_MS),
        ),
        MyTradesFrom::Order(_) => Utc::now().timestamp_millis(),
    };
    let mut all_pages = Vec::new();
    let mut requests = 0u32;
    let mut next = MarginTradesQuery::first(from, now_ms);
    while let Some(query) = next {
        requests += 1;
        let response = rest_call_with_retry(rest, rate_limiter, kind, |rest| {
            let sym = symbol_str.clone();
            let isolated = isolated.clone();
            Box::pin(async move {
                let builder = QueryMarginAccountsTradeListParams::builder(sym)
                    .is_isolated(isolated)
                    .limit(limit);
                let params = match query {
                    MarginTradesQuery::Window { start, end } => {
                        builder.start_time(start).end_time(end)
                    }
                    MarginTradesQuery::FromId(id) => builder.from_id(id),
                    MarginTradesQuery::Order(order_id) => builder.order_id(order_id),
                    MarginTradesQuery::OrderFromId { order_id, from_id } => {
                        builder.order_id(order_id).from_id(from_id)
                    }
                }
                .build()?;
                rest.query_margin_accounts_trade_list(params).await
            })
        })
        .await
        .map_err(|e| classify_rest_query_error(&e, Some(instrument)))?;

        let mut page = response.data().await.map_err(response_decode_error)?;

        let page_len = page.len();
        let last_id = page.last().and_then(|t| t.id);
        // A span ends once a page reaches past it; paging by id would otherwise read on.
        let past_end = match from {
            MyTradesFrom::Span { end, .. } => drop_after(&mut page, end),
            MyTradesFrom::Order(_) => false,
        };
        all_pages.extend(page);
        if past_end {
            break;
        }

        if page_len > 0 && last_id.is_none() {
            warn!(%instrument, "BinanceMargin paginate_my_trades: trade missing ID, cannot page past it");
            break;
        }
        next = query.next(page_len, last_id, now_ms);
    }
    debug!(
        %instrument,
        requests,
        trades = all_pages.len(),
        "BinanceMargin paginate_my_trades: walk complete"
    );
    Ok(all_pages)
}

// ---------------------------------------------------------------------------
// Account conversion helpers (margin-specific)
// ---------------------------------------------------------------------------

/// Convert one cross-margin `userAsset` into a margin [`AssetBalance`], carrying the per-asset
/// `borrowed`/`interest` debt via [`Balance::new_margin`]. `total = free + locked`.
///
/// Distinct from spot's `convert_balance_entry`: spot has no debt fields and calls
/// [`Balance::new`]. Returns `None` (with a warning) if a required field is missing/unparseable.
fn convert_margin_balance_entry(
    b: QueryCrossMarginAccountDetailsResponseUserAssetsInner,
    time: DateTime<Utc>,
) -> Option<AssetBalance<AssetNameExchange>> {
    let asset_name = AssetNameExchange::new(b.asset.as_deref()?);
    let free = match b.free.as_deref().and_then(|s| Decimal::from_str(s).ok()) {
        Some(v) => v,
        None => {
            warn!(%asset_name, "BinanceMargin balance missing/unparseable 'free' field");
            return None;
        }
    };
    let locked = match b.locked.as_deref().and_then(|s| Decimal::from_str(s).ok()) {
        Some(v) => v,
        None => {
            warn!(%asset_name, "BinanceMargin balance missing/unparseable 'locked' field");
            return None;
        }
    };
    // Debt fields default to zero when missing/unparseable: a userAsset row always represents a
    // real margin position, so absent debt means "no debt", not corrupt data — emit a margin
    // Balance (carrying zero debt) rather than dropping the asset. This preserves the
    // Design-decision-#4 invariant that a REST snapshot always populates `margin`.
    let borrowed = b
        .borrowed
        .as_deref()
        .and_then(|s| Decimal::from_str(s).ok())
        .unwrap_or(Decimal::ZERO);
    let interest = b
        .interest
        .as_deref()
        .and_then(|s| Decimal::from_str(s).ok())
        .unwrap_or(Decimal::ZERO);
    Some(AssetBalance::new(
        asset_name,
        Balance::new_margin(free + locked, free, borrowed, interest),
        time,
    ))
}

/// Filter cross-margin `userAssets` to the requested assets and convert to [`AssetBalance`]s,
/// each stamped `time`. An empty `assets` slice returns all. Mirrors spot's
/// `filter_and_convert_balances`.
fn filter_and_convert_margin_balances(
    user_assets: Vec<QueryCrossMarginAccountDetailsResponseUserAssetsInner>,
    assets: &[AssetNameExchange],
    time: DateTime<Utc>,
) -> Vec<AssetBalance<AssetNameExchange>> {
    // Empty assets slice means "return all" — skip building the set entirely.
    if assets.is_empty() {
        return user_assets
            .into_iter()
            .filter_map(|b| convert_margin_balance_entry(b, time))
            .collect();
    }
    // For small slices (≤16 assets), linear scan avoids allocation and hashing overhead.
    if assets.len() <= 16 {
        return user_assets
            .into_iter()
            .filter_map(|b| {
                let asset_name_str = b.asset.as_deref()?;
                if !assets.iter().any(|a| a.name().as_str() == asset_name_str) {
                    return None;
                }
                convert_margin_balance_entry(b, time)
            })
            .collect();
    }
    use std::collections::HashSet;
    let asset_set: HashSet<&str> = assets.iter().map(|a| a.name().as_str()).collect();
    user_assets
        .into_iter()
        .filter_map(|b| {
            let asset_name_str = b.asset.as_deref()?;
            if !asset_set.contains(asset_name_str) {
                return None;
            }
            convert_margin_balance_entry(b, time)
        })
        .collect()
}

/// Wrap a fetched `Open` order as an `OrderState::active` snapshot for `account_snapshot`.
///
/// `account_snapshot` reports open orders wrapped in `OrderState::active(..)`, whereas
/// `fetch_open_orders` returns the bare `Open` — both share
/// [`fetch_margin_open_orders_for_instrument`], so this is the single wrap used by the cross and
/// isolated snapshot paths.
fn active_order_snapshot(
    o: Order<ExchangeId, InstrumentNameExchange, Open>,
) -> Order<ExchangeId, InstrumentNameExchange, OrderState<AssetNameExchange, InstrumentNameExchange>>
{
    Order {
        key: o.key,
        side: o.side,
        price: o.price,
        quantity: o.quantity,
        kind: o.kind,
        time_in_force: o.time_in_force,
        state: OrderState::active(o.state),
    }
}

/// Convert one side (base or quote) of an isolated `assets[]` entry into a margin [`AssetBalance`].
///
/// Mirrors [`convert_margin_balance_entry`]'s field handling: `free`/`locked` are required (missing
/// → `None`, drop), while `borrowed`/`interest` default to zero when absent — a real isolated
/// sub-account row with absent debt means "no debt", not corrupt data, upholding the
/// Design-decision-#4 invariant that a REST snapshot always populates `margin`. `total = free + locked`.
///
/// Takes the fields rather than the SDK side type because the base and quote sides are distinct
/// nominal types (`...BaseAsset` / `...QuoteAsset`) with identical fields.
fn convert_isolated_asset_balance(
    asset: Option<&str>,
    free: Option<&str>,
    locked: Option<&str>,
    borrowed: Option<&str>,
    interest: Option<&str>,
    time: DateTime<Utc>,
) -> Option<AssetBalance<AssetNameExchange>> {
    let asset_name = AssetNameExchange::new(asset?);
    let free = match free.and_then(|s| Decimal::from_str(s).ok()) {
        Some(v) => v,
        None => {
            warn!(%asset_name, "BinanceMargin isolated balance missing/unparseable 'free' field");
            return None;
        }
    };
    let locked = match locked.and_then(|s| Decimal::from_str(s).ok()) {
        Some(v) => v,
        None => {
            warn!(%asset_name, "BinanceMargin isolated balance missing/unparseable 'locked' field");
            return None;
        }
    };
    let borrowed = borrowed
        .and_then(|s| Decimal::from_str(s).ok())
        .unwrap_or(Decimal::ZERO);
    let interest = interest
        .and_then(|s| Decimal::from_str(s).ok())
        .unwrap_or(Decimal::ZERO);
    Some(AssetBalance::new(
        asset_name,
        Balance::new_margin(free + locked, free, borrowed, interest),
        time,
    ))
}

/// Map isolated `query_isolated_margin_account_info` `assets[]` entries to per-instrument
/// [`IsolatedInstrumentState`], keyed by instrument (pair symbol).
///
/// An entry must carry a `symbol` plus a `baseAsset` and `quoteAsset` with parseable `free`/`locked`;
/// an entry missing any of these is dropped with a `warn!` (observable, never silently mis-mapped).
/// Per-pair risk metrics (`marginLevel`/`marginRatio`/`liquidatePrice`) are best-effort `Option`s — a
/// missing/unparseable risk field becomes `None` and does NOT drop the entry. Every balance is
/// stamped `time`: the entries came from one REST response.
fn convert_isolated_margin_assets(
    assets: Vec<QueryIsolatedMarginAccountInfoResponseAssetsInner>,
    time: DateTime<Utc>,
) -> HashMap<InstrumentNameExchange, IsolatedInstrumentState<AssetNameExchange>> {
    let mut map = HashMap::with_capacity(assets.len());
    for entry in assets {
        let Some(symbol) = entry.symbol.as_deref() else {
            warn!("BinanceMargin isolated asset entry missing 'symbol' — skipping");
            continue;
        };
        let instrument = InstrumentNameExchange::new(symbol);

        let Some(base_raw) = entry.base_asset.as_deref() else {
            warn!(%instrument, "BinanceMargin isolated entry missing baseAsset — skipping");
            continue;
        };
        let Some(quote_raw) = entry.quote_asset.as_deref() else {
            warn!(%instrument, "BinanceMargin isolated entry missing quoteAsset — skipping");
            continue;
        };

        let Some(base) = convert_isolated_asset_balance(
            base_raw.asset.as_deref(),
            base_raw.free.as_deref(),
            base_raw.locked.as_deref(),
            base_raw.borrowed.as_deref(),
            base_raw.interest.as_deref(),
            time,
        ) else {
            warn!(%instrument, "BinanceMargin isolated baseAsset missing required field — skipping");
            continue;
        };
        let Some(quote) = convert_isolated_asset_balance(
            quote_raw.asset.as_deref(),
            quote_raw.free.as_deref(),
            quote_raw.locked.as_deref(),
            quote_raw.borrowed.as_deref(),
            quote_raw.interest.as_deref(),
            time,
        ) else {
            warn!(%instrument, "BinanceMargin isolated quoteAsset missing required field — skipping");
            continue;
        };

        let risk = IsolatedMarginRisk {
            margin_level: entry
                .margin_level
                .as_deref()
                .and_then(|s| Decimal::from_str(s).ok()),
            margin_ratio: entry
                .margin_ratio
                .as_deref()
                .and_then(|s| Decimal::from_str(s).ok()),
            liquidation_price: entry
                .liquidate_price
                .as_deref()
                .and_then(|s| Decimal::from_str(s).ok()),
        };

        map.insert(instrument, IsolatedInstrumentState { base, quote, risk });
    }
    map
}

/// Group symbols into comma-separated request strings, ≤5 symbols each (the venue's per-request
/// cap on `query_isolated_margin_account_info`'s `symbols`). An empty input yields no chunks.
fn chunk_symbols(symbols: &[InstrumentNameExchange]) -> Vec<String> {
    symbols
        .chunks(5)
        .map(|chunk| {
            chunk
                .iter()
                .map(|s| s.name().as_str())
                .collect::<Vec<_>>()
                .join(",")
        })
        .collect()
}

/// One request's `assets[]` entries from `query_isolated_margin_account_info`, with when the
/// request was sent ([`rest_read_with_retry`]).
struct IsolatedAccountRead {
    assets: Vec<QueryIsolatedMarginAccountInfoResponseAssetsInner>,
    sent: DateTime<Utc>,
}

/// Fetch isolated-margin account info for `symbols`, chunked at the venue's max-5-symbols/request
/// cap, one [`IsolatedAccountRead`] per chunk.
///
/// `symbols` must be non-empty (a no-symbol isolated call is invalid). Chunks are fetched
/// concurrently (bounded to 8) through the shared rate-limit/backoff machinery.
async fn fetch_isolated_margin_account_info(
    rest: Arc<RestApi>,
    rate_limiter: Arc<RateLimitTracker>,
    symbols: Vec<InstrumentNameExchange>,
) -> Result<Vec<IsolatedAccountRead>, UnindexedClientError> {
    use futures::{StreamExt as _, TryStreamExt as _};

    let chunks = chunk_symbols(&symbols);

    futures::stream::iter(chunks.into_iter().map(|symbols_param| {
        let rest = rest.clone();
        let rate_limiter = rate_limiter.clone();
        async move {
            let (response, sent) =
                rest_read_with_retry(&rest, &rate_limiter, RequestKind::Query, |rest| {
                    let symbols_param = symbols_param.clone();
                    Box::pin(async move {
                        let params = QueryIsolatedMarginAccountInfoParams::builder()
                            .symbols(symbols_param)
                            .build()?;
                        rest.query_isolated_margin_account_info(params).await
                    })
                })
                .await
                .map_err(|e| classify_rest_query_error(&e, None))?;

            let info = response.data().await.map_err(response_decode_error)?;
            Ok::<_, UnindexedClientError>(IsolatedAccountRead {
                assets: info.assets.unwrap_or_default(),
                sent,
            })
        }
    }))
    .buffer_unordered(8)
    .try_collect()
    .await
}

/// Convert a margin trade-list response into a rustrade [`Trade`].
///
/// Field-for-field identical to spot's `convert_my_trade` but typed to the margin SDK response and
/// stamped with [`ExchangeId::BinanceMargin`]. Fee asset is taken verbatim from `commissionAsset`
/// (may be base, quote, or third-party e.g. BNB); `fees_quote` is left `None` for the indexer to
/// resolve.
fn convert_margin_trade(
    t: &QueryMarginAccountsTradeListResponseInner,
    instrument: &InstrumentNameExchange,
) -> Option<Trade<AssetNameExchange, InstrumentNameExchange>> {
    let trade_id_raw = match t.id {
        Some(id) => id,
        None => {
            warn!(%instrument, "BinanceMargin trade missing id");
            return None;
        }
    };
    let trade_id = TradeId(format_smolstr!("{trade_id_raw}"));
    let order_id = match t.order_id {
        Some(id) => OrderId(format_smolstr!("{id}")),
        None => {
            warn!(%instrument, trade_id = %trade_id_raw, "BinanceMargin trade missing orderId");
            return None;
        }
    };
    let side = match t.is_buyer {
        Some(true) => Side::Buy,
        Some(false) => Side::Sell,
        None => {
            warn!(%instrument, trade_id = %trade_id_raw, "BinanceMargin trade missing isBuyer");
            return None;
        }
    };
    let price = match t.price.as_deref().and_then(|s| Decimal::from_str(s).ok()) {
        Some(v) => v,
        None => {
            warn!(%instrument, trade_id = %trade_id_raw, "BinanceMargin trade missing/unparseable price");
            return None;
        }
    };
    let quantity = match t.qty.as_deref().and_then(|s| Decimal::from_str(s).ok()) {
        Some(v) => v,
        None => {
            warn!(%instrument, trade_id = %trade_id_raw, "BinanceMargin trade missing/unparseable qty");
            return None;
        }
    };
    let commission = t
        .commission
        .as_deref()
        .and_then(|s| Decimal::from_str(s).ok())
        .unwrap_or(Decimal::ZERO);
    // The trade carries no other time, so without `time` it is kept, stamped with this host's
    // clock and marked so.
    let (time_exchange, time_source) = match t
        .time
        .and_then(|ms| Utc.timestamp_millis_opt(ms).single())
    {
        Some(ts) => (ts, TimeSource::Venue),
        None => {
            warn!(%instrument, trade_id = %trade_id_raw, "BinanceMargin trade missing/unparseable time, using now");
            (Utc::now(), TimeSource::Host)
        }
    };

    // Use actual commission asset from Binance (e.g., BNB, USDT, BTC). fees_quote is None here;
    // the indexer computes it when the fee is in quote or base asset. "UNKNOWN" fallback (rare:
    // API omits commissionAsset): the indexer keeps the trade with its fee zeroed, and warns,
    // rather than misattribute the fee to an asset.
    let fee_asset = t
        .commission_asset
        .as_deref()
        .map(AssetNameExchange::from)
        .unwrap_or_else(|| AssetNameExchange::from("UNKNOWN"));

    Some(
        Trade::new(
            trade_id,
            order_id,
            instrument.clone(),
            StrategyId::unknown(), // Binance doesn't carry strategy IDs
            time_exchange,
            side,
            price,
            quantity,
            // `myTrades` reports executions only -- no cumulative, no order status.
            None,
            AssetFees::new(fee_asset, commission, None),
        )
        .with_time_source(time_source),
    )
}

// ---------------------------------------------------------------------------
// Order conversion helpers (margin-specific)
// ---------------------------------------------------------------------------

/// Failure modes when constructing [`MarginAccountNewOrderParams`] from a rustrade request.
#[derive(Debug)]
enum BuildOrderError {
    /// The `OrderKind`/`TimeInForce` combination is not supported on Binance margin.
    Unsupported,
    /// The SDK params builder rejected the inputs (carries the builder's error message).
    Build(String),
    /// A trailing order cannot be sent as asked.
    Trailing(TrailingOrderError),
}

/// Construct a margin endpoint's `isIsolated` parameter enum from the configured margin mode.
///
/// The SDK generates a *separate* two-variant `isIsolated` enum per endpoint, all with identical
/// `True`/`False` variants serialising to `"TRUE"`/`"FALSE"`. This trait keeps the one
/// `bool` → wire-value mapping defined (and unit-tested) in a single place, as the former
/// `isolated_str` helper did, rather than repeating a ternary at each call site.
///
/// `true` → `True` (`"TRUE"`, isolated, per-pair sub-accounts); `false` → `False` (`"FALSE"`,
/// cross, account-wide). Threaded through every margin order/cancel/query call so the mode is
/// config-driven from a single source rather than hardcoded per-call.
trait IsolatedFlag {
    /// The endpoint's `isIsolated` variant for `is_isolated`.
    fn from_flag(is_isolated: bool) -> Self;
}

/// Implement [`IsolatedFlag`] for each endpoint's `isIsolated` enum.
///
/// Every one of these SDK enums is the same two-variant shape, so a macro keeps the impls honest:
/// adding an endpoint is one line here rather than a copy-pasted block that can silently invert.
macro_rules! impl_isolated_flag {
    ($($t:ty),* $(,)?) => {
        $(
            impl IsolatedFlag for $t {
                fn from_flag(is_isolated: bool) -> Self {
                    if is_isolated { Self::True } else { Self::False }
                }
            }
        )*
    };
}

impl_isolated_flag!(
    MarginAccountNewOrderIsIsolatedEnum,
    MarginAccountCancelOrderIsIsolatedEnum,
    QueryMarginAccountsOpenOrdersIsIsolatedEnum,
    QueryMarginAccountsOrderIsIsolatedEnum,
    QueryMarginAccountsTradeListIsIsolatedEnum,
    GetFutureHourlyInterestRateIsIsolatedEnum,
);

/// Build the margin cancel-order params (pure; no I/O).
///
/// Cancels by exchange `orderId` when present and parseable as `i64`, otherwise falls back to the
/// originating client order id (`origClientOrderId`). `isIsolated` is config-driven. Factored out of
/// [`BinanceMargin::cancel_order`] so the id-vs-cid fallback branch and the `isIsolated` value are
/// unit-testable without a live REST call. Returns the builder's error message (stringified) on the
/// rare build failure (e.g. a malformed symbol).
fn build_cancel_order_params(
    symbol: String,
    id: Option<&OrderId>,
    cid: &ClientOrderId,
    is_isolated: bool,
) -> Result<MarginAccountCancelOrderParams, String> {
    let mut builder = MarginAccountCancelOrderParams::builder(symbol).is_isolated(
        MarginAccountCancelOrderIsIsolatedEnum::from_flag(is_isolated),
    );

    match id {
        Some(order_id) => match order_id.0.parse::<i64>() {
            Ok(id) => builder = builder.order_id(id),
            Err(_) => {
                // exchange order id exists but isn't a valid i64 — fall back to the cid.
                error!(
                    order_id = %order_id.0,
                    "BinanceMargin cancel: exchange orderId not parseable as i64, falling back to clientOrderId"
                );
                builder = builder.orig_client_order_id(cid.0.to_string());
            }
        },
        None => builder = builder.orig_client_order_id(cid.0.to_string()),
    }

    builder.build().map_err(|e| e.to_string())
}

/// Build the margin new-order params from a rustrade order request (pure; no I/O).
///
/// Factored out of [`BinanceMargin::open_order`] so the rustrade→Binance mapping (sideEffectType,
/// `isIsolated`, conditional `stopPrice`, `trailingDelta`, `autoRepayAtCancel` gating) is
/// unit-testable without a live REST call. `isIsolated` is config-driven via `is_isolated`
/// (`true` = isolated, `false` = cross).
#[allow(clippy::too_many_arguments)] // mirrors the flat request fields; grouping them into a
// struct would just shuffle the same data and obscure the 1:1 mapping to SDK params.
fn build_new_order_params(
    symbol: String,
    side: Side,
    price: Option<Decimal>,
    quantity: Decimal,
    kind: OrderKind,
    time_in_force: TimeInForce,
    new_client_order_id: String,
    margin_effect: MarginEffect,
    auto_repay_at_cancel: bool,
    is_isolated: bool,
) -> Result<MarginAccountNewOrderParams, BuildOrderError> {
    let binance_side = match side {
        Side::Buy => MarginAccountNewOrderSideEnum::Buy,
        Side::Sell => MarginAccountNewOrderSideEnum::Sell,
    };

    let (binance_type, binance_tif) =
        convert_order_kind_tif_margin(kind, time_in_force).ok_or(BuildOrderError::Unsupported)?;

    // isIsolated is config-driven: "TRUE" for isolated, "FALSE" for cross.
    let mut builder = MarginAccountNewOrderParams::builder(symbol, binance_side, binance_type)
        .quantity(quantity)
        .is_isolated(MarginAccountNewOrderIsIsolatedEnum::from_flag(is_isolated))
        .side_effect_type(binance_side_effect(margin_effect))
        .new_client_order_id(new_client_order_id)
        .new_order_resp_type(MarginAccountNewOrderNewOrderRespTypeEnum::Full);

    // Binance applies `autoRepayAtCancel` to MARGIN_BUY only, and only that order has a loan to
    // repay when it is cancelled, so it is sent with `Borrow` and with nothing else.
    if margin_effect == MarginEffect::Borrow {
        builder = builder.auto_repay_at_cancel(auto_repay_at_cancel);
    }

    if let Some(tif) = binance_tif {
        builder = builder.time_in_force(tif);
    }

    // A trailing order's trailingDelta, fixed limit and activation price (as stopPrice), as on
    // spot.
    if let Some(trailing) = trailing_params(&kind, price).map_err(BuildOrderError::Trailing)? {
        builder = builder.trailing_delta(i64::from(trailing.trailing_delta));
        if let Some(limit) = trailing.price {
            builder = builder.price(limit);
        }
        if let Some(activation_price) = trailing.stop_price {
            builder = builder.stop_price(activation_price);
        }
    }

    // The other kinds' price fields (mirror spot): LIMIT carries price; STOP/TAKE_PROFIT carry a
    // stop (trigger) price; the *_LIMIT variants carry both. No wildcard, so that a new OrderKind
    // fails to compile until it is handled here.
    match kind {
        OrderKind::Limit => {
            builder = builder.price(price);
        }
        OrderKind::Stop { trigger_price } | OrderKind::TakeProfit { trigger_price } => {
            builder = builder.stop_price(trigger_price);
        }
        OrderKind::StopLimit { trigger_price } | OrderKind::TakeProfitLimit { trigger_price } => {
            builder = builder.price(price).stop_price(trigger_price);
        }
        // Trailing orders are set above. Market carries no price, and TrailingStopLimit and
        // ConditionalUnresolved are rejected by convert_order_kind_tif_margin before this.
        OrderKind::TrailingStop { .. }
        | OrderKind::TrailingStopFixedLimit { .. }
        | OrderKind::Market
        | OrderKind::TrailingStopLimit { .. }
        | OrderKind::ConditionalUnresolved { .. } => {}
    }

    builder
        .build()
        .map_err(|e| BuildOrderError::Build(e.to_string()))
}

/// The margin order endpoint, joined onto the REST config's base path: `POST` places an order and
/// `DELETE` cancels one.
const ORDER_PATH: &str = "/sapi/v1/margin/order";

/// The query of a margin new-order request, built from `params` as the SDK's own
/// `margin_account_new_order` builds it.
///
/// [`BinanceMargin::open_margin_order`] sends the order through the SDK's `send_request` instead of
/// that call, so that the answer decodes into [`NewOrderAnswer`]. The SDK's response type reads
/// `marginBuyBorrowAmount` as an integer, while Binance sends a decimal string, so every order
/// that borrows failed its decode. The destructuring names every field, so a field the SDK adds
/// fails to compile here rather than go unsent.
fn new_order_query(params: MarginAccountNewOrderParams) -> BTreeMap<String, serde_json::Value> {
    use serde_json::json;

    let MarginAccountNewOrderParams {
        symbol,
        side,
        r#type,
        is_isolated,
        quantity,
        quote_order_qty,
        price,
        stop_price,
        new_client_order_id,
        iceberg_qty,
        new_order_resp_type,
        side_effect_type,
        time_in_force,
        self_trade_prevention_mode,
        trailing_delta,
        auto_repay_at_cancel,
        recv_window,
    } = params;

    let mut query = BTreeMap::new();
    let mut insert = |key: &str, value: Option<serde_json::Value>| {
        if let Some(value) = value {
            query.insert(key.to_string(), value);
        }
    };
    insert("symbol", Some(json!(symbol)));
    insert("isIsolated", is_isolated.map(|v| json!(v)));
    insert("side", Some(json!(side)));
    insert("type", Some(json!(r#type)));
    insert("quantity", quantity.map(|v| json!(v)));
    insert("quoteOrderQty", quote_order_qty.map(|v| json!(v)));
    insert("price", price.map(|v| json!(v)));
    insert("stopPrice", stop_price.map(|v| json!(v)));
    insert("newClientOrderId", new_client_order_id.map(|v| json!(v)));
    insert("icebergQty", iceberg_qty.map(|v| json!(v)));
    insert("newOrderRespType", new_order_resp_type.map(|v| json!(v)));
    insert("sideEffectType", side_effect_type.map(|v| json!(v)));
    insert("timeInForce", time_in_force.map(|v| json!(v)));
    insert(
        "selfTradePreventionMode",
        self_trade_prevention_mode.map(|v| json!(v)),
    );
    insert("trailingDelta", trailing_delta.map(|v| json!(v)));
    insert("autoRepayAtCancel", auto_repay_at_cancel.map(|v| json!(v)));
    insert("recvWindow", recv_window.map(|v| json!(v)));
    query
}

/// The query of a margin cancel request, built from `params` as the SDK's own
/// `margin_account_cancel_order` builds it.
///
/// [`BinanceMargin::cancel_order`] sends the cancel through the SDK's `send_request` instead of that
/// call, so that the answer decodes into [`CancelOrderAnswer`]: the SDK's response type has no
/// `transactTime`. The destructuring names every field, so a field the SDK adds fails to compile
/// here rather than go unsent.
fn cancel_order_query(
    params: MarginAccountCancelOrderParams,
) -> BTreeMap<String, serde_json::Value> {
    use serde_json::json;

    let MarginAccountCancelOrderParams {
        symbol,
        is_isolated,
        order_id,
        orig_client_order_id,
        new_client_order_id,
        recv_window,
    } = params;

    let mut query = BTreeMap::new();
    let mut insert = |key: &str, value: Option<serde_json::Value>| {
        if let Some(value) = value {
            query.insert(key.to_string(), value);
        }
    };
    insert("symbol", Some(json!(symbol)));
    insert("isIsolated", is_isolated.map(|v| json!(v)));
    insert("orderId", order_id.map(|v| json!(v)));
    insert("origClientOrderId", orig_client_order_id.map(|v| json!(v)));
    insert("newClientOrderId", new_client_order_id.map(|v| json!(v)));
    insert("recvWindow", recv_window.map(|v| json!(v)));
    query
}

/// The fields of the answer to a margin cancel request that [`BinanceMargin::cancel_order`] reads.
///
/// Serde ignores the other fields, so a field the client does not read cannot fail the decode.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CancelOrderAnswer {
    /// Binance sends it as a string here, though as a number in its other order answers, so
    /// either is accepted.
    order_id: Option<WireInt>,
    transact_time: Option<i64>,
    executed_qty: Option<String>,
}

/// The fields of the answer to a margin new-order request that
/// [`BinanceMargin::open_margin_order`] reads.
///
/// Serde ignores the other fields, so a field the client does not read cannot fail the decode.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct NewOrderAnswer {
    order_id: Option<i64>,
    transact_time: Option<i64>,
    /// Absent from an `ACK` answer.
    status: Option<String>,
    /// Absent from an `ACK` answer.
    executed_qty: Option<String>,
    cummulative_quote_qty: Option<String>,
    /// What a `MARGIN_BUY` order borrowed, as a decimal string. Absent when it borrowed nothing.
    margin_buy_borrow_amount: Option<Decimal>,
    /// The asset `margin_buy_borrow_amount` is of.
    margin_buy_borrow_asset: Option<AssetNameExchange>,
}

/// Map a rustrade [`OrderKind`] + [`TimeInForce`] to the margin SDK's order `type`/`timeInForce`.
///
/// Reuses the shared [`classify_order_kind_tif`] decision logic, mapping only the venue-neutral
/// result onto margin's SDK output types — a [`MarginAccountNewOrderTypeEnum`] and a
/// [`MarginAccountNewOrderTimeInForceEnum`]. Mirrors spot's own `convert_order_kind_tif` adapter.
///
/// Returns `None` for unsupported combinations, as spot does: a `TrailingStop` maps to a
/// `STOP_LOSS` and a `TrailingStopFixedLimit` to a `STOP_LOSS_LIMIT` (with the `trailingDelta`
/// [`build_new_order_params`] adds), and `TrailingStopLimit` and Absolute trailing offsets are
/// unsupported.
fn convert_order_kind_tif_margin(
    kind: OrderKind,
    tif: TimeInForce,
) -> Option<(
    MarginAccountNewOrderTypeEnum,
    Option<MarginAccountNewOrderTimeInForceEnum>,
)> {
    let (binance_type, binance_tif) = classify_order_kind_tif(kind, tif)?;
    let margin_type = match binance_type {
        BinanceOrderType::Market => MarginAccountNewOrderTypeEnum::Market,
        BinanceOrderType::Limit => MarginAccountNewOrderTypeEnum::Limit,
        BinanceOrderType::LimitMaker => MarginAccountNewOrderTypeEnum::LimitMaker,
        BinanceOrderType::StopLoss => MarginAccountNewOrderTypeEnum::StopLoss,
        BinanceOrderType::StopLossLimit => MarginAccountNewOrderTypeEnum::StopLossLimit,
        BinanceOrderType::TakeProfit => MarginAccountNewOrderTypeEnum::TakeProfit,
        BinanceOrderType::TakeProfitLimit => MarginAccountNewOrderTypeEnum::TakeProfitLimit,
    };
    let margin_tif = binance_tif.map(|t| match t {
        BinanceTimeInForce::Gtc => MarginAccountNewOrderTimeInForceEnum::Gtc,
        BinanceTimeInForce::Ioc => MarginAccountNewOrderTimeInForceEnum::Ioc,
        BinanceTimeInForce::Fok => MarginAccountNewOrderTimeInForceEnum::Fok,
    });
    Some((margin_type, margin_tif))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use crate::client::binance::shared::convert_open_order;
    use crate::order::state::ActiveOrderState;
    use binance_sdk::margin_trading::rest_api::QueryMarginAccountsOpenOrdersResponseInner;

    #[test]
    fn margin_effect_default_is_no_side_effect() {
        assert_eq!(MarginEffect::default(), MarginEffect::NoSideEffect);
    }

    #[test]
    fn margin_effect_wire_values() {
        for (effect, wire) in [
            (MarginEffect::NoSideEffect, "NO_SIDE_EFFECT"),
            (MarginEffect::Borrow, "MARGIN_BUY"),
            (MarginEffect::Repay, "AUTO_REPAY"),
        ] {
            assert_eq!(binance_side_effect(effect).as_str(), wire);
        }
    }

    #[test]
    fn config_debug_redacts_secrets() {
        let config = BinanceMarginConfig::new(
            "my_api_key".to_string(),
            "my_secret_key".to_string(),
            false,
            false,
            Vec::new(),
            MarginEffect::default(),
            true,
        );
        let debug = format!("{config:?}");
        assert!(!debug.contains("my_api_key"));
        assert!(!debug.contains("my_secret_key"));
        assert!(debug.contains("***"));
    }

    #[test]
    fn cross_margin_uses_common_case_defaults() {
        let config = BinanceMarginConfig::cross_margin("k".to_string(), "s".to_string());
        assert!(!config.testnet);
        assert!(!config.is_isolated);
        assert_eq!(config.margin_effect, MarginEffect::NoSideEffect);
        assert!(config.auto_repay_at_cancel);
        assert_eq!(config.api_key(), "k");
    }

    #[test]
    fn config_deserializes_with_defaults() {
        // is_isolated, margin_effect and auto_repay_at_cancel default when omitted; testnet has no
        // default and must be present.
        let config: BinanceMarginConfig =
            serde_json::from_str(r#"{"api_key":"k","secret_key":"s","testnet":false}"#)
                .expect("deserialize");
        assert!(!config.is_isolated);
        assert_eq!(config.margin_effect, MarginEffect::NoSideEffect);
        assert!(config.auto_repay_at_cancel);
    }

    #[test]
    fn config_deserializes_explicit_margin_effect() {
        let config: BinanceMarginConfig = serde_json::from_str(
            r#"{"api_key":"k","secret_key":"s","testnet":false,"is_isolated":true,"margin_effect":"borrow","auto_repay_at_cancel":false}"#,
        )
        .expect("deserialize");
        assert!(config.is_isolated);
        assert_eq!(config.margin_effect, MarginEffect::Borrow);
        assert!(!config.auto_repay_at_cancel);
    }

    #[test]
    fn config_refuses_the_retired_combined_effect() {
        // Binance retired AUTO_BORROW_REPAY; a config naming it must not parse as something else.
        let parsed = serde_json::from_str::<BinanceMarginConfig>(
            r#"{"api_key":"k","secret_key":"s","testnet":false,"margin_effect":"auto_borrow_repay"}"#,
        );
        assert!(parsed.is_err());
    }

    #[test]
    fn isolated_ctor_sets_symbols_and_flag() {
        let symbols = vec![
            InstrumentNameExchange::new("BTCUSDT"),
            InstrumentNameExchange::new("ETHUSDT"),
        ];
        let config =
            BinanceMarginConfig::isolated("k".to_string(), "s".to_string(), symbols.clone());
        assert!(config.is_isolated);
        assert_eq!(config.isolated_symbols, symbols);
        assert_eq!(config.margin_effect, MarginEffect::NoSideEffect);
    }

    #[test]
    fn isolated_config_with_symbols_constructs() {
        // The construction gate only rejects isolated + *empty* symbols; a populated set is fine.
        let config = BinanceMarginConfig::isolated(
            "k".to_string(),
            "s".to_string(),
            vec![InstrumentNameExchange::new("BTCUSDT")],
        );
        let _client = BinanceMargin::new(config); // must not panic
    }

    #[test]
    #[should_panic(expected = "non-empty isolated_symbols")]
    fn isolated_empty_symbols_panics_at_new() {
        // is_isolated = true with no isolated_symbols is an unusable config → fail-fast panic.
        let config = BinanceMarginConfig::new(
            "k".to_string(),
            "s".to_string(),
            false,
            true,
            Vec::new(),
            MarginEffect::default(),
            true,
        );
        let _ = BinanceMargin::new(config);
    }

    #[test]
    #[should_panic(expected = "non-empty isolated_symbols")]
    fn isolated_empty_symbols_panics_via_deserialize_path() {
        // The gate must also cover the Deserialize-only path (a config file with is_isolated = true
        // and no isolated_symbols bypasses the named constructor).
        let config: BinanceMarginConfig = serde_json::from_str(
            r#"{"api_key":"k","secret_key":"s","testnet":false,"is_isolated":true}"#,
        )
        .expect("deserialize");
        assert!(config.isolated_symbols.is_empty());
        let _ = BinanceMargin::new(config);
    }

    #[test]
    fn isolated_flag_maps_mode() {
        // One assertion per endpoint enum: the SDK generates a separate `isIsolated` type for
        // each, so an inverted arm in any single impl would otherwise go unnoticed.
        assert_eq!(
            MarginAccountNewOrderIsIsolatedEnum::from_flag(true).as_str(),
            "TRUE"
        );
        assert_eq!(
            MarginAccountNewOrderIsIsolatedEnum::from_flag(false).as_str(),
            "FALSE"
        );
        assert_eq!(
            MarginAccountCancelOrderIsIsolatedEnum::from_flag(true).as_str(),
            "TRUE"
        );
        assert_eq!(
            MarginAccountCancelOrderIsIsolatedEnum::from_flag(false).as_str(),
            "FALSE"
        );
        assert_eq!(
            QueryMarginAccountsOpenOrdersIsIsolatedEnum::from_flag(true).as_str(),
            "TRUE"
        );
        assert_eq!(
            QueryMarginAccountsOpenOrdersIsIsolatedEnum::from_flag(false).as_str(),
            "FALSE"
        );
        assert_eq!(
            QueryMarginAccountsTradeListIsIsolatedEnum::from_flag(true).as_str(),
            "TRUE"
        );
        assert_eq!(
            QueryMarginAccountsTradeListIsIsolatedEnum::from_flag(false).as_str(),
            "FALSE"
        );
    }

    // -----------------------------------------------------------------------
    // Cancel param mapping (orderId-vs-cid fallback + config-driven isIsolated)
    // -----------------------------------------------------------------------

    #[test]
    fn cancel_params_cross_with_parseable_order_id() {
        let p = build_cancel_order_params(
            "BTCUSDT".to_string(),
            Some(&OrderId::new("12345")),
            &ClientOrderId::new("cid-1"),
            false,
        )
        .expect("build");
        assert_eq!(p.symbol, "BTCUSDT");
        assert_eq!(p.is_isolated.as_ref().map(|i| i.as_str()), Some("FALSE"));
        assert_eq!(p.order_id, Some(12345));
        assert_eq!(p.orig_client_order_id, None);
    }

    #[test]
    fn cancel_params_isolated_is_config_driven() {
        let p = build_cancel_order_params(
            "BTCUSDT".to_string(),
            Some(&OrderId::new("12345")),
            &ClientOrderId::new("cid-1"),
            true,
        )
        .expect("build");
        assert_eq!(p.is_isolated.as_ref().map(|i| i.as_str()), Some("TRUE"));
        assert_eq!(p.order_id, Some(12345));
    }

    #[test]
    fn cancel_params_non_i64_order_id_falls_back_to_cid() {
        let p = build_cancel_order_params(
            "BTCUSDT".to_string(),
            Some(&OrderId::new("not-an-i64")),
            &ClientOrderId::new("cid-1"),
            false,
        )
        .expect("build");
        assert_eq!(p.order_id, None);
        assert_eq!(p.orig_client_order_id.as_deref(), Some("cid-1"));
    }

    #[test]
    fn cancel_params_absent_order_id_uses_cid() {
        let p = build_cancel_order_params(
            "BTCUSDT".to_string(),
            None,
            &ClientOrderId::new("cid-1"),
            false,
        )
        .expect("build");
        assert_eq!(p.order_id, None);
        assert_eq!(p.orig_client_order_id.as_deref(), Some("cid-1"));
    }

    // -----------------------------------------------------------------------
    // Order param mapping (17.4.x)
    // -----------------------------------------------------------------------

    use crate::order::TrailingOffsetType;

    /// Build params for the common case (cross), overriding only what a test cares about.
    fn params(
        side: Side,
        price: Option<Decimal>,
        kind: OrderKind,
        tif: TimeInForce,
        margin_effect: MarginEffect,
    ) -> Result<MarginAccountNewOrderParams, BuildOrderError> {
        build_new_order_params(
            "BTCUSDT".to_string(),
            side,
            price,
            Decimal::from(2),
            kind,
            tif,
            "cid-1".to_string(),
            margin_effect,
            true,
            false, // cross
        )
    }

    fn gtc() -> TimeInForce {
        TimeInForce::GoodUntilCancelled { post_only: false }
    }

    #[test]
    fn new_order_limit_maps_core_fields() {
        let p = params(
            Side::Buy,
            Some(Decimal::from(50_000)),
            OrderKind::Limit,
            gtc(),
            MarginEffect::Borrow,
        )
        .expect("build");

        assert_eq!(p.symbol, "BTCUSDT");
        assert_eq!(p.side.as_str(), "BUY");
        assert_eq!(p.r#type.as_str(), "LIMIT");
        assert_eq!(p.quantity, Some(Decimal::from(2)));
        assert_eq!(p.price, Some(Decimal::from(50_000)));
        assert_eq!(p.stop_price, None);
        assert_eq!(p.time_in_force.as_ref().map(|t| t.as_str()), Some("GTC"));
        assert_eq!(p.new_client_order_id.as_deref(), Some("cid-1"));
        // FULL response so immediate fills come back in the order response.
        assert_eq!(
            p.new_order_resp_type.as_ref().map(|r| r.as_str()),
            Some("FULL")
        );
    }

    #[test]
    fn new_order_is_isolated_is_config_driven() {
        // Cross (is_isolated = false) → "FALSE".
        let cross = build_new_order_params(
            "BTCUSDT".to_string(),
            Side::Sell,
            Some(Decimal::from(10)),
            Decimal::from(2),
            OrderKind::Limit,
            gtc(),
            "cid-1".to_string(),
            MarginEffect::Borrow,
            true,
            false,
        )
        .expect("build");
        assert_eq!(
            cross.is_isolated.as_ref().map(|i| i.as_str()),
            Some("FALSE")
        );
        assert_eq!(cross.side.as_str(), "SELL");

        // Isolated (is_isolated = true) → "TRUE".
        let isolated = build_new_order_params(
            "BTCUSDT".to_string(),
            Side::Sell,
            Some(Decimal::from(10)),
            Decimal::from(2),
            OrderKind::Limit,
            gtc(),
            "cid-1".to_string(),
            MarginEffect::Borrow,
            true,
            true,
        )
        .expect("build");
        assert_eq!(
            isolated.is_isolated.as_ref().map(|i| i.as_str()),
            Some("TRUE")
        );
    }

    #[test]
    fn side_effect_and_auto_repay_gating() {
        let build = |effect, auto_repay_at_cancel| {
            build_new_order_params(
                "BTCUSDT".to_string(),
                Side::Buy,
                Some(Decimal::from(1)),
                Decimal::from(2),
                OrderKind::Limit,
                gtc(),
                "cid-1".to_string(),
                effect,
                auto_repay_at_cancel,
                false,
            )
            .expect("build")
        };
        let wire = |params: &MarginAccountNewOrderParams| {
            params
                .side_effect_type
                .as_ref()
                .map(|s| s.as_str().to_owned())
        };

        // Borrow → MARGIN_BUY, with autoRepayAtCancel as configured, either way explicit.
        let borrow = build(MarginEffect::Borrow, true);
        assert_eq!(wire(&borrow).as_deref(), Some("MARGIN_BUY"));
        assert_eq!(borrow.auto_repay_at_cancel, Some(true));
        let borrow_off = build(MarginEffect::Borrow, false);
        assert_eq!(borrow_off.auto_repay_at_cancel, Some(false));

        // Repay and NoSideEffect have no loan to repay at cancel, so send no autoRepayAtCancel.
        let repay = build(MarginEffect::Repay, true);
        assert_eq!(wire(&repay).as_deref(), Some("AUTO_REPAY"));
        assert_eq!(repay.auto_repay_at_cancel, None);
        let none = build(MarginEffect::NoSideEffect, true);
        assert_eq!(wire(&none).as_deref(), Some("NO_SIDE_EFFECT"));
        assert_eq!(none.auto_repay_at_cancel, None);
    }

    #[test]
    fn new_order_market_has_no_price_or_tif() {
        let p = params(
            Side::Buy,
            None,
            OrderKind::Market,
            gtc(),
            MarginEffect::Borrow,
        )
        .expect("build");
        assert_eq!(p.r#type.as_str(), "MARKET");
        assert_eq!(p.price, None);
        assert_eq!(p.stop_price, None);
        assert!(p.time_in_force.is_none());
    }

    #[test]
    fn post_only_limit_maps_to_limit_maker() {
        let p = params(
            Side::Buy,
            Some(Decimal::from(10)),
            OrderKind::Limit,
            TimeInForce::GoodUntilCancelled { post_only: true },
            MarginEffect::Borrow,
        )
        .expect("build");
        assert_eq!(p.r#type.as_str(), "LIMIT_MAKER");
        // LIMIT_MAKER carries no timeInForce (post-only is the type, not a TIF).
        assert!(p.time_in_force.is_none());
    }

    #[test]
    fn conditional_kinds_set_stop_price() {
        let trigger = Decimal::from(48_000);

        let stop = params(
            Side::Sell,
            None,
            OrderKind::Stop {
                trigger_price: trigger,
            },
            gtc(),
            MarginEffect::Borrow,
        )
        .expect("build");
        assert_eq!(stop.r#type.as_str(), "STOP_LOSS");
        assert_eq!(stop.stop_price, Some(trigger));
        assert_eq!(stop.price, None);

        let stop_limit = params(
            Side::Sell,
            Some(Decimal::from(47_900)),
            OrderKind::StopLimit {
                trigger_price: trigger,
            },
            gtc(),
            MarginEffect::Borrow,
        )
        .expect("build");
        assert_eq!(stop_limit.r#type.as_str(), "STOP_LOSS_LIMIT");
        assert_eq!(stop_limit.stop_price, Some(trigger));
        assert_eq!(stop_limit.price, Some(Decimal::from(47_900)));
        assert_eq!(
            stop_limit.time_in_force.as_ref().map(|t| t.as_str()),
            Some("GTC")
        );

        let take_profit = params(
            Side::Sell,
            None,
            OrderKind::TakeProfit {
                trigger_price: trigger,
            },
            gtc(),
            MarginEffect::Borrow,
        )
        .expect("build");
        assert_eq!(take_profit.r#type.as_str(), "TAKE_PROFIT");
        assert_eq!(take_profit.stop_price, Some(trigger));
    }

    /// A `TrailingStop` is a `STOP_LOSS` with `trailingDelta` in basis points and no `stopPrice`,
    /// as on spot.
    #[test]
    fn a_trailing_stop_maps_to_stop_loss_with_trailing_delta() {
        let cases = [
            (Decimal::from(25), TrailingOffsetType::BasisPoints, 25),
            (Decimal::new(15, 1), TrailingOffsetType::Percentage, 150),
        ];
        for (offset, offset_type, trailing_delta) in cases {
            let p = params(
                Side::Sell,
                None,
                OrderKind::TrailingStop {
                    offset,
                    offset_type,
                    activation_price: None,
                },
                gtc(),
                MarginEffect::NoSideEffect,
            )
            .expect("build");
            assert_eq!(p.r#type.as_str(), "STOP_LOSS");
            assert_eq!(p.trailing_delta, Some(trailing_delta), "{offset_type:?}");
            assert_eq!(p.stop_price, None);
            assert_eq!(p.price, None);
            assert!(p.time_in_force.is_none());
        }
    }

    /// An activation price goes as `stopPrice`. A `TrailingStopFixedLimit` is a
    /// `STOP_LOSS_LIMIT` with `trailingDelta`, its fixed limit as `price`, and a stop-limit's time
    /// in force.
    #[test]
    fn trailing_orders_carry_their_activation_price_and_fixed_limit() {
        let activation = Some(Decimal::from(50_000));
        let trailing_stop = params(
            Side::Sell,
            None,
            OrderKind::TrailingStop {
                offset: Decimal::from(25),
                offset_type: TrailingOffsetType::BasisPoints,
                activation_price: activation,
            },
            gtc(),
            MarginEffect::NoSideEffect,
        )
        .expect("build");
        assert_eq!(trailing_stop.r#type.as_str(), "STOP_LOSS");
        assert_eq!(trailing_stop.trailing_delta, Some(25));
        assert_eq!(trailing_stop.stop_price, activation);
        assert_eq!(trailing_stop.price, None);

        for activation_price in [None, activation] {
            let fixed_limit = params(
                Side::Sell,
                Some(Decimal::from(49_000)),
                OrderKind::TrailingStopFixedLimit {
                    offset: Decimal::new(15, 1),
                    offset_type: TrailingOffsetType::Percentage,
                    activation_price,
                },
                TimeInForce::ImmediateOrCancel,
                MarginEffect::NoSideEffect,
            )
            .expect("build");
            assert_eq!(fixed_limit.r#type.as_str(), "STOP_LOSS_LIMIT");
            assert_eq!(fixed_limit.trailing_delta, Some(150));
            assert_eq!(fixed_limit.price, Some(Decimal::from(49_000)));
            assert_eq!(fixed_limit.stop_price, activation_price);
            assert_eq!(
                fixed_limit.time_in_force.as_ref().map(|t| t.as_str()),
                Some("IOC")
            );
        }

        let absolute = params(
            Side::Sell,
            Some(Decimal::from(49_000)),
            OrderKind::TrailingStopFixedLimit {
                offset: Decimal::from(100),
                offset_type: TrailingOffsetType::Absolute,
                activation_price: None,
            },
            gtc(),
            MarginEffect::NoSideEffect,
        );
        assert!(matches!(absolute, Err(BuildOrderError::Unsupported)));

        // Refused unsent: a fixed limit without its price, and an activation price of zero.
        let no_limit = params(
            Side::Sell,
            None,
            OrderKind::TrailingStopFixedLimit {
                offset: Decimal::from(25),
                offset_type: TrailingOffsetType::BasisPoints,
                activation_price: None,
            },
            gtc(),
            MarginEffect::NoSideEffect,
        );
        assert!(matches!(
            no_limit,
            Err(BuildOrderError::Trailing(TrailingOrderError::LimitPrice(
                None
            )))
        ));
        let zero_activation = params(
            Side::Sell,
            None,
            OrderKind::TrailingStop {
                offset: Decimal::from(25),
                offset_type: TrailingOffsetType::BasisPoints,
                activation_price: Some(Decimal::ZERO),
            },
            gtc(),
            MarginEffect::NoSideEffect,
        );
        assert!(matches!(
            zero_activation,
            Err(BuildOrderError::Trailing(
                TrailingOrderError::NonPositiveActivation(_)
            ))
        ));
    }

    #[test]
    fn absolute_trailing_offsets_and_trailing_stop_limits_are_unsupported() {
        let absolute = params(
            Side::Sell,
            None,
            OrderKind::TrailingStop {
                offset: Decimal::from(100),
                offset_type: TrailingOffsetType::Absolute,
                activation_price: None,
            },
            gtc(),
            MarginEffect::Borrow,
        );
        assert!(matches!(absolute, Err(BuildOrderError::Unsupported)));

        let trailing_stop_limit = params(
            Side::Sell,
            Some(Decimal::from(100)),
            OrderKind::TrailingStopLimit {
                offset: Decimal::from(100),
                offset_type: TrailingOffsetType::BasisPoints,
                stop_price: Decimal::from(95),
                limit_offset: Decimal::from(10),
            },
            gtc(),
            MarginEffect::Borrow,
        );
        assert!(matches!(
            trailing_stop_limit,
            Err(BuildOrderError::Unsupported)
        ));

        // Only read back from a venue, never placed.
        let unresolved = params(
            Side::Sell,
            Some(Decimal::from(100)),
            OrderKind::ConditionalUnresolved {
                family: crate::order::ConditionalFamily::StopLimit,
                trigger_price: Some(Decimal::from(95)),
            },
            gtc(),
            MarginEffect::Borrow,
        );
        assert!(matches!(unresolved, Err(BuildOrderError::Unsupported)));
    }

    /// An offset that is not a whole number of basis points is refused, not rounded.
    #[test]
    fn a_fractional_trailing_offset_is_refused() {
        let fractional = params(
            Side::Sell,
            None,
            OrderKind::TrailingStop {
                offset: Decimal::new(155, 3),
                offset_type: TrailingOffsetType::Percentage,
                activation_price: None,
            },
            gtc(),
            MarginEffect::Borrow,
        );
        assert!(matches!(
            fractional,
            Err(BuildOrderError::Trailing(TrailingOrderError::Delta(
                crate::client::binance::shared::TrailingDeltaError::Invalid(_)
            )))
        ));
    }

    /// The kinds recalled through the client are the ones it then knows its orders by, and what
    /// the limit leaves out is warned of once the lock is released, and only then.
    #[test]
    fn recalled_order_kinds_are_known_and_those_past_the_limit_warned_of() {
        use crate::client::order_recovery::tests::logged_with_lock_free;

        let client = BinanceMargin::new(BinanceMarginConfig::cross_margin(
            "k".to_string(),
            "s".to_string(),
        ));
        let trailing = OrderKind::TrailingStopFixedLimit {
            offset: Decimal::from(100),
            offset_type: TrailingOffsetType::BasisPoints,
            activation_price: None,
        };
        let cid = |n: usize| ClientOrderId::new(format!("cid-{n}"));

        let seen = logged_with_lock_free(&client.known_live, || {
            client.recall_order_kinds((0..MAX_KNOWN_LIVE_ORDERS).map(|n| (cid(n), trailing)));
        });
        assert!(seen.is_empty(), "nothing past the limit: {seen:?}");
        assert_eq!(client.known_live.lock().kind_of(&cid(0)), Some(trailing));

        let seen = logged_with_lock_free(&client.known_live, || {
            client.recall_order_kinds([(cid(MAX_KNOWN_LIVE_ORDERS), trailing)]);
        });
        assert_eq!(seen, [(tracing::Level::WARN, true)]);
        assert_eq!(
            client
                .known_live
                .lock()
                .kind_of(&cid(MAX_KNOWN_LIVE_ORDERS)),
            None
        );
    }

    #[test]
    fn tif_variants_map_to_margin_enum() {
        for (tif, expected) in [
            (TimeInForce::ImmediateOrCancel, "IOC"),
            (TimeInForce::FillOrKill, "FOK"),
            (gtc(), "GTC"),
        ] {
            let p = params(
                Side::Buy,
                Some(Decimal::from(1)),
                OrderKind::Limit,
                tif,
                MarginEffect::Borrow,
            )
            .expect("build");
            assert_eq!(p.time_in_force.as_ref().map(|t| t.as_str()), Some(expected));
        }
    }

    // -----------------------------------------------------------------------
    // Account query converters (17.5.x)
    // -----------------------------------------------------------------------

    fn user_asset(
        asset: &str,
        free: &str,
        locked: &str,
        borrowed: &str,
        interest: &str,
    ) -> QueryCrossMarginAccountDetailsResponseUserAssetsInner {
        let mut a = QueryCrossMarginAccountDetailsResponseUserAssetsInner::new();
        a.asset = Some(asset.to_string());
        a.free = Some(free.to_string());
        a.locked = Some(locked.to_string());
        a.borrowed = Some(borrowed.to_string());
        a.interest = Some(interest.to_string());
        a
    }

    #[test]
    fn margin_balance_entry_maps_debt() {
        // free=10, locked=2 → total=12; borrowed=3, interest=0.5 → net_asset = total - borrowed = 9.
        let ab =
            convert_margin_balance_entry(user_asset("USDT", "10", "2", "3", "0.5"), Utc::now())
                .expect("convert");
        assert_eq!(ab.asset.name().as_str(), "USDT");
        assert_eq!(ab.balance.total, Decimal::from(12));
        assert_eq!(ab.balance.free, Decimal::from(10));
        assert!(
            ab.balance.margin.is_some(),
            "REST snapshot must populate margin"
        );
        // net_asset deducts borrowed principal (interest is tracked separately, not deducted here).
        assert_eq!(ab.balance.net_asset(), Decimal::from(9));
    }

    #[test]
    fn margin_balance_short_is_negative_net() {
        // A short borrows more base than it holds: total=1, borrowed=5 → net_asset = -4.
        let ab = convert_margin_balance_entry(user_asset("BTC", "1", "0", "5", "0"), Utc::now())
            .expect("convert");
        assert_eq!(ab.balance.net_asset(), Decimal::from(-4));
    }

    #[test]
    fn margin_balance_missing_debt_defaults_to_zero() {
        // A userAsset with no borrowed/interest is a real no-debt position, not corrupt data:
        // still emit a margin Balance (carrying zero debt) so the REST snapshot always sets margin.
        let mut a = QueryCrossMarginAccountDetailsResponseUserAssetsInner::new();
        a.asset = Some("ETH".to_string());
        a.free = Some("4".to_string());
        a.locked = Some("0".to_string());
        let ab = convert_margin_balance_entry(a, Utc::now()).expect("convert");
        assert!(ab.balance.margin.is_some());
        assert_eq!(ab.balance.net_asset(), Decimal::from(4));
    }

    #[test]
    fn margin_balance_missing_free_is_dropped() {
        // Missing free/locked is corrupt (not no-debt) → drop the asset rather than guess.
        let mut a = QueryCrossMarginAccountDetailsResponseUserAssetsInner::new();
        a.asset = Some("USDT".to_string());
        a.locked = Some("0".to_string());
        assert!(convert_margin_balance_entry(a, Utc::now()).is_none());
    }

    #[test]
    fn margin_balance_filtering() {
        let assets = vec![
            user_asset("USDT", "10", "0", "0", "0"),
            user_asset("BTC", "1", "0", "0", "0"),
            user_asset("ETH", "5", "0", "0", "0"),
        ];
        // Empty filter → all assets.
        assert_eq!(
            filter_and_convert_margin_balances(assets.clone(), &[], Utc::now()).len(),
            3
        );
        // Subset filter → only requested assets.
        let filtered = filter_and_convert_margin_balances(
            assets,
            &[AssetNameExchange::new("BTC"), AssetNameExchange::new("ETH")],
            Utc::now(),
        );
        assert_eq!(filtered.len(), 2);
        assert!(filtered.iter().all(|b| b.asset.name().as_str() != "USDT"));
    }

    #[test]
    fn margin_balance_filtering_large_slice_uses_hashset_branch() {
        // >16 requested assets takes the HashSet path (vs the ≤16 linear scan) — exercise it.
        let assets = vec![
            user_asset("BTC", "1", "0", "0", "0"),
            user_asset("ETH", "5", "0", "0", "0"),
            user_asset("DOGE", "9", "0", "0", "0"), // present in account, not requested → dropped
        ];
        // 17 requested assets (A00..A16) forces the HashSet branch; BTC and ETH also requested.
        let mut requested: Vec<AssetNameExchange> = (0..17)
            .map(|i| AssetNameExchange::new(format!("A{i:02}")))
            .collect();
        requested.push(AssetNameExchange::new("BTC"));
        requested.push(AssetNameExchange::new("ETH"));
        assert!(
            requested.len() > 16,
            "must exceed the linear-scan threshold"
        );

        let filtered = filter_and_convert_margin_balances(assets, &requested, Utc::now());
        assert_eq!(filtered.len(), 2);
        assert!(filtered.iter().all(|b| b.asset.name().as_str() != "DOGE"));
    }

    // -----------------------------------------------------------------------
    // Isolated account snapshot converters + query scoping
    // -----------------------------------------------------------------------

    use binance_sdk::margin_trading::rest_api::{
        QueryIsolatedMarginAccountInfoResponseAssetsInnerBaseAsset as IsoBase,
        QueryIsolatedMarginAccountInfoResponseAssetsInnerQuoteAsset as IsoQuote,
    };

    fn d(s: &str) -> Decimal {
        Decimal::from_str(s).unwrap()
    }

    fn iso_base(asset: &str, free: &str, locked: &str, borrowed: &str, interest: &str) -> IsoBase {
        let mut b = IsoBase::new();
        b.asset = Some(asset.to_string());
        b.free = Some(free.to_string());
        b.locked = Some(locked.to_string());
        b.borrowed = Some(borrowed.to_string());
        b.interest = Some(interest.to_string());
        b
    }

    fn iso_quote(
        asset: &str,
        free: &str,
        locked: &str,
        borrowed: &str,
        interest: &str,
    ) -> IsoQuote {
        let mut q = IsoQuote::new();
        q.asset = Some(asset.to_string());
        q.free = Some(free.to_string());
        q.locked = Some(locked.to_string());
        q.borrowed = Some(borrowed.to_string());
        q.interest = Some(interest.to_string());
        q
    }

    fn iso_entry(
        symbol: &str,
        base: IsoBase,
        quote: IsoQuote,
        margin_level: Option<&str>,
        margin_ratio: Option<&str>,
        liquidate_price: Option<&str>,
    ) -> QueryIsolatedMarginAccountInfoResponseAssetsInner {
        let mut e = QueryIsolatedMarginAccountInfoResponseAssetsInner::new();
        e.symbol = Some(symbol.to_string());
        e.base_asset = Some(Box::new(base));
        e.quote_asset = Some(Box::new(quote));
        e.margin_level = margin_level.map(str::to_string);
        e.margin_ratio = margin_ratio.map(str::to_string);
        e.liquidate_price = liquidate_price.map(str::to_string);
        e
    }

    fn isolated_client(symbols: &[&str]) -> BinanceMargin {
        let config = BinanceMarginConfig::isolated(
            "k".to_string(),
            "s".to_string(),
            symbols
                .iter()
                .map(|s| InstrumentNameExchange::new(*s))
                .collect(),
        );
        BinanceMargin::new(config)
    }

    #[test]
    fn client_debug_shows_mode_and_margin_effect_without_credentials() {
        let debug = format!("{:?}", isolated_client(&["BTCUSDT"]));
        assert!(debug.contains("BinanceMargin"));
        assert!(debug.contains("is_isolated: true"));
        assert!(debug.contains("margin_effect: NoSideEffect"));
        assert!(!debug.contains("\"k\"") && !debug.contains("\"s\""));
    }

    #[test]
    fn isolated_assets_map_base_quote_and_risk() {
        // base: free 0.5 + locked 0.1 = total 0.6, borrowed 0.2 → net 0.4.
        // quote: free 1000 + locked 50 = total 1050, borrowed 300 → net 750.
        let entry = iso_entry(
            "BTCUSDT",
            iso_base("BTC", "0.5", "0.1", "0.2", "0.001"),
            iso_quote("USDT", "1000", "50", "300", "1.5"),
            Some("3.5"),
            Some("0.12"),
            Some("48000"),
        );
        let map = convert_isolated_margin_assets(vec![entry], Utc::now());
        let state = map
            .get(&InstrumentNameExchange::new("BTCUSDT"))
            .expect("entry present");

        assert_eq!(state.base.asset.name().as_str(), "BTC");
        assert_eq!(state.base.balance.total, d("0.6"));
        assert_eq!(state.base.balance.free, d("0.5"));
        assert_eq!(state.base.balance.net_asset(), d("0.4"));

        assert_eq!(state.quote.asset.name().as_str(), "USDT");
        assert_eq!(state.quote.balance.total, d("1050"));
        assert_eq!(state.quote.balance.net_asset(), d("750"));

        assert_eq!(state.risk.margin_level, Some(d("3.5")));
        assert_eq!(state.risk.margin_ratio, Some(d("0.12")));
        assert_eq!(state.risk.liquidation_price, Some(d("48000")));
    }

    #[test]
    fn isolated_base_short_is_negative_net() {
        // A short on the base side: total 0, borrowed 1.5 → net_asset = -1.5.
        let entry = iso_entry(
            "BTCUSDT",
            iso_base("BTC", "0", "0", "1.5", "0.001"),
            iso_quote("USDT", "100", "0", "0", "0"),
            None,
            None,
            None,
        );
        let map = convert_isolated_margin_assets(vec![entry], Utc::now());
        let state = map.get(&InstrumentNameExchange::new("BTCUSDT")).unwrap();
        assert_eq!(state.base.balance.net_asset(), d("-1.5"));
    }

    #[test]
    fn isolated_missing_debt_defaults_zero_and_risk_optional() {
        // No borrowed/interest = real no-debt position → margin still populated (zero debt), not
        // dropped. Absent risk fields → None, but the snapshot is still produced.
        let mut base = IsoBase::new();
        base.asset = Some("BTC".to_string());
        base.free = Some("0.5".to_string());
        base.locked = Some("0".to_string());
        let mut quote = IsoQuote::new();
        quote.asset = Some("USDT".to_string());
        quote.free = Some("100".to_string());
        quote.locked = Some("0".to_string());

        let map = convert_isolated_margin_assets(
            vec![iso_entry("BTCUSDT", base, quote, None, None, None)],
            Utc::now(),
        );
        let state = map.get(&InstrumentNameExchange::new("BTCUSDT")).unwrap();
        assert!(state.base.balance.margin.is_some());
        assert_eq!(state.base.balance.net_asset(), d("0.5"));
        assert_eq!(state.risk.margin_level, None);
        assert_eq!(state.risk.liquidation_price, None);
    }

    #[test]
    fn isolated_unparseable_risk_is_none_but_entry_kept() {
        let entry = iso_entry(
            "BTCUSDT",
            iso_base("BTC", "1", "0", "0", "0"),
            iso_quote("USDT", "1", "0", "0", "0"),
            Some("not_a_number"),
            None,
            None,
        );
        let map = convert_isolated_margin_assets(vec![entry], Utc::now());
        let state = map.get(&InstrumentNameExchange::new("BTCUSDT")).unwrap();
        assert_eq!(state.risk.margin_level, None);
    }

    #[test]
    fn isolated_missing_base_free_drops_entry() {
        // Missing free/locked is corrupt (not no-debt) → drop the whole pair entry.
        let mut base = IsoBase::new();
        base.asset = Some("BTC".to_string());
        base.locked = Some("0".to_string()); // no free
        let entry = iso_entry(
            "BTCUSDT",
            base,
            iso_quote("USDT", "100", "0", "0", "0"),
            None,
            None,
            None,
        );
        assert!(convert_isolated_margin_assets(vec![entry], Utc::now()).is_empty());
    }

    #[test]
    fn isolated_missing_symbol_drops_entry() {
        let mut e = QueryIsolatedMarginAccountInfoResponseAssetsInner::new();
        e.base_asset = Some(Box::new(iso_base("BTC", "1", "0", "0", "0")));
        e.quote_asset = Some(Box::new(iso_quote("USDT", "1", "0", "0", "0")));
        // no symbol set
        assert!(convert_isolated_margin_assets(vec![e], Utc::now()).is_empty());
    }

    #[test]
    fn chunk_symbols_batches_by_five() {
        let syms: Vec<InstrumentNameExchange> = (0..12)
            .map(|i| InstrumentNameExchange::new(format!("S{i:02}")))
            .collect();
        let chunks = chunk_symbols(&syms);
        assert_eq!(chunks.len(), 3, "12 symbols → 5 + 5 + 2");
        assert_eq!(chunks[0].split(',').count(), 5);
        assert_eq!(chunks[1].split(',').count(), 5);
        assert_eq!(chunks[2].split(',').count(), 2);

        // ≤5 → a single chunk.
        let three: Vec<_> = (0..3)
            .map(|i| InstrumentNameExchange::new(format!("S{i}")))
            .collect();
        assert_eq!(chunk_symbols(&three).len(), 1);

        // exactly 5 → a single chunk of 5.
        let five: Vec<_> = (0..5)
            .map(|i| InstrumentNameExchange::new(format!("S{i}")))
            .collect();
        let c = chunk_symbols(&five);
        assert_eq!(c.len(), 1);
        assert_eq!(c[0].split(',').count(), 5);

        // empty → no chunks (never a no-symbol call).
        assert!(chunk_symbols(&[]).is_empty());
    }

    #[test]
    fn effective_isolated_set_empty_returns_all_configured() {
        let client = isolated_client(&["BTCUSDT", "ETHUSDT"]);
        assert_eq!(
            client.effective_isolated_set(&[]),
            vec![
                InstrumentNameExchange::new("BTCUSDT"),
                InstrumentNameExchange::new("ETHUSDT"),
            ]
        );
    }

    #[test]
    fn effective_isolated_set_intersects_requested() {
        let client = isolated_client(&["BTCUSDT", "ETHUSDT"]);
        assert_eq!(
            client.effective_isolated_set(&[InstrumentNameExchange::new("ETHUSDT")]),
            vec![InstrumentNameExchange::new("ETHUSDT")]
        );
    }

    #[test]
    fn effective_isolated_set_skips_out_of_set() {
        let client = isolated_client(&["BTCUSDT", "ETHUSDT"]);
        // DOGEUSDT is not configured → skipped (warn), only BTCUSDT survives.
        assert_eq!(
            client.effective_isolated_set(&[
                InstrumentNameExchange::new("BTCUSDT"),
                InstrumentNameExchange::new("DOGEUSDT"),
            ]),
            vec![InstrumentNameExchange::new("BTCUSDT")]
        );
    }

    #[tokio::test]
    async fn fetch_balances_isolated_returns_empty() {
        // Isolated balances are per-pair (on account_snapshot); the asset-keyed fetch returns empty
        // without a network call.
        let client = isolated_client(&["BTCUSDT"]);
        assert!(client.fetch_balances(&[]).await.expect("ok").is_empty());
    }

    fn open_order(order_id: Option<i64>, side: &str) -> QueryMarginAccountsOpenOrdersResponseInner {
        let mut o = QueryMarginAccountsOpenOrdersResponseInner::new();
        o.order_id = order_id;
        o.client_order_id = Some("cid-9".to_string());
        o.side = Some(side.to_string());
        o.price = Some("50000".to_string());
        o.orig_qty = Some("2".to_string());
        o.executed_qty = Some("0.5".to_string());
        o.r#type = Some("LIMIT".to_string());
        o.time_in_force = Some("GTC".to_string());
        o.time = Some(1_700_000_000_000);
        o.status = Some("PARTIALLY_FILLED".to_string());
        o
    }

    #[test]
    fn margin_open_order_converts() {
        let inst = InstrumentNameExchange::new("BTCUSDT");
        let order = convert_open_order(
            &open_order(Some(42), "BUY"),
            ExchangeId::BinanceMargin,
            &inst,
            &|_| None,
        )
        .expect("convert");
        assert_eq!(order.key.exchange, ExchangeId::BinanceMargin);
        assert_eq!(
            order.state.id.assigned().map(|id| id.0.as_str()),
            Some("42")
        );
        assert_eq!(order.key.cid.0.as_str(), "cid-9");
        assert_eq!(order.side, Side::Buy);
        assert_eq!(order.price, Some(Decimal::from(50_000)));
        assert_eq!(order.quantity, Decimal::from(2));
        assert_eq!(order.state.filled_quantity, Decimal::new(5, 1));
        assert_eq!(order.kind, OrderKind::Limit);
    }

    /// Margin shares the converter, so it shares the guard: a cancelled order that had partly
    /// filled must not come back as `Open`.
    #[test]
    fn margin_open_order_that_is_not_live_is_dropped() {
        let inst = InstrumentNameExchange::new("BTCUSDT");
        let mut o = open_order(Some(42), "BUY");
        o.status = Some("CANCELED".to_string());
        assert!(convert_open_order(&o, ExchangeId::BinanceMargin, &inst, &|_| None).is_none());
    }

    #[test]
    fn margin_open_order_missing_order_id_is_dropped() {
        let inst = InstrumentNameExchange::new("BTCUSDT");
        assert!(
            convert_open_order(
                &open_order(None, "BUY"),
                ExchangeId::BinanceMargin,
                &inst,
                &|_| None
            )
            .is_none()
        );
    }

    /// A margin REST snapshot must be stamped with when the order last *changed*, not when it was
    /// created. The engine orders an order's states by `Open::time_exchange` and discards any
    /// snapshot older than the state it already tracks, so a creation stamp -- identical across
    /// every snapshot of one order -- is discarded the moment anything advances that order past
    /// creation. That is exactly the partially-filled order a reconciliation fetch exists to repair.
    #[test]
    fn margin_open_order_prefers_update_time_over_creation_time() {
        let inst = InstrumentNameExchange::new("BTCUSDT");

        let mut updated = open_order(Some(42), "BUY");
        updated.update_time = Some(1_700_000_005_000);
        let order = convert_open_order(&updated, ExchangeId::BinanceMargin, &inst, &|_| None)
            .expect("convert");
        assert_eq!(
            order.state.time_exchange,
            Utc.timestamp_millis_opt(1_700_000_005_000)
                .single()
                .unwrap(),
            "update_time must win over time"
        );
    }

    /// Falls back to `time` when the venue omits `update_time`, rather than to `now` -- which would
    /// be worse than creation time, since it is not a venue-reported instant at all.
    #[test]
    fn margin_open_order_falls_back_to_creation_time_when_update_time_absent() {
        let inst = InstrumentNameExchange::new("BTCUSDT");

        let base = open_order(Some(42), "BUY");
        assert!(base.update_time.is_none(), "fixture must omit update_time");
        let order = convert_open_order(&base, ExchangeId::BinanceMargin, &inst, &|_| None)
            .expect("convert");
        assert_eq!(
            order.state.time_exchange,
            Utc.timestamp_millis_opt(1_700_000_000_000)
                .single()
                .unwrap(),
            "absent update_time falls back to time"
        );
    }

    #[test]
    fn margin_open_order_owned_symbol_recovers_instrument() {
        // The no-symbol "return all" path derives the instrument from each order's own `symbol`.
        let mut o = open_order(Some(42), "SELL");
        o.symbol = Some("ETHUSDT".to_string());
        let order = convert_open_order_owned_symbol(&o, ExchangeId::BinanceMargin, &|_| None)
            .expect("convert");
        assert_eq!(order.key.instrument.name().as_str(), "ETHUSDT");
        assert_eq!(order.key.exchange, ExchangeId::BinanceMargin);
        assert_eq!(order.side, Side::Sell);
    }

    #[test]
    fn margin_open_order_owned_symbol_missing_symbol_is_dropped() {
        // open_order() leaves `symbol` unset — the return-all path must drop it rather than guess.
        let o = open_order(Some(42), "BUY");
        assert!(o.symbol.is_none());
        assert!(
            convert_open_order_owned_symbol(&o, ExchangeId::BinanceMargin, &|_| None).is_none()
        );
    }

    fn trade(id: Option<i64>, is_buyer: Option<bool>) -> QueryMarginAccountsTradeListResponseInner {
        let mut t = QueryMarginAccountsTradeListResponseInner::new();
        t.id = id;
        t.order_id = Some(7);
        t.is_buyer = is_buyer;
        t.price = Some("48000".to_string());
        t.qty = Some("0.25".to_string());
        t.commission = Some("0.001".to_string());
        t.commission_asset = Some("BNB".to_string());
        t.time = Some(1_700_000_000_000);
        t
    }

    #[test]
    fn margin_trade_converts() {
        let inst = InstrumentNameExchange::new("BTCUSDT");
        let tr = convert_margin_trade(&trade(Some(11), Some(false)), &inst).expect("convert");
        assert_eq!(tr.id.0.as_str(), "11");
        assert_eq!(tr.order_id.0.as_str(), "7");
        assert_eq!(tr.side, Side::Sell);
        assert_eq!(tr.price, Decimal::from(48_000));
        assert_eq!(tr.quantity, Decimal::new(25, 2));
        // Third-party fee asset (BNB) preserved verbatim; fees_quote left for the indexer.
        assert_eq!(tr.fees.asset.name().as_str(), "BNB");
        assert_eq!(tr.fees.fees, Decimal::new(1, 3));
    }

    /// A trade is at its `time`, the venue's; without one it is kept, at this host's time, and
    /// says so.
    #[test]
    fn a_margin_trade_without_a_time_is_at_this_hosts_time() {
        let inst = InstrumentNameExchange::new("BTCUSDT");
        let tr = convert_margin_trade(&trade(Some(11), Some(false)), &inst).unwrap();
        assert_eq!(
            (tr.time_exchange, tr.time_source),
            (
                Utc.timestamp_millis_opt(1_700_000_000_000).unwrap(),
                TimeSource::Venue
            )
        );
        let before = Utc::now();
        let mut untimed = trade(Some(11), Some(false));
        untimed.time = None;
        let tr = convert_margin_trade(&untimed, &inst).unwrap();
        assert_eq!(tr.time_source, TimeSource::Host);
        assert!((before..=Utc::now()).contains(&tr.time_exchange));
    }

    #[test]
    fn margin_trade_missing_fields_are_dropped() {
        let inst = InstrumentNameExchange::new("BTCUSDT");
        // Missing id and missing isBuyer both drop the trade rather than guess.
        assert!(convert_margin_trade(&trade(None, Some(true)), &inst).is_none());
        assert!(convert_margin_trade(&trade(Some(11), None), &inst).is_none());
    }

    // -- Isolated user-data stream: base/quote map ---------------------------------------------

    #[test]
    fn base_quote_map_extracts_base_and_quote() {
        let entries = vec![
            iso_entry(
                "BTCUSDT",
                iso_base("BTC", "0", "0", "0", "0"),
                iso_quote("USDT", "0", "0", "0", "0"),
                None,
                None,
                None,
            ),
            iso_entry(
                "ETHBTC",
                iso_base("ETH", "0", "0", "0", "0"),
                iso_quote("BTC", "0", "0", "0", "0"),
                None,
                None,
                None,
            ),
        ];
        let map = build_base_quote_map(&entries);
        assert_eq!(
            map.get(&InstrumentNameExchange::new("BTCUSDT"))
                .map(|(b, q)| (b.name().as_str(), q.name().as_str())),
            Some(("BTC", "USDT"))
        );
        // ETHBTC: prefix/suffix string-matching would be ambiguous (BTC is the quote here, base
        // elsewhere) — the authoritative map resolves it unambiguously to base=ETH, quote=BTC.
        assert_eq!(
            map.get(&InstrumentNameExchange::new("ETHBTC"))
                .map(|(b, q)| (b.name().as_str(), q.name().as_str())),
            Some(("ETH", "BTC"))
        );
    }

    /// A margin `myTrades` venue that enforces Binance's documented 24-hour rules: a
    /// `startTime`..`endTime` span of 24 hours or more is rejected, and a query by time without
    /// `fromId` returns at most 24 hours of trades. A `fromId` query returns trades from that id
    /// onwards whatever their time, and `orderId` narrows any query to one order. Pages hold at
    /// most `limit` trades, in ascending id.
    ///
    /// Two readings here are assumptions, not observed venue behaviour. A bare `startTime` is taken
    /// to mean the 24 hours from `startTime`; the pager under test never sends one, so only the
    /// previous pager's failure depends on it. `endTime` is inclusive unless `end_exclusive`, so a
    /// test can run under either reading.
    struct MarginMyTradesVenue {
        /// `(id, time_ms, order_id)`, in ascending id and time.
        trades: Vec<(i64, i64, i64)>,
        end_exclusive: bool,
    }

    impl wiremock::Respond for MarginMyTradesVenue {
        fn respond(&self, request: &wiremock::Request) -> wiremock::ResponseTemplate {
            const DAY_MS: i64 = 24 * 60 * 60 * 1000;
            let query: std::collections::HashMap<String, String> =
                request.url.query_pairs().into_owned().collect();
            let param = |name: &str| query.get(name).map(|v| v.parse::<i64>().unwrap());
            let limit = usize::try_from(param("limit").unwrap_or(500)).unwrap();
            let order = param("orderId");

            let mut selected: Vec<&(i64, i64, i64)> = self
                .trades
                .iter()
                .filter(|(_, _, order_id)| order.is_none_or(|order| *order_id == order))
                .collect();
            if let Some(from_id) = param("fromId") {
                selected.retain(|(id, _, _)| *id >= from_id);
            } else if order.is_none() {
                let start = param("startTime").expect("a query by time sends startTime");
                let end = match param("endTime") {
                    Some(end) if end - start >= DAY_MS => {
                        return wiremock::ResponseTemplate::new(400).set_body_json(
                            serde_json::json!({"code": -1127, "msg": "More than 24 hours between startTime and endTime."}),
                        );
                    }
                    Some(end) if self.end_exclusive => end - 1,
                    Some(end) => end,
                    None => start + DAY_MS - 1,
                };
                selected.retain(|(_, time, _)| (start..=end).contains(time));
            }

            let page: Vec<serde_json::Value> = selected
                .into_iter()
                .take(limit)
                .map(|(id, time, order_id)| {
                    serde_json::json!({
                        "id": id, "symbol": "BTCUSDT", "time": time, "orderId": order_id,
                        "price": "50000", "qty": "0.01", "commission": "0",
                        "commissionAsset": "USDT", "isBuyer": true, "isMaker": false,
                        "isBestMatch": true, "isIsolated": true,
                    })
                })
                .collect();
            wiremock::ResponseTemplate::new(200).set_body_json(page)
        }
    }

    /// A walk by time from `start` to the local clock's present.
    fn since(start: i64) -> MyTradesFrom {
        MyTradesFrom::Span {
            start,
            end: Utc::now().timestamp_millis(),
        }
    }

    /// Run `paginate_margin_my_trades` from `from` against `venue`, returning the ids read and the
    /// query parameters of each request sent.
    async fn read_margin_trades(
        venue: MarginMyTradesVenue,
        from: MyTradesFrom,
    ) -> (Vec<i64>, Vec<std::collections::HashMap<String, String>>) {
        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .and(wiremock::matchers::path("/sapi/v1/margin/myTrades"))
            .respond_with(venue)
            .mount(&server)
            .await;
        let rest = Arc::new(MarginTradingRestApi::from_config(
            ConfigurationRestApi::builder()
                .api_key("key")
                .api_secret("secret")
                .base_path(server.uri())
                .build()
                .unwrap(),
        ));

        let read = paginate_margin_my_trades(
            &rest,
            &Arc::new(RateLimitTracker::new(WeightPool::Sapi)),
            &InstrumentNameExchange::new("BTCUSDT"),
            from,
            true,
            RequestKind::Query,
        )
        .await
        .unwrap();

        let requests = server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .map(|request| request.url.query_pairs().into_owned().collect())
            .collect();
        (read.into_iter().map(|t| t.id.unwrap()).collect(), requests)
    }

    /// Recovery reads each recovered fill's order once, in the account it trades, for the client
    /// order id `myTrades` lacks. An order Binance no longer knows leaves its fill without one.
    #[tokio::test]
    async fn recovered_margin_fills_carry_their_orders_client_order_ids() {
        recovered_margin_fills_carry_client_order_ids(true).await;
        recovered_margin_fills_carry_client_order_ids(false).await;
    }

    async fn recovered_margin_fills_carry_client_order_ids(is_isolated: bool) {
        let disconnect = Utc::now() - chrono::Duration::minutes(10);
        let reconnect = Utc::now() - chrono::Duration::minutes(5);
        let fill_time = (disconnect + chrono::Duration::minutes(1)).timestamp_millis();
        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .and(wiremock::matchers::path("/sapi/v1/margin/myTrades"))
            .respond_with(MarginMyTradesVenue {
                trades: vec![(1, fill_time, 1), (2, fill_time, 2)],
                end_exclusive: false,
            })
            .mount(&server)
            .await;
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .and(wiremock::matchers::path("/sapi/v1/margin/order"))
            .and(wiremock::matchers::query_param("orderId", "1"))
            .and(wiremock::matchers::query_param(
                "isIsolated",
                if is_isolated { "TRUE" } else { "FALSE" },
            ))
            .respond_with(wiremock::ResponseTemplate::new(200).set_body_json(
                serde_json::json!({"symbol": "BTCUSDT", "orderId": 1, "clientOrderId": "repay-1"}),
            ))
            .mount(&server)
            .await;
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .and(wiremock::matchers::path("/sapi/v1/margin/order"))
            .and(wiremock::matchers::query_param("orderId", "2"))
            .respond_with(
                wiremock::ResponseTemplate::new(400).set_body_json(
                    serde_json::json!({"code": -2013, "msg": "Order does not exist."}),
                ),
            )
            .mount(&server)
            .await;
        let rest = Arc::new(MarginTradingRestApi::from_config(
            ConfigurationRestApi::builder()
                .api_key("key")
                .api_secret("secret")
                .base_path(server.uri())
                .build()
                .unwrap(),
        ));
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut unrecovered = UnrecoveredFills::default();
        unrecovered.open(
            &[InstrumentNameExchange::new("BTCUSDT")],
            disconnect,
            reconnect,
        );

        recover_margin_fills(
            &rest,
            &Arc::new(RateLimitTracker::new(WeightPool::Sapi)),
            &mut unrecovered,
            &tx,
            &new_dedup_cache(),
            &KnownLiveOrders::shared(ExchangeId::BinanceMargin),
            is_isolated,
        )
        .await;

        let mut cids: Vec<_> = std::iter::from_fn(|| rx.try_recv().ok())
            .filter_map(|event| match event.kind {
                AccountEventKind::Trade(trade) => Some((
                    trade.id.0.to_string(),
                    trade.client_order_id,
                    trade.order_filled_quantity.is_some(),
                )),
                _ => None,
            })
            .collect();
        cids.sort();
        // The cumulatives from the walks, read first, are untouched by the reads after them.
        assert_eq!(
            cids,
            [
                ("1".to_string(), Some(ClientOrderId::new("repay-1")), true),
                ("2".to_string(), None, true),
            ],
            "is_isolated: {is_isolated}"
        );
        let order_reads = server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .filter(|request| request.url.path() == "/sapi/v1/margin/order")
            .count();
        assert_eq!(order_reads, 2, "one read per order");
    }

    /// A venue whose trades each belong to an order of the same id.
    fn venue(trades: Vec<(i64, i64)>) -> MarginMyTradesVenue {
        MarginMyTradesVenue {
            trades: trades
                .into_iter()
                .map(|(id, time)| (id, time, id))
                .collect(),
            end_exclusive: false,
        }
    }

    const HOUR_MS: i64 = 60 * 60 * 1000;

    /// The only fill lies on day 3 of a five-day lookback. A bare `startTime` query sees only the
    /// first 24 hours, finds nothing and stops; the windows must reach day 3.
    #[tokio::test]
    async fn margin_trades_read_a_lone_fill_beyond_the_first_day() {
        let now = Utc::now().timestamp_millis();
        let (ids, _) = read_margin_trades(
            venue(vec![(7, now - 72 * HOUR_MS)]),
            since(now - 120 * HOUR_MS),
        )
        .await;

        assert_eq!(ids, vec![7]);
    }

    /// Fills in separate days, with empty days between and a short page in the first window
    /// that holds one: every fill is read once, in order.
    #[tokio::test]
    async fn margin_trades_read_fills_across_several_windows_once_each() {
        let now = Utc::now().timestamp_millis();
        let trades = vec![
            (1, now - 119 * HOUR_MS),
            (2, now - 72 * HOUR_MS),
            (3, now - HOUR_MS),
        ];
        let (ids, _) = read_margin_trades(venue(trades), since(now - 144 * HOUR_MS)).await;

        assert_eq!(ids, vec![1, 2, 3]);
    }

    /// A trade stamped exactly on the end of the first window is read once, whether the venue
    /// treats `endTime` as inclusive or exclusive.
    #[tokio::test]
    async fn margin_trades_read_a_trade_on_a_window_boundary_once() {
        for end_exclusive in [false, true] {
            let now = Utc::now().timestamp_millis();
            let from = now - 48 * HOUR_MS;
            let venue = MarginMyTradesVenue {
                end_exclusive,
                ..venue(vec![(5, from + MARGIN_MY_TRADES_WINDOW_MS)])
            };
            let (ids, _) = read_margin_trades(venue, since(from)).await;

            assert_eq!(ids, vec![5], "end_exclusive: {end_exclusive}");
        }
    }

    /// A window holding more than one page of fills pages on by `fromId`.
    #[tokio::test]
    async fn margin_trades_page_on_by_id_past_a_full_window() {
        let now = Utc::now().timestamp_millis();
        let count = i64::try_from(BINANCE_MAX_TRADES).unwrap() + 1;
        let trades = (1..=count)
            .map(|id| (id, now - 48 * HOUR_MS + id))
            .collect();
        let (ids, _) = read_margin_trades(venue(trades), since(now - 50 * HOUR_MS)).await;

        assert_eq!(ids, (1..=count).collect::<Vec<_>>());
    }

    /// A final window holding more than one page still pages on by `fromId`: the early stop
    /// after the final window applies only to a short page.
    #[tokio::test]
    async fn margin_trades_page_on_past_a_full_final_window() {
        let now = Utc::now().timestamp_millis();
        let count = i64::try_from(BINANCE_MAX_TRADES).unwrap() + 1;
        let trades = (1..=count).map(|id| (id, now - HOUR_MS + id)).collect();
        let (ids, requests) = read_margin_trades(venue(trades), since(now - 2 * HOUR_MS)).await;

        assert_eq!(ids, (1..=count).collect::<Vec<_>>());
        assert_eq!(requests.len(), 2);
    }

    /// A lookback with no fills costs one request per window, and every window is accepted
    /// (the venue rejects a span of 24 hours or more, which would fail the read).
    #[tokio::test]
    async fn margin_trades_quiet_lookback_sends_one_request_per_window() {
        let now = Utc::now().timestamp_millis();
        let (ids, requests) =
            read_margin_trades(venue(Vec::new()), since(now - 72 * HOUR_MS)).await;

        assert!(ids.is_empty());
        // Windows start at 0, 23, 46 and 69 hours into the 72-hour lookback.
        assert_eq!(requests.len(), 4);
    }

    /// An ordinary reconnect gap holding a fill is one request: the only window reaches the
    /// call's start and reads short, so there is nothing to page on to.
    #[tokio::test]
    async fn margin_trades_read_a_short_gap_in_one_request() {
        let now = Utc::now().timestamp_millis();
        let (ids, requests) =
            read_margin_trades(venue(vec![(9, now - HOUR_MS)]), since(now - 2 * HOUR_MS)).await;

        assert_eq!(ids, vec![9]);
        assert_eq!(requests.len(), 1);
    }

    /// A span ending past the local clock, as a read placed on a venue clock ahead of it ends,
    /// reads a fill the venue stamped after the local clock's present.
    #[tokio::test]
    async fn margin_trades_read_past_the_local_clock_to_the_spans_end() {
        let now = Utc::now().timestamp_millis();
        let span = MyTradesFrom::Span {
            start: now - HOUR_MS,
            end: now + HOUR_MS,
        };
        let (ids, requests) = read_margin_trades(venue(vec![(4, now + HOUR_MS / 2)]), span).await;

        assert_eq!(ids, vec![4]);
        assert_eq!(requests.len(), 1);
    }

    /// An end far past the local clock is cut to one window past it: an empty walk steps through
    /// the windows up to there and stops, rather than on toward the end.
    #[tokio::test]
    async fn margin_trades_to_a_far_end_stop_one_window_past_the_local_clock() {
        let now = Utc::now().timestamp_millis();
        let span = MyTradesFrom::Span {
            start: now - HOUR_MS,
            end: i64::MAX,
        };
        let (ids, requests) = read_margin_trades(venue(Vec::new()), span).await;

        assert!(ids.is_empty());
        // Windows from 1 hour before the local clock to 22 hours after it, then to 23 hours.
        assert_eq!(requests.len(), 2);
        let last_end = requests[1]["endTime"].parse::<i64>().unwrap();
        let cut = now + MARGIN_MY_TRADES_WINDOW_MS;
        // The cut is taken from the clock when the walk starts, a moment after `now`.
        assert!(
            (cut..cut + 60_000).contains(&last_end),
            "the last window ends at {last_end}, not one window past {now}"
        );
    }

    /// A walk from a time after the call's start has nothing to read and sends nothing.
    #[tokio::test]
    async fn margin_trades_from_the_future_send_no_request() {
        let now = Utc::now().timestamp_millis();
        let (ids, requests) = read_margin_trades(venue(Vec::new()), since(now + HOUR_MS)).await;

        assert!(ids.is_empty());
        assert!(requests.is_empty());
    }

    /// A walk by order pages that order's executions by `fromId`, never by time, and reads no
    /// other order's.
    #[tokio::test]
    async fn margin_trades_by_order_page_by_id_without_time_bounds() {
        let now = Utc::now().timestamp_millis();
        let count = i64::try_from(BINANCE_MAX_TRADES).unwrap() + 1;
        // Order 42's executions, interleaved with another order's.
        let trades = (1..=2 * count)
            .map(|id| {
                (
                    id,
                    now - 48 * HOUR_MS + id,
                    if id % 2 == 0 { 42 } else { 7 },
                )
            })
            .collect();
        let venue = MarginMyTradesVenue {
            trades,
            end_exclusive: false,
        };
        let (ids, requests) = read_margin_trades(venue, MyTradesFrom::Order(42)).await;

        assert_eq!(ids, (1..=count).map(|n| 2 * n).collect::<Vec<_>>());
        assert_eq!(requests.len(), 2);
        assert!(
            requests.iter().all(
                |query| query.get("orderId").map(String::as_str) == Some("42")
                    && !query.contains_key("startTime")
                    && !query.contains_key("endTime")
            )
        );
    }

    /// Run `paginate_margin_my_trades` against a venue that answers every request with
    /// `response`, returning the error it fails with.
    async fn margin_trades_error(response: wiremock::ResponseTemplate) -> UnindexedClientError {
        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .and(wiremock::matchers::path("/sapi/v1/margin/myTrades"))
            .respond_with(response)
            .mount(&server)
            .await;
        let rest = Arc::new(MarginTradingRestApi::from_config(
            ConfigurationRestApi::builder()
                .api_key("key")
                .api_secret("secret")
                .base_path(server.uri())
                .build()
                .unwrap(),
        ));

        paginate_margin_my_trades(
            &rest,
            &Arc::new(RateLimitTracker::new(WeightPool::Sapi)),
            &InstrumentNameExchange::new("BTCUSDT"),
            since(Utc::now().timestamp_millis() - HOUR_MS),
            true,
            RequestKind::Query,
        )
        .await
        .unwrap_err()
    }

    /// A venue rejection of a query is reported as such, with its code, and not as a transient
    /// connectivity failure a caller would retry forever.
    #[tokio::test]
    async fn margin_trades_report_a_venue_rejection_as_a_non_transient_request_rejection() {
        let err = margin_trades_error(wiremock::ResponseTemplate::new(400).set_body_json(
            serde_json::json!({"code": -1127, "msg": "More than 24 hours between startTime and endTime."}),
        ))
        .await;

        let UnindexedClientError::Api(ApiError::RequestRejected(msg)) = &err else {
            panic!("expected RequestRejected, got {err:?}");
        };
        assert!(msg.contains("-1127"), "code missing from {msg:?}");
        assert!(!err.is_transient());
    }

    /// A 2xx body that does not fit the SDK's model fails identically on every retry.
    #[tokio::test]
    async fn margin_trades_report_an_undecodable_response_as_internal() {
        let err = margin_trades_error(
            wiremock::ResponseTemplate::new(200)
                .set_body_json(serde_json::json!({"unexpected": 1})),
        )
        .await;

        assert!(
            matches!(err, UnindexedClientError::Internal(_)),
            "got {err:?}"
        );
        assert!(!err.is_transient());
    }

    /// A `/sapi` response reporting `x-sapi-used-ip-weight-1m` at 90% of the documented IP limit
    /// pauses later queries but not orders; under it, or unreadable, nothing pauses.
    #[tokio::test]
    async fn sapi_used_ip_weight_near_the_limit_pauses_queries() {
        for (used, pauses) in [("10799", false), ("10800", true), ("not a number", false)] {
            let server = wiremock::MockServer::start().await;
            wiremock::Mock::given(wiremock::matchers::method("GET"))
                .and(wiremock::matchers::path("/sapi/v1/margin/openOrders"))
                .respond_with(
                    wiremock::ResponseTemplate::new(200)
                        .insert_header("x-sapi-used-ip-weight-1m", used)
                        .set_body_json(serde_json::json!([])),
                )
                .mount(&server)
                .await;
            let rest = Arc::new(MarginTradingRestApi::from_config(
                ConfigurationRestApi::builder()
                    .api_key("key")
                    .api_secret("secret")
                    .base_path(server.uri())
                    .build()
                    .unwrap(),
            ));
            let tracker = Arc::new(RateLimitTracker::new(WeightPool::Sapi));

            fetch_margin_all_open_orders(
                rest,
                Arc::clone(&tracker),
                KnownLiveOrders::shared(ExchangeId::BinanceMargin),
                false,
            )
            .await
            .unwrap();

            let query_waits = tokio::time::timeout(
                Duration::from_millis(50),
                tracker.wait_if_blocked(RequestKind::Query),
            )
            .await
            .is_err();
            assert_eq!(query_waits, pauses, "used IP weight {used}");
            tokio::time::timeout(
                Duration::from_millis(50),
                tracker.wait_if_blocked(RequestKind::Order),
            )
            .await
            .expect("orders never wait on the pause")
            .expect("no cooldown too long to sleep through");
        }
    }

    /// Margin fill recovery does not wait for the pause near the weight limit either.
    #[tokio::test]
    async fn margin_fill_recovery_does_not_wait_for_the_weight_pause() {
        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .and(wiremock::matchers::path("/sapi/v1/margin/myTrades"))
            // One trade, so recovery also looks its order up: both reads run under the pause.
            .respond_with(
                wiremock::ResponseTemplate::new(200).set_body_json(serde_json::json!([{
                    "symbol": "BTCUSDT",
                    "id": 1,
                    "orderId": 7,
                    "price": "100",
                    "qty": "1",
                    "commission": "0",
                    "commissionAsset": "USDT",
                    "time": Utc::now().timestamp_millis(),
                    "isBuyer": true,
                    "isMaker": false,
                }])),
            )
            .expect(2)
            .mount(&server)
            .await;
        let rest = Arc::new(MarginTradingRestApi::from_config(
            ConfigurationRestApi::builder()
                .api_key("key")
                .api_secret("secret")
                .base_path(server.uri())
                .build()
                .unwrap(),
        ));
        let tracker = Arc::new(RateLimitTracker::new(WeightPool::Sapi));
        tracker.throttle(Duration::from_secs(60));
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        let mut unrecovered = UnrecoveredFills::default();
        unrecovered.open(
            &[InstrumentNameExchange::new("BTCUSDT")],
            Utc::now() - chrono::Duration::minutes(1),
            Utc::now(),
        );

        // Far under the 60 s pause set above, and ample for two local round trips.
        tokio::time::timeout(
            Duration::from_millis(800),
            recover_margin_fills(
                &rest,
                &tracker,
                &mut unrecovered,
                &tx,
                &new_dedup_cache(),
                &KnownLiveOrders::shared(ExchangeId::BinanceMargin),
                false,
            ),
        )
        .await
        .expect("recovery does not wait for the pause");
    }

    /// A walk over a span reads only the trades inside it, stopping at the first page by id that
    /// reaches past its end.
    #[tokio::test]
    async fn margin_trades_read_over_a_span_stop_at_its_end() {
        let now = Utc::now().timestamp_millis();
        let trades = vec![
            (1, now - 50 * HOUR_MS),
            (2, now - 49 * HOUR_MS),
            (3, now - 10 * HOUR_MS),
        ];
        let (ids, _) = read_margin_trades(
            venue(trades),
            MyTradesFrom::Span {
                start: now - 60 * HOUR_MS,
                end: now - 20 * HOUR_MS,
            },
        )
        .await;

        assert_eq!(ids, vec![1, 2]);
    }

    /// A margin gap whose every read fails is given up, and reported on the stream with the last read's
    /// error.
    #[tokio::test]
    async fn a_margin_gap_whose_every_read_fails_is_reported_when_given_up() {
        let disconnect = Utc::now() - chrono::Duration::minutes(10);
        let reconnect = Utc::now() - chrono::Duration::minutes(5);
        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .and(wiremock::matchers::path("/sapi/v1/margin/myTrades"))
            .respond_with(
                wiremock::ResponseTemplate::new(400)
                    .set_body_json(serde_json::json!({"code": -1100, "msg": "rejected"})),
            )
            .mount(&server)
            .await;
        let rest = Arc::new(MarginTradingRestApi::from_config(
            ConfigurationRestApi::builder()
                .api_key("key")
                .api_secret("secret")
                .base_path(server.uri())
                .build()
                .expect("valid config"),
        ));
        let tracker = Arc::new(RateLimitTracker::new(WeightPool::Sapi));
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let dedup = new_dedup_cache();
        let btc = InstrumentNameExchange::new("BTCUSDT");
        let mut unrecovered = UnrecoveredFills::default();
        unrecovered.open(std::slice::from_ref(&btc), disconnect, reconnect);

        for _ in 0..=crate::client::order_recovery::MAX_GAP_RETRIES {
            recover_margin_fills(
                &rest,
                &tracker,
                &mut unrecovered,
                &tx,
                &dedup,
                &KnownLiveOrders::shared(ExchangeId::BinanceMargin),
                false,
            )
            .await;
            unrecovered.make_due();
        }

        assert!(unrecovered.is_empty(), "the gap is given up");
        let forwarded: Vec<_> = std::iter::from_fn(|| rx.try_recv().ok()).collect();
        let [event] = forwarded.as_slice() else {
            panic!("only the give-up is sent: {forwarded:?}");
        };
        let AccountEventKind::FillRecoveryGaveUp(gave_up) = &event.kind else {
            panic!("expected FillRecoveryGaveUp, got {event:?}");
        };
        assert_eq!(event.exchange, ExchangeId::BinanceMargin);
        assert_eq!(
            gave_up.scope,
            crate::FillRecoveryScope::Instruments(vec![btc])
        );
        assert_eq!(
            gave_up.start.timestamp_millis(),
            disconnect.timestamp_millis()
        );
        assert_eq!(
            gave_up.attempts,
            crate::client::order_recovery::MAX_GAP_RETRIES + 1
        );
        assert!(
            matches!(&gave_up.reason, FillRecoveryFailure::Request(reason) if reason.contains("rejected")),
            "the last read's error: {:?}",
            gave_up.reason
        );
    }

    /// A margin recovery that fails keeps its gap, closed at its own start; the retry reads only
    /// that span, forwards only the fill inside it, and clears it.
    #[tokio::test]
    async fn a_failed_margin_recovery_keeps_its_closed_gap_for_the_retry() {
        let disconnect = Utc::now() - chrono::Duration::minutes(10);
        let reconnect = Utc::now() - chrono::Duration::minutes(5);
        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .and(wiremock::matchers::path("/sapi/v1/margin/myTrades"))
            .respond_with(
                wiremock::ResponseTemplate::new(400)
                    .set_body_json(serde_json::json!({"code": -1100, "msg": "rejected"})),
            )
            .up_to_n_times(1)
            .with_priority(1)
            .mount(&server)
            .await;
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .and(wiremock::matchers::path("/sapi/v1/margin/myTrades"))
            .respond_with(venue(vec![
                (
                    1,
                    (disconnect + chrono::Duration::minutes(1)).timestamp_millis(),
                ),
                (
                    2,
                    (Utc::now() - chrono::Duration::minutes(1)).timestamp_millis(),
                ),
            ]))
            .mount(&server)
            .await;
        let rest = Arc::new(MarginTradingRestApi::from_config(
            ConfigurationRestApi::builder()
                .api_key("key")
                .api_secret("secret")
                .base_path(server.uri())
                .build()
                .unwrap(),
        ));
        let tracker = Arc::new(RateLimitTracker::new(WeightPool::Sapi));
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let dedup = new_dedup_cache();
        let mut unrecovered = UnrecoveredFills::default();
        unrecovered.open(
            &[InstrumentNameExchange::new("BTCUSDT")],
            disconnect,
            reconnect,
        );

        recover_margin_fills(
            &rest,
            &tracker,
            &mut unrecovered,
            &tx,
            &dedup,
            &KnownLiveOrders::shared(ExchangeId::BinanceMargin),
            false,
        )
        .await;
        assert!(!unrecovered.is_empty(), "the failed gap is kept");
        assert!(rx.try_recv().is_err(), "nothing forwarded");

        unrecovered.make_due();
        recover_margin_fills(
            &rest,
            &tracker,
            &mut unrecovered,
            &tx,
            &dedup,
            &KnownLiveOrders::shared(ExchangeId::BinanceMargin),
            false,
        )
        .await;
        assert!(unrecovered.is_empty(), "the retry recovered it");
        let forwarded: Vec<_> = std::iter::from_fn(|| rx.try_recv().ok()).collect();
        assert_eq!(
            forwarded.len(),
            1,
            "only the fill inside the gap: {forwarded:?}"
        );
    }

    /// When margin recovery times out, a gap whose read started has failed and waits for its retry, but
    /// one never started (eight are read at a time) is not charged and stays due.
    #[tokio::test]
    async fn a_timed_out_margin_recovery_charges_only_the_gaps_it_started() {
        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .and(wiremock::matchers::path("/sapi/v1/margin/myTrades"))
            .respond_with(
                wiremock::ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!([]))
                    .set_delay(Duration::from_secs(3_600)),
            )
            .mount(&server)
            .await;
        let rest = Arc::new(MarginTradingRestApi::from_config(
            ConfigurationRestApi::builder()
                .api_key("key")
                .api_secret("secret")
                .base_path(server.uri())
                .timeout(3_600_000_u64)
                .retries(0_u32)
                .build()
                .unwrap(),
        ));
        let instruments: Vec<_> = (0..9)
            .map(|i| InstrumentNameExchange::new(format!("SYM{i}USDT")))
            .collect();
        let mut unrecovered = UnrecoveredFills::default();
        unrecovered.open(
            &instruments,
            Utc::now() - chrono::Duration::minutes(10),
            Utc::now(),
        );
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        // Paused: once every read is waiting, the clock jumps to the recovery timeout.
        tokio::time::pause();

        recover_margin_fills(
            &rest,
            &Arc::new(RateLimitTracker::new(WeightPool::Sapi)),
            &mut unrecovered,
            &tx,
            &new_dedup_cache(),
            &KnownLiveOrders::shared(ExchangeId::BinanceMargin),
            false,
        )
        .await;

        let now = tokio::time::Instant::now();
        assert_eq!(
            unrecovered.due(now).len(),
            1,
            "the gap never started stays due"
        );
        assert_eq!(
            unrecovered
                .due(now + Duration::from_secs(crate::client::order_recovery::GAP_RETRY_BASE_SECS))
                .len(),
            9,
            "the eight started wait for their first retry"
        );
    }

    // -----------------------------------------------------------------------
    // How orders ended: fetch_ended_orders, recover_margin_ended_orders, the known-live feed
    // -----------------------------------------------------------------------

    /// A margin order row for client order id `cid` in `status`: 1 of 2 filled, for 105 quote.
    fn margin_order_row(cid: &str, status: &str) -> serde_json::Value {
        serde_json::json!({
            "symbol": "BTCUSDT", "orderId": 7, "clientOrderId": cid, "price": "100",
            "origQty": "2", "executedQty": "1", "cummulativeQuoteQty": "105", "status": status,
            "timeInForce": "GTC", "type": "LIMIT", "side": "BUY", "isIsolated": false,
            "time": 1_700_000_000_000_i64, "updateTime": 1_700_000_060_000_i64,
        })
    }

    fn margin_key(instrument: &str, cid: &str) -> UnindexedOrderKey {
        OrderKey::new(
            ExchangeId::BinanceMargin,
            InstrumentNameExchange::new(instrument),
            StrategyId::new("strategy"),
            ClientOrderId::new(cid),
        )
    }

    #[test]
    fn a_margin_order_row_says_how_the_order_ended() {
        use crate::order::state::InactiveOrderState;
        use binance_sdk::margin_trading::rest_api::QueryMarginAccountsOrderResponse;
        let key = margin_key("BTCUSDT", "a");
        let ended = |row: serde_json::Value| {
            let row: QueryMarginAccountsOrderResponse = serde_json::from_value(row).unwrap();
            convert_ended_order(&row, ExchangeId::BinanceMargin, &key, None)
                .map(|order| order.state)
        };

        let mut filled = margin_order_row("a", "FILLED");
        filled["executedQty"] = "2".into();
        filled["cummulativeQuoteQty"] = "210".into();
        let Some(InactiveOrderState::FullyFilled(filled)) = ended(filled) else {
            panic!("FILLED is fully filled");
        };
        assert_eq!(filled.avg_price, Some(Decimal::from(105)));
        assert!(matches!(
            ended(margin_order_row("a", "CANCELED")),
            Some(InactiveOrderState::Cancelled(cancelled)) if cancelled.filled_quantity == Some(Decimal::ONE)
        ));
        assert_eq!(ended(margin_order_row("a", "NEW")), None);

        // Binance's sign that it does not have the quote traded for a historical order.
        let mut unknown_quote = margin_order_row("a", "FILLED");
        unknown_quote["executedQty"] = "2".into();
        unknown_quote["cummulativeQuoteQty"] = "-1".into();
        let Some(InactiveOrderState::FullyFilled(filled)) = ended(unknown_quote) else {
            panic!("still fully filled");
        };
        assert_eq!(filled.avg_price, None, "no negative average price");
    }

    /// A REST config whose requests go to `server`.
    fn margin_rest_config_at(server: &wiremock::MockServer) -> ConfigurationRestApi {
        ConfigurationRestApi::builder()
            .api_key("key")
            .api_secret("secret")
            .base_path(server.uri())
            .build()
            .unwrap()
    }

    fn margin_rest_at(server: &wiremock::MockServer) -> Arc<RestApi> {
        Arc::new(MarginTradingRestApi::from_config(margin_rest_config_at(
            server,
        )))
    }

    /// A client of the cross account, or of the isolated BTCUSDT one, talking to `server`.
    pub(super) fn margin_client_at(
        server: &wiremock::MockServer,
        is_isolated: bool,
    ) -> BinanceMargin {
        let config = if is_isolated {
            BinanceMarginConfig::isolated(
                "key".into(),
                "secret".into(),
                vec![InstrumentNameExchange::new("BTCUSDT")],
            )
        } else {
            BinanceMarginConfig::cross_margin("key".into(), "secret".into())
        };
        let mode = config.pre_send_rules;
        let mut client = <BinanceMargin as ExecutionClient>::new(config);
        client.rest = margin_rest_at(server);
        client.rest_config = Arc::new(margin_rest_config_at(server));
        client.rules = rules_at(server, mode);
        client
    }

    /// Rules loaded from the spot `exchangeInfo` that `server` answers.
    fn rules_at(server: &wiremock::MockServer, mode: PreSendRules) -> Arc<RulesLoader> {
        Arc::new(RulesLoader::new(
            mode,
            Arc::new(binance_sdk::spot::SpotRestApi::from_config(
                ConfigurationRestApi::builder()
                    .api_key("key")
                    .api_secret("secret")
                    .base_path(server.uri())
                    .build()
                    .unwrap(),
            )),
            Arc::new(RateLimitTracker::new(WeightPool::Spot)),
        ))
    }

    /// Answer `GET /sapi/v1/margin/order` for client order id `cid` with `response`.
    async fn mount_margin_order(
        server: &wiremock::MockServer,
        cid: &str,
        response: wiremock::ResponseTemplate,
    ) {
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .and(wiremock::matchers::path("/sapi/v1/margin/order"))
            .and(wiremock::matchers::query_param("origClientOrderId", cid))
            .respond_with(response)
            .mount(server)
            .await;
    }

    fn margin_venue_error(code: i64) -> wiremock::ResponseTemplate {
        wiremock::ResponseTemplate::new(400)
            .set_body_json(serde_json::json!({"code": code, "msg": "refused"}))
    }

    fn margin_row_response(cid: &str, status: &str) -> wiremock::ResponseTemplate {
        wiremock::ResponseTemplate::new(200).set_body_json(margin_order_row(cid, status))
    }

    /// The path and `isIsolated` of every request `server` received.
    async fn margin_requests(server: &wiremock::MockServer) -> Vec<(String, Option<String>)> {
        server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .map(|request| {
                let isolated = request
                    .url
                    .query_pairs()
                    .find(|(key, _)| key == "isIsolated")
                    .map(|(_, value)| value.into_owned());
                (request.url.path().to_owned(), isolated)
            })
            .collect()
    }

    /// Each balance a margin read returns is stamped [`REST_BALANCE_LAG`] before its request was
    /// sent, never when the answer arrives: the cross account's REST view has trailed its
    /// stream, and a stream balance or debt event the read does not yet show must stay more
    /// recent than it.
    #[tokio::test]
    async fn a_cross_balance_read_is_stamped_the_rest_lag_before_its_request_was_sent() {
        margin_balance_reads_are_stamped_the_rest_lag_before_their_requests(false).await;
    }

    /// As for the cross account, for an isolated pair's balances, which come only on its
    /// instrument snapshot.
    #[tokio::test]
    async fn an_isolated_balance_read_is_stamped_the_rest_lag_before_its_request_was_sent() {
        margin_balance_reads_are_stamped_the_rest_lag_before_their_requests(true).await;
    }

    /// Read the account's balances with both `fetch_balances` and `account_snapshot`, each answer
    /// delayed, and check every balance's time.
    async fn margin_balance_reads_are_stamped_the_rest_lag_before_their_requests(
        is_isolated: bool,
    ) {
        use crate::client::binance::shared::REST_BALANCE_LAG;
        const ANSWER_DELAY: Duration = Duration::from_millis(200);
        let asset = |name: &str| {
            serde_json::json!({
                "asset": name, "free": "1", "locked": "0", "borrowed": "0.5", "interest": "0",
                "netAsset": "0.5",
            })
        };
        let (account_path, body) = if is_isolated {
            (
                "/sapi/v1/margin/isolated/account",
                serde_json::json!({ "assets": [{
                    "symbol": "BTCUSDT", "baseAsset": asset("BTC"), "quoteAsset": asset("USDT"),
                }] }),
            )
        } else {
            (
                "/sapi/v1/margin/account",
                serde_json::json!({ "userAssets": [asset("BTC")] }),
            )
        };
        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .and(wiremock::matchers::path(account_path))
            .respond_with(
                wiremock::ResponseTemplate::new(200)
                    .set_body_json(body)
                    .set_delay(ANSWER_DELAY),
            )
            .mount(&server)
            .await;
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .and(wiremock::matchers::path("/sapi/v1/margin/openOrders"))
            .respond_with(wiremock::ResponseTemplate::new(200).set_body_json(serde_json::json!([])))
            .mount(&server)
            .await;
        let client = margin_client_at(&server, is_isolated);

        let before = Utc::now();
        let fetched = client.fetch_balances(&[]).await.unwrap();
        let between = Utc::now();
        let snapshot = client
            .account_snapshot(&[], &[InstrumentNameExchange::new("BTCUSDT")])
            .await
            .unwrap();
        let after = Utc::now();

        // Each read's balances, and the span of host time its request was sent in.
        let reads: Vec<(Vec<DateTime<Utc>>, _, _)> = if is_isolated {
            // Isolated balances are the pair's, on its instrument snapshot.
            assert!(fetched.is_empty());
            let pair = snapshot.instruments[0].isolated.as_ref().unwrap();
            vec![(
                vec![pair.base.time_exchange, pair.quote.time_exchange],
                between,
                after,
            )]
        } else {
            let times = |balances: &[AssetBalance<AssetNameExchange>]| {
                balances
                    .iter()
                    .map(|balance| balance.time_exchange)
                    .collect()
            };
            vec![
                (times(&fetched), before, between),
                (times(&snapshot.balances), between, after),
            ]
        };
        for (times, from, to) in reads {
            assert!(!times.is_empty());
            for time in times {
                assert!(
                    from <= time + REST_BALANCE_LAG && time + REST_BALANCE_LAG + ANSWER_DELAY <= to,
                    "{time} + the lag not within {from} ..= {to} - {ANSWER_DELAY:?}"
                );
            }
        }
    }

    /// A cross restatement read the venue fails is an error; the next returns each balance, debt
    /// included, as a balance snapshot.
    #[tokio::test]
    async fn a_cross_restatement_read_returns_the_balances_as_snapshots() {
        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .and(wiremock::matchers::path("/sapi/v1/margin/account"))
            .respond_with(wiremock::ResponseTemplate::new(500))
            .up_to_n_times(1)
            .mount(&server)
            .await;
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .and(wiremock::matchers::path("/sapi/v1/margin/account"))
            .respond_with(wiremock::ResponseTemplate::new(200).set_body_json(
                serde_json::json!({ "userAssets": [{
                    "asset": "BTC", "free": "1", "locked": "0", "borrowed": "0.5", "interest": "0",
                    "netAsset": "0.5",
                }] }),
            ))
            .mount(&server)
            .await;
        let (rest, limiter) = (
            margin_rest_at(&server),
            Arc::new(RateLimitTracker::new(WeightPool::Sapi)),
        );
        let clock = SharedStreamClock::default();
        // The cross account's balances, all of them.
        let read = || read_margin_restatement(&rest, &limiter, false, &[], &[], &clock, None);

        let failed = read().await;
        assert!(failed.is_err(), "{failed:?}");

        let restated = read().await.unwrap();
        let [event] = restated.as_slice() else {
            panic!("one balance: {restated:?}");
        };
        assert_eq!(event.exchange, ExchangeId::BinanceMargin);
        let AccountEventKind::BalanceSnapshot(balance) = &event.kind else {
            panic!("a balance snapshot: {event:?}");
        };
        assert_eq!(balance.0.asset, AssetNameExchange::new("BTC"));
        assert_eq!(
            balance.0.balance.margin,
            Some(crate::balance::MarginDetails::new(
                Decimal::new(5, 1),
                Decimal::ZERO
            ))
        );
    }

    #[tokio::test]
    async fn margin_fetch_ended_orders_asks_the_configured_account_and_omits_the_rest() {
        use crate::order::state::InactiveOrderState;
        for (is_isolated, flag) in [(false, "FALSE"), (true, "TRUE")] {
            let server = wiremock::MockServer::start().await;
            mount_margin_order(&server, "filled", margin_row_response("filled", "FILLED")).await;
            mount_margin_order(&server, "live", margin_row_response("live", "NEW")).await;
            mount_margin_order(&server, "unknown", margin_venue_error(-2013)).await;
            mount_margin_order(&server, "no-symbol", margin_venue_error(-1121)).await;
            let client = margin_client_at(&server, is_isolated);
            let keys = ["filled", "live", "unknown"].map(|cid| margin_key("BTCUSDT", cid));

            let ended = client
                .fetch_ended_orders(
                    &[keys.to_vec(), vec![margin_key("NOSUCH", "no-symbol")]].concat(),
                )
                .await
                .unwrap();

            let [order] = ended.as_slice() else {
                panic!("one order ended: {ended:?}");
            };
            assert_eq!(order.key, keys[0], "the strategy asked survives");
            assert!(matches!(order.state, InactiveOrderState::FullyFilled(_)));
            let requests = margin_requests(&server).await;
            assert_eq!(requests.len(), 4);
            for (path, isolated) in requests {
                assert_eq!(path, "/sapi/v1/margin/order");
                assert_eq!(
                    isolated.as_deref(),
                    Some(flag),
                    "is_isolated = {is_isolated}"
                );
            }
        }
    }

    #[tokio::test]
    async fn margin_fetch_ended_orders_fails_when_any_lookup_fails() {
        let server = wiremock::MockServer::start().await;
        mount_margin_order(
            &server,
            "cancelled",
            margin_row_response("cancelled", "CANCELED"),
        )
        .await;
        mount_margin_order(&server, "refused", margin_venue_error(-1100)).await;
        let client = margin_client_at(&server, false);

        let result = client
            .fetch_ended_orders(&[
                margin_key("BTCUSDT", "cancelled"),
                margin_key("BTCUSDT", "refused"),
            ])
            .await;

        assert!(
            matches!(
                result,
                Err(UnindexedClientError::Api(ApiError::RequestRejected(_)))
            ),
            "{result:?}"
        );
    }

    /// `live`, `gone` and `unknown`, all held as live on BTCUSDT.
    fn held_margin_orders() -> SharedKnownLiveOrders {
        let known = KnownLiveOrders::shared(ExchangeId::BinanceMargin);
        for (n, cid) in ["live", "gone", "unknown"].into_iter().enumerate() {
            known.lock().live(
                &margin_key("BTCUSDT", cid),
                Decimal::TWO,
                OrderKind::Limit,
                &Open::new(
                    VenueOrderId::Assigned(OrderId::new(n.to_string())),
                    Utc::now(),
                    Decimal::ZERO,
                ),
            );
        }
        known
    }

    /// A live conditional row, which cannot say whether it trails, is listed by both listings and
    /// the reconnect check, unresolved until the client knows its kind, then with that kind.
    #[tokio::test]
    async fn a_live_conditional_margin_order_is_listed_with_the_kind_the_client_knows() {
        use crate::order::ConditionalFamily;
        let server = wiremock::MockServer::start().await;
        let mut row = margin_order_row("conditional", "NEW");
        row["type"] = "STOP_LOSS_LIMIT".into();
        row["price"] = "94".into();
        row["stopPrice"] = "95".into();
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .and(wiremock::matchers::path("/sapi/v1/margin/openOrders"))
            .respond_with(
                wiremock::ResponseTemplate::new(200).set_body_json(serde_json::json!([row])),
            )
            .mount(&server)
            .await;
        let client = margin_client_at(&server, false);
        let btcusdt = InstrumentNameExchange::new("BTCUSDT");
        let cid = ClientOrderId::new("conditional");
        let kinds = |orders: Vec<Order<ExchangeId, InstrumentNameExchange, Open>>| {
            orders
                .into_iter()
                .map(|order| (order.key.cid, order.kind))
                .collect::<Vec<_>>()
        };

        let unresolved = OrderKind::ConditionalUnresolved {
            family: ConditionalFamily::StopLimit,
            trigger_price: Some(Decimal::from(95)),
        };
        assert_eq!(
            kinds(client.fetch_open_orders(&[]).await.unwrap()),
            [(cid.clone(), unresolved)]
        );
        assert_eq!(
            kinds(
                client
                    .fetch_open_orders(std::slice::from_ref(&btcusdt))
                    .await
                    .unwrap()
            ),
            [(cid.clone(), unresolved)]
        );
        let listed = listed_margin_open_cids(
            margin_rest_at(&server),
            Arc::new(RateLimitTracker::new(WeightPool::Sapi)),
            client.known_live.clone(),
            vec![btcusdt.clone()],
            false,
        )
        .await
        .unwrap();
        assert!(listed.contains(&cid), "{listed:?}");

        let trailing = OrderKind::TrailingStopFixedLimit {
            offset: Decimal::from(25),
            offset_type: TrailingOffsetType::BasisPoints,
            activation_price: Some(Decimal::from(95)),
        };
        client.recall_order_kinds([(cid.clone(), trailing)]);
        assert_eq!(
            kinds(client.fetch_open_orders(&[]).await.unwrap()),
            [(cid.clone(), trailing)]
        );
        assert_eq!(
            kinds(client.fetch_open_orders(&[btcusdt]).await.unwrap()),
            [(cid, trailing)]
        );
    }

    /// A margin trailing stop's row cannot say that it trailed (#541), and carries no positive
    /// `stopPrice` when it was placed without an activation price, yet how it ended is still
    /// reported, with the kind the client holds it with: by a reconnect's check and by
    /// `fetch_ended_orders`.
    #[tokio::test]
    async fn an_ended_margin_trailing_stop_is_reported_with_the_kind_it_was_held_with() {
        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .and(wiremock::matchers::path("/sapi/v1/margin/openOrders"))
            .respond_with(wiremock::ResponseTemplate::new(200).set_body_json(serde_json::json!([])))
            .mount(&server)
            .await;
        let mut row = margin_order_row("trailing", "CANCELED");
        row["type"] = "STOP_LOSS".into();
        row["price"] = "0.00000000".into();
        row["stopPrice"] = "0.00000000".into();
        mount_margin_order(
            &server,
            "trailing",
            wiremock::ResponseTemplate::new(200).set_body_json(row),
        )
        .await;
        let trailing_stop = OrderKind::TrailingStop {
            offset: Decimal::from(25),
            offset_type: TrailingOffsetType::BasisPoints,
            activation_price: None,
        };
        let hold = |known: &SharedKnownLiveOrders| {
            known.lock().live(
                &margin_key("BTCUSDT", "trailing"),
                Decimal::TWO,
                trailing_stop,
                &Open::new(
                    VenueOrderId::Assigned(OrderId::new("7")),
                    Utc::now(),
                    Decimal::ZERO,
                ),
            );
        };

        let known = KnownLiveOrders::shared(ExchangeId::BinanceMargin);
        hold(&known);
        let mut unchecked = UncheckedOrders::default();
        unchecked.open([InstrumentNameExchange::new("BTCUSDT")]);
        let (tx, mut rx) = mpsc::unbounded_channel();
        recover_margin_ended_orders(
            &margin_rest_at(&server),
            &Arc::new(RateLimitTracker::new(WeightPool::Sapi)),
            &known,
            &mut unchecked,
            &UnrecoveredFills::default(),
            &tx,
            false,
        )
        .await;
        let sent: Vec<_> = std::iter::from_fn(|| rx.try_recv().ok()).collect();
        let [event] = sent.as_slice() else {
            panic!("the trailing stop ended: {sent:?}");
        };
        let AccountEventKind::OrderSnapshot(snapshot) = &event.kind else {
            panic!("an order snapshot: {event:?}");
        };
        assert_eq!(snapshot.0.kind, trailing_stop);
        assert!(matches!(
            snapshot.0.state,
            OrderState::Inactive(crate::order::state::InactiveOrderState::Cancelled(_))
        ));
        assert!(!known.lock().contains(&ClientOrderId::new("trailing")));

        let client = margin_client_at(&server, false);
        hold(&client.known_live);
        let ended = client
            .fetch_ended_orders(&[margin_key("BTCUSDT", "trailing")])
            .await
            .expect("lookup");
        let [order] = ended.as_slice() else {
            panic!("the trailing stop ended: {ended:?}");
        };
        assert_eq!(order.kind, trailing_stop);
    }

    #[tokio::test]
    async fn a_margin_reconnect_reports_how_an_unlisted_order_ended_in_the_configured_account() {
        for (is_isolated, flag) in [(false, "FALSE"), (true, "TRUE")] {
            let server = wiremock::MockServer::start().await;
            wiremock::Mock::given(wiremock::matchers::method("GET"))
                .and(wiremock::matchers::path("/sapi/v1/margin/openOrders"))
                .and(wiremock::matchers::query_param("symbol", "BTCUSDT"))
                .respond_with(
                    wiremock::ResponseTemplate::new(200)
                        .set_body_json(serde_json::json!([margin_order_row("live", "NEW")])),
                )
                .mount(&server)
                .await;
            mount_margin_order(&server, "gone", margin_row_response("gone", "CANCELED")).await;
            mount_margin_order(&server, "unknown", margin_venue_error(-2013)).await;
            let known = held_margin_orders();
            let mut unchecked = UncheckedOrders::default();
            unchecked.open([InstrumentNameExchange::new("BTCUSDT")]);
            let (tx, mut rx) = mpsc::unbounded_channel();

            recover_margin_ended_orders(
                &margin_rest_at(&server),
                &Arc::new(RateLimitTracker::new(WeightPool::Sapi)),
                &known,
                &mut unchecked,
                &UnrecoveredFills::default(),
                &tx,
                is_isolated,
            )
            .await;

            let sent: Vec<_> = std::iter::from_fn(|| rx.try_recv().ok()).collect();
            let [event] = sent.as_slice() else {
                panic!("one order ended: {sent:?}");
            };
            assert_eq!(event.exchange, ExchangeId::BinanceMargin);
            let AccountEventKind::OrderSnapshot(snapshot) = &event.kind else {
                panic!("an order snapshot: {event:?}");
            };
            assert_eq!(snapshot.0.key.cid, ClientOrderId::new("gone"));
            assert_eq!(snapshot.0.key.strategy, StrategyId::unknown());
            assert!(matches!(
                snapshot.0.state,
                OrderState::Inactive(crate::order::state::InactiveOrderState::Cancelled(_))
            ));
            let mut requests = margin_requests(&server).await;
            requests.sort();
            let expected = |path: &str| (path.to_owned(), Some(flag.to_owned()));
            assert_eq!(
                requests,
                [
                    expected("/sapi/v1/margin/openOrders"),
                    expected("/sapi/v1/margin/order"),
                    expected("/sapi/v1/margin/order"),
                ],
                "one listing, and a lookup of each unlisted order only"
            );
            let known = known.lock();
            assert!(known.contains(&ClientOrderId::new("live")));
            assert!(!known.contains(&ClientOrderId::new("gone")));
            assert!(
                !known.contains(&ClientOrderId::new("unknown")),
                "asking again cannot help"
            );
            assert!(unchecked.is_empty());
        }
    }

    fn margin_open_request<'a>(
        instrument: &'a InstrumentNameExchange,
        cid: &str,
    ) -> OrderRequestOpen<ExchangeId, &'a InstrumentNameExchange> {
        OrderRequestOpen {
            key: OrderKey::new(
                ExchangeId::BinanceMargin,
                instrument,
                StrategyId::new("strategy"),
                ClientOrderId::new(cid),
            ),
            state: crate::order::request::RequestOpen::new(
                Side::Buy,
                Some(Decimal::from(100)),
                Decimal::TWO,
                OrderKind::Limit,
                TimeInForce::GoodUntilCancelled { post_only: false },
            ),
        }
    }

    /// A margin `TrailingStop` reaches Binance as a `STOP_LOSS` with `trailingDelta`, and an
    /// offset Binance cannot take is refused without a request.
    #[tokio::test]
    async fn a_margin_trailing_stop_is_sent_with_trailing_delta() {
        let server = wiremock::MockServer::start().await;
        let sent_as_trailing_stop = |request: &wiremock::Request| {
            // The SDK may send the params in the query string or the form body; read both.
            let mut body_as_query = request.url.clone();
            body_as_query.set_query(Some(&String::from_utf8_lossy(&request.body)));
            let params: Vec<(String, String)> = request
                .url
                .query_pairs()
                .chain(body_as_query.query_pairs())
                .map(|(k, v)| (k.into_owned(), v.into_owned()))
                .collect();
            let has = |key: &str, value: &str| params.iter().any(|(k, v)| k == key && v == value);
            has("type", "STOP_LOSS")
                && has("trailingDelta", "25")
                && has("sideEffectType", "NO_SIDE_EFFECT")
                && !params.iter().any(|(k, _)| k == "stopPrice" || k == "price")
        };
        wiremock::Mock::given(wiremock::matchers::method("POST"))
            .and(wiremock::matchers::path("/sapi/v1/margin/order"))
            .and(sent_as_trailing_stop)
            .respond_with(
                wiremock::ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "symbol": "BTCUSDT", "orderId": 7, "clientOrderId": "trailing",
                    "transactTime": 1_700_000_000_000_i64, "price": "0", "origQty": "2",
                    "executedQty": "0", "cummulativeQuoteQty": "0", "status": "NEW",
                    "timeInForce": "GTC", "type": "STOP_LOSS", "side": "SELL",
                })),
            )
            .expect(1)
            .mount(&server)
            .await;
        let client = margin_client_at(&server, false);
        let btcusdt = InstrumentNameExchange::new("BTCUSDT");
        let trailing_request = |cid, offset| {
            let mut request = margin_open_request(&btcusdt, cid);
            request.state.side = Side::Sell;
            request.state.price = None;
            request.state.kind = OrderKind::TrailingStop {
                offset,
                offset_type: TrailingOffsetType::BasisPoints,
                activation_price: None,
            };
            request
        };

        let placed = client
            .open_order(trailing_request("trailing", Decimal::from(25)))
            .await;
        assert!(
            matches!(placed.state, OrderState::Active(ActiveOrderState::Open(_))),
            "{placed:?}"
        );

        let refused = client
            .open_order(trailing_request("fractional", Decimal::new(25, 1)))
            .await;
        assert!(
            matches!(
                refused.state,
                OrderState::Inactive(crate::order::state::InactiveOrderState::OpenFailed(
                    OrderError::InvalidPrecision(_)
                ))
            ),
            "{refused:?}"
        );
    }

    /// A request's [`MarginEffect`] reaches Binance as its `sideEffectType`, and as
    /// `autoRepayAtCancel` only on a `Borrow`, from the config; a request that names none takes
    /// the config's default.
    #[tokio::test]
    async fn a_request_margin_effect_picks_the_side_effect_type_sent() {
        let server = wiremock::MockServer::start().await;
        let sends = |wire: &'static str, auto_repay_at_cancel: Option<&'static str>| {
            move |request: &wiremock::Request| {
                let mut body_as_query = request.url.clone();
                body_as_query.set_query(Some(&String::from_utf8_lossy(&request.body)));
                let params: Vec<(String, String)> = request
                    .url
                    .query_pairs()
                    .chain(body_as_query.query_pairs())
                    .map(|(k, v)| (k.into_owned(), v.into_owned()))
                    .collect();
                let value = |key: &str| {
                    params
                        .iter()
                        .find(|(k, _)| k == key)
                        .map(|(_, v)| v.as_str())
                };
                value("sideEffectType") == Some(wire)
                    && value("autoRepayAtCancel") == auto_repay_at_cancel
            }
        };
        for (matcher, cid) in [
            (sends("MARGIN_BUY", Some("true")), "borrow"),
            (sends("AUTO_REPAY", None), "repay"),
            (sends("NO_SIDE_EFFECT", None), "default"),
        ] {
            wiremock::Mock::given(wiremock::matchers::method("POST"))
                .and(wiremock::matchers::path("/sapi/v1/margin/order"))
                .and(matcher)
                .respond_with(wiremock::ResponseTemplate::new(200).set_body_json(
                    serde_json::json!({
                        "symbol": "BTCUSDT", "orderId": 7, "clientOrderId": cid,
                        "transactTime": 1_700_000_000_000_i64, "price": "50000", "origQty": "2",
                        "executedQty": "0", "cummulativeQuoteQty": "0", "status": "NEW",
                        "timeInForce": "GTC", "type": "LIMIT", "side": "BUY",
                    }),
                ))
                .expect(1)
                .mount(&server)
                .await;
        }
        let client = margin_client_at(&server, false);
        let btcusdt = InstrumentNameExchange::new("BTCUSDT");

        for (cid, effect) in [
            ("borrow", Some(MarginEffect::Borrow)),
            ("repay", Some(MarginEffect::Repay)),
            ("default", None),
        ] {
            let mut request = margin_open_request(&btcusdt, cid);
            request.state.margin_effect = effect;
            let placed = client.open_order(request).await;
            assert!(
                matches!(placed.state, OrderState::Active(ActiveOrderState::Open(_))),
                "{cid}: {placed:?}"
            );
        }
    }

    use crate::error::{Bound, BoundsViolation, OrderField, PrecisionLimit, PrecisionViolation};
    use rust_decimal_macros::dec;

    /// BTCUSDT's spot `exchangeInfo` with a 0.01 tick, a 0.001 lot and a 5 minimum notional.
    fn btcusdt_exchange_info() -> wiremock::ResponseTemplate {
        wiremock::ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "timezone": "UTC", "serverTime": 1_700_000_000_000_i64,
            "symbols": [{
                "symbol": "BTCUSDT", "status": "TRADING",
                "filters": [
                    {"filterType": "PRICE_FILTER", "minPrice": "0.01", "maxPrice": "0", "tickSize": "0.01"},
                    {"filterType": "LOT_SIZE", "minQty": "0.001", "maxQty": "100", "stepSize": "0.001"},
                    {"filterType": "NOTIONAL", "minNotional": "5", "applyMinToMarket": true,
                     "maxNotional": "9000000", "applyMaxToMarket": false, "avgPriceMins": 5},
                ],
            }],
        }))
    }

    async fn mount_order_accepted(server: &wiremock::MockServer, expected: u64) {
        wiremock::Mock::given(wiremock::matchers::method("POST"))
            .and(wiremock::matchers::path("/sapi/v1/margin/order"))
            .respond_with(
                wiremock::ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "symbol": "BTCUSDT", "orderId": 7, "clientOrderId": "c",
                    "transactTime": 1_700_000_000_000_i64, "price": "50000", "origQty": "2",
                    "executedQty": "0", "cummulativeQuoteQty": "0", "status": "NEW",
                    "timeInForce": "GTC", "type": "LIMIT", "side": "BUY",
                })),
            )
            .expect(expected)
            .mount(server)
            .await;
    }

    /// An order that breaks the symbol's rules is refused without a request to the order
    /// endpoint, and the rules are read once for every order after.
    #[tokio::test]
    async fn an_order_breaking_the_symbols_rules_is_refused_unsent() {
        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .and(wiremock::matchers::path("/api/v3/exchangeInfo"))
            .and(wiremock::matchers::query_param("symbols", r#"["BTCUSDT"]"#))
            .respond_with(btcusdt_exchange_info())
            .expect(1)
            .mount(&server)
            .await;
        mount_order_accepted(&server, 1).await;
        let client = margin_client_at(&server, false);
        let btcusdt = InstrumentNameExchange::new("BTCUSDT");
        let order = |cid: &str, price: Decimal, quantity: Decimal| {
            let mut request = margin_open_request(&btcusdt, cid);
            request.state.price = Some(price);
            request.state.quantity = quantity;
            request
        };

        let off_tick = client.open_order(order("a", dec!(100.001), dec!(1))).await;
        assert_eq!(
            off_tick.state,
            OrderState::inactive(OrderError::InvalidPrecision(PrecisionViolation::new(
                OrderField::Price,
                dec!(100.001),
                PrecisionLimit::NotOnGrid { step: dec!(0.01) }
            )))
        );
        let dust = client.open_order(order("b", dec!(100), dec!(0.001))).await;
        assert_eq!(
            dust.state,
            OrderState::inactive(OrderError::OutOfBounds(BoundsViolation::new(
                OrderField::Notional,
                Bound::Min,
                dec!(5),
                dec!(0.100)
            )))
        );
        let placed = client.open_order(order("c", dec!(100), dec!(1))).await;
        assert!(
            matches!(placed.state, OrderState::Active(ActiveOrderState::Open(_))),
            "{placed:?}"
        );
    }

    /// With the rules off, nothing is read and every order is sent.
    #[tokio::test]
    async fn with_the_rules_off_every_order_is_sent_and_no_rules_are_read() {
        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::path("/api/v3/exchangeInfo"))
            .respond_with(btcusdt_exchange_info())
            .expect(0)
            .mount(&server)
            .await;
        mount_order_accepted(&server, 1).await;
        let mut client = margin_client_at(&server, false);
        client.rules = rules_at(&server, PreSendRules::Off);
        let btcusdt = InstrumentNameExchange::new("BTCUSDT");
        let mut request = margin_open_request(&btcusdt, "x");
        request.state.price = Some(dec!(100.001));
        let placed = client.open_order(request).await;
        assert!(
            matches!(placed.state, OrderState::Active(ActiveOrderState::Open(_))),
            "{placed:?}"
        );
    }

    /// An order for a symbol whose rules cannot be loaded is sent, for the venue to decide.
    #[tokio::test]
    async fn an_order_whose_rules_cannot_be_loaded_is_sent_unchecked() {
        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::path("/api/v3/exchangeInfo"))
            .respond_with(wiremock::ResponseTemplate::new(500))
            .mount(&server)
            .await;
        mount_order_accepted(&server, 1).await;
        let client = margin_client_at(&server, false);
        let btcusdt = InstrumentNameExchange::new("BTCUSDT");
        let mut request = margin_open_request(&btcusdt, "x");
        request.state.price = Some(dec!(100.001));
        let placed = client.open_order(request).await;
        assert!(
            matches!(placed.state, OrderState::Active(ActiveOrderState::Open(_))),
            "{placed:?}"
        );
    }

    /// `load_rules` returns the rules it read and holds them, so the first order needs no load.
    #[tokio::test]
    async fn load_rules_returns_the_rules_and_the_first_order_reuses_them() {
        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::path("/api/v3/exchangeInfo"))
            .respond_with(btcusdt_exchange_info())
            .expect(1)
            .mount(&server)
            .await;
        mount_order_accepted(&server, 0).await;
        let client = margin_client_at(&server, false);
        let btcusdt = InstrumentNameExchange::new("BTCUSDT");

        let loaded = client.load_rules(std::slice::from_ref(&btcusdt)).await;
        let rules = loaded.ok().and_then(|mut loaded| loaded.remove(&btcusdt));
        assert_eq!(
            rules.map(|rules| (rules.price.step, rules.quantity.step, rules.notional.min)),
            Some((Some(dec!(0.01)), Some(dec!(0.001)), Some(dec!(5))))
        );

        let mut request = margin_open_request(&btcusdt, "x");
        request.state.price = Some(dec!(100.001));
        let refused = client.open_order(request).await;
        assert!(
            matches!(refused.state, OrderState::Inactive(_)),
            "{refused:?}"
        );
    }

    /// Loading no symbols asks Binance for nothing.
    #[tokio::test]
    async fn load_rules_of_no_symbols_sends_no_request() {
        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::path("/api/v3/exchangeInfo"))
            .respond_with(btcusdt_exchange_info())
            .expect(0)
            .mount(&server)
            .await;
        let client = margin_client_at(&server, false);
        assert_eq!(client.load_rules(&[]).await, Ok(HashMap::new()));
    }

    /// A cross account refuses a trailing `STOP_LOSS` with `-3111`. The order fails as
    /// [`OrderError::UnsupportedOrderType`] with the venue's message, after one request, so a
    /// caller can tell it from other refusals and place a `TrailingStopFixedLimit` instead.
    #[tokio::test]
    async fn a_cross_refusal_of_a_trailing_stop_is_an_unsupported_order_type() {
        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("POST"))
            .and(wiremock::matchers::path("/sapi/v1/margin/order"))
            .respond_with(
                wiremock::ResponseTemplate::new(400).set_body_json(serde_json::json!({
                    "code": -3111,
                    "msg": "Trailing stop order only supports limit type.",
                })),
            )
            .expect(1)
            .mount(&server)
            .await;
        let client = margin_client_at(&server, false);
        let btcusdt = InstrumentNameExchange::new("BTCUSDT");
        let mut request = margin_open_request(&btcusdt, "trailing");
        request.state.side = Side::Sell;
        request.state.price = None;
        request.state.kind = OrderKind::TrailingStop {
            offset: Decimal::from(25),
            offset_type: TrailingOffsetType::BasisPoints,
            activation_price: None,
        };

        let refused = client.open_order(request).await;

        let OrderState::Inactive(crate::order::state::InactiveOrderState::OpenFailed(
            OrderError::UnsupportedOrderType(msg),
        )) = &refused.state
        else {
            panic!("expected UnsupportedOrderType: {refused:?}");
        };
        assert!(
            msg.contains("-3111") && msg.contains("only supports limit type"),
            "{msg}"
        );
    }

    #[tokio::test]
    async fn placing_listing_and_cancelling_margin_orders_feed_the_known_live_set() {
        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("POST"))
            .and(wiremock::matchers::path("/sapi/v1/margin/order"))
            .respond_with(
                wiremock::ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "symbol": "BTCUSDT", "orderId": 7, "clientOrderId": "placed",
                    "transactTime": 1_700_000_000_000_i64, "price": "100", "origQty": "2",
                    "executedQty": "0", "cummulativeQuoteQty": "0", "status": "NEW",
                    "timeInForce": "GTC", "type": "LIMIT", "side": "BUY",
                })),
            )
            .mount(&server)
            .await;
        wiremock::Mock::given(wiremock::matchers::method("DELETE"))
            .and(wiremock::matchers::path("/sapi/v1/margin/order"))
            .respond_with(
                wiremock::ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "symbol": "BTCUSDT", "orderId": "7", "origClientOrderId": "placed",
                    "clientOrderId": "cancel", "price": "100", "origQty": "2", "executedQty": "0",
                    "cummulativeQuoteQty": "0", "status": "CANCELED", "timeInForce": "GTC",
                    "type": "LIMIT", "side": "BUY",
                })),
            )
            .mount(&server)
            .await;
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .and(wiremock::matchers::path("/sapi/v1/margin/openOrders"))
            .respond_with(
                wiremock::ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!([margin_order_row("listed", "NEW")])),
            )
            .mount(&server)
            .await;
        let client = margin_client_at(&server, false);
        let btcusdt = InstrumentNameExchange::new("BTCUSDT");

        let placed = client
            .open_order(margin_open_request(&btcusdt, "placed"))
            .await;
        assert!(
            matches!(placed.state, OrderState::Active(ActiveOrderState::Open(_))),
            "{placed:?}"
        );
        assert!(
            client
                .known_live
                .lock()
                .contains(&ClientOrderId::new("placed"))
        );

        client.fetch_open_orders(&[]).await.unwrap();
        assert!(
            client
                .known_live
                .lock()
                .contains(&ClientOrderId::new("listed"))
        );

        let cancelled = client
            .cancel_order(OrderRequestCancel {
                key: OrderKey::new(
                    ExchangeId::BinanceMargin,
                    &btcusdt,
                    StrategyId::new("strategy"),
                    ClientOrderId::new("placed"),
                ),
                state: crate::order::request::RequestCancel {
                    id: Some(VenueOrderId::Assigned(OrderId::new("7"))),
                },
            })
            .await;
        assert!(cancelled.state.is_ok(), "{cancelled:?}");
        assert!(
            !client
                .known_live
                .lock()
                .contains(&ClientOrderId::new("placed"))
        );
    }

    /// The kind a margin order is placed with is known while its request is out, for a stream
    /// report that arrives before the answer, and dropped when the order is refused.
    #[tokio::test]
    async fn a_margin_order_kind_is_known_while_it_is_placed() {
        struct KindWhenAsked {
            known: SharedKnownLiveOrders,
            seen: Arc<parking_lot::Mutex<Option<OrderKind>>>,
        }
        impl wiremock::Respond for KindWhenAsked {
            fn respond(&self, _: &wiremock::Request) -> wiremock::ResponseTemplate {
                *self.seen.lock() = self.known.lock().kind_of(&ClientOrderId::new("racing"));
                wiremock::ResponseTemplate::new(400).set_body_json(
                    serde_json::json!({"code": -2010, "msg": "Account has insufficient balance."}),
                )
            }
        }

        let server = wiremock::MockServer::start().await;
        let client = margin_client_at(&server, true);
        let seen = Arc::new(parking_lot::Mutex::new(None));
        wiremock::Mock::given(wiremock::matchers::method("POST"))
            .and(wiremock::matchers::path("/sapi/v1/margin/order"))
            .respond_with(KindWhenAsked {
                known: client.known_live.clone(),
                seen: Arc::clone(&seen),
            })
            .mount(&server)
            .await;
        let btcusdt = InstrumentNameExchange::new("BTCUSDT");

        let refused = client
            .open_order(margin_open_request(&btcusdt, "racing"))
            .await;
        assert!(
            matches!(refused.state, OrderState::Inactive(_)) && !refused.state.is_outcome_unknown(),
            "{refused:?}"
        );
        assert_eq!(*seen.lock(), Some(OrderKind::Limit));
        assert_eq!(
            client
                .known_live
                .lock()
                .kind_of(&ClientOrderId::new("racing")),
            None
        );
    }

    /// A margin placement dropped while it waits on the rate limiter sent nothing, so keeps no
    /// kind. One dropped once its request is out may have placed the order, so keeps its kind
    /// until the order is seen to end.
    #[tokio::test]
    async fn a_dropped_margin_placement_keeps_its_kind_only_once_its_request_is_out() {
        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("POST"))
            .and(wiremock::matchers::path("/sapi/v1/margin/order"))
            .respond_with(
                wiremock::ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({}))
                    .set_delay(Duration::from_secs(3_600)),
            )
            .mount(&server)
            .await;
        let client = margin_client_at(&server, true);
        let btcusdt = InstrumentNameExchange::new("BTCUSDT");
        let kind_of = |cid: &str| client.known_live.lock().kind_of(&ClientOrderId::new(cid));
        let orders_sent = || async {
            let requests = server.received_requests().await.unwrap();
            requests
                .iter()
                .filter(|request| request.method == wiremock::http::Method::POST)
                .count()
        };

        client
            .rate_limiter
            .on_rate_limited(Some(Duration::from_secs(5)));
        let waiting = tokio::time::timeout(
            Duration::from_millis(100),
            client.open_order(margin_open_request(&btcusdt, "waiting")),
        )
        .await;
        assert!(waiting.is_err(), "still waiting on the rate limiter");
        assert_eq!(orders_sent().await, 0);
        assert_eq!(kind_of("waiting"), None);

        let fresh = margin_client_at(&server, true);
        // The placement is dropped once its request has reached the server, however long that
        // takes, and never answered.
        tokio::time::timeout(Duration::from_secs(10), async {
            tokio::select! {
                placed = fresh.open_order(margin_open_request(&btcusdt, "sent")) => {
                    panic!("the answer is held back: {placed:?}")
                }
                () = async {
                    while orders_sent().await == 0 {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                } => {}
            }
        })
        .await
        .expect("the request reaches the server");
        assert_eq!(orders_sent().await, 1);
        assert_eq!(
            fresh.known_live.lock().kind_of(&ClientOrderId::new("sent")),
            Some(OrderKind::Limit)
        );
    }

    /// A cancel response that does not say what filled reports the fill unknown, not zero.
    #[tokio::test]
    async fn a_cancel_response_without_executed_qty_reports_the_fill_unknown() {
        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("DELETE"))
            .and(wiremock::matchers::path("/sapi/v1/margin/order"))
            .respond_with(
                wiremock::ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "symbol": "BTCUSDT", "orderId": "7", "origClientOrderId": "placed",
                    "clientOrderId": "cancel", "price": "100", "origQty": "2",
                    "status": "CANCELED", "timeInForce": "GTC", "type": "LIMIT", "side": "BUY",
                })),
            )
            .mount(&server)
            .await;
        let client = margin_client_at(&server, false);

        let cancelled = client
            .cancel_order(OrderRequestCancel {
                key: OrderKey::new(
                    ExchangeId::BinanceMargin,
                    &InstrumentNameExchange::new("BTCUSDT"),
                    StrategyId::new("strategy"),
                    ClientOrderId::new("placed"),
                ),
                state: crate::order::request::RequestCancel {
                    id: Some(VenueOrderId::Assigned(OrderId::new("7"))),
                },
            })
            .await;
        let Ok(cancelled) = cancelled.state else {
            panic!("expected Ok, got {cancelled:?}");
        };
        assert_eq!(cancelled.filled_quantity, None);
    }

    /// Binance's `FULL` answer to an order that borrowed, with the borrowed amount as the decimal
    /// string Binance sends.
    fn borrowing_answer() -> serde_json::Value {
        serde_json::json!({
            "symbol": "BTCUSDT", "orderId": 7, "clientOrderId": "borrow",
            "transactTime": 1_700_000_000_000_i64, "price": "0", "origQty": "0.0001",
            "executedQty": "0.0001", "cummulativeQuoteQty": "8", "status": "FILLED",
            "timeInForce": "GTC", "type": "MARKET", "side": "SELL", "isIsolated": false,
            "selfTradePreventionMode": "EXPIRE_MAKER",
            "marginBuyBorrowAmount": "0.00004321", "marginBuyBorrowAsset": "BTC",
            "fills": [{
                "price": "80000", "qty": "0.0001", "commission": "0.008",
                "commissionAsset": "USDT", "tradeId": 1,
            }],
        })
    }

    /// A market sell of 0.0001 BTCUSDT that borrows what it lacks.
    fn borrowing_request(
        instrument: &InstrumentNameExchange,
    ) -> OrderRequestOpen<ExchangeId, &InstrumentNameExchange> {
        let mut request = margin_open_request(instrument, "borrow");
        request.state.side = Side::Sell;
        request.state.kind = OrderKind::Market;
        request.state.price = None;
        request.state.quantity = dec!(0.0001);
        request.state.margin_effect = Some(MarginEffect::Borrow);
        request
    }

    /// `borrowing_answer` without the fields named.
    fn borrowing_answer_without(fields: &[&str]) -> serde_json::Value {
        let mut answer = borrowing_answer();
        let object = answer.as_object_mut().unwrap();
        for field in fields {
            object.remove(*field);
        }
        answer
    }

    /// A placement reports what the order borrowed as Binance's answer gives it, with its asset:
    /// the amount as a decimal string, or as a number; and no loan when the answer gives none.
    #[tokio::test]
    async fn a_margin_placement_reports_what_the_order_borrowed() {
        let mut as_a_number = borrowing_answer();
        as_a_number["marginBuyBorrowAmount"] = serde_json::json!(5);
        let btc = AssetNameExchange::new("BTC");
        for (answer, expected) in [
            (
                borrowing_answer(),
                Some(MarginBorrow::new(btc.clone(), dec!(0.00004321))),
            ),
            (as_a_number, Some(MarginBorrow::new(btc, dec!(5)))),
            (
                borrowing_answer_without(&["marginBuyBorrowAmount", "marginBuyBorrowAsset"]),
                None,
            ),
        ] {
            let server = wiremock::MockServer::start().await;
            wiremock::Mock::given(wiremock::matchers::method("POST"))
                .and(wiremock::matchers::path("/sapi/v1/margin/order"))
                .respond_with(wiremock::ResponseTemplate::new(200).set_body_json(&answer))
                .expect(1)
                .mount(&server)
                .await;
            let client = margin_client_at(&server, false);
            let btcusdt = InstrumentNameExchange::new("BTCUSDT");

            let placement = client.open_margin_order(borrowing_request(&btcusdt)).await;

            assert!(
                matches!(
                    placement.order.state,
                    OrderState::Inactive(crate::order::state::InactiveOrderState::FullyFilled(_))
                ),
                "{answer}: {placement:?}"
            );
            assert_eq!(placement.borrowed, expected, "{answer}");
        }
    }

    /// An order that borrowed is answered with the borrowed amount as a decimal string, which
    /// does not stop the answer being read: the order is reported as it is.
    #[tokio::test]
    async fn an_order_answered_with_a_fractional_borrowed_amount_is_read() {
        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("POST"))
            .and(wiremock::matchers::path("/sapi/v1/margin/order"))
            .respond_with(wiremock::ResponseTemplate::new(200).set_body_json(borrowing_answer()))
            .expect(1)
            .mount(&server)
            .await;
        let client = margin_client_at(&server, false);
        let btcusdt = InstrumentNameExchange::new("BTCUSDT");

        let placed = client.open_order(borrowing_request(&btcusdt)).await;

        let OrderState::Inactive(crate::order::state::InactiveOrderState::FullyFilled(filled)) =
            placed.state
        else {
            panic!("expected a filled order, got {placed:?}");
        };
        assert_eq!(filled.id, OrderId::new("7"));
        assert_eq!(filled.filled_quantity, dec!(0.0001));
    }

    /// An open answered with a success that cannot be read was acted on, so its outcome is
    /// unknown: `Connectivity`, not a refusal, and the order is not held as ended.
    #[tokio::test]
    async fn an_open_answered_unreadably_is_an_unknown_outcome() {
        for answer in [
            serde_json::json!({ "symbol": "BTCUSDT", "clientOrderId": "x" }),
            serde_json::json!({ "symbol": "BTCUSDT", "orderId": "not a number" }),
            borrowing_answer_without(&["marginBuyBorrowAsset"]),
            borrowing_answer_without(&["marginBuyBorrowAmount"]),
        ] {
            let server = wiremock::MockServer::start().await;
            wiremock::Mock::given(wiremock::matchers::method("POST"))
                .and(wiremock::matchers::path("/sapi/v1/margin/order"))
                .respond_with(wiremock::ResponseTemplate::new(200).set_body_json(&answer))
                .expect(1)
                .mount(&server)
                .await;
            let client = margin_client_at(&server, false);
            let btcusdt = InstrumentNameExchange::new("BTCUSDT");

            let placed = client
                .open_margin_order(margin_open_request(&btcusdt, "x"))
                .await;

            assert!(
                matches!(
                    placed.order.state,
                    OrderState::Inactive(crate::order::state::InactiveOrderState::OpenFailed(
                        OrderError::Connectivity(_)
                    ))
                ),
                "{answer}: {placed:?}"
            );
            assert_eq!(placed.borrowed, None);
        }
    }

    /// A cancel answered with a success that cannot be read was acted on, so whether the order
    /// still works is unknown: `Connectivity`, not a refusal, and the order stays held live.
    #[tokio::test]
    async fn a_cancel_answered_unreadably_is_an_unknown_outcome() {
        for answer in [
            serde_json::json!({ "symbol": "BTCUSDT", "status": "CANCELED" }),
            serde_json::json!({ "symbol": "BTCUSDT", "orderId": ["7"] }),
            serde_json::json!({ "symbol": "BTCUSDT", "orderId": "abc", "transactTime": 1 }),
        ] {
            let server = wiremock::MockServer::start().await;
            mount_order_accepted(&server, 1).await;
            wiremock::Mock::given(wiremock::matchers::method("DELETE"))
                .and(wiremock::matchers::path("/sapi/v1/margin/order"))
                .respond_with(wiremock::ResponseTemplate::new(200).set_body_json(&answer))
                .expect(1)
                .mount(&server)
                .await;
            let client = margin_client_at(&server, false);
            let btcusdt = InstrumentNameExchange::new("BTCUSDT");
            client.open_order(margin_open_request(&btcusdt, "c")).await;

            let cancelled = client
                .cancel_order(OrderRequestCancel {
                    key: OrderKey::new(
                        ExchangeId::BinanceMargin,
                        &btcusdt,
                        StrategyId::new("strategy"),
                        ClientOrderId::new("c"),
                    ),
                    state: crate::order::request::RequestCancel {
                        id: Some(VenueOrderId::Assigned(OrderId::new("7"))),
                    },
                })
                .await;

            assert!(
                matches!(cancelled.state, Err(OrderError::Connectivity(_))),
                "{answer}: {cancelled:?}"
            );
            assert!(client.known_live.lock().contains(&ClientOrderId::new("c")));
        }
    }

    /// Each request `server` received, as its sorted query and its body, less the timestamp and
    /// the signature over it, so two signings of one request compare equal.
    async fn signed_requests_sent(
        server: &wiremock::MockServer,
    ) -> Vec<(Vec<(String, String)>, Vec<u8>)> {
        server
            .received_requests()
            .await
            .unwrap()
            .into_iter()
            .map(|request| {
                let mut query: Vec<(String, String)> = request
                    .url
                    .query_pairs()
                    .filter(|(k, _)| k != "timestamp" && k != "signature")
                    .map(|(k, v)| (k.into_owned(), v.into_owned()))
                    .collect();
                query.sort();
                (query, request.body)
            })
            .collect()
    }

    /// A cancel is stamped with the answer's `transactTime`, and reads the order id whether
    /// Binance sends it as a string (as it does in this answer) or as a number. An answer without a
    /// time is read at the local time.
    #[tokio::test]
    async fn a_cancel_is_stamped_with_the_answers_transact_time() {
        let cancel = |answer: serde_json::Value| async move {
            let server = wiremock::MockServer::start().await;
            wiremock::Mock::given(wiremock::matchers::method("DELETE"))
                .and(wiremock::matchers::path("/sapi/v1/margin/order"))
                .respond_with(wiremock::ResponseTemplate::new(200).set_body_json(&answer))
                .expect(1)
                .mount(&server)
                .await;
            let client = margin_client_at(&server, false);
            let btcusdt = InstrumentNameExchange::new("BTCUSDT");
            let cancelled = client
                .cancel_order(OrderRequestCancel {
                    key: OrderKey::new(
                        ExchangeId::BinanceMargin,
                        &btcusdt,
                        StrategyId::new("strategy"),
                        ClientOrderId::new("c"),
                    ),
                    state: crate::order::request::RequestCancel {
                        id: Some(VenueOrderId::Assigned(OrderId::new("7"))),
                    },
                })
                .await;
            match cancelled.state {
                Ok(cancelled) => cancelled,
                Err(error) => panic!("{answer}: {error:?}"),
            }
        };
        let answer = |order_id: serde_json::Value, time: Option<i64>| {
            let mut answer = serde_json::json!({
                "orderId": order_id, "symbol": "BTCUSDT", "origClientOrderId": "c",
                "clientOrderId": "cancel", "price": "100", "origQty": "2", "executedQty": "0.5",
                "cummulativeQuoteQty": "50", "status": "CANCELED", "timeInForce": "GTC",
                "type": "LIMIT", "side": "BUY", "isIsolated": false,
                "selfTradePreventionMode": "EXPIRE_MAKER",
            });
            if let Some(time) = time {
                answer["transactTime"] = serde_json::json!(time);
            }
            answer
        };
        let sent_at = Utc.timestamp_millis_opt(1_700_000_000_123).unwrap();

        for order_id in [serde_json::json!("7"), serde_json::json!(7)] {
            let cancelled = cancel(answer(order_id, Some(1_700_000_000_123))).await;
            assert_eq!(cancelled.id, OrderId::new("7"));
            assert_eq!(cancelled.time_exchange, sent_at);
            assert_eq!(cancelled.filled_quantity, Some(dec!(0.5)));
        }

        let before = Utc::now();
        let cancelled = cancel(answer(serde_json::json!("7"), None)).await;
        assert!(
            cancelled.time_exchange >= before && cancelled.time_exchange <= Utc::now(),
            "{cancelled:?}"
        );
    }

    /// The cancel query is the one the SDK's own call sends, field for field, with every optional
    /// field set.
    #[tokio::test]
    async fn the_cancel_order_query_is_the_one_the_sdk_sends() {
        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("DELETE"))
            .and(wiremock::matchers::path("/sapi/v1/margin/order"))
            .respond_with(wiremock::ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
            .expect(2)
            .mount(&server)
            .await;
        let params = MarginAccountCancelOrderParams::builder("BTCUSDT".to_string())
            .is_isolated(MarginAccountCancelOrderIsIsolatedEnum::from_flag(true))
            .order_id(7)
            .orig_client_order_id("c".to_string())
            .new_client_order_id("cancel".to_string())
            .recv_window(5000)
            .build()
            .unwrap();

        let _ = margin_rest_at(&server)
            .margin_account_cancel_order(params.clone())
            .await
            .unwrap();
        let _ = binance_sdk::common::utils::send_request::<serde_json::Value>(
            &margin_rest_config_at(&server),
            ORDER_PATH,
            reqwest::Method::DELETE,
            cancel_order_query(params),
            BTreeMap::new(),
            None,
            true,
        )
        .await
        .unwrap();

        let sent = signed_requests_sent(&server).await;
        assert_eq!(sent.len(), 2);
        assert_eq!(sent[0].0.len(), 6, "every field is sent: {:?}", sent[0].0);
        assert_eq!(sent[0], sent[1]);
    }

    /// The new-order query is the one the SDK's own call sends, field for field, with every
    /// optional field set.
    #[tokio::test]
    async fn the_new_order_query_is_the_one_the_sdk_sends() {
        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("POST"))
            .and(wiremock::matchers::path("/sapi/v1/margin/order"))
            .respond_with(wiremock::ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
            .expect(2)
            .mount(&server)
            .await;
        let params = MarginAccountNewOrderParams::builder(
            "BTCUSDT".to_string(),
            MarginAccountNewOrderSideEnum::Sell,
            MarginAccountNewOrderTypeEnum::StopLossLimit,
        )
        .is_isolated(MarginAccountNewOrderIsIsolatedEnum::True)
        .quantity(dec!(0.5))
        .quote_order_qty(dec!(100.25))
        .price(dec!(50000.1))
        .stop_price(dec!(49000))
        .new_client_order_id("cid".to_string())
        .iceberg_qty(dec!(0.1))
        .new_order_resp_type(MarginAccountNewOrderNewOrderRespTypeEnum::Full)
        .side_effect_type(MarginAccountNewOrderSideEffectTypeEnum::MarginBuy)
        .time_in_force(MarginAccountNewOrderTimeInForceEnum::Gtc)
        .self_trade_prevention_mode(
            binance_sdk::margin_trading::rest_api::MarginAccountNewOrderSelfTradePreventionModeEnum::ExpireMaker,
        )
        .trailing_delta(25)
        .auto_repay_at_cancel(false)
        .recv_window(5000)
        .build()
        .unwrap();

        let _ = margin_rest_at(&server)
            .margin_account_new_order(params.clone())
            .await
            .unwrap();
        let _ = binance_sdk::common::utils::send_request::<serde_json::Value>(
            &margin_rest_config_at(&server),
            ORDER_PATH,
            reqwest::Method::POST,
            new_order_query(params),
            BTreeMap::new(),
            None,
            true,
        )
        .await
        .unwrap();

        let sent = signed_requests_sent(&server).await;
        assert_eq!(sent.len(), 2);
        assert_eq!(sent[0].0.len(), 17, "every field is sent: {:?}", sent[0].0);
        assert_eq!(sent[0], sent[1]);
    }

    #[test]
    fn a_reconnect_opens_a_gap_on_each_stream_instrument_and_checks_those_with_orders_held() {
        let [btc, eth, xrp] = ["BTCUSDT", "ETHUSDT", "XRPUSDT"].map(InstrumentNameExchange::new);
        let known = KnownLiveOrders::shared(ExchangeId::BinanceMargin);
        for (instrument, cid) in [(&btc, "btc"), (&xrp, "xrp")] {
            known.lock().live(
                &margin_key(instrument.name(), cid),
                Decimal::TWO,
                OrderKind::Limit,
                &Open::new(
                    VenueOrderId::Assigned(OrderId::new(cid)),
                    Utc::now(),
                    Decimal::ZERO,
                ),
            );
        }
        let mut unrecovered = UnrecoveredFills::default();
        let mut unchecked = UncheckedOrders::default();

        open_reconnect_recovery(
            &mut unrecovered,
            &mut unchecked,
            &known,
            &[btc.clone(), eth.clone()],
            Utc::now() - chrono::Duration::minutes(1),
        );

        assert!(unrecovered.covers(&btc) && unrecovered.covers(&eth));
        assert!(!unrecovered.covers(&xrp), "not the stream's");
        assert!(unchecked.contains(&btc));
        assert!(!unchecked.contains(&eth), "no order held");
        assert!(!unchecked.contains(&xrp), "its fills are not recovered");
    }

    /// Margin fill recovery ends a held order that a recovered fill completes, and keeps one it
    /// only part-fills, so a later check does not report the completed one again.
    #[tokio::test]
    async fn a_recovered_margin_fill_that_completes_a_held_order_ends_it() {
        let disconnect = Utc::now() - chrono::Duration::minutes(10);
        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .and(wiremock::matchers::path("/sapi/v1/margin/myTrades"))
            .respond_with(venue(vec![
                (
                    1,
                    (disconnect + chrono::Duration::minutes(1)).timestamp_millis(),
                ),
                (
                    2,
                    (disconnect + chrono::Duration::minutes(2)).timestamp_millis(),
                ),
            ]))
            .mount(&server)
            .await;
        // Each venue trade is 0.01 of an order of its own id: order 1 is complete at 0.01, order 2
        // is of 0.02 and part-filled.
        let known = KnownLiveOrders::shared(ExchangeId::BinanceMargin);
        for (cid, order_id, quantity) in [("one", "1", "0.01"), ("two", "2", "0.02")] {
            known.lock().live(
                &margin_key("BTCUSDT", cid),
                Decimal::from_str(quantity).unwrap(),
                OrderKind::Limit,
                &Open::new(
                    VenueOrderId::Assigned(OrderId::new(order_id)),
                    Utc::now(),
                    Decimal::ZERO,
                ),
            );
        }
        let mut unrecovered = UnrecoveredFills::default();
        unrecovered.open(
            &[InstrumentNameExchange::new("BTCUSDT")],
            disconnect,
            Utc::now(),
        );
        let (tx, mut rx) = mpsc::unbounded_channel();

        recover_margin_fills(
            &margin_rest_at(&server),
            &Arc::new(RateLimitTracker::new(WeightPool::Sapi)),
            &mut unrecovered,
            &tx,
            &new_dedup_cache(),
            &known,
            false,
        )
        .await;

        assert_eq!(
            std::iter::from_fn(|| rx.try_recv().ok()).count(),
            2,
            "both fills"
        );
        let known = known.lock();
        assert!(!known.contains(&ClientOrderId::new("one")), "completed");
        assert!(known.contains(&ClientOrderId::new("two")), "part-filled");
    }

    /// An IOC order that ended in its placement response is reported as expired with what
    /// filled, and is not held as live, so a late report of it as live cannot bring it back.
    #[tokio::test]
    async fn a_margin_order_that_expired_on_placement_is_not_reported_or_held_open() {
        use rust_decimal_macros::dec;

        for (cid, filled, quote) in [("unfilled", "0", "0"), ("partly", "1", "100")] {
            let server = wiremock::MockServer::start().await;
            wiremock::Mock::given(wiremock::matchers::method("POST"))
                .and(wiremock::matchers::path("/sapi/v1/margin/order"))
                .respond_with(wiremock::ResponseTemplate::new(200).set_body_json(
                    serde_json::json!({
                        "symbol": "BTCUSDT", "orderId": 7, "clientOrderId": cid,
                        "transactTime": 1_700_000_000_000_i64, "price": "100", "origQty": "2",
                        "executedQty": filled, "cummulativeQuoteQty": quote,
                        "status": "EXPIRED", "timeInForce": "IOC", "type": "LIMIT", "side": "BUY",
                    }),
                ))
                .mount(&server)
                .await;
            let client = margin_client_at(&server, false);
            let btcusdt = InstrumentNameExchange::new("BTCUSDT");

            let placed = client.open_order(margin_open_request(&btcusdt, cid)).await;

            let OrderState::Inactive(crate::order::state::InactiveOrderState::Expired(expired)) =
                &placed.state
            else {
                panic!("{cid}: expired, not open: {placed:?}");
            };
            assert_eq!(expired.filled_quantity, Decimal::from_str(filled).ok());
            let late = Open::new(
                VenueOrderId::Assigned(OrderId::new("7")),
                Utc::now(),
                Decimal::ZERO,
            );
            let mut known = client.known_live.lock();
            known.live(&placed.key, dec!(2), OrderKind::Limit, &late);
            assert!(
                !known.contains(&ClientOrderId::new(cid)),
                "{cid}: not held, even after a late live report"
            );
        }
    }

    /// Answer the requests behind `fetch_shortability` and `fetch_borrow_capacity`: the BTCUSDT
    /// pair on cross and isolated margin, the base asset, its hourly rate, its max borrow, and an
    /// empty delisting schedule, which [`mount_delist_schedule`] overrides.
    async fn mount_borrow_terms(
        server: &wiremock::MockServer,
        pair: serde_json::Value,
        asset: serde_json::Value,
        rate: serde_json::Value,
        max_borrow: serde_json::Value,
    ) {
        use wiremock::{
            Mock, ResponseTemplate,
            matchers::{method, path},
        };
        for (route, body) in [
            ("/sapi/v1/margin/allPairs", pair.clone()),
            ("/sapi/v1/margin/isolated/allPairs", pair),
            ("/sapi/v1/margin/allAssets", asset),
            ("/sapi/v1/margin/next-hourly-interest-rate", rate),
            ("/sapi/v1/margin/maxBorrowable", max_borrow),
            ("/sapi/v1/margin/delist-schedule", serde_json::json!([])),
        ] {
            Mock::given(method("GET"))
                .and(path(route))
                .respond_with(ResponseTemplate::new(200).set_body_json(body))
                .mount(server)
                .await;
        }
    }

    /// Answer `/sapi/v1/margin/delist-schedule` with `schedule`, ahead of the empty one
    /// [`mount_borrow_terms`] mounts.
    async fn mount_delist_schedule(server: &wiremock::MockServer, schedule: serde_json::Value) {
        use wiremock::{
            Mock, ResponseTemplate,
            matchers::{method, path},
        };
        Mock::given(method("GET"))
            .and(path("/sapi/v1/margin/delist-schedule"))
            .respond_with(ResponseTemplate::new(200).set_body_json(schedule))
            .with_priority(1)
            .mount(server)
            .await;
    }

    fn btcusdt_pair(is_margin_trade: bool, is_sell_allowed: bool) -> serde_json::Value {
        serde_json::json!([{
            "base": "BTC", "quote": "USDT", "symbol": "BTCUSDT",
            "isMarginTrade": is_margin_trade, "isSellAllowed": is_sell_allowed, "isBuyAllowed": true,
        }])
    }

    fn btc_asset(is_borrowable: bool) -> serde_json::Value {
        serde_json::json!([{ "assetName": "BTC", "isBorrowable": is_borrowable }])
    }

    fn btc_rate(rate: &str) -> serde_json::Value {
        serde_json::json!([{ "asset": "BTC", "nextHourlyInterestRate": rate }])
    }

    /// The value of `key` in the query of the request `server` received on `route`.
    async fn query_value(server: &wiremock::MockServer, route: &str, key: &str) -> Option<String> {
        server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .find(|request| request.url.path() == route)
            .and_then(|request| {
                request
                    .url
                    .query_pairs()
                    .find(|(name, _)| name == key)
                    .map(|(_, value)| value.into_owned())
            })
    }

    #[tokio::test]
    async fn margin_shortability_reads_the_pair_the_asset_and_the_annualised_rate() {
        use crate::client::ShortabilityClient;
        use rust_decimal_macros::dec;
        for (is_isolated, pairs_route, flag) in [
            (false, "/sapi/v1/margin/allPairs", "FALSE"),
            (true, "/sapi/v1/margin/isolated/allPairs", "TRUE"),
        ] {
            let server = wiremock::MockServer::start().await;
            mount_borrow_terms(
                &server,
                btcusdt_pair(true, true),
                btc_asset(true),
                btc_rate("0.00000571"),
                serde_json::json!({}),
            )
            .await;

            let shortability = margin_client_at(&server, is_isolated)
                .fetch_shortability(&InstrumentNameExchange::new("BTCUSDT"))
                .await
                .unwrap();

            assert_eq!(
                shortability,
                Shortability::new()
                    .with_shortable(true)
                    .with_fee_rate(dec!(0.00000571) * dec!(8760)),
                "isolated: {is_isolated}"
            );
            assert_eq!(
                query_value(&server, pairs_route, "symbol").await.as_deref(),
                Some("BTCUSDT"),
                "the pair list is asked for the one symbol"
            );
            assert_eq!(
                query_value(&server, "/sapi/v1/margin/allAssets", "asset")
                    .await
                    .as_deref(),
                Some("BTC"),
                "the base asset is the one borrowed"
            );
            let rate = "/sapi/v1/margin/next-hourly-interest-rate";
            assert_eq!(
                query_value(&server, rate, "assets").await.as_deref(),
                Some("BTC")
            );
            assert_eq!(
                query_value(&server, rate, "isIsolated").await.as_deref(),
                Some(flag)
            );
        }
    }

    #[tokio::test]
    async fn margin_shortability_is_refused_by_any_flag_and_unknown_without_them() {
        use crate::client::ShortabilityClient;
        let cases = [
            (btcusdt_pair(false, true), btc_asset(true), Some(false)),
            (btcusdt_pair(true, false), btc_asset(true), Some(false)),
            (btcusdt_pair(true, true), btc_asset(false), Some(false)),
            // Binance does not list the asset, so whether it lends it is unknown.
            (btcusdt_pair(true, true), serde_json::json!([]), None),
        ];
        for (pair, asset, shortable) in cases {
            let server = wiremock::MockServer::start().await;
            mount_borrow_terms(
                &server,
                pair.clone(),
                asset.clone(),
                serde_json::json!([]),
                serde_json::json!({}),
            )
            .await;

            let shortability = margin_client_at(&server, false)
                .fetch_shortability(&InstrumentNameExchange::new("BTCUSDT"))
                .await
                .unwrap();

            assert_eq!(shortability.shortable, shortable, "{pair} {asset}");
            assert_eq!(
                shortability.fee_rate, None,
                "no rate returned is an unknown fee"
            );
            assert_eq!(shortability.easy_to_borrow, None);
            assert_eq!(shortability.available, None);
        }
    }

    /// Cross margin schedules assets, so either of the pair's assets delists the pair; isolated
    /// margin schedules symbols. The earliest matching time is reported, in milliseconds, and once
    /// it has passed the pair is not shortable.
    #[tokio::test]
    async fn margin_shortability_reads_the_delisting_schedule_for_this_margin() {
        use crate::client::ShortabilityClient;
        let past = Utc::now() - chrono::Duration::hours(1);
        let soon = Utc::now() + chrono::Duration::days(3);
        let later = soon + chrono::Duration::days(7);
        let entry = |time: DateTime<Utc>, assets: &[&str], symbols: &[&str]| {
            serde_json::json!({
                "delistTime": time.timestamp_millis(),
                "crossMarginAssets": assets,
                "isolatedMarginSymbols": symbols,
            })
        };
        let millis = |time: DateTime<Utc>| DateTime::from_timestamp_millis(time.timestamp_millis());
        let cases = [
            // Cross: the base asset, the quote asset, the earliest of two, none matching.
            (
                false,
                vec![entry(soon, &["BTC"], &[])],
                millis(soon),
                Some(true),
            ),
            (
                false,
                vec![entry(soon, &["USDT"], &[])],
                millis(soon),
                Some(true),
            ),
            (
                false,
                vec![
                    entry(later, &["BTC"], &[]),
                    entry(soon, &["ETH", "USDT"], &[]),
                ],
                millis(soon),
                Some(true),
            ),
            (
                false,
                vec![entry(soon, &["ETH"], &["BTCUSDT"])],
                None,
                Some(true),
            ),
            (
                false,
                vec![entry(past, &["BTC"], &[])],
                millis(past),
                Some(false),
            ),
            // Isolated: the symbol only, never the assets.
            (
                true,
                vec![entry(soon, &[], &["BTCUSDT"])],
                millis(soon),
                Some(true),
            ),
            (
                true,
                vec![entry(soon, &["BTC", "USDT"], &["ETHUSDT"])],
                None,
                Some(true),
            ),
            (
                true,
                vec![entry(past, &[], &["BTCUSDT"])],
                millis(past),
                Some(false),
            ),
        ];
        for (is_isolated, schedule, delisting_at, shortable) in cases {
            let server = wiremock::MockServer::start().await;
            mount_borrow_terms(
                &server,
                btcusdt_pair(true, true),
                btc_asset(true),
                serde_json::json!([]),
                serde_json::json!({}),
            )
            .await;
            let schedule = serde_json::Value::from(schedule);
            mount_delist_schedule(&server, schedule.clone()).await;

            let shortability = margin_client_at(&server, is_isolated)
                .fetch_shortability(&InstrumentNameExchange::new("BTCUSDT"))
                .await
                .unwrap();

            let case = format!("isolated: {is_isolated}, {schedule}");
            assert_eq!(shortability.delisting_at, delisting_at, "{case}");
            assert_eq!(shortability.shortable, shortable, "{case}");
        }
    }

    /// A scheduled entry without a readable time is an error, not a pair left shortable.
    #[tokio::test]
    async fn margin_shortability_of_a_scheduled_pair_without_a_time_is_an_error() {
        use crate::client::ShortabilityClient;
        let server = wiremock::MockServer::start().await;
        mount_borrow_terms(
            &server,
            btcusdt_pair(true, true),
            btc_asset(true),
            serde_json::json!([]),
            serde_json::json!({}),
        )
        .await;
        mount_delist_schedule(
            &server,
            serde_json::json!([{ "crossMarginAssets": ["BTC"] }]),
        )
        .await;

        let result = margin_client_at(&server, false)
            .fetch_shortability(&InstrumentNameExchange::new("BTCUSDT"))
            .await;

        assert!(
            matches!(result, Err(UnindexedClientError::Internal(ref message))
                if message.contains("delistTime")),
            "{result:?}"
        );
    }

    #[tokio::test]
    async fn margin_shortability_of_an_unlisted_pair_is_an_invalid_instrument() {
        use crate::client::ShortabilityClient;
        let server = wiremock::MockServer::start().await;
        mount_borrow_terms(
            &server,
            serde_json::json!([]),
            btc_asset(true),
            btc_rate("0.00000571"),
            serde_json::json!({}),
        )
        .await;

        let result = margin_client_at(&server, false)
            .fetch_shortability(&InstrumentNameExchange::new("BTCUSDT"))
            .await;

        assert!(
            matches!(
                result,
                Err(UnindexedClientError::Api(ApiError::InstrumentInvalid(ref instrument, _)))
                    if instrument.name().as_str() == "BTCUSDT"
            ),
            "{result:?}"
        );
    }

    #[tokio::test]
    async fn margin_borrow_capacity_asks_for_the_asset_the_side_borrows() {
        use crate::client::BorrowCapacityClient;
        use rust_decimal_macros::dec;
        for (is_isolated, side, asset, isolated_symbol) in [
            (false, Side::Sell, "BTC", None),
            (false, Side::Buy, "USDT", None),
            (true, Side::Sell, "BTC", Some("BTCUSDT")),
        ] {
            let server = wiremock::MockServer::start().await;
            mount_borrow_terms(
                &server,
                btcusdt_pair(true, true),
                btc_asset(true),
                btc_rate("0.00000571"),
                serde_json::json!({ "amount": "1.69248805", "borrowLimit": "60" }),
            )
            .await;

            let capacity = margin_client_at(&server, is_isolated)
                .fetch_borrow_capacity(&InstrumentNameExchange::new("BTCUSDT"), side)
                .await
                .unwrap();

            assert_eq!(
                capacity,
                BorrowCapacity::new(AssetNameExchange::new(asset))
                    .with_borrowable_now(dec!(1.69248805))
                    .with_account_limit(dec!(60)),
                "{side:?} isolated: {is_isolated}"
            );
            let max_borrow = "/sapi/v1/margin/maxBorrowable";
            assert_eq!(
                query_value(&server, max_borrow, "asset").await.as_deref(),
                Some(asset)
            );
            assert_eq!(
                query_value(&server, max_borrow, "isolatedSymbol")
                    .await
                    .as_deref(),
                isolated_symbol,
                "only isolated margin names the pair's account"
            );
        }
    }

    #[tokio::test]
    async fn margin_borrow_capacity_keeps_an_omitted_amount_unknown_and_fails_a_malformed_one() {
        use crate::client::BorrowCapacityClient;
        use rust_decimal_macros::dec;
        let instrument = InstrumentNameExchange::new("BTCUSDT");

        let server = wiremock::MockServer::start().await;
        mount_borrow_terms(
            &server,
            btcusdt_pair(true, true),
            btc_asset(true),
            btc_rate("0"),
            serde_json::json!({ "borrowLimit": "60" }),
        )
        .await;
        let capacity = margin_client_at(&server, false)
            .fetch_borrow_capacity(&instrument, Side::Sell)
            .await
            .unwrap();
        assert_eq!(capacity.borrowable_now, None);
        assert_eq!(capacity.account_limit, Some(dec!(60)));

        let server = wiremock::MockServer::start().await;
        mount_borrow_terms(
            &server,
            btcusdt_pair(true, true),
            btc_asset(true),
            btc_rate("0"),
            serde_json::json!({ "amount": "lots", "borrowLimit": "60" }),
        )
        .await;
        let result = margin_client_at(&server, false)
            .fetch_borrow_capacity(&instrument, Side::Sell)
            .await;
        assert!(
            matches!(result, Err(UnindexedClientError::Internal(ref message)) if message.contains("amount")),
            "{result:?}"
        );
    }

    #[tokio::test]
    async fn margin_borrow_capacity_without_an_isolated_account_names_the_pair() {
        use crate::client::BorrowCapacityClient;
        use wiremock::{
            Mock,
            matchers::{method, path},
        };
        let server = wiremock::MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/sapi/v1/margin/maxBorrowable"))
            .respond_with(
                wiremock::ResponseTemplate::new(400).set_body_json(serde_json::json!({
                    "code": -11001, "msg": "Isolated margin account does not exist."
                })),
            )
            .mount(&server)
            .await;
        mount_borrow_terms(
            &server,
            btcusdt_pair(true, true),
            btc_asset(true),
            btc_rate("0"),
            serde_json::json!({}),
        )
        .await;

        let err = margin_client_at(&server, true)
            .fetch_borrow_capacity(&InstrumentNameExchange::new("BTCUSDT"), Side::Sell)
            .await
            .unwrap_err();

        assert_eq!(
            err.to_string(),
            "API: request rejected: BTCUSDT: -11001 Isolated margin account does not exist."
        );
        assert!(!err.is_transient());
    }
}
