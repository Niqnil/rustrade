//! Binance Cross Margin execution client.
//!
//! [`BinanceMargin`] is a margin counterpart to [`super::spot::BinanceSpot`]. It shares the
//! exchange-agnostic infrastructure in [`super::shared`] (rate-limit tracking, reconnect/backoff,
//! event deduplication, error parsing) and is intended to implement the same `ExecutionClient`
//! trait so callers do not branch on spot-vs-margin transport.
//!
//! ## Scope
//! This module currently provides the client's *identity and configuration* only: the
//! [`BinanceMargin`] struct, its [`BinanceMarginConfig`], and the [`MarginSideEffect`] borrow
//! policy. Order submission, account snapshots, and the live user-data stream are added in
//! follow-up work; until then `BinanceMargin` is not yet a usable `ExecutionClient`.
//!
//! ## Borrow/repay
//! Margin orders carry a `sideEffectType` controlling whether the venue borrows on entry and
//! repays on close. This is configured once per client via [`MarginSideEffect`] (see its docs for
//! the silent-borrow footgun under the default [`MarginSideEffect::AutoBorrowRepay`]).
//!
//! ## No testnet
//! Binance margin/SAPI exists on **no** testnet — the SDK exposes only
//! `MarginTradingRestApi::production`. A `testnet: true` config is therefore inert for margin and
//! always resolves to production endpoints; the constructor logs a warning so this is observable
//! rather than silent. See [`BinanceMarginConfig::testnet`].

use super::shared::{
    BinanceOrderType, BinanceTimeInForce, RateLimitTracker, classify_order_kind_tif,
    classify_rest_order_error, rest_call_with_retry,
};
use crate::{
    error::{ApiError, OrderError},
    order::{
        Order, OrderKey, OrderKind, TimeInForce,
        id::OrderId,
        request::{OrderRequestCancel, OrderRequestOpen, UnindexedOrderResponseCancel},
        state::{Cancelled, Filled, Open, OrderState, UnindexedOrderState},
    },
};
use binance_sdk::{
    common::config::{ConfigurationRestApi, ConfigurationWebsocketApi},
    margin_trading::{
        MarginTradingRestApi,
        rest_api::{
            MarginAccountCancelOrderParams, MarginAccountNewOrderNewOrderRespTypeEnum,
            MarginAccountNewOrderParams, MarginAccountNewOrderSideEnum,
            MarginAccountNewOrderTimeInForceEnum, RestApi,
        },
    },
};
use chrono::{TimeZone, Utc};
use rust_decimal::Decimal;
use rustrade_instrument::{Side, exchange::ExchangeId, instrument::name::InstrumentNameExchange};
use serde::{Deserialize, Serialize};
use smol_str::{SmolStr, format_smolstr};
use std::{str::FromStr, sync::Arc};
use tracing::{error, warn};

// ---------------------------------------------------------------------------
// Margin sideEffectType
// ---------------------------------------------------------------------------

/// Margin `sideEffectType` borrow/repay policy, fixed once per [`BinanceMargin`] client.
///
/// Only the two *intent-agnostic* modes are exposed as client-level variants. The borrow-vs-repay
/// decision derives from open/close direction — position state the library deliberately does not
/// track — so the venue is left to infer it from the account's loan state. The per-order modes
/// (`MARGIN_BUY` / `AUTO_REPAY`) are intentionally **not** modelled here; they belong to a future
/// generic per-order `RequestOpen::margin_effect` field that all margin adapters would map, added
/// only when a venue with genuine per-order borrow intent (e.g. another CEX margin) appears.
/// Mixed borrow appetite on a single venue today means running two clients (one per mode).
///
/// The `Serialize`/`Deserialize` wire form is the config-file value (`"auto_borrow_repay"` /
/// `"no_borrow"`); this is distinct from the Binance API value returned by
/// [`as_binance_str`](Self::as_binance_str) (`"AUTO_BORROW_REPAY"` / `"NO_SIDE_EFFECT"`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MarginSideEffect {
    /// `AUTO_BORROW_REPAY` — the venue borrows on entry and repays on close as needed.
    ///
    /// # Warning
    /// This is the default because it makes shorting work out of the box, but it means a
    /// **mis-sized order silently takes on debt**: an order larger than the free balance borrows
    /// the shortfall without any further opt-in (same footgun class as `RequestOpen::reduce_only`).
    /// Use [`MarginSideEffect::NoBorrow`] for a client that must never silently borrow.
    #[default]
    AutoBorrowRepay,
    /// `NO_SIDE_EFFECT` — never borrow or repay; orders that would require borrowing are rejected
    /// by the venue. The conservative opt-out from the [`AutoBorrowRepay`](Self::AutoBorrowRepay)
    /// silent-borrow behaviour.
    NoBorrow,
}

impl MarginSideEffect {
    /// The Binance `sideEffectType` wire string for this policy.
    pub fn as_binance_str(self) -> &'static str {
        match self {
            MarginSideEffect::AutoBorrowRepay => "AUTO_BORROW_REPAY",
            MarginSideEffect::NoBorrow => "NO_SIDE_EFFECT",
        }
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
    /// Isolated margin when `true`; cross margin (account-wide) when `false`.
    ///
    /// Defaults to `false` (cross). Isolated support is a follow-up; cross is the primary mode.
    ///
    /// # Warning
    /// Setting this to `true` is **not yet honoured**: the client currently operates as cross
    /// margin regardless, logging a warning at construction (see [`BinanceMargin::new`]) rather
    /// than failing silently. Isolated execution is a separate follow-up.
    #[serde(default)]
    pub is_isolated: bool,
    /// Borrow/repay policy applied to every order placed by this client.
    ///
    /// Defaults to [`MarginSideEffect::AutoBorrowRepay`] — see its `# Warning` on silent borrowing.
    #[serde(default)]
    pub side_effect: MarginSideEffect,
}

// custom Debug to avoid leaking credentials in logs
impl std::fmt::Debug for BinanceMarginConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BinanceMarginConfig")
            .field("api_key", &"***")
            .field("secret_key", &"***")
            .field("testnet", &self.testnet)
            .field("is_isolated", &self.is_isolated)
            .field("side_effect", &self.side_effect)
            .finish()
    }
}

impl BinanceMarginConfig {
    /// Construct a [`BinanceMarginConfig`] with explicit control over every field.
    ///
    /// Prefer [`BinanceMarginConfig::cross_margin`] for the common case, or deserialize from a
    /// config file to keep credentials out of source. The required positional args are the
    /// credentials plus any non-defaultable knobs; defaultable knobs (`is_isolated`, `side_effect`)
    /// are exposed here for full control and via `#[serde(default)]` for the file path.
    pub fn new(
        api_key: String,
        secret_key: String,
        testnet: bool,
        is_isolated: bool,
        side_effect: MarginSideEffect,
    ) -> Self {
        Self {
            api_key,
            secret_key,
            testnet,
            is_isolated,
            side_effect,
        }
    }

    /// Convenience constructor for the common case: cross margin, production endpoints, and the
    /// default [`MarginSideEffect::AutoBorrowRepay`] borrow/repay policy.
    ///
    /// Equivalent to [`new`](Self::new) with `testnet = false`, `is_isolated = false`, and the
    /// default `side_effect`. Use [`new`](Self::new) when any of those need to differ.
    pub fn cross_margin(api_key: String, secret_key: String) -> Self {
        Self::new(
            api_key,
            secret_key,
            false,
            false,
            MarginSideEffect::default(),
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

/// Binance Cross Margin execution client using the official binance-sdk.
///
/// Places orders and queries account state over the margin REST API
/// (`margin_trading::rest_api`). The live user-data stream (added later) uses a hand-rolled
/// `userListenToken` flow over the WS API, not the SDK's retired listen-key path.
///
/// See the [module docs](self) for scope, borrow/repay behaviour, and the no-testnet caveat.
#[derive(Clone)]
pub struct BinanceMargin {
    config: Arc<BinanceMarginConfig>,
    // REST client for orders/queries (order submission/cancel; account snapshots in follow-up work).
    rest: Arc<RestApi>,
    // WS-API configuration (credentials → common-layer config). Held here so the live user-data
    // stream — built later as a directly-constructed `common::websocket::WebsocketApi`, which needs
    // a connection pool that only exists at connect time — can consume it without re-reading creds.
    #[allow(dead_code)] // consumed when the user-data stream connection is built in follow-up work
    ws_config: ConfigurationWebsocketApi,
    // shared rate-limit tracker across all REST calls
    rate_limiter: Arc<RateLimitTracker>,
}

impl std::fmt::Debug for BinanceMargin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BinanceMargin")
            .field("testnet", &self.config.testnet)
            .field("is_isolated", &self.config.is_isolated)
            .field("side_effect", &self.config.side_effect)
            .finish_non_exhaustive()
    }
}

impl BinanceMargin {
    /// Construct a `BinanceMargin` client from its configuration.
    ///
    /// # Panics
    /// Panics if the binance-sdk configuration builder fails (e.g. empty or malformed
    /// API key/secret), matching [`BinanceSpot`](super::spot::BinanceSpot)'s startup contract.
    pub fn new(config: BinanceMarginConfig) -> Self {
        if config.testnet {
            warn!(
                "BinanceMarginConfig.testnet = true is ignored: Binance margin has no testnet; \
                 using production endpoints"
            );
        }
        if config.is_isolated {
            warn!(
                "BinanceMarginConfig.is_isolated = true is not yet supported: isolated margin is a \
                 follow-up; operating as cross margin"
            );
        }
        let rest = Self::build_rest(&config);
        let ws_config = Self::build_ws_config(&config);
        Self {
            config: Arc::new(config),
            rest,
            ws_config,
            rate_limiter: Arc::new(RateLimitTracker::new()),
        }
    }

    /// # Panics
    /// Panics if the binance-sdk configuration builder fails (invalid credentials format).
    #[allow(clippy::expect_used)] // Documented panic: invalid credentials detected at startup
    fn build_rest(config: &BinanceMarginConfig) -> Arc<RestApi> {
        let rest_config = ConfigurationRestApi::builder()
            .api_key(config.api_key.clone())
            .api_secret(config.secret_key.clone())
            .build()
            .expect("failed to build Binance margin REST configuration");

        // Margin/SAPI has no testnet — `MarginTradingRestApi` exposes only `production`.
        Arc::new(MarginTradingRestApi::production(rest_config))
    }

    /// Build the WS-API configuration for the user-data stream.
    ///
    /// The connection itself is established later (it requires a live connection pool); this only
    /// captures the credentials at the common-layer config so the stream task need not re-read them.
    ///
    /// # Panics
    /// Panics if the binance-sdk configuration builder fails (invalid credentials format).
    #[allow(clippy::expect_used)] // Documented panic: invalid credentials detected at startup
    fn build_ws_config(config: &BinanceMarginConfig) -> ConfigurationWebsocketApi {
        ConfigurationWebsocketApi::builder()
            .api_key(config.api_key.clone())
            .api_secret(config.secret_key.clone())
            .build()
            .expect("failed to build Binance margin WebSocket configuration")
    }

    /// Submit a margin order over the SAPI `POST /sapi/v1/margin/order` endpoint.
    ///
    /// Mirrors [`BinanceSpot::open_order`](super::spot::BinanceSpot)'s contract: never returns
    /// `None` (every failure is folded into the returned [`Order`]'s state as
    /// [`OrderState::inactive`]), so the engine always sees a definitive outcome.
    ///
    /// Margin specifics:
    /// - `sideEffectType` is the client-level [`MarginSideEffect`] (borrow/repay policy).
    /// - `isIsolated` is always `"FALSE"` (cross) in this version — isolated margin is a follow-up.
    /// - `autoRepayAtCancel` is set only under [`MarginSideEffect::AutoBorrowRepay`]: a `NoBorrow`
    ///   client takes no loan, so requesting repay-on-cancel would be incoherent.
    /// - Trailing-stop kinds return [`OrderError::UnsupportedOrderType`] (the SDK omits
    ///   `trailingDelta` on the margin binding).
    ///
    /// This is an inherent method for now; it becomes the `ExecutionClient::open_order` body once
    /// the account snapshot/query/stream methods land and the trait can be implemented in full.
    pub async fn open_order(
        &self,
        request: OrderRequestOpen<ExchangeId, &InstrumentNameExchange>,
    ) -> Option<Order<ExchangeId, InstrumentNameExchange, UnindexedOrderState>> {
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

        // Build the returned Order with a given inactive (failure) state, preserving the request
        // fields — keeps the many early-return error paths to one line each.
        let inactive = |state: UnindexedOrderState| {
            Some(Order {
                key: order_key.clone(),
                side,
                price,
                quantity,
                kind,
                time_in_force,
                state,
            })
        };

        let params = match build_new_order_params(
            instrument.name().to_string(),
            side,
            price,
            quantity,
            kind,
            time_in_force,
            cid.0.to_string(),
            self.config.side_effect,
        ) {
            Ok(params) => params,
            Err(BuildOrderError::Unsupported) => {
                return inactive(OrderState::inactive(OrderError::UnsupportedOrderType(
                    format!(
                        "BinanceMargin does not support OrderKind::{kind:?} with {time_in_force:?}"
                    ),
                )));
            }
            Err(BuildOrderError::Build(msg)) => {
                error!(%msg, "BinanceMargin failed to build new order params");
                return inactive(OrderState::inactive(OrderError::Rejected(
                    ApiError::OrderRejected(msg),
                )));
            }
        };

        let response = match rest_call_with_retry(&self.rest, &self.rate_limiter, |rest| {
            let params = params.clone();
            Box::pin(async move { rest.margin_account_new_order(params).await })
        })
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
                // Deserialization failure on a 2xx response — surface as a rejection, not a
                // transport error (the order did reach the venue).
                return inactive(OrderState::inactive(OrderError::Rejected(
                    ApiError::OrderRejected(e.to_string()),
                )));
            }
        };

        let time_exchange = data
            .transact_time
            .and_then(|ms| Utc.timestamp_millis_opt(ms).single())
            .unwrap_or_else(Utc::now);

        let exchange_order_id = match data.order_id {
            Some(id) => OrderId(format_smolstr!("{id}")),
            None => {
                error!("BinanceMargin open_order response missing orderId");
                return inactive(OrderState::inactive(OrderError::Rejected(
                    ApiError::OrderRejected("open_order response missing orderId".into()),
                )));
            }
        };

        let filled_qty = match data.executed_qty.as_deref() {
            Some(q) => Decimal::from_str(q).unwrap_or_else(|_| {
                // Present-but-unparseable executedQty is corrupt data, not an expected
                // absence: silently defaulting to zero could misreport a filled order as
                // Open. Surface it (mirrors margin_avg_price).
                warn!(
                    executed_qty = q,
                    "BinanceMargin: failed to parse executedQty; treating as zero"
                );
                Decimal::ZERO
            }),
            None => {
                // executedQty is expected on margin's REST order/cancel responses; its absence is
                // anomalous (not the expected-empty case), so surface it rather than silently zero.
                warn!("BinanceMargin: executedQty missing in response; treating as zero");
                Decimal::ZERO
            }
        };

        let state = if filled_qty >= quantity {
            // Fully filled on placement — derive avg price from cumulative quote qty (margin's
            // REST response exposes this; spot's WS response does not, hence spot passes None).
            let avg_price = margin_avg_price(data.cummulative_quote_qty.as_deref(), filled_qty);
            OrderState::fully_filled(Filled::new(
                exchange_order_id,
                time_exchange,
                filled_qty,
                avg_price,
            ))
        } else {
            OrderState::active(Open::new(exchange_order_id, time_exchange, filled_qty))
        };

        Some(Order {
            key: order_key,
            side,
            price,
            quantity,
            kind,
            time_in_force,
            state,
        })
    }

    /// Cancel a resting margin order via `DELETE /sapi/v1/margin/order`.
    ///
    /// Cancels by exchange `orderId` when present and parseable, otherwise by the originating
    /// client order id. Cross only (`isIsolated = "FALSE"`). Mirrors
    /// [`BinanceSpot::cancel_order`](super::spot::BinanceSpot): every failure is folded into the
    /// returned response's `state` as an `Err`.
    ///
    /// The margin cancel response carries no `transactTime`, so the cancellation timestamp is the
    /// local receive time.
    ///
    /// Inherent for now; becomes the `ExecutionClient::cancel_order` body when the trait is
    /// implemented in full (see [`open_order`](Self::open_order)).
    pub async fn cancel_order(
        &self,
        request: OrderRequestCancel<ExchangeId, &InstrumentNameExchange>,
    ) -> Option<UnindexedOrderResponseCancel> {
        let instrument = request.key.instrument.clone();
        let key = OrderKey {
            exchange: request.key.exchange,
            instrument: instrument.clone(),
            strategy: request.key.strategy.clone(),
            cid: request.key.cid.clone(),
        };

        let mut builder = MarginAccountCancelOrderParams::builder(instrument.name().to_string())
            .is_isolated("FALSE".to_string());

        if let Some(ref order_id) = request.state.id {
            if let Ok(id) = order_id.0.parse::<i64>() {
                builder = builder.order_id(id);
            } else {
                // exchange order id exists but isn't a valid i64 — fall back to the cid.
                error!(
                    order_id = %order_id.0,
                    "BinanceMargin cancel: exchange orderId not parseable as i64, falling back to clientOrderId"
                );
                builder = builder.orig_client_order_id(request.key.cid.0.to_string());
            }
        } else {
            builder = builder.orig_client_order_id(request.key.cid.0.to_string());
        }

        let params = match builder.build() {
            Ok(p) => p,
            Err(e) => {
                error!(%e, "BinanceMargin failed to build cancel order params");
                return Some(UnindexedOrderResponseCancel {
                    key,
                    state: Err(OrderError::Rejected(ApiError::OrderRejected(e.to_string()))),
                });
            }
        };

        let response = match rest_call_with_retry(&self.rest, &self.rate_limiter, |rest| {
            let params = params.clone();
            Box::pin(async move { rest.margin_account_cancel_order(params).await })
        })
        .await
        {
            Ok(response) => response,
            Err(e) => {
                return Some(UnindexedOrderResponseCancel {
                    key,
                    state: Err(classify_rest_order_error(&e, &instrument)),
                });
            }
        };

        let data = match response.data().await {
            Ok(data) => data,
            Err(e) => {
                // Deserialization failure on a 2xx response — surface as a rejection, not a
                // transport error (the cancel request did reach the venue), mirroring open_order.
                return Some(UnindexedOrderResponseCancel {
                    key,
                    state: Err(OrderError::Rejected(ApiError::OrderRejected(e.to_string()))),
                });
            }
        };

        // Margin cancel response has no transactTime field — use local receive time.
        let time_exchange = Utc::now();

        let exchange_order_id = match data.order_id {
            // NB: the margin cancel response types orderId as a String (the new-order response
            // types it as i64) — use it verbatim.
            Some(id) => OrderId(SmolStr::new(id)),
            None => {
                error!("BinanceMargin cancel response missing orderId");
                return Some(UnindexedOrderResponseCancel {
                    key,
                    state: Err(OrderError::Rejected(ApiError::OrderRejected(
                        "cancel response missing orderId".into(),
                    ))),
                });
            }
        };

        let filled_qty = match data.executed_qty.as_deref() {
            Some(q) => Decimal::from_str(q).unwrap_or_else(|_| {
                // Present-but-unparseable executedQty is corrupt data, not an expected
                // absence: silently defaulting to zero could misreport a filled order as
                // Open. Surface it (mirrors margin_avg_price).
                warn!(
                    executed_qty = q,
                    "BinanceMargin: failed to parse executedQty; treating as zero"
                );
                Decimal::ZERO
            }),
            None => {
                // executedQty is expected on margin's REST order/cancel responses; its absence is
                // anomalous (not the expected-empty case), so surface it rather than silently zero.
                warn!("BinanceMargin: executedQty missing in response; treating as zero");
                Decimal::ZERO
            }
        };

        Some(UnindexedOrderResponseCancel {
            key,
            state: Ok(Cancelled::new(exchange_order_id, time_exchange, filled_qty)),
        })
    }
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
}

/// Build the margin new-order params from a rustrade order request (pure; no I/O).
///
/// Factored out of [`BinanceMargin::open_order`] so the rustrade→Binance mapping (sideEffectType,
/// cross `isIsolated`, conditional `stopPrice`, `autoRepayAtCancel` gating, trailing rejection) is
/// unit-testable without a live REST call. `isIsolated` is always `"FALSE"` (cross) in this
/// version.
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
    side_effect: MarginSideEffect,
) -> Result<MarginAccountNewOrderParams, BuildOrderError> {
    let binance_side = match side {
        Side::Buy => MarginAccountNewOrderSideEnum::Buy,
        Side::Sell => MarginAccountNewOrderSideEnum::Sell,
    };

    let (binance_type, binance_tif) =
        convert_order_kind_tif_margin(kind, time_in_force).ok_or(BuildOrderError::Unsupported)?;

    // isIsolated is always "FALSE" (cross) in this version — isolated margin is a follow-up.
    let mut builder = MarginAccountNewOrderParams::builder(
        symbol,
        binance_side,
        binance_type.as_binance_str().to_string(),
    )
    .quantity(quantity)
    .is_isolated("FALSE".to_string())
    .side_effect_type(side_effect.as_binance_str().to_string())
    .new_client_order_id(new_client_order_id)
    .new_order_resp_type(MarginAccountNewOrderNewOrderRespTypeEnum::Full);

    // auto_repay_at_cancel only coheres under AutoBorrowRepay: a NoBorrow client never borrows,
    // so there is no loan to repay when an order is cancelled.
    if side_effect == MarginSideEffect::AutoBorrowRepay {
        builder = builder.auto_repay_at_cancel(true);
    }

    if let Some(tif) = binance_tif {
        builder = builder.time_in_force(tif);
    }

    // Conditional price fields (mirror spot): LIMIT carries price; STOP/TAKE_PROFIT carry a stop
    // (trigger) price; the *_LIMIT variants carry both.
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
        // Market carries no price/stop_price; trailing-stop kinds are rejected earlier by
        // convert_order_kind_tif_margin. A future OrderKind needing price fields that lands here
        // would surface as a BuildOrderError::Build from the SDK builder, not a silent bad order.
        _ => {}
    }

    builder
        .build()
        .map_err(|e| BuildOrderError::Build(e.to_string()))
}

/// Map a rustrade [`OrderKind`] + [`TimeInForce`] to the margin SDK's order `type`/`timeInForce`.
///
/// Reuses the shared [`classify_order_kind_tif`] decision logic, mapping only the venue-neutral
/// result onto margin's SDK output types — the `r#type` stays a [`BinanceOrderType`] (the caller
/// emits its [`as_binance_str`](BinanceOrderType::as_binance_str) wire string into the SDK's
/// `String` field) and the TIF becomes a [`MarginAccountNewOrderTimeInForceEnum`].
///
/// Returns `None` for unsupported combinations. Trailing-stop kinds are rejected here (the margin
/// SDK has no `trailingDelta` binding — Design decision #3), unlike spot which maps them to a
/// `STOP_LOSS` with `trailingDelta`.
fn convert_order_kind_tif_margin(
    kind: OrderKind,
    tif: TimeInForce,
) -> Option<(
    BinanceOrderType,
    Option<MarginAccountNewOrderTimeInForceEnum>,
)> {
    if matches!(
        kind,
        OrderKind::TrailingStop { .. } | OrderKind::TrailingStopLimit { .. }
    ) {
        warn!(
            ?kind,
            "BinanceMargin does not support trailing-stop orders (SDK trailingDelta binding gap)"
        );
        return None;
    }

    let (binance_type, binance_tif) = classify_order_kind_tif(kind, tif)?;
    let margin_tif = binance_tif.map(|t| match t {
        BinanceTimeInForce::Gtc => MarginAccountNewOrderTimeInForceEnum::Gtc,
        BinanceTimeInForce::Ioc => MarginAccountNewOrderTimeInForceEnum::Ioc,
        BinanceTimeInForce::Fok => MarginAccountNewOrderTimeInForceEnum::Fok,
    });
    Some((binance_type, margin_tif))
}

/// Volume-weighted average fill price from a margin order response's cumulative quote quantity.
///
/// `avg_price = cummulative_quote_qty / executed_qty`. Returns `None` when `filled_qty` is zero
/// (no fills, or division would be undefined) or the quote quantity is missing/unparseable.
// `cummulative_quote_qty` keeps Binance's own field-name typo (sic, double-m) to mirror the SDK.
fn margin_avg_price(cummulative_quote_qty: Option<&str>, filled_qty: Decimal) -> Option<Decimal> {
    if filled_qty.is_zero() {
        return None;
    }
    let s = cummulative_quote_qty?;
    match Decimal::from_str(s) {
        Ok(cumulative) => cumulative.checked_div(filled_qty),
        Err(_) => {
            // A filled order with an unparseable cumulative quote qty is corrupt data, not an
            // expected absence — log it rather than silently returning a price-less Filled.
            warn!(
                cummulative_quote_qty = s,
                "BinanceMargin: failed to parse cummulativeQuoteQty; avg price unavailable"
            );
            None
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;

    #[test]
    fn margin_side_effect_default_is_auto_borrow_repay() {
        assert_eq!(
            MarginSideEffect::default(),
            MarginSideEffect::AutoBorrowRepay
        );
    }

    #[test]
    fn margin_side_effect_wire_strings() {
        assert_eq!(
            MarginSideEffect::AutoBorrowRepay.as_binance_str(),
            "AUTO_BORROW_REPAY"
        );
        assert_eq!(
            MarginSideEffect::NoBorrow.as_binance_str(),
            "NO_SIDE_EFFECT"
        );
    }

    #[test]
    fn margin_side_effect_serde_round_trip() {
        // serde/config-file form is snake_case, distinct from the Binance wire string (as_binance_str).
        assert_eq!(
            serde_json::to_string(&MarginSideEffect::AutoBorrowRepay).unwrap(),
            r#""auto_borrow_repay""#
        );
        assert_eq!(
            serde_json::to_string(&MarginSideEffect::NoBorrow).unwrap(),
            r#""no_borrow""#
        );
        assert_eq!(
            serde_json::from_str::<MarginSideEffect>(r#""auto_borrow_repay""#).unwrap(),
            MarginSideEffect::AutoBorrowRepay
        );
        assert_eq!(
            serde_json::from_str::<MarginSideEffect>(r#""no_borrow""#).unwrap(),
            MarginSideEffect::NoBorrow
        );
    }

    #[test]
    fn config_debug_redacts_secrets() {
        let config = BinanceMarginConfig::new(
            "my_api_key".to_string(),
            "my_secret_key".to_string(),
            false,
            false,
            MarginSideEffect::default(),
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
        assert_eq!(config.side_effect, MarginSideEffect::AutoBorrowRepay);
        assert_eq!(config.api_key(), "k");
    }

    #[test]
    fn config_deserializes_with_defaults() {
        // is_isolated and side_effect default when omitted; testnet has no default and must be present.
        let config: BinanceMarginConfig =
            serde_json::from_str(r#"{"api_key":"k","secret_key":"s","testnet":false}"#)
                .expect("deserialize");
        assert!(!config.is_isolated);
        assert_eq!(config.side_effect, MarginSideEffect::AutoBorrowRepay);
    }

    #[test]
    fn config_deserializes_explicit_side_effect() {
        let config: BinanceMarginConfig = serde_json::from_str(
            r#"{"api_key":"k","secret_key":"s","testnet":false,"is_isolated":true,"side_effect":"no_borrow"}"#,
        )
        .expect("deserialize");
        assert!(config.is_isolated);
        assert_eq!(config.side_effect, MarginSideEffect::NoBorrow);
    }

    // -----------------------------------------------------------------------
    // Order param mapping (17.4.x)
    // -----------------------------------------------------------------------

    use crate::order::TrailingOffsetType;

    /// Build params for the common case, overriding only what a test cares about.
    fn params(
        side: Side,
        price: Option<Decimal>,
        kind: OrderKind,
        tif: TimeInForce,
        side_effect: MarginSideEffect,
    ) -> Result<MarginAccountNewOrderParams, BuildOrderError> {
        build_new_order_params(
            "BTCUSDT".to_string(),
            side,
            price,
            Decimal::from(2),
            kind,
            tif,
            "cid-1".to_string(),
            side_effect,
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
            MarginSideEffect::AutoBorrowRepay,
        )
        .expect("build");

        assert_eq!(p.symbol, "BTCUSDT");
        assert_eq!(p.side.as_str(), "BUY");
        assert_eq!(p.r#type, "LIMIT");
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
    fn new_order_is_always_cross() {
        // TG17 is cross-only: isIsolated must be "FALSE" regardless of config.is_isolated.
        let p = params(
            Side::Sell,
            Some(Decimal::from(10)),
            OrderKind::Limit,
            gtc(),
            MarginSideEffect::AutoBorrowRepay,
        )
        .expect("build");
        assert_eq!(p.is_isolated.as_deref(), Some("FALSE"));
        assert_eq!(p.side.as_str(), "SELL");
    }

    #[test]
    fn side_effect_and_auto_repay_gating() {
        // AutoBorrowRepay → AUTO_BORROW_REPAY + auto_repay_at_cancel(true).
        let auto = params(
            Side::Buy,
            Some(Decimal::from(1)),
            OrderKind::Limit,
            gtc(),
            MarginSideEffect::AutoBorrowRepay,
        )
        .expect("build");
        assert_eq!(auto.side_effect_type.as_deref(), Some("AUTO_BORROW_REPAY"));
        assert_eq!(auto.auto_repay_at_cancel, Some(true));

        // NoBorrow → NO_SIDE_EFFECT and NO auto_repay_at_cancel (no loan to repay).
        let no_borrow = params(
            Side::Buy,
            Some(Decimal::from(1)),
            OrderKind::Limit,
            gtc(),
            MarginSideEffect::NoBorrow,
        )
        .expect("build");
        assert_eq!(
            no_borrow.side_effect_type.as_deref(),
            Some("NO_SIDE_EFFECT")
        );
        assert_eq!(no_borrow.auto_repay_at_cancel, None);
    }

    #[test]
    fn new_order_market_has_no_price_or_tif() {
        let p = params(
            Side::Buy,
            None,
            OrderKind::Market,
            gtc(),
            MarginSideEffect::AutoBorrowRepay,
        )
        .expect("build");
        assert_eq!(p.r#type, "MARKET");
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
            MarginSideEffect::AutoBorrowRepay,
        )
        .expect("build");
        assert_eq!(p.r#type, "LIMIT_MAKER");
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
            MarginSideEffect::AutoBorrowRepay,
        )
        .expect("build");
        assert_eq!(stop.r#type, "STOP_LOSS");
        assert_eq!(stop.stop_price, Some(trigger));
        assert_eq!(stop.price, None);

        let stop_limit = params(
            Side::Sell,
            Some(Decimal::from(47_900)),
            OrderKind::StopLimit {
                trigger_price: trigger,
            },
            gtc(),
            MarginSideEffect::AutoBorrowRepay,
        )
        .expect("build");
        assert_eq!(stop_limit.r#type, "STOP_LOSS_LIMIT");
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
            MarginSideEffect::AutoBorrowRepay,
        )
        .expect("build");
        assert_eq!(take_profit.r#type, "TAKE_PROFIT");
        assert_eq!(take_profit.stop_price, Some(trigger));
    }

    #[test]
    fn trailing_kinds_are_rejected() {
        let trailing_stop = params(
            Side::Sell,
            None,
            OrderKind::TrailingStop {
                offset: Decimal::from(100),
                offset_type: TrailingOffsetType::BasisPoints,
            },
            gtc(),
            MarginSideEffect::AutoBorrowRepay,
        );
        assert!(matches!(trailing_stop, Err(BuildOrderError::Unsupported)));

        let trailing_stop_limit = params(
            Side::Sell,
            Some(Decimal::from(100)),
            OrderKind::TrailingStopLimit {
                offset: Decimal::from(100),
                offset_type: TrailingOffsetType::BasisPoints,
                limit_offset: Decimal::from(10),
            },
            gtc(),
            MarginSideEffect::AutoBorrowRepay,
        );
        assert!(matches!(
            trailing_stop_limit,
            Err(BuildOrderError::Unsupported)
        ));
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
                MarginSideEffect::AutoBorrowRepay,
            )
            .expect("build");
            assert_eq!(p.time_in_force.as_ref().map(|t| t.as_str()), Some(expected));
        }
    }

    #[test]
    fn avg_price_from_cumulative_quote_qty() {
        // 100 quote / 4 base = 25.
        assert_eq!(
            margin_avg_price(Some("100"), Decimal::from(4)),
            Some(Decimal::from(25))
        );
        // Zero fill → no average (avoids division by zero).
        assert_eq!(margin_avg_price(Some("100"), Decimal::ZERO), None);
        // Missing / unparseable quote qty → None.
        assert_eq!(margin_avg_price(None, Decimal::from(4)), None);
        assert_eq!(
            margin_avg_price(Some("not-a-number"), Decimal::from(4)),
            None
        );
    }
}
