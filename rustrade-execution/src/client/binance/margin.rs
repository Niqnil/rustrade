//! Binance Cross Margin execution client.
//!
//! [`BinanceMargin`] is a margin counterpart to [`super::spot::BinanceSpot`]. It shares the
//! exchange-agnostic infrastructure in [`super::shared`] (rate-limit tracking, reconnect/backoff,
//! event deduplication, error parsing) and is intended to implement the same `ExecutionClient`
//! trait so callers do not branch on spot-vs-margin transport.
//!
//! ## Scope
//! This module provides the client's identity and configuration ([`BinanceMargin`],
//! [`BinanceMarginConfig`], [`MarginSideEffect`]) plus the REST surface: order submission/cancel
//! and the account snapshot / balance / open-order / trade queries — all currently exposed as
//! inherent methods. The live `userListenToken` user-data stream (`account_stream`) is added in
//! follow-up work; until it lands and the inherent methods are folded into the trait,
//! `BinanceMargin` is not yet a usable `ExecutionClient`. Cross margin only (`isIsolated = "FALSE"`);
//! isolated margin is a separate follow-up.
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
    BINANCE_MAX_TRADES, BinanceOrderType, BinanceTimeInForce, RateLimitTracker,
    classify_order_kind_tif, classify_rest_order_error, connectivity_error, parse_order_kind,
    parse_side, parse_time_in_force, rest_call_with_retry,
};
use crate::{
    AccountSnapshot, InstrumentAccountSnapshot, UnindexedAccountSnapshot,
    balance::{AssetBalance, Balance},
    error::{ApiError, OrderError, UnindexedClientError},
    order::{
        Order, OrderKey, OrderKind, TimeInForce,
        id::{ClientOrderId, OrderId, StrategyId},
        request::{OrderRequestCancel, OrderRequestOpen, UnindexedOrderResponseCancel},
        state::{Cancelled, Filled, Open, OrderState, UnindexedOrderState},
    },
    trade::{AssetFees, Trade, TradeId},
};
use binance_sdk::{
    common::config::{ConfigurationRestApi, ConfigurationWebsocketApi},
    margin_trading::{
        MarginTradingRestApi,
        rest_api::{
            MarginAccountCancelOrderParams, MarginAccountNewOrderNewOrderRespTypeEnum,
            MarginAccountNewOrderParams, MarginAccountNewOrderSideEnum,
            MarginAccountNewOrderTimeInForceEnum, QueryCrossMarginAccountDetailsParams,
            QueryCrossMarginAccountDetailsResponseUserAssetsInner,
            QueryMarginAccountsOpenOrdersParams, QueryMarginAccountsOpenOrdersResponseInner,
            QueryMarginAccountsTradeListParams, QueryMarginAccountsTradeListResponseInner, RestApi,
        },
    },
};
use chrono::{DateTime, TimeZone, Utc};
use rust_decimal::Decimal;
use rustrade_instrument::{
    Side, asset::name::AssetNameExchange, exchange::ExchangeId,
    instrument::name::InstrumentNameExchange,
};
use serde::{Deserialize, Serialize};
use smol_str::{SmolStr, format_smolstr};
use std::{str::FromStr, sync::Arc};
use tracing::{debug, error, warn};

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

    /// Fetch a full cross-margin account snapshot: per-asset balances (incl. debt) plus open
    /// orders for each requested instrument.
    ///
    /// Balances come from `query_cross_margin_account_details` (account-wide `userAssets`,
    /// carrying `borrowed`/`interest` → [`Balance::new_margin`]); open orders are fetched
    /// per-instrument, mirroring [`BinanceSpot::account_snapshot`](super::spot::BinanceSpot).
    /// Cross only (`isIsolated = "FALSE"`).
    ///
    /// Inherent for now; becomes the `ExecutionClient::account_snapshot` body once the
    /// account stream lands and the trait is implemented in full (see [`open_order`](Self::open_order)).
    pub async fn account_snapshot(
        &self,
        assets: &[AssetNameExchange],
        instruments: &[InstrumentNameExchange],
    ) -> Result<UnindexedAccountSnapshot, UnindexedClientError> {
        let response = rest_call_with_retry(&self.rest, &self.rate_limiter, |rest| {
            Box::pin(async move {
                let params = QueryCrossMarginAccountDetailsParams::builder().build()?;
                rest.query_cross_margin_account_details(params).await
            })
        })
        .await
        .map_err(connectivity_error)?;

        let account = response
            .data()
            .await
            .map_err(|e| connectivity_error(e.into()))?;

        let balances =
            filter_and_convert_margin_balances(account.user_assets.unwrap_or_default(), assets);

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
                    instrument,
                )
            }))
            .buffer_unordered(8)
            .map(|result| {
                let (inst, orders) = result?;
                let wrapped = orders
                    .into_iter()
                    .map(|o| Order {
                        key: o.key,
                        side: o.side,
                        price: o.price,
                        quantity: o.quantity,
                        kind: o.kind,
                        time_in_force: o.time_in_force,
                        state: OrderState::active(o.state),
                    })
                    .collect();
                Ok::<_, UnindexedClientError>(InstrumentAccountSnapshot::new(inst, wrapped, None))
            })
            .try_collect()
            .await?;

        Ok(AccountSnapshot::new(
            ExchangeId::BinanceMargin,
            balances,
            instrument_snapshots,
        ))
    }

    /// Fetch current cross-margin balances (incl. `borrowed`/`interest` debt) for the requested
    /// assets. An empty `assets` slice is the "return all" sentinel.
    ///
    /// Inherent for now; becomes the `ExecutionClient::fetch_balances` body in a follow-up.
    pub async fn fetch_balances(
        &self,
        assets: &[AssetNameExchange],
    ) -> Result<Vec<AssetBalance<AssetNameExchange>>, UnindexedClientError> {
        let response = rest_call_with_retry(&self.rest, &self.rate_limiter, |rest| {
            Box::pin(async move {
                let params = QueryCrossMarginAccountDetailsParams::builder().build()?;
                rest.query_cross_margin_account_details(params).await
            })
        })
        .await
        .map_err(connectivity_error)?;

        let account = response
            .data()
            .await
            .map_err(|e| connectivity_error(e.into()))?;

        Ok(filter_and_convert_margin_balances(
            account.user_assets.unwrap_or_default(),
            assets,
        ))
    }

    /// Fetch currently open cross-margin orders, optionally filtered by instrument.
    ///
    /// Honours the `ExecutionClient::fetch_open_orders` contract: an empty `instruments` slice is
    /// the "return all" sentinel, served by a single no-symbol `query_margin_accounts_open_orders`
    /// call returning open orders across every cross-margin instrument (each order's instrument is
    /// taken from its own `symbol` field). A non-empty slice fetches the listed instruments
    /// concurrently, per-symbol. Cross only (`isIsolated = "FALSE"`).
    ///
    /// Inherent for now; becomes the `ExecutionClient::fetch_open_orders` body in a follow-up.
    pub async fn fetch_open_orders(
        &self,
        instruments: &[InstrumentNameExchange],
    ) -> Result<Vec<Order<ExchangeId, InstrumentNameExchange, Open>>, UnindexedClientError> {
        // Empty slice = "return all" sentinel: a single no-symbol query is both correct (the
        // contract requires all instruments) and far cheaper than enumerating every symbol.
        if instruments.is_empty() {
            return fetch_margin_all_open_orders(self.rest.clone(), self.rate_limiter.clone())
                .await;
        }
        use futures::{StreamExt as _, TryStreamExt as _};
        futures::stream::iter(instruments.iter().cloned().map(|instrument| {
            fetch_margin_open_orders_for_instrument(
                self.rest.clone(),
                self.rate_limiter.clone(),
                instrument,
            )
        }))
        .buffer_unordered(8)
        .try_fold(
            Vec::with_capacity(instruments.len()),
            |mut acc: Vec<Order<ExchangeId, InstrumentNameExchange, Open>>, (_, orders)| async move {
                acc.extend(orders);
                Ok(acc)
            },
        )
        .await
    }

    /// Fetch cross-margin trades (fills) since `time_since`, optionally filtered by instrument.
    ///
    /// **Documented deviation from the `ExecutionClient::fetch_trades` "return all" contract:**
    /// Binance's margin trade-list endpoint (`myTrades`) requires a symbol — there is no no-symbol
    /// "all trades" query (unlike open orders). An empty `instruments` slice therefore has nothing
    /// to query and returns an empty `Vec`; callers wanting all trades must enumerate instruments
    /// explicitly. (Open orders *do* honour the sentinel — see [`fetch_open_orders`](Self::fetch_open_orders).)
    ///
    /// Inherent for now; becomes the `ExecutionClient::fetch_trades` body in a follow-up.
    // `.iter().cloned()` is required: Rust async closures cannot satisfy the HRTB needed by the
    // iterator machinery, even when the clone is moved inside the closure body (mirrors spot).
    #[allow(clippy::redundant_iter_cloned)]
    pub async fn fetch_trades(
        &self,
        time_since: DateTime<Utc>,
        instruments: &[InstrumentNameExchange],
    ) -> Result<Vec<Trade<AssetNameExchange, InstrumentNameExchange>>, UnindexedClientError> {
        use futures::StreamExt as _;

        if instruments.is_empty() {
            debug!(
                "BinanceMargin fetch_trades called with empty instruments slice — returning empty result"
            );
            return Ok(Vec::new());
        }
        let start_time_ms = time_since.timestamp_millis();
        let mut all_trades = Vec::new();

        // Binance requires per-symbol queries for trade history. Limit concurrency to avoid
        // bursting Binance's request weight limits.
        let mut stream = futures::stream::iter(instruments.iter().cloned().map(|inst| {
            let rest = self.rest.clone();
            let rate_limiter = self.rate_limiter.clone();
            async move {
                let pages =
                    paginate_margin_my_trades(&rest, &rate_limiter, &inst, start_time_ms).await?;
                Ok::<_, UnindexedClientError>((inst, pages))
            }
        }))
        .buffer_unordered(8);
        while let Some(result) = stream.next().await {
            let (instrument, trades_data) = result?;
            for t in trades_data {
                if let Some(trade) = convert_margin_trade(&t, &instrument) {
                    all_trades.push(trade);
                }
            }
        }

        Ok(all_trades)
    }
}

// ---------------------------------------------------------------------------
// Account query helpers (margin-specific)
// ---------------------------------------------------------------------------

/// Fetch open orders for a single instrument via `query_margin_accounts_open_orders`
/// (cross, `isIsolated = "FALSE"`). Mirrors `BinanceSpot::fetch_open_orders_for_instrument`.
async fn fetch_margin_open_orders_for_instrument(
    rest: Arc<RestApi>,
    rate_limiter: Arc<RateLimitTracker>,
    instrument: InstrumentNameExchange,
) -> Result<
    (
        InstrumentNameExchange,
        Vec<Order<ExchangeId, InstrumentNameExchange, Open>>,
    ),
    UnindexedClientError,
> {
    // Convert once before the retry closure to avoid a String allocation on every retry.
    let symbol_str = instrument.name().to_string();
    let response = rest_call_with_retry(&rest, &rate_limiter, |rest| {
        let sym = symbol_str.clone();
        Box::pin(async move {
            let params = QueryMarginAccountsOpenOrdersParams::builder()
                .symbol(sym)
                .is_isolated("FALSE".to_string())
                .build()?;
            rest.query_margin_accounts_open_orders(params).await
        })
    })
    .await
    .map_err(connectivity_error)?;

    let orders_data = response
        .data()
        .await
        .map_err(|e| connectivity_error(e.into()))?;

    let orders = orders_data
        .into_iter()
        .filter_map(|o| convert_margin_open_order(&o, &instrument))
        .collect();

    Ok((instrument, orders))
}

/// Fetch *all* open cross-margin orders in a single no-symbol `query_margin_accounts_open_orders`
/// call (cross, `isIsolated = "FALSE"`). Backs the [`fetch_open_orders`](BinanceMargin::fetch_open_orders)
/// "return all" sentinel: with no symbol the venue returns orders across every instrument, so each
/// order's instrument is recovered from its own `symbol` field (orders missing it are dropped).
async fn fetch_margin_all_open_orders(
    rest: Arc<RestApi>,
    rate_limiter: Arc<RateLimitTracker>,
) -> Result<Vec<Order<ExchangeId, InstrumentNameExchange, Open>>, UnindexedClientError> {
    let response = rest_call_with_retry(&rest, &rate_limiter, |rest| {
        Box::pin(async move {
            let params = QueryMarginAccountsOpenOrdersParams::builder()
                .is_isolated("FALSE".to_string())
                .build()?;
            rest.query_margin_accounts_open_orders(params).await
        })
    })
    .await
    .map_err(connectivity_error)?;

    let orders_data = response
        .data()
        .await
        .map_err(|e| connectivity_error(e.into()))?;

    let orders = orders_data
        .into_iter()
        .filter_map(|o| convert_margin_open_order_owned_symbol(&o))
        .collect();

    Ok(orders)
}

/// Paginate the margin trade list for a single instrument since `start_time_ms`
/// (cross, `isIsolated = "FALSE"`). Mirrors `BinanceSpot::paginate_my_trades`: cursor-based,
/// first page by `start_time`, subsequent pages by `from_id = last_id + 1` (Binance ignores
/// `start_time` once `from_id` is set), producing a gapless result.
async fn paginate_margin_my_trades(
    rest: &Arc<RestApi>,
    rate_limiter: &Arc<RateLimitTracker>,
    instrument: &InstrumentNameExchange,
    start_time_ms: i64,
) -> Result<Vec<QueryMarginAccountsTradeListResponseInner>, UnindexedClientError> {
    // Convert once before the retry closure to avoid a String allocation on every retry.
    let symbol_str = instrument.name().to_string();
    // The const_assert! in shared bounds BINANCE_MAX_TRADES <= i32::MAX, which trivially fits in
    // i64, so this cast is always lossless. clippy::cast_possible_wrap fires only because usize is
    // the same width as i64 on 64-bit targets (a hypothetical >64-bit usize could wrap); the bound
    // rules that out.
    #[allow(clippy::cast_possible_wrap)]
    let limit = BINANCE_MAX_TRADES as i64;
    let mut all_pages = Vec::new();
    let mut cursor: Option<i64> = None;
    loop {
        let fid = cursor; // Option<i64> is Copy
        let response = rest_call_with_retry(rest, rate_limiter, |rest| {
            let sym = symbol_str.clone();
            let stm = start_time_ms;
            Box::pin(async move {
                let builder = QueryMarginAccountsTradeListParams::builder(sym)
                    .is_isolated("FALSE".to_string())
                    .limit(limit);
                let params = if let Some(id) = fid {
                    builder.from_id(id).build()?
                } else {
                    builder.start_time(stm).build()?
                };
                rest.query_margin_accounts_trade_list(params).await
            })
        })
        .await
        .map_err(connectivity_error)?;

        let page = response
            .data()
            .await
            .map_err(|e| connectivity_error(e.into()))?;

        let page_len = page.len();
        let last_id = page.last().and_then(|t| t.id);
        all_pages.extend(page);

        if page_len < BINANCE_MAX_TRADES {
            break;
        }
        match last_id {
            Some(id) => {
                debug!(%instrument, "BinanceMargin paginate_my_trades: fetching next page ({page_len} results)");
                match id.checked_add(1) {
                    Some(next) => cursor = Some(next),
                    None => break, // saturated at i64::MAX; no further pages possible
                }
            }
            None => {
                warn!(%instrument, "BinanceMargin paginate_my_trades: trade missing ID, stopping pagination");
                break;
            }
        }
    }
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
    now: DateTime<Utc>,
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
        now,
    ))
}

/// Filter cross-margin `userAssets` to the requested assets and convert to [`AssetBalance`]s.
/// An empty `assets` slice returns all. Mirrors spot's `filter_and_convert_balances`.
fn filter_and_convert_margin_balances(
    user_assets: Vec<QueryCrossMarginAccountDetailsResponseUserAssetsInner>,
    assets: &[AssetNameExchange],
) -> Vec<AssetBalance<AssetNameExchange>> {
    let now = Utc::now();
    // Empty assets slice means "return all" — skip building the set entirely.
    if assets.is_empty() {
        return user_assets
            .into_iter()
            .filter_map(|b| convert_margin_balance_entry(b, now))
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
                convert_margin_balance_entry(b, now)
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
            convert_margin_balance_entry(b, now)
        })
        .collect()
}

/// Convert a margin open-order response into rustrade's `Open` state order.
///
/// Field-for-field identical to spot's `convert_open_order` but typed to the margin SDK response
/// and stamped with [`ExchangeId::BinanceMargin`]. Reuses the shared wire-string parsers
/// (`parse_side`/`parse_order_kind`/`parse_time_in_force`); see the duplication rationale in the
/// module's design notes (two clients is below the abstraction threshold).
fn convert_margin_open_order(
    o: &QueryMarginAccountsOpenOrdersResponseInner,
    instrument: &InstrumentNameExchange,
) -> Option<Order<ExchangeId, InstrumentNameExchange, Open>> {
    let order_id_raw = match o.order_id {
        Some(id) => id,
        None => {
            warn!(%instrument, "BinanceMargin open order missing orderId");
            return None;
        }
    };
    let order_id = OrderId(format_smolstr!("{order_id_raw}"));
    if o.client_order_id.is_none() {
        warn!(%instrument, order_id = %order_id_raw, "BinanceMargin open order missing clientOrderId, using orderId as fallback — order may not reconcile with engine state");
    }
    let cid = ClientOrderId::new(
        o.client_order_id
            .as_deref()
            .unwrap_or(&format_smolstr!("{order_id_raw}")),
    );
    let side = match o.side.as_deref() {
        // parse_side already logs a warning on unknown values
        Some(s) => parse_side(s)?,
        None => {
            warn!(%instrument, order_id = %order_id_raw, "BinanceMargin open order missing side");
            return None;
        }
    };
    let price = o.price.as_deref().and_then(|s| Decimal::from_str(s).ok());
    let quantity = match o
        .orig_qty
        .as_deref()
        .and_then(|s| Decimal::from_str(s).ok())
    {
        Some(v) => v,
        None => {
            warn!(%instrument, order_id = %order_id_raw, "BinanceMargin open order missing/unparseable origQty");
            return None;
        }
    };
    let filled_qty = match o.executed_qty.as_deref() {
        Some(s) => match Decimal::from_str(s) {
            Ok(v) => v,
            Err(_) => {
                warn!(%instrument, order_id = %order_id_raw, executed_qty = s, "BinanceMargin open order unparseable executedQty, defaulting to 0");
                Decimal::ZERO
            }
        },
        None => Decimal::ZERO,
    };
    let kind = match o.r#type.as_deref() {
        // parse_order_kind already logs a warning on unknown values
        Some(t) => parse_order_kind(t)?,
        None => {
            warn!(%instrument, order_id = %order_id_raw, "BinanceMargin open order missing type");
            return None;
        }
    };
    let time_in_force = parse_time_in_force(o.time_in_force.as_deref().unwrap_or("GTC"));
    let time_exchange = match o.time.and_then(|ms| Utc.timestamp_millis_opt(ms).single()) {
        Some(ts) => ts,
        None => {
            warn!(%instrument, order_id = %order_id_raw, "BinanceMargin open order missing/unparseable time, using now");
            Utc::now()
        }
    };

    Some(Order {
        key: OrderKey::new(
            ExchangeId::BinanceMargin,
            instrument.clone(),
            // Binance doesn't carry strategy IDs in any response field.
            // Callers must reconcile orders by ClientOrderId or OrderId — never StrategyId.
            StrategyId::unknown(),
            cid,
        ),
        side,
        price,
        quantity,
        kind,
        time_in_force,
        state: Open::new(order_id, time_exchange, filled_qty),
    })
}

/// Convert a margin open-order response whose instrument is recovered from its own `symbol` field,
/// rather than supplied by the caller. Used by the no-symbol "return all" path
/// ([`fetch_margin_all_open_orders`]), where each order may belong to a different instrument.
/// Drops (with a warning) any order missing `symbol`; otherwise delegates to
/// [`convert_margin_open_order`].
fn convert_margin_open_order_owned_symbol(
    o: &QueryMarginAccountsOpenOrdersResponseInner,
) -> Option<Order<ExchangeId, InstrumentNameExchange, Open>> {
    let instrument = match o.symbol.as_deref() {
        Some(s) => InstrumentNameExchange::new(s),
        None => {
            warn!("BinanceMargin open order missing symbol in return-all query, dropping order");
            return None;
        }
    };
    convert_margin_open_order(o, &instrument)
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
    let time_exchange = match t.time.and_then(|ms| Utc.timestamp_millis_opt(ms).single()) {
        Some(ts) => ts,
        None => {
            warn!(%instrument, trade_id = %trade_id_raw, "BinanceMargin trade missing/unparseable time, using now");
            Utc::now()
        }
    };

    // Use actual commission asset from Binance (e.g., BNB, USDT, BTC). fees_quote is None here;
    // the indexer computes it when the fee is in quote or base asset. "UNKNOWN" fallback (rare:
    // API omits commissionAsset) will fail indexing rather than silently misattribute the fee.
    let fee_asset = t
        .commission_asset
        .as_deref()
        .map(AssetNameExchange::from)
        .unwrap_or_else(|| AssetNameExchange::from("UNKNOWN"));

    Some(Trade::new(
        trade_id,
        order_id,
        instrument.clone(),
        StrategyId::unknown(), // Binance doesn't carry strategy IDs
        time_exchange,
        side,
        price,
        quantity,
        AssetFees::new(fee_asset, commission, None),
    ))
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
            filter_and_convert_margin_balances(assets.clone(), &[]).len(),
            3
        );
        // Subset filter → only requested assets.
        let filtered = filter_and_convert_margin_balances(
            assets,
            &[AssetNameExchange::new("BTC"), AssetNameExchange::new("ETH")],
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

        let filtered = filter_and_convert_margin_balances(assets, &requested);
        assert_eq!(filtered.len(), 2);
        assert!(filtered.iter().all(|b| b.asset.name().as_str() != "DOGE"));
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
        o
    }

    #[test]
    fn margin_open_order_converts() {
        let inst = InstrumentNameExchange::new("BTCUSDT");
        let order =
            convert_margin_open_order(&open_order(Some(42), "BUY"), &inst).expect("convert");
        assert_eq!(order.key.exchange, ExchangeId::BinanceMargin);
        assert_eq!(order.state.id.0.as_str(), "42");
        assert_eq!(order.key.cid.0.as_str(), "cid-9");
        assert_eq!(order.side, Side::Buy);
        assert_eq!(order.price, Some(Decimal::from(50_000)));
        assert_eq!(order.quantity, Decimal::from(2));
        assert_eq!(order.state.filled_quantity, Decimal::new(5, 1));
        assert_eq!(order.kind, OrderKind::Limit);
    }

    #[test]
    fn margin_open_order_missing_order_id_is_dropped() {
        let inst = InstrumentNameExchange::new("BTCUSDT");
        assert!(convert_margin_open_order(&open_order(None, "BUY"), &inst).is_none());
    }

    #[test]
    fn margin_open_order_owned_symbol_recovers_instrument() {
        // The no-symbol "return all" path derives the instrument from each order's own `symbol`.
        let mut o = open_order(Some(42), "SELL");
        o.symbol = Some("ETHUSDT".to_string());
        let order = convert_margin_open_order_owned_symbol(&o).expect("convert");
        assert_eq!(order.key.instrument.name().as_str(), "ETHUSDT");
        assert_eq!(order.key.exchange, ExchangeId::BinanceMargin);
        assert_eq!(order.side, Side::Sell);
    }

    #[test]
    fn margin_open_order_owned_symbol_missing_symbol_is_dropped() {
        // open_order() leaves `symbol` unset — the return-all path must drop it rather than guess.
        let o = open_order(Some(42), "BUY");
        assert!(o.symbol.is_none());
        assert!(convert_margin_open_order_owned_symbol(&o).is_none());
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

    #[test]
    fn margin_trade_missing_fields_are_dropped() {
        let inst = InstrumentNameExchange::new("BTCUSDT");
        // Missing id and missing isBuyer both drop the trade rather than guess.
        assert!(convert_margin_trade(&trade(None, Some(true)), &inst).is_none());
        assert!(convert_margin_trade(&trade(Some(11), None), &inst).is_none());
    }
}
