//! Binance Spot's `POST /api/v3/order/cancelReplace`: a cancel and a new order in one request.
//!
//! Sent past `binance-sdk`, through [`SignedRest`], because the SDK keeps only the `code` and
//! `msg` of an error answer, on REST and on the WS API alike. A cancel-replace that partly failed
//! answers with an error status, and its `data` carries each leg's own result: the cancel's
//! details, and the new order's or the cancel's reason for failing. Without it, neither leg could
//! be answered as it would be if sent alone.

use super::{
    shared::{
        BinanceOrderType, BinanceTimeInForce, PlacementResponse, RestAnswer, answer_time,
        binance_filled_qty, order_rejection, placed_order_state, unreadable_answer,
    },
    spot::SpotOrderFields,
};
use crate::{
    error::UnindexedOrderError,
    order::{
        id::{ClientOrderId, OrderId, VenueOrderId},
        request::OnCancelFailure,
        state::{Cancelled, OrderState, UnindexedOrderState},
    },
};
use binance_sdk::{
    common::{
        config::TimestampProvider,
        errors::ConnectorError,
        models::RestApiRateLimit,
        utils::{
            SignatureGenerator, build_user_agent, get_timestamp_from, parse_rate_limit_headers,
        },
    },
    spot::rest_api::{
        OrderCancelReplaceCancelReplaceModeEnum, OrderCancelReplaceNewOrderRespTypeEnum,
        OrderCancelReplaceParams, OrderCancelReplaceSideEnum, OrderCancelReplaceTimeInForceEnum,
        OrderCancelReplaceTypeEnum,
    },
};
use rust_decimal::Decimal;
use rustrade_instrument::{Side, exchange::ExchangeId, instrument::name::InstrumentNameExchange};
use serde::Deserialize;
use serde_json::{Value, json};
use smol_str::format_smolstr;
use std::{collections::BTreeMap, collections::HashMap, time::Duration};

/// The cancel-replace endpoint's path.
pub(super) const CANCEL_REPLACE_PATH: &str = "/api/v3/order/cancelReplace";

/// Signed REST requests sent past `binance-sdk`, answered with their whole body whatever the
/// status.
///
/// Signs as the SDK does, with the SDK's own [`SignatureGenerator`] and the client's timestamp
/// provider, so a request is stamped on the same clock as every other.
pub(super) struct SignedRest {
    client: reqwest::Client,
    base_url: String,
    api_key: String,
    signer: SignatureGenerator,
    timestamp: Option<TimestampProvider>,
    user_agent: String,
}

impl std::fmt::Debug for SignedRest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The key and the signer are left out: they are credentials.
        f.debug_struct("SignedRest")
            .field("base_url", &self.base_url)
            .finish_non_exhaustive()
    }
}

impl SignedRest {
    /// Requests to `base_url`, signed with `api_secret` under `api_key`, each bounded by
    /// `timeout`.
    ///
    /// # Panics
    /// If the HTTP client cannot be built, which happens only when its TLS backend cannot start.
    #[allow(clippy::expect_used)] // Documented panic: built once at startup, as the SDK's client is.
    pub(super) fn new(
        base_url: String,
        api_key: String,
        api_secret: String,
        timestamp: Option<TimestampProvider>,
        timeout: Duration,
    ) -> Self {
        Self {
            client: reqwest::Client::builder()
                .timeout(timeout)
                .build()
                .expect("failed to build the Binance REST client"),
            base_url,
            api_key,
            signer: SignatureGenerator::new(Some(api_secret), None, None),
            timestamp,
            user_agent: build_user_agent("spot"),
        }
    }

    /// Send `query` to `path` with `method`, signed, and return the answer whatever its status.
    ///
    /// # Errors
    /// A [`ConnectorError::ConnectorClientError`] with no code when the request did not complete
    /// or its body could not be read, worded as `binance-sdk` words those failures, so that the
    /// classifiers read them as transport failures. Whether the venue acted on it is unknown.
    pub(super) async fn send(
        &self,
        method: reqwest::Method,
        path: &str,
        mut query: BTreeMap<String, Value>,
    ) -> anyhow::Result<RawAnswer> {
        let transport = |msg: String| ConnectorError::ConnectorClientError { msg, code: None };
        query.insert(
            "timestamp".to_string(),
            json!(get_timestamp_from(self.timestamp.as_ref())),
        );
        let signature = self.signer.get_signature(&query, None)?;

        let mut url = reqwest::Url::parse(&self.base_url)?.join(path)?;
        {
            let mut pairs = url.query_pairs_mut();
            for (key, value) in &query {
                match value {
                    Value::String(value) => pairs.append_pair(key, value),
                    value => pairs.append_pair(key, &value.to_string()),
                };
            }
            pairs.append_pair("signature", &signature);
        }

        let response = self
            .client
            .request(method, url)
            .header("X-MBX-APIKEY", &self.api_key)
            .header(reqwest::header::USER_AGENT, &self.user_agent)
            .send()
            .await
            .map_err(|e| transport(format!("HTTP request failed: {:#}", anyhow::Error::new(e))))?;

        let status = response.status().as_u16();
        let headers: HashMap<String, String> = response
            .headers()
            .iter()
            .filter_map(|(name, value)| {
                Some((
                    name.as_str().to_ascii_lowercase(),
                    value.to_str().ok()?.to_owned(),
                ))
            })
            .collect();
        let rate_limits = parse_rate_limit_headers(&headers);
        let body = response
            .text()
            .await
            .map_err(|e| transport(format!("Failed to get response bytes: {e}")))?;
        Ok(RawAnswer {
            status,
            headers,
            rate_limits,
            body,
        })
    }
}

/// A REST answer read whole, whatever its status.
#[derive(Debug, Clone)]
pub(super) struct RawAnswer {
    pub(super) status: u16,
    /// Named in lowercase.
    pub(super) headers: HashMap<String, String>,
    pub(super) rate_limits: Vec<RestApiRateLimit>,
    pub(super) body: String,
}

impl RestAnswer for RawAnswer {
    fn rate_limits(&self) -> &[RestApiRateLimit] {
        &self.rate_limits
    }

    fn headers(&self) -> &HashMap<String, String> {
        &self.headers
    }
}

impl RawAnswer {
    /// Whether the venue answered the cancel-replace leg by leg: a success, or an error whose
    /// `data` reports each leg. Any other answer refused the whole request.
    pub(super) fn reports_legs(&self) -> bool {
        (200..300).contains(&self.status) || refusal(&self.body).is_some_and(|r| r.data.is_some())
    }

    /// The error `binance-sdk` would have returned for this answer, so that the client's
    /// classifiers, which read the SDK's errors, read this one alike.
    pub(super) fn into_connector_error(self) -> ConnectorError {
        let (msg, code) = match refusal(&self.body) {
            Some(Refusal {
                msg: Some(msg),
                code,
                ..
            }) => (msg, code),
            Some(Refusal { code, .. }) => (self.body, code),
            None => (self.body, None),
        };
        let retry_after = || {
            self.headers
                .get("retry-after")
                .and_then(|secs| secs.trim().parse().ok())
        };
        match self.status {
            400 => ConnectorError::BadRequestError { msg, code },
            401 => ConnectorError::UnauthorizedError { msg, code },
            403 => ConnectorError::ForbiddenError { msg, code },
            404 => ConnectorError::NotFoundError { msg, code },
            418 => ConnectorError::RateLimitBanError {
                msg,
                code,
                retry_after: retry_after(),
            },
            429 => ConnectorError::TooManyRequestsError {
                msg,
                code,
                retry_after: retry_after(),
            },
            status @ 500..=599 => ConnectorError::ServerError {
                msg: format!("Server error: {status}"),
                status_code: Some(status),
            },
            _ => ConnectorError::ConnectorClientError { msg, code },
        }
    }
}

/// The query of a cancel-replace, from the SDK's own parameters.
///
/// The destructuring names every field, so a field the SDK adds fails to compile here rather than
/// go unsent.
pub(super) fn cancel_replace_query(params: OrderCancelReplaceParams) -> BTreeMap<String, Value> {
    let OrderCancelReplaceParams {
        symbol,
        side,
        r#type,
        cancel_replace_mode,
        time_in_force,
        quantity,
        quote_order_qty,
        price,
        cancel_new_client_order_id,
        cancel_orig_client_order_id,
        cancel_order_id,
        new_client_order_id,
        strategy_id,
        strategy_type,
        stop_price,
        trailing_delta,
        iceberg_qty,
        new_order_resp_type,
        self_trade_prevention_mode,
        cancel_restrictions,
        order_rate_limit_exceeded_mode,
        peg_price_type,
        peg_offset_value,
        peg_offset_type,
        recv_window,
    } = params;

    let mut query = BTreeMap::new();
    let mut insert = |key: &str, value: Option<Value>| {
        if let Some(value) = value {
            query.insert(key.to_string(), value);
        }
    };
    insert("symbol", Some(json!(symbol)));
    insert("side", Some(json!(side)));
    insert("type", Some(json!(r#type)));
    insert("cancelReplaceMode", Some(json!(cancel_replace_mode)));
    insert("timeInForce", time_in_force.map(|v| json!(v)));
    insert("quantity", quantity.map(|v| json!(v)));
    insert("quoteOrderQty", quote_order_qty.map(|v| json!(v)));
    insert("price", price.map(|v| json!(v)));
    insert(
        "cancelNewClientOrderId",
        cancel_new_client_order_id.map(|v| json!(v)),
    );
    insert(
        "cancelOrigClientOrderId",
        cancel_orig_client_order_id.map(|v| json!(v)),
    );
    insert("cancelOrderId", cancel_order_id.map(|v| json!(v)));
    insert("newClientOrderId", new_client_order_id.map(|v| json!(v)));
    insert("strategyId", strategy_id.map(|v| json!(v)));
    insert("strategyType", strategy_type.map(|v| json!(v)));
    insert("stopPrice", stop_price.map(|v| json!(v)));
    insert("trailingDelta", trailing_delta.map(|v| json!(v)));
    insert("icebergQty", iceberg_qty.map(|v| json!(v)));
    insert("newOrderRespType", new_order_resp_type.map(|v| json!(v)));
    insert(
        "selfTradePreventionMode",
        self_trade_prevention_mode.map(|v| json!(v)),
    );
    insert("cancelRestrictions", cancel_restrictions.map(|v| json!(v)));
    insert(
        "orderRateLimitExceededMode",
        order_rate_limit_exceeded_mode.map(|v| json!(v)),
    );
    insert("pegPriceType", peg_price_type.map(|v| json!(v)));
    insert("pegOffsetValue", peg_offset_value.map(|v| json!(v)));
    insert("pegOffsetType", peg_offset_type.map(|v| json!(v)));
    insert("recvWindow", recv_window.map(|v| json!(v)));
    query
}

/// What a cancel-replace carries: the order to cancel, and the new order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct CancelReplaceRequest<'a> {
    pub(super) symbol: &'a str,
    /// The order to cancel, by its venue id when it has one, else by its client order id.
    pub(super) cancel: CancelTarget,
    /// The new order's client order id.
    pub(super) cid: ClientOrderId,
    pub(super) side: Side,
    pub(super) quantity: Decimal,
    pub(super) fields: SpotOrderFields,
    pub(super) on_cancel_failure: OnCancelFailure,
}

/// How a cancel-replace names the order it cancels.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum CancelTarget {
    /// By the venue's order id (`cancelOrderId`), the faster lookup.
    OrderId(i64),
    /// By the client order id it was placed with (`cancelOrigClientOrderId`).
    ClientOrderId(ClientOrderId),
}

impl CancelTarget {
    /// How to name the order `cid`, whose venue id is `id` if known: by that id when it reads as
    /// Binance's numeric id, by `cid` when there is none. `None` for an id that does not read as
    /// a number, which the plain cancel reports and falls back from.
    pub(super) fn new(id: Option<&VenueOrderId>, cid: &ClientOrderId) -> Option<Self> {
        match id.and_then(VenueOrderId::assigned) {
            Some(order_id) => order_id.0.parse().ok().map(Self::OrderId),
            None => Some(Self::ClientOrderId(cid.clone())),
        }
    }
}

impl CancelReplaceRequest<'_> {
    /// The SDK's parameters for this request.
    ///
    /// The mode follows the group's policy: `STOP_ON_FAILURE` places nothing when the cancel
    /// fails, as [`OnCancelFailure::DropOpens`] asks, and `ALLOW_FAILURE` places the order
    /// whatever the cancel's result, as [`OnCancelFailure::SendOpens`] asks. The answer is asked
    /// for as `RESULT`, which carries the new order's status and fill.
    ///
    /// # Errors
    /// The SDK's builder error, which names a missing field.
    pub(super) fn params(&self) -> anyhow::Result<OrderCancelReplaceParams> {
        let side = match self.side {
            Side::Buy => OrderCancelReplaceSideEnum::Buy,
            Side::Sell => OrderCancelReplaceSideEnum::Sell,
        };
        let order_type = match self.fields.order_type {
            BinanceOrderType::Market => OrderCancelReplaceTypeEnum::Market,
            BinanceOrderType::Limit => OrderCancelReplaceTypeEnum::Limit,
            BinanceOrderType::LimitMaker => OrderCancelReplaceTypeEnum::LimitMaker,
            BinanceOrderType::StopLoss => OrderCancelReplaceTypeEnum::StopLoss,
            BinanceOrderType::StopLossLimit => OrderCancelReplaceTypeEnum::StopLossLimit,
            BinanceOrderType::TakeProfit => OrderCancelReplaceTypeEnum::TakeProfit,
            BinanceOrderType::TakeProfitLimit => OrderCancelReplaceTypeEnum::TakeProfitLimit,
        };
        let mode = match self.on_cancel_failure {
            OnCancelFailure::DropOpens => OrderCancelReplaceCancelReplaceModeEnum::StopOnFailure,
            OnCancelFailure::SendOpens => OrderCancelReplaceCancelReplaceModeEnum::AllowFailure,
        };

        // SDK constraint: the builder takes Strings.
        let mut builder =
            OrderCancelReplaceParams::builder(self.symbol.to_string(), side, order_type, mode)
                .quantity(self.quantity)
                .new_client_order_id(self.cid.0.to_string())
                .new_order_resp_type(OrderCancelReplaceNewOrderRespTypeEnum::Result);
        builder = match &self.cancel {
            CancelTarget::OrderId(id) => builder.cancel_order_id(*id),
            CancelTarget::ClientOrderId(cid) => {
                builder.cancel_orig_client_order_id(cid.0.to_string())
            }
        };
        if let Some(price) = self.fields.price {
            builder = builder.price(price);
        }
        if let Some(stop_price) = self.fields.stop_price {
            builder = builder.stop_price(stop_price);
        }
        if let Some(trailing_delta) = self.fields.trailing_delta {
            builder = builder.trailing_delta(i64::from(trailing_delta));
        }
        if let Some(tif) = self.fields.time_in_force {
            builder = builder.time_in_force(match tif {
                BinanceTimeInForce::Gtc => OrderCancelReplaceTimeInForceEnum::Gtc,
                BinanceTimeInForce::Ioc => OrderCancelReplaceTimeInForceEnum::Ioc,
                BinanceTimeInForce::Fok => OrderCancelReplaceTimeInForceEnum::Fok,
            });
        }
        Ok(builder.build()?)
    }
}

/// An error answer: Binance's code and message, and for a cancel-replace that got as far as its
/// legs, each leg's result.
#[derive(Debug, Deserialize)]
struct Refusal {
    code: Option<i64>,
    msg: Option<String>,
    data: Option<LegResults>,
}

/// `body` read as an error answer, if it is one.
fn refusal(body: &str) -> Option<Refusal> {
    serde_json::from_str(body).ok()
}

/// Each leg's result: the body of a success, or the `data` of an error answer.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct LegResults {
    cancel_result: Option<String>,
    new_order_result: Option<String>,
    cancel_response: Option<Leg>,
    new_order_response: Option<Leg>,
}

/// One leg's response: the order's fields when the leg succeeded, Binance's code and message when
/// it failed.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Leg {
    code: Option<i64>,
    msg: Option<String>,
    order_id: Option<i64>,
    transact_time: Option<i64>,
    status: Option<String>,
    executed_qty: Option<String>,
    cummulative_quote_qty: Option<String>,
}

/// Each leg's answer to a cancel-replace: the cancel's, and the new order's state, `None` when the
/// venue did not attempt it.
pub(super) type LegAnswers = (
    Result<Cancelled, UnindexedOrderError>,
    Option<UnindexedOrderState>,
);

/// Each leg's answer, read from `answer`, which [`RawAnswer::reports_legs`].
///
/// A leg whose result cannot be read is answered with an error whose outcome is unknown, so that
/// the order is looked up rather than taken as cancelled, placed or refused.
pub(super) fn read_legs(
    answer: &RawAnswer,
    instrument: &InstrumentNameExchange,
    quantity: Decimal,
) -> LegAnswers {
    let unreadable = |problem: &str| unreadable_answer(ExchangeId::BinanceSpot, problem);
    let results = if (200..300).contains(&answer.status) {
        serde_json::from_str::<LegResults>(&answer.body).ok()
    } else {
        refusal(&answer.body).and_then(|refusal| refusal.data)
    };
    let Some(results) = results else {
        let problem = "cancel-replace answer has no readable leg results";
        return (
            Err(unreadable(problem)),
            Some(OrderState::inactive(unreadable(problem))),
        );
    };

    let leg_error = |leg: Option<&Leg>, which: &str| match leg {
        Some(Leg {
            code: Some(code),
            msg,
            ..
        }) => order_rejection(Some(*code), msg.as_deref().unwrap_or_default(), instrument),
        _ => unreadable(&format!("cancel-replace {which} failed without a code")),
    };

    let cancel = match (results.cancel_result.as_deref(), results.cancel_response) {
        (Some("SUCCESS"), Some(leg)) => match leg.order_id {
            Some(id) => {
                let order_id = OrderId(format_smolstr!("{id}"));
                let filled = binance_filled_qty(
                    ExchangeId::BinanceSpot,
                    &order_id,
                    leg.executed_qty.as_deref(),
                );
                let time = answer_time(
                    ExchangeId::BinanceSpot,
                    instrument,
                    "cancel_replace",
                    leg.transact_time,
                );
                Ok(Cancelled::new(order_id, time, filled))
            }
            None => Err(unreadable(
                "cancel-replace cancel succeeded without an orderId",
            )),
        },
        (Some("FAILURE"), leg) => Err(leg_error(leg.as_ref(), "cancel")),
        (result, _) => Err(unreadable(&format!(
            "cancel-replace cancelResult {result:?} not understood"
        ))),
    };

    let open = match (
        results.new_order_result.as_deref(),
        results.new_order_response,
    ) {
        (Some("SUCCESS"), Some(leg)) => Some(match leg.order_id {
            Some(id) => placed_order_state(
                ExchangeId::BinanceSpot,
                instrument,
                quantity,
                PlacementResponse {
                    status: leg.status.as_deref(),
                    order_id: OrderId(format_smolstr!("{id}")),
                    time_exchange: answer_time(
                        ExchangeId::BinanceSpot,
                        instrument,
                        "cancel_replace",
                        leg.transact_time,
                    ),
                    executed_qty: leg.executed_qty.as_deref(),
                    cumulative_quote_qty: leg.cummulative_quote_qty.as_deref(),
                },
            ),
            None => OrderState::inactive(unreadable(
                "cancel-replace new order succeeded without an orderId",
            )),
        }),
        (Some("FAILURE"), leg) => Some(OrderState::inactive(leg_error(leg.as_ref(), "new order"))),
        (Some("NOT_ATTEMPTED"), _) => None,
        (result, _) => Some(OrderState::inactive(unreadable(&format!(
            "cancel-replace newOrderResult {result:?} not understood"
        )))),
    };

    (cancel, open)
}

/// Each leg's answer to a cancel-replace whose whole request failed with `error`.
///
/// An outcome the failure leaves unknown stays unknown on both legs, so the new order is looked
/// up. A definite refusal processed neither leg: the order to cancel still rests, and the new one
/// was not placed. It is answered as not attempted under [`OnCancelFailure::DropOpens`], since its
/// cancel failed, and as refused for the same reason under [`OnCancelFailure::SendOpens`].
pub(super) fn refused_legs(
    error: UnindexedOrderError,
    on_cancel_failure: OnCancelFailure,
) -> LegAnswers {
    let open = (error.is_outcome_unknown() || on_cancel_failure == OnCancelFailure::SendOpens)
        .then(|| OrderState::inactive(error.clone()));
    (Err(error), open)
}

/// Each leg's answer when the new order was refused before anything was sent, by a check the
/// plain open makes too, so the group goes the way it would have gone sequenced: the cancel alone
/// was sent and answered `cancel`, and the open is answered with `refused`, or as not attempted
/// when the cancel failed under [`OnCancelFailure::DropOpens`].
pub(super) fn open_refused_legs(
    cancel: Result<Cancelled, UnindexedOrderError>,
    refused: UnindexedOrderError,
    on_cancel_failure: OnCancelFailure,
) -> LegAnswers {
    let open = (cancel.is_ok() || on_cancel_failure == OnCancelFailure::SendOpens)
        .then(|| OrderState::inactive(refused));
    (cancel, open)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use crate::{
        error::{ApiError, ConnectivityError, OrderError},
        order::{OrderKind, TimeInForce, state::ActiveOrderState},
    };
    use rust_decimal_macros::dec;

    fn btcusdt() -> InstrumentNameExchange {
        InstrumentNameExchange::new("BTCUSDT")
    }

    fn answer(status: u16, body: Value) -> RawAnswer {
        RawAnswer {
            status,
            headers: HashMap::new(),
            rate_limits: Vec::new(),
            body: body.to_string(),
        }
    }

    fn cancelled_leg() -> Value {
        json!({
            "symbol": "BTCUSDT", "orderId": 11, "transactTime": 1_700_000_000_000_i64,
            "status": "CANCELED", "executedQty": "0.25",
        })
    }

    fn placed_leg() -> Value {
        json!({
            "symbol": "BTCUSDT", "orderId": 12, "transactTime": 1_700_000_000_001_i64,
            "status": "NEW", "executedQty": "0", "cummulativeQuoteQty": "0",
        })
    }

    fn failed_leg(code: i64, msg: &str) -> Value {
        json!({ "code": code, "msg": msg })
    }

    fn assert_cancelled(cancel: &Result<Cancelled, UnindexedOrderError>) {
        let cancelled = cancel.as_ref().expect("the cancel succeeded");
        assert_eq!(cancelled.id, OrderId::new("11"));
        assert_eq!(cancelled.filled_quantity, Some(dec!(0.25)));
    }

    fn assert_placed(open: Option<&UnindexedOrderState>) {
        match open {
            Some(OrderState::Active(ActiveOrderState::Open(open))) => {
                assert_eq!(open.id, VenueOrderId::Assigned(OrderId::new("12")));
            }
            other => panic!("expected a resting order, got {other:?}"),
        }
    }

    fn assert_unknown(error: &UnindexedOrderError) {
        assert!(error.is_outcome_unknown(), "{error:?}");
    }

    /// A success answers each leg from its response.
    #[test]
    fn a_success_answers_each_leg_from_its_response() {
        let (cancel, open) = read_legs(
            &answer(
                200,
                json!({
                    "cancelResult": "SUCCESS", "newOrderResult": "SUCCESS",
                    "cancelResponse": cancelled_leg(), "newOrderResponse": placed_leg(),
                }),
            ),
            &btcusdt(),
            dec!(1),
        );

        assert_cancelled(&cancel);
        assert_placed(open.as_ref());
    }

    /// A partial failure (409) answers the cancel as done and the new order with its own
    /// rejection, read by Binance's code as a plain open's would be.
    #[test]
    fn a_partial_failure_answers_the_new_order_with_its_own_rejection() {
        let (cancel, open) = read_legs(
            &answer(
                409,
                json!({
                    "code": -2021, "msg": "Order cancel-replace partially failed.",
                    "data": {
                        "cancelResult": "SUCCESS", "newOrderResult": "FAILURE",
                        "cancelResponse": cancelled_leg(),
                        "newOrderResponse": failed_leg(-2010, "Account has insufficient balance for requested action."),
                    },
                }),
            ),
            &btcusdt(),
            dec!(1),
        );

        assert_cancelled(&cancel);
        let expected = order_rejection(
            Some(-2010),
            "Account has insufficient balance for requested action.",
            &btcusdt(),
        );
        assert!(matches!(expected, OrderError::Rejected(_)), "{expected:?}");
        assert_eq!(open, Some(OrderState::inactive(expected)));
    }

    /// A failed cancel under `STOP_ON_FAILURE` (400) answers the cancel with its own reason, and
    /// the new order as not attempted.
    #[test]
    fn a_failed_cancel_answers_with_its_reason_and_the_new_order_not_attempted() {
        let (cancel, open) = read_legs(
            &answer(
                400,
                json!({
                    "code": -2022, "msg": "Order cancel-replace failed.",
                    "data": {
                        "cancelResult": "FAILURE", "newOrderResult": "NOT_ATTEMPTED",
                        "cancelResponse": failed_leg(-2011, "Unknown order sent."),
                        "newOrderResponse": null,
                    },
                }),
            ),
            &btcusdt(),
            dec!(1),
        );

        assert_eq!(
            cancel,
            Err(order_rejection(
                Some(-2011),
                "Unknown order sent.",
                &btcusdt()
            ))
        );
        assert_eq!(open, None);
    }

    /// Under `ALLOW_FAILURE`, a failed cancel with a placed order (409) answers each leg alike.
    #[test]
    fn a_failed_cancel_with_a_placed_order_answers_each_leg() {
        let (cancel, open) = read_legs(
            &answer(
                409,
                json!({
                    "code": -2021, "msg": "Order cancel-replace partially failed.",
                    "data": {
                        "cancelResult": "FAILURE", "newOrderResult": "SUCCESS",
                        "cancelResponse": failed_leg(-2011, "Unknown order sent."),
                        "newOrderResponse": placed_leg(),
                    },
                }),
            ),
            &btcusdt(),
            dec!(1),
        );

        assert!(cancel.is_err());
        assert_placed(open.as_ref());
    }

    /// An answer whose legs cannot be read leaves both outcomes unknown, so the new order is
    /// looked up rather than taken as placed or refused.
    #[test]
    fn unreadable_legs_leave_both_outcomes_unknown() {
        let bodies = [
            answer(200, json!({})),
            answer(200, json!("not an object")),
            answer(
                200,
                json!({
                    "cancelResult": "SUCCESS", "newOrderResult": "SUCCESS",
                    "cancelResponse": {"status": "CANCELED"}, "newOrderResponse": {"status": "NEW"},
                }),
            ),
            answer(
                409,
                json!({
                    "code": -2021,
                    "data": {
                        "cancelResult": "FAILURE", "newOrderResult": "FAILURE",
                        "cancelResponse": {}, "newOrderResponse": {},
                    },
                }),
            ),
            answer(
                200,
                json!({ "cancelResult": "PENDING", "newOrderResult": "PENDING" }),
            ),
        ];
        for body in bodies {
            let (cancel, open) = read_legs(&body, &btcusdt(), dec!(1));
            assert_unknown(&cancel.expect_err("the cancel cannot be read"));
            match open {
                Some(OrderState::Inactive(
                    crate::order::state::InactiveOrderState::OpenFailed(error),
                )) => assert_unknown(&error),
                other => panic!("expected an unknown open for {body:?}, got {other:?}"),
            }
        }
    }

    /// A whole request that failed with its outcome unknown leaves the new order unknown too.
    #[test]
    fn a_failure_of_unknown_outcome_leaves_the_new_order_unknown() {
        let error = OrderError::Connectivity(ConnectivityError::Socket("reset".to_string()));
        for policy in [OnCancelFailure::DropOpens, OnCancelFailure::SendOpens] {
            assert_eq!(
                refused_legs(error.clone(), policy),
                (
                    Err(error.clone()),
                    Some(OrderState::inactive(error.clone()))
                )
            );
        }
    }

    /// A whole request definitely refused placed nothing: the new order is not attempted under
    /// `DropOpens`, and refused for the same reason under `SendOpens`.
    #[test]
    fn a_definite_refusal_follows_the_policy() {
        let error = OrderError::Rejected(ApiError::rate_limited(None));
        assert_eq!(
            refused_legs(error.clone(), OnCancelFailure::DropOpens),
            (Err(error.clone()), None)
        );
        assert_eq!(
            refused_legs(error.clone(), OnCancelFailure::SendOpens),
            (Err(error.clone()), Some(OrderState::inactive(error)))
        );
    }

    /// An open refused before sending is answered as when sequenced: refused after a good
    /// cancel, and after a failed one only under `SendOpens`.
    #[test]
    fn an_open_refused_unsent_is_answered_as_when_sequenced() {
        let refused = OrderError::UnsupportedOrderType("refused".to_string());
        let cancelled = Ok(Cancelled::new(OrderId::new("11"), Default::default(), None));
        let failed: Result<Cancelled, UnindexedOrderError> =
            Err(OrderError::Rejected(ApiError::OrderAlreadyFullyFilled));

        for policy in [OnCancelFailure::DropOpens, OnCancelFailure::SendOpens] {
            assert_eq!(
                open_refused_legs(cancelled.clone(), refused.clone(), policy).1,
                Some(OrderState::inactive(refused.clone()))
            );
        }
        assert_eq!(
            open_refused_legs(failed.clone(), refused.clone(), OnCancelFailure::DropOpens),
            (failed.clone(), None)
        );
        assert_eq!(
            open_refused_legs(failed.clone(), refused.clone(), OnCancelFailure::SendOpens),
            (failed, Some(OrderState::inactive(refused)))
        );
    }

    /// Only a success, or an error carrying leg results, reports legs.
    #[test]
    fn only_a_success_or_an_error_with_leg_results_reports_legs() {
        assert!(answer(200, json!({})).reports_legs());
        assert!(answer(409, json!({"code": -2021, "data": {}})).reports_legs());
        assert!(!answer(400, json!({"code": -1013, "msg": "Filter failure"})).reports_legs());
        assert!(
            !RawAnswer {
                body: "<html>".to_string(),
                ..answer(403, json!({}))
            }
            .reports_legs()
        );
    }

    /// An answer refusing the whole request reads as the error `binance-sdk` would return for
    /// its status, carrying Binance's code and message, and a rate limit's `Retry-After`.
    #[test]
    fn a_whole_refusal_reads_as_the_sdks_error_for_its_status() {
        let refusal = |status| answer(status, json!({"code": -1013, "msg": "Filter failure"}));
        let msg = || "Filter failure".to_string();
        let code = Some(-1013);

        assert!(matches!(
            refusal(400).into_connector_error(),
            ConnectorError::BadRequestError { msg: m, code: c } if m == msg() && c == code
        ));
        assert!(matches!(
            refusal(401).into_connector_error(),
            ConnectorError::UnauthorizedError { code: c, .. } if c == code
        ));
        assert!(matches!(
            refusal(403).into_connector_error(),
            ConnectorError::ForbiddenError { code: c, .. } if c == code
        ));
        assert!(matches!(
            refusal(404).into_connector_error(),
            ConnectorError::NotFoundError { code: c, .. } if c == code
        ));
        assert!(matches!(
            refusal(409).into_connector_error(),
            ConnectorError::ConnectorClientError { code: c, .. } if c == code
        ));
        assert!(matches!(
            refusal(503).into_connector_error(),
            ConnectorError::ServerError {
                status_code: Some(503),
                ..
            }
        ));
    }

    /// A rate-limit refusal also carries its `Retry-After`, and a body that is not Binance's
    /// error object is carried as the message, with no code.
    #[test]
    fn a_whole_refusal_carries_its_wait_or_its_raw_body() {
        let refusal = |status| answer(status, json!({"code": -1013, "msg": "Filter failure"}));
        let code = Some(-1013);

        let mut throttled = refusal(429);
        throttled
            .headers
            .insert("retry-after".to_string(), "7".to_string());
        assert!(matches!(
            throttled.into_connector_error(),
            ConnectorError::TooManyRequestsError { retry_after: Some(7), code: c, .. } if c == code
        ));
        let mut banned = refusal(418);
        banned
            .headers
            .insert("retry-after".to_string(), "60".to_string());
        assert!(matches!(
            banned.into_connector_error(),
            ConnectorError::RateLimitBanError {
                retry_after: Some(60),
                ..
            }
        ));

        let codeless = RawAnswer {
            body: "<html>blocked</html>".to_string(),
            ..refusal(403)
        };
        assert!(matches!(
            codeless.into_connector_error(),
            ConnectorError::ForbiddenError { msg: m, code: None } if m == "<html>blocked</html>"
        ));
        let no_msg = answer(400, json!({"code": -1100}));
        assert!(matches!(
            no_msg.into_connector_error(),
            ConnectorError::BadRequestError { msg: m, code: Some(-1100) } if m == r#"{"code":-1100}"#
        ));
    }

    /// The order to cancel is named by its numeric venue id when it has one, by its client order
    /// id otherwise, and not at all when its venue id is not a number.
    #[test]
    fn the_cancel_target_prefers_a_numeric_venue_id() {
        let cid = ClientOrderId::new("stop");
        assert_eq!(
            CancelTarget::new(Some(&VenueOrderId::Assigned(OrderId::new("11"))), &cid),
            Some(CancelTarget::OrderId(11))
        );
        assert_eq!(
            CancelTarget::new(None, &cid),
            Some(CancelTarget::ClientOrderId(cid.clone()))
        );
        assert_eq!(
            CancelTarget::new(Some(&VenueOrderId::ClientAssigned), &cid),
            Some(CancelTarget::ClientOrderId(cid.clone()))
        );
        assert_eq!(
            CancelTarget::new(Some(&VenueOrderId::Assigned(OrderId::new("x1"))), &cid),
            None
        );
    }

    fn request(
        cancel: CancelTarget,
        kind: OrderKind,
        price: Option<Decimal>,
        on_cancel_failure: OnCancelFailure,
    ) -> BTreeMap<String, Value> {
        let fields = SpotOrderFields::new(
            kind,
            TimeInForce::GoodUntilCancelled { post_only: false },
            price,
        )
        .unwrap();
        let request = CancelReplaceRequest {
            symbol: "BTCUSDT",
            cancel,
            cid: ClientOrderId::new("exit"),
            side: Side::Sell,
            quantity: dec!(0.5),
            fields,
            on_cancel_failure,
        };
        cancel_replace_query(request.params().unwrap())
    }

    fn text(query: &BTreeMap<String, Value>, key: &str) -> Option<String> {
        query.get(key).map(|value| match value {
            Value::String(value) => value.clone(),
            value => value.to_string(),
        })
    }

    /// The query carries the new order as a plain open sends it, the order to cancel, the mode
    /// the policy asks for, and a `RESULT` answer.
    #[test]
    fn the_query_carries_both_legs_and_the_policys_mode() {
        let query = request(
            CancelTarget::OrderId(11),
            OrderKind::Limit,
            Some(dec!(101.5)),
            OnCancelFailure::DropOpens,
        );
        let expected = [
            ("symbol", "BTCUSDT"),
            ("side", "SELL"),
            ("type", "LIMIT"),
            ("cancelReplaceMode", "STOP_ON_FAILURE"),
            ("timeInForce", "GTC"),
            ("quantity", "0.5"),
            ("price", "101.5"),
            ("cancelOrderId", "11"),
            ("newClientOrderId", "exit"),
            ("newOrderRespType", "RESULT"),
        ];
        for (key, value) in expected {
            assert_eq!(text(&query, key).as_deref(), Some(value), "{key}");
        }
        assert_eq!(query.len(), expected.len(), "{query:?}");

        let query = request(
            CancelTarget::ClientOrderId(ClientOrderId::new("stop")),
            OrderKind::Stop {
                trigger_price: dec!(95),
            },
            None,
            OnCancelFailure::SendOpens,
        );
        assert_eq!(
            text(&query, "cancelReplaceMode").as_deref(),
            Some("ALLOW_FAILURE")
        );
        assert_eq!(
            text(&query, "cancelOrigClientOrderId").as_deref(),
            Some("stop")
        );
        assert_eq!(text(&query, "type").as_deref(), Some("STOP_LOSS"));
        assert_eq!(text(&query, "stopPrice").as_deref(), Some("95"));
        assert!(!query.contains_key("cancelOrderId"));
        assert!(!query.contains_key("price"));
        assert!(!query.contains_key("timeInForce"));
    }

    /// A trailing order's delta and activation price reach the query.
    #[test]
    fn the_query_carries_a_trailing_orders_delta() {
        let query = request(
            CancelTarget::OrderId(11),
            OrderKind::TrailingStop {
                offset: dec!(1),
                offset_type: crate::order::TrailingOffsetType::Percentage,
                activation_price: Some(dec!(90)),
            },
            None,
            OnCancelFailure::DropOpens,
        );
        assert_eq!(text(&query, "trailingDelta").as_deref(), Some("100"));
        assert_eq!(text(&query, "stopPrice").as_deref(), Some("90"));
    }
}
