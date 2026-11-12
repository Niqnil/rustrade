//! Binance Spot's `POST /api/v3/order/cancelReplace`: a cancel and a new order in one request.
//!
//! A cancel-replace that partly failed answers with an error status, and the `data` of its error
//! carries each leg's own result: the cancel's details, and the new order's or the cancel's
//! reason for failing. [`LegsAnswer::read`] reads each leg from the SDK's answer, so that each is
//! answered as it would be if sent alone.

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
        errors::ConnectorError,
        models::{RestApiRateLimit, RestApiResponse},
    },
    spot::rest_api::{
        OrderCancelReplaceCancelReplaceModeEnum, OrderCancelReplaceNewOrderRespTypeEnum,
        OrderCancelReplaceParams, OrderCancelReplaceResponse,
        OrderCancelReplaceResponseCancelResponse, OrderCancelReplaceResponseNewOrderResponse,
        OrderCancelReplaceSideEnum, OrderCancelReplaceTimeInForceEnum, OrderCancelReplaceTypeEnum,
    },
};
use rust_decimal::Decimal;
use rustrade_instrument::{Side, exchange::ExchangeId, instrument::name::InstrumentNameExchange};
use serde::Deserialize;
use serde_json::Value;
use smol_str::format_smolstr;
use std::collections::HashMap;
use tracing::warn;

/// A cancel-replace answer that got as far as the legs: a success, a partial failure, or a
/// failure of the cancel.
#[derive(Debug, Clone, Default)]
pub(super) struct LegsAnswer {
    rate_limits: Vec<RestApiRateLimit>,
    /// Named in lowercase.
    headers: HashMap<String, String>,
    /// Each leg's result, `None` when the answer's could not be read.
    results: Option<LegResults>,
}

impl RestAnswer for LegsAnswer {
    fn rate_limits(&self) -> &[RestApiRateLimit] {
        &self.rate_limits
    }

    fn headers(&self) -> &HashMap<String, String> {
        &self.headers
    }
}

impl LegsAnswer {
    /// The legs of the SDK's `answer` to a cancel-replace, or the SDK's error when the venue
    /// refused the whole request.
    ///
    /// # Errors
    /// The SDK's error, unchanged, when it is not one [`failed`](Self::failed) reads legs from.
    pub(super) async fn read(
        answer: anyhow::Result<RestApiResponse<OrderCancelReplaceResponse>>,
    ) -> anyhow::Result<Self> {
        match answer {
            Ok(mut response) => {
                let rate_limits = response.rate_limits.take().unwrap_or_default();
                let headers = std::mem::take(&mut response.headers);
                let decoded = response
                    .data()
                    .await
                    .inspect_err(|e| {
                        warn!(%e, "BinanceSpot cancel-replace answer did not decode; legs unknown");
                    })
                    .ok();
                Ok(Self::succeeded(decoded, rate_limits, headers))
            }
            Err(e) => match Self::failed(&e) {
                Some(answer) => Ok(answer),
                None => Err(e),
            },
        }
    }

    /// A success, whose body decoded as `decoded`, or did not decode when `None`: the venue
    /// answered, so a leg the body does not report readably is unknown.
    fn succeeded(
        decoded: Option<OrderCancelReplaceResponse>,
        rate_limits: Vec<RestApiRateLimit>,
        headers: HashMap<String, String>,
    ) -> Self {
        Self {
            rate_limits,
            headers,
            results: decoded.map(LegResults::from),
        }
    }

    /// The legs of a failed cancel-replace, when the venue got as far as them: a partial failure
    /// (409), or an error that is coded as a cancel-replace failure (`-2021`, `-2022`) or carries
    /// `data`. A leg it does not report readably is answered as unknown. `None` for any other
    /// error, which refused the whole request.
    ///
    /// Its headers, and so the weight and order count it reports, are not kept: the SDK's error
    /// carries neither.
    fn failed(e: &anyhow::Error) -> Option<Self> {
        let error = e.downcast_ref::<ConnectorError>()?;
        let code = error
            .body()
            .and_then(|body| body.get("code"))
            .and_then(Value::as_i64);
        let reports_legs = matches!(error, ConnectorError::ConflictError { .. })
            || error.data().is_some()
            || matches!(code, Some(-2021 | -2022));
        reports_legs.then(|| Self {
            results: error
                .data()
                .and_then(|data| LegResults::deserialize(data).ok()),
            ..Self::default()
        })
    }
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

/// Each leg's result: the body of a success, or the `data` of an error answer.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct LegResults {
    cancel_result: Option<String>,
    new_order_result: Option<String>,
    cancel_response: Option<Leg>,
    new_order_response: Option<Leg>,
}

impl From<OrderCancelReplaceResponse> for LegResults {
    fn from(response: OrderCancelReplaceResponse) -> Self {
        let OrderCancelReplaceResponse {
            cancel_result,
            new_order_result,
            cancel_response,
            new_order_response,
        } = response;
        Self {
            cancel_result,
            new_order_result,
            cancel_response: cancel_response.map(|leg| Leg::from(*leg)),
            new_order_response: new_order_response.map(|leg| Leg::from(*leg)),
        }
    }
}

/// One leg's response: the order's fields when the leg succeeded, Binance's code and message when
/// it failed.
#[derive(Debug, Clone, Deserialize)]
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

impl From<OrderCancelReplaceResponseCancelResponse> for Leg {
    fn from(leg: OrderCancelReplaceResponseCancelResponse) -> Self {
        Self {
            code: None,
            msg: None,
            order_id: leg.order_id,
            transact_time: leg.transact_time,
            status: leg.status,
            executed_qty: leg.executed_qty,
            cummulative_quote_qty: leg.cummulative_quote_qty,
        }
    }
}

impl From<OrderCancelReplaceResponseNewOrderResponse> for Leg {
    fn from(leg: OrderCancelReplaceResponseNewOrderResponse) -> Self {
        Self {
            code: None,
            msg: None,
            order_id: leg.order_id,
            transact_time: leg.transact_time,
            status: leg.status,
            executed_qty: leg.executed_qty,
            cummulative_quote_qty: leg.cummulative_quote_qty,
        }
    }
}

/// Each leg's answer to a cancel-replace: the cancel's, and the new order's state, `None` when the
/// venue did not attempt it.
pub(super) type LegAnswers = (
    Result<Cancelled, UnindexedOrderError>,
    Option<UnindexedOrderState>,
);

/// Each leg's answer, read from `answer`.
///
/// A leg whose result cannot be read is answered with an error whose outcome is unknown, so that
/// the order is looked up rather than taken as cancelled, placed or refused.
pub(super) fn read_legs(
    answer: LegsAnswer,
    instrument: &InstrumentNameExchange,
    quantity: Decimal,
) -> LegAnswers {
    let unreadable = |problem: &str| unreadable_answer(ExchangeId::BinanceSpot, problem);
    let Some(results) = answer.results else {
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
        client::binance::sdk_errors::rest_error,
        error::{ApiError, ConnectivityError, OrderError},
        order::{OrderKind, TimeInForce, state::ActiveOrderState},
    };
    use binance_sdk::common::utils::parse_rate_limit_headers;
    use rust_decimal_macros::dec;
    use serde_json::json;
    use std::time::Duration;

    fn btcusdt() -> InstrumentNameExchange {
        InstrumentNameExchange::new("BTCUSDT")
    }

    /// A success whose body is `body`, decoded as the SDK decodes it.
    fn success(body: Value) -> LegsAnswer {
        LegsAnswer::succeeded(
            serde_json::from_value(body).ok(),
            Vec::new(),
            HashMap::new(),
        )
    }

    /// The legs of the SDK's error for an answer of `status` with `body`, if it reports any.
    fn failure(status: u16, body: &str) -> Option<LegsAnswer> {
        LegsAnswer::failed(&anyhow::Error::new(rest_error(status, body, &[])))
    }

    /// The legs of the SDK's error for an answer of `status` with the JSON `body`.
    fn failed_legs(status: u16, body: Value) -> LegsAnswer {
        failure(status, &body.to_string()).expect("the failure reports legs")
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

    fn assert_both_unknown((cancel, open): LegAnswers, label: &str) {
        assert_unknown(cancel.as_ref().expect_err(label));
        match open {
            Some(OrderState::Inactive(crate::order::state::InactiveOrderState::OpenFailed(
                error,
            ))) => assert_unknown(&error),
            other => panic!("{label}: expected the new order unknown, got {other:?}"),
        }
    }

    /// A success answers each leg from its response.
    #[test]
    fn a_success_answers_each_leg_from_its_response() {
        let (cancel, open) = read_legs(
            success(json!({
                "cancelResult": "SUCCESS", "newOrderResult": "SUCCESS",
                "cancelResponse": cancelled_leg(), "newOrderResponse": placed_leg(),
            })),
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
            failed_legs(
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
            failed_legs(
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
            failed_legs(
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

    /// A success whose legs cannot be read leaves both outcomes unknown, so the new order is
    /// looked up rather than taken as placed or refused.
    #[test]
    fn a_success_with_unreadable_legs_leaves_both_outcomes_unknown() {
        let bodies = [
            json!({}),
            json!("not an object"),
            json!({
                "cancelResult": "SUCCESS", "newOrderResult": "SUCCESS",
                "cancelResponse": {"status": "CANCELED"}, "newOrderResponse": {"status": "NEW"},
            }),
            json!({ "cancelResult": "PENDING", "newOrderResult": "PENDING" }),
        ];
        for body in bodies {
            let label = body.to_string();
            assert_both_unknown(read_legs(success(body), &btcusdt(), dec!(1)), &label);
        }
    }

    /// A cancel-replace failure whose leg results are missing or garbled still reports legs, and
    /// leaves both outcomes unknown: the venue got as far as the legs, so neither is refused.
    #[test]
    fn a_failure_without_readable_leg_results_leaves_both_legs_unknown() {
        let bodies = [
            (
                409,
                json!({"code": -2021, "msg": "Order cancel-replace partially failed."}).to_string(),
            ),
            (409, json!({"msg": "no code"}).to_string()),
            (
                400,
                json!({"code": -2022, "msg": "Order cancel-replace failed."}).to_string(),
            ),
            (400, json!({"code": -2022, "data": "garbled"}).to_string()),
            (
                400,
                json!({"code": -2021, "data": {"cancelResult": 7}}).to_string(),
            ),
            (
                409,
                json!({
                    "code": -2021,
                    "data": {
                        "cancelResult": "FAILURE", "newOrderResult": "FAILURE",
                        "cancelResponse": {}, "newOrderResponse": {},
                    },
                })
                .to_string(),
            ),
            (409, "<html>".to_string()),
        ];
        for (status, body) in bodies {
            let answer = failure(status, &body).unwrap_or_else(|| panic!("{status} {body}"));
            assert_both_unknown(
                read_legs(answer, &btcusdt(), dec!(1)),
                &format!("{status} {body}"),
            );
        }
    }

    /// Only a partial failure, or an error coded as a cancel-replace failure or carrying leg
    /// results, reports legs. Any other error refused the whole request, and reaches the retry
    /// and the classifiers as the SDK returned it.
    #[test]
    fn a_failure_reports_legs_for_a_409_a_leg_code_or_leg_data() {
        let reports = |status, body: Value| failure(status, &body.to_string()).is_some();
        assert!(reports(409, json!({"code": -2021, "data": {}})));
        assert!(reports(
            400,
            json!({"code": -2022, "msg": "Order cancel-replace failed."})
        ));
        // Any `data` is read leg by leg, whatever the code: a leg it does not report readably is
        // then unknown, never refused.
        assert!(reports(400, json!({"code": -1013, "data": {}})));
        assert!(!reports(
            400,
            json!({"code": -1013, "msg": "Filter failure"})
        ));
        assert!(!reports(
            400,
            json!({"code": -2011, "msg": "Unknown order sent."})
        ));
        assert!(!reports(400, json!({"code": -1013, "data": null})));
        // A throttle refuses the whole request, so it reaches the retry as an error.
        assert!(!reports(
            429,
            json!({"code": -1003, "msg": "Too many requests."})
        ));
        assert!(failure(403, "<html>").is_none());

        let not_the_sdks = anyhow::anyhow!("-2021 not a ConnectorError");
        assert!(LegsAnswer::failed(&not_the_sdks).is_none());
    }

    /// The weight an answer reports as used reaches the rate limiter, as a plain REST answer's
    /// does: from its parsed `x-mbx-*` limits for the spot pool, and from its headers for the
    /// SAPI pool.
    #[tokio::test(start_paused = true)]
    async fn an_answers_used_weight_reaches_the_rate_limiter() {
        use super::super::shared::{RateLimitTracker, RequestKind, WeightPool, unix_ms};

        let near_limit = |name: &str| {
            let headers = HashMap::from([(name.to_string(), "1000000".to_string())]);
            LegsAnswer::succeeded(None, parse_rate_limit_headers(&headers), headers)
        };
        let pauses = |pool, answer: &LegsAnswer| {
            let tracker = RateLimitTracker::new(pool);
            tracker.observe_rest(answer, unix_ms());
            async move {
                tokio::time::timeout(
                    Duration::from_millis(1),
                    tracker.wait_if_blocked(RequestKind::Query),
                )
                .await
                .is_err()
            }
        };

        let spot = near_limit("x-mbx-used-weight-1m");
        assert!(!spot.rate_limits.is_empty(), "the header parses as a limit");
        assert!(pauses(WeightPool::Spot, &spot).await);
        assert!(pauses(WeightPool::Sapi, &near_limit("x-sapi-used-ip-weight-1m")).await);
        assert!(!pauses(WeightPool::Sapi, &success(json!({}))).await);
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

    fn params(
        cancel: CancelTarget,
        kind: OrderKind,
        price: Option<Decimal>,
        on_cancel_failure: OnCancelFailure,
    ) -> OrderCancelReplaceParams {
        let fields = SpotOrderFields::new(
            kind,
            TimeInForce::GoodUntilCancelled { post_only: false },
            price,
        )
        .unwrap();
        CancelReplaceRequest {
            symbol: "BTCUSDT",
            cancel,
            cid: ClientOrderId::new("exit"),
            side: Side::Sell,
            quantity: dec!(0.5),
            fields,
            on_cancel_failure,
        }
        .params()
        .unwrap()
    }

    /// How the SDK writes `value` on the wire.
    fn wire<T: serde::Serialize>(value: &T) -> Value {
        serde_json::to_value(value).unwrap()
    }

    /// The parameters carry the new order as a plain open sends it, the order to cancel, the mode
    /// the policy asks for, and a `RESULT` answer, and nothing else.
    ///
    /// The destructuring names every field, so a field the SDK adds fails to compile here and is
    /// looked at.
    #[test]
    fn the_params_carry_both_legs_and_the_policys_mode() {
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
        } = params(
            CancelTarget::OrderId(11),
            OrderKind::Limit,
            Some(dec!(101.5)),
            OnCancelFailure::DropOpens,
        );
        assert_eq!(symbol, "BTCUSDT");
        assert_eq!(wire(&side), json!("SELL"));
        assert_eq!(wire(&r#type), json!("LIMIT"));
        assert_eq!(wire(&cancel_replace_mode), json!("STOP_ON_FAILURE"));
        assert_eq!(wire(&time_in_force), json!("GTC"));
        assert_eq!(quantity, Some(dec!(0.5)));
        assert_eq!(price, Some(dec!(101.5)));
        assert_eq!(cancel_order_id, Some(11));
        assert_eq!(new_client_order_id.as_deref(), Some("exit"));
        assert_eq!(wire(&new_order_resp_type), json!("RESULT"));
        assert!(quote_order_qty.is_none() && stop_price.is_none() && iceberg_qty.is_none());
        assert!(cancel_new_client_order_id.is_none() && cancel_orig_client_order_id.is_none());
        assert!(strategy_id.is_none() && strategy_type.is_none() && trailing_delta.is_none());
        assert!(self_trade_prevention_mode.is_none() && cancel_restrictions.is_none());
        assert!(order_rate_limit_exceeded_mode.is_none() && recv_window.is_none());
        assert!(peg_price_type.is_none() && peg_offset_value.is_none());
        assert!(peg_offset_type.is_none());

        let stop = params(
            CancelTarget::ClientOrderId(ClientOrderId::new("stop")),
            OrderKind::Stop {
                trigger_price: dec!(95),
            },
            None,
            OnCancelFailure::SendOpens,
        );
        assert_eq!(wire(&stop.cancel_replace_mode), json!("ALLOW_FAILURE"));
        assert_eq!(stop.cancel_orig_client_order_id.as_deref(), Some("stop"));
        assert_eq!(wire(&stop.r#type), json!("STOP_LOSS"));
        assert_eq!(stop.stop_price, Some(dec!(95)));
        assert!(stop.cancel_order_id.is_none());
        assert!(stop.price.is_none());
        assert!(stop.time_in_force.is_none());
    }

    /// A trailing order's delta and activation price reach the parameters.
    #[test]
    fn the_params_carry_a_trailing_orders_delta() {
        let trailing = params(
            CancelTarget::OrderId(11),
            OrderKind::TrailingStop {
                offset: dec!(1),
                offset_type: crate::order::TrailingOffsetType::Percentage,
                activation_price: Some(dec!(90)),
            },
            None,
            OnCancelFailure::DropOpens,
        );
        assert_eq!(trailing.trailing_delta, Some(100));
        assert_eq!(trailing.stop_price, Some(dec!(90)));
    }
}
