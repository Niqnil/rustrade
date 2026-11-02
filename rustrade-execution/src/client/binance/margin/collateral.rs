//! Binance cross margin's collateral ratios: how much of each asset's value counts as collateral,
//! read from `GET /sapi/v1/margin/crossMarginCollateralRatio`.

use super::BinanceMargin;
use crate::{
    client::binance::shared::{
        RequestKind, classify_rest_query_error, optional_decimal, response_decode_error,
        rest_call_with_retry,
    },
    error::UnindexedClientError,
};
use binance_sdk::margin_trading::rest_api::{
    CrossMarginCollateralRatioResponseInner,
    CrossMarginCollateralRatioResponseInnerCollateralsInner,
};
use rust_decimal::Decimal;
use rustrade_instrument::asset::name::AssetNameExchange;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, hash_map::Entry};

/// One tier of a Binance cross-margin collateral ratio: the ratio applied to an asset's value in
/// a range of USD values.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CollateralRatioTier {
    /// The lowest USD value the tier covers (Binance's `minUsdValue`).
    pub min_usd_value: Decimal,
    /// The highest USD value the tier covers (Binance's `maxUsdValue`). `None` when Binance sends
    /// none, as it does for the open top tier.
    pub max_usd_value: Option<Decimal>,
    /// The fraction of the asset's value counted as collateral in this tier: 1 counts all of it,
    /// 0.95 counts 95% (Binance's `discountRate`). Reported as Binance sends it, unchecked.
    pub ratio: Decimal,
}

impl CollateralRatioTier {
    /// A tier with these bounds and ratio.
    pub fn new(min_usd_value: Decimal, max_usd_value: Option<Decimal>, ratio: Decimal) -> Self {
        Self {
            min_usd_value,
            max_usd_value,
            ratio,
        }
    }
}

impl BinanceMargin {
    /// The collateral ratio tiers of every asset on Binance cross margin:
    /// `GET /sapi/v1/margin/crossMarginCollateralRatio`.
    ///
    /// Each asset maps to its tiers in ascending
    /// [`min_usd_value`](CollateralRatioTier::min_usd_value). Binance groups assets that share a
    /// tier list; each asset of a group gets the group's tiers. An asset Binance does not list is
    /// absent.
    ///
    /// The ratios are Binance's for cross margin, read whichever margin this client is
    /// configured for. They are market data, not this account's: the request carries the API key
    /// and is not signed.
    ///
    /// # What the answer holds
    ///
    /// Binance publishes three collateral ratios per asset on its Margin Data page (borrow,
    /// transfer-out and liquidation), and the endpoint returns one. Binance does not document
    /// which, nor how an asset with several tiers is weighted across them. What was observed on
    /// 2026-11-02:
    ///
    /// - every asset had one tier with a ratio of 1, matching the page's liquidation ratios
    ///   (100% for every asset) and not its borrow ratios (as low as 10%);
    /// - that tier had no `maxUsdValue`, so it is reported with
    ///   [`max_usd_value`](CollateralRatioTier::max_usd_value) `None`.
    ///
    /// Read the answer as observed, not guaranteed. Binance's API documents no endpoint for the
    /// borrow ratios, which govern how much the account may borrow.
    ///
    /// To model these ratios on the simulated venue, a single-tier ratio as observed above maps to
    /// [`SpotMarginConfig::with_liquidation_collateral_ratio`]. Take the borrow ratios
    /// ([`SpotMarginConfig::with_borrow_collateral_ratio`]) from Binance's Margin Data page, not
    /// from this answer: they can be far lower, and using this answer for them would let a backtest
    /// borrow more than Binance lends.
    ///
    /// [`SpotMarginConfig::with_liquidation_collateral_ratio`]:
    ///     crate::margin::SpotMarginConfig::with_liquidation_collateral_ratio
    /// [`SpotMarginConfig::with_borrow_collateral_ratio`]:
    ///     crate::margin::SpotMarginConfig::with_borrow_collateral_ratio
    ///
    /// # Cost
    ///
    /// One request, costing 100 of Binance's per-IP request weight.
    ///
    /// # Errors
    ///
    /// - Whatever error the request returns.
    /// - [`UnindexedClientError::Internal`] for an answer that cannot be read whole, rather than
    ///   return a map that silently lacks its assets or tiers: a group missing its assets or its
    ///   tiers, a group with no tiers, a tier missing its `minUsdValue` or `discountRate`, a value
    ///   that does not read as a decimal, or an asset listed twice.
    pub async fn fetch_collateral_ratios(
        &self,
    ) -> Result<HashMap<AssetNameExchange, Vec<CollateralRatioTier>>, UnindexedClientError> {
        let response =
            rest_call_with_retry(&self.rest, &self.rate_limiter, RequestKind::Query, |rest| {
                Box::pin(async move { rest.cross_margin_collateral_ratio().await })
            })
            .await
            .map_err(|e| classify_rest_query_error(&e, None))?;
        collateral_ratios(response.data().await.map_err(response_decode_error)?)
    }
}

/// Each asset of `groups` with its group's tiers, in ascending minimum USD value.
fn collateral_ratios(
    groups: Vec<CrossMarginCollateralRatioResponseInner>,
) -> Result<HashMap<AssetNameExchange, Vec<CollateralRatioTier>>, UnindexedClientError> {
    let mut ratios = HashMap::new();
    for (index, group) in groups.into_iter().enumerate() {
        let assets = group
            .asset_names
            .ok_or_else(|| decode_error(format!("group {index}: assetNames missing")))?;
        let tiers = group
            .collaterals
            .ok_or_else(|| decode_error(format!("group {index}: collaterals missing")))?;
        if tiers.is_empty() {
            return Err(decode_error(format!("group {index}: no collaterals")));
        }
        let mut tiers = tiers
            .into_iter()
            .map(|tier| collateral_ratio_tier(index, tier))
            .collect::<Result<Vec<_>, _>>()?;
        tiers.sort_by_key(|tier| tier.min_usd_value);
        for asset in assets {
            match ratios.entry(AssetNameExchange::new(asset)) {
                Entry::Vacant(entry) => {
                    entry.insert(tiers.clone());
                }
                Entry::Occupied(entry) => {
                    return Err(decode_error(format!("{} listed twice", entry.key())));
                }
            }
        }
    }
    Ok(ratios)
}

/// The tier `tier` of group `index`. Its minimum and ratio are required, its maximum is not.
fn collateral_ratio_tier(
    index: usize,
    tier: CrossMarginCollateralRatioResponseInnerCollateralsInner,
) -> Result<CollateralRatioTier, UnindexedClientError> {
    let required = |field: &str, value: Option<String>| {
        let field = format!("group {index}: {field}");
        optional_decimal(&field, value.as_deref())?
            .ok_or_else(|| decode_error(format!("{field} missing")))
    };
    Ok(CollateralRatioTier {
        min_usd_value: required("minUsdValue", tier.min_usd_value)?,
        max_usd_value: optional_decimal(
            &format!("group {index}: maxUsdValue"),
            tier.max_usd_value.as_deref(),
        )?,
        ratio: required("discountRate", tier.discount_rate)?,
    })
}

fn decode_error(detail: String) -> UnindexedClientError {
    UnindexedClientError::Internal(format!("decoding Binance response: {detail}"))
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::{super::tests::margin_client_at, *};
    use crate::error::ApiError;
    use rust_decimal_macros::dec;

    const ROUTE: &str = "/sapi/v1/margin/crossMarginCollateralRatio";

    fn groups(body: serde_json::Value) -> Vec<CrossMarginCollateralRatioResponseInner> {
        serde_json::from_value(body).unwrap()
    }

    fn asset(name: &str) -> AssetNameExchange {
        AssetNameExchange::new(name)
    }

    /// Two groups: one with the single open tier Binance was seen to send, one with three tiers
    /// listed out of order.
    fn tiered() -> serde_json::Value {
        serde_json::json!([
            {
                "collaterals": [{"minUsdValue": "0", "discountRate": "1"}],
                "assetNames": ["USDT", "BTC"]
            },
            {
                "collaterals": [
                    {"minUsdValue": "5000", "maxUsdValue": "10000", "discountRate": "0.5"},
                    {"minUsdValue": "10000", "discountRate": "0"},
                    {"minUsdValue": "0", "maxUsdValue": "5000", "discountRate": "0.9"}
                ],
                "assetNames": ["SHIB"]
            }
        ])
    }

    #[test]
    fn collateral_ratios_give_each_asset_its_groups_tiers_in_ascending_order() {
        let ratios = collateral_ratios(groups(tiered())).unwrap();
        let open = vec![CollateralRatioTier::new(dec!(0), None, dec!(1))];
        assert_eq!(
            ratios,
            HashMap::from([
                (asset("USDT"), open.clone()),
                (asset("BTC"), open),
                (
                    asset("SHIB"),
                    vec![
                        CollateralRatioTier::new(dec!(0), Some(dec!(5000)), dec!(0.9)),
                        CollateralRatioTier::new(dec!(5000), Some(dec!(10000)), dec!(0.5)),
                        CollateralRatioTier::new(dec!(10000), None, dec!(0)),
                    ]
                ),
            ])
        );
    }

    #[test]
    fn collateral_ratios_of_no_groups_or_a_group_of_no_assets_are_empty() {
        assert!(collateral_ratios(Vec::new()).unwrap().is_empty());
        let body = serde_json::json!([{
            "collaterals": [{"minUsdValue": "0", "discountRate": "1"}],
            "assetNames": []
        }]);
        assert!(collateral_ratios(groups(body)).unwrap().is_empty());
    }

    #[test]
    fn collateral_ratios_refuse_an_answer_they_cannot_read_whole() {
        let mut cases = Vec::new();
        for field in ["assetNames", "collaterals"] {
            let mut body = tiered();
            body[1].as_object_mut().unwrap().remove(field);
            cases.push((format!("group 1: {field} missing"), body));
        }
        let mut body = tiered();
        body[1]["collaterals"] = serde_json::json!([]);
        cases.push(("group 1: no collaterals".to_string(), body));
        for field in ["minUsdValue", "discountRate"] {
            let mut body = tiered();
            body[1]["collaterals"][0]
                .as_object_mut()
                .unwrap()
                .remove(field);
            cases.push((format!("group 1: {field} missing"), body));
        }
        for field in ["minUsdValue", "maxUsdValue", "discountRate"] {
            let mut body = tiered();
            body[1]["collaterals"][0][field] = serde_json::json!("0.5.0");
            cases.push((format!("group 1: {field} \"0.5.0\""), body));
        }
        let mut body = tiered();
        body[1]["assetNames"] = serde_json::json!(["SHIB", "BTC"]);
        cases.push(("BTC listed twice".to_string(), body));
        let mut body = tiered();
        body[1]["assetNames"] = serde_json::json!(["SHIB", "SHIB"]);
        cases.push(("SHIB listed twice".to_string(), body));

        for (expected, body) in cases {
            match collateral_ratios(groups(body)) {
                Err(UnindexedClientError::Internal(message)) => {
                    assert!(message.contains(&expected), "{expected}: {message}");
                }
                other => panic!("{expected}: {other:?}"),
            }
        }
    }

    #[tokio::test]
    async fn fetch_collateral_ratios_reads_the_endpoint_once_with_the_api_key_unsigned() {
        for is_isolated in [false, true] {
            let server = wiremock::MockServer::start().await;
            wiremock::Mock::given(wiremock::matchers::method("GET"))
                .and(wiremock::matchers::path(ROUTE))
                .and(wiremock::matchers::header("X-MBX-APIKEY", "key"))
                .respond_with(wiremock::ResponseTemplate::new(200).set_body_json(tiered()))
                .expect(1)
                .mount(&server)
                .await;
            let client = margin_client_at(&server, is_isolated);

            let ratios = client.fetch_collateral_ratios().await.unwrap();
            assert_eq!(ratios, collateral_ratios(groups(tiered())).unwrap());
            let requests = server.received_requests().await.unwrap();
            let [request] = requests.as_slice() else {
                panic!("{requests:?}");
            };
            assert!(
                !request.url.query_pairs().any(|(key, _)| key == "signature"),
                "unsigned: {}",
                request.url
            );
        }
    }

    #[tokio::test]
    async fn fetch_collateral_ratios_reports_a_refusal_and_an_unreadable_answer() {
        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::path(ROUTE))
            .respond_with(
                wiremock::ResponseTemplate::new(400)
                    .set_body_json(serde_json::json!({"code": -1102, "msg": "refused"})),
            )
            .up_to_n_times(1)
            .mount(&server)
            .await;
        wiremock::Mock::given(wiremock::matchers::path(ROUTE))
            .respond_with(
                wiremock::ResponseTemplate::new(200).set_body_json(serde_json::json!([{
                    "collaterals": [{"minUsdValue": "0", "discountRate": "one"}],
                    "assetNames": ["BTC"]
                }])),
            )
            .mount(&server)
            .await;
        let client = margin_client_at(&server, false);

        let refused = client.fetch_collateral_ratios().await;
        assert!(
            matches!(
                refused,
                Err(UnindexedClientError::Api(ApiError::RequestRejected(_)))
            ),
            "{refused:?}"
        );
        match client.fetch_collateral_ratios().await {
            Err(UnindexedClientError::Internal(message)) => {
                assert!(message.contains("discountRate \"one\""), "{message}");
            }
            other => panic!("{other:?}"),
        }
    }
}
