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
use rust_decimal::Decimal;
use rustrade_instrument::asset::name::AssetNameExchange;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, hash_map::Entry};

/// The collateral ratio endpoint, joined onto the REST config's base path.
const COLLATERAL_RATIO_PATH: &str = "/sapi/v1/margin/crossMarginCollateralRatio";

/// One tier of a Binance cross-margin collateral ratio: the ratio applied to the slice of an
/// asset's value that falls in a range of USD values.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CollateralRatioTier {
    /// The lowest USD value the tier covers (Binance's `minUsdValue`).
    pub min_usd_value: Decimal,
    /// The highest USD value the tier covers (Binance's `maxUsdValue`). `None` for the open top
    /// tier, which has no upper limit: Binance leaves its `maxUsdValue` out.
    pub max_usd_value: Option<Decimal>,
    /// The fraction of the slice's value counted as collateral in this tier: 1 counts all of it,
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

/// An asset's three Binance cross-margin collateral ratios, one per check, each as its tiers in
/// ascending [`min_usd_value`](CollateralRatioTier::min_usd_value).
///
/// The tiers are progressive: each slice of the asset's USD value is weighted at the ratio of the
/// tier that slice falls in, as income tax brackets are, not the whole value at the ratio of the
/// tier its total reaches.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CollateralRatios {
    /// The ratio for the account's margin level, which decides margin calls and liquidation
    /// (Binance's `collaterals`).
    pub liquidation: Vec<CollateralRatioTier>,
    /// The ratio for how much the account can borrow (Binance's `borrowCollaterals`).
    pub borrow: Vec<CollateralRatioTier>,
    /// The ratio for how much the account can transfer out of margin (Binance's
    /// `withdrawCollaterals`).
    pub transfer_out: Vec<CollateralRatioTier>,
}

impl CollateralRatios {
    /// An asset's ratios with these tiers, each as given.
    pub fn new(
        liquidation: Vec<CollateralRatioTier>,
        borrow: Vec<CollateralRatioTier>,
        transfer_out: Vec<CollateralRatioTier>,
    ) -> Self {
        Self {
            liquidation,
            borrow,
            transfer_out,
        }
    }
}

impl BinanceMargin {
    /// The collateral ratios of every asset on Binance cross margin:
    /// `GET /sapi/v1/margin/crossMarginCollateralRatio`.
    ///
    /// Each asset maps to its three ratios, one per check: liquidation, borrow and transfer-out
    /// (see [`CollateralRatios`]). Binance groups assets that share their ratios; each asset of a
    /// group gets the group's. An asset Binance does not list is absent.
    ///
    /// The ratios are Binance's for cross margin, read whichever margin this client is
    /// configured for. They are market data, not this account's: the request carries the API key
    /// and is not signed.
    ///
    /// # What the answer holds
    ///
    /// Binance's API documentation listed only the liquidation set as of 2026-11-04; Binance has
    /// confirmed that the endpoint returns all three. What was observed on 2026-11-04:
    ///
    /// - every asset had one open tier per check, so its ratio applies to the asset's whole value;
    /// - the liquidation ratio was 1 for every asset;
    /// - the borrow and transfer-out ratios were equal, and matched the Margin Data page: 0.9999
    ///   for the main stablecoins, 0.95 for the largest assets, and as low as 0.1.
    ///
    /// To model these ratios on the simulated venue, an asset's single-tier ratios map to
    /// [`SpotMarginConfig::with_liquidation_collateral_ratio`] and
    /// [`SpotMarginConfig::with_borrow_collateral_ratio`]. The simulated venue checks no
    /// transfer-out, so that ratio has no counterpart there, and it applies one ratio per asset,
    /// so an asset with several tiers has no exact counterpart either.
    ///
    /// The borrow check also values each asset at the lowest (held) or highest (owed) of its
    /// recent prices. Binance publishes no API for those prices nor for the window they span.
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
    ///   return a map that silently lacks its assets, ratios or tiers: a group missing its assets
    ///   or any of its three tier sets, a set with no tiers, a tier missing its `minUsdValue` or
    ///   `discountRate`, a value that does not read as a decimal, or an asset listed twice.
    pub async fn fetch_collateral_ratios(
        &self,
    ) -> Result<HashMap<AssetNameExchange, CollateralRatios>, UnindexedClientError> {
        // Sent through `send_request` so that the answer decodes into `CollateralRatioGroup`: the
        // SDK's response type has no borrow or transfer-out set, so the SDK's own call drops them.
        let response = rest_call_with_retry(
            &self.rest_config,
            &self.rate_limiter,
            RequestKind::Query,
            |cfg| {
                Box::pin(async move {
                    binance_sdk::common::utils::send_request::<Vec<CollateralRatioGroup>>(
                        &cfg,
                        COLLATERAL_RATIO_PATH,
                        reqwest::Method::GET,
                        BTreeMap::new(),
                        BTreeMap::new(),
                        None,
                        false,
                    )
                    .await
                })
            },
        )
        .await
        .map_err(|e| classify_rest_query_error(&e, None))?;
        collateral_ratios(response.data().await.map_err(response_decode_error)?)
    }
}

/// One group of the collateral ratio answer: assets sharing their three tier sets. Every field is
/// optional here so that a missing one is reported by name rather than as a bare decode error.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CollateralRatioGroup {
    asset_names: Option<Vec<String>>,
    collaterals: Option<Vec<CollateralRatioTierAnswer>>,
    borrow_collaterals: Option<Vec<CollateralRatioTierAnswer>>,
    withdraw_collaterals: Option<Vec<CollateralRatioTierAnswer>>,
}

/// One tier of the collateral ratio answer, its values as Binance's decimal strings.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CollateralRatioTierAnswer {
    min_usd_value: Option<String>,
    max_usd_value: Option<String>,
    discount_rate: Option<String>,
}

/// Each asset of `groups` with its group's ratios.
fn collateral_ratios(
    groups: Vec<CollateralRatioGroup>,
) -> Result<HashMap<AssetNameExchange, CollateralRatios>, UnindexedClientError> {
    let mut ratios = HashMap::new();
    for (index, group) in groups.into_iter().enumerate() {
        let assets = group
            .asset_names
            .ok_or_else(|| decode_error(format!("group {index}: assetNames missing")))?;
        let group_ratios = CollateralRatios {
            liquidation: tier_set(index, "collaterals", group.collaterals)?,
            borrow: tier_set(index, "borrowCollaterals", group.borrow_collaterals)?,
            transfer_out: tier_set(index, "withdrawCollaterals", group.withdraw_collaterals)?,
        };
        for asset in assets {
            match ratios.entry(AssetNameExchange::new(asset)) {
                Entry::Vacant(entry) => {
                    entry.insert(group_ratios.clone());
                }
                Entry::Occupied(entry) => {
                    return Err(decode_error(format!("{} listed twice", entry.key())));
                }
            }
        }
    }
    Ok(ratios)
}

/// The tier set `set` of group `index`, in ascending minimum USD value. It must be present and
/// hold at least one tier.
fn tier_set(
    index: usize,
    set: &str,
    tiers: Option<Vec<CollateralRatioTierAnswer>>,
) -> Result<Vec<CollateralRatioTier>, UnindexedClientError> {
    let tiers = tiers.ok_or_else(|| decode_error(format!("group {index}: {set} missing")))?;
    if tiers.is_empty() {
        return Err(decode_error(format!("group {index}: no {set}")));
    }
    let mut tiers = tiers
        .into_iter()
        .map(|tier| collateral_ratio_tier(index, set, tier))
        .collect::<Result<Vec<_>, _>>()?;
    tiers.sort_by_key(|tier| tier.min_usd_value);
    Ok(tiers)
}

/// A tier of set `set` of group `index`. Its minimum and ratio are required, its maximum is not.
fn collateral_ratio_tier(
    index: usize,
    set: &str,
    tier: CollateralRatioTierAnswer,
) -> Result<CollateralRatioTier, UnindexedClientError> {
    let field_name = |field: &str| format!("group {index} {set}: {field}");
    let required = |field: &str, value: Option<String>| {
        let field = field_name(field);
        optional_decimal(&field, value.as_deref())?
            .ok_or_else(|| decode_error(format!("{field} missing")))
    };
    Ok(CollateralRatioTier {
        min_usd_value: required("minUsdValue", tier.min_usd_value)?,
        max_usd_value: optional_decimal(&field_name("maxUsdValue"), tier.max_usd_value.as_deref())?,
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

    const SETS: [&str; 3] = ["collaterals", "borrowCollaterals", "withdrawCollaterals"];

    fn groups(body: serde_json::Value) -> Vec<CollateralRatioGroup> {
        serde_json::from_value(body).unwrap()
    }

    fn asset(name: &str) -> AssetNameExchange {
        AssetNameExchange::new(name)
    }

    fn open(ratio: Decimal) -> Vec<CollateralRatioTier> {
        vec![CollateralRatioTier::new(dec!(0), None, ratio)]
    }

    /// Two groups: one with the single open tier per set Binance was seen to send, each set's
    /// ratio distinct so that a set read into the wrong field shows; and one with sets of several
    /// tiers listed out of order.
    fn tiered() -> serde_json::Value {
        serde_json::json!([
            {
                "collaterals": [{"minUsdValue": "0", "discountRate": "1"}],
                "borrowCollaterals": [{"minUsdValue": "0", "discountRate": "0.95"}],
                "withdrawCollaterals": [{"minUsdValue": "0", "discountRate": "0.9"}],
                "assetNames": ["USDT", "BTC"]
            },
            {
                "collaterals": [
                    {"minUsdValue": "5000", "maxUsdValue": "10000", "discountRate": "0.5"},
                    {"minUsdValue": "10000", "discountRate": "0"},
                    {"minUsdValue": "0", "maxUsdValue": "5000", "discountRate": "0.9"}
                ],
                "borrowCollaterals": [
                    {"minUsdValue": "2000", "discountRate": "0.05"},
                    {"minUsdValue": "0", "maxUsdValue": "2000", "discountRate": "0.1"}
                ],
                "withdrawCollaterals": [
                    {"minUsdValue": "3000", "discountRate": "0.02"},
                    {"minUsdValue": "0", "maxUsdValue": "3000", "discountRate": "0.2"}
                ],
                "assetNames": ["SHIB"]
            }
        ])
    }

    #[test]
    fn collateral_ratios_give_each_asset_its_groups_three_sets_in_ascending_order() {
        let ratios = collateral_ratios(groups(tiered())).unwrap();
        let single = CollateralRatios::new(open(dec!(1)), open(dec!(0.95)), open(dec!(0.9)));
        assert_eq!(
            ratios,
            HashMap::from([
                (asset("USDT"), single.clone()),
                (asset("BTC"), single),
                (
                    asset("SHIB"),
                    CollateralRatios::new(
                        vec![
                            CollateralRatioTier::new(dec!(0), Some(dec!(5000)), dec!(0.9)),
                            CollateralRatioTier::new(dec!(5000), Some(dec!(10000)), dec!(0.5)),
                            CollateralRatioTier::new(dec!(10000), None, dec!(0)),
                        ],
                        vec![
                            CollateralRatioTier::new(dec!(0), Some(dec!(2000)), dec!(0.1)),
                            CollateralRatioTier::new(dec!(2000), None, dec!(0.05)),
                        ],
                        vec![
                            CollateralRatioTier::new(dec!(0), Some(dec!(3000)), dec!(0.2)),
                            CollateralRatioTier::new(dec!(3000), None, dec!(0.02)),
                        ],
                    )
                ),
            ])
        );
    }

    #[test]
    fn collateral_ratios_of_no_groups_or_a_group_of_no_assets_are_empty() {
        assert!(collateral_ratios(Vec::new()).unwrap().is_empty());
        let mut body = tiered();
        body[0]["assetNames"] = serde_json::json!([]);
        body.as_array_mut().unwrap().truncate(1);
        assert!(collateral_ratios(groups(body)).unwrap().is_empty());
    }

    #[test]
    fn collateral_ratios_refuse_an_answer_they_cannot_read_whole() {
        let mut cases = Vec::new();
        let mut body = tiered();
        body[1].as_object_mut().unwrap().remove("assetNames");
        cases.push(("group 1: assetNames missing".to_string(), body));
        for set in SETS {
            let mut body = tiered();
            body[1].as_object_mut().unwrap().remove(set);
            cases.push((format!("group 1: {set} missing"), body));
            let mut body = tiered();
            body[1][set] = serde_json::json!([]);
            cases.push((format!("group 1: no {set}"), body));
            for field in ["minUsdValue", "discountRate"] {
                let mut body = tiered();
                body[1][set][1].as_object_mut().unwrap().remove(field);
                cases.push((format!("group 1 {set}: {field} missing"), body));
            }
            for field in ["minUsdValue", "maxUsdValue", "discountRate"] {
                let mut body = tiered();
                body[1][set][0][field] = serde_json::json!("0.5.0");
                cases.push((format!("group 1 {set}: {field} \"0.5.0\""), body));
            }
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
                .and(wiremock::matchers::path(COLLATERAL_RATIO_PATH))
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
            assert_eq!(
                request.url.query_pairs().count(),
                0,
                "unsigned, no parameters: {}",
                request.url
            );
        }
    }

    #[tokio::test]
    async fn fetch_collateral_ratios_reports_a_refusal_and_an_unreadable_answer() {
        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::path(COLLATERAL_RATIO_PATH))
            .respond_with(
                wiremock::ResponseTemplate::new(400)
                    .set_body_json(serde_json::json!({"code": -1102, "msg": "refused"})),
            )
            .up_to_n_times(1)
            .mount(&server)
            .await;
        let mut unreadable = tiered();
        unreadable[0]["borrowCollaterals"][0]["discountRate"] = serde_json::json!("one");
        wiremock::Mock::given(wiremock::matchers::path(COLLATERAL_RATIO_PATH))
            .respond_with(wiremock::ResponseTemplate::new(200).set_body_json(unreadable))
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
                assert!(
                    message.contains("borrowCollaterals: discountRate \"one\""),
                    "{message}"
                );
            }
            other => panic!("{other:?}"),
        }
    }
}
