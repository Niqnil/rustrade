//! A perpetuals account across the DEXs a [`HyperliquidClient`](super::HyperliquidClient)
//! trades: each DEX's positions and open orders, and the collateral balances, which the account's
//! abstraction mode decides where to read.

use super::common::{OpenOrder, parse_decimal, spot_balances, user_info};
use super::error::map_sdk_error;
use super::perp_dexes::PerpDexes;
use crate::{
    balance::{AssetBalance, Balance},
    error::UnindexedClientError,
};
use chrono::{DateTime, Utc};
use ethers::types::H160;
use futures::future::try_join_all;
use hyperliquid_rust_sdk::{InfoClient, UserStateResponse, UserTokenBalance};
use rust_decimal::Decimal;
use rustrade_instrument::asset::name::AssetNameExchange;
use smol_str::format_smolstr;

/// How an account holds its collateral, as `userAbstraction` reports it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum AccountMode {
    /// Standard mode (`disabled`, or `default` for an account that never chose one), and the
    /// discontinued `dexAbstraction`: perpetuals and spot hold separate balances, and each DEX
    /// margins on its own, from the collateral its `clearinghouseState` holds.
    PerDex,
    /// Unified account and portfolio margin: one balance per token, held in the spot
    /// clearinghouse, backs every DEX. Hyperliquid documents each DEX's own state as not
    /// meaningful for balances in these modes; it still lists the positions.
    Unified,
}

impl AccountMode {
    fn parse(mode: &str) -> Result<Self, UnindexedClientError> {
        match mode {
            "default" | "disabled" | "dexAbstraction" => Ok(Self::PerDex),
            "unifiedAccount" | "portfolioMargin" => Ok(Self::Unified),
            other => Err(UnindexedClientError::Internal(format!(
                "Hyperliquid account abstraction mode {other:?} is not one this client knows where \
                 to read balances under"
            ))),
        }
    }
}

/// The account's abstraction mode, read with `userAbstraction` (weight 20).
pub(super) async fn account_mode(
    info_client: &InfoClient,
    address: H160,
) -> Result<AccountMode, UnindexedClientError> {
    let mode: String = user_info(info_client, "userAbstraction", address, None).await?;
    AccountMode::parse(&mode)
}

/// The default DEX, as `None`, then each configured HIP-3 DEX.
fn every_dex(dexes: &PerpDexes) -> impl Iterator<Item = Option<&str>> {
    std::iter::once(None).chain(dexes.hip3().map(Some))
}

/// Each DEX's `clearinghouseState` (weight 2 each), by DEX: `None` for the default one.
pub(super) async fn dex_states<'a>(
    info_client: &InfoClient,
    address: H160,
    dexes: &'a PerpDexes,
) -> Result<Vec<(Option<&'a str>, UserStateResponse)>, UnindexedClientError> {
    let dexes = every_dex(dexes).collect::<Vec<_>>();
    let states = try_join_all(
        dexes
            .iter()
            .map(|dex| user_info(info_client, "clearinghouseState", address, *dex)),
    )
    .await?;
    Ok(dexes.into_iter().zip(states).collect())
}

/// Every open order on the DEXs traded, with `openOrders` (weight 20) on each: Hyperliquid lists
/// a HIP-3 DEX's orders only when asked for that DEX. The default DEX's listing carries spot
/// orders too, which the caller leaves out.
pub(super) async fn open_orders(
    info_client: &InfoClient,
    address: H160,
    dexes: &PerpDexes,
) -> Result<Vec<OpenOrder>, UnindexedClientError> {
    let listings = try_join_all(
        every_dex(dexes)
            .map(|dex| user_info::<Vec<OpenOrder>>(info_client, "openOrders", address, dex)),
    )
    .await?;
    Ok(listings.into_iter().flatten().collect())
}

/// The spot clearinghouse's token balances, with `spotClearinghouseState` (weight 2).
pub(super) async fn spot_state(
    info_client: &InfoClient,
    address: H160,
) -> Result<Vec<UserTokenBalance>, UnindexedClientError> {
    Ok(info_client
        .user_token_balances(address)
        .await
        .map_err(map_sdk_error)?
        .balances)
}

/// The collateral balances of the DEXs traded, read where `mode` holds them: from `states`, each
/// DEX's `clearinghouseState`, under [`AccountMode::PerDex`], or from `spotClearinghouseState`,
/// read here, under [`AccountMode::Unified`]. See [`per_dex_balances`] and [`unified_balances`].
pub(super) async fn balances(
    info_client: &InfoClient,
    address: H160,
    dexes: &PerpDexes,
    mode: AccountMode,
    states: &[(Option<&str>, UserStateResponse)],
) -> Result<Vec<AssetBalance<AssetNameExchange>>, UnindexedClientError> {
    Ok(match mode {
        AccountMode::PerDex => per_dex_balances(states, dexes, Utc::now()),
        AccountMode::Unified => {
            let spot = spot_state(info_client, address).await?;
            unified_balances(&spot, dexes, Utc::now())
        }
    })
}

/// The collateral balances of the DEXs traded: `userAbstraction`, then only what that mode holds
/// them in; see [`balances`].
pub(super) async fn fetch_balances(
    info_client: &InfoClient,
    address: H160,
    dexes: &PerpDexes,
) -> Result<Vec<AssetBalance<AssetNameExchange>>, UnindexedClientError> {
    let mode = account_mode(info_client, address).await?;
    let states = match mode {
        AccountMode::PerDex => dex_states(info_client, address, dexes).await?,
        AccountMode::Unified => Vec::new(),
    };
    balances(info_client, address, dexes, mode, &states).await
}

/// One balance per DEX, from its margin summary: the default DEX's under its collateral's name
/// (`USDC`), each HIP-3 DEX's under `{dex}:{collateral}` (`xyz:USDC`, `flx:USDH`), since each
/// margins separately and the same token in two DEXs is two balances.
///
/// The total is the account value, and the free balance what margin does not use, never below
/// zero: it goes negative during a liquidation, where it has no meaning.
pub(super) fn per_dex_balances(
    states: &[(Option<&str>, UserStateResponse)],
    dexes: &PerpDexes,
    now: DateTime<Utc>,
) -> Vec<AssetBalance<AssetNameExchange>> {
    states
        .iter()
        .filter_map(|(dex, state)| {
            let collateral = dexes.collateral(*dex)?;
            let asset = match dex {
                None => AssetNameExchange::from(collateral),
                Some(dex) => AssetNameExchange::from(format_smolstr!("{dex}:{collateral}")),
            };
            let summary = &state.margin_summary;
            let total =
                parse_decimal(&summary.account_value, "account_value").unwrap_or(Decimal::ZERO);
            let used = parse_decimal(&summary.total_margin_used, "total_margin_used")
                .unwrap_or(Decimal::ZERO);
            let free = (total - used).max(Decimal::ZERO);
            Some(AssetBalance::new(asset, Balance::new(total, free), now))
        })
        .collect()
}

/// One balance per collateral token of the DEXs traded, from the spot clearinghouse, free of
/// what is on hold (margin and open orders included). A token the clearinghouse does not list
/// is held at zero.
pub(super) fn unified_balances(
    spot: &[UserTokenBalance],
    dexes: &PerpDexes,
    now: DateTime<Utc>,
) -> Vec<AssetBalance<AssetNameExchange>> {
    let collaterals = dexes.collaterals();
    let mut balances = spot_balances(
        spot.iter()
            .filter(|balance| collaterals.contains(balance.coin.as_str())),
        now,
    )
    .collect::<Vec<_>>();
    for collateral in collaterals {
        if !balances
            .iter()
            .any(|balance| balance.asset.as_ref() == collateral)
        {
            balances.push(AssetBalance::new(
                AssetNameExchange::from(collateral),
                Balance::new(Decimal::ZERO, Decimal::ZERO),
                now,
            ));
        }
    }
    balances
}

#[cfg(test)]
// Test code: panics on bad input are acceptable
#[allow(clippy::unwrap_used)]
mod tests {
    use super::super::perp_dexes::tests::xyz_and_flx;
    use super::*;
    use rust_decimal_macros::dec;

    fn state(account_value: &str, margin_used: &str) -> UserStateResponse {
        serde_json::from_value(serde_json::json!({
            "assetPositions": [],
            "crossMarginSummary": {"accountValue": account_value, "totalMarginUsed": margin_used,
                                   "totalNtlPos": "0.0", "totalRawUsd": "0.0"},
            "marginSummary": {"accountValue": account_value, "totalMarginUsed": margin_used,
                              "totalNtlPos": "0.0", "totalRawUsd": "0.0"},
            "withdrawable": "0.0"
        }))
        .unwrap()
    }

    fn token(coin: &str, total: &str, hold: &str) -> UserTokenBalance {
        serde_json::from_value(serde_json::json!({
            "coin": coin, "token": 0, "total": total, "hold": hold, "entryNtl": "0.0"
        }))
        .unwrap()
    }

    fn summary(balances: &[AssetBalance<AssetNameExchange>]) -> Vec<(String, Decimal, Decimal)> {
        let mut rows = balances
            .iter()
            .map(|b| (b.asset.to_string(), b.balance.total, b.balance.free))
            .collect::<Vec<_>>();
        rows.sort();
        rows
    }

    /// Mount on `server` an answer to `request` for each DEX: `None` (no `dex` field) and each
    /// named one.
    async fn per_dex(
        server: &wiremock::MockServer,
        kind: &str,
        answers: [(Option<&str>, serde_json::Value); 3],
    ) {
        use wiremock::matchers::{body_json, method, path};
        let user = format!("{:?}", H160::zero());
        for (dex, answer) in answers {
            let mut request = serde_json::json!({"type": kind, "user": user});
            if let Some(dex) = dex {
                request["dex"] = dex.into();
            }
            wiremock::Mock::given(method("POST"))
                .and(path("/info"))
                .and(body_json(request))
                .respond_with(wiremock::ResponseTemplate::new(200).set_body_json(answer))
                .mount(server)
                .await;
        }
    }

    #[tokio::test]
    async fn open_orders_are_listed_on_every_dex_traded() {
        use super::super::common::info_tests::info_client_against;
        use super::super::order_recovery::tests::row;
        let dexes = xyz_and_flx().await;
        let server = wiremock::MockServer::start().await;
        per_dex(
            &server,
            "openOrders",
            [
                (
                    None,
                    serde_json::json!([row("BTC", 1, None), row("@107", 2, None)]),
                ),
                (Some("xyz"), serde_json::json!([row("xyz:TSLA", 3, None)])),
                (Some("flx"), serde_json::json!([])),
            ],
        )
        .await;
        let info_client = info_client_against(server.uri()).await;

        let mut coins = open_orders(&info_client, H160::zero(), &dexes)
            .await
            .unwrap()
            .into_iter()
            .map(|order| order.coin)
            .collect::<Vec<_>>();
        coins.sort();
        assert_eq!(coins, ["@107", "BTC", "xyz:TSLA"]);
    }

    #[tokio::test]
    async fn each_dex_state_is_read_with_its_dex() {
        use super::super::common::info_tests::info_client_against;
        let dexes = xyz_and_flx().await;
        let server = wiremock::MockServer::start().await;
        let state = |value: &str| {
            serde_json::json!({
                "assetPositions": [],
                "crossMarginSummary": {"accountValue": value, "totalMarginUsed": "0.0",
                                       "totalNtlPos": "0.0", "totalRawUsd": "0.0"},
                "marginSummary": {"accountValue": value, "totalMarginUsed": "0.0",
                                  "totalNtlPos": "0.0", "totalRawUsd": "0.0"},
                "withdrawable": "0.0"
            })
        };
        per_dex(
            &server,
            "clearinghouseState",
            [
                (None, state("1.0")),
                (Some("xyz"), state("2.0")),
                (Some("flx"), state("3.0")),
            ],
        )
        .await;
        let info_client = info_client_against(server.uri()).await;

        let states = dex_states(&info_client, H160::zero(), &dexes)
            .await
            .unwrap();
        assert_eq!(
            summary(&per_dex_balances(&states, &dexes, Utc::now())),
            [
                ("USDC".to_owned(), dec!(1), dec!(1)),
                ("flx:USDH".to_owned(), dec!(3), dec!(3)),
                ("xyz:USDC".to_owned(), dec!(2), dec!(2)),
            ]
        );
    }

    /// A server answering `userAbstraction` with `mode`, `spotClearinghouseState` with 998.99
    /// USDC (5.31 on hold), and each DEX's `clearinghouseState` with a distinct account value.
    async fn balance_server(mode: &str) -> wiremock::MockServer {
        use wiremock::matchers::{body_partial_json, method, path};
        let server = wiremock::MockServer::start().await;
        let mount = |request: serde_json::Value, answer: serde_json::Value| {
            wiremock::Mock::given(method("POST"))
                .and(path("/info"))
                .and(body_partial_json(request))
                .respond_with(wiremock::ResponseTemplate::new(200).set_body_json(answer))
        };
        mount(serde_json::json!({"type": "userAbstraction"}), mode.into())
            .mount(&server)
            .await;
        mount(
            serde_json::json!({"type": "spotClearinghouseState"}),
            serde_json::json!({"balances": [
                {"coin": "USDC", "token": 0, "total": "998.99", "hold": "5.31", "entryNtl": "0.0"}
            ]}),
        )
        .mount(&server)
        .await;
        let state = |value: &str| {
            serde_json::json!({
                "assetPositions": [],
                "crossMarginSummary": {"accountValue": value, "totalMarginUsed": "0.0",
                                       "totalNtlPos": "0.0", "totalRawUsd": "0.0"},
                "marginSummary": {"accountValue": value, "totalMarginUsed": "0.0",
                                  "totalNtlPos": "0.0", "totalRawUsd": "0.0"},
                "withdrawable": "0.0"
            })
        };
        // The DEX-specific mocks first: a request with `dex` also matches the bare one.
        for (dex, value) in [("xyz", "2.0"), ("flx", "3.0")] {
            mount(
                serde_json::json!({"type": "clearinghouseState", "dex": dex}),
                state(value),
            )
            .mount(&server)
            .await;
        }
        mount(
            serde_json::json!({"type": "clearinghouseState"}),
            state("1.0"),
        )
        .mount(&server)
        .await;
        server
    }

    #[tokio::test]
    async fn a_unified_account_reads_its_balances_from_the_spot_clearinghouse() {
        use super::super::common::info_tests::info_client_against;
        let dexes = xyz_and_flx().await;
        for mode in ["unifiedAccount", "portfolioMargin"] {
            let server = balance_server(mode).await;
            let info_client = info_client_against(server.uri()).await;
            let balances = fetch_balances(&info_client, H160::zero(), &dexes)
                .await
                .unwrap();
            assert_eq!(
                summary(&balances),
                [
                    ("USDC".to_owned(), dec!(998.99), dec!(993.68)),
                    ("USDH".to_owned(), dec!(0), dec!(0)),
                ],
                "{mode}"
            );
        }
    }

    #[tokio::test]
    async fn a_standard_account_reads_its_balances_from_each_dex() {
        use super::super::common::info_tests::info_client_against;
        let dexes = xyz_and_flx().await;
        for mode in ["default", "disabled", "dexAbstraction"] {
            let server = balance_server(mode).await;
            let info_client = info_client_against(server.uri()).await;
            let balances = fetch_balances(&info_client, H160::zero(), &dexes)
                .await
                .unwrap();
            assert_eq!(
                summary(&balances),
                [
                    ("USDC".to_owned(), dec!(1), dec!(1)),
                    ("flx:USDH".to_owned(), dec!(3), dec!(3)),
                    ("xyz:USDC".to_owned(), dec!(2), dec!(2)),
                ],
                "{mode}"
            );
        }
    }

    #[tokio::test]
    async fn an_unknown_account_mode_fails_the_balance_read() {
        use super::super::common::info_tests::info_client_against;
        let dexes = xyz_and_flx().await;
        let server = balance_server("somethingNew").await;
        let info_client = info_client_against(server.uri()).await;
        let error = fetch_balances(&info_client, H160::zero(), &dexes)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("somethingNew"), "{error}");
    }

    #[test]
    fn every_documented_mode_is_read_and_others_fail() {
        for mode in ["default", "disabled", "dexAbstraction"] {
            assert_eq!(AccountMode::parse(mode).unwrap(), AccountMode::PerDex);
        }
        for mode in ["unifiedAccount", "portfolioMargin"] {
            assert_eq!(AccountMode::parse(mode).unwrap(), AccountMode::Unified);
        }
        assert!(AccountMode::parse("somethingNew").is_err());
    }

    #[tokio::test]
    async fn per_dex_balances_name_each_hip3_dex_and_its_collateral() {
        let dexes = xyz_and_flx().await;
        let states = [
            (None, state("100.0", "30.0")),
            (Some("xyz"), state("50.0", "0.0")),
            (Some("flx"), state("10.0", "12.0")),
        ];
        assert_eq!(
            summary(&per_dex_balances(&states, &dexes, Utc::now())),
            [
                ("USDC".to_owned(), dec!(100), dec!(70)),
                ("flx:USDH".to_owned(), dec!(10), dec!(0)),
                ("xyz:USDC".to_owned(), dec!(50), dec!(50)),
            ]
        );
    }

    #[tokio::test]
    async fn unified_balances_are_the_collateral_tokens_spot_holds() {
        let dexes = xyz_and_flx().await;
        // HYPE is not a collateral; USDH is not held.
        let spot = [token("USDC", "998.99", "5.31"), token("HYPE", "2.0", "0.0")];
        assert_eq!(
            summary(&unified_balances(&spot, &dexes, Utc::now())),
            [
                ("USDC".to_owned(), dec!(998.99), dec!(993.68)),
                ("USDH".to_owned(), dec!(0), dec!(0)),
            ]
        );
    }
}
