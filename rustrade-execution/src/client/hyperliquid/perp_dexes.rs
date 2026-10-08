//! The perpetual DEXs a [`HyperliquidClient`](super::HyperliquidClient) trades: Hyperliquid's
//! default one and the builder-deployed (HIP-3) ones its config names.

use super::common::info;
use super::error::HyperliquidConnectError;
use hyperliquid_rust_sdk::InfoClient;
use rustrade_instrument::{hyperliquid::CoinKind, instrument::name::InstrumentNameExchange};
use serde::Deserialize;
use smol_str::{SmolStr, format_smolstr};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use tracing::warn;

/// The collateral token of Hyperliquid's default perpetual DEX.
pub(super) const DEFAULT_COLLATERAL: &str = "USDC";

/// Hyperliquid addresses a HIP-3 perpetual as asset `100000 + dex_index * 10000 + index`, where
/// `dex_index` is the DEX's place in `perpDexs` and `index` the perpetual's in the DEX's `meta`.
const HIP3_ASSET_BASE: u32 = 100_000;
const HIP3_ASSETS_PER_DEX: u32 = 10_000;

/// The perpetual DEXs a client trades, and the names of their perpetuals.
///
/// A perpetual is named `{coin}-{collateral}-PERP`, after the token its DEX settles in:
/// `BTC-USDC-PERP` on the default DEX, `xyz:TSLA-USDC-PERP` and `flx:TSLA-USDH-PERP` on HIP-3
/// ones. A coin on a HIP-3 DEX the client was not configured with has no name here: its
/// collateral is not known. [`UnconfiguredDexes`] logs such coins.
///
/// Read-only once fetched, so shared between tasks without a lock.
#[derive(Debug, Default)]
pub(super) struct PerpDexes {
    /// Each configured HIP-3 DEX's collateral token, by DEX name. Ordered, so every read that
    /// walks the DEXs reports them in the same order.
    collateral: BTreeMap<SmolStr, SmolStr>,
    /// The asset id of every perpetual the configured HIP-3 DEXs list, by coin.
    asset_ids: HashMap<String, u32>,
    /// The `szDecimals` of every perpetual traded, the default DEX's included, by coin.
    sz_decimals: HashMap<String, u32>,
}

/// One entry of the `perpDexs` response. The default DEX is listed first, as `null`.
#[derive(Debug, Deserialize)]
struct ListedDex {
    name: SmolStr,
}

/// The parts of a `meta` response, read with a `dex` field, that this client uses.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct DexMeta {
    universe: Vec<DexPerp>,
    /// The index, in `spotMeta`'s tokens, of the token the DEX settles in.
    collateral_token: usize,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct DexPerp {
    name: String,
    sz_decimals: u32,
}

/// The parts of a `spotMeta` response that this client uses.
#[derive(Debug, Deserialize)]
struct SpotTokens {
    tokens: Vec<SpotToken>,
}

#[derive(Debug, Deserialize)]
struct SpotToken {
    name: SmolStr,
    index: usize,
}

impl PerpDexes {
    /// Read the HIP-3 DEXs `dexes` from Hyperliquid: `perpDexs` and `spotMeta` (weight 20 each),
    /// then each DEX's `meta` (weight 20 each). Nothing is read when `dexes` is empty.
    ///
    /// # Errors
    ///
    /// [`HyperliquidConnectError::UnknownDex`] for a name `perpDexs` does not list, exactly as
    /// spelled; [`HyperliquidConnectError::Connectivity`] when a request fails in transit; and
    /// [`HyperliquidConnectError::Metadata`] when a response does not parse, or names a
    /// collateral token `spotMeta` does not list.
    pub(super) async fn fetch(
        info_client: &InfoClient,
        dexes: &[SmolStr],
    ) -> Result<Self, HyperliquidConnectError> {
        if dexes.is_empty() {
            return Ok(Self::default());
        }

        let (listed, spot) = tokio::try_join!(
            info::<Vec<Option<ListedDex>>>(
                info_client,
                "perpDexs",
                r#"{"type":"perpDexs"}"#.to_owned()
            ),
            info::<SpotTokens>(info_client, "spotMeta", r#"{"type":"spotMeta"}"#.to_owned()),
        )?;

        let dexes = dexes.iter().collect::<BTreeSet<_>>();
        let indexed = dexes
            .into_iter()
            .map(|dex| {
                let index = listed
                    .iter()
                    .position(|listed| listed.as_ref().is_some_and(|listed| listed.name == *dex))
                    .ok_or_else(|| HyperliquidConnectError::UnknownDex(dex.clone()))?;
                Ok((dex, index))
            })
            .collect::<Result<Vec<_>, HyperliquidConnectError>>()?;

        let metas = futures::future::try_join_all(indexed.iter().map(|(dex, _)| {
            info::<Option<DexMeta>>(
                info_client,
                "meta",
                serde_json::json!({"type": "meta", "dex": dex}).to_string(),
            )
        }))
        .await?;

        let tokens = spot
            .tokens
            .into_iter()
            .map(|token| (token.index, token.name))
            .collect::<HashMap<_, _>>();

        let mut perp_dexes = Self::default();
        for ((dex, dex_index), meta) in indexed.into_iter().zip(metas) {
            let meta = meta.ok_or_else(|| {
                HyperliquidConnectError::Metadata(format!("no meta for listed DEX {dex:?}"))
            })?;
            perp_dexes.insert(dex, dex_index, meta, &tokens)?;
        }
        Ok(perp_dexes)
    }

    fn insert(
        &mut self,
        dex: &SmolStr,
        dex_index: usize,
        meta: DexMeta,
        tokens: &HashMap<usize, SmolStr>,
    ) -> Result<(), HyperliquidConnectError> {
        let collateral = tokens.get(&meta.collateral_token).ok_or_else(|| {
            HyperliquidConnectError::Metadata(format!(
                "DEX {dex:?} settles in token {}, which spotMeta does not list",
                meta.collateral_token
            ))
        })?;
        let first_asset = u32::try_from(dex_index)
            .ok()
            .and_then(|index| index.checked_mul(HIP3_ASSETS_PER_DEX))
            .and_then(|offset| offset.checked_add(HIP3_ASSET_BASE))
            .ok_or_else(|| {
                HyperliquidConnectError::Metadata(format!(
                    "DEX {dex:?} is listed at index {dex_index}, past any asset id"
                ))
            })?;

        for (index, perp) in meta.universe.into_iter().enumerate() {
            let asset = u32::try_from(index)
                .ok()
                .filter(|index| *index < HIP3_ASSETS_PER_DEX)
                .map(|index| first_asset + index)
                .ok_or_else(|| {
                    HyperliquidConnectError::Metadata(format!(
                        "DEX {dex:?} lists more than {HIP3_ASSETS_PER_DEX} perpetuals"
                    ))
                })?;
            self.sz_decimals.insert(perp.name.clone(), perp.sz_decimals);
            self.asset_ids.insert(perp.name, asset);
        }
        self.collateral.insert(dex.clone(), collateral.clone());
        Ok(())
    }

    /// The asset id of every perpetual the configured HIP-3 DEXs list, by coin, for the SDK's
    /// `coin_to_asset`, which knows only the default DEX's.
    pub(super) fn asset_ids(&self) -> impl Iterator<Item = (String, u32)> + '_ {
        self.asset_ids
            .iter()
            .map(|(coin, asset)| (coin.clone(), *asset))
    }

    /// Record the `szDecimals` of the default DEX's perpetuals, from the `meta` the SDK read.
    pub(super) fn add_default_perps(&mut self, universe: &[hyperliquid_rust_sdk::AssetMeta]) {
        self.sz_decimals.extend(
            universe
                .iter()
                .map(|asset| (asset.name.clone(), asset.sz_decimals)),
        );
    }

    /// The `szDecimals` of the perpetual `coin`, `None` if no DEX traded listed it when the
    /// client connected.
    pub(super) fn sz_decimals(&self, coin: &str) -> Option<u32> {
        self.sz_decimals.get(coin).copied()
    }

    /// The configured HIP-3 DEXs, by name.
    pub(super) fn hip3(&self) -> impl Iterator<Item = &str> {
        self.collateral.keys().map(SmolStr::as_str)
    }

    /// The collateral token of `dex`: the default DEX's for `None`, `None` for a HIP-3 DEX not
    /// configured.
    pub(super) fn collateral(&self, dex: Option<&str>) -> Option<&str> {
        match dex {
            None => Some(DEFAULT_COLLATERAL),
            Some(dex) => self.collateral.get(dex).map(SmolStr::as_str),
        }
    }

    /// The distinct collateral tokens of every DEX traded, the default one's included.
    pub(super) fn collaterals(&self) -> BTreeSet<&str> {
        std::iter::once(DEFAULT_COLLATERAL)
            .chain(self.collateral.values().map(SmolStr::as_str))
            .collect()
    }

    /// The perpetual instrument `coin` names, `{coin}-{collateral}-PERP`, with the collateral
    /// token, or `None` when it names a spot pair, a market of a kind [`CoinKind::of`] does not
    /// recognise, or a perpetual on a HIP-3 DEX not configured.
    ///
    /// The account streams and open orders deliver every market's coins together, so other
    /// markets are expected here.
    pub(super) fn perp(&self, coin: &str) -> Option<(InstrumentNameExchange, &str)> {
        if CoinKind::of(coin) != CoinKind::Perp {
            return None;
        }
        let collateral = self.collateral(dex_of(coin))?;
        let instrument = InstrumentNameExchange::from(format_smolstr!("{coin}-{collateral}-PERP"));
        Some((instrument, collateral))
    }

    /// The perpetual instrument `coin` names; see [`perp`](Self::perp).
    pub(super) fn instrument(&self, coin: &str) -> Option<InstrumentNameExchange> {
        self.perp(coin).map(|(instrument, _)| instrument)
    }

    /// The HIP-3 DEX of `coin` when `coin` is a perpetual on one this client is not configured
    /// with, and so is left out.
    fn unconfigured_dex<'a>(&self, coin: &'a str) -> Option<&'a str> {
        let dex = dex_of(coin)?;
        (CoinKind::of(coin) == CoinKind::Perp && !self.collateral.contains_key(dex)).then_some(dex)
    }

    /// The coin `instrument` names: the inverse of [`instrument`](Self::instrument).
    ///
    /// # Errors
    ///
    /// Why `instrument` names no perpetual this client trades: it is not `{coin}-{quote}-PERP`
    /// for a perpetual coin, its DEX is not configured, or its quote is not its DEX's
    /// collateral.
    pub(super) fn coin<'a>(
        &self,
        instrument: &'a InstrumentNameExchange,
    ) -> Result<&'a str, String> {
        let name = instrument.as_ref();
        let Some((coin, quote)) = name
            .strip_suffix("-PERP")
            .and_then(|rest| rest.rsplit_once('-'))
            .filter(|(coin, _)| CoinKind::of(coin) == CoinKind::Perp)
        else {
            return Err(format!(
                "{name} is not a Hyperliquid perpetual: expected {{coin}}-{{collateral}}-PERP, \
                 such as BTC-USDC-PERP"
            ));
        };
        let dex = dex_of(coin);
        match self.collateral(dex) {
            Some(collateral) if collateral == quote => Ok(coin),
            Some(collateral) => Err(format!(
                "{name} is quoted in {quote}, but {coin} settles in {collateral}: expected \
                 {coin}-{collateral}-PERP"
            )),
            None => Err(format!(
                "{name} is on the HIP-3 DEX {:?}, which HyperliquidConfig::dexes does not name",
                dex.unwrap_or_default()
            )),
        }
    }
}

/// The HIP-3 DEXs not configured whose perpetuals an account stream has left out, so it logs
/// each DEX once rather than on every event. Held by each stream task, so it needs no lock.
#[derive(Debug, Default)]
pub(super) struct UnconfiguredDexes(HashSet<SmolStr>);

impl UnconfiguredDexes {
    /// Log with `warn!` the DEX of `coin` if `coin` is a perpetual on a DEX not configured, and
    /// the DEX was not logged before.
    pub(super) fn warn_once(&mut self, dexes: &PerpDexes, coin: &str) {
        if let Some(dex) = dexes.unconfigured_dex(coin)
            && !self.0.contains(dex)
        {
            warn!(
                %dex,
                %coin,
                "Hyperliquid perpetual is on a DEX this client is not configured with, so its \
                 collateral is unknown; leaving that DEX's coins out (logged once per stream)"
            );
            self.0.insert(SmolStr::from(dex));
        }
    }
}

/// Log with one `warn!` the distinct HIP-3 DEXs not configured that `coins` are perpetuals on,
/// which are left out. For a read that lists many rows at once.
pub(super) fn warn_unconfigured_dexes<'a>(
    dexes: &PerpDexes,
    coins: impl IntoIterator<Item = &'a str>,
) {
    let unconfigured = coins
        .into_iter()
        .filter_map(|coin| dexes.unconfigured_dex(coin))
        .collect::<BTreeSet<_>>();
    if !unconfigured.is_empty() {
        warn!(
            ?unconfigured,
            "Hyperliquid perpetuals are on DEXs this client is not configured with, so their \
             collateral is unknown; leaving them out"
        );
    }
}

/// The HIP-3 DEX `coin` trades on (`xyz` for `xyz:TSLA`), `None` for the default DEX.
pub(super) fn dex_of(coin: &str) -> Option<&str> {
    coin.split_once(':').map(|(dex, _)| dex)
}

#[cfg(test)]
// Test code: panics on bad input are acceptable
#[allow(clippy::unwrap_used)]
pub(super) mod tests {
    use super::super::common::info_tests::info_client_against;
    use super::*;
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{body_partial_json, method, path},
    };

    /// `perpDexs` listing `xyz` at index 1 and `flx` at index 2.
    fn perp_dexs() -> serde_json::Value {
        serde_json::json!([
            null,
            {"name": "xyz", "fullName": "XYZ", "deployer": "0x0"},
            {"name": "flx", "fullName": "Felix Exchange", "deployer": "0x0"}
        ])
    }

    fn spot_meta() -> serde_json::Value {
        serde_json::json!({
            "universe": [],
            "tokens": [
                {"name": "USDC", "index": 0},
                {"name": "USDH", "index": 360}
            ]
        })
    }

    async fn mount(server: &MockServer, request: serde_json::Value, response: serde_json::Value) {
        Mock::given(method("POST"))
            .and(path("/info"))
            .and(body_partial_json(request))
            .respond_with(ResponseTemplate::new(200).set_body_json(response))
            .mount(server)
            .await;
    }

    /// A server answering `perpDexs`, `spotMeta`, and `meta` for `xyz` (USDC, two perpetuals)
    /// and `flx` (USDH, one).
    pub(in super::super) async fn dex_server() -> MockServer {
        let server = MockServer::start().await;
        mount(
            &server,
            serde_json::json!({"type": "perpDexs"}),
            perp_dexs(),
        )
        .await;
        mount(
            &server,
            serde_json::json!({"type": "spotMeta"}),
            spot_meta(),
        )
        .await;
        mount(
            &server,
            serde_json::json!({"type": "meta", "dex": "xyz"}),
            serde_json::json!({
                "universe": [{"name": "xyz:TSLA", "szDecimals": 2}, {"name": "xyz:NVDA", "szDecimals": 2}],
                "collateralToken": 0
            }),
        )
        .await;
        mount(
            &server,
            serde_json::json!({"type": "meta", "dex": "flx"}),
            serde_json::json!({
                "universe": [{"name": "flx:TSLA", "szDecimals": 2}],
                "collateralToken": 360
            }),
        )
        .await;
        server
    }

    /// The DEXs `xyz` (USDC) and `flx` (USDH), read from [`dex_server`].
    pub(in super::super) async fn xyz_and_flx() -> PerpDexes {
        let server = dex_server().await;
        let info_client = info_client_against(server.uri()).await;
        PerpDexes::fetch(&info_client, &["xyz".into(), "flx".into()])
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn hip3_perpetuals_get_asset_ids_from_their_dex_index() {
        let dexes = xyz_and_flx().await;
        let mut ids = dexes.asset_ids().collect::<Vec<_>>();
        ids.sort();
        assert_eq!(
            ids,
            [
                ("flx:TSLA".to_owned(), 120_000),
                ("xyz:NVDA".to_owned(), 110_001),
                ("xyz:TSLA".to_owned(), 110_000),
            ]
        );
    }

    #[tokio::test]
    async fn perpetuals_are_named_after_their_dex_collateral() {
        let dexes = xyz_and_flx().await;
        let name = |coin| dexes.instrument(coin).map(|name| name.to_string());
        assert_eq!(name("BTC").as_deref(), Some("BTC-USDC-PERP"));
        assert_eq!(name("xyz:TSLA").as_deref(), Some("xyz:TSLA-USDC-PERP"));
        assert_eq!(name("flx:TSLA").as_deref(), Some("flx:TSLA-USDH-PERP"));
        // Not configured: its collateral is unknown.
        assert_eq!(name("km:TSLA"), None);
        // Not perpetuals.
        assert_eq!(name("@107"), None);
        assert_eq!(name("#12"), None);
    }

    #[tokio::test]
    async fn an_instrument_names_its_coin_only_with_its_dex_collateral() {
        let dexes = xyz_and_flx().await;
        let coin = |name: &str| {
            dexes
                .coin(&InstrumentNameExchange::from(name))
                .map(str::to_owned)
        };
        assert_eq!(coin("BTC-USDC-PERP").unwrap(), "BTC");
        assert_eq!(coin("kPEPE-USDC-PERP").unwrap(), "kPEPE");
        assert_eq!(coin("flx:TSLA-USDH-PERP").unwrap(), "flx:TSLA");
        assert_eq!(coin("xyz:TSLA-USDC-PERP").unwrap(), "xyz:TSLA");

        assert!(
            coin("BTC-USD-PERP")
                .unwrap_err()
                .contains("settles in USDC")
        );
        assert!(
            coin("flx:TSLA-USDC-PERP")
                .unwrap_err()
                .contains("settles in USDH")
        );
        assert!(coin("km:TSLA-USDH-PERP").unwrap_err().contains("\"km\""));
        assert!(coin("BTC").is_err());
        assert!(coin("HYPE-USDC-SPOT").is_err());
        assert!(coin("@107-USDC-PERP").is_err());
    }

    #[tokio::test]
    async fn only_perpetuals_on_unconfigured_dexes_are_logged_once_each() {
        let dexes = xyz_and_flx().await;
        let mut logged = UnconfiguredDexes::default();
        for coin in ["km:TSLA", "km:NVDA", "xyz:TSLA", "BTC", "@107", "hyna:BTC"] {
            logged.warn_once(&dexes, coin);
        }
        assert_eq!(logged.0, HashSet::from(["km".into(), "hyna".into()]));
    }

    #[tokio::test]
    async fn a_perpetual_comes_with_its_collateral() {
        let dexes = xyz_and_flx().await;
        let (instrument, collateral) = dexes.perp("flx:TSLA").unwrap();
        assert_eq!(
            (instrument.as_ref(), collateral),
            ("flx:TSLA-USDH-PERP", "USDH")
        );
    }

    #[tokio::test]
    async fn the_collaterals_are_each_dex_token_once() {
        let dexes = xyz_and_flx().await;
        assert_eq!(
            dexes.collaterals().into_iter().collect::<Vec<_>>(),
            ["USDC", "USDH"]
        );
    }

    #[tokio::test]
    async fn no_dex_configured_reads_nothing_and_names_default_perpetuals() {
        // No mocks mounted: any request would fail.
        let server = MockServer::start().await;
        let info_client = info_client_against(server.uri()).await;
        let dexes = PerpDexes::fetch(&info_client, &[]).await.unwrap();
        assert_eq!(dexes.asset_ids().count(), 0);
        assert_eq!(dexes.instrument("ETH").unwrap().as_ref(), "ETH-USDC-PERP");
        assert_eq!(dexes.instrument("xyz:TSLA"), None);
    }

    #[tokio::test]
    async fn a_dex_hyperliquid_does_not_list_fails_the_read() {
        let server = dex_server().await;
        let info_client = info_client_against(server.uri()).await;
        // Names are exact: testnet lists both `volmex` and `VOLMEX`.
        let error = PerpDexes::fetch(&info_client, &["xyz".into(), "XYZ".into()])
            .await
            .unwrap_err();
        assert_eq!(error, HyperliquidConnectError::UnknownDex("XYZ".into()));
    }

    #[tokio::test]
    async fn a_collateral_spot_meta_does_not_list_fails_the_read() {
        let server = MockServer::start().await;
        mount(
            &server,
            serde_json::json!({"type": "perpDexs"}),
            perp_dexs(),
        )
        .await;
        mount(
            &server,
            serde_json::json!({"type": "spotMeta"}),
            spot_meta(),
        )
        .await;
        mount(
            &server,
            serde_json::json!({"type": "meta", "dex": "xyz"}),
            serde_json::json!({"universe": [], "collateralToken": 999}),
        )
        .await;
        let info_client = info_client_against(server.uri()).await;
        let error = PerpDexes::fetch(&info_client, &["xyz".into()])
            .await
            .unwrap_err();
        assert!(
            matches!(error, HyperliquidConnectError::Metadata(reason) if reason.contains("999"))
        );
    }
}
