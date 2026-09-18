//! Shared utilities for Hyperliquid execution clients (perps and spot).
//!
//! Contains parsing helpers and stream wrappers used by both
//! `HyperliquidClient` (perps) and `HyperliquidSpotClient`.
//!
//! Error mapping is in the `error` module.

use crate::client::hyperliquid::error::map_sdk_error;
use crate::error::UnindexedClientError;
use crate::order::{TimeInForce, id::ClientOrderId};
use chrono::{DateTime, TimeZone, Utc};
use futures::Stream;
use rust_decimal::Decimal;
use rustrade_instrument::{Side, instrument::name::InstrumentNameExchange};
use smol_str::format_smolstr;
use std::pin::Pin;
use std::str::FromStr;
use std::task::{Context, Poll};
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};
use uuid::Uuid;

/// Stream wrapper that cancels background tasks when dropped.
///
/// Ensures spawned WebSocket processing tasks are cleaned up when the consumer
/// drops the account stream, preventing task leaks.
#[derive(Debug)]
pub struct CancelOnDropStream<S> {
    inner: S,
    cancel_token: CancellationToken,
}

impl<S> CancelOnDropStream<S> {
    /// Create a new cancel-on-drop stream wrapper.
    pub(crate) fn new(inner: S, cancel_token: CancellationToken) -> Self {
        Self {
            inner,
            cancel_token,
        }
    }
}

impl<S: Stream + Unpin> Stream for CancelOnDropStream<S> {
    type Item = S::Item;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.inner).poll_next(cx)
    }
}

impl<S> Drop for CancelOnDropStream<S> {
    fn drop(&mut self) {
        self.cancel_token.cancel();
    }
}

/// Parse a decimal string from SDK response, logging warnings on failure.
/// One fill from the `userFills` info endpoint, as the venue actually returns it.
///
/// # Why this exists rather than `hyperliquid_rust_sdk::UserFillsResponse`
///
/// The SDK's type does not model `tid`, the only per-fill identifier the endpoint offers, and it
/// does not model `feeToken`. Neither omission is visible at runtime: the SDK does not set
/// `deny_unknown_fields`, so both are parsed away silently. Without `tid` the only id left is
/// `hash`, which identifies the *transaction* — one aggressive order sweeping several resting
/// orders produces several fills under a single hash, and they are then indistinguishable.
///
/// Only the fields this client reads are declared. Unknown fields are ignored, so `hash`,
/// `closedPnl`, `dir`, `startPosition`, `crossed` and `builderFee` cost nothing by being absent
/// here.
///
/// Drop this in favour of the SDK type if it ever models `tid`.
#[derive(Debug, Clone, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UserFill {
    pub coin: String,
    pub side: String,
    pub px: String,
    pub sz: String,
    pub time: u64,
    pub oid: u64,
    pub fee: String,

    /// Identifies this fill. `hash` identifies the transaction that contained it, and one
    /// transaction can contain many fills.
    ///
    /// Declared required rather than `Option<u64>` deliberately. If the venue ever stops sending
    /// it, this client should fail loudly at the parse — falling back to `hash` would silently
    /// restore a duplicate-id defect that is invisible until fills go missing during
    /// reconciliation.
    ///
    /// Hyperliquid documents this as unique per fill, but qualified by coin rather than global,
    /// which is why the dedup cache keys on the instrument alongside it.
    pub tid: u64,

    /// Asset the fee is denominated in.
    ///
    /// Optional because, unlike every other field here, nothing proves the endpoint sends it: the
    /// SDK's own type omits it, so it has never been observed through this dependency. The caller
    /// falls back to inferring the asset from the side when it is absent.
    pub fee_token: Option<String>,
}

/// Fetch `userFills` for `address`, parsing it into [`UserFill`] rather than the SDK's lossy type.
///
/// Issued through the SDK's own [`HttpClient`](hyperliquid_rust_sdk::InfoClient::http_client), so
/// base URL, TLS configuration and error type are exactly those of every other info request this
/// client makes — only the response type differs.
///
/// # Errors
///
/// Returns a transport error if the POST fails, or a parse error if the response does not match
/// [`UserFill`] — which includes the case where `tid` has stopped being sent.
pub async fn user_fills(
    info_client: &hyperliquid_rust_sdk::InfoClient,
    address: ethers::types::H160,
) -> Result<Vec<UserFill>, UnindexedClientError> {
    // `{:?}` on H160 renders the checksummed 0x-prefixed form the endpoint expects. `Display`
    // abbreviates the middle of the address ("0x1234…5678"), so it must not be used here.
    let body = format!(r#"{{"type":"userFills","user":"{address:?}"}}"#);

    let raw = info_client
        .http_client
        .post("/info", body)
        .await
        .map_err(map_sdk_error)?;

    // `Internal` rather than a connectivity error: a response that does not parse is a schema
    // change or a venue-side regression, not a transient fault, and retrying will not fix it.
    serde_json::from_str(&raw).map_err(|e| {
        UnindexedClientError::Internal(format!("Hyperliquid userFills response did not parse: {e}"))
    })
}

pub fn parse_decimal(value: &str, field: &str) -> Option<Decimal> {
    Decimal::from_str(value)
        .map_err(|e| warn!(%field, %value, %e, "Failed to parse decimal"))
        .ok()
}

/// Parse SDK side string to rustrade Side.
///
/// Hyperliquid API returns "B" for buy/long and "A" for sell/short (ask-side).
/// Extra variants included for defensive parsing of potential future API changes.
pub fn parse_side(side: &str) -> Option<Side> {
    match side {
        "B" | "b" | "BUY" | "Buy" | "buy" => Some(Side::Buy),
        "A" | "a" | "S" | "s" | "SELL" | "Sell" | "sell" => Some(Side::Sell),
        _ => {
            warn!(%side, "Unknown side string");
            None
        }
    }
}

/// Convert milliseconds timestamp to `DateTime<Utc>`.
///
/// Returns `None` for timestamps outside the representable range (year 292M+).
pub fn millis_to_datetime(millis: u64) -> Option<DateTime<Utc>> {
    Utc.timestamp_millis_opt(i64::try_from(millis).ok()?)
        .single()
}

/// Round a price to 5 significant figures (Hyperliquid requirement).
///
/// Performs rounding using `Decimal` arithmetic to avoid floating-point precision
/// errors. The final `f64` conversion happens only at the SDK interface boundary.
pub fn round_to_5_sig_figs(value: Decimal) -> f64 {
    use rust_decimal::prelude::ToPrimitive;

    if value.is_zero() {
        return 0.0;
    }

    // Use f64 only for computing magnitude (acceptable precision for this purpose;
    // we only need to know which power of 10 the number is close to).
    let abs_f = value.abs().to_f64().unwrap_or(0.0);
    if abs_f == 0.0 {
        return 0.0;
    }

    // Clamp magnitude to prevent overflow when computing scale factor
    #[allow(clippy::cast_possible_truncation)]
    let magnitude = abs_f.log10().floor().clamp(-30.0, 30.0) as i32;

    // Round using Decimal arithmetic to preserve precision
    let rounded = if magnitude >= 4 {
        // Large numbers (e.g., 123456): scale down, round integer, scale back up
        // Safety: magnitude >= 4 guarantees (magnitude - 4) is non-negative
        #[allow(clippy::cast_sign_loss)]
        let factor = Decimal::from(10i64.pow((magnitude - 4) as u32));
        (value / factor).round() * factor
    } else {
        // Small/medium numbers: round to appropriate decimal places
        #[allow(clippy::cast_sign_loss)]
        let dp = (4 - magnitude) as u32;
        value.round_dp(dp)
    };

    // SDK requires f64 — convert only at the interface boundary
    rounded.to_f64().unwrap_or(0.0)
}

/// Map rustrade TimeInForce to Hyperliquid TIF string.
pub fn map_tif(tif: &TimeInForce) -> &'static str {
    match tif {
        TimeInForce::GoodUntilCancelled { post_only: true } => "Alo",
        TimeInForce::GoodUntilCancelled { post_only: false } => "Gtc",
        TimeInForce::ImmediateOrCancel => "Ioc",
        TimeInForce::FillOrKill => "Ioc", // Hyperliquid doesn't have FOK, use IOC
        TimeInForce::GoodUntilEndOfDay => "Gtc", // No EOD on Hyperliquid
        // Hyperliquid is a perpetuals DEX with no auction sessions and no native
        // GTD. Coerce to GTC and surface the loss of semantics so downstream
        // wrappers can decide whether to reject upstream.
        TimeInForce::GoodTillDate { .. } | TimeInForce::AtOpen | TimeInForce::AtClose => {
            warn!(time_in_force = ?tif, "Hyperliquid does not support this TimeInForce; coercing to Gtc");
            "Gtc"
        }
    }
}

/// Convert a ClientOrderId to SDK cloid format (UUID) if valid.
///
/// Returns `Some(Uuid)` if the cid is a valid UUID, `None` otherwise.
/// Non-UUID CIDs are logged at debug level since they're common in tests/examples.
pub fn cid_to_cloid(cid: &ClientOrderId) -> Option<Uuid> {
    match Uuid::parse_str(cid.0.as_str()) {
        Ok(uuid) => Some(uuid),
        Err(_) => {
            debug!(cid = %cid.0, "CID is not a valid UUID, cloid will be None");
            None
        }
    }
}

/// Build perp instrument name from Hyperliquid coin name (e.g., "BTC" -> "BTC-USD-PERP").
pub fn perp_coin_to_instrument(coin: &str) -> InstrumentNameExchange {
    InstrumentNameExchange::from(format_smolstr!("{}-USD-PERP", coin))
}

/// Extract coin name from perp instrument (e.g., "BTC-USD-PERP" -> "BTC").
///
/// Returns `String` because Hyperliquid SDK requires `String` for asset fields.
pub fn instrument_to_perp_coin(instrument: &InstrumentNameExchange) -> String {
    let s = instrument.as_ref();
    // Expected format: "COIN-USD-PERP" or just "COIN"
    match s.split_once('-') {
        Some((coin, _)) => coin.to_string(),
        None => s.to_string(),
    }
}

/// Build spot instrument name from Hyperliquid coin pair (e.g., "PURR/USDC" -> "PURR-USDC-SPOT").
///
/// # Panics (debug builds only)
///
/// Debug-asserts if `coin` does not contain '/' — callers must verify `is_spot_coin()`
/// before calling. The fallback path produces a malformed instrument name.
pub fn spot_coin_to_instrument(coin: &str) -> InstrumentNameExchange {
    match coin.split_once('/') {
        Some((base, quote)) => {
            InstrumentNameExchange::from(format_smolstr!("{}-{}-SPOT", base, quote))
        }
        None => {
            debug_assert!(
                false,
                "spot_coin_to_instrument called with non-spot coin: {coin}"
            );
            InstrumentNameExchange::from(format_smolstr!("{}-SPOT", coin))
        }
    }
}

/// Extract coin pair from spot instrument (e.g., "PURR-USDC-SPOT" -> "PURR/USDC").
///
/// Returns `Option<String>` — `None` if the instrument doesn't match expected
/// `BASE-QUOTE-SPOT` format. Callers should fail fast on `None` rather than
/// send a malformed asset to the exchange.
pub fn instrument_to_spot_coin(instrument: &InstrumentNameExchange) -> Option<String> {
    let s = instrument.as_ref();
    // Expected format: "BASE-QUOTE-SPOT" -> "BASE/QUOTE"
    let without_suffix = s.strip_suffix("-SPOT")?;
    let (base, quote) = without_suffix.split_once('-')?;
    Some(format!("{}/{}", base, quote))
}

/// Check if a Hyperliquid coin name is a spot pair (contains '/').
///
/// Hyperliquid API uses pair format `"BASE/QUOTE"` (e.g., `"PURR/USDC"`) for spot coins
/// and single symbols (e.g., `"BTC"`) for perpetuals. This naming convention is observed
/// across all SDK examples and test fixtures. The invariant is validated by our spot
/// fixture tests in `hyperliquid_spot_execution.rs`.
pub fn is_spot_coin(coin: &str) -> bool {
    coin.contains('/')
}

#[cfg(test)]
// Test code: panics on bad input are acceptable
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn test_parse_decimal_valid() {
        assert_eq!(parse_decimal("123.456", "test"), Some(dec!(123.456)));
        assert_eq!(parse_decimal("0", "test"), Some(dec!(0)));
        assert_eq!(parse_decimal("-50.5", "test"), Some(dec!(-50.5)));
    }

    #[test]
    fn test_parse_decimal_invalid() {
        assert_eq!(parse_decimal("", "test"), None);
        assert_eq!(parse_decimal("abc", "test"), None);
        assert_eq!(parse_decimal("12.34.56", "test"), None);
    }

    #[test]
    fn test_parse_side() {
        assert_eq!(parse_side("B"), Some(Side::Buy));
        assert_eq!(parse_side("BUY"), Some(Side::Buy));
        assert_eq!(parse_side("buy"), Some(Side::Buy));
        assert_eq!(parse_side("A"), Some(Side::Sell));
        assert_eq!(parse_side("S"), Some(Side::Sell));
        assert_eq!(parse_side("SELL"), Some(Side::Sell));
        assert_eq!(parse_side("sell"), Some(Side::Sell));
        assert_eq!(parse_side("X"), None);
        assert_eq!(parse_side(""), None);
    }

    #[test]
    fn test_perp_coin_to_instrument() {
        let inst = perp_coin_to_instrument("BTC");
        assert_eq!(inst.as_ref(), "BTC-USD-PERP");

        let inst = perp_coin_to_instrument("ETH");
        assert_eq!(inst.as_ref(), "ETH-USD-PERP");
    }

    #[test]
    fn test_instrument_to_perp_coin() {
        let coin = instrument_to_perp_coin(&InstrumentNameExchange::from("BTC-USD-PERP"));
        assert_eq!(coin, "BTC");

        let coin = instrument_to_perp_coin(&InstrumentNameExchange::from("ETH-USD-PERP"));
        assert_eq!(coin, "ETH");

        // Just coin name without suffix
        let coin = instrument_to_perp_coin(&InstrumentNameExchange::from("SOL"));
        assert_eq!(coin, "SOL");
    }

    #[test]
    fn test_spot_coin_to_instrument() {
        let inst = spot_coin_to_instrument("PURR/USDC");
        assert_eq!(inst.as_ref(), "PURR-USDC-SPOT");

        let inst = spot_coin_to_instrument("HYPE/USDC");
        assert_eq!(inst.as_ref(), "HYPE-USDC-SPOT");
    }

    #[test]
    fn test_instrument_to_spot_coin() {
        let coin = instrument_to_spot_coin(&InstrumentNameExchange::from("PURR-USDC-SPOT"));
        assert_eq!(coin, Some("PURR/USDC".to_string()));

        let coin = instrument_to_spot_coin(&InstrumentNameExchange::from("HYPE-USDC-SPOT"));
        assert_eq!(coin, Some("HYPE/USDC".to_string()));

        // Malformed instruments return None
        assert_eq!(
            instrument_to_spot_coin(&InstrumentNameExchange::from("BTC-USD-PERP")),
            None
        );
        assert_eq!(
            instrument_to_spot_coin(&InstrumentNameExchange::from("INVALID")),
            None
        );
    }

    #[test]
    fn test_is_spot_coin() {
        assert!(is_spot_coin("PURR/USDC"));
        assert!(is_spot_coin("HYPE/USDC"));
        assert!(!is_spot_coin("BTC"));
        assert!(!is_spot_coin("ETH"));
    }

    #[test]
    fn test_round_to_5_sig_figs() {
        assert_eq!(round_to_5_sig_figs(dec!(0)), 0.0);
        assert_eq!(round_to_5_sig_figs(dec!(12345)), 12345.0);
        assert_eq!(round_to_5_sig_figs(dec!(123456)), 123460.0);
        assert_eq!(round_to_5_sig_figs(dec!(0.00012345)), 0.00012345);
        assert_eq!(round_to_5_sig_figs(dec!(0.000123456)), 0.00012346);
        assert_eq!(round_to_5_sig_figs(dec!(1.23456789)), 1.2346);
    }

    #[test]
    fn test_map_tif() {
        assert_eq!(
            map_tif(&TimeInForce::GoodUntilCancelled { post_only: false }),
            "Gtc"
        );
        assert_eq!(
            map_tif(&TimeInForce::GoodUntilCancelled { post_only: true }),
            "Alo"
        );
        assert_eq!(map_tif(&TimeInForce::ImmediateOrCancel), "Ioc");
        assert_eq!(map_tif(&TimeInForce::FillOrKill), "Ioc");
        assert_eq!(map_tif(&TimeInForce::GoodUntilEndOfDay), "Gtc");
    }

    #[test]
    fn test_millis_to_datetime() {
        let dt = millis_to_datetime(1714100000000).unwrap();
        assert_eq!(dt.timestamp_millis(), 1714100000000);

        // Zero timestamp (Unix epoch) is valid
        assert!(millis_to_datetime(0).is_some());
    }
}

#[cfg(test)]
// Test code: panics on bad input are acceptable
#[allow(clippy::unwrap_used)]
mod user_fills_tests {
    use super::*;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    /// One fill exactly as the `userFills` documentation shows it, including the two fields the
    /// SDK's own response type drops.
    const DOCUMENTED_FILL: &str = r#"{
        "closedPnl": "0.0",
        "coin": "AVAX",
        "crossed": false,
        "dir": "Open Long",
        "hash": "0xa166e3fa63c25663024b03f2e0da011a00307e4017465df020210d3d432e7cb8",
        "oid": 90542681,
        "px": "18.435",
        "side": "B",
        "startPosition": "26.86",
        "sz": "93.53",
        "time": 1681222254710,
        "fee": "0.01",
        "feeToken": "USDC",
        "builderFee": "0.01",
        "tid": 118906512037719
    }"#;

    /// Build an `InfoClient` pointed at `uri`. `InfoClient::new` opens no connection -- it only
    /// fills in the struct -- so this costs nothing and touches no network.
    async fn info_client_against(uri: String) -> hyperliquid_rust_sdk::InfoClient {
        let mut client = hyperliquid_rust_sdk::InfoClient::new(
            None,
            Some(hyperliquid_rust_sdk::BaseUrl::Localhost),
        )
        .await
        .unwrap();
        client.http_client.base_url = uri;
        client
    }

    async fn serve(body: String) -> (MockServer, hyperliquid_rust_sdk::InfoClient) {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/info"))
            .respond_with(ResponseTemplate::new(200).set_body_raw(body, "application/json"))
            .mount(&server)
            .await;
        let client = info_client_against(server.uri()).await;
        (server, client)
    }

    #[tokio::test]
    async fn a_fill_carries_the_tid_and_fee_token_the_sdk_type_drops() {
        let (_server, client) = serve(format!("[{DOCUMENTED_FILL}]")).await;

        let fills = user_fills(&client, ethers::types::H160::zero())
            .await
            .unwrap();

        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].tid, 118_906_512_037_719);
        assert_eq!(fills[0].fee_token.as_deref(), Some("USDC"));
        assert_eq!(fills[0].oid, 90_542_681);
        assert_eq!(fills[0].coin, "AVAX");
    }

    #[tokio::test]
    async fn one_transaction_can_carry_several_fills_and_they_are_told_apart_by_tid() {
        // A single aggressive order sweeping three resting orders: one `hash`, one `oid`, three
        // fills. This is the shape the old `TradeId(hash)` collapsed into one trade.
        let sweep = r#"[
            {"closedPnl":"0.0","coin":"AVAX","crossed":true,"dir":"Open Long",
             "hash":"0xsweep","oid":1,"px":"18.40","side":"B","startPosition":"0",
             "sz":"10","time":1681222254710,"fee":"0.01","feeToken":"USDC","tid":111},
            {"closedPnl":"0.0","coin":"AVAX","crossed":true,"dir":"Open Long",
             "hash":"0xsweep","oid":1,"px":"18.45","side":"B","startPosition":"10",
             "sz":"10","time":1681222254710,"fee":"0.01","feeToken":"USDC","tid":222},
            {"closedPnl":"0.0","coin":"AVAX","crossed":true,"dir":"Open Long",
             "hash":"0xsweep","oid":1,"px":"18.50","side":"B","startPosition":"20",
             "sz":"10","time":1681222254710,"fee":"0.01","feeToken":"USDC","tid":333}
        ]"#;
        let (_server, client) = serve(sweep.to_string()).await;

        let fills = user_fills(&client, ethers::types::H160::zero())
            .await
            .unwrap();

        let tids: Vec<u64> = fills.iter().map(|f| f.tid).collect();
        assert_eq!(tids, vec![111, 222, 333]);
    }

    #[tokio::test]
    async fn a_missing_fee_token_falls_back_rather_than_failing() {
        // `feeToken` is the one field here that nothing proves the endpoint sends, so its absence
        // must not cost the caller the whole response.
        let without = DOCUMENTED_FILL.replace(r#""feeToken": "USDC","#, "");
        let (_server, client) = serve(format!("[{without}]")).await;

        let fills = user_fills(&client, ethers::types::H160::zero())
            .await
            .unwrap();

        assert_eq!(fills.len(), 1);
        assert!(fills[0].fee_token.is_none());
    }

    #[tokio::test]
    async fn a_missing_tid_fails_loudly_instead_of_falling_back_to_the_hash() {
        // Silently reverting to `hash` would reinstate the duplicate-id defect, and it would only
        // become visible as fills going missing during reconciliation.
        let without = DOCUMENTED_FILL.replace(r#""tid": 118906512037719"#, r#""unused": 0"#);
        let (_server, client) = serve(format!("[{without}]")).await;

        let error = user_fills(&client, ethers::types::H160::zero())
            .await
            .unwrap_err();

        assert!(
            matches!(&error, UnindexedClientError::Internal(msg) if msg.contains("userFills")),
            "expected a parse failure naming the endpoint, got {error:?}"
        );
    }

    #[tokio::test]
    async fn the_request_names_the_address_in_full() {
        // `Display` for H160 abbreviates the middle of an address, which the venue would not
        // recognise. Only the `Debug` rendering is the full 0x-prefixed form.
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/info"))
            .and(wiremock::matchers::body_string_contains(
                "0x000000000000000000000000000000000000dead",
            ))
            .and(wiremock::matchers::body_string_contains("userFills"))
            .respond_with(ResponseTemplate::new(200).set_body_raw("[]", "application/json"))
            .mount(&server)
            .await;
        let client = info_client_against(server.uri()).await;

        let address = "0x000000000000000000000000000000000000dEaD"
            .parse::<ethers::types::H160>()
            .unwrap();

        // The mock only answers a body carrying the full address, so reaching `Ok` is the
        // assertion.
        assert!(user_fills(&client, address).await.unwrap().is_empty());
    }
}
