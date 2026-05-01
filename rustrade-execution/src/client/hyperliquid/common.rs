//! Shared utilities for Hyperliquid execution clients (perps and spot).
//!
//! Contains parsing helpers and stream wrappers used by both
//! `HyperliquidClient` (perps) and `HyperliquidSpotClient`.
//!
//! Error mapping is in the `error` module.

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
