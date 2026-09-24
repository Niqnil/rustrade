//! Shared utilities for Hyperliquid execution clients (perps and spot).
//!
//! Contains parsing helpers and stream wrappers used by both
//! `HyperliquidClient` (perps) and `HyperliquidSpotClient`.
//!
//! Error mapping is in the `error` module.

use crate::client::hyperliquid::error::map_sdk_error;
use crate::error::{ApiError, OrderError, UnindexedClientError};
use crate::order::{
    Order, OrderKey, OrderKind, TimeInForce, UnindexedOrderSnapshot,
    id::{ClientOrderId, OrderId, StrategyId, VenueOrderId},
    state::{Cancelled, Filled, Open, OrderState},
};
use crate::{InstrumentAccountSnapshot, UnindexedAccountEvent};
use chrono::{DateTime, TimeZone, Utc};
use futures::Stream;
use rust_decimal::Decimal;
use rustrade_instrument::{
    Side, asset::name::AssetNameExchange, exchange::ExchangeId,
    instrument::name::InstrumentNameExchange,
};
use serde::de::DeserializeOwned;
use smol_str::format_smolstr;
use std::collections::{HashMap, HashSet};
use std::pin::Pin;
use std::str::FromStr;
use std::task::{Context, Poll};
use tokio_util::sync::CancellationToken;
use tracing::warn;
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
    user_info(info_client, "userFills", address).await
}

/// One order from the `openOrders` info endpoint, as the venue actually returns it.
///
/// # Why this exists rather than `hyperliquid_rust_sdk::OpenOrdersResponse`
///
/// The SDK's type does not model `cloid`, the client order id the order was placed with, nor
/// `origSz`. The venue sends both, although its documentation lists neither. Without `cloid` an
/// order can only be reported under its venue `oid`, which is not the id the engine tracks it by.
///
/// Drop this in favour of the SDK type if it ever models `cloid`.
#[derive(Debug, Clone, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OpenOrder {
    pub coin: String,
    pub side: String,
    pub limit_px: String,
    /// Quantity still to fill.
    pub sz: String,
    pub oid: u64,
    pub timestamp: u64,

    /// Quantity the order was placed for.
    ///
    /// Optional because the endpoint's documentation does not list it. Without it the order's
    /// filled quantity is unknown, and it is reported as its remaining quantity with nothing
    /// filled.
    #[serde(default)]
    pub orig_sz: Option<String>,

    /// The client order id the order was placed with, as `0x` and 32 hex digits (see
    /// [`cloid_to_cid`]). Absent for an order placed without one.
    #[serde(default)]
    pub cloid: Option<String>,
}

/// Fetch every open order for `address`, parsed into [`OpenOrder`] rather than the SDK's lossy
/// type, which drops the `cloid`.
///
/// # Errors
///
/// Returns a transport error if the POST fails, or a parse error if the response does not match
/// [`OpenOrder`].
pub async fn open_orders(
    info_client: &hyperliquid_rust_sdk::InfoClient,
    address: ethers::types::H160,
) -> Result<Vec<OpenOrder>, UnindexedClientError> {
    user_info(info_client, "openOrders", address).await
}

/// POST the info request `{"type": kind, "user": address}` and parse the response as `T`.
///
/// Issued through the SDK's own [`HttpClient`](hyperliquid_rust_sdk::InfoClient::http_client), so
/// base URL, TLS configuration and error type are exactly those of every other info request this
/// client makes — only the response type differs.
async fn user_info<T: DeserializeOwned>(
    info_client: &hyperliquid_rust_sdk::InfoClient,
    kind: &str,
    address: ethers::types::H160,
) -> Result<T, UnindexedClientError> {
    // `{:?}` on H160 renders the checksummed 0x-prefixed form the endpoint expects. `Display`
    // abbreviates the middle of the address ("0x1234…5678"), so it must not be used here.
    let body = format!(r#"{{"type":"{kind}","user":"{address:?}"}}"#);

    let raw = info_client
        .http_client
        .post("/info", body)
        .await
        .map_err(map_sdk_error)?;

    // `Internal` rather than a connectivity error: a response that does not parse is a schema
    // change or a venue-side regression, not a transient fault, and retrying will not fix it.
    serde_json::from_str(&raw).map_err(|e| {
        UnindexedClientError::Internal(format!("Hyperliquid {kind} response did not parse: {e}"))
    })
}

/// Convert one [`OpenOrder`] into an `Open` order on `instrument`, keyed as [`record_cid`] says.
///
/// `None`, with a `warn!`, when a field does not parse. The caller then cannot say the order is
/// not open, so it must not declare its list complete.
pub(super) fn open_order_to_order(
    row: &OpenOrder,
    exchange: ExchangeId,
    instrument: InstrumentNameExchange,
) -> Option<Order<ExchangeId, InstrumentNameExchange, Open>> {
    let parsed = (|| {
        let cid = record_cid(row.cloid.as_deref(), row.oid)?;
        let side = parse_side(&row.side)?;
        let price = parse_decimal(&row.limit_px, "limitPx")?;
        let remaining = parse_decimal(&row.sz, "sz")?;
        let quantity = match &row.orig_sz {
            Some(orig_sz) => parse_decimal(orig_sz, "origSz")?,
            None => remaining,
        };
        let time_exchange = millis_to_datetime(row.timestamp)?;
        Some((cid, side, price, quantity, remaining, time_exchange))
    })();

    let Some((cid, side, price, quantity, remaining, time_exchange)) = parsed else {
        warn!(
            %exchange,
            %instrument,
            oid = row.oid,
            order = ?row,
            "Hyperliquid open order did not convert - leaving it out, so this instrument's order \
             list is not complete"
        );
        return None;
    };

    Some(Order {
        key: OrderKey {
            exchange,
            instrument,
            strategy: StrategyId::unknown(),
            cid,
        },
        side,
        price: Some(price),
        quantity,
        // Neither the kind nor the time in force is in this response.
        kind: OrderKind::Limit,
        time_in_force: TimeInForce::GoodUntilCancelled { post_only: false },
        state: Open {
            id: VenueOrderId::Assigned(OrderId(format_smolstr!("{}", row.oid))),
            time_exchange,
            filled_quantity: (quantity - remaining).max(Decimal::ZERO),
        },
    })
}

type UnindexedInstrumentAccountSnapshot =
    InstrumentAccountSnapshot<ExchangeId, AssetNameExchange, InstrumentNameExchange>;

/// The open orders of an account snapshot, one entry per instrument.
pub(super) struct OpenOrderListing(
    HashMap<InstrumentNameExchange, UnindexedInstrumentAccountSnapshot>,
);

impl OpenOrderListing {
    /// List `rows` by instrument, keeping only the rows `to_instrument` maps to an instrument in
    /// `requested` (every instrument when `requested` is empty).
    ///
    /// Each entry declares its orders complete unless one of its rows failed to convert. That
    /// holds because the endpoint returns every open order, unpaged, and because [`record_cid`]
    /// reports each order placed through this client under the id it was placed with.
    ///
    /// Every requested instrument gets an entry, open orders or not. An entry is what lets the
    /// engine judge an order that is no longer listed to be gone, and the instrument whose last
    /// order was cancelled while the account stream was down is exactly the one with nothing
    /// left to list. With `requested` empty there is nothing to enumerate, so only instruments
    /// with open orders get one.
    pub(super) fn new(
        rows: &[OpenOrder],
        exchange: ExchangeId,
        requested: &[InstrumentNameExchange],
        to_instrument: impl Fn(&str) -> Option<InstrumentNameExchange>,
    ) -> Self {
        fn entry<'a>(
            listing: &'a mut HashMap<InstrumentNameExchange, UnindexedInstrumentAccountSnapshot>,
            instrument: &InstrumentNameExchange,
        ) -> &'a mut UnindexedInstrumentAccountSnapshot {
            listing
                .entry(instrument.clone())
                .or_insert_with(|| InstrumentAccountSnapshot {
                    instrument: instrument.clone(),
                    orders: Vec::new(),
                    orders_complete: true,
                    position: None,
                    isolated: None,
                })
        }

        let requested_set = requested.iter().collect::<HashSet<_>>();
        let mut listing = HashMap::with_capacity(requested.len());

        for instrument in requested {
            entry(&mut listing, instrument);
        }

        for row in rows {
            let Some(instrument) = to_instrument(&row.coin) else {
                continue;
            };
            if !requested_set.is_empty() && !requested_set.contains(&instrument) {
                continue;
            }
            let order = open_order_to_order(row, exchange, instrument.clone());
            let snapshot = entry(&mut listing, &instrument);
            match order {
                Some(order) => snapshot.orders.push(Order {
                    key: order.key,
                    side: order.side,
                    price: order.price,
                    quantity: order.quantity,
                    kind: order.kind,
                    time_in_force: order.time_in_force,
                    state: OrderState::active(order.state),
                }),
                None => snapshot.orders_complete = false,
            }
        }

        Self(listing)
    }

    /// Take the entry for `instrument`, if it has one.
    pub(super) fn remove(
        &mut self,
        instrument: &InstrumentNameExchange,
    ) -> Option<UnindexedInstrumentAccountSnapshot> {
        self.0.remove(instrument)
    }

    /// Every remaining entry.
    pub(super) fn into_snapshots(self) -> impl Iterator<Item = UnindexedInstrumentAccountSnapshot> {
        self.0.into_values()
    }
}

/// What a Hyperliquid order status says about the order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OrderStatus {
    Open,
    Filled,
    Cancelled,
    Rejected,
}

impl OrderStatus {
    /// Classify an `orderUpdates` / `orderStatus` status string.
    ///
    /// Beyond `open`, `filled`, `canceled` and `rejected`, Hyperliquid names the reason for each
    /// other way an order ends: `marginCanceled`, `selfTradeCanceled`, `reduceOnlyCanceled`,
    /// `tickRejected`, `badAloPxRejected` and a dozen more. They are classified by their suffix
    /// so that a reason added later still ends the order, rather than leaving it tracked as open
    /// for good. `scheduledCancel` is the one ending that breaks the pattern.
    ///
    /// `triggered` is a trigger order whose condition was met. It is still working, and ends
    /// with a later status of its own.
    fn classify(status: &str) -> Option<Self> {
        match status {
            "open" | "triggered" => Some(Self::Open),
            "filled" => Some(Self::Filled),
            "canceled" | "scheduledCancel" => Some(Self::Cancelled),
            "rejected" => Some(Self::Rejected),
            status if status.ends_with("Canceled") => Some(Self::Cancelled),
            status if status.ends_with("Rejected") => Some(Self::Rejected),
            _ => None,
        }
    }
}

/// Convert an `orderUpdates` message into an order snapshot event on `instrument`, keyed as
/// [`record_cid`] says.
///
/// `None`, with a `warn!` naming the cause where the parse helpers do not, when a field does not
/// parse or the status is not one [`OrderStatus::classify`] recognises.
pub(super) fn order_update_to_account_event(
    update: &hyperliquid_rust_sdk::OrderUpdate,
    exchange: ExchangeId,
    instrument: InstrumentNameExchange,
) -> Option<UnindexedAccountEvent> {
    let order = &update.order;
    let Some(status) = OrderStatus::classify(&update.status) else {
        warn!(%exchange, status = %update.status, oid = order.oid, "Unknown Hyperliquid order status - ignoring the update");
        return None;
    };
    let cid = record_cid(order.cloid.as_deref(), order.oid)?;
    let side = parse_side(&order.side)?;
    let price = parse_decimal(&order.limit_px, "order.limitPx")?;
    let orig_sz = parse_decimal(&order.orig_sz, "order.origSz")?;
    let Some(time_exchange) = millis_to_datetime(update.status_timestamp) else {
        warn!(%exchange, oid = order.oid, status_timestamp = update.status_timestamp, "Invalid Hyperliquid order update timestamp - ignoring the update");
        return None;
    };
    let order_id = OrderId(format_smolstr!("{}", order.oid));
    let filled_quantity = || {
        parse_decimal(&order.sz, "order.sz")
            .map(|remaining| (orig_sz - remaining).max(Decimal::ZERO))
    };

    let state = match status {
        OrderStatus::Open => OrderState::active(Open {
            id: VenueOrderId::Assigned(order_id),
            time_exchange,
            filled_quantity: filled_quantity()?,
        }),
        OrderStatus::Filled => OrderState::fully_filled(Filled::new(
            order_id,
            time_exchange,
            orig_sz,
            None, // The update does not carry the average price.
        )),
        OrderStatus::Cancelled => {
            OrderState::inactive(Cancelled::new(order_id, time_exchange, filled_quantity()?))
        }
        OrderStatus::Rejected => OrderState::inactive(OrderError::Rejected(
            ApiError::OrderRejected(update.status.clone()),
        )),
    };

    // The update carries neither the order's kind nor its time in force.
    let snapshot: UnindexedOrderSnapshot = Order {
        key: OrderKey {
            exchange,
            instrument,
            strategy: StrategyId::unknown(),
            cid,
        },
        side,
        price: Some(price),
        quantity: orig_sz,
        kind: OrderKind::Limit,
        time_in_force: TimeInForce::GoodUntilCancelled { post_only: false },
        state,
    };

    Some(crate::AccountEvent::new(
        exchange,
        crate::AccountEventKind::OrderSnapshot(
            rustrade_integration::collection::snapshot::Snapshot(snapshot),
        ),
    ))
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

/// Why an order is refused when its client id cannot be a cloid ([`cid_to_cloid`]).
pub(super) const CLOID_REQUIRED: &str = "Hyperliquid requires the client order id to be a UUID in \
     canonical form, as ClientOrderId::uuid() makes: the venue reports each order under that id, \
     and an order under any other id could not be matched to its own updates";

/// The cloid to place an order under `cid` with, or `None` if `cid` cannot be one.
///
/// Hyperliquid stores a cloid as 16 bytes and reports it back as `0x` and 32 lowercase hex
/// digits, which [`cloid_to_cid`] turns back into a client id. Only a `cid` that is the canonical
/// text of a UUID — lowercase and hyphenated, as [`ClientOrderId::uuid`] makes — comes back
/// unchanged. Any other spelling of a UUID would come back as a different id, under which the
/// order's own updates could not find it, so it is refused along with every id that is not a UUID.
pub fn cid_to_cloid(cid: &ClientOrderId) -> Option<Uuid> {
    let uuid = Uuid::try_parse(cid.0.as_str()).ok()?;
    let mut canonical = Uuid::encode_buffer();
    (uuid.hyphenated().encode_lower(&mut canonical) == cid.0.as_str()).then_some(uuid)
}

/// The client id an order placed with `cloid` was placed under: the inverse of [`cid_to_cloid`].
///
/// `cloid` is the venue's form, `0x` and 32 hex digits. `None` for anything else.
pub fn cloid_to_cid(cloid: &str) -> Option<ClientOrderId> {
    let hex = cloid.strip_prefix("0x")?;
    if hex.len() != 32 {
        return None;
    }
    // The 32-digit length check rules out the other spellings `try_parse` accepts.
    let uuid = Uuid::try_parse(hex).ok()?;
    Some(ClientOrderId::new(format_smolstr!("{}", uuid.hyphenated())))
}

/// The client id to report a venue order record under, given the record's `cloid` and `oid`.
///
/// Every order this client places carries a cloid ([`cid_to_cloid`]), so a record with one is
/// reported under the id the order was placed with. A record without one was placed some other
/// way, such as the web app, and has no client id at all. It is reported under its venue `oid`,
/// the only identifier it has, which is stable across reads so the order is tracked once.
///
/// `None`, with a `warn!`, for a cloid that is not in the venue's form. The record's order is
/// then unidentified, and must be left out rather than reported under a guess.
pub(super) fn record_cid(cloid: Option<&str>, oid: u64) -> Option<ClientOrderId> {
    match cloid {
        None => Some(ClientOrderId::new(format_smolstr!("{oid}"))),
        Some(cloid) => {
            let cid = cloid_to_cid(cloid);
            if cid.is_none() {
                warn!(%cloid, oid, "Hyperliquid order carries a cloid that is not 0x and 32 hex digits");
            }
            cid
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
mod info_tests {
    use super::*;
    use rust_decimal_macros::dec;
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

    // ---- client order ids -------------------------------------------------------------------

    const CID: &str = "85b60bff-64d5-49ec-95af-6268d7d1df63";
    /// `CID` as the venue reports it back: `0x` and the 32 lowercase hex digits.
    const CLOID: &str = "0x85b60bff64d549ec95af6268d7d1df63";

    #[test]
    fn only_the_canonical_uuid_form_can_be_a_cloid() {
        let cloid = cid_to_cloid(&ClientOrderId::new(CID)).unwrap();
        assert_eq!(cloid.to_string(), CID);

        // Every other spelling of the same UUID would come back as `CID`, not as itself.
        for other in [
            "85B60BFF-64D5-49EC-95AF-6268D7D1DF63",
            "85b60bff64d549ec95af6268d7d1df63",
            "{85b60bff-64d5-49ec-95af-6268d7d1df63}",
            "urn:uuid:85b60bff-64d5-49ec-95af-6268d7d1df63",
            "example-1714100000000",
            "",
        ] {
            assert_eq!(cid_to_cloid(&ClientOrderId::new(other)), None, "{other}");
        }
    }

    #[test]
    fn a_generated_uuid_client_id_is_always_a_cloid() {
        for _ in 0..32 {
            let cid = ClientOrderId::uuid();
            assert!(cid_to_cloid(&cid).is_some(), "{cid}");
        }
    }

    #[test]
    fn the_reported_cloid_is_the_client_id_the_order_was_placed_under() {
        assert_eq!(cloid_to_cid(CLOID), Some(ClientOrderId::new(CID)));

        // The round trip through the form the SDK sends, for a fresh id.
        let cid = ClientOrderId::uuid();
        let sent = format!("0x{}", cid_to_cloid(&cid).unwrap().simple());
        assert_eq!(cloid_to_cid(&sent), Some(cid));
    }

    #[test]
    fn a_cloid_not_in_the_venue_form_names_no_client_id() {
        for malformed in [
            CID,
            "85b60bff64d549ec95af6268d7d1df63",
            "0x85b60bff64d549ec95af6268d7d1df",
            "0x85b60bff64d549ec95af6268d7d1df6300",
            "0x85b60bff-64d5-49ec-95af-6268d7d1df63",
            "0xzzb60bff64d549ec95af6268d7d1df63",
            "0x",
        ] {
            assert_eq!(cloid_to_cid(malformed), None, "{malformed}");
        }
    }

    #[test]
    fn a_record_without_a_cloid_is_reported_under_its_oid() {
        assert_eq!(record_cid(None, 42), Some(ClientOrderId::new("42")));
        assert_eq!(record_cid(Some(CLOID), 42), Some(ClientOrderId::new(CID)));
        assert_eq!(record_cid(Some("0x1234"), 42), None);
    }

    // ---- order statuses -----------------------------------------------------------------------

    #[test]
    fn every_documented_order_status_is_classified() {
        use OrderStatus::*;
        for (status, expected) in [
            ("open", Open),
            ("triggered", Open),
            ("filled", Filled),
            ("canceled", Cancelled),
            ("marginCanceled", Cancelled),
            ("vaultWithdrawalCanceled", Cancelled),
            ("openInterestCapCanceled", Cancelled),
            ("selfTradeCanceled", Cancelled),
            ("reduceOnlyCanceled", Cancelled),
            ("siblingFilledCanceled", Cancelled),
            ("delistedCanceled", Cancelled),
            ("liquidatedCanceled", Cancelled),
            ("scheduledCancel", Cancelled),
            ("rejected", Rejected),
            ("tickRejected", Rejected),
            ("minTradeNtlRejected", Rejected),
            ("perpMarginRejected", Rejected),
            ("reduceOnlyRejected", Rejected),
            ("badAloPxRejected", Rejected),
            ("iocCancelRejected", Rejected),
            ("badTriggerPxRejected", Rejected),
            ("marketOrderNoLiquidityRejected", Rejected),
            ("positionIncreaseAtOpenInterestCapRejected", Rejected),
            ("positionFlipAtOpenInterestCapRejected", Rejected),
            ("tooAggressiveAtOpenInterestCapRejected", Rejected),
            ("openInterestIncreaseRejected", Rejected),
            ("insufficientSpotBalanceRejected", Rejected),
            ("oracleRejected", Rejected),
            ("perpMaxPositionRejected", Rejected),
        ] {
            assert_eq!(OrderStatus::classify(status), Some(expected), "{status}");
        }
        assert_eq!(OrderStatus::classify("unknown_status"), None);
    }

    fn order_update(status: &str, cloid: &str, sz: &str) -> hyperliquid_rust_sdk::OrderUpdate {
        serde_json::from_str(&format!(
            r#"{{"order": {{"coin": "ETH", "side": "B", "limitPx": "1605.0", "sz": "{sz}",
                 "oid": 7001, "timestamp": 1714100000000, "origSz": "0.0075", "cloid": {cloid}}},
               "status": "{status}", "statusTimestamp": 1714100001000}}"#
        ))
        .unwrap()
    }

    fn snapshot_of(event: UnindexedAccountEvent) -> UnindexedOrderSnapshot {
        match event.kind {
            crate::AccountEventKind::OrderSnapshot(snapshot) => snapshot.0,
            other => panic!("expected an order snapshot, got {other:?}"),
        }
    }

    fn eth() -> InstrumentNameExchange {
        InstrumentNameExchange::from("ETH-USD-PERP")
    }

    #[test]
    fn an_order_update_names_the_order_by_the_id_it_was_placed_under() {
        let update = order_update("open", &format!(r#""{CLOID}""#), "0.005");
        let order = snapshot_of(
            order_update_to_account_event(&update, ExchangeId::HyperliquidPerp, eth()).unwrap(),
        );

        assert_eq!(order.key.cid, ClientOrderId::new(CID));
        assert_eq!(order.quantity, dec!(0.0075));
        match order.state {
            OrderState::Active(crate::order::state::ActiveOrderState::Open(open)) => {
                assert_eq!(open.id, VenueOrderId::Assigned(OrderId::new("7001")));
                assert_eq!(open.filled_quantity, dec!(0.0025));
            }
            other => panic!("expected Open, got {other:?}"),
        }
    }

    #[test]
    fn an_order_update_without_a_cloid_is_named_by_its_oid() {
        let update = order_update("open", "null", "0.0075");
        let order = snapshot_of(
            order_update_to_account_event(&update, ExchangeId::HyperliquidPerp, eth()).unwrap(),
        );
        assert_eq!(order.key.cid, ClientOrderId::new("7001"));
    }

    #[test]
    fn an_order_update_with_a_malformed_cloid_is_dropped() {
        let update = order_update("open", r#""0x1234""#, "0.0075");
        assert!(
            order_update_to_account_event(&update, ExchangeId::HyperliquidPerp, eth()).is_none()
        );
    }

    #[test]
    fn a_cancellation_for_any_reason_ends_the_order() {
        let update = order_update("selfTradeCanceled", &format!(r#""{CLOID}""#), "0.005");
        let order = snapshot_of(
            order_update_to_account_event(&update, ExchangeId::HyperliquidPerp, eth()).unwrap(),
        );
        match order.state {
            OrderState::Inactive(crate::order::state::InactiveOrderState::Cancelled(cancelled)) => {
                assert_eq!(cancelled.filled_quantity, dec!(0.0025));
            }
            other => panic!("expected Cancelled, got {other:?}"),
        }
    }

    #[test]
    fn a_rejection_ends_the_order_and_names_the_reason() {
        let update = order_update("badAloPxRejected", &format!(r#""{CLOID}""#), "0.0075");
        let order = snapshot_of(
            order_update_to_account_event(&update, ExchangeId::HyperliquidPerp, eth()).unwrap(),
        );
        match order.state {
            OrderState::Inactive(crate::order::state::InactiveOrderState::OpenFailed(
                OrderError::Rejected(ApiError::OrderRejected(reason)),
            )) => assert_eq!(reason, "badAloPxRejected"),
            other => panic!("expected a rejection, got {other:?}"),
        }
    }

    // ---- open orders --------------------------------------------------------------------------

    /// The shape `openOrders` was observed to return: `origSz` and `cloid` included, although the
    /// endpoint's documentation lists neither. Values are synthetic.
    const OPEN_ORDERS: &str = r#"[
        {"coin":"ETH","side":"B","limitPx":"1605.0","sz":"0.005","oid":7001,
         "timestamp":1714100000000,"origSz":"0.0075","cloid":"0x85b60bff64d549ec95af6268d7d1df63"},
        {"coin":"ETH","side":"A","limitPx":"1700.0","sz":"0.01","oid":7002,
         "timestamp":1714100000000,"origSz":"0.01"},
        {"coin":"BTC","side":"A","limitPx":"50000.0","sz":"0.001","oid":7003,
         "timestamp":1714100000000}
    ]"#;

    #[tokio::test]
    async fn open_orders_carry_the_cloid_and_original_size_the_sdk_type_drops() {
        let (_server, client) = serve(OPEN_ORDERS.to_string()).await;

        let rows = open_orders(&client, ethers::types::H160::zero())
            .await
            .unwrap();

        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].cloid.as_deref(), Some(CLOID));
        assert_eq!(rows[0].orig_sz.as_deref(), Some("0.0075"));
        assert_eq!(rows[1].cloid, None);
        assert_eq!(rows[2].orig_sz, None);
    }

    fn rows() -> Vec<OpenOrder> {
        serde_json::from_str(OPEN_ORDERS).unwrap()
    }

    fn perp(coin: &str) -> Option<InstrumentNameExchange> {
        Some(perp_coin_to_instrument(coin))
    }

    #[test]
    fn an_open_order_is_listed_under_its_client_id_with_its_fill_so_far() {
        let order = open_order_to_order(&rows()[0], ExchangeId::HyperliquidPerp, eth()).unwrap();

        assert_eq!(order.key.cid, ClientOrderId::new(CID));
        assert_eq!(order.quantity, dec!(0.0075));
        assert_eq!(order.state.filled_quantity, dec!(0.0025));
        assert_eq!(order.state.id, VenueOrderId::Assigned(OrderId::new("7001")));
    }

    #[test]
    fn an_open_order_without_its_original_size_is_its_remainder_with_nothing_filled() {
        let row = &rows()[2];
        let btc = InstrumentNameExchange::from("BTC-USD-PERP");
        let order = open_order_to_order(row, ExchangeId::HyperliquidPerp, btc).unwrap();

        assert_eq!(order.key.cid, ClientOrderId::new("7003"));
        assert_eq!(order.quantity, dec!(0.001));
        assert_eq!(order.state.filled_quantity, Decimal::ZERO);
    }

    #[test]
    fn every_requested_instrument_is_listed_complete_even_with_nothing_open() {
        let sol = InstrumentNameExchange::from("SOL-USD-PERP");
        let mut listing = OpenOrderListing::new(
            &rows(),
            ExchangeId::HyperliquidPerp,
            &[eth(), sol.clone()],
            perp,
        );

        let eth_entry = listing.remove(&eth()).unwrap();
        assert!(eth_entry.orders_complete);
        assert_eq!(eth_entry.orders.len(), 2);

        // Nothing open: the entry is what lets an order that ended offline be seen to be gone.
        let sol_entry = listing.remove(&sol).unwrap();
        assert!(sol_entry.orders_complete);
        assert!(sol_entry.orders.is_empty());

        // BTC was not requested.
        assert_eq!(listing.into_snapshots().count(), 0);
    }

    #[test]
    fn an_unfiltered_listing_covers_every_instrument_with_open_orders() {
        let listing = OpenOrderListing::new(&rows(), ExchangeId::HyperliquidPerp, &[], perp);
        let mut instruments = listing
            .into_snapshots()
            .map(|entry| entry.instrument.to_string())
            .collect::<Vec<_>>();
        instruments.sort();
        assert_eq!(instruments, ["BTC-USD-PERP", "ETH-USD-PERP"]);
    }

    #[test]
    fn an_order_that_does_not_convert_leaves_only_its_instrument_incomplete() {
        let mut rows = rows();
        rows[1].side = "?".to_string();
        let btc = InstrumentNameExchange::from("BTC-USD-PERP");
        let mut listing = OpenOrderListing::new(
            &rows,
            ExchangeId::HyperliquidPerp,
            &[eth(), btc.clone()],
            perp,
        );

        let eth_entry = listing.remove(&eth()).unwrap();
        assert!(!eth_entry.orders_complete);
        assert_eq!(eth_entry.orders.len(), 1);
        assert!(listing.remove(&btc).unwrap().orders_complete);
    }

    #[test]
    fn an_order_with_a_malformed_cloid_leaves_its_instrument_incomplete() {
        let mut rows = rows();
        rows[0].cloid = Some("0x1234".to_string());
        let mut listing = OpenOrderListing::new(&rows, ExchangeId::HyperliquidPerp, &[eth()], perp);

        let eth_entry = listing.remove(&eth()).unwrap();
        assert!(!eth_entry.orders_complete);
        assert_eq!(eth_entry.orders.len(), 1);
    }

    #[test]
    fn coins_the_client_does_not_trade_are_left_out() {
        let listing = OpenOrderListing::new(&rows(), ExchangeId::HyperliquidSpot, &[], |coin| {
            is_spot_coin(coin).then(|| spot_coin_to_instrument(coin))
        });
        assert_eq!(listing.into_snapshots().count(), 0);
    }
}
