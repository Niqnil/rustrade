//! Historical klines (OHLCV candles) via Binance's **public, unauthenticated**
//! REST endpoints.
//!
//! Gives consumers free historical candle data for research/backtest on both
//! [`BinanceSpot`](crate::exchange::binance::spot::BinanceSpot) and
//! [`BinanceFuturesUsd`](crate::exchange::binance::futures::BinanceFuturesUsd) — no API key, no paid
//! data subscription. Construct a client for the surface you want and call
//! [`fetch_candles`](BinanceHistoricalClient::fetch_candles):
//!
//! ```ignore
//! use rustrade_data::exchange::binance::historical::BinanceHistoricalClient;
//! use rustrade_data::subscription::candle::CandleInterval;
//! use chrono::{Duration, Utc};
//! use futures::StreamExt;
//!
//! let client = BinanceHistoricalClient::spot();
//! let end = Utc::now();
//! let start = end - Duration::days(1);
//!
//! let mut stream = client.fetch_candles("BTCUSDT", CandleInterval::Min1, start, end);
//! while let Some(candle) = stream.next().await {
//!     println!("{:?}", candle?);
//! }
//! ```
//!
//! # Two surfaces, one mapping
//!
//! Spot and futures return the **same** array-of-arrays row shape and share one
//! row→[`Candle`](crate::subscription::candle::Candle) mapping. They differ only on host, page cap, and URL params:
//!
//! | Surface | Endpoint | Host | Page cap | Market param |
//! |---|---|---|---|---|
//! | [`spot`](BinanceHistoricalClient::spot) | `/api/v3/klines` | `api.binance.com` | 1000 | `symbol` |
//! | [`futures`](BinanceHistoricalClient::futures) | `/fapi/v1/continuousKlines` | `fapi.binance.com` | 1500 | `pair` + `contractType=PERPETUAL` |
//!
//! The futures path uses the **continuous-contract** surface (`contractType=PERPETUAL`)
//! rather than `/fapi/v1/klines`. For a perpetual this is the same data as the
//! symbol klines **plus** sub-minute resolutions: `/fapi/v1/klines` returns
//! `400 Invalid interval` for [`Sec1`](crate::subscription::candle::CandleInterval::Sec1), whereas the
//! continuous surface serves genuine 1-second candles.
//!
//! # Rate limits & resumable backfill
//!
//! On HTTP `429`/`418` the stream yields
//! [`BinanceDataError::RateLimited`] and **ends** — it never waits, retries, or
//! runs a process-global limiter (the consumer owns retry/backoff). The stream is
//! **resumable**: on a `RateLimited` error, wait `retry_after`, then re-invoke
//! `fetch_candles` with `start` advanced to **1ms past the last `close_time`
//! already received** (`last_close_time + 1ms`, i.e. the next candle's open). The
//! `[start, end]` range is `close_time`-inclusive, so resuming exactly at
//! `last_close_time` would re-yield that final candle; the `+1ms` step skips it
//! without leaving a gap. No progress is lost — pagination keys off `open_time`,
//! and `open ≡ close − interval`.
//!
//! A long unattended backfill (a 90-day `1s` series is ≈ 7.8M candles over
//! thousands of requests) will **not** "just work" without that resume loop on
//! the consumer side, but the default [pre-pacing](BinanceHistoricalClient#pre-pacing)
//! keeps a single steady backfill within Binance's weight budget so the common
//! case rarely trips a `429` in the first place.

use super::error::BinanceDataError;
use crate::subscription::candle::{
    Candle, CandleInterval, close_time_from_open, open_time_from_close,
};
use async_stream::try_stream;
use chrono::{DateTime, Utc};
use futures::{Stream, StreamExt};
use reqwest::{Client, StatusCode};
use rust_decimal::Decimal;
use serde::Deserialize;
use std::time::Duration;
use tracing::debug;

/// Spot REST base URL (`/api/v3/klines`).
const SPOT_BASE_URL: &str = "https://api.binance.com";
/// USD-M futures REST base URL (`/fapi/v1/continuousKlines`).
const FUTURES_BASE_URL: &str = "https://fapi.binance.com";

/// Maximum klines per page on the spot surface.
const SPOT_PAGE_LIMIT: u32 = 1000;
/// Maximum klines per page on the futures continuous surface.
const FUTURES_PAGE_LIMIT: u32 = 1500;

/// Default inter-page pace for the **spot** surface.
///
/// Spot `/api/v3/klines` is flat **weight 2/req** against the IP budget of
/// ~6,000 weight/min ⇒ ~3,000 req/min. A ~20ms floor keeps a single backfill
/// comfortably under that (throughput ceiling ≈ 3M candles/min at 1000/page).
const DEFAULT_SPOT_PACE: Duration = Duration::from_millis(20);

/// Default inter-page pace for the **futures** continuous surface.
///
/// Futures `/fapi/v1/continuousKlines` weight is **limit-based** — weight 10/req
/// at the 1500/page max — against a *lower* ~2,400 weight/min budget ⇒ only
/// ~240 req/min. Futures is therefore far more request-constrained than spot
/// despite its bigger page, so its default pace is ~12× larger (~250ms ⇒
/// throughput ≈ 360k candles/min).
const DEFAULT_FUTURES_PACE: Duration = Duration::from_millis(250);

/// Per-request HTTP timeout.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Which Binance REST surface a [`BinanceHistoricalClient`] targets.
///
/// Selects the endpoint path, page cap, and market query parameter. Set once at
/// construction via [`BinanceHistoricalClient::spot`] / [`BinanceHistoricalClient::futures`].
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
enum Surface {
    /// Spot `/api/v3/klines` (`symbol=`, max 1000/page).
    Spot,
    /// USD-M futures continuous contract `/fapi/v1/continuousKlines`
    /// (`pair=` + `contractType=PERPETUAL`, max 1500/page).
    FuturesContinuous,
}

impl Surface {
    /// Maximum klines returned per page on this surface.
    fn page_limit(self) -> u32 {
        match self {
            Surface::Spot => SPOT_PAGE_LIMIT,
            Surface::FuturesContinuous => FUTURES_PAGE_LIMIT,
        }
    }
}

/// REST client for Binance historical klines on a single surface (spot **or**
/// futures-continuous). Construct with [`spot`](Self::spot) or
/// [`futures`](Self::futures); both bake in the surface's host, page cap, and a
/// conservative default [pre-pace](#pre-pacing).
///
/// # Pre-pacing
///
/// A fixed, bounded delay is applied **between pages** so a single backfill stays
/// within Binance's weight budget without tripping `429`/`418`. It is
/// `tracing`-observable (logged at `debug`) and **caller-overridable** via
/// [`with_pace`](Self::with_pace). This is *proactive courtesy only* — it never
/// inspects a `429`, never retries, and never adapts to `retry_after`; the
/// surface-`RateLimited`-and-end contract is unchanged.
#[derive(Clone, Debug)]
pub struct BinanceHistoricalClient {
    client: Client,
    base_url: String,
    surface: Surface,
    pace: Duration,
}

impl BinanceHistoricalClient {
    /// Create a client for the **spot** surface (`/api/v3/klines`, max 1000/page,
    /// default pace ~20ms).
    #[must_use]
    pub fn spot() -> Self {
        Self {
            client: Client::new(),
            base_url: SPOT_BASE_URL.to_owned(),
            surface: Surface::Spot,
            pace: DEFAULT_SPOT_PACE,
        }
    }

    /// Create a client for the **USD-M futures continuous** surface
    /// (`/fapi/v1/continuousKlines`, `contractType=PERPETUAL`, max 1500/page,
    /// default pace ~250ms).
    ///
    /// This is the surface that unlocks [`Sec1`](CandleInterval::Sec1) on
    /// futures; `/fapi/v1/klines` rejects it with `400 Invalid interval`.
    #[must_use]
    pub fn futures() -> Self {
        Self {
            client: Client::new(),
            base_url: FUTURES_BASE_URL.to_owned(),
            surface: Surface::FuturesContinuous,
            pace: DEFAULT_FUTURES_PACE,
        }
    }

    /// Override the inter-page [pre-pace](#pre-pacing).
    ///
    /// Use a smaller value on a higher API weight tier, or [`Duration::ZERO`] to
    /// disable pacing entirely (the caller then owns staying within the weight
    /// budget). The default is sized to the surface's *public* weight budget.
    #[must_use]
    pub fn with_pace(mut self, pace: Duration) -> Self {
        self.pace = pace;
        self
    }

    /// Override the REST base URL (for tests against a mock server).
    #[must_use]
    pub fn with_base_url(mut self, base_url: impl Into<String>) -> Self {
        self.base_url = base_url.into();
        self
    }

    /// Inject a pre-built [`reqwest::Client`].
    ///
    /// By default each constructor builds its own `Client` (one connection pool
    /// per client). Pass a shared `Client` here to reuse a single pool across,
    /// e.g., a spot and a futures client, or to apply custom transport
    /// configuration (proxy, TLS).
    ///
    /// Note: a 30-second per-request timeout is always applied at request level
    /// regardless of any client-level timeout configured on the injected
    /// `Client`, so a shorter client-level timeout will not take effect.
    #[must_use]
    pub fn with_client(mut self, client: Client) -> Self {
        self.client = client;
        self
    }

    /// Fetch historical candles for `symbol` at `interval`, paginating
    /// automatically across the surface's page cap.
    ///
    /// Returns a [`Stream`] that processes each page as it arrives (does **not**
    /// buffer the whole range in memory — a 90d `1s` backfill is millions of
    /// candles). For a convenience `Vec` see [`collect_candles`](Self::collect_candles).
    ///
    /// # Range contract
    ///
    /// Yields exactly the candles whose [`close_time`](Candle::close_time) falls
    /// in `[start, end]` (both inclusive), matched on `close_time` — the field
    /// consumers receive. Binance filters by the bar's *open* time, so this method
    /// maps both bounds from `close_time` to `open_time` (lower bound widens to
    /// capture the candle whose `close_time == start`, i.e. `open == start −
    /// interval`; upper bound narrows to `open == end − interval`) and trims by
    /// `close_time`, consistent with the library's other historical fetches.
    ///
    /// `close_time` is computed library-side as the exclusive period-end boundary
    /// (`open_time + interval`) — Binance's raw wire `closeTime` (`period-end −
    /// 1ms`) is **discarded** (see [`Candle::close_time`]).
    ///
    /// Zero-trade periods are **not** dropped: Binance REST server-side gap-fills
    /// them (`volume == 0`, OHLC == prior close), and the library delivers them
    /// (filtering is consumer policy). The live WS path omits them entirely — an
    /// asymmetry consumers should expect.
    ///
    /// # Arguments
    ///
    /// * `symbol` - Market symbol, e.g. `"BTCUSDT"` (uppercased for Binance).
    /// * `interval` - Candle resolution. Both Binance surfaces support the full
    ///   [`CandleInterval`] set, including [`Sec1`](CandleInterval::Sec1).
    /// * `start` / `end` - Inclusive `close_time` range bounds.
    ///
    /// # Errors
    ///
    /// Each yielded item is a `Result`. On HTTP `429`/`418` the stream yields
    /// [`BinanceDataError::RateLimited`] and ends (resume by re-calling with
    /// `start` = last received `close_time` **+ 1ms**, the next candle's open —
    /// resuming exactly at the last `close_time` re-yields that candle, as the
    /// range is `close_time`-inclusive). Other failures surface as
    /// [`BinanceDataError::Api`] / [`Http`](BinanceDataError::Http) /
    /// [`Deserialize`](BinanceDataError::Deserialize) /
    /// [`InvalidInput`](BinanceDataError::InvalidInput).
    #[must_use = "fetch_candles returns a lazy Stream that does nothing unless polled"]
    pub fn fetch_candles<'a>(
        &'a self,
        symbol: &'a str,
        interval: CandleInterval,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> impl Stream<Item = Result<Candle, BinanceDataError>> + 'a {
        try_stream! {
            // An inverted range is a caller error, not an empty result: Binance
            // would return an empty array (silent success) or a confusing 400.
            // A zero-width range (`start == end`) stays valid — it yields the
            // single candle whose `close_time == start == end`.
            if start > end {
                Err(BinanceDataError::InvalidInput {
                    message: format!("start ({start}) must not be after end ({end})"),
                })?;
            }
            let market = validate_symbol(symbol)?;
            let step = interval.to_step();

            // Range contract: yield candles whose `close_time ∈ [start, end]`.
            // Binance filters by the bar's open-time, so widen the lower bound by
            // one interval to capture the candle whose `close_time == start`
            // (open == start − interval), then trim by `close_time` below.
            // `None` (underflow near DateTime::MIN_UTC) is not an error: the
            // boundary candle would have an unrepresentable open and so cannot
            // exist, making the un-widened bound already correct.
            let request_start = open_time_from_close(start, step).unwrap_or(start);
            let mut start_ms = request_start.timestamp_millis();
            // Mirror the lower bound: `endTime` is an open-time filter too, and the
            // last wanted candle (`close_time == end`) opens at `end − interval`.
            // Narrowing the upper bound the same way keeps `endTime` an honest
            // open-time value (not a close-time) and makes the trim exact on the
            // upper end. Underflow near DateTime::MIN ⇒ fall back to `end`: the
            // `close_time <= end` trim below stays exact regardless, so the
            // un-narrowed bound only requests at most one extra page and never
            // admits an out-of-range candle.
            let request_end = open_time_from_close(end, step).unwrap_or(end);
            let end_ms = request_end.timestamp_millis();
            let limit = self.surface.page_limit();

            loop {
                let url = self.page_url(&market, interval, start_ms, end_ms, limit);
                debug!(%url, "Fetching Binance klines page");

                let rows = self.fetch_page(&url).await?;
                if rows.is_empty() {
                    break;
                }

                // Advance the cursor to the next candle's open BEFORE trimming, so
                // pagination is driven purely by open-time (path ii) and never by
                // the trimmed/yielded subset.
                let last_open_ms = rows[rows.len() - 1].open_time_ms;
                let row_count = rows.len();

                for row in rows {
                    let candle = row.into_candle(interval)?;
                    if candle.close_time >= start && candle.close_time <= end {
                        yield candle;
                    }
                }

                // A short page means Binance had no more data in the window.
                if row_count < limit as usize {
                    break;
                }

                // Next page starts at the candle after the last one received:
                // open_of_last + interval (keyed off open-time, consistent with
                // path ii). Overflow ⇒ no further representable candles ⇒ stop.
                let Some(next_open) = close_time_from_open(
                    DateTime::from_timestamp_millis(last_open_ms)
                        .ok_or_else(|| BinanceDataError::Deserialize {
                            message: format!("open_time {last_open_ms} out of range"),
                            payload: String::new(),
                        })?,
                    step,
                ) else {
                    break;
                };
                start_ms = next_open.timestamp_millis();
                if start_ms > end_ms {
                    break;
                }

                // Proactive courtesy pace between pages (see struct docs). Bounded,
                // observable, never reacts to a 429.
                if !self.pace.is_zero() {
                    debug!(pace_ms = self.pace.as_millis(), "Pacing before next klines page");
                    tokio::time::sleep(self.pace).await;
                }
            }
        }
    }

    /// Convenience wrapper over [`fetch_candles`](Self::fetch_candles) that
    /// collects the full range into a `Vec` (oldest first).
    ///
    /// **Heavy for large ranges** — a 90d `1s` backfill is millions of `Candle`s
    /// (hundreds of MB). Prefer the streaming API for long ranges.
    #[must_use = "collect_candles returns the fetched candles (or an error) that should be used"]
    pub async fn collect_candles(
        &self,
        symbol: &str,
        interval: CandleInterval,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<Vec<Candle>, BinanceDataError> {
        let mut stream = std::pin::pin!(self.fetch_candles(symbol, interval, start, end));
        let mut candles = Vec::new();
        while let Some(candle) = stream.next().await {
            candles.push(candle?);
        }
        Ok(candles)
    }

    /// Build the paginated request URL for this surface.
    fn page_url(
        &self,
        market: &str,
        interval: CandleInterval,
        start_ms: i64,
        end_ms: i64,
        limit: u32,
    ) -> String {
        match self.surface {
            Surface::Spot => format!(
                "{}/api/v3/klines?symbol={}&interval={}&startTime={}&endTime={}&limit={}",
                self.base_url,
                market,
                interval.as_str(),
                start_ms,
                end_ms,
                limit,
            ),
            Surface::FuturesContinuous => format!(
                "{}/fapi/v1/continuousKlines?pair={}&contractType=PERPETUAL&interval={}&startTime={}&endTime={}&limit={}",
                self.base_url,
                market,
                interval.as_str(),
                start_ms,
                end_ms,
                limit,
            ),
        }
    }

    /// Fetch and deserialise a single page of klines.
    async fn fetch_page(&self, url: &str) -> Result<Vec<BinanceKlineRow>, BinanceDataError> {
        let response = self.client.get(url).timeout(REQUEST_TIMEOUT).send().await?;
        let status = response.status();

        // Extract retry-after before consuming the body. Only the integer
        // delay-seconds form is parsed; the RFC 7231 §7.1.3 HTTP-date form is
        // not supported (Binance sends delay-seconds) and yields `None`.
        let retry_after = response
            .headers()
            .get(reqwest::header::RETRY_AFTER)
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<u64>().ok())
            .map(Duration::from_secs);

        // 429 (rate limited) and 418 (IP banned for repeat violations) both end
        // the stream with RateLimited — the consumer owns retry/backoff/resume.
        if status == StatusCode::TOO_MANY_REQUESTS || status == StatusCode::IM_A_TEAPOT {
            return Err(BinanceDataError::RateLimited { retry_after });
        }

        let body = response.text().await?;

        if !status.is_success() {
            return Err(BinanceDataError::Api {
                status: status.as_u16(),
                message: truncate_body(&body),
            });
        }

        serde_json::from_str::<Vec<BinanceKlineRow>>(&body).map_err(|e| {
            BinanceDataError::Deserialize {
                message: e.to_string(),
                payload: truncate_body(&body),
            }
        })
    }
}

/// Validate and normalise a market symbol for a Binance REST request.
///
/// Binance REST requires uppercase symbols and rejects lowercase with `400
/// Invalid symbol`, so the symbol is uppercased. Rejects empty input and any
/// URL-breaking characters up front (an observable client-side error rather than
/// a confusing API 400).
fn validate_symbol(symbol: &str) -> Result<String, BinanceDataError> {
    if symbol.is_empty() {
        return Err(BinanceDataError::InvalidInput {
            message: "symbol must not be empty".to_owned(),
        });
    }
    if symbol.contains(['/', '?', '#', ' ', '%', '&', '=', '+']) {
        return Err(BinanceDataError::InvalidInput {
            message: format!("symbol contains invalid URL characters: {symbol:?}"),
        });
    }
    Ok(symbol.to_uppercase())
}

/// Truncate a response body for error messages (max 512 chars, UTF-8 safe).
fn truncate_body(body: &str) -> String {
    let boundary = body.floor_char_boundary(512);
    body[..boundary].to_owned()
}

/// One Binance kline, as the wire's positional array-of-arrays row.
///
/// Both the spot (`/api/v3/klines`) and futures continuous
/// (`/fapi/v1/continuousKlines`) surfaces return the identical layout:
///
/// ```text
/// [ openTime(int ms), open(str), high(str), low(str), close(str),
///   volume(str), closeTime(int ms), quoteVolume(str), trades(int), ... ]
/// ```
///
/// OHLCV are JSON **strings** and are parsed `str`→[`Decimal`] (an `f64` hop
/// would silently truncate precision). `openTime`/`trades` are JSON integers.
/// The wire `closeTime` (index 6) is **ignored** — [`into_candle`](Self::into_candle)
/// recomputes the boundary from `openTime` (see [`Candle::close_time`]).
#[derive(Debug, Clone, PartialEq)]
struct BinanceKlineRow {
    open_time_ms: i64,
    open: Decimal,
    high: Decimal,
    low: Decimal,
    close: Decimal,
    volume: Decimal,
    trade_count: u64,
}

impl BinanceKlineRow {
    /// Map this row to a normalised [`Candle`].
    ///
    /// `close_time = close_time_from_open(openTime, interval.step)` — the exclusive
    /// period-end boundary, **not** the wire `closeTime` (`period-end − 1ms`).
    fn into_candle(self, interval: CandleInterval) -> Result<Candle, BinanceDataError> {
        let open_time = DateTime::from_timestamp_millis(self.open_time_ms).ok_or_else(|| {
            BinanceDataError::Deserialize {
                message: format!("open_time {} out of representable range", self.open_time_ms),
                payload: String::new(),
            }
        })?;

        let close_time = close_time_from_open(open_time, interval.to_step()).ok_or_else(|| {
            BinanceDataError::Deserialize {
                message: format!(
                    "close_time overflow: open={open_time}, interval={}",
                    interval.as_str()
                ),
                payload: String::new(),
            }
        })?;

        Ok(Candle {
            close_time,
            open: self.open,
            high: self.high,
            low: self.low,
            close: self.close,
            volume: self.volume,
            trade_count: self.trade_count,
        })
    }
}

impl<'de> Deserialize<'de> for BinanceKlineRow {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::{self, SeqAccess, Visitor};
        use std::fmt;

        struct RowVisitor;

        impl<'de> Visitor<'de> for RowVisitor {
            type Value = BinanceKlineRow;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a Binance kline array [openTime, O, H, L, C, V, closeTime, ...]")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<BinanceKlineRow, A::Error>
            where
                A: SeqAccess<'de>,
            {
                /// Read the array element at index `idx` (field `field`), erroring
                /// if the array ran short. `idx` is the actual element count seen so
                /// far, so `invalid_length` reports an honest length rather than `0`.
                macro_rules! next {
                    ($idx:literal, $field:literal, $ty:ty) => {
                        seq.next_element::<$ty>()?.ok_or_else(|| {
                            de::Error::invalid_length($idx, &concat!("missing ", $field))
                        })?
                    };
                }

                let open_time_ms = next!(0, "openTime", i64);
                // OHLCV arrive as JSON strings; parse str→Decimal (never f64).
                let open = parse_decimal::<A::Error>(next!(1, "open", &str))?;
                let high = parse_decimal::<A::Error>(next!(2, "high", &str))?;
                let low = parse_decimal::<A::Error>(next!(3, "low", &str))?;
                let close = parse_decimal::<A::Error>(next!(4, "close", &str))?;
                let volume = parse_decimal::<A::Error>(next!(5, "volume", &str))?;
                // [6] closeTime — consumed and ignored (boundary is recomputed).
                let _close_time = next!(6, "closeTime", de::IgnoredAny);
                // [7] quoteVolume — ignored.
                let _quote_volume = next!(7, "quoteVolume", de::IgnoredAny);
                let trade_count = next!(8, "trades", u64);

                // Drain any trailing elements (takerBuyBase, takerBuyQuote, …) so
                // the seq is fully consumed.
                while seq.next_element::<de::IgnoredAny>()?.is_some() {}

                Ok(BinanceKlineRow {
                    open_time_ms,
                    open,
                    high,
                    low,
                    close,
                    volume,
                    trade_count,
                })
            }
        }

        deserializer.deserialize_seq(RowVisitor)
    }
}

/// Parse a Binance OHLCV string field as [`Decimal`], mapping a parse failure to
/// a serde error so it surfaces as [`BinanceDataError::Deserialize`].
fn parse_decimal<E: serde::de::Error>(raw: &str) -> Result<Decimal, E> {
    raw.parse::<Decimal>()
        .map_err(|e| E::custom(format!("invalid decimal {raw:?}: {e}")))
}

#[cfg(test)]
// Test code: panics on bad input are acceptable (expect carries a diagnostic message).
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn row_deserializes_ohlcv_as_decimal_strings() {
        // Real spot row shape (trailing fields present); OHLCV are JSON strings.
        let json = r#"[1780908960000,"63073.95000000","63093.79000000","63072.61000000","63093.79000000","3.09099000",1780909019999,"194978.89758500",1617,"1.48973000","93971.70741680","0"]"#;
        let row: BinanceKlineRow = serde_json::from_str(json).unwrap();
        assert_eq!(row.open_time_ms, 1_780_908_960_000);
        assert_eq!(row.open, dec!(63073.95000000));
        assert_eq!(row.high, dec!(63093.79000000));
        assert_eq!(row.low, dec!(63072.61000000));
        assert_eq!(row.close, dec!(63093.79000000));
        assert_eq!(row.volume, dec!(3.09099000));
        assert_eq!(row.trade_count, 1617);
    }

    #[test]
    fn high_precision_ohlcv_round_trips_exactly() {
        // An f64 intermediate would truncate this; str→Decimal must not.
        let json = r#"[0,"0.000000010000000","0.000000010000000","0.000000010000000","0.000000010000000","0.000000010000000",59999,"0",0]"#;
        let row: BinanceKlineRow = serde_json::from_str(json).unwrap();
        assert_eq!(row.open, dec!(0.000000010000000));
        assert_eq!(row.open.to_string(), "0.000000010000000");
    }

    #[test]
    fn row_into_candle_recomputes_close_time_from_open() {
        // Wire closeTime (index 6) is open + 59999ms; the candle's close_time must
        // be the exclusive boundary open + 60000ms instead.
        let json = r#"[1780908960000,"1","2","0.5","1.5","10",1780909019999,"0",42]"#;
        let row: BinanceKlineRow = serde_json::from_str(json).unwrap();
        let candle = row.into_candle(CandleInterval::Min1).unwrap();
        assert_eq!(candle.close_time.timestamp_millis(), 1_780_909_020_000);
        assert_eq!(candle.open, dec!(1));
        assert_eq!(candle.high, dec!(2));
        assert_eq!(candle.low, dec!(0.5));
        assert_eq!(candle.close, dec!(1.5));
        assert_eq!(candle.volume, dec!(10));
        assert_eq!(candle.trade_count, 42);
    }

    #[test]
    fn zero_volume_gap_filled_candle_maps_not_dropped() {
        // Binance REST gap-fills zero-trade periods (V=0, OHLC=prev close). The
        // mapping must produce a candle — dropping V=0 is consumer policy.
        let json = r#"[1780909046000,"63051.50","63051.50","63051.50","63051.50","0",1780909046999,"0",0]"#;
        let row: BinanceKlineRow = serde_json::from_str(json).unwrap();
        let candle = row.into_candle(CandleInterval::Sec1).unwrap();
        assert_eq!(candle.volume, Decimal::ZERO);
        assert_eq!(candle.trade_count, 0);
        assert_eq!(candle.close_time.timestamp_millis(), 1_780_909_047_000);
    }

    #[test]
    fn malformed_decimal_is_observable_error_not_silent() {
        let json = r#"[0,"not_a_number","2","0.5","1.5","10",59999,"0",1]"#;
        let err = serde_json::from_str::<BinanceKlineRow>(json).unwrap_err();
        assert!(err.to_string().contains("invalid decimal"), "{err}");
    }

    #[test]
    fn short_row_is_error_not_silent_default() {
        // A truncated row must fail loudly rather than defaulting missing fields.
        let json = r#"[0,"1","2","0.5"]"#;
        assert!(serde_json::from_str::<BinanceKlineRow>(json).is_err());
    }

    #[test]
    fn validate_symbol_uppercases_and_rejects_bad_input() {
        assert_eq!(validate_symbol("btcusdt").unwrap(), "BTCUSDT");
        assert!(validate_symbol("").is_err());
        assert!(validate_symbol("BTC/USDT").is_err());
        assert!(validate_symbol("BTC USDT").is_err());
    }

    #[test]
    fn page_url_differs_per_surface() {
        let spot = BinanceHistoricalClient::spot();
        let url = spot.page_url("BTCUSDT", CandleInterval::Min1, 100, 200, 1000);
        assert!(url.contains("/api/v3/klines?symbol=BTCUSDT"));
        assert!(url.contains("interval=1m"));
        assert!(!url.contains("contractType"));

        let futures = BinanceHistoricalClient::futures();
        let url = futures.page_url("BTCUSDT", CandleInterval::Sec1, 100, 200, 1500);
        assert!(url.contains("/fapi/v1/continuousKlines?pair=BTCUSDT"));
        assert!(url.contains("contractType=PERPETUAL"));
        assert!(url.contains("interval=1s"));
        assert!(url.contains("limit=1500"));
    }

    #[test]
    fn surface_defaults_are_distinct() {
        assert_eq!(BinanceHistoricalClient::spot().pace, DEFAULT_SPOT_PACE);
        assert_eq!(
            BinanceHistoricalClient::futures().pace,
            DEFAULT_FUTURES_PACE
        );
        assert_eq!(Surface::Spot.page_limit(), 1000);
        assert_eq!(Surface::FuturesContinuous.page_limit(), 1500);
    }

    #[test]
    fn with_pace_overrides_default() {
        let c = BinanceHistoricalClient::futures().with_pace(Duration::from_millis(500));
        assert_eq!(c.pace, Duration::from_millis(500));
    }

    #[tokio::test]
    async fn inverted_range_is_invalid_input_not_empty() {
        // An inverted range must surface as an observable client-side error
        // before any network I/O — not a silently-empty result.
        let end = DateTime::from_timestamp_millis(1_780_908_960_000).unwrap();
        let start = end + chrono::Duration::hours(1);
        let err = BinanceHistoricalClient::spot()
            .collect_candles("BTCUSDT", CandleInterval::Min1, start, end)
            .await
            .unwrap_err();
        assert!(
            matches!(err, BinanceDataError::InvalidInput { .. }),
            "expected InvalidInput, got {err:?}"
        );
    }

    // --- Mock-server pagination / rate-limit tests -------------------------------------------
    //
    // A throwaway in-process HTTP/1.1 server (std-only, no extra dependency) serves canned
    // responses to the real `reqwest` client via `with_base_url`, exercising the multi-page
    // loop, the `close_time` trim, the resume contract, and the `RateLimited`-then-ends path.

    use std::io::{Read, Write};
    use std::net::TcpListener;

    /// One canned HTTP response for the mock server.
    struct MockResponse {
        status: u16,
        retry_after_secs: Option<u64>,
        body: String,
    }

    fn ok(body: String) -> MockResponse {
        MockResponse {
            status: 200,
            retry_after_secs: None,
            body,
        }
    }

    /// Spawn a single-shot HTTP/1.1 server that serves `responses` in order, one per accepted
    /// connection (`Connection: close` makes `reqwest` open a fresh connection per page). Returns
    /// `(base_url, join_handle)`. The caller must:
    ///
    /// 1. Pass the base URL to [`with_base_url`](BinanceHistoricalClient::with_base_url).
    /// 2. Drive **exactly** `responses.len()` page requests (fewer leaves the server thread
    ///    parked in `accept()` with responses unserved, so the test silently covers fewer pages
    ///    than intended).
    /// 3. Call `handle.join().expect("mock server panicked")` after all awaits complete so that
    ///    any server-side panic surfaces as a test failure rather than being swallowed.
    fn spawn_mock(responses: Vec<MockResponse>) -> (String, std::thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let handle = std::thread::spawn(move || {
            for resp in responses {
                let (mut stream, _) = listener.accept().unwrap();
                // A GET carries no body; one read of the request line + headers is enough on
                // loopback (the request is far smaller than the buffer — we only need to let the
                // client finish writing before we reply; the bytes themselves are discarded).
                let _ = stream.read(&mut [0u8; 4096]);
                let retry = resp
                    .retry_after_secs
                    .map(|s| format!("Retry-After: {s}\r\n"))
                    .unwrap_or_default();
                let http = format!(
                    "HTTP/1.1 {} X\r\nContent-Type: application/json\r\n{retry}Content-Length: {}\r\nConnection: close\r\n\r\n{}",
                    resp.status,
                    resp.body.len(),
                    resp.body,
                );
                stream.write_all(http.as_bytes()).unwrap();
            }
        });
        (base, handle)
    }

    /// A minimal valid spot kline row (9 elements) opening at `open_ms`. The wire `closeTime`
    /// (index 6) is `open + 59_999ms`, so the recomputed exclusive boundary (`open + 60_000ms`)
    /// is provably not the wire value.
    fn row(open_ms: i64) -> String {
        format!(
            r#"[{open_ms},"1","2","0.5","1.5","10",{},"0",1]"#,
            open_ms + 59_999
        )
    }

    /// Build a JSON array page from the given row open-times.
    fn page(opens: impl IntoIterator<Item = i64>) -> String {
        let rows: Vec<String> = opens.into_iter().map(row).collect();
        format!("[{}]", rows.join(","))
    }

    fn ms(millis: i64) -> DateTime<Utc> {
        DateTime::from_timestamp_millis(millis).unwrap()
    }

    const MIN_MS: i64 = 60_000;
    const BASE_MS: i64 = 1_700_000_000_000;

    #[tokio::test]
    async fn paginates_across_pages_without_gap_or_duplicate() {
        // Page 1 fills the spot page cap (1000) so the loop continues; page 2 is a short page
        // that terminates it. The cursor advance keys off open-time, so the page seam must be
        // seamless: candle[1000] follows candle[999] by exactly one interval.
        let page1 = page((0..1000).map(|i| BASE_MS + i * MIN_MS));
        let page2 = page([BASE_MS + 1000 * MIN_MS, BASE_MS + 1001 * MIN_MS]);
        let (url, server) = spawn_mock(vec![ok(page1), ok(page2)]);

        let candles = BinanceHistoricalClient::spot()
            .with_base_url(url)
            .with_pace(Duration::ZERO)
            .collect_candles(
                "BTCUSDT",
                CandleInterval::Min1,
                ms(BASE_MS + MIN_MS),        // close of row 0
                ms(BASE_MS + 1002 * MIN_MS), // close of the last row
            )
            .await
            .unwrap();

        assert_eq!(
            candles.len(),
            1002,
            "every row across both pages is delivered once"
        );
        for w in candles.windows(2) {
            assert_eq!(
                w[1].close_time - w[0].close_time,
                chrono::Duration::milliseconds(MIN_MS),
                "close_time must advance by exactly one interval — no gap, no duplicate at the seam",
            );
        }
        server.join().expect("mock server panicked");
    }

    #[tokio::test]
    async fn close_time_trim_excludes_out_of_range_candles() {
        // A page of three candles (close_times base+1m, base+2m, base+3m); the window admits
        // only the middle one, proving both trim bounds are exact and applied per row.
        let (url, server) = spawn_mock(vec![ok(page([
            BASE_MS,
            BASE_MS + MIN_MS,
            BASE_MS + 2 * MIN_MS,
        ]))]);

        let candles = BinanceHistoricalClient::spot()
            .with_base_url(url)
            .with_pace(Duration::ZERO)
            .collect_candles(
                "BTCUSDT",
                CandleInterval::Min1,
                ms(BASE_MS + 2 * MIN_MS), // == close of the middle candle
                ms(BASE_MS + 2 * MIN_MS),
            )
            .await
            .unwrap();

        assert_eq!(candles.len(), 1);
        assert_eq!(candles[0].close_time, ms(BASE_MS + 2 * MIN_MS));
        server.join().expect("mock server panicked");
    }

    #[tokio::test]
    async fn resume_at_last_close_plus_1ms_skips_the_boundary_duplicate() {
        // The candle already received before the 429 has close_time `last_close`. Binance widens
        // the lower bound by one interval, so a resume page leads with that boundary candle
        // (open = last_close − interval) followed by the next candle (open = last_close). The mock
        // serves this page verbatim regardless of the query params, so the exclusion asserted
        // below is driven by the library's `close_time >= start` trim, not Binance-side
        // `startTime` filtering — which is exactly the layer the resume contract relies on.
        let last_close = BASE_MS + MIN_MS;
        let resume_page = page([last_close - MIN_MS, last_close]);

        // Resuming exactly at `last_close` re-yields the boundary candle (the `[start, end]`
        // lower bound is inclusive) → duplicate. This is the trap the resume contract warns of.
        let (dup_url, dup_server) = spawn_mock(vec![ok(resume_page.clone())]);
        let dup = BinanceHistoricalClient::spot()
            .with_base_url(dup_url)
            .with_pace(Duration::ZERO)
            .collect_candles(
                "BTCUSDT",
                CandleInterval::Min1,
                ms(last_close),
                ms(last_close + MIN_MS),
            )
            .await
            .unwrap();
        assert_eq!(
            dup.len(),
            2,
            "resuming at last close_time re-delivers it (documents the duplicate)"
        );
        dup_server.join().expect("mock server panicked");

        // Resuming at `last_close + 1ms` (the documented contract) skips the boundary candle and
        // delivers only the next one — lossless and duplicate-free.
        let (resumed_url, resumed_server) = spawn_mock(vec![ok(resume_page)]);
        let resumed = BinanceHistoricalClient::spot()
            .with_base_url(resumed_url)
            .with_pace(Duration::ZERO)
            .collect_candles(
                "BTCUSDT",
                CandleInterval::Min1,
                ms(last_close + 1),
                ms(last_close + MIN_MS),
            )
            .await
            .unwrap();
        assert_eq!(
            resumed.len(),
            1,
            "last_close + 1ms resume skips the duplicate"
        );
        assert_eq!(resumed[0].close_time, ms(last_close + MIN_MS));
        resumed_server.join().expect("mock server panicked");
    }

    #[tokio::test]
    async fn rate_limited_yields_once_then_ends() {
        let (url, server) = spawn_mock(vec![MockResponse {
            status: 429,
            retry_after_secs: Some(30),
            body: "{}".to_owned(),
        }]);

        let client = BinanceHistoricalClient::spot()
            .with_base_url(url)
            .with_pace(Duration::ZERO);
        let mut stream = std::pin::pin!(client.fetch_candles(
            "BTCUSDT",
            CandleInterval::Min1,
            ms(0),
            ms(MIN_MS)
        ));

        let first = stream
            .next()
            .await
            .expect("the stream must yield the rate-limit error");
        assert!(
            matches!(
                first,
                Err(BinanceDataError::RateLimited { retry_after: Some(d) }) if d == Duration::from_secs(30)
            ),
            "429 must yield RateLimited carrying the parsed Retry-After, got {first:?}",
        );
        assert!(
            stream.next().await.is_none(),
            "the stream must end after RateLimited"
        );
        server.join().expect("mock server panicked");
    }
}
