//! Error type for the Binance historical klines REST client.
//!
//! Modelled on `MassiveError` (the `massive` feature's REST error): the
//! library surfaces these errors **without** automatic retry, reconnection, or
//! adaptive backoff. The consumer decides how to handle rate limits and API
//! failures (see [`BinanceDataError::RateLimited`] for the resume contract).
//!
//! It is deliberately a **dedicated** type rather than a new variant on the
//! shared [`DataError`](crate::error::DataError) — that shared enum derives
//! `Eq`/`Ord`/`Hash`/`Serialize`,
//! and a `retry_after: Duration` payload would not fit cleanly. Keeping the
//! Binance REST error local mirrors the Massive client exactly.

use std::time::Duration;

/// Errors returned by the Binance historical klines REST client.
///
/// The kline endpoints are public and unauthenticated, so there is no auth or
/// environment-variable variant (contrast the `massive` feature's `MassiveError`,
/// which carries both). WebSocket disconnects are handled by the live path's
/// `Connector`, not here.
#[derive(Debug, Clone, PartialEq)]
pub enum BinanceDataError {
    /// Rate limited by the API (HTTP `429`) or IP-banned for repeat violations
    /// (HTTP `418`). Carries the optional `Retry-After` duration parsed from the
    /// response header.
    ///
    /// # Contract
    ///
    /// The historical `Stream` **yields this error and ends** — it does **not**
    /// wait, retry, or run a process-global limiter. The consumer owns
    /// retry/backoff and **resumes** losslessly by re-invoking `fetch_candles`
    /// with `start` advanced to `last_close_time + 1ms` (the next candle's open).
    /// The `[start, end]` range is `close_time`-inclusive, so resuming exactly at
    /// the last `close_time` would re-yield that final candle; the `+1ms` step
    /// skips it without leaving a gap (pagination keys off `open_time`, and
    /// `open ≡ close − interval`).
    RateLimited { retry_after: Option<Duration> },

    /// API returned a non-success, non-rate-limit response (e.g. `400 Invalid
    /// symbol`, `400 Invalid interval`).
    Api { status: u16, message: String },

    /// Network or HTTP client error (DNS, TLS, timeout, connection reset).
    Http { message: String },

    /// Response body could not be deserialised into the expected kline shape.
    Deserialize { message: String, payload: String },

    /// Client-side input validation failed before any request was made
    /// (e.g. empty symbol, or a candle whose `close_time` overflows the
    /// representable `DateTime<Utc>` range).
    InvalidInput { message: String },
}

impl std::fmt::Display for BinanceDataError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BinanceDataError::RateLimited { retry_after } => {
                write!(f, "Binance rate limited")?;
                if let Some(duration) = retry_after {
                    write!(f, " (retry after {:?})", duration)?;
                }
                Ok(())
            }
            BinanceDataError::Api { status, message } => {
                write!(f, "Binance API error ({}): {}", status, message)
            }
            BinanceDataError::Http { message } => {
                write!(f, "Binance HTTP error: {}", message)
            }
            BinanceDataError::Deserialize { message, payload } => {
                let boundary = payload.floor_char_boundary(100);
                let truncated = &payload[..boundary];
                let ellipsis = if boundary < payload.len() { "..." } else { "" };
                write!(
                    f,
                    "Binance deserialize error: {} (payload: {truncated}{ellipsis})",
                    message
                )
            }
            BinanceDataError::InvalidInput { message } => {
                write!(f, "Binance invalid input: {}", message)
            }
        }
    }
}

impl std::error::Error for BinanceDataError {}

impl From<reqwest::Error> for BinanceDataError {
    fn from(err: reqwest::Error) -> Self {
        BinanceDataError::Http {
            message: err.to_string(),
        }
    }
}
