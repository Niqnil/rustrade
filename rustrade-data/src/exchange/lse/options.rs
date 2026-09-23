//! US equity and ETF option prints and one-minute option candles from the London Strategic Edge
//! vault.
//!
//! ```no_run
//! # use chrono::{Duration, TimeZone, Utc};
//! # use futures::StreamExt;
//! # use rustrade_data::exchange::lse::error::LseError;
//! # use rustrade_data::exchange::lse::vault::LseVaultClient;
//! # async fn example() -> Result<(), LseError> {
//! let client = LseVaultClient::from_env()?;
//! let start = Utc.with_ymd_and_hms(2026, 9, 22, 15, 0, 0).unwrap();
//! let end = start + Duration::minutes(5);
//!
//! let prints = client.fetch_option_flow(Some("SPY"), start, end);
//! futures::pin_mut!(prints);
//!
//! while let Some(print) = prints.next().await {
//!     let print = print?;
//!     println!("{} {} x{} delta={:?}", print.contract.ticker, print.price, print.volume, print.greeks.delta);
//! }
//! # Ok(())
//! # }
//! ```
//!
//! # ⚠️ Licensing
//! Data retrieved here is **not redistributable**. See the [module documentation](super) and
//! <https://londonstrategicedge.com/terms>.
//!
//! # What this feed is — and is not
//! - **Prints, not quotes.** Every row is an executed option trade. There is no bid, ask or book
//!   anywhere on these paths, so nothing here can build an [`OrderBookL1`](crate::subscription::book::OrderBookL1).
//! - **⚠️ Greeks are PRINT-TRIGGERED, not continuous.** Each print carries the greeks the provider
//!   computed at that print, and nothing publishes them in between. A contract's greeks are
//!   therefore only as fresh as its last trade, which on an illiquid strike can be weeks old — and
//!   an unheld contract that never trades is never marked at all. This is a property of the source:
//!   the provider's chain snapshot endpoint, which would be the continuous alternative, serves
//!   stale stored fields (expired contracts, a `dte` that disagrees with the expiry) and is
//!   deliberately not wrapped. Do not treat the latest print's greeks as a current risk figure
//!   without checking its age.
//! - **Timestamps are batch stamps at whole-second granularity.** Every print inside one second
//!   carries a stamp within a few milliseconds of every other, so the sub-second part is not a
//!   print time. Never order, de-duplicate or join on the timestamp alone; the provider's print
//!   [`id`](LseOptionPrint::id) is the unique key.
//! - **Strikes are not always round.** Contracts adjusted for a corporate action carry fractional
//!   strikes (`2.67`, `9.85`), so the strike is a [`Decimal`] and the contract's identity is its
//!   [`ticker`](LseOptionContract::ticker), not a strike rebuilt from integers.
//! - **An unknown underlying or ticker is an empty result, not an error.** The provider answers
//!   `200` with no rows for a symbol it has never heard of, indistinguishable here from a quiet
//!   period.
//! - **Exercise style is not reported**, so none is claimed: most listed US equity options are
//!   American, but index options are European, and guessing would be this library inventing a fact.

use crate::event::{DataKind, MarketEvent};
use crate::exchange::lse::PROVIDER_TIMESTAMP_FORMAT;
use crate::exchange::lse::error::LseError;
use crate::exchange::lse::historical::{VaultCandleRow, parse_candle_open};
use crate::exchange::lse::vault::LseVaultClient;
use crate::subscription::{
    candle::{Candle, CandleInterval},
    greeks::OptionGreeks,
    trade::PublicTrade,
};
use async_stream::try_stream;
use chrono::{DateTime, NaiveDate, NaiveDateTime, TimeDelta, Timelike, Utc};
use futures::{Stream, StreamExt};
use rust_decimal::Decimal;
use rustrade_instrument::{exchange::ExchangeId, instrument::kind::option::OptionKind};
use serde::{Deserialize, Serialize};
use smol_str::{SmolStr, ToSmolStr};
use tracing::debug;

/// How long after a moment the provider's print tape can be relied on to be complete up to it.
///
/// Ingestion lag on `/options/flow` is variable and **episodic**: a window is usually complete
/// about fifteen seconds after it closes, but during a bad minute one was measured returning **no
/// rows at all** thirty seconds after closing, and another sat at roughly a fifth of its final count
/// across two consecutive reads. Both answered `200`. A partial window holds steady, so completeness
/// cannot be detected by polling until the count stops changing.
///
/// [`fetch_option_flow`](LseVaultClient::fetch_option_flow) therefore refuses a range ending
/// inside this margin rather than returning rows that look final and are not. Sixty seconds is
/// twice the longest outlier measured, which makes it a floor informed by observation rather than a
/// guarantee — a fetch near it during provider degradation can still come back short.
pub const OPTION_FLOW_SETTLE_MARGIN: TimeDelta = TimeDelta::seconds(60);

/// Format accepted by the flow endpoint's `start` / `end` parameters: whole seconds only. A
/// fractional second, and the ISO `T…Z` form, are rejected with a `400`.
const WINDOW_FORMAT: &str = "%Y-%m-%d %H:%M:%S";

/// The first adaptive window [`fetch_option_flow`](LseVaultClient::fetch_option_flow) tries.
///
/// One minute of the busiest underlying measured is roughly two and a half thousand prints —
/// inside the provider's 5,000-row cap — so a busy underlying rarely needs a retry on its first
/// window, and a quiet one grows out of it in a few doublings.
const INITIAL_FLOW_WINDOW: TimeDelta = TimeDelta::seconds(60);

/// The widest window the adaptive walk grows to.
///
/// Bounds how far a sparse stretch — a weekend, a quiet underlying — is covered by one request.
/// Growth is only ever a request-count optimisation: correctness comes from the cap check, not from
/// the window size.
const MAX_FLOW_WINDOW: TimeDelta = TimeDelta::days(1);

/// The narrowest window the walk can shrink to, set by the bounds' whole-second precision.
const MIN_FLOW_WINDOW: TimeDelta = TimeDelta::seconds(1);

/// One option contract, as the provider identifies it.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct LseOptionContract {
    /// The OSI contract symbol, e.g. `SPY260928C00764000` — the contract's identity.
    pub ticker: SmolStr,
    /// The underlying's symbol, e.g. `SPY`.
    pub underlying: SmolStr,
    pub kind: OptionKind,
    /// Fractional on contracts adjusted for a corporate action.
    pub strike: Decimal,
    pub expiry: NaiveDate,
}

/// One executed option trade, with the greeks the provider computed at that print.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LseOptionPrint {
    /// The provider's print identifier — unique, and the only reliable key; see the
    /// [module documentation](self).
    pub id: u64,
    /// When the print was recorded, UTC — a whole-second batch stamp, not an execution time.
    pub time: DateTime<Utc>,
    pub contract: LseOptionContract,
    /// Premium per share.
    pub price: Decimal,
    /// Size in contracts.
    pub volume: u64,
    /// Notional premium in the quote currency, as the provider reports it: `price × volume ×` the
    /// contract multiplier.
    pub premium: Decimal,
    /// Greeks at this print, with the underlying's price at the time in
    /// [`underlying_price`](OptionGreeks::underlying_price). Any greek may be `None`, and on a
    /// small share of prints all of them are. See the
    /// [module documentation](self) on staleness.
    pub greeks: OptionGreeks,
}

impl LseOptionPrint {
    /// This print as a library [`PublicTrade`], sized in contracts.
    ///
    /// `side` is `None`: the provider does not report an aggressor.
    #[must_use]
    pub fn public_trade(&self) -> PublicTrade {
        PublicTrade {
            id: self.id.to_smolstr(),
            price: self.price,
            amount: Decimal::from(self.volume),
            side: None,
        }
    }

    /// This print as the market events an engine consumes: its trade, then its greeks.
    ///
    /// Both are stamped with the print's [`time`](Self::time) as `time_exchange` **and**
    /// `time_received` — a historical fetch has no receipt instant of its own — on
    /// [`ExchangeId::LseOptions`]. The greeks event is emitted even when every greek is `None`; a
    /// consumer such as the engine's option state decides whether an empty update means anything.
    #[must_use]
    pub fn into_market_events<InstrumentKey: Clone>(
        self,
        instrument: InstrumentKey,
    ) -> [MarketEvent<InstrumentKey, DataKind>; 2] {
        let trade = self.public_trade();
        [
            option_event(self.time, instrument.clone(), DataKind::Trade(trade)),
            option_event(self.time, instrument, DataKind::OptionGreeks(self.greeks)),
        ]
    }
}

/// One minute of trading in one option contract.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LseOptionCandle {
    pub contract: LseOptionContract,
    /// Premium OHLC for the minute, with [`volume`](Candle::volume) in contracts and
    /// [`trade_count`](Candle::trade_count) the number of prints.
    pub candle: Candle,
    /// Total notional premium traded in the minute.
    pub premium: Decimal,
    /// Greeks **averaged over the minute's prints**, not sampled at its close, with the averaged
    /// underlying price in [`underlying_price`](OptionGreeks::underlying_price).
    pub greeks: OptionGreeks,
}

/// One `/options/flow` row as served. Unknown fields — `dte`, derivable from `expiry` — are ignored
/// rather than rejected, so an added field does not break decoding.
#[derive(Debug, Deserialize)]
struct FlowRow {
    id: u64,
    ts: String,
    underlying: SmolStr,
    ticker: SmolStr,
    strike: Decimal,
    expiry: NaiveDate,
    contract_type: OptionKind,
    last_price: Decimal,
    volume: u64,
    premium: Decimal,
    #[serde(default)]
    underlying_price: Option<f64>,
    #[serde(default)]
    iv: Option<f64>,
    #[serde(default)]
    delta: Option<f64>,
    #[serde(default)]
    gamma: Option<f64>,
    #[serde(default)]
    theta: Option<f64>,
    #[serde(default)]
    vega: Option<f64>,
    #[serde(default)]
    rho: Option<f64>,
}

impl FlowRow {
    fn time(&self) -> Result<DateTime<Utc>, LseError> {
        NaiveDateTime::parse_from_str(&self.ts, PROVIDER_TIMESTAMP_FORMAT)
            .map(|naive| naive.and_utc())
            .map_err(|error| LseError::Deserialize {
                message: format!("invalid option print timestamp {:?}: {error}", self.ts),
            })
    }

    fn into_print(self, time: DateTime<Utc>) -> LseOptionPrint {
        LseOptionPrint {
            id: self.id,
            time,
            contract: LseOptionContract {
                ticker: self.ticker,
                underlying: self.underlying,
                kind: self.contract_type,
                strike: self.strike,
                expiry: self.expiry,
            },
            price: self.last_price,
            volume: self.volume,
            premium: self.premium,
            greeks: OptionGreeks {
                delta: self.delta,
                gamma: self.gamma,
                theta: self.theta,
                vega: self.vega,
                rho: self.rho,
                implied_volatility: self.iv,
                theoretical_price: None,
                underlying_price: self.underlying_price,
            },
        }
    }
}

/// One `/options/candles` row as served.
#[derive(Debug, Deserialize)]
struct OptionCandleRow {
    ticker: SmolStr,
    underlying: SmolStr,
    strike: Decimal,
    expiry: NaiveDate,
    contract_type: OptionKind,
    /// The bar's **open**, UTC, with no zone suffix.
    minute: String,
    open: Decimal,
    high: Decimal,
    low: Decimal,
    close: Decimal,
    volume: u64,
    premium: Decimal,
    print_count: u64,
    #[serde(default)]
    underlying_price: Option<f64>,
    #[serde(default)]
    iv_avg: Option<f64>,
    #[serde(default)]
    delta_avg: Option<f64>,
    #[serde(default)]
    gamma_avg: Option<f64>,
    #[serde(default)]
    theta_avg: Option<f64>,
    #[serde(default)]
    vega_avg: Option<f64>,
    #[serde(default)]
    rho_avg: Option<f64>,
}

impl VaultCandleRow for OptionCandleRow {
    fn open_time(&self) -> Result<DateTime<Utc>, LseError> {
        parse_candle_open(&self.minute)
    }
}

impl LseOptionCandle {
    /// This candle as the market events an engine consumes: the bar, then its averaged greeks.
    ///
    /// Both are stamped with the bar's [`close_time`](Candle::close_time) — the instant its
    /// contents, the averages included, became knowable — as `time_exchange` and `time_received`,
    /// on [`ExchangeId::LseOptions`]. Stamping any earlier would hand an engine the minute's outcome
    /// before the minute has ended.
    #[must_use]
    pub fn into_market_events<InstrumentKey: Clone>(
        self,
        instrument: InstrumentKey,
    ) -> [MarketEvent<InstrumentKey, DataKind>; 2] {
        let time = self.candle.close_time;
        [
            option_event(time, instrument.clone(), DataKind::Candle(self.candle)),
            option_event(time, instrument, DataKind::OptionGreeks(self.greeks)),
        ]
    }
}

fn option_event<InstrumentKey>(
    time: DateTime<Utc>,
    instrument: InstrumentKey,
    kind: DataKind,
) -> MarketEvent<InstrumentKey, DataKind> {
    MarketEvent {
        time_exchange: time,
        time_received: time,
        exchange: ExchangeId::LseOptions,
        instrument,
        kind,
    }
}

impl OptionCandleRow {
    fn into_option_candle(self, close_time: DateTime<Utc>) -> LseOptionCandle {
        LseOptionCandle {
            contract: LseOptionContract {
                ticker: self.ticker,
                underlying: self.underlying,
                kind: self.contract_type,
                strike: self.strike,
                expiry: self.expiry,
            },
            candle: Candle {
                close_time,
                open: self.open,
                high: self.high,
                low: self.low,
                close: self.close,
                volume: Some(Decimal::from(self.volume)),
                trade_count: Some(self.print_count),
            },
            premium: self.premium,
            greeks: OptionGreeks {
                delta: self.delta_avg,
                gamma: self.gamma_avg,
                theta: self.theta_avg,
                vega: self.vega_avg,
                rho: self.rho_avg,
                implied_volatility: self.iv_avg,
                theoretical_price: None,
                underlying_price: self.underlying_price,
            },
        }
    }
}

impl LseVaultClient {
    /// Fetch one-minute candles for one option contract, paginating automatically.
    ///
    /// Same range contract as [`fetch_candles`](Self::fetch_candles): yields exactly the bars whose
    /// [`close_time`](Candle::close_time) falls in `[start, end]` (**both inclusive**), ascending,
    /// with the same ordering, resolution and range checks and the same errors. Minutes with no
    /// print are absent rather than zero-filled.
    ///
    /// # Arguments
    /// * `ticker` - The OSI contract symbol, e.g. `"SPY260922P00773000"`. Case-insensitive.
    /// * `start` / `end` - Inclusive `close_time` bounds.
    ///
    /// # Errors
    /// See [`fetch_candles`](Self::fetch_candles). A ticker the provider does not know yields an
    /// empty stream, not an error.
    #[must_use = "fetch_option_candles returns a lazy Stream that does nothing unless polled"]
    pub fn fetch_option_candles<'a>(
        &'a self,
        ticker: &'a str,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> impl Stream<Item = Result<LseOptionCandle, LseError>> + 'a {
        self.candle_rows::<OptionCandleRow>(
            "options/candles",
            ticker,
            vec![("ticker", ticker.to_owned())],
            CandleInterval::Min1,
            start,
            end,
        )
        .map(|row| row.map(|(row, close_time)| row.into_option_candle(close_time)))
    }

    /// Fetch option candles into a `Vec`. **Buffers the whole range.**
    ///
    /// # Errors
    /// Fails on the first error, discarding any candles already received. See
    /// [`fetch_option_candles`](Self::fetch_option_candles).
    pub async fn collect_option_candles(
        &self,
        ticker: &str,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<Vec<LseOptionCandle>, LseError> {
        let stream = self.fetch_option_candles(ticker, start, end);
        futures::pin_mut!(stream);

        let mut candles = Vec::new();
        while let Some(candle) = stream.next().await {
            candles.push(candle?);
        }

        Ok(candles)
    }

    /// Fetch every option print in `[start, end)`, oldest first, walking the range automatically.
    ///
    /// # Range and order
    /// Yields exactly the prints whose [`time`](LseOptionPrint::time) falls in `[start, end)` —
    /// half-open, so adjacent ranges tile without overlap — ordered by time and then by
    /// [`id`](LseOptionPrint::id). Sub-second bounds are honoured by trimming: the provider accepts
    /// only whole seconds, so the requested range is widened to them and cut back here.
    ///
    /// # How the range is walked
    /// The provider answers newest-first and silently truncates at a 5,000-row cap, with no offset
    /// or cursor, so a range can only be read in windows small enough to come back whole. This
    /// method walks forward in adaptive windows: a window that returns a full page is halved and
    /// read again, and one that comes back sparse lets the next window grow. Each complete window is
    /// emitted oldest-first, so memory holds at most one page. The extra requests fall only on
    /// windows that hit the cap.
    ///
    /// # Arguments
    /// * `underlying` - Restrict to one underlying (case-insensitive), or `None` for every
    ///   underlying the provider carries. The endpoint cannot filter by contract — it silently
    ///   ignores a `ticker` parameter — so select contracts from the result.
    /// * `start` / `end` - The half-open time range.
    ///
    /// # Errors
    /// Each yielded item is a `Result`, and the stream ends after the first error.
    /// - [`LseError::InvalidInput`] for an inverted range, or one ending within
    ///   [`OPTION_FLOW_SETTLE_MARGIN`] of now: the provider can serve a recent window
    ///   incomplete with a `200`, and returning it would hand back a short tape that looks final.
    /// - [`LseError::OptionFlowWindowSaturated`] when a single second holds more prints than one
    ///   page can carry, so the window cannot be narrowed further without losing some.
    /// - [`LseError::UnexpectedOptionFlowRow`] when a row falls outside the window or underlying it
    ///   was requested for — the provider ignoring a parameter, which it is known to do silently.
    ///   Checked before any row of that window is yielded.
    /// - On `429`, [`LseError::RateLimited`]; resume by re-calling with `start` set to the last
    ///   print's time. Prints sharing that time are then yielded again; de-duplicate on
    ///   [`id`](LseOptionPrint::id).
    /// - Otherwise [`LseError::Api`] / [`Http`](LseError::Http) /
    ///   [`Deserialize`](LseError::Deserialize).
    #[must_use = "fetch_option_flow returns a lazy Stream that does nothing unless polled"]
    pub fn fetch_option_flow<'a>(
        &'a self,
        underlying: Option<&'a str>,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> impl Stream<Item = Result<LseOptionPrint, LseError>> + 'a {
        try_stream! {
            if start > end {
                Err(LseError::InvalidInput {
                    message: format!("start ({start}) must not be after end ({end})"),
                })?;
            }

            let settled_until = Utc::now() - OPTION_FLOW_SETTLE_MARGIN;
            if end > settled_until {
                Err(LseError::InvalidInput {
                    message: format!(
                        "end ({end}) is within the {}s settle margin of now: the provider can \
                         serve a window that recent incomplete with no indication, so it is \
                         refused rather than returned short (see OPTION_FLOW_SETTLE_MARGIN)",
                        OPTION_FLOW_SETTLE_MARGIN.num_seconds()
                    ),
                })?;
            }

            let cap = usize::try_from(self.page_limit().get()).unwrap_or(usize::MAX);
            let range_end = ceil_to_second(end);
            let mut cursor = floor_to_second(start);
            let mut window = INITIAL_FLOW_WINDOW;

            while cursor < range_end {
                let window_end = cursor
                    .checked_add_signed(window)
                    .map_or(range_end, |candidate| candidate.min(range_end));

                let mut query = vec![
                    ("start", cursor.format(WINDOW_FORMAT).to_string()),
                    ("end", window_end.format(WINDOW_FORMAT).to_string()),
                    ("limit", self.page_limit().to_string()),
                ];
                if let Some(underlying) = underlying {
                    query.push(("underlying", underlying.to_owned()));
                }

                let rows: Vec<FlowRow> = self.get_json("options/flow", &query).await?;
                debug!(
                    ?underlying, %cursor, %window_end, rows = rows.len(),
                    "vault option flow window received"
                );

                // A full page may be a truncated one: the cap is applied silently, and the rows it
                // drops are the OLDEST in the window. Narrow and read the same window start again.
                if rows.len() >= cap {
                    let span = window_end - cursor;
                    if span <= MIN_FLOW_WINDOW {
                        Err(LseError::OptionFlowWindowSaturated {
                            window_start: cursor,
                            rows: rows.len(),
                        })?;
                    }
                    // Halved in WHOLE seconds: every bound sent must be one the endpoint can express,
                    // and an odd span halved exactly would put the next window's start mid-second.
                    window = TimeDelta::seconds(span.num_seconds() / 2).max(MIN_FLOW_WINDOW);
                    continue;
                }

                // Validate the whole window before yielding any of it, so an ignored parameter
                // never leaks a partial window into the caller's tape.
                let mut prints = Vec::with_capacity(rows.len());
                for row in rows {
                    let time = row.time()?;

                    if time < cursor || time >= window_end {
                        Err(LseError::UnexpectedOptionFlowRow {
                            id: row.id,
                            message: format!(
                                "stamped {time}, outside the requested window [{cursor}, \
                                 {window_end}): the range parameters appear to have been ignored"
                            ),
                        })?;
                    }
                    if let Some(underlying) = underlying
                        && !row.underlying.eq_ignore_ascii_case(underlying)
                    {
                        Err(LseError::UnexpectedOptionFlowRow {
                            id: row.id,
                            message: format!(
                                "underlying {:?} where {underlying:?} was requested: the \
                                 underlying filter appears to have been ignored",
                                row.underlying
                            ),
                        })?;
                    }

                    prints.push(row.into_print(time));
                }

                // The provider serves newest-first; sort rather than reverse, so the order promised
                // does not rest on how the response happens to be sorted today.
                prints.sort_unstable_by_key(|print| (print.time, print.id));
                let received = prints.len();

                for print in prints {
                    if print.time >= start && print.time < end {
                        yield print;
                    }
                }

                cursor = window_end;
                // Grow only out of a clearly sparse window, so the next one is unlikely to hit the
                // cap and cost a retry.
                if received < cap / 4 {
                    window = (window * 2).min(MAX_FLOW_WINDOW);
                }
            }
        }
    }

    /// Fetch option prints into a `Vec`, oldest first. **Buffers the whole range** — a full session
    /// for one busy underlying is several hundred thousand prints; prefer the stream for long
    /// ranges.
    ///
    /// # Errors
    /// Fails on the first error, discarding any prints already received. See
    /// [`fetch_option_flow`](Self::fetch_option_flow).
    pub async fn collect_option_flow(
        &self,
        underlying: Option<&str>,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<Vec<LseOptionPrint>, LseError> {
        let stream = self.fetch_option_flow(underlying, start, end);
        futures::pin_mut!(stream);

        let mut prints = Vec::new();
        while let Some(print) = stream.next().await {
            prints.push(print?);
        }

        Ok(prints)
    }
}

/// Truncate to the whole second at or before `time`.
fn floor_to_second(time: DateTime<Utc>) -> DateTime<Utc> {
    time.with_nanosecond(0).unwrap_or(time)
}

/// Round up to the whole second at or after `time`, saturating at the representable maximum.
fn ceil_to_second(time: DateTime<Utc>) -> DateTime<Utc> {
    let floor = floor_to_second(time);
    if floor == time {
        time
    } else {
        floor
            .checked_add_signed(TimeDelta::seconds(1))
            .unwrap_or(time)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    // Every value in these rows is invented. No row in this repository may be mistaken for provider
    // data, which is not licensed for redistribution (<https://londonstrategicedge.com/terms>).
    // Decoding is value-independent, so a synthetic row proves what a captured one would.

    const FLOW_ROW: &str = r#"{"id":7,"ts":"2024-01-02 15:00:00.075000","underlying":"TEST",
        "ticker":"TEST240105C00010500","strike":10.5,"expiry":"2024-01-05","contract_type":"call",
        "last_price":1,"volume":3,"premium":300,"underlying_price":11.0,"dte":3,"iv":0.2,
        "delta":0.5,"gamma":0.1,"theta":-0.01,"vega":0.02,"rho":0.003}"#;

    #[test]
    fn a_flow_row_decodes_integer_or_float_numbers_and_maps_every_greek() {
        let row: FlowRow = serde_json::from_str(FLOW_ROW).unwrap();
        let time = row.time().unwrap();
        let print = row.into_print(time);

        assert_eq!(
            time,
            "2024-01-02T15:00:00.075Z".parse::<DateTime<Utc>>().unwrap()
        );
        assert_eq!(print.contract.kind, OptionKind::Call);
        assert_eq!(print.contract.strike, dec!(10.5));
        assert_eq!(
            print.contract.expiry,
            NaiveDate::from_ymd_opt(2024, 1, 5).unwrap()
        );
        // `last_price` arrived as a JSON integer.
        assert_eq!(print.price, dec!(1));
        assert_eq!(print.premium, dec!(300));
        assert_eq!(print.greeks.implied_volatility, Some(0.2));
        assert_eq!(print.greeks.rho, Some(0.003));
        assert_eq!(print.greeks.underlying_price, Some(11.0));
        assert_eq!(print.greeks.theoretical_price, None);
    }

    #[test]
    fn a_flow_row_with_null_greeks_decodes_them_as_none() {
        let json = FLOW_ROW
            .replace(r#""iv":0.2"#, r#""iv":null"#)
            .replace(r#""delta":0.5"#, r#""delta":null"#)
            .replace(r#""rho":0.003"#, r#""rho":null"#);
        let row: FlowRow = serde_json::from_str(&json).unwrap();
        let print = row.into_print(Utc::now());

        assert_eq!(print.greeks.implied_volatility, None);
        assert_eq!(print.greeks.delta, None);
        assert_eq!(print.greeks.rho, None);
    }

    #[test]
    fn a_print_is_a_public_trade_sized_in_contracts_with_no_side() {
        let row: FlowRow = serde_json::from_str(FLOW_ROW).unwrap();
        let trade = row.into_print(Utc::now()).public_trade();

        assert_eq!(trade.id, "7");
        assert_eq!(trade.price, dec!(1));
        assert_eq!(trade.amount, dec!(3));
        assert_eq!(trade.side, None);
    }

    #[test]
    fn an_option_candle_row_keeps_prints_as_trade_count_and_averages_as_greeks() {
        let json = r#"{"ticker":"TEST240105P00010000","underlying":"TEST","strike":10,
            "expiry":"2024-01-05","contract_type":"put","minute":"2024-01-02 15:00:00","dte":3,
            "open":1.0,"high":2.0,"low":0.5,"close":1.5,"volume":40,"premium":6000,"print_count":9,
            "iv_avg":0.3,"delta_avg":-0.4,"gamma_avg":0.1,"theta_avg":-0.02,"vega_avg":0.05,
            "rho_avg":-0.001,"underlying_price":10.25}"#;
        let row: OptionCandleRow = serde_json::from_str(json).unwrap();
        let open = row.open_time().unwrap();
        let candle = row.into_option_candle(open + TimeDelta::minutes(1));

        assert_eq!(candle.contract.kind, OptionKind::Put);
        assert_eq!(candle.candle.trade_count, Some(9));
        assert_eq!(candle.candle.volume, Some(dec!(40)));
        assert_eq!(
            candle.candle.close_time,
            "2024-01-02T15:01:00Z".parse::<DateTime<Utc>>().unwrap()
        );
        assert_eq!(candle.greeks.delta, Some(-0.4));
        assert_eq!(candle.greeks.rho, Some(-0.001));
        assert_eq!(candle.greeks.underlying_price, Some(10.25));
    }

    #[test]
    fn a_print_becomes_a_trade_then_its_greeks_stamped_at_the_print() {
        let row: FlowRow = serde_json::from_str(FLOW_ROW).unwrap();
        let time = row.time().unwrap();
        let [trade, greeks] = row.into_print(time).into_market_events(0usize);

        for event in [&trade, &greeks] {
            assert_eq!(event.exchange, ExchangeId::LseOptions);
            assert_eq!(event.time_exchange, time);
            assert_eq!(event.time_received, time);
        }
        assert!(matches!(trade.kind, DataKind::Trade(_)));
        assert!(matches!(greeks.kind, DataKind::OptionGreeks(ref g) if g.rho == Some(0.003)));
    }

    #[test]
    fn an_option_candle_is_stamped_at_its_close_so_the_minute_is_never_seen_early() {
        let json = r#"{"ticker":"TEST240105P00010000","underlying":"TEST","strike":10,
            "expiry":"2024-01-05","contract_type":"put","minute":"2024-01-02 15:00:00",
            "open":1.0,"high":2.0,"low":0.5,"close":1.5,"volume":40,"premium":6000,"print_count":9}"#;
        let row: OptionCandleRow = serde_json::from_str(json).unwrap();
        let close = row.open_time().unwrap() + TimeDelta::minutes(1);
        let [bar, greeks] = row.into_option_candle(close).into_market_events(0usize);

        assert_eq!(bar.time_exchange, close);
        assert_eq!(greeks.time_exchange, close);
        assert!(matches!(bar.kind, DataKind::Candle(c) if c.close_time == close));
    }

    #[test]
    fn a_malformed_print_timestamp_is_a_typed_error() {
        let json = FLOW_ROW.replace("2024-01-02 15:00:00.075000", "not-a-time");
        let row: FlowRow = serde_json::from_str(&json).unwrap();

        assert!(matches!(row.time(), Err(LseError::Deserialize { .. })));
    }

    #[test]
    fn whole_second_rounding() {
        let exact = "2024-01-02T15:00:00Z".parse::<DateTime<Utc>>().unwrap();
        let fractional = "2024-01-02T15:00:00.25Z".parse::<DateTime<Utc>>().unwrap();

        assert_eq!(floor_to_second(fractional), exact);
        assert_eq!(ceil_to_second(fractional), exact + TimeDelta::seconds(1));
        assert_eq!(ceil_to_second(exact), exact);
    }
}
