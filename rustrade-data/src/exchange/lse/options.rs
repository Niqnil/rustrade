//! US equity and ETF option prints, chains, candles and Greeks from LSE Data 3.0.
//!
//! ```no_run
//! # use chrono::{Duration, TimeZone, Utc};
//! # use futures::StreamExt;
//! # use rustrade_data::exchange::lse::client::LseClient;
//! # use rustrade_data::exchange::lse::error::LseError;
//! # async fn example() -> Result<(), LseError> {
//! let client = LseClient::from_env()?;
//! let start = Utc.with_ymd_and_hms(2026, 11, 25, 15, 0, 0).unwrap();
//! let end = start + Duration::minutes(5);
//!
//! let prints = client.fetch_option_flow(Some("SPY"), start, end);
//! futures::pin_mut!(prints);
//!
//! while let Some(print) = prints.next().await {
//!     let print = print?;
//!     let (contract, greeks) = (&print.contract, &print.greeks);
//!     println!("{} {} x{} delta={:?}", contract.ticker, print.price, print.size, greeks.delta);
//! }
//! # Ok(())
//! # }
//! ```
//!
//! # ⚠️ Licensing
//! Data retrieved here is **not redistributable**. See the [module documentation](super#licensing).
//!
//! # What these feeds are — and are not
//! - **Prints, not quotes.** A flow row is an executed option trade, and the chain carries each
//!   contract's last trade. There is no bid, ask or book on these paths, so nothing here can build
//!   an [`OrderBookL1`](crate::subscription::book::OrderBookL1).
//! - **⚠️ A print's Greeks are those of its minute.** Each flow row carries the Greeks the provider
//!   computed for the minute it traded in, and the chain carries each contract's Greeks as of its
//!   [`greeks_at`](LseOptionChainEntry::greeks_at). A contract that does not trade is not marked:
//!   check the age before treating a figure as a current risk number.
//! - **No print identifier.** The provider sends none, so two prints of one contract at one size
//!   and price in one nanosecond cannot be told apart. The walk preserves the provider's order.
//! - **Strikes are not always round.** Contracts adjusted for a corporate action carry fractional
//!   strikes (`2.67`), so the strike is a [`Decimal`] and the contract's identity is its
//!   [`ticker`](LseOptionContract::ticker).
//! - **⚠️ A contract is read from its ticker, not from the row's separate fields.** The served
//!   `strike` is a float and has carried noise (`504.99999999999994` for a 505 strike), so
//!   [`kind`](LseOptionContract::kind), [`strike`](LseOptionContract::strike) and
//!   [`expiry`](LseOptionContract::expiry) are all read exactly from the OSI ticker. A row whose
//!   ticker is not an OSI symbol is a decode error.
//! - **Two spellings of a contract.** The OSI ticker (`SPY261127C00650000`) is the identity used
//!   here and on the websocket; the REST market data selection spells the same contract with its
//!   root padded to six characters (`SPY   261127C00650000`), which
//!   [`rest_symbol`](LseOptionContract::rest_symbol) produces. The unpadded spelling is refused
//!   there with `404 symbol-unknown`.
//! - **Exercise style is not reported**, so none is claimed: most listed US equity options are
//!   American, but index options are European.

use crate::event::{DataKind, MarketEvent};
use crate::exchange::lse::client::LseClient;
use crate::exchange::lse::error::LseError;
use crate::exchange::lse::historical::{
    LseCandleRequest, Window, cell, csv_records, parse_decimal, parse_provider_time, windows,
};
use crate::exchange::lse::market::LseMarketCode;
use crate::exchange::osi;
use crate::subscription::{
    candle::{Candle, CandleInterval},
    greeks::OptionGreeks,
    trade::PublicTrade,
};
use async_stream::try_stream;
use chrono::{DateTime, NaiveDate, TimeDelta, Utc};
use futures::{Stream, StreamExt};
use rust_decimal::Decimal;
use rustrade_instrument::{exchange::ExchangeId, instrument::kind::option::OptionKind};
use serde::{Deserialize, Serialize};
use smol_str::{SmolStr, format_smolstr};
use tracing::debug;

/// How long after a moment the provider's print tape is taken to be complete up to it.
///
/// Ingestion lag on the print tape was measured as variable and **episodic** on the provider's
/// pre-3.0 API: a window was usually complete about fifteen seconds after it closed, but one
/// returned no rows at all thirty seconds after closing, and another sat at a fifth of its final
/// count across two reads. Both answered `200`. It has not been re-measured on LSE Data 3.0, and a
/// late print also shifts the offsets the walk pages by, so
/// [`fetch_option_flow`](LseClient::fetch_option_flow) refuses a range ending inside this margin
/// rather than return rows that look final and are not. A floor informed by observation, not a
/// guarantee.
pub const OPTION_FLOW_SETTLE_MARGIN: TimeDelta = TimeDelta::seconds(60);

/// Format of a print-tape bound: UTC, with the fraction the provider accepts (to the nanosecond).
const FLOW_BOUND_FORMAT: &str = "%Y-%m-%d %H:%M:%S%.f";

/// The widest window one walk step pages through. The provider refuses a sweep of the whole tape
/// (no underlying) over more than seven days; a day keeps every window well inside that.
const MAX_FLOW_WINDOW: TimeDelta = TimeDelta::days(1);

/// Rows asked for per page of the print tape: the provider's maximum.
const FLOW_PAGE_ROWS: u32 = 5000;

/// Length of an OSI symbol's suffix: `YYMMDD`, `C`/`P` and an eight-digit strike.
const OSI_SUFFIX_LEN: usize = 15;

/// Width the REST selection pads an option root to.
const REST_ROOT_WIDTH: usize = 6;

/// One option contract, as the provider identifies it.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct LseOptionContract {
    /// The OSI contract symbol, e.g. `SPY260928C00764000` — the contract's identity.
    pub ticker: SmolStr,
    /// The underlying's symbol, e.g. `SPY`.
    pub underlying: SmolStr,
    /// Read from [`ticker`](Self::ticker), like `strike` and `expiry`.
    pub kind: OptionKind,
    /// Fractional on contracts adjusted for a corporate action. Read from
    /// [`ticker`](Self::ticker), so it is exact and spells the ticker back.
    pub strike: Decimal,
    /// Read from [`ticker`](Self::ticker), like `kind` and `strike`.
    pub expiry: NaiveDate,
}

impl LseOptionContract {
    /// Build a contract from a served row, reading its kind, strike and expiry from the ticker.
    ///
    /// ⚠️ The row's own `strike`, `contract_type` and `expiry` fields are deliberately not decoded:
    /// the strike is a JSON float that has carried noise, and the ticker carries all three exactly.
    /// `underlying` stays the row's: an adjusted contract's OSI root (`SPY1`) is not its
    /// underlying's symbol.
    fn from_row(ticker: SmolStr, underlying: SmolStr) -> Result<Self, LseError> {
        let parsed = osi::parse(&ticker).ok_or_else(|| LseError::Deserialize {
            message: format!(
                "option ticker {ticker:?} is not an OSI contract symbol with a calendar expiry \
                 and a positive strike"
            ),
        })?;
        let (kind, strike, expiry) = (parsed.kind, parsed.strike, parsed.expiry);

        Ok(Self {
            ticker,
            underlying,
            kind,
            strike,
            expiry,
        })
    }

    /// The contract as the REST market data selection spells it: root padded to six characters.
    #[must_use]
    pub fn rest_symbol(&self) -> SmolStr {
        rest_symbol(&self.ticker).unwrap_or_else(|| self.ticker.clone())
    }
}

/// An OSI ticker as the REST selection spells it, or `None` if `ticker` is not an OSI symbol or
/// its root is longer than the padded width.
pub fn rest_symbol(ticker: &str) -> Option<SmolStr> {
    let root = osi::root(ticker)?;
    if root.len() > REST_ROOT_WIDTH {
        return None;
    }
    let suffix = &ticker[ticker.len() - OSI_SUFFIX_LEN..];
    Some(format_smolstr!("{root:<REST_ROOT_WIDTH$}{suffix}"))
}

/// One executed option trade, with the Greeks of the minute it traded in.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LseOptionPrint {
    /// When the print was recorded, UTC, to the nanosecond.
    pub time: DateTime<Utc>,
    pub contract: LseOptionContract,
    /// The provider's number for the contract.
    pub pair_id: u64,
    /// Premium per share.
    pub price: Decimal,
    /// Size in contracts.
    pub size: u64,
    /// Notional premium, as the provider reports it: `price × size × 100`. Carries float noise
    /// (`249219.99999999997`).
    pub premium: Decimal,
    /// The exchange the print was reported from, as the OPRA participant letter (`A` is NYSE
    /// American).
    pub venue: SmolStr,
    /// The Greeks of the print's minute, with the underlying's price in
    /// [`underlying_price`](OptionGreeks::underlying_price). Any figure may be `None`, and the
    /// provider publishes no `rho`.
    pub greeks: OptionGreeks,
}

impl LseOptionPrint {
    /// This print as a library [`PublicTrade`], sized in contracts.
    ///
    /// `id` is empty, because the provider sends no print identifier, and `side` is `None`: the
    /// provider reports no aggressor.
    #[must_use]
    pub fn public_trade(&self) -> PublicTrade {
        PublicTrade {
            id: SmolStr::default(),
            price: self.price,
            amount: Decimal::from(self.size),
            side: None,
        }
    }

    /// This print as the market events an engine consumes: its trade, then its Greeks.
    ///
    /// Both are stamped with the print's [`time`](Self::time) as `time_exchange` **and**
    /// `time_received` — a historical fetch has no receipt instant of its own — on
    /// [`ExchangeId::LseOptions`]. The Greeks event is emitted even when every figure is `None`.
    ///
    /// # ⚠️ The Greeks are the print's minute's
    /// They may have been computed after the print, so a backtest replaying them at the print's
    /// time can see a figure a little early.
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

/// One contract of an option chain, as `/v3/options/chain` serves it.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LseOptionChainEntry {
    pub contract: LseOptionContract,
    /// The provider's number for the contract.
    pub pair_id: u64,
    /// The contract's last trade price, if it has traded.
    pub last_price: Option<Decimal>,
    /// When it last traded.
    pub last_trade_at: Option<DateTime<Utc>>,
    /// Contracts traded today.
    pub volume_today: Option<u64>,
    /// Notional premium traded today.
    pub premium_today: Option<Decimal>,
    /// The contract's Greeks as of [`greeks_at`](Self::greeks_at), with the underlying's price
    /// then.
    pub greeks: OptionGreeks,
    /// When the Greeks were computed.
    pub greeks_at: Option<DateTime<Utc>>,
}

/// One page of an option chain.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LseOptionChain {
    /// The underlying, as the provider echoes it.
    pub underlying: SmolStr,
    /// The underlying's price, and when it was taken.
    pub underlying_price: Option<Decimal>,
    pub underlying_at: Option<DateTime<Utc>>,
    /// How many contracts matched the filters, across every page.
    pub contracts: u64,
    /// This page's contracts.
    pub entries: Vec<LseOptionChainEntry>,
}

/// One minute's Greeks for one option contract, from the `greeks` selection.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LseOptionMinuteGreeks {
    /// The minute's close: its open plus one minute. The instant the figures are stamped with.
    pub close_time: DateTime<Utc>,
    /// The instant of the option price the figures were computed from.
    pub price_at: Option<DateTime<Utc>>,
    /// The option price the figures were computed from.
    pub option_price: Option<Decimal>,
    /// Where that price came from, as the provider names it.
    pub price_source: Option<SmolStr>,
    /// The figures, with the underlying's price in
    /// [`underlying_price`](OptionGreeks::underlying_price). A figure the provider's model could
    /// not compute is `None`; no `rho` is published.
    pub greeks: OptionGreeks,
    /// The model inputs the provider used: years to expiry, the rate, and the present value of
    /// dividends.
    pub years_to_expiry: Option<f64>,
    pub rate: Option<f64>,
    pub dividends_pv: Option<f64>,
}

impl LseOptionMinuteGreeks {
    /// These Greeks as the market event an engine consumes, stamped at the minute's close on
    /// [`ExchangeId::LseOptions`], so the minute's figures are never seen before it ends.
    #[must_use]
    pub fn into_market_event<InstrumentKey>(
        self,
        instrument: InstrumentKey,
    ) -> MarketEvent<InstrumentKey, DataKind> {
        option_event(
            self.close_time,
            instrument,
            DataKind::OptionGreeks(self.greeks),
        )
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

/// One `/v3/options/flow` row as served. Unknown fields (`symbol`, `strike`, `expiry`, `dte`,
/// `contract_type`) are ignored rather than rejected; the contract is read from `ticker`.
#[derive(Debug, Deserialize)]
struct FlowRow {
    ts: String,
    underlying: SmolStr,
    ticker: SmolStr,
    pair_id: u64,
    price: Decimal,
    size: u64,
    premium: Decimal,
    #[serde(default)]
    venue: SmolStr,
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
}

impl FlowRow {
    fn into_print(self, time: DateTime<Utc>) -> Result<LseOptionPrint, LseError> {
        Ok(LseOptionPrint {
            time,
            contract: LseOptionContract::from_row(self.ticker, self.underlying)?,
            pair_id: self.pair_id,
            price: self.price,
            size: self.size,
            premium: self.premium,
            venue: self.venue,
            greeks: OptionGreeks {
                delta: self.delta,
                gamma: self.gamma,
                theta: self.theta,
                vega: self.vega,
                rho: None,
                implied_volatility: self.iv,
                theoretical_price: None,
                underlying_price: self.underlying_price,
            },
        })
    }
}

/// One `/v3/options/flow` page.
#[derive(Debug, Deserialize)]
struct FlowPage {
    rows: Vec<FlowRow>,
}

/// One `/v3/options/chain` row as served.
#[derive(Debug, Deserialize)]
struct ChainRow {
    ticker: SmolStr,
    underlying: SmolStr,
    pair_id: u64,
    #[serde(default)]
    last_price: Option<Decimal>,
    #[serde(default)]
    last_trade_at: Option<String>,
    #[serde(default)]
    volume_today: Option<u64>,
    #[serde(default)]
    premium_today: Option<Decimal>,
    #[serde(default)]
    greeks_at: Option<String>,
    #[serde(default)]
    greeks_underlying_price: Option<f64>,
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
}

/// One `/v3/options/chain` page.
#[derive(Debug, Deserialize)]
struct ChainPage {
    underlying: SmolStr,
    #[serde(default)]
    underlying_price: Option<Decimal>,
    #[serde(default)]
    underlying_at: Option<String>,
    contracts: u64,
    rows: Vec<ChainRow>,
}

/// Parse an optional provider timestamp.
fn optional_time(label: Option<&str>) -> Result<Option<DateTime<Utc>>, LseError> {
    label
        .filter(|label| !label.is_empty())
        .map(parse_provider_time)
        .transpose()
}

impl TryFrom<ChainPage> for LseOptionChain {
    type Error = LseError;

    fn try_from(page: ChainPage) -> Result<Self, LseError> {
        let entries = page
            .rows
            .into_iter()
            .map(|row| {
                Ok(LseOptionChainEntry {
                    contract: LseOptionContract::from_row(row.ticker, row.underlying)?,
                    pair_id: row.pair_id,
                    last_price: row.last_price,
                    last_trade_at: optional_time(row.last_trade_at.as_deref())?,
                    volume_today: row.volume_today,
                    premium_today: row.premium_today,
                    greeks_at: optional_time(row.greeks_at.as_deref())?,
                    greeks: OptionGreeks {
                        delta: row.delta,
                        gamma: row.gamma,
                        theta: row.theta,
                        vega: row.vega,
                        rho: None,
                        implied_volatility: row.iv,
                        theoretical_price: None,
                        underlying_price: row.greeks_underlying_price,
                    },
                })
            })
            .collect::<Result<_, LseError>>()?;

        Ok(Self {
            underlying: page.underlying,
            underlying_price: page.underlying_price,
            underlying_at: optional_time(page.underlying_at.as_deref())?,
            contracts: page.contracts,
            entries,
        })
    }
}

/// Parse one row of the `greeks` selection's CSV.
fn minute_greeks(
    columns: &crate::exchange::lse::historical::CsvColumns,
    record: &csv::StringRecord,
) -> Result<LseOptionMinuteGreeks, LseError> {
    let float = |name: &str| -> Result<Option<f64>, LseError> {
        columns
            .optional(name)
            .and_then(|index| cell(record, index))
            .map(|cell| {
                cell.parse::<f64>().map_err(|error| LseError::Deserialize {
                    message: format!("column {name}: invalid number {cell:?}: {error}"),
                })
            })
            .transpose()
    };
    let time = columns.required("time")?;
    let open =
        parse_provider_time(cell(record, time).ok_or(LseError::NullValue { column: "time" })?)?;
    let close_time =
        open.checked_add_signed(TimeDelta::minutes(1))
            .ok_or(LseError::TimestampOverflow {
                open,
                interval: CandleInterval::Min1,
            })?;

    Ok(LseOptionMinuteGreeks {
        close_time,
        price_at: optional_time(
            columns
                .optional("price_at")
                .and_then(|index| cell(record, index)),
        )?,
        option_price: columns
            .optional("option_price")
            .and_then(|index| cell(record, index))
            .map(|cell| parse_decimal("option_price", cell))
            .transpose()?,
        price_source: columns
            .optional("price_source")
            .and_then(|index| cell(record, index))
            .map(SmolStr::new),
        greeks: OptionGreeks {
            delta: float("delta")?,
            gamma: float("gamma")?,
            theta: float("theta")?,
            vega: float("vega")?,
            rho: None,
            implied_volatility: float("iv")?,
            theoretical_price: None,
            underlying_price: float("underlying_price")?,
        },
        years_to_expiry: float("years_to_expiry")?,
        rate: float("rate")?,
        dividends_pv: float("dividends_pv")?,
    })
}

impl LseClient {
    /// Fetch candles for one option contract.
    ///
    /// [`fetch_candles`](Self::fetch_candles) on the US options market, with the contract spelt as
    /// the selection requires (its root padded to six characters): the same range contract, checks
    /// and errors. [`volume`](Candle::volume) is in contracts and
    /// [`trade_count`](Candle::trade_count) the number of prints. Minutes with no print are absent.
    ///
    /// ⚠️ Bars have been observed before the US regular session opens (from `12:30` UTC); check
    /// them against your session calendar.
    ///
    /// # Errors
    /// [`LseError::InvalidInput`] for a `ticker` that is not an OSI symbol, else see
    /// [`fetch_candles`](Self::fetch_candles).
    #[must_use = "fetch_option_candles returns a lazy Stream that does nothing unless polled"]
    pub fn fetch_option_candles<'a>(
        &'a self,
        ticker: &'a str,
        interval: CandleInterval,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> impl Stream<Item = Result<Candle, LseError>> + 'a {
        try_stream! {
            let symbol = rest_symbol(ticker).ok_or_else(|| LseError::InvalidInput {
                message: format!("{ticker:?} is not an OSI option symbol with a root of six characters or fewer"),
            })?;
            let candles = self.fetch_candles(LseCandleRequest::new(
                LseMarketCode::UsOptions,
                symbol,
                interval,
                start,
                end,
            ));
            futures::pin_mut!(candles);
            while let Some(candle) = candles.next().await {
                yield candle?;
            }
        }
    }

    /// Fetch one option contract's Greeks, one row per traded minute.
    ///
    /// Yields the minutes whose [`close_time`](LseOptionMinuteGreeks::close_time) falls in
    /// `[start, end]` (**both inclusive**), ascending, in verified chunks like
    /// [`fetch_candles`](Self::fetch_candles). A minute with no trade has no row, and a chunk the
    /// provider stores nothing for yields none, as there.
    ///
    /// # Errors
    /// [`LseError::InvalidInput`] for a `ticker` that is not an OSI symbol or an inverted range; a
    /// row out of order is [`LseError::NonMonotonicCandles`]; else as
    /// [`fetch_candles`](Self::fetch_candles).
    #[must_use = "fetch_option_greeks returns a lazy Stream that does nothing unless polled"]
    pub fn fetch_option_greeks<'a>(
        &'a self,
        ticker: &'a str,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> impl Stream<Item = Result<LseOptionMinuteGreeks, LseError>> + 'a {
        try_stream! {
            let symbol = rest_symbol(ticker).ok_or_else(|| LseError::InvalidInput {
                message: format!("{ticker:?} is not an OSI option symbol with a root of six characters or fewer"),
            })?;
            if start > end {
                Err(LseError::InvalidInput {
                    message: format!("start ({start}) must not be after end ({end})"),
                })?;
            }

            let mut previous: Option<DateTime<Utc>> = None;
            for window in windows(CandleInterval::Min1, start, end, self.chunk_bars().get()) {
                let (from, to) = window.bounds();
                let query = [
                    ("market", LseMarketCode::UsOptions.as_str().to_owned()),
                    ("symbol", symbol.to_string()),
                    ("data", "greeks".to_owned()),
                    ("timeframe", "1m".to_owned()),
                    ("from", from),
                    ("to", to),
                    ("format", "csv".to_owned()),
                ];
                let Some(body) = self.get_export(&query).await? else {
                    debug!(%symbol, ?window, "lse option greeks chunk: no stored data");
                    continue;
                };
                let (columns, records) = csv_records(&body)?;
                debug!(%symbol, ?window, rows = records.len(), "lse option greeks chunk received");

                let Window::Times { from: window_start, to: window_end } = window else {
                    continue;
                };
                let mut minutes = Vec::with_capacity(records.len());
                for record in &records {
                    let minute = minute_greeks(&columns, record)?;
                    let open = minute.close_time - TimeDelta::minutes(1);
                    if open < window_start || open >= window_end {
                        Err(LseError::UnexpectedCandleRange {
                            symbol: symbol.to_string(),
                            open,
                            window_start,
                            window_end,
                        })?;
                    }
                    if let Some(previous) = previous
                        && open <= previous
                    {
                        Err(LseError::NonMonotonicCandles {
                            symbol: symbol.to_string(),
                            previous_open: previous,
                            open,
                        })?;
                    }
                    previous = Some(open);
                    if minute.close_time >= start && minute.close_time <= end {
                        minutes.push(minute);
                    }
                }
                for minute in minutes {
                    yield minute;
                }
            }
        }
    }

    /// Fetch one page of an underlying's option chain, nearest expiry first.
    ///
    /// `limit` caps the page (the provider allows up to 5,000) and `offset` skips that many matched
    /// contracts; [`contracts`](LseOptionChain::contracts) says how many matched in all.
    ///
    /// # Errors
    /// See [`LseError`]. A row whose ticker is not an OSI symbol is [`LseError::Deserialize`].
    pub async fn fetch_option_chain(
        &self,
        underlying: &str,
        limit: u32,
        offset: u64,
    ) -> Result<LseOptionChain, LseError> {
        let page: ChainPage = self
            .get_json(
                "options/chain",
                &[
                    ("underlying", underlying.to_owned()),
                    ("limit", limit.to_string()),
                    ("offset", offset.to_string()),
                ],
            )
            .await?;
        LseOptionChain::try_from(page)
    }

    /// Fetch every option print in `[start, end)`, oldest first, paging automatically.
    ///
    /// # Range and order
    /// The range is half-open and exact to the nanosecond. It is walked in windows of at most a
    /// day, each paged oldest first until the provider answers an empty page: a short page is not
    /// taken as the end, so a cap lowered by the provider costs one more request instead of
    /// silently dropping prints.
    ///
    /// # Paged by time, not by offset
    /// The provider orders prints by time alone and returns prints sharing an instant in a
    /// different order from one request to the next (measured 2026-11-27). Paging by offset would
    /// then repeat one print and drop another wherever a page boundary splits such a group. Each
    /// page instead starts at an instant: the prints at a page's last instant are held back and
    /// the next page starts at that instant, so the group is read whole by one request. Prints
    /// that are identical in every field are kept, as the provider sends them.
    ///
    /// # Arguments
    /// * `underlying` - Restrict to one underlying (case-insensitive), or `None` for every
    ///   underlying the provider carries.
    /// * `start` / `end` - The half-open time range.
    ///
    /// # Errors
    /// Each yielded item is a `Result`, and the stream ends after the first error.
    /// - [`LseError::InvalidInput`] for an inverted range, or one ending within
    ///   [`OPTION_FLOW_SETTLE_MARGIN`] of now.
    /// - [`LseError::UnexpectedOptionFlowRow`] when a row falls outside the window or underlying it
    ///   was requested for, or steps back in time: the provider ignoring a parameter. Checked
    ///   before any row of that page is yielded. Also when a whole page of prints shares one
    ///   instant, which paging by time cannot get past.
    /// - A limit reached is [`LseError::RateLimited`]; resume by re-calling with `start` one
    ///   nanosecond after the last print's time. An instant's prints are yielded only once all of
    ///   them have been read, so the resumed walk neither repeats nor misses one.
    /// - Otherwise [`LseError::Api`] / [`Http`](LseError::Http) /
    ///   [`Deserialize`](LseError::Deserialize), the last also when a row's ticker is not an OSI
    ///   symbol.
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

            let mut previous: Option<DateTime<Utc>> = None;
            let mut window_start = start;
            while window_start < end {
                let window_end = window_start
                    .checked_add_signed(MAX_FLOW_WINDOW)
                    .map_or(end, |candidate| candidate.min(end));
                let mut cursor = window_start;

                while cursor < window_end {
                    let mut query = vec![
                        ("from", cursor.format(FLOW_BOUND_FORMAT).to_string()),
                        ("to", window_end.format(FLOW_BOUND_FORMAT).to_string()),
                        ("order", "asc".to_owned()),
                        ("limit", FLOW_PAGE_ROWS.to_string()),
                    ];
                    if let Some(underlying) = underlying {
                        query.push(("underlying", underlying.to_owned()));
                    }
                    let page: FlowPage = self.get_json("options/flow", &query).await?;
                    debug!(
                        ?underlying, %cursor, %window_end, rows = page.rows.len(),
                        "lse option flow page received"
                    );
                    if page.rows.is_empty() {
                        break;
                    }
                    let full = page.rows.len() >= FLOW_PAGE_ROWS as usize;

                    // Validate the whole page before yielding any of it, so an ignored parameter
                    // never leaks a partial page into the caller's tape.
                    let mut prints = Vec::with_capacity(page.rows.len());
                    let mut last = previous;
                    for row in page.rows {
                        let time = parse_provider_time(&row.ts)?;
                        let refuse = |message: String| LseError::UnexpectedOptionFlowRow {
                            ticker: row.ticker.clone(),
                            message,
                        };
                        if time < cursor || time >= window_end {
                            Err(refuse(format!(
                                "stamped {time}, outside the requested window [{cursor}, \
                                 {window_end}): the range parameters appear to have been ignored"
                            )))?;
                        }
                        if let Some(underlying) = underlying
                            && !row.underlying.eq_ignore_ascii_case(underlying)
                        {
                            Err(refuse(format!(
                                "underlying {:?} where {underlying:?} was requested: the \
                                 underlying filter appears to have been ignored",
                                row.underlying
                            )))?;
                        }
                        if let Some(last) = last
                            && time < last
                        {
                            Err(refuse(format!(
                                "stamped {time}, before the previous print at {last}: the order \
                                 parameter appears to have been ignored"
                            )))?;
                        }
                        last = Some(time);
                        prints.push(row.into_print(time)?);
                    }

                    // Ascending and non-empty, so the last print holds the page's latest instant.
                    let Some((latest, ticker)) = prints
                        .last()
                        .map(|print| (print.time, print.contract.ticker.clone()))
                    else {
                        break;
                    };
                    if latest == cursor {
                        // Every print sits at the cursor: the group held back last page, now whole
                        // unless it alone filled the page.
                        if full {
                            Err(LseError::UnexpectedOptionFlowRow {
                                ticker,
                                message: format!(
                                    "a full page of {} prints is stamped {latest}: prints sharing \
                                     one instant cannot be paged past",
                                    prints.len()
                                ),
                            })?;
                        }
                        // `latest` is before `window_end`, so one nanosecond on is representable.
                        cursor = latest
                            .checked_add_signed(TimeDelta::nanoseconds(1))
                            .unwrap_or(window_end);
                    } else {
                        // Hold back the latest instant's group, which the page may have split; the
                        // next page starts at it and reads it whole.
                        prints.retain(|print| print.time < latest);
                        cursor = latest;
                    }
                    if let Some(print) = prints.last() {
                        previous = Some(print.time);
                    }
                    for print in prints {
                        yield print;
                    }
                }

                window_start = window_end;
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

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    // Every value in these rows is invented. No row in this repository may be mistaken for provider
    // data, which is not licensed for redistribution (<https://londonstrategicedge.com/licence>).
    // Decoding is value-independent, so a synthetic row proves what a captured one would.

    const FLOW_ROW: &str = r#"{"ts":"2024-01-02 15:00:00.075000123","underlying":"TEST",
        "symbol":"TEST  240105C00010500","ticker":"TEST240105C00010500","strike":10.5,
        "expiry":"2024-01-05","contract_type":"call","dte":3,"pair_id":42,"price":1,"size":3,
        "premium":300,"venue":"A","underlying_price":11.0,"iv":0.2,"delta":0.5,"gamma":0.1,
        "theta":-0.01,"vega":0.02}"#;

    fn print() -> LseOptionPrint {
        let row: FlowRow = serde_json::from_str(FLOW_ROW).unwrap();
        let time = parse_provider_time(&row.ts).unwrap();
        row.into_print(time).unwrap()
    }

    #[test]
    fn a_flow_row_decodes_and_maps_every_greek() {
        let print = print();

        assert_eq!(print.contract.ticker, "TEST240105C00010500");
        assert_eq!((print.pair_id, print.size), (42, 3));
        assert_eq!((print.price, print.premium), (dec!(1), dec!(300)));
        assert_eq!(print.venue, "A");
        assert_eq!(print.time.timestamp_subsec_nanos(), 75_000_123);
        assert_eq!(
            print.greeks,
            OptionGreeks {
                delta: Some(0.5),
                gamma: Some(0.1),
                theta: Some(-0.01),
                vega: Some(0.02),
                rho: None,
                implied_volatility: Some(0.2),
                theoretical_price: None,
                underlying_price: Some(11.0),
            }
        );
    }

    #[test]
    fn a_flow_row_with_null_greeks_decodes_them_as_none() {
        let json = FLOW_ROW
            .replace(r#""iv":0.2"#, r#""iv":null"#)
            .replace(r#""delta":0.5"#, r#""delta":null"#);
        let row: FlowRow = serde_json::from_str(&json).unwrap();

        assert_eq!((row.iv, row.delta), (None, None));
    }

    #[test]
    fn a_print_is_a_public_trade_sized_in_contracts_with_no_side_or_id() {
        let trade = print().public_trade();

        assert_eq!(trade.amount, dec!(3));
        assert_eq!(trade.side, None);
        assert!(trade.id.is_empty());
    }

    #[test]
    fn a_print_becomes_a_trade_then_its_greeks_stamped_at_the_print() {
        let print = print();
        let time = print.time;
        let [trade, greeks] = print.into_market_events(7u8);

        assert!(matches!(trade.kind, DataKind::Trade(_)));
        assert!(matches!(greeks.kind, DataKind::OptionGreeks(_)));
        for event in [&trade, &greeks] {
            assert_eq!((event.time_exchange, event.time_received), (time, time));
            assert_eq!(event.exchange, ExchangeId::LseOptions);
        }
    }

    #[test]
    fn the_contract_is_read_from_the_ticker_not_the_row_fields() {
        let json = FLOW_ROW
            .replace(r#""strike":10.5"#, r#""strike":10.499999999999998"#)
            .replace(r#""contract_type":"call""#, r#""contract_type":"put""#);
        let row: FlowRow = serde_json::from_str(&json).unwrap();
        let contract = row.into_print(Utc::now()).unwrap().contract;

        assert_eq!(contract.strike.to_string(), "10.5");
        assert_eq!(contract.kind, OptionKind::Call);
        assert_eq!(
            contract.expiry,
            NaiveDate::from_ymd_opt(2024, 1, 5).unwrap()
        );
    }

    #[test]
    fn a_ticker_that_is_not_osi_is_a_typed_error() {
        let json = FLOW_ROW.replace("\"ticker\":\"TEST240105C00010500\"", "\"ticker\":\"TEST\"");
        let row: FlowRow = serde_json::from_str(&json).unwrap();
        let error = row.into_print(Utc::now()).unwrap_err();

        assert!(matches!(error, LseError::Deserialize { .. }));
        assert!(error.to_string().contains("\"TEST\""), "{error}");
    }

    #[test]
    fn the_rest_symbol_pads_the_root_to_six_characters() {
        assert_eq!(
            rest_symbol("SPY261127C00650000").unwrap(),
            "SPY   261127C00650000"
        );
        assert_eq!(
            rest_symbol("GOOGL261127P00100500").unwrap(),
            "GOOGL 261127P00100500"
        );
        assert_eq!(
            rest_symbol("ABCDEF261127C00650000").unwrap(),
            "ABCDEF261127C00650000"
        );
        assert_eq!(print().contract.rest_symbol(), "TEST  240105C00010500");
    }

    #[test]
    fn a_root_too_long_or_a_non_osi_ticker_has_no_rest_symbol() {
        assert_eq!(rest_symbol("ABCDEFG261127C00650000"), None);
        assert_eq!(rest_symbol("SPY"), None);
    }

    #[test]
    fn a_chain_page_decodes_times_and_reads_missing_or_empty_ones_as_none() {
        let json = r#"{"underlying":"TEST","underlying_price":11.0,
            "underlying_at":"2024-01-02 15:00:00","contracts":2,"limit":2,"offset":0,"returned":2,
            "greeks_as_of":"2024-01-02 15:00:00","rows":[
            {"ticker":"TEST240105C00010500","symbol":"TEST  240105C00010500","underlying":"TEST",
             "pair_id":1,"last_price":1.5,"last_trade_at":"2024-01-02 14:59:00.5","volume_today":9,
             "premium_today":1350,"greeks_at":"2024-01-02 14:59:00","greeks_underlying_price":11.0,
             "iv":0.2,"delta":0.5,"gamma":0.1,"theta":-0.01,"vega":0.02},
            {"ticker":"TEST240105P00010500","underlying":"TEST","pair_id":2,"last_price":null,
             "last_trade_at":null,"volume_today":0,"premium_today":0,"greeks_at":"",
             "iv":null,"delta":null,"gamma":null,"theta":null,"vega":null}]}"#;
        let page: ChainPage = serde_json::from_str(json).unwrap();
        let chain = LseOptionChain::try_from(page).unwrap();

        assert_eq!(chain.contracts, 2);
        assert_eq!(chain.entries.len(), 2);
        assert_eq!(chain.entries[0].last_price, Some(dec!(1.5)));
        assert_eq!(chain.entries[0].greeks.underlying_price, Some(11.0));
        assert_eq!(
            chain.underlying_at,
            Some(parse_provider_time("2024-01-02 15:00:00").unwrap())
        );
        assert_eq!(
            chain.entries[0].last_trade_at,
            Some(parse_provider_time("2024-01-02 14:59:00.5").unwrap())
        );
        assert_eq!(chain.entries[1].last_trade_at, None);
        assert_eq!(chain.entries[1].greeks_at, None);
        assert_eq!(chain.entries[1].contract.kind, OptionKind::Put);
    }

    #[test]
    fn a_greeks_row_is_stamped_at_the_minute_close_with_null_figures_as_none() {
        let body = b"time,price_at,iv,delta,gamma,theta,vega,option_price,price_source,\
underlying_price,years_to_expiry,rate,dividends_pv\n\
2024-01-02 15:00:00,2024-01-02 15:00:30.5,0.2,\\N,0.1,-0.01,0.02,1.5,trade,11.0,0.01,0.04,0\n";
        let (columns, records) = csv_records(body).unwrap();
        let minute = minute_greeks(&columns, &records[0]).unwrap();

        assert_eq!(
            minute.close_time,
            parse_provider_time("2024-01-02 15:01:00").unwrap()
        );
        assert_eq!(minute.greeks.delta, None);
        assert_eq!(minute.greeks.implied_volatility, Some(0.2));
        assert_eq!(minute.option_price, Some(dec!(1.5)));
        assert_eq!(minute.price_source.as_deref(), Some("trade"));

        let event = minute.clone().into_market_event(1u8);
        assert_eq!(event.time_exchange, minute.close_time);
    }
}
